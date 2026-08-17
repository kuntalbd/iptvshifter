"""Tests for the Favorites subsystem (§Fav): separate from the main pipeline.

Covers: favorites table creation + schema migration, add/list/enable/delete/group,
edit, validate-now (recording health, include-disabled toggle), refresh-mode Part B
(favorite token refresh + favorite.m3u export into output dir), and pass counters.
Also the `is_run_active` rename on /api/run-status and /api/live.
"""
import os
import tempfile
import yaml

import pytest

from m3u_processor import config as cfg_mod
from m3u_processor.database import Database
from m3u_processor.webui.app import create_app
from fastapi.testclient import TestClient


def _cfg(tmp):
    dbp = os.path.join(tmp, "m3u.db")
    cfgd = dict(cfg_mod.DEFAULTS)
    cfgd["database"]["path"] = dbp
    cfgd["output"]["dir"] = os.path.join(tmp, "out")
    cfgd["publish"] = {"enabled": True, "target_dir": os.path.join(tmp, "repo_out"),
                       "git": {"enabled": False}}
    cp = os.path.join(tmp, "config.yaml")
    yaml.safe_dump(cfgd, open(cp, "w"))
    return cfg_mod.load_config(config_path=cp)


@pytest.fixture
def client(tmp_path):
    cfg = _cfg(str(tmp_path))
    return TestClient(create_app(cfg))


def test_favorites_tables_created(tmp_path):
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    names = {r[0] for r in db.query(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"favorites", "favorite_groups", "favorite_membership"} <= names
    db.close()


def test_favorites_add_list_group(client):
    r = client.post("/api/favorites/add",
                    json={"url": "http://x/a.m3u", "name": "A", "group": "G1"})
    assert r.json()["ok"] and r.json()["id"] == 1
    rows = client.get("/api/favorites").json()
    assert len(rows) == 1
    assert rows[0]["groups"] == "G1"
    assert rows[0]["is_enabled"] is True
    assert rows[0]["is_working"] is None


def test_favorites_add_existing_missing(client):
    r = client.post("/api/favorites/add-existing", json={"url": "http://nope/z.m3u"})
    assert r.json() == {"ok": False, "error": "stream not found"}


def test_favorites_enable_and_delete(client):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    fid = client.get("/api/favorites").json()[0]["id"]
    client.post("/api/favorites/set-enabled", json={"id": fid, "enabled": False})
    assert client.get("/api/favorites").json()[0]["is_enabled"] is False
    client.post("/api/favorites/delete", json={"id": fid})
    assert client.get("/api/favorites").json() == []


def test_favorites_batch_group(client):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    client.post("/api/favorites/add", json={"url": "http://x/b.m3u", "name": "B"})
    ids = [r["id"] for r in client.get("/api/favorites").json()]
    client.post("/api/favorites/set-group", json={"ids": ids, "group": "Shared"})
    rows = client.get("/api/favorites").json()
    assert all(r["groups"] == "Shared" for r in rows)


def test_favorites_validate_records_health(client):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    # URL is unreachable; validator should record a failure, not crash.
    r = client.post("/api/favorites/validate-now")
    assert r.json()["ok"] and r.json()["checked"] == 1
    row = client.get("/api/favorites").json()[0]
    assert row["is_working"] == 0
    assert row["total_failures"] == 1
    assert row["consecutive_failures"] == 1


def test_favorites_validate_uncheckable_not_counted_as_failure(client):
    # A non-http (rtmp) favorite can't be validated over HTTP; must NOT be
    # recorded as a failure.
    client.post("/api/favorites/add", json={"url": "rtmp://x/a", "name": "R"})
    r = client.post("/api/favorites/validate-now")
    assert r.json()["ok"], r.json()
    row = client.get("/api/favorites").json()[0]
    assert row["is_working"] is None, "uncheckable should leave status untouched"
    assert row["total_failures"] == 0


def test_favorites_pass_counters_track_on_validate(client, tmp_path):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    fid = client.get("/api/favorites").json()[0]["id"]
    # validate-now 2x (URL unreachable -> 2 failures)
    client.post("/api/favorites/validate-now")
    client.post("/api/favorites/validate-now")
    row = client.get("/api/favorites").json()[0]
    assert row["total_failures"] == 2 and row["consecutive_failures"] == 2
    assert row["total_pass"] == 0 and row["consecutive_pass"] == 0
    # simulate successes via direct record (deterministic) on the same DB
    d = Database(client.app.state.cfg.get("database.path")); d.init_db(backup=False)
    d.favorite_record_result(fid, ok=True)
    d.favorite_record_result(fid, ok=True)
    row = d.query("SELECT total_pass, consecutive_pass, total_failures, "
                  "consecutive_failures FROM favorites WHERE id=?", (fid,))[0]
    assert row["total_pass"] == 2 and row["consecutive_pass"] == 2
    assert row["total_failures"] == 2  # failures accumulate
    assert row["consecutive_failures"] == 0  # reset on success
    d.close()


def test_favorites_validate_include_disabled_toggle(client):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    fid = client.get("/api/favorites").json()[0]["id"]
    client.post("/api/favorites/set-enabled", json={"id": fid, "enabled": False})
    # default: disabled excluded
    r = client.post("/api/favorites/validate-now", json={"include_disabled": False})
    assert r.json()["checked"] == 0
    # include_disabled true: validated
    r = client.post("/api/favorites/validate-now", json={"include_disabled": True})
    assert r.json()["checked"] == 1


def test_favorites_edit_endpoint(client):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    fid = client.get("/api/favorites").json()[0]["id"]
    r = client.post("/api/favorites/edit", json={
        "id": fid, "name": "A2", "source_path": "/s/x.m3u", "is_url": True,
        "is_enabled": False})
    assert r.json()["ok"]
    row = client.get("/api/favorites").json()[0]
    assert row["name"] == "A2"
    assert row["source_path"] == "/s/x.m3u"
    assert row["is_url"] is True
    assert row["is_enabled"] is False


def test_refresh_mode_part_b_refreshes_favorites_and_exports(tmp_path):
    # Build a source playlist with a fresh token, seed a favorite pointing at it
    # (enabled + tokened url), run refresh mode, and confirm the favorite's
    # original_url is re-extracted AND favorite.m3u is written.
    src = os.path.join(str(tmp_path), "src.m3u")
    open(src, "w").write(
        "#EXTM3U\n#EXTINF:-1,t\nhttp://x/a.m3u?token=NEW\n")
    dbp = os.path.join(str(tmp_path), "m3u.db")
    outd = os.path.join(str(tmp_path), "out")
    cfgd = dict(cfg_mod.DEFAULTS)
    cfgd["database"]["path"] = dbp
    cfgd["output"]["dir"] = outd
    cfgd["publish"] = {"enabled": False}
    cpath = os.path.join(str(tmp_path), "config.yaml")
    yaml.safe_dump(cfgd, open(cpath, "w"))
    cfg = cfg_mod.load_config(config_path=cpath)
    db = Database(dbp)
    db.init_db(backup=False)
    db.favorite_add(name="A", url="http://x/a.m3u", original_url="http://x/a.m3u?token=OLD",
                    source_path=src, is_url=False)
    from m3u_processor.orchestrator import Orchestrator
    orch = Orchestrator(db, cfg)
    stats = orch.run(mode="refresh")
    assert stats["favorite_token_refreshed"] == 1, stats
    row = db.query("SELECT original_url FROM favorites WHERE id=1")[0]
    assert "token=NEW" in row["original_url"], row["original_url"]
    assert os.path.exists(os.path.join(outd, "favorite.m3u"))
    # POLICY (Option B / Decision 33): favorite.m3u publishes the tokened
    # original_url, mirroring working.m3u, so favorites actually play. After
    # refresh re-extracted a fresh token, that tokened url MUST appear.
    content = open(os.path.join(outd, "favorite.m3u")).read()
    assert "token=NEW" in content, "refreshed tokened original_url not published in favorite.m3u!"
    assert "http://x/a.m3u?token=NEW" in content
    db.close()


def test_favorites_published_m3u_carries_tokened_original_url(tmp_path):
    # Manual add with a tokened original_url: it MUST be published (Option B),
    # same as working.m3u — so the favorite remains playable.
    dbp = os.path.join(str(tmp_path), "m3u.db")
    outd = os.path.join(str(tmp_path), "out")
    cfgd = dict(cfg_mod.DEFAULTS)
    cfgd["database"]["path"] = dbp
    cfgd["output"]["dir"] = outd
    cfgd["publish"] = {"enabled": False}
    cpath = os.path.join(str(tmp_path), "config.yaml")
    yaml.safe_dump(cfgd, open(cpath, "w"))
    cfg = cfg_mod.load_config(config_path=cpath)
    db = Database(dbp); db.init_db(backup=False)
    db.favorite_add(name="A", url="http://x/a.m3u",
                    original_url="http://x/a.m3u?token=SECRET", is_enabled=1)
    from m3u_processor.orchestrator import Orchestrator
    orch = Orchestrator(db, cfg)
    orch.run(mode="refresh")
    content = open(os.path.join(outd, "favorite.m3u")).read()
    assert "SECRET" in content, "tokened original_url should be published (Option B)!"
    assert "http://x/a.m3u?token=SECRET" in content
    db.close()


def test_run_status_uses_is_run_active(client):
    r = client.get("/api/run-status")
    assert "is_run_active" in r.json()
    assert "active" not in r.json()


def test_live_uses_is_run_active(client):
    r = client.get("/api/live")
    assert "is_run_active" in r.json()
