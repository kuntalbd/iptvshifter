"""Tests for the Favorites subsystem (§Fav): separate from the main pipeline.

Covers: favorites table creation, add/list/enable/delete/group, validate-now
(recording health), and export (m3u generation -> prod/out -> out/ -> publish).
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


def test_favorites_export_writes_and_publishes(client, tmp_path):
    client.post("/api/favorites/add", json={"url": "http://x/a.m3u", "name": "A"})
    r = client.post("/api/favorites/export",
                    json={"include_disabled": True, "include_notworking": True})
    j = r.json()
    assert j["ok"], j
    assert os.path.exists(os.path.join(str(tmp_path), "out", "favorite.m3u"))
    # copied to target dir
    assert os.path.exists(os.path.join(str(tmp_path), "repo_out", "favorite.m3u"))


def test_favorites_export_prefers_tokenless_origin(client, tmp_path):
    # A tokened playable url must NOT be published; origin_url (tokenless) should.
    client.post("/api/favorites/add", json={
        "url": "http://x/a.m3u?token=SECRET123", "name": "A",
        "origin_url": "http://x/a.m3u"})
    r = client.post("/api/favorites/export",
                    json={"include_disabled": True, "include_notworking": True})
    assert r.json()["ok"]
    content = open(os.path.join(str(tmp_path), "out", "favorite.m3u")).read()
    assert "SECRET123" not in content, "tokened url leaked into public m3u!"
    assert "http://x/a.m3u" in content


def test_run_status_uses_is_run_active(client):
    r = client.get("/api/run-status")
    assert "is_run_active" in r.json()
    assert "active" not in r.json()


def test_live_uses_is_run_active(client):
    r = client.get("/api/live")
    assert "is_run_active" in r.json()
