"""Phase 13: Solution A (2-phase funnel), refresh plain mode, single-run guard,
scheduler CRUD config persistence, and run-stop plumbing."""
import os, tempfile, json, time
import pytest

from m3u_processor import config as cfg_mod
from m3u_processor.database import Database
from m3u_processor.orchestrator import Orchestrator, _pid_from_run_id, _process_alive
from m3u_processor.config import load_config, save_config

FAKE_M3U = """#EXTM3U
#EXTINF:-1 tvg-name="Ok",Ok
http://example.com/ok/stream.m3u8
#EXTINF:-1 tvg-name="Dead",Dead
http://example.com/dead/stream.m3u8
#EXTINF:-1 tvg-name="Rtmp",Rtmp
rtmp://example.com/live/foo
"""


class RouterClient:
    """Minimal injectable HTTP client (see test_phase5)."""
    def __init__(self):
        self.routes = {}
    def set(self, sub, status, ct="application/vnd.apple.mpegurl"):
        self.routes[sub] = (status, ct)
    def __call__(self, method, url, headers=None, timeout=None, verify=None, allow_redirects=None):
        for sub, (st, ct) in self.routes.items():
            if sub in url:
                return FakeResp(st, ct)
        return FakeResp(404)


class FakeResp:
    def __init__(self, status, ct="application/vnd.apple.mpegurl"):
        self.status_code = status
        self.headers = {"Content-Type": ct}
        self.text = ""


def _cfg():
    return load_config(config_path="examples/config.example.yaml")


def _make_db(tmp):
    db = Database(os.path.join(tmp, "t.db"))
    db.init_db(backup=False)
    return db


def test_two_phase_counts_each_link_once():
    tmp = tempfile.mkdtemp()
    db = _make_db(tmp)
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)
    c = RouterClient()
    c.set("/ok/", 200)
    c.set("/dead/", 404)
    orch = Orchestrator(db, _cfg(), http_client=c)
    orch.ingest_source(pl, source_type="local")
    stats = orch.run(mode="quick")
    assert stats["parsed"] == 3
    assert stats["eligible"] == 3
    # ok=1, dead=1(fail), rtmp=1(uncheckable) -> each counted once
    assert stats["working"] == 1
    assert stats["failed"] == 1
    assert stats["uncheckable"] == 1
    assert stats["checked"] == 3  # all 3 links went through processing (ok+dead+rtmp)


def test_refresh_plain_no_health_check():
    """Refresh mode re-extracts tokens (if source re-readable) without active/
    health validation; it must still reach _finalize and publish-path."""
    tmp = tempfile.mkdtemp()
    db = _make_db(tmp)
    pl = os.path.join(tmp, "pl.m3u")
    # tokened URL; source file re-readable so _refresh_token can update it
    pl_body = """#EXTM3U
#EXTINF:-1 tvg-name="Tok",Tok
http://example.com/tok/stream.m3u8?token=OLD&expires=1
"""
    with open(pl, "w") as f:
        f.write(pl_body)
    c = RouterClient()
    c.set("/tok/", 200)
    orch = Orchestrator(db, _cfg(), http_client=c)
    orch.ingest_source(pl, source_type="local")
    # mark working + tokened so refresh eligibility picks it
    db.execute("UPDATE streams SET is_working=1, blacklist_tier='none' WHERE id=1")
    db.commit()
    stats = orch.run(mode="refresh")
    assert stats.get("mode") == "refresh"
    # elapsed finalize ran (duration captured)
    row = db.query("SELECT status FROM runs WHERE run_id=?", (orch.run_id,))[0]
    assert row["status"] in ("completed", "stopped")


def test_single_run_guard_discards_concurrent():
    tmp = tempfile.mkdtemp()
    db = _make_db(tmp)
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)
    c = RouterClient()
    c.set("/ok/", 200)
    c.set("/dead/", 404)
    orch = Orchestrator(db, _cfg(), http_client=c)
    orch.ingest_source(pl, source_type="local")
    # simulate an active run by acquiring the file lock
    from m3u_processor.orchestrator import RunLock
    held_lock = RunLock(db.path, timeout=2)
    held_lock.acquire()
    try:
        stats = orch.run(mode="quick")
        assert stats.get("discarded") is True
        assert "another run already active" in (stats.get("discard_reason") or "")
        # a discarded row was recorded
        disc = db.query("SELECT status, stats_json FROM runs WHERE run_id=?", (orch.run_id,))[0]
        assert disc["status"] == "discarded"
        sj = json.loads(disc["stats_json"] or "{}")
        assert "another run already active" in sj.get("reason", "")
    finally:
        held_lock.release()


def test_run_guard_keeps_dead_zombie_running_row():
    """A 'running' row whose PID is dead must be reaped (stopped), NOT block."""
    tmp = tempfile.mkdtemp()
    db = _make_db(tmp)
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)
    c = RouterClient()
    c.set("/ok/", 200)
    c.set("/dead/", 404)
    orch = Orchestrator(db, _cfg(), http_client=c)
    orch.ingest_source(pl, source_type="local")
    # zombie: pid 999999 is not alive
    db.execute(
        "INSERT INTO runs(run_id, mode, started_at, status) VALUES(?,?,?,?)",
        ("20200101T0000000000-999999", "full", "2020-01-01T00:00:00+00:00", "running"),
    )
    db.commit()
    stats = orch.run(mode="quick")
    assert stats.get("discarded") is not True  # not blocked
    z = db.query("SELECT status FROM runs WHERE run_id='20200101T0000000000-999999'")[0]
    assert z["status"] == "stopped"  # reaped


def test_scheduler_crud_persists_config(tmp_path):
    # load example config, add a job, save, reload -> job present
    cfg = load_config(config_path="examples/config.example.yaml")
    cfg.data.setdefault("scheduler", {})["jobs"].append(
        {"name": "test-job", "mode": "quick", "cron": "0 4 * * *"}
    )
    p = str(tmp_path / "cfg.yaml")
    cfg.config_path = p
    save_config(cfg, p)
    reloaded = load_config(config_path=p)
    jobs = reloaded.get("scheduler.jobs", [])
    assert any(j.get("name") == "test-job" for j in jobs)
    # remove it
    reloaded.data["scheduler"]["jobs"] = [j for j in jobs if j.get("name") != "test-job"]
    save_config(reloaded, p)
    reloaded2 = load_config(config_path=p)
    assert not any(j.get("name") == "test-job" for j in reloaded2.get("scheduler.jobs", []))


def test_pid_helpers():
    assert _process_alive(os.getpid()) is True
    assert _process_alive(999999) is False
    assert _pid_from_run_id("20200101T0000000000-12345") == 12345
    assert _pid_from_run_id("no-pid") is None


def test_webui_scheduler_and_run_status(tmp_path):
    """Web UI: scheduler list + stop endpoint + run-status plumbing."""
    from fastapi.testclient import TestClient
    from m3u_processor.webui.app import create_app
    from m3u_processor import config as cfg_mod

    cfg = cfg_mod.load_config(config_path="examples/config.example.yaml")
    cfg.config_path = str(tmp_path / "cfg.yaml")
    cfg.config_dir = str(tmp_path)
    app = create_app(cfg)
    client = TestClient(app)

    # scheduler list
    r = client.get("/api/scheduler")
    assert r.status_code == 200
    jobs = r.json()["jobs"]
    assert any(j["name"] == "token-refresh" for j in jobs)

    # add a job (persists to config.yaml on disk)
    r = client.post("/api/scheduler", json={"name": "night", "mode": "quick", "cron": "0 4 * * *"})
    assert r.status_code == 200
    assert r.json()["ok"] is True
    saved = cfg_mod.load_config(config_path=cfg.config_path)
    assert any(j["name"] == "night" for j in saved.get("scheduler.jobs", []))

    # remove it
    import json as _json
    r = client.request("DELETE", "/api/scheduler", json={"name": "night"})
    assert r.status_code == 200
    saved2 = cfg_mod.load_config(config_path=cfg.config_path)
    assert not any(j["name"] == "night" for j in saved2.get("scheduler.jobs", []))

    # run-status with no active run
    r = client.get("/api/run-status")
    assert r.status_code == 200
    assert r.json()["is_run_active"] is False

    # stop with no active run -> ok False
    r = client.post("/api/run/stop")
    assert r.status_code == 200
    assert r.json()["ok"] is False


def test_regenerate_units_enable_not_enable_now(tmp_path, monkeypatch):
    """Scheduler CRUD must `enable` (not `enable --now`) so adding a job does
    NOT immediately fire a run (which would collide with the single-run guard)."""
    from m3u_processor.webui import app as webapp
    from fastapi.testclient import TestClient
    calls = []
    def fake_run(args, capture_output=False, timeout=None):
        calls.append(args)
        class R: returncode = 0
        return R()
    monkeypatch.setattr(webapp.subprocess, "run", fake_run)

    cfg = cfg_mod.load_config(config_path="examples/config.example.yaml")
    cfg.config_path = str(tmp_path / "cfg.yaml")
    cfg.config_dir = str(tmp_path)
    cfg.data["scheduler"]["jobs"] = [{"name": "j1", "mode": "quick", "cron": "0 4 * * *"}]
    app = webapp.create_app(cfg)
    client = TestClient(app)
    r = client.post("/api/scheduler", json={"name": "j1", "mode": "quick", "cron": "0 4 * * *"})
    assert r.status_code == 200
    # ensure no 'enable --now' was invoked
    for c in calls:
        assert "--now" not in c, f"unexpected --now in {c}"
    # exactly one 'enable' call for the job unit
    enables = [c for c in calls if "enable" in c and "daemon-reload" not in c]
    assert any("m3u-processor-j1" in " ".join(c) for c in enables)


def test_webui_run_discard_event(tmp_path, monkeypatch):
    """Web UI: starting a run via API works; discard logic is tested via
    test_single_run_guard_discards_concurrent which directly tests the lock."""
    from fastapi.testclient import TestClient
    from m3u_processor.webui.app import create_app
    from m3u_processor.database import Database
    from m3u_processor import config as cfg_mod

    cfg = cfg_mod.load_config(config_path="examples/config.example.yaml")
    cfg.config_path = str(tmp_path / "cfg.yaml")
    cfg.config_dir = str(tmp_path)
    cfg.set("database.path", str(tmp_path / "t.db"))
    db = Database(str(tmp_path / "t.db"))
    db.init_db(backup=False)
    db.close()

    # ADR-015: /api/run now launches the run as a DETACHED systemd transient
    # unit (m3u-web-<run_id>) instead of an in-process worker thread, so the
    # validation's child processes live OUTSIDE the web service's MemoryMax
    # cgroup. In tests we stub systemd-run to a no-op and drive the run to
    # completion via the DB row (which is exactly what the detached process
    # does), then assert the SSE endpoint streams the done event.
    import subprocess as _sp

    def _noop_systemd_run(*a, **kw):
        class R:
            returncode = 0
            stderr = ""
        return R()

    monkeypatch.setattr(_sp, "run", _noop_systemd_run)

    import json as _j
    app = create_app(cfg)
    client = TestClient(app)
    # start a run via API -> should succeed (no active run to discard against)
    r = client.post("/api/run", json={"mode": "quick"})
    assert r.status_code == 200
    run_id = r.json()["run_id"]
    import time as _t
    # The detached CLI process would write progress + a final 'completed' row
    # to the DB as it runs. Simulate that here so the API contract holds.
    db = Database(str(tmp_path / "t.db"))
    db.execute(
        "INSERT INTO runs(run_id, mode, started_at, status, progress_json, stats_json) "
        "VALUES(?,?,CURRENT_TIMESTAMP,'completed','{\"done\":3,\"total\":3}',"
        "'{\"checked\":3,\"working\":3,\"failed\":0}')",
        (run_id, "quick"),
    )
    db.commit()
    db.close()
    # SSE endpoint should stream the completed event from the DB row
    with client.stream("GET", f"/api/events?run_id={run_id}") as resp:
        assert resp.status_code == 200
        body = "".join(resp.iter_text())
    assert '"type": "done"' in body
    assert run_id in body
    # Wait for run to complete
    for _ in range(50):
        rows = client.get("/api/runs").json()
        match = next((x for x in rows if x["run_id"] == run_id), None)
        if match and match.get("status") in ("completed", "error"):
            break
        _t.sleep(0.1)
    assert match is not None, "run not recorded"
    assert match["status"] == "completed"


def test_finalize_regenerates_output_from_db(tmp_path):
    """CRITICAL: a run must regenerate the output playlists from the DB in
    _finalize (so the published playlist reflects this run's results), not
    merely copy pre-existing stale files. Regression for bug #2."""
    import os as _os
    db = _make_db(str(tmp_path))
    pl = _os.path.join(str(tmp_path), "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)
    c = RouterClient()
    c.set("/ok/", 200)
    c.set("/dead/", 404)
    cfg = _cfg()
    out_dir = str(tmp_path / "out")
    cfg.set("output.dir", out_dir)
    cfg.set("output.formats", ["vlc", "kodi", "tivimate"])
    orch = Orchestrator(db, cfg, http_client=c)
    orch.ingest_source(pl, source_type="local")
    stats = orch.run(mode="quick")
    # output files must now exist and contain the working stream
    working = _os.path.join(out_dir, "working.m3u")
    assert _os.path.isfile(working), "output not regenerated by run"
    body = open(working, encoding="utf-8").read()
    assert "http://example.com/ok/stream.m3u8" in body, "working stream missing from output"
    assert "http://example.com/dead/stream.m3u8" not in body, "dead stream leaked into output"
    assert stats.get("generated_files"), "no generated_files in stats"


def test_refresh_publishes_full_output_not_partial(tmp_path):
    """Refresh mode must still regenerate the FULL output (all working streams)
    in _finalize, because publish copies the whole output dir — a refresh run
    must not push a partial/token-only playlist."""
    import os as _os
    db = _make_db(str(tmp_path))
    pl = _os.path.join(str(tmp_path), "pl.m3u")
    pl_body = (
        "#EXTM3U\n"
        "#EXTINF:-1 tvg-name=\"Tok\",Tok\nhttp://example.com/tok/stream.m3u8?token=OLD&expires=1\n"
        "#EXTINF:-1 tvg-name=\"Plain\",Plain\nhttp://example.com/plain/stream.m3u8\n"
    )
    with open(pl, "w") as f:
        f.write(pl_body)
    c = RouterClient()
    c.set("/tok/", 200)
    c.set("/plain/", 200)
    cfg = _cfg()
    out_dir = str(tmp_path / "out")
    cfg.set("output.dir", out_dir)
    orch = Orchestrator(db, cfg, http_client=c)
    orch.ingest_source(pl, source_type="local")
    db.execute("UPDATE streams SET is_working=1, blacklist_tier='none' WHERE id=1")
    db.execute("UPDATE streams SET is_working=1, blacklist_tier='none' WHERE id=2")
    db.commit()
    orch.run(mode="refresh")
    working = _os.path.join(out_dir, "working.m3u")
    assert _os.path.isfile(working), "refresh run did not regenerate output"
    body = open(working, encoding="utf-8").read()
    assert "http://example.com/plain/stream.m3u8" in body
    assert "http://example.com/tok/stream.m3u8" in body

