"""Phase 12 tests: auto-publish (copy + git), run duration, and the
runs/live web endpoints (no network; TestClient only)."""
import os
import sys
import json
import tempfile
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor import config as cfg_mod, publish as publish_mod
from m3u_processor.webui.app import create_app
from m3u_processor.database import Database
from m3u_processor.parser import PlaylistParser, merge_into_db


def _build_cfg(tmp, extra=None):
    data = dict(cfg_mod.DEFAULTS)
    data["database"]["path"] = os.path.join(tmp, "m3u.db")
    data["output"]["dir"] = os.path.join(tmp, "out")
    data["publish"] = {
        "enabled": True,
        "target_dir": os.path.join(tmp, "repo_out"),
        "git": {"enabled": False},  # skip real git in unit tests
    }
    if extra:
        data.update(extra)
    cfgp = os.path.join(tmp, "config.yaml")
    cfg_mod.Config(_data=data).as_yaml()  # ensure serializable
    import yaml
    yaml.safe_dump(data, open(cfgp, "w"))
    cfg = cfg_mod.load_config(config_path=cfgp)
    return cfg


def test_publish_copies_outputs_to_target():
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    os.makedirs(cfg.get("output.dir"))
    # seed a finished output file
    with open(os.path.join(cfg.get("output.dir"), "working.m3u"), "w") as f:
        f.write("#EXTM3U\nhttp://x/y.m3u8\n")
    res = publish_mod.publish_outputs(cfg, run_id="t1")
    assert res["published"] is True, res
    dst = os.path.join(tmp, "repo_out", "working.m3u")
    assert os.path.isfile(dst), res
    with open(dst) as f:
        assert "http://x/y.m3u8" in f.read()
    # git disabled -> skipped note, but copy done
    assert res.get("skipped") == "git push disabled", res


def test_publish_disabled_skips():
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp, extra={"publish": {"enabled": False}})
    res = publish_mod.publish_outputs(cfg, run_id="t2")
    assert res["published"] is False
    assert res.get("skipped") == "publish disabled in config", res


def test_publish_refresh_mode_publishes():
    # Refresh runs regenerate the FULL output from the whole DB, so publishing is
    # safe (not a partial playlist). A refresh run should publish like any other.
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    os.makedirs(cfg.get("output.dir"))
    with open(os.path.join(cfg.get("output.dir"), "working.m3u"), "w") as f:
        f.write("#EXTM3U\nhttp://x/y.m3u8\n")
    res = publish_mod.publish_outputs(cfg, run_id="t3", mode="refresh")
    assert res["published"] is True, res
    # disabling publish (config) still skips
    cfg2 = _build_cfg(tmp, extra={"publish": {"enabled": False}})
    res2 = publish_mod.publish_outputs(cfg2, run_id="t3b", mode="refresh")
    assert res2["published"] is False
    assert "disabled" in (res2.get("skipped") or ""), res2


def test_publish_lock_serializes():
    # The FileLockish lock must serialize: a second acquirer (separate instance,
    # like a second process) blocks until the first releases.
    import threading, time
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, ".publish.lock")
    lock1 = publish_mod.FileLockish(path, timeout=5)
    lock2 = publish_mod.FileLockish(path, timeout=5)
    acquired_second = []
    def second():
        try:
            lock2.acquire()
            acquired_second.append(True)
            lock2.release()
        except TimeoutError:
            acquired_second.append(False)
    lock1.acquire()
    t = threading.Thread(target=second)
    t.start()
    time.sleep(0.3)
    # second should be blocked (not yet acquired) while first holds lock
    assert acquired_second == [], "lock did not block concurrent acquire"
    lock1.release()
    t.join(timeout=6)
    assert acquired_second == [True], "second acquirer should succeed after release"


def test_run_duration_recorded():
    # Orchestrator writes started_at; _finalize computes duration_seconds.
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    from m3u_processor.orchestrator import Orchestrator
    orch = Orchestrator(db, cfg)
    # minimal run over 0 eligible rows (full selects all enabled; our seeded
    # stream may be eligible, so just assert the runs row gets a duration)
    stats = orch.run(mode="full")
    row = db.query(
        "SELECT status, duration_seconds, stats_json FROM runs WHERE run_id=?",
        (orch.run_id,),
    )[0]
    assert row[0] == "completed"
    assert row[1] is not None and row[1] >= 0, row
    # progress_json persisted
    pj = db.query("SELECT progress_json FROM runs WHERE run_id=?", (orch.run_id,))[0][0]
    assert pj is not None
    db.close()


def test_reaper_marks_stale_running_rows_stopped():
    # A run killed/interrupted before _finalize leaves a 'running' zombie that
    # must not shadow the real active run in the UI. The reaper in run()
    # should finalize any prior 'running' row as stopped.
    import json as _json
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    from m3u_processor.orchestrator import Orchestrator
    # seed a stale 'running' row (different pid, old timestamp)
    db.execute(
        "INSERT INTO runs(run_id, mode, started_at, status, stats_json) "
        "VALUES(?, 'full', '2020-01-01T00:00:00+00:00', 'running', ?)",
        ("zombie-999999", _json.dumps({"mode": "full", "checked": 5})),
    )
    db.commit()
    assert db.query("SELECT COUNT(*) FROM runs WHERE status='running'")[0][0] == 1
    # start a real run -> reaper should stop the zombie
    orch = Orchestrator(db, cfg)
    orch.run(mode="quick")
    db.close()
    reopen = Database(cfg.get("database.path"))
    reopen.init_db(backup=False)
    running = reopen.query("SELECT run_id FROM runs WHERE status='running'")
    # the real run is 'completed' (quick over 0 eligible), zombie now 'stopped'
    assert len(running) == 0, running
    z = reopen.query("SELECT status, stats_json FROM runs WHERE run_id='zombie-999999'")[0]
    assert z["status"] == "stopped", z
    assert "interrupted" in (z["stats_json"] or ""), z
    reopen.close()


def test_reaper_stops_same_mode_stale_run():
    # Rule: two runs of the same mode must never both be 'running'. A stale
    # same-mode 'running' row from a KILLED run (dead PID) must be stopped when
    # a new run of that mode starts.
    import json as _json
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    from m3u_processor.orchestrator import Orchestrator, _pid_from_run_id, _process_alive
    # zombie: run_id ending in a PID that does not exist -> dead -> reaped
    dead_pid = 999999
    assert not _process_alive(dead_pid)
    db.execute(
        "INSERT INTO runs(run_id, mode, started_at, status, stats_json) "
        "VALUES(?, 'full', '2020-01-01T00:00:00+00:00', 'running', ?)",
        (f"stale-full-{dead_pid}", _json.dumps({"mode": "full"})),
    )
    db.commit()
    orch = Orchestrator(db, cfg)
    orch.run(mode="full")  # new full run
    db.close()
    reopen = Database(cfg.get("database.path"))
    reopen.init_db(backup=False)
    rows = reopen.query("SELECT run_id, status FROM runs WHERE mode='full'")
    statuses = {r["run_id"]: r["status"] for r in rows}
    assert statuses.get(f"stale-full-{dead_pid}") == "stopped", statuses
    reopen.close()


def test_reaper_keeps_alive_concurrent_run():
    # A 'running' row whose PID is STILL ALIVE (a genuine concurrent run) must
    # NOT be stopped by a new run's reaper — otherwise a scheduled token-refresh
    # would wrongly kill a long-running full pass that overlaps it.
    import json as _json
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    from m3u_processor.orchestrator import Orchestrator
    # alive PID = this test process's own pid, which is running
    alive_pid = os.getpid()
    db.execute(
        "INSERT INTO runs(run_id, mode, started_at, status, stats_json) "
        "VALUES(?, 'full', '2020-01-01T00:00:00+00:00', 'running', ?)",
        (f"live-full-{alive_pid}", _json.dumps({"mode": "full", "checked": 100})),
    )
    db.commit()
    orch = Orchestrator(db, cfg)
    orch.run(mode="refresh")  # a different run starts; reaper must spare the alive full
    db.close()
    reopen = Database(cfg.get("database.path"))
    reopen.init_db(backup=False)
    st = reopen.query("SELECT status FROM runs WHERE run_id=?", (f"live-full-{alive_pid}",))[0][0]
    assert st == "running", f"alive concurrent run was wrongly stopped: {st}"
    reopen.close()


def test_publish_repo_root_searches_upward_not_dirname():
    # Regression: repo_root must be found by walking UP from dst (the out/
    # folder) for a .git, NOT by taking dirname(dst). The old dirname() logic
    # resolved repo_root="/" when dst was an absolute mount like /out, which
    # would `git init` the whole filesystem root.
    import subprocess as _sp
    captured = {}

    def fake_git(args, cwd, auth_file, timeout=120):
        captured.setdefault("calls", []).append((list(args), cwd))
        # pretend every git command succeeds; report .git present at cwd so the
        # "not a git repo" guard passes.
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    orig = publish_mod._git
    publish_mod._git = fake_git
    try:
        tmp = tempfile.mkdtemp()
        # dst = /<tmp>/proj/out  -> .git lives at /<tmp>/proj
        proj = os.path.join(tmp, "proj")
        os.makedirs(os.path.join(proj, "out"))
        os.makedirs(os.path.join(proj, ".git"))
        cfg = _build_cfg(tmp)
        # override publish target_dir to point at proj/out and enable git
        data = dict(cfg_mod.DEFAULTS)
        data["database"]["path"] = os.path.join(tmp, "m3u.db")
        data["output"]["dir"] = os.path.join(tmp, "out")
        data["publish"] = {
            "enabled": True,
            "target_dir": os.path.join(proj, "out"),
            "git": {"enabled": True, "remote": "origin",
                    "branch": "main", "repo_url": "https://github.com/x/y.git"},
        }
        import yaml
        cp = os.path.join(tmp, "config.yaml")
        yaml.safe_dump(data, open(cp, "w"))
        cfg = cfg_mod.load_config(config_path=cp)
        os.makedirs(cfg.get("output.dir"))
        with open(os.path.join(cfg.get("output.dir"), "working.m3u"), "w") as f:
            f.write("#EXTM3U\n")
        res = publish_mod.publish_outputs(cfg, run_id="t-reporoot")
        # repo_root must be the proj dir (where .git is), NOT "/" and NOT tmp
        assert os.path.isdir(os.path.join(proj, ".git"))
        # the init/push cwd should be `proj`, never filesystem root
        cwds = [c for _, c in captured.get("calls", [])]
        assert proj in cwds, f"repo_root not proj; calls={captured.get('calls')}"
        assert "/" not in cwds or all(c != "/" for c in cwds), f"repo_root=/ leak: {cwds}"
        assert res.get("error") is None, res
    finally:
        publish_mod._git = orig


def test_publish_env_creds_build_askpass_without_auth_file():
    # Regression: when no auth_file is configured but GITHUB_USER/GITHUB_PAT
    # env vars are set (Docker / .env model), _git must still supply a
    # GIT_ASKPASS helper containing the PAT so the push can authenticate.
    import subprocess as _sp
    captured = {}

    def fake_run(args, cwd=None, env=None, capture_output=False, text=False, timeout=None):
        # capture the askpass helper that _git wrote, BEFORE its finally deletes it
        ap = (env or {}).get("GIT_ASKPASS")
        if ap and os.path.exists(ap):
            captured["askpass_body"] = open(ap).read()
        class R:
            returncode = 0
            stdout = ""
            stderr = ""
        return R()

    orig_run = publish_mod.subprocess.run
    publish_mod.subprocess.run = fake_run
    old_user, old_pat = os.environ.get("GITHUB_USER"), os.environ.get("GITHUB_PAT")
    os.environ["GITHUB_USER"] = "demo-user"
    os.environ["GITHUB_PAT"] = "ghp_DEMO_TOKEN_1234567890"
    try:
        tmp = tempfile.mkdtemp()
        proj = os.path.join(tmp, "proj")
        os.makedirs(os.path.join(proj, "out"))
        os.makedirs(os.path.join(proj, ".git"))
        cfg = _build_cfg(tmp)
        data = dict(cfg_mod.DEFAULTS)
        data["database"]["path"] = os.path.join(tmp, "m3u.db")
        data["output"]["dir"] = os.path.join(tmp, "out")
        data["publish"] = {
            "enabled": True,
            "target_dir": os.path.join(proj, "out"),
            "git": {"enabled": True, "remote": "origin", "branch": "main",
                    "repo_url": "https://github.com/x/y.git"},
        }
        import yaml
        cp = os.path.join(tmp, "config.yaml")
        yaml.safe_dump(data, open(cp, "w"))
        cfg = cfg_mod.load_config(config_path=cp)
        os.makedirs(cfg.get("output.dir"))
        with open(os.path.join(cfg.get("output.dir"), "working.m3u"), "w") as f:
            f.write("#EXTM3U\n")
        res = publish_mod.publish_outputs(cfg, run_id="t-envcred")
        # the push call must have used an askpass that embeds the PAT
        assert "ghp_DEMO_TOKEN_1234567890" in captured.get("askpass_body", ""), \
            f"env PAT not in askpass: {captured.get('askpass_body')}"
        assert res.get("error") is None, res
    finally:
        publish_mod.subprocess.run = orig_run
        if old_user is None:
            os.environ.pop("GITHUB_USER", None)
        else:
            os.environ["GITHUB_USER"] = old_user
        if old_pat is None:
            os.environ.pop("GITHUB_PAT", None)
        else:
            os.environ["GITHUB_PAT"] = old_pat


def test_api_runs_and_live_endpoints():
    tmp = tempfile.mkdtemp()
    cfg = _build_cfg(tmp)
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    from m3u_processor.orchestrator import Orchestrator
    orch = Orchestrator(db, cfg)
    orch.run(mode="full")
    db.close()

    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    r = c.get("/api/runs?limit=10")
    assert r.status_code == 200
    runs = r.json()
    assert isinstance(runs, list) and len(runs) >= 1
    assert "duration_seconds" in runs[0]
    assert runs[0]["mode"] == "full"
    # live: run is finished -> no active run
    live = c.get("/api/live").json()
    assert live["active"] is False
    # pages render
    assert c.get("/runs").status_code == 200
    assert c.get("/live").status_code == 200
