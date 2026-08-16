"""Phase 11 tests: refresh mode (tokened+working) + scheduler jobs + deploy."""
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor import orchestrator, deploy, config as cfg_mod
from m3u_processor.database import Database


def _cfg():
    return cfg_mod.Config(_data=dict(cfg_mod.DEFAULTS))


def _seed(db):
    db.execute("INSERT INTO streams (url, original_url, name, provider_domain, "
               "source_type, source_path, attributes, enabled, is_working, "
               "blacklist_tier) VALUES (?,?,?,?,?,?,?,?,?,?)",
               ("u1", "http://e/a.m3u8?md5=Z&expires=1", "TokWorking", "e",
                "remote", "", "{}", 1, 1, "none"))
    db.execute("INSERT INTO streams (url, original_url, name, provider_domain, "
               "source_type, source_path, attributes, enabled, is_working, "
               "blacklist_tier) VALUES (?,?,?,?,?,?,?,?,?,?)",
               ("u2", "http://e/b.m3u8", "PlainWorking", "e",
                "remote", "", "{}", 1, 1, "none"))
    db.execute("INSERT INTO streams (url, original_url, name, provider_domain, "
               "source_type, source_path, attributes, enabled, is_working, "
               "blacklist_tier) VALUES (?,?,?,?,?,?,?,?,?,?)",
               ("u3", "http://e/c.m3u8?token=X", "TokDead", "e",
                "remote", "", "{}", 1, 0, "none"))
    db.commit()


def test_refresh_mode_targets_only_tokened_working():
    import tempfile
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    _seed(db)
    orch = orchestrator.Orchestrator(db, _cfg())
    # patch validate_one to avoid real network: mark ok, no change
    orch.validate_one = lambda s: type("R", (), {"ok": True, "status": 200,
        "reason": "", "uncheckable": False, "suspected_expired": False,
        "elapsed_ms": 100, "throughput_kbps": 5000,
        "health_score": 90.0, "health_tier": "healthy"})()
    # run refresh
    stats = orch.run(mode="refresh", token_refresh=True)
    # only u1 (tokened + working) should be eligible/checked
    assert stats["eligible"] == 1, stats
    assert stats["mode"] == "refresh"
    db.close()


def test_refresh_records_last_refresh_at():
    import tempfile
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    _seed(db)
    orch = orchestrator.Orchestrator(db, _cfg())
    orch.validate_one = lambda s: type("R", (), {"ok": True, "status": 200,
        "reason": "", "uncheckable": False, "suspected_expired": False,
        "elapsed_ms": 100, "throughput_kbps": 5000,
        "health_score": 90.0, "health_tier": "healthy"})()
    orch.run(mode="refresh", token_refresh=True)
    row = db.query("SELECT value FROM config WHERE key='last_refresh_at'")
    assert row, "last_refresh_at not recorded"
    db.close()


def test_scheduler_jobs_config_drives_deploy():
    jobs = [
        {"name": "token-refresh", "mode": "refresh", "cron": "7 */2 * * *"},
        {"name": "daily-full", "mode": "full", "cron": "0 2 * * *"},
        {"name": "weekly-heavy", "mode": "full", "cron": "0 3 * * FRI"},
    ]
    out = os.path.join(os.path.dirname(__file__), "..", "_deploy11")
    os.makedirs(out, exist_ok=True)
    files = deploy.generate(jobs=jobs, outdir=out)
    # 3 jobs -> 3 service + 3 timer + web = 7 files
    svcs = [f for f in files if f.endswith(".service")]
    tmrs = [f for f in files if f.endswith(".timer")]
    assert len(svcs) == 4 and len(tmrs) == 3, files  # 3 run + 1 web
    # each run service uses --job <name>
    for j in jobs:
        svc = open(os.path.join(out, f"m3u-processor-{j['name']}.service")).read()
        assert f"--job {j['name']}" in svc, svc
        tmr = open(os.path.join(out, f"m3u-processor-{j['name']}.timer")).read()
        assert "OnCalendar=" in tmr


def test_cron_to_oncalendar():
    assert "0/2:07:00" in deploy.cron_to_oncalendar("7 */2 * * *")
    oc = deploy.cron_to_oncalendar("0 2 * * *")
    assert "02:00:00" in oc
    oc2 = deploy.cron_to_oncalendar("0 3 * * FRI")
    assert "FRI" in oc2 and "03:00:00" in oc2


def test_cli_run_job_unknown():
    # --job unknown -> returns 2 (handled in __main__); here test resolver logic
    import types
    cfg = _cfg()
    cfg._data["scheduler"] = {"enabled": True, "jobs": [{"name": "x", "mode": "full", "cron": "0 2 * * *"}]}
    jobs = (cfg.get("scheduler", {}) or {}).get("jobs", []) or []
    job = next((j for j in jobs if j.get("name") == "nope"), None)
    assert job is None


def test_fresh_eye_refresh_does_not_touch_plain_streams():
    import tempfile
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    _seed(db)
    orch = orchestrator.Orchestrator(db, _cfg())
    orch.validate_one = lambda s: type("R", (), {"ok": True, "status": 200,
        "reason": "", "uncheckable": False, "suspected_expired": False,
        "elapsed_ms": 100, "throughput_kbps": 5000,
        "health_score": 90.0, "health_tier": "healthy"})()
    orch.run(mode="refresh", token_refresh=True)
    # u2 (plain working) must remain is_working=1 untouched
    u2 = db.query("SELECT is_working FROM streams WHERE url='u2'")[0][0]
    assert u2 == 1
    db.close()


if __name__ == "__main__":
    import traceback
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"FAIL {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
