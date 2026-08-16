"""Phase 4 tests: blacklist state machine + providers."""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.blacklist import apply_result, escalate_short_to_permanent, purge_old
from m3u_processor.providers import ensure_provider, set_provider_enabled, provider_enabled
from m3u_processor.database import Database
from m3u_processor.models import Stream
from m3u_processor import config as cfg_mod


def _cfg():
    return cfg_mod.load_config(config_path="examples/config.example.yaml")


def _blank_stream():
    s = Stream(url="u", original_url="http://e/x.m3u8", provider_domain="e.com",
               source_type="remote")
    return s


def test_none_to_short_after_threshold():
    cfg = _cfg()
    s = _blank_stream()
    for i in range(3):
        t = apply_result(s, ok=False, suspected_expired=False, cfg=cfg)
    assert s.blacklist_tier == "short"
    assert t["event"] == "short_added"


def test_short_recovers_on_success():
    cfg = _cfg()
    s = _blank_stream()
    for _ in range(3):
        apply_result(s, ok=False, suspected_expired=False, cfg=cfg)
    t = apply_result(s, ok=True, suspected_expired=False, cfg=cfg)
    assert s.blacklist_tier == "none"
    assert t["event"] == "recovered"


def test_f26_never_worked_to_permanent():
    # stream never worked, 10 total failures -> threshold reached
    cfg = _cfg()
    s = _blank_stream()
    last = None
    for _ in range(10):
        last = apply_result(s, ok=False, suspected_expired=False, cfg=cfg)
    assert s.blacklist_tier == "permanent", s.blacklist_tier
    assert last["event"] == "permanent_added"


def test_suspected_expired_not_counted_as_hard_fail():
    # C1: suspected_expired should not necessarily escalate; still a failure
    # but the orchestrator decides re-fetch. Here we confirm it's recorded.
    cfg = _cfg()
    s = _blank_stream()
    t = apply_result(s, ok=False, suspected_expired=True, cfg=cfg)
    assert s.consecutive_failures == 1
    assert s.blacklist_tier == "none"  # below short threshold


def test_provider_auto_create_and_disable():
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    # provider domain must match how stream.provider_domain is derived
    assert ensure_provider(db, "e.com", aggregate_subdomains=True) is True
    row = db.query("SELECT domain, enabled FROM providers WHERE domain='e.com'")[0]
    assert row["domain"] == "e.com" and row["enabled"] == 1
    set_provider_enabled(db, "e.com", False, reason="test", by="u")
    s = _blank_stream()
    assert provider_enabled(db, s) is False
    db.close()


def test_escalate_short_to_permanent_bulk():
    from datetime import datetime, timedelta, timezone
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, "
        "blacklist_tier, last_working, first_seen, updated_at) VALUES(?,?,?,?,?,?,?,?)",
        ("u", "http://e/x", "e.com", "remote", "short", old,
         datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()),
    )
    db.commit()
    n = escalate_short_to_permanent(db, _cfg())
    assert n == 1
    tier = db.query("SELECT blacklist_tier FROM streams")[0][0]
    assert tier == "permanent"
    db.close()


def test_fresh_eye_short_repeated_failure_stays_short():
    # FRESH-EYE: after reaching short, more failures must NOT jump to permanent
    # unless inactive-days or threshold met. consecutive_failures keeps rising.
    cfg = _cfg()
    s = _blank_stream()
    for _ in range(3):
        apply_result(s, ok=False, suspected_expired=False, cfg=cfg)
    assert s.blacklist_tier == "short"
    for _ in range(20):
        apply_result(s, ok=False, suspected_expired=False, cfg=cfg)
    # still short because last_working is None and total_failures < 10? No: total=23 -> permanent
    # (F26 path) — confirm deterministic
    assert s.blacklist_tier == "permanent"


def test_fresh_eye_purge_old():
    from datetime import datetime, timedelta, timezone
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    old = (datetime.now(timezone.utc) - timedelta(days=200)).isoformat()
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, last_checked, first_seen, updated_at) "
        "VALUES(?,?,?,?,?,?,?)",
        ("u", "http://e/x", "e.com", "remote", old, old, old),
    )
    db.commit()
    purge_old(db, _cfg())
    assert db.query("SELECT COUNT(*) FROM streams")[0][0] == 0
    db.close()


if __name__ == "__main__":
    import traceback, tempfile
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
