"""Phase 5 tests: orchestrator end-to-end (offline, fake HTTP)."""
import os
import sys
import json
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.orchestrator import Orchestrator
from m3u_processor.database import Database
from m3u_processor import config as cfg_mod
from m3u_processor.providers import set_provider_enabled

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "requirement", "samples", "downloads")

# Fake client: a small playlist file we create locally for token-refresh test
FAKE_M3U = """#EXTM3U
#EXTINF:-1,Working
http://example.com/ok/master.m3u8
#EXTINF:-1,Dead
http://example.com/dead/x.m3u8
#EXTINF:-1,RTMP
rtmp://example.com/live/s
"""


class FakeResp:
    def __init__(self, status, ct="application/vnd.apple.mpegurl"):
        self.status_code = status
        self.headers = {"Content-Type": ct}


class RouterClient:
    def __init__(self):
        self.map = {}
    def set(self, substr, status, ct="application/vnd.apple.mpegurl"):
        self.map[substr] = (status, ct)
    def __call__(self, method, url, **kw):
        for sub, (st, ct) in self.map.items():
            if sub in url:
                return FakeResp(st, ct)
        return FakeResp(404)


def _cfg():
    return cfg_mod.load_config(config_path="examples/config.example.yaml")


def test_orchestrator_quick_run_end_to_end():
    tmp = tempfile.mkdtemp()
    db = Database(os.path.join(tmp, "t.db"))
    db.init_db(backup=False)
    # write a local playlist
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)

    c = RouterClient()
    c.set("/ok/", 200)
    c.set("/dead/", 404)
    # rtmp -> uncheckable (no HTTP call)

    orch = Orchestrator(db, _cfg(), http_client=c)
    orch.ingest_source(pl, source_type="local")
    stats = orch.run(mode="quick")

    assert stats["parsed"] == 3
    assert stats["eligible"] == 3
    assert stats["working"] == 1
    assert stats["failed"] == 1
    assert stats["uncheckable"] == 1
    # dead stream -> short blacklist (3 consec fails? no: single run =1 fail)
    dead_tier = db.query("SELECT blacklist_tier FROM streams WHERE url LIKE '%dead%'")[0][0]
    assert dead_tier == "none"  # only 1 failure, below short threshold(3)
    db.close()


def test_orchestrator_full_mode_checks_blacklisted():
    tmp = tempfile.mkdtemp()
    db = Database(os.path.join(tmp, "t.db"))
    db.init_db(backup=False)
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)
    c = RouterClient(); c.set("/ok/", 200); c.set("/dead/", 404)
    orch = Orchestrator(db, _cfg(), http_client=c)
    orch.ingest_source(pl)
    # first quick run
    orch.run(mode="quick")
    # make dead permanently blacklisted by repeated failures
    for _ in range(10):
        orch.run(mode="full")
    dead = db.query("SELECT blacklist_tier, total_failures FROM streams WHERE url LIKE '%dead%'")[0]
    assert dead["blacklist_tier"] == "permanent", dead
    db.close()


def test_stale_token_refreshed_only_in_refresh_mode():
    # Option B: token re-extraction happens ONLY in the dedicated refresh mode,
    # not in regular/quick runs. This test verifies a stale token is rotated by a
    # refresh run (which re-reads the source with a fresh token) and is NOT
    # silently hard-failed by a regular run.
    tmp = tempfile.mkdtemp()
    db = Database(os.path.join(tmp, "t.db"))
    db.init_db(backup=False)
    # source file v1: expired token
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write("#EXTM3U\n#EXTINF:-1,T\nhttp://e/tok/x.m3u8?md5=OLD&expires=1\n")
    orch = Orchestrator(db, _cfg(), http_client=None)
    orch.ingest_source(pl)

    # 1) a regular run with a 403 validator must NOT refresh the token
    class Always403:
        def __call__(self, method, url, **kw): return FakeResp(403, "text/html")
    orch.http_client = Always403()
    stats_reg = orch.run(mode="regular")
    tok = db.query("SELECT original_url, is_working FROM streams WHERE url LIKE '%tok%'")[0]
    assert tok["original_url"] == "http://e/tok/x.m3u8?md5=OLD&expires=1"
    assert stats_reg.get("token_refreshed", 0) == 0

    # 2) source file now has a FRESH token (simulate list refreshed)
    with open(pl, "w") as f:
        f.write("#EXTM3U\n#EXTINF:-1,T\nhttp://e/tok/x.m3u8?md5=NEW&expires=9999999999\n")
    # refresh mode re-reads the source and rotates the token
    orch2 = Orchestrator(db, _cfg(), http_client=Always403())
    orch2.run(mode="refresh")
    tok2 = db.query("SELECT original_url FROM streams WHERE url LIKE '%tok%'")[0]
    assert "md5=NEW" in tok2["original_url"], tok2["original_url"]
    db.close()



def test_disabled_provider_streams_skipped():
    tmp = tempfile.mkdtemp()
    db = Database(os.path.join(tmp, "t.db"))
    db.init_db(backup=False)
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write(FAKE_M3U)
    orch = Orchestrator(db, _cfg(), http_client=RouterClient())
    orch.ingest_source(pl)
    # disable provider for example.com (all our streams derive to example.com)
    set_provider_enabled(db, "example.com", False, reason="x", by="t")
    checked_before = db.query("SELECT COUNT(*) FROM runs")[0][0]
    stats = orch.run(mode="quick")
    # no stream checked because provider disabled
    assert stats["checked"] == 0, stats
    db.close()


def test_fresh_eye_run_writes_runs_row():
    tmp = tempfile.mkdtemp()
    db = Database(os.path.join(tmp, "t.db"))
    db.init_db(backup=False)
    pl = os.path.join(tmp, "pl.m3u")
    with open(pl, "w") as f:
        f.write("#EXTM3U\n#EXTINF:-1,A\nhttp://e/ok/m.m3u8\n")
    orch = Orchestrator(db, _cfg(), http_client=RouterClient())
    orch.ingest_source(pl)
    stats = orch.run(mode="quick")
    row = db.query("SELECT status, stats_json FROM runs WHERE run_id=?", (orch.run_id,))[0]
    assert row["status"] in ("completed", "stopped")
    assert json.loads(row["stats_json"])["mode"] == "quick"
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
