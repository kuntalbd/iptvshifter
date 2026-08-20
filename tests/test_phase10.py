"""Phase 10 tests: quality / health checking (latency A + throughput B)."""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor import validator, config as cfg_mod
from m3u_processor.database import Database
from m3u_processor.writers import write_streams


def _cfg(quality):
    c = cfg_mod.DEFAULTS.copy()
    c["quality"] = quality
    return cfg_mod.Config(_data=c)


def _stream(url):
    class S:
        def __init__(self, u):
            self.url = u
            self.original_url = u
            self.attributes = {}
    return S(url)


def test_latency_tiers_from_config():
    # healthy_max_ms=2000, medium_max_ms=5000 (defaults)
    c = _cfg({"latency_check": True, "healthy_max_ms": 2000, "medium_max_ms": 5000,
              "throughput_check": False})
    v = validator.StreamValidator(c)
    # fake client: returns ok with controlled elapsed via monkeypatch
    import types
    res = validator.Result()
    res.ok = True
    res.status = 200
    # directly exercise _measure_health with synthetic elapsed
    res.elapsed_ms = 1000
    v._measure_health(res, None, "http://e/x.m3u8", {})
    assert res.health_tier == "healthy", res.health_tier
    r2 = validator.Result(); r2.ok = True; r2.status = 200; r2.elapsed_ms = 3000
    v._measure_health(r2, None, "http://e/x.m3u8", {})
    assert r2.health_tier == "medium"
    r3 = validator.Result(); r3.ok = True; r3.status = 200; r3.elapsed_ms = 8000
    v._measure_health(r3, None, "http://e/x.m3u8", {})
    assert r3.health_tier == "slow"


def test_throughput_threshold():
    # throughput_check on, but client injected -> B sampling skipped (guard:
    # only samples with real transport). Tier should be "unknown", score 50.
    c = _cfg({"latency_check": False, "throughput_check": True,
              "throughput_min_kbps": 500})
    v = validator.StreamValidator(c, http_client=lambda *a, **k: None)
    res = validator.Result(); res.ok = True; res.status = 200; res.elapsed_ms = 100
    v._measure_health(res, None, "http://e/x.m3u8", {})
    # with fake client, B is skipped -> tier unknown, score 50 (not None)
    assert res.health_tier == "unknown"
    assert res.health_score == 50.0


def test_both_disabled_bypasses():
    c = _cfg({"latency_check": False, "throughput_check": False})
    v = validator.StreamValidator(c)
    res = validator.Result(); res.ok = True; res.status = 200; res.elapsed_ms = 8000
    v._measure_health(res, None, "http://e/x.m3u8", {})
    assert res.health_tier is None
    assert res.health_score is None


def test_mark_in_group_title():
    rows = [
        {"url": "u1", "original_url": "http://e/1.m3u8", "attributes": "{}",
         "name": "Fast", "provider_domain": "e", "health_tier": "healthy"},
        {"url": "u2", "original_url": "http://e/2.m3u8", "attributes": "{}",
         "name": "Slow", "provider_domain": "e", "health_tier": "slow"},
    ]
    q = {"mark_in_group_title": True}
    res = write_streams(rows, "/tmp/_qtest.m3u", formats=["vlc"], quality_cfg=q)
    txt = open(res["vlc"]).read()
    assert "⭐" in txt and "🐢" in txt, txt


def test_separate_healthy_file():
    rows = [
        {"url": "u1", "original_url": "http://e/1.m3u8", "attributes": "{}",
         "name": "Fast", "provider_domain": "e", "health_tier": "healthy"},
        {"url": "u2", "original_url": "http://e/2.m3u8", "attributes": "{}",
         "name": "Slow", "provider_domain": "e", "health_tier": "slow"},
    ]
    q = {"separate_healthy_file": True}
    res = write_streams(rows, "/tmp/_qtest2.m3u", formats=["vlc"], quality_cfg=q)
    htxt = open(res["vlc_healthy"]).read()
    assert "Fast" in htxt and "Slow" not in htxt, htxt


def test_db_migration_health_columns():
    import tempfile
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    cols = {r[1] for r in db.query("PRAGMA table_info(streams)")}
    assert "health_score" in cols and "health_tier" in cols
    db.close()


def test_fresh_eye_combine_tier_worst_wins():
    # static combine: latency healthy + throughput slow => slow (worst dominates)
    assert validator.StreamValidator._combine_tier("healthy", "slow") == "slow"
    assert validator.StreamValidator._combine_tier("slow", "healthy") == "slow"
    assert validator.StreamValidator._combine_tier("medium", "healthy") == "medium"
    assert validator.StreamValidator._combine_tier("healthy", "healthy") == "healthy"


def test_manifest_url_rated_by_latency_only():
    # .m3u8 manifest must NOT be penalized by throughput sampling -> latency only.
    c = _cfg({"latency_check": True, "healthy_max_ms": 2000, "medium_max_ms": 5000,
              "throughput_check": True, "throughput_sample_seconds": 3,
              "throughput_min_kbps": 500})
    v = validator.StreamValidator(c)
    res = validator.Result(); res.ok = True; res.status = 200; res.elapsed_ms = 300
    # fake client so real transport (throughput path) is bypassed anyway, but the
    # key assertion is that a manifest URL is NEVER throughput-rated.
    v._client = lambda *a, **k: None
    resp = type("R", (), {"headers": {"Content-Type": "application/vnd.apple.mpegurl"}})()
    v._measure_health(res, resp, "http://e/x.m3u8", {})
    assert res.health_tier == "healthy", (res.health_tier, res.throughput_kbps)
    assert res.throughput_kbps is None  # no sampling on manifest


def test_manifest_detected_from_url_extension():
    c = _cfg({"latency_check": True, "throughput_check": True})
    v = validator.StreamValidator(c)
    res = validator.Result(); res.ok = True; res.status = 200; res.elapsed_ms = 300
    v._client = lambda *a, **k: None
    resp = type("R", (), {"headers": {}})()
    # .m3u8 with query string (common for tokened manifests)
    v._measure_health(res, resp, "http://e/x.m3u8?token=abc&expires=123", {})
    assert res.health_tier == "healthy"
    assert res.throughput_kbps is None


def _fake_resp(content_type="video/mp2t"):
    return type(
        "R", (),
        {"status_code": 200, "headers": {"Content-Type": content_type},
         "close": lambda self: None})()


def test_combined_health_media_weighted():
    # Regression (review PASS1): regular/full mode on a NON-manifest media URL
    # must combine latency + sampled throughput (weighted 40/60, worst tier
    # wins). It used to call the nonexistent _sample_throughput() and crash
    # with AttributeError inside validate_one -> reason="AttributeError" and
    # health_score/health_tier always None for every media stream.
    c = _cfg({"latency_check": True, "healthy_max_ms": 2000, "medium_max_ms": 5000,
              "throughput_check": True, "throughput_sample_seconds": 1,
              "throughput_min_kbps": 500})
    v = validator.StreamValidator(c)
    v._sample_throughput_raw = lambda url, headers: (None, 2000.0)  # >= min -> healthy
    v._do_request = lambda method, url, headers: _fake_resp()
    res = v.validate_one(_stream("http://e/media.ts?token=abc"), health=True)
    assert res.ok, res.reason
    assert res.reason == "", res.reason  # no AttributeError leaked into reason
    assert res.throughput_kbps == 2000.0
    assert res.health_tier == "healthy", res.health_tier
    assert res.health_score is not None and res.health_score > 50


def test_combined_health_slow_throughput_wins():
    # Fast latency + slow throughput -> combined tier must be "slow" (worst
    # dominates) and the weighted score must still be computed, not None.
    c = _cfg({"latency_check": True, "healthy_max_ms": 2000, "medium_max_ms": 5000,
              "throughput_check": True, "throughput_sample_seconds": 1,
              "throughput_min_kbps": 500})
    v = validator.StreamValidator(c)
    v._sample_throughput_raw = lambda url, headers: (None, 100.0)  # < min -> slow
    v._do_request = lambda method, url, headers: _fake_resp()
    res = v.validate_one(_stream("http://e/media.ts"), health=True)
    assert res.ok, res.reason
    assert res.throughput_kbps == 100.0
    assert res.health_tier == "slow", res.health_tier
    assert res.health_score is not None


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
