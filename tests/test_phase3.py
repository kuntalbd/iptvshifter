"""Phase 3 tests: validator with injected fake HTTP client (no network)."""
import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.validator import StreamValidator, Result
from m3u_processor.parser import PlaylistParser
from m3u_processor.database import Database
from m3u_processor import config as cfg_mod

# --- fake HTTP client -------------------------------------------------------
class FakeResp:
    def __init__(self, status, ct="application/vnd.apple.mpegurl"):
        self.status_code = status
        self.headers = {"Content-Type": ct}

class FakeClient:
    """Configurable responder. `rules`: dict url-substr -> (status, ct)."""
    def __init__(self, rules=None, calls=None):
        self.rules = rules or {}
        self.calls = calls if calls is not None else []
        self.head_calls = 0

    def __call__(self, method, url, **kw):
        self.calls.append((method, url))
        if method == "HEAD":
            self.head_calls += 1
        for sub, (st, ct) in self.rules.items():
            if sub in url:
                return FakeResp(st, ct)
        return FakeResp(404)


def _cfg():
    return cfg_mod.load_config(config_path="examples/config.example.yaml")


def _mk_stream(text):
    return PlaylistParser().parse_text(text)[0]


def test_validate_success_200():
    c = FakeClient({"/ok/": (200, "application/vnd.apple.mpegurl")})
    v = StreamValidator(_cfg(), http_client=c)
    s = _mk_stream("#EXTINF:-1,A\nhttp://e/ok/master.m3u8\n")
    r = v.validate_one(s)
    assert r.ok and r.status == 200


def test_validate_206_segment_ok():
    c = FakeClient({"/seg/": (206, "video/mp2t")})
    v = StreamValidator(_cfg(), http_client=c)
    s = _mk_stream("#EXTINF:-1,S\nhttp://e/seg/1.ts\n")
    r = v.validate_one(s)
    assert r.ok


def test_validate_bad_content_type():
    # text/html (error/login page) must be rejected even on HTTP 200
    c = FakeClient({"/bad/": (200, "text/html")})
    v = StreamValidator(_cfg(), http_client=c)
    s = _mk_stream("#EXTINF:-1,B\nhttp://e/bad/x.m3u8\n")
    r = v.validate_one(s)
    assert not r.ok
    assert r.reason == "http_200"


def test_validate_text_plain_accepted():
    # Many IPTV servers return text/plain but serve a working stream -> accept
    c = FakeClient({"/ok/": (200, "text/plain")})
    v = StreamValidator(_cfg(), http_client=c)
    s = _mk_stream("#EXTINF:-1,P\nhttp://e/ok/x.m3u8\n")
    r = v.validate_one(s)
    assert r.ok, r.reason


def test_non_http_uncheckable():
    v = StreamValidator(_cfg(), http_client=FakeClient())
    s = _mk_stream("#EXTINF:-1,R\nrtmp://e/live/stream\n")
    r = v.validate_one(s)
    assert r.uncheckable and not r.ok


def test_tokened_403_suspected_expired():
    # C1: tokened URL 403 -> suspected_expired, not plain failure
    c = FakeClient({"/tok/": (403, "text/html")})
    v = StreamValidator(_cfg(), http_client=c)
    s = _mk_stream("#EXTINF:-1,T\nhttp://e/tok/x.m3u8?md5=Z&expires=1\n")
    r = v.validate_one(s)
    assert r.suspected_expired and not r.ok
    assert r.reason == "http_403_tokened"


def test_retry_backoff_on_timeout():
    # FRESH-EYE: a URL that fails twice then 200 -> retries, succeeds
    class Flaky:
        def __init__(self): self.n = 0
        def __call__(self, method, url, **kw):
            self.n += 1
            if self.n < 3:
                raise TimeoutError("boom")
            return FakeResp(200, "application/vnd.apple.mpegurl")
    c = Flaky()
    v = StreamValidator(_cfg(), http_client=c, retries=2, backoff=[0, 0, 0])
    s = _mk_stream("#EXTINF:-1,F\nhttp://e/flaky/x.m3u8\n")
    r = v.validate_one(s)
    assert r.ok and c.n >= 3


def test_embedded_header_used():
    # F1: validator must send embedded UA/Referer
    seen = []
    def cap(method, url, **kw):
        seen.append(kw.get("headers", {}))
        return FakeResp(200, "application/vnd.apple.mpegurl")
    v = StreamValidator(_cfg(), http_client=cap)
    s = _mk_stream(
        '#EXTINF:-1,C\n#EXTVLCOPT:http-user-agent=MYUA\n#EXTVLCOPT:http-referrer=https://r/\nhttp://e/x.m3u8\n'
    )
    v.validate_one(s)
    h = seen[0]
    assert h.get("User-Agent") == "MYUA", h
    assert h.get("Referer") == "https://r/", h


def test_fresh_eye_verify_ssl_passed_to_client():
    # FRESH-EYE: verify_ssl config must reach the transport call
    captured = {}
    def cap(method, url, **kw):
        captured.update(kw)
        return FakeResp(200, "application/vnd.apple.mpegurl")
    v = StreamValidator(_cfg(), http_client=cap)
    s = _mk_stream("#EXTINF:-1,C\nhttp://e/x.m3u8\n")
    v.validate_one(s)
    assert "verify" in captured


def test_validate_batch_concurrent():
    c = FakeClient({"/ok/": (200, "application/vnd.apple.mpegurl")})
    v = StreamValidator(_cfg(), http_client=c, workers=4)
    streams = [_mk_stream(f"#EXTINF:-1,A{i}\nhttp://e/ok/s{i}.m3u8\n") for i in range(8)]
    res = v.validate_batch(streams)
    assert len(res) == 8
    assert all(r.ok for _, r in res)


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
