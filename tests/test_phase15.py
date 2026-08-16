"""Tests for the FIXED batched token refresh (replaces per-stream re-read).

Verifies:
- New columns `source` / `is_url` exist and are populated at parse time.
- Refresh groups eligible tokened streams by `source`, reads each source ONCE,
  and updates all matching streams (shared source fetched once, not N times).
- Normalized-url matching works despite token presence differences.
"""
import os
import tempfile

import pytest

from m3u_processor import config as cfg_mod
from m3u_processor.database import Database
from m3u_processor.orchestrator import Orchestrator
from m3u_processor.parser import PlaylistParser, merge_into_db


def _cfg(tmp):
    dbp = os.path.join(tmp, "m3u.db")
    cfgd = dict(cfg_mod.DEFAULTS)
    cfgd["database"]["path"] = dbp
    cp = os.path.join(tmp, "config.yaml")
    import yaml
    yaml.safe_dump(cfgd, open(cp, "w"))
    return cfg_mod.load_config(config_path=cp)


def test_source_columns_populated_at_parse(tmp_path):
    src = tmp_path / "feed.m3u"
    src.write_text(
        "#EXTM3U\n"
        "#EXTINF:-1,ChanA\nhttps://a.com/live/chanA.m3u8?token=OLD1\n"
        "#EXTINF:-1,ChanB\nhttps://b.com/live/chanB.m3u8?token=OLD2\n")
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)
    orch.ingest_source(str(src))
    rows = db.query("SELECT url, source, is_url FROM streams")
    assert len(rows) == 2
    for r in rows:
        assert r["source"] == str(src)        # source = the specific file path
        assert r["is_url"] == 0                # local file -> is_url=0
    db.close()


def test_batched_refresh_reads_source_once(tmp_path):
    # A source file with FRESH tokens; two streams share it and are tokened.
    src = tmp_path / "feed.m3u"
    src.write_text(
        "#EXTM3U\n"
        "#EXTINF:-1,ChanA\nhttps://a.com/live/chanA.m3u8?token=NEW1\n"
        "#EXTINF:-1,ChanB\nhttps://b.com/live/chanB.m3u8?token=NEW2\n")
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)
    # Insert streams manually with OLD tokened original_url + source set.
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, "
        "source_path, source, is_url) VALUES(?,?,?,?,?,?,?)",
        ("https://a.com/live/chanA.m3u8", "https://a.com/live/chanA.m3u8?token=OLD1",
         "a.com", "local", str(src), str(src), 0))
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, "
        "source_path, source, is_url) VALUES(?,?,?,?,?,?,?)",
        ("https://b.com/live/chanB.m3u8", "https://b.com/live/chanB.m3u8?token=OLD2",
         "b.com", "local", str(src), str(src), 0))
    db.commit()

    # Track how many times the source is read by monkeypatching parse_text.
    orig_parse = PlaylistParser.parse_text
    calls = {"n": 0}

    def counting_parse(self, text, **kw):
        calls["n"] += 1
        return orig_parse(self, text, **kw)

    PlaylistParser.parse_text = counting_parse
    try:
        orch._refresh_tokens_batched()
    finally:
        PlaylistParser.parse_text = orig_parse

    # source read exactly ONCE (not twice for two shared streams)
    assert calls["n"] == 1, f"source parsed {calls['n']} times, expected 1"

    # both streams got fresh tokens
    got = {r["url"]: r["original_url"] for r in db.query("SELECT url, original_url FROM streams")}
    assert got["https://a.com/live/chanA.m3u8"] == "https://a.com/live/chanA.m3u8?token=NEW1"
    assert got["https://b.com/live/chanB.m3u8"] == "https://b.com/live/chanB.m3u8?token=NEW2"
    db.close()


def test_norm_url_strips_token_params(tmp_path):
    cfg = _cfg(str(tmp_path))
    orch = Orchestrator.__new__(Orchestrator)
    orch.config = cfg
    # tokened vs tokenless of the SAME resource must normalize equal
    a = "https://x.com/live/chan.m3u8?md5=ABC&expires=123"
    b = "https://x.com/live/chan.m3u8"
    assert orch._norm_url(a) == orch._norm_url(b) == "https://x.com/live/chan.m3u8"
    # non-token param preserved
    c = "https://x.com/live/chan.m3u8?foo=bar&token=ZZZ"
    assert orch._norm_url(c) == "https://x.com/live/chan.m3u8?foo=bar"


def test_batched_refresh_remote_source(tmp_path):
    # Remote source: uses a local file as a stand-in but marks is_url=1.
    src = tmp_path / "remote_feed.m3u"
    src.write_text(
        "#EXTM3U\n#EXTINF:-1,ChanX\nhttps://x.com/live/chanX.m3u8?token=FRESH\n")
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)
    # is_url=1 path; _refresh_tokens_batched reads local file since we can't
    # hit network in test, but the code path for is_url reads via requests.
    # To keep the test offline, we just assert the SQL/eligibility grouping
    # doesn't crash and a local file with is_url=0 updates (covered above).
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, "
        "source_path, source, is_url) VALUES(?,?,?,?,?,?,?)",
        ("https://x.com/live/chanX.m3u8", "https://x.com/live/chanX.m3u8?token=OLD",
         "x.com", "remote", str(src), str(src), 1))
    db.commit()
    # monkeypatch requests.get to serve the local file content
    import requests
    orig_get = requests.get

    class _R:
        text = src.read_text()
        def raise_for_status(self):
            pass
    def fake_get(url, timeout=30):
        return _R()
    requests.get = fake_get
    try:
        n = orch._refresh_tokens_batched()
    finally:
        requests.get = orig_get
    assert n == 1
    row = db.query("SELECT original_url FROM streams WHERE url=?",
                   ("https://x.com/live/chanX.m3u8",))[0]
    assert row["original_url"] == "https://x.com/live/chanX.m3u8?token=FRESH"
    db.close()


def test_quick_mode_does_not_refresh_tokens(tmp_path):
    # After Option B, quick mode must NOT re-extract tokens in-run.
    src = tmp_path / "feed.m3u"
    src.write_text(
        "#EXTM3U\n#EXTINF:-1,A\nhttps://a.com/live/A.m3u8?token=NEW\n")
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)
    # stream with OLD token, source = local file (which has NEW token)
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, "
        "source_path, source, is_url) VALUES(?,?,?,?,?,?,?)",
        ("https://a.com/live/A.m3u8", "https://a.com/live/A.m3u8?token=OLD",
         "a.com", "local", str(src), str(src), 0))
    db.commit(); db.close()
    orch2 = Orchestrator(Database(cfg.get("database.path")), cfg)
    stats = orch2.run(mode="quick")   # quick mode
    # no token_refreshed in quick mode
    assert stats.get("token_refreshed", 0) == 0
    db3 = Database(cfg.get("database.path"))
    row = db3.query("SELECT original_url FROM streams WHERE url=?",
                    ("https://a.com/live/A.m3u8",))[0]
    # token must remain OLD (quick mode never refreshed it)
    assert row["original_url"].endswith("token=OLD")
    db3.close()


def test_error_logged_on_source_fetch_failure(tmp_path):
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)
    # eligible tokened stream whose source is a non-existent remote URL
    db.execute(
        "INSERT INTO streams(url, original_url, provider_domain, source_type, "
        "source_path, source, is_url) VALUES(?,?,?,?,?,?,?)",
        ("https://a.com/live/A.m3u8", "https://a.com/live/A.m3u8?token=OLD",
         "a.com", "remote", "http://127.0.0.1:9/nope.m3u", "http://127.0.0.1:9/nope.m3u", 1))
    db.commit()
    # monkeypatch requests.get to always fail
    import requests
    orig = requests.get
    def boom(url, timeout=30):
        raise RuntimeError("connection refused")
    requests.get = boom
    try:
        n = orch._refresh_tokens_batched()
    finally:
        requests.get = orig
    assert n == 0
    errs = db.get_run_errors(run_id=orch.run_id)
    assert len(errs) >= 1
    assert errs[0]["error_type"] == "source_fetch_failed"
    db.close()


def test_refresh_mode_fatal_error_is_logged(tmp_path):
    # A fatal error inside refresh mode must be logged (run_errors) AND recorded
    # on the runs row, so the Web UI surfaces it instead of leaving a zombie run.
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)
    # force _refresh_tokens_batched to raise
    def boom():
        raise RuntimeError("db exploded")
    orch._refresh_tokens_batched = boom
    try:
        orch.run(mode="refresh")
    except RuntimeError:
        pass
    # the error should be captured even though run() re-raised
    errs = db.get_run_errors(run_id=orch.run_id)
    assert any(e["error_type"] == "fatal_run_error" for e in errs), errs
    run_row = db.query("SELECT status, error_message FROM runs WHERE run_id=?",
                       (orch.run_id,))[0]
    assert run_row["status"] == "error"
    assert run_row["error_message"]
    db.close()


def test_log_error_capped_per_run(tmp_path):
    # A mass-failure run must not flood run_errors (cap per run_id).
    db = Database(str(tmp_path / "m.db"))
    db.init_db(backup=False)
    for _ in range(500):
        db.log_error("runX", "source_fetch_failed", message="boom", source="s")
    errs = db.get_run_errors(run_id="runX", limit=1000)
    assert len(errs) == db.MAX_ERRORS_PER_RUN, len(errs)
    db.close()

