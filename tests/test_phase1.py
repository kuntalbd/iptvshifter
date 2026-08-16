"""Phase 1 smoke tests: config loading, DB init, utils normalization.

Run:  PYTHONPATH=src python3 -m pytest tests/ -q
(or)   PYTHONPATH=src python3 tests/test_phase1.py
"""
import os
import sys
import sqlite3
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor import config as cfg_mod
from m3u_processor import utils as u
from m3u_processor.database import Database


def test_config_defaults():
    c = cfg_mod.load_config(config_path="examples/config.example.yaml")
    assert c.get("validation.workers") == 20
    assert c.get("blacklist.short_threshold") == 3
    assert c.get("output.formats") == ["vlc", "kodi", "tivimate"]


def test_config_env_override():
    c = cfg_mod.load_config(
        config_path="examples/config.example.yaml", env={"M3U_WORKERS": "42"}
    )
    assert c.get("validation.workers") == 42


def test_config_cli_override_beats_env():
    c = cfg_mod.load_config(
        config_path="examples/config.example.yaml",
        env={"M3U_WORKERS": "42"},
        cli_overrides={"validation.workers": 99},
    )
    assert c.get("validation.workers") == 99


def test_normalize_strips_rotating_tokens():
    url = "https://host/x/master.m3u8?md5=ABC&expires=1786729816&token=xyz"
    assert u.normalize_url(url) == "https://host/x/master.m3u8"


def test_normalize_drops_watermark_tails():
    assert u.normalize_url("https://h/x.m3u8?Live-TV%E2%84%A2") == "https://h/x.m3u8"
    assert (
        u.normalize_url("https://feeds.intoday.in/a/master.m3u8?@Shamim")
        == "https://feeds.intoday.in/a/master.m3u8"
    )


def test_normalize_keeps_static_fingerprint():
    url = "https://h/x.m3u8?deviceId=channel&appVersion=1.2"
    n = u.normalize_url(url)
    assert "deviceid=channel" in n.lower()
    assert "appversion=1.2" in n.lower()


def test_normalize_case_insensitive_token():
    # capital Expires (seen in Mrgify-BDIX) must also be stripped
    assert (
        u.normalize_url("https://h/x.m3u8?Expires=123&KeyName=k")
        == "https://h/x.m3u8?KeyName=k"
    )


def test_is_tokened():
    assert u.is_tokened("https://h/x.m3u8?md5=Z&expires=1") is True
    assert u.is_tokened("https://h/x.m3u8?deviceId=chan") is False


def test_extract_domain():
    assert u.extract_domain("https://cdn.example.com/x", True) == "example.com"
    assert u.extract_domain("https://cdn.example.com/x", False) == "cdn.example.com"
    assert u.extract_domain("https://example.co.uk/x", True) == "example.co.uk"


def test_header_merge_and_writers():
    h = u.merge_headers(
        {"http-user-agent": "UA1", "http-referrer": "R1"}, {"User-Agent": "UA2"}
    )
    assert h["User-Agent"] == "UA2"  # later overrides
    assert h["Referer"] == "R1"
    vlc = u.headers_to_vlc(h)
    assert "#EXTVLCOPT:http-user-agent=UA2" in vlc
    assert "#EXTVLCOPT:http-referrer=R1" in vlc
    assert u.headers_to_kodi(h).startswith(
        "#KODIPROP:inputstream.adaptive.stream_headers=http-user-agent=UA2&http-referrer=R1"
    )
    assert u.headers_to_pipe(h) == "http-user-agent=UA2&http-referrer=R1"


def test_split_pipe_url():
    base, hdr = u.split_pipe_url(
        "https://h/x.m3u8|User-Agent=Mozilla/5.0&Referer=https://y/"
    )
    assert base == "https://h/x.m3u8"
    assert hdr == "User-Agent=Mozilla/5.0&Referer=https://y/"


def test_db_init_and_pragmas(tmp_path):
    db_path = os.path.join(tmp_path, "t.db")
    db = Database(db_path)
    db.init_db(backup=False)
    with sqlite3.connect(db_path) as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"streams", "providers", "runs", "blacklist_events",
                "enable_events", "config"} <= tables
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    db.close()


def test_db_backup_gzip(tmp_path):
    db_path = os.path.join(tmp_path, "t.db")
    db = Database(db_path)
    db.init_db(backup=False)
    out = db.backup_db(output=os.path.join(tmp_path, "b.gz"))
    assert os.path.exists(out)
    db.close()


def test_fresh_eye_relative_paths_anchor_to_config_dir():
    # Option A: when config.yaml uses relative paths, they resolve against the
    # config file's directory (not CWD) so the whole project can be MOVED by
    # relocating the folder. Simulates the /play -> /bd/soft move.
    import tempfile, yaml
    d = tempfile.mkdtemp()
    cfgdir = os.path.join(d, "some", "moved", "location")
    os.makedirs(cfgdir)
    out_dir = os.path.join(cfgdir, "output")
    os.makedirs(out_dir)
    data = dict(cfg_mod.DEFAULTS)
    data["database"]["path"] = "./data/m3u.db"
    data["output"]["dir"] = "./output"
    data["sources"]["feed_file"] = "./feeds.txt"
    yaml.safe_dump(data, open(os.path.join(cfgdir, "config.yaml"), "w"))
    old = os.getcwd()
    os.chdir(tempfile.mkdtemp())  # load from a DIFFERENT cwd
    try:
        c = cfg_mod.load_config(config_path=os.path.join(cfgdir, "config.yaml"))
        assert c.config_dir == cfgdir, c.config_dir
        # resolved to the moved folder, NOT cwd
        assert c.get("database.path") == os.path.join(cfgdir, "data", "m3u.db"), c.get("database.path")
        assert c.get("output.dir") == out_dir, c.get("output.dir")
        assert c.get("sources.feed_file") == os.path.join(cfgdir, "feeds.txt")
        assert c.resolve_path("database.path") == os.path.join(cfgdir, "data", "m3u.db")
    finally:
        os.chdir(old)


def test_fresh_eye_absolute_paths_pass_through():
    # Absolute paths in config are left untouched (no anchoring).
    import tempfile, yaml
    d = tempfile.mkdtemp()
    abs_db = os.path.join(d, "abs.db")
    data = dict(cfg_mod.DEFAULTS)
    data["database"]["path"] = abs_db
    yaml.safe_dump(data, open(os.path.join(d, "config.yaml"), "w"))
    c = cfg_mod.load_config(config_path=os.path.join(d, "config.yaml"))
    assert c.get("database.path") == abs_db, c.get("database.path")


if __name__ == "__main__":
    # Minimal runner (no pytest dependency needed for Phase 1 smoke).
    import traceback
    import tempfile
    _tmp = tempfile.TemporaryDirectory()
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            code = getattr(t, "__code__", None)
            uses_tmp = "tmp_path" in (code.co_varnames if code else ())
            if uses_tmp:
                t(_tmp.name)
            else:
                t()
            print(f"PASS {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"FAIL {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
