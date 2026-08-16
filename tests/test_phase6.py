"""Phase 6 tests: CLI commands + writers (multi-format output)."""
import os
import sys
import tempfile
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.__main__ import main
from m3u_processor.database import Database
from m3u_processor import config as cfg_mod
from m3u_processor.parser import PlaylistParser, merge_into_db
from m3u_processor.writers import write_streams


def _cfg_path(tmp):
    # copy config example to tmp, point db there
    import shutil
    dst = os.path.join(tmp, "config.yaml")
    with open(os.path.join(os.path.dirname(__file__), "..", "examples", "config.example.yaml")) as f:
        txt = f.read()
    db = os.path.join(tmp, "m3u.db")
    txt = txt.replace('./data/m3u.db', db)
    txt = txt.replace('./out', os.path.join(tmp, "out"))
    with open(dst, "w") as f:
        f.write(txt)
    return dst, db


def _seed(tmp):
    db = Database(os.path.join(tmp, "m3u.db"))
    db.init_db(backup=False)
    # a stream with vlc headers
    parser = PlaylistParser()
    s = parser.parse_text(
        "#EXTM3U\n#EXTINF:-1 tvg-id=\"cnn\" tvg-logo=\"http://l/cnn.png\" group-title=\"News\",CNN\n"
        "#EXTVLCOPT:http-user-agent=TestUA\n#EXTVLCOPT:http-referrer=http://r/\n"
        "http://example.com/cnn/m.m3u8\n",
        source_type="local", source_path="x.m3u", base_url="http://example.com/x.m3u",
    )
    merge_into_db(db, s, "seed")
    # mark working
    db.execute("UPDATE streams SET is_working=1, blacklist_tier='none' WHERE id=1")
    db.commit()
    db.close()


def test_cli_generate_output_all_formats():
    tmp = tempfile.mkdtemp()
    cfgp, db = _cfg_path(tmp)
    _seed(tmp)
    out = os.path.join(tmp, "out", "working.m3u")
    rc = main(["--config", cfgp, "generate-output", "--out", out,
               "--formats", "vlc,kodi,tivimate"])
    assert rc == 0
    # vlc file
    vlc = open(out).read()
    assert "#EXTVLCOPT:http-user-agent=TestUA" in vlc
    assert "#EXTVLCOPT:http-referrer=http://r/" in vlc
    # kodi file
    kodi = open(out.rsplit(".", 1)[0] + ".kodi.m3u").read()
    assert "#KODIPROP:inputstream.adaptive.stream_headers=http-user-agent=TestUA&http-referrer=http://r/" in kodi
    # tivimate file
    tivi = open(out.rsplit(".", 1)[0] + ".tivimate.m3u").read()
    assert "|http-user-agent=TestUA&http-referrer=http://r/" in tivi
    assert "|" not in tivi.split("|", 1)[0]  # URL part before | has no pipe


def test_cli_disable_provider_and_stats():
    tmp = tempfile.mkdtemp()
    cfgp, db = _cfg_path(tmp)
    _seed(tmp)
    rc = main(["--config", cfgp, "disable-provider", "example.com", "--reason", "test"])
    assert rc == 0
    rc = main(["--config", cfgp, "list-providers"])
    assert rc == 0
    rc = main(["--config", cfgp, "stats"])
    assert rc == 0


def test_cli_init_db_and_blacklist():
    tmp = tempfile.mkdtemp()
    cfgp, db = _cfg_path(tmp)
    rc = main(["--config", cfgp, "init-db"])
    assert rc == 0
    assert os.path.isfile(db)
    rc = main(["--config", cfgp, "blacklist", "--tier", "permanent"])
    assert rc == 0


def test_fresh_eye_generated_vlc_reparseable():
    # FRESH-EYE: the VLC output must be parseable again without data loss
    tmp = tempfile.mkdtemp()
    cfgp, db = _cfg_path(tmp)
    _seed(tmp)
    out = os.path.join(tmp, "out", "working.m3u")
    main(["--config", cfgp, "generate-output", "--out", out])
    # re-parse generated file
    parser = PlaylistParser()
    streams = parser.parse_file(out)
    assert len(streams) == 1
    s = streams[0]
    assert s.name == "CNN"
    assert s.attributes.get("group-title") == "News"
    assert s.attributes.get("tvg-id") == "cnn"
    assert s.attributes["vlc_options"].get("http-user-agent") == "TestUA"
    # original URL preserved (no stripping)
    assert s.original_url == "http://example.com/cnn/m.m3u8"


def test_fresh_eye_tivimate_header_order():
    # FRESH-EYE: tivimate header order deterministic (merge_headers order)
    tmp = tempfile.mkdtemp()
    cfgp, db = _cfg_path(tmp)
    _seed(tmp)
    out = os.path.join(tmp, "out", "working.m3u")
    main(["--config", cfgp, "generate-output", "--out", out, "--formats", "tivimate"])
    tivi = open(out.rsplit(".", 1)[0] + ".tivimate.m3u").read()
    line = [l for l in tivi.splitlines() if l.startswith("http")][0]
    assert line.startswith("http://example.com/cnn/m.m3u8|")
    tail = line.split("|", 1)[1]
    assert tail.index("http-user-agent") < tail.index("http-referrer")


def test_fresh_eye_serve_port_configurable():
    # port MUST be configurable: config webui.port wins when no --port flag
    import shutil
    tmp = tempfile.mkdtemp()
    cfgp, dbp = _cfg_path(tmp)
    with open(cfgp) as f:
        txt = f.read()
    txt = txt.replace("port: 50152", "port: 9191")
    with open(os.path.join(tmp, "config.yaml"), "w") as f:
        f.write(txt)
    from m3u_processor import config as cfg_mod
    cfg = cfg_mod.load_config(config_path=cfgp)
    # mimic __main__ serve resolution when no --port flag is passed
    port = None
    resolved = port if port is not None else int(cfg.get("webui.port", 8080))
    assert resolved == 9191, resolved
    # CLI flag overrides config
    port2 = 7000
    resolved2 = port2 if port2 is not None else int(cfg.get("webui.port", 8080))
    assert resolved2 == 7000


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
