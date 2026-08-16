"""Phase 7 tests: FastAPI web UI (TestClient, no network/browser)."""
import os
import sys
import tempfile
import json
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.webui.app import create_app
from m3u_processor.database import Database
from m3u_processor import config as cfg_mod
from m3u_processor.parser import PlaylistParser, merge_into_db
from m3u_processor.providers import set_provider_enabled


def _build(tmp):
    cfgp = os.path.join(tmp, "config.yaml")
    with open(os.path.join(os.path.dirname(__file__), "..", "examples", "config.example.yaml")) as f:
        txt = f.read()
    dbp = os.path.join(tmp, "m3u.db")
    txt = txt.replace("./data/m3u.db", dbp)
    txt = txt.replace("./out", os.path.join(tmp, "out"))
    with open(cfgp, "w") as f:
        f.write(txt)
    cfg = cfg_mod.load_config(config_path=cfgp)
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    parser = PlaylistParser()
    s = parser.parse_text(
        "#EXTM3U\n#EXTINF:-1 tvg-id=\"cnn\" group-title=\"News\",CNN\n"
        "#EXTVLCOPT:http-user-agent=UA\nhttp://example.com/cnn/m.m3u8\n",
        source_type="local", source_path="x.m3u", base_url="http://example.com/x.m3u")
    merge_into_db(db, s, "seed")
    from m3u_processor.providers import ensure_provider
    ensure_provider(db, "example.com", True)
    db.execute("UPDATE streams SET is_working=1, blacklist_tier='none' WHERE id=1")
    db.commit()
    db.close()
    return cfg


def test_pages_render():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    for path in ("/", "/streams", "/providers", "/blacklist", "/run", "/settings"):
        r = c.get(path)
        assert r.status_code == 200, (path, r.status_code)


def test_api_streams_and_providers():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    s = c.get("/api/streams").json()
    assert len(s) == 1 and s[0]["name"] == "CNN"
    p = c.get("/api/providers").json()
    assert any(x["domain"] == "example.com" for x in p)
    st = c.get("/api/health-stats").json()
    assert "healthy" in st and "last_refresh_at" in st  # endpoint present
    # note: /api/stats removed in favor of /api/health-stats


def test_api_provider_disable_enable():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    r = c.post("/api/provider/disable", json={"domain": "example.com", "reason": "t"})
    assert r.json()["ok"]
    p = c.get("/api/providers").json()
    assert [x for x in p if x["domain"] == "example.com"][0]["enabled"] == 0
    r = c.post("/api/provider/enable", json={"domain": "example.com"})
    assert r.json()["ok"]
    p = c.get("/api/providers").json()
    assert [x for x in p if x["domain"] == "example.com"][0]["enabled"] == 1


def test_api_generate_writes_files():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    r = c.post("/api/generate", json={"formats": ["vlc", "kodi", "tivimate"]})
    j = r.json()
    assert j["ok"]
    assert os.path.isfile(j["files"]["vlc"])
    assert os.path.isfile(j["files"]["kodi"])
    assert os.path.isfile(j["files"]["tivimate"])


def test_fresh_eye_unknown_run_id_404():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    r = c.get("/api/events?run_id=does-not-exist")
    assert r.status_code == 404


def test_fresh_eye_generate_output_includes_health():
    # F38: CLI generate-output must SELECT health_tier so separate_healthy_file
    # and mark_in_group_title actually work (not produce empty healthy file).
    tmp = tempfile.mkdtemp()
    cfgp = os.path.join(tmp, "c.yaml")
    data = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "..", "examples", "config.example.yaml")))
    data["database"]["path"] = os.path.join(tmp, "m3u.db")
    data["output"]["dir"] = os.path.join(tmp, "out")
    data["quality"]["separate_healthy_file"] = True
    data["quality"]["mark_in_group_title"] = True
    yaml.safe_dump(data, open(cfgp, "w"))
    cfg = cfg_mod.Config(_data=data)
    db = Database(cfg.get("database.path")); db.init_db(backup=False)
    p = PlaylistParser()
    s = p.parse_text("#EXTM3U\n#EXTINF:-1 group-title=\"News\",CNN\nhttp://e/cnn.m3u8\n", source_type="local", source_path="x.m3u")
    merge_into_db(db, s, "seed")
    db.execute("UPDATE streams SET is_working=1, blacklist_tier='none', health_tier='healthy', health_score=95 WHERE id=1")
    db.commit(); db.close()
    out = os.path.join(tmp, "out", "working.m3u")
    import subprocess
    rc = subprocess.run([sys.executable, "-m", "m3u_processor", "--config", cfgp,
                        "generate-output", "--formats", "vlc"],
                       capture_output=True, text=True).returncode
    assert rc == 0, rc
    healthy = os.path.join(tmp, "out", "working.healthy.m3u")
    assert os.path.isfile(healthy)
    content = open(healthy).read()
    assert "http://e/cnn.m3u8" in content, "healthy file must contain the healthy stream"
    assert "⭐" in content, "mark_in_group_title should prefix ⭐"
    db.close()


def test_fresh_eye_run_api_returns_run_id():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    # run in quick mode against local data (no network needed since stream is http but untested -> uncheckable path)
    r = c.post("/api/run", json={"mode": "quick"})
    assert "run_id" in r.json()
    # the background thread will process; just confirm it started
    assert r.status_code == 200


def test_api_streams_returns_health_fields():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    # seed a health tier
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    db.execute("UPDATE streams SET health_tier='healthy', health_score=95.0 WHERE id=1")
    db.commit(); db.close()
    s = c.get("/api/streams").json()
    assert s[0]["health_tier"] == "healthy"
    assert s[0]["health_score"] == 95.0
    # health filter
    s2 = c.get("/api/streams?health=slow").json()
    assert s2 == []


def test_api_run_with_job_resolves_mode():
    tmp = tempfile.mkdtemp()
    cfg = _build(tmp)
    app = create_app(cfg)
    from fastapi.testclient import TestClient
    c = TestClient(app)
    # config example has jobs incl. token-refresh (refresh mode)
    r = c.post("/api/run", json={"job": "token-refresh"})
    j = r.json()
    assert "run_id" in j
    # refresh mode on a 1-stream DB: eligible likely 0 (CNN has no token), ok
    assert r.status_code == 200


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
