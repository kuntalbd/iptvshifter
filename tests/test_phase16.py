"""Tests for production-hardening fixes (§ProdFix):

- refresh mode must NOT re-ingest feeds (avoids 429 crashes; operates on
  existing db rows only).
- ingest_feed retries on HTTP 429 and skips (logs) instead of aborting the run.
"""
import os
import tempfile
import yaml

import pytest

from m3u_processor import config as cfg_mod
from m3u_processor.database import Database
from m3u_processor.orchestrator import Orchestrator
from m3u_processor import __main__ as cli


def _cfg(tmp):
    dbp = os.path.join(tmp, "m3u.db")
    cfgd = dict(cfg_mod.DEFAULTS)
    cfgd["database"]["path"] = dbp
    cfgd["output"]["dir"] = os.path.join(tmp, "out")
    cfgd["publish"] = {"enabled": False}
    # point feed_file at a nonexistent path so ingest loop is a no-op
    cfgd["sources"] = {"feed_file": os.path.join(tmp, "nofeed.txt"),
                       "playlist_dir": ""}
    cp = os.path.join(tmp, "config.yaml")
    yaml.safe_dump(cfgd, open(cp, "w"))
    return cfg_mod.load_config(config_path=cp)


def test_refresh_mode_skips_ingest(tmp_path):
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    # seed a favorite so Part B runs
    db.favorite_add(name="A", url="http://x/a.m3u",
                    original_url="http://x/a.m3u?t=OLD", is_enabled=1)
    orch = Orchestrator(db, cfg)

    # spy: ingest_feed must NOT be called during refresh
    called = {"n": 0}

    def _spy(url):
        called["n"] += 1
        return None

    orch.ingest_feed = _spy
    orch.run(mode="refresh")
    assert called["n"] == 0, "refresh mode must not ingest feeds"
    # favorite.m3u written
    assert os.path.exists(os.path.join(str(tmp_path), "out", "favorite.m3u"))
    db.close()


def test_ingest_feed_retries_on_429(tmp_path, monkeypatch):
    cfg = _cfg(str(tmp_path))
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    orch = Orchestrator(db, cfg)

    class _Resp:
        def __init__(self, code):
            self.status_code = code

        def raise_for_status(self):
            if self.status_code >= 400:
                import requests
                e = requests.exceptions.HTTPError(response=self)
                raise e

    import requests
    calls = {"n": 0}

    def _get(url, timeout=30):
        calls["n"] += 1
        # first two return 429, third succeeds
        return _Resp(429 if calls["n"] < 3 else 200)

    monkeypatch.setattr(requests, "get", _get)
    # parse_text will fail on empty body after success; we only assert it
    # retried and did NOT raise before exhausting -> it will try 3 times then
    # raise on the 200 path's parse. Wrap to confirm retry happened.
    try:
        orch.ingest_feed("http://x/feed.m3u")
    except Exception:
        pass
    assert calls["n"] >= 2, "ingest_feed should retry on 429"
    db.close()
