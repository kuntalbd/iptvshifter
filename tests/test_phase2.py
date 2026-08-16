"""Phase 2 tests: parser against real + synthetic samples (F1-F31 coverage)."""
import os
import sys
import json
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor.parser import PlaylistParser, merge_into_db
from m3u_processor.database import Database
from m3u_processor.utils import normalize_url

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "requirement", "samples", "downloads")


def _streams_from(fn, source_type="local"):
    p = PlaylistParser()
    return p.parse_file(os.path.join(SAMPLES, fn), source_type=source_type)


def test_extinf_name_first_comma_only():
    # M2: name contains comma -> split on FIRST comma only
    text = '#EXTINF:-1 tvg-name="X" group-title="News",BBC One, HD\nhttp://e/x.m3u8\n'
    p = PlaylistParser()
    s = p.parse_text(text)[0]
    assert s.name == "BBC One, HD", f"got {s.name!r}"
    assert s.attributes["group-title"] == "News"


def test_extvlopt_parsed():
    # F1: #EXTVLCOPT captured
    text = '#EXTINF:-1,Chan\n#EXTVLCOPT:http-user-agent=UA1\n#EXTVLCOPT:http-referrer=R1\nhttp://e/x.m3u8\n'
    s = PlaylistParser().parse_text(text)[0]
    assert s.attributes["vlc_options"]["http-user-agent"] == "UA1"
    assert s.attributes["vlc_options"]["http-referrer"] == "R1"


def test_exthttp_parsed():
    # F14: #EXTHTTP captured
    text = '#EXTINF:-1,Chan\n#EXTHTTP:http-user-agent=UA2\nhttp://e/x.m3u8\n'
    s = PlaylistParser().parse_text(text)[0]
    assert s.attributes["http_options"]["http-user-agent"] == "UA2"


def test_kodiprop_parsed_and_internal_pipe_safe():
    # F16/F15: KODIPROP parsed; internal pipe in value must NOT break URL parse
    s_list = _streams_from("9999kodi.m3u")
    # only ORF TV channels (first 5) carry KODIPROP; rest are plain
    kodi_streams = [s for s in s_list if s.attributes["kodi_headers"]]
    assert len(kodi_streams) >= 5, f"expected >=5 kodi streams, got {len(kodi_streams)}"
    for s in kodi_streams:
        assert "|" not in s.original_url, f"pipe leaked into URL: {s.original_url}"
        assert s.attributes["kodi_headers"], "kodi_headers empty"


def test_pipe_syntax_parsed():
    # F15: pipe URL|Headers split
    s_list = _streams_from("9998pipe.m3u")
    assert len(s_list) == 12
    s0 = s_list[0]
    assert "|" not in s0.original_url
    assert "User-Agent" in s0.attributes["pipe_headers"]


def test_pipe_referer_with_equals():
    # FRESH-EYE: pipe header value containing '=' (Referer=https://x/)
    txt = "#EXTINF:-1,C\nhttp://e/c.m3u8|User-Agent=M&Referer=https://x/\n"
    s = PlaylistParser().parse_text(txt)[0]
    assert s.attributes["pipe_headers"].get("Referer") == "https://x/"


def test_vlc_state_no_leak_between_channels():
    # FRESH-EYE: pending vlc options must reset after each URL (no cross-channel leak)
    txt = "#EXTINF:-1,A\n#EXTVLCOPT:http-user-agent=UA1\nhttp://e/a.m3u8\n#EXTINF:-1,B\nhttp://e/b.m3u8\n"
    sl = PlaylistParser().parse_text(txt)
    assert sl[0].attributes["vlc_options"] == {"http-user-agent": "UA1"}
    assert sl[1].attributes["vlc_options"] == {}, "VLC options leaked to B"


def test_orphan_url_without_extinf():
    # FRESH-EYE: bare URL line with no preceding #EXTINF still yields a stream
    sl = PlaylistParser().parse_text("http://e/orphan.m3u8\n")
    assert len(sl) == 1
    assert sl[0].name == ""


def test_attributes_json_serializable():
    # FRESH-EYE: attributes must survive json.dumps (DB storage)
    import json
    s = PlaylistParser().parse_text(
        '#EXTINF:-1,A\n#EXTVLCOPT:http-user-agent=UA1\nhttp://e/a.m3u8\n'
    )[0]
    assert json.loads(json.dumps(s.attributes))["vlc_options"]["http-user-agent"] == "UA1"


def test_non_http_scheme_flagged():
    # F4: rtmp etc flagged non-http (uncheckable)
    text = "#EXTINF:-1,RTMP\nrtmp://e/live/stream\n"
    s = PlaylistParser().parse_text(text)[0]
    assert getattr(s, "_non_http", False) is True


def test_group_title_fallback_extgrp():
    # M1: #EXTGRP maps to group-title
    text = "#EXTGRP:Sports\n#EXTINF:-1,Chan\nhttp://e/x.m3u8\n"
    s = PlaylistParser().parse_text(text)[0]
    assert s.attributes["group-title"] == "Sports"


def test_real_playlist_parse_iptv_org():
    s_list = _streams_from("a5413816.m3u")
    assert len(s_list) > 1000
    # every stream has a normalized url
    assert all(s.url for s in s_list)


def test_merge_winner_prefers_tokened():
    # F11: prefer tokened original_url over bare
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    p = PlaylistParser()
    bare = p.parse_text("#EXTINF:-1,C\nhttp://e/x.m3u8\n")[0]
    tokened = p.parse_text(
        "#EXTINF:-1,C\nhttp://e/x.m3u8?md5=Z&expires=1\n"
    )[0]
    merge_into_db(db, [bare], "r")
    before = db.query("SELECT original_url FROM streams WHERE url=?", (bare.url,))[0][0]
    assert before == "http://e/x.m3u8"
    merge_into_db(db, [tokened], "r")
    after = db.query("SELECT original_url FROM streams WHERE url=?", (bare.url,))[0][0]
    assert after == "http://e/x.m3u8?md5=Z&expires=1", after
    db.close()


def test_merge_multi_token_distinct_rows():
    # C3: two distinct tokened variants -> two rows
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    p = PlaylistParser()
    a = p.parse_text("#EXTINF:-1,C\nhttp://e/x.m3u8?md5=A&expires=1\n")[0]
    b = p.parse_text("#EXTINF:-1,C\nhttp://e/x.m3u8?md5=B&expires=2\n")[0]
    stats = merge_into_db(db, [a, b], "r")
    rows = db.query("SELECT COUNT(*) FROM streams WHERE url LIKE ?", (normalize_url("http://e/x.m3u8") + "%",))
    assert rows[0][0] == 2, stats
    db.close()


def test_fresh_eye_f37_many_tokened_no_unique_crash():
    # F37: ingesting thousands of distinct tokened variants of the same base
    # URL must NOT crash with UNIQUE constraint (old abs(hash()) collided).
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    p = PlaylistParser()
    streams = []
    for i in range(2000):
        # all share normalize_url base but differ only in token -> same norm key
        s = p.parse_text(f"#EXTINF:-1,Ch{i}\nhttp://e/x.m3u8?md5={i}&expires={i}\n")[0]
        streams.append(s)
    # first insert one, then merge the rest (exercises multi_token INSERT path)
    merge_into_db(db, [streams[0]], "r1")
    stats = merge_into_db(db, streams[1:], "r2")
    # total distinct rows for this base url
    n = db.query("SELECT COUNT(*) FROM streams WHERE url LIKE ?",
                 (normalize_url("http://e/x.m3u8") + "%",))[0][0]
    assert n == 2000, (n, stats)  # all kept, no UNIQUE crash
    db.close()


def test_at_url_prefix_stripped():
    # VLC-MRL bug: playlist uses `@url:` + backtick-wrapped URL.
    # Parser must strip `@url:` and backticks so the stored URL is clean.
    cases = [
        ('@url:`http://212.5.144.156/disneyjr/index.m3u8`', "http://212.5.144.156/disneyjr/index.m3u8"),
        ("@url:http://e/x.m3u8", "http://e/x.m3u8"),
        ("`http://e/x.m3u8`", "http://e/x.m3u8"),
        ('@URL:`http://e/x.m3u8`', "http://e/x.m3u8"),
    ]
    for raw, expected in cases:
        s = PlaylistParser().parse_text(f"#EXTINF:-1,Chan\n{raw}\n")[0]
        assert s.original_url == expected, f"{raw!r} -> {s.original_url!r}"
        assert not s.original_url.startswith("@"), s.original_url
        assert "`" not in s.original_url, s.original_url


def test_fresh_eye_at_url_with_pipe():
    # @url: prefix combined with pipe headers
    raw = '@url:`http://e/x.m3u8|User-Agent=UA`'
    s = PlaylistParser().parse_text(f"#EXTINF:-1,Chan\n{raw}\n")[0]
    assert s.original_url == "http://e/x.m3u8", s.original_url
    assert s.attributes["pipe_headers"].get("User-Agent") == "UA"


def test_merge_self_heal_malformed_at_url():
    # F-fix: a pre-existing `@url:`-prefixed (malformed) URL in the DB (from a
    # playlist parsed before this fix) must be replaced by a clean re-parsed
    # URL on the next run (VLC-MRL bug self-heal).
    db = Database(os.path.join(tempfile.mkdtemp(), "t.db"))
    db.init_db(backup=False)
    # simulate a corrupted row already in the DB
    db.execute(
        "INSERT INTO streams (url, original_url, name, provider_domain, "
        "source_type, source_path, extinf_raw, attributes, first_seen, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        ("http://e/x.m3u8", "@url:`http://e/x.m3u8`", "C", "e",
         "remote", "", "", "{}"),
    )
    db.commit()
    assert db.query("SELECT original_url FROM streams WHERE url=?",
                    ("http://e/x.m3u8",))[0][0].startswith("@")
    good = PlaylistParser().parse_text("#EXTINF:-1,C\nhttp://e/x.m3u8\n")[0]
    merge_into_db(db, [good], "r")  # re-run heals it
    healed = db.query("SELECT original_url FROM streams WHERE url=?",
                      ("http://e/x.m3u8",))[0][0]
    assert healed == "http://e/x.m3u8", healed
    db.close()


if __name__ == "__main__":
    import traceback
    _tmp = tempfile.TemporaryDirectory()
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
