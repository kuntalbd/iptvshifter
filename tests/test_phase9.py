"""Phase 9 tests: stream categorization / group normalization."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from m3u_processor import categorize


def test_bangladesh_aliases_collapse():
    c = categorize.Categorizer()
    for raw in ["bangla", "BanglaIPTV", "Bangladeshi", "Bangladesh", "BD",
                "BD TV", "bdix", "deshi", "বাংলা", "টিভি", "bangla tv"]:
        assert c.resolve(raw, "Some Channel", None) == "Bangladesh", raw


def test_genre_over_country():
    # "Bangladesh News" -> News (genre > country)
    c = categorize.Categorizer()
    assert c.resolve("Bangladesh News", "NTV", None) == "News"
    # "India Sports" -> Sports
    assert c.resolve("India Sports", "Star Sports", None) == "Sports"


def test_country_fallback_when_no_genre():
    c = categorize.Categorizer()
    assert c.resolve("Bangla", "ATN Bangla", None) == "Bangladesh"
    assert c.resolve("hindi", "Zee", None) == "India"
    assert c.resolve("china", "CCTV", None) == "South Korea and China"
    assert c.resolve("usa", "HBO", None) == "USA"


def test_unknown_catchall():
    c = categorize.Categorizer()
    assert c.resolve("random stuff xyz", "WeirdChan", None) == "Other"


def test_provider_domain_hint():
    c = categorize.Categorizer()
    # no group/name signal, but bdix domain -> Bangladesh
    assert c.resolve(None, "MyStream", "cdn.bdix.net") == "Bangladesh"


def test_config_override():
    cfg = {"unknown": "Misc", "genre": {"Tech": ["tech", "coding"]},
           "country": {"BD": ["bd"]}}
    c = categorize.Categorizer(cfg)
    assert c.resolve("tech news", "CodeTV", None) == "Tech"
    assert c.resolve("bd", "X", None) == "BD"


def test_fresh_eye_section_order_small_taxonomy():
    # taxonomy must stay <=20 groups (user hard limit)
    c = categorize.Categorizer()
    total = len(c.genre) + len(c.country) + 1  # +1 unknown
    assert total <= 20, f"taxonomy too big: {total}"
    # order: genre first, then country, then unknown
    assert c.order_index("News") < c.order_index("Bangladesh")
    assert c.order_index("Bangladesh") < c.order_index("Other")


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
