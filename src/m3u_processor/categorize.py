"""Stream categorization / group normalization (user directive 2026-08-15).

Goal: collapse wildly-varying playlist group labels into a SMALL curated
taxonomy (<=20 groups) so the output is human-readable and player-friendly.

Approach (multi-signal, genre > country priority):
  1. Normalize the raw label (lowercase, strip punctuation, keep unicode so
     Bangla labels survive — we match them directly, not via transliteration).
  2. Match against GENRE aliases first, then COUNTRY aliases (genre > country).
  3. If no group-title, fall back to the stream NAME, then provider DOMAIN.
  4. Unknown -> configured `unknown` group (default "Other").

The taxonomy + aliases live in `config.yaml` under `categories:` so the user
can tune them WITHOUT touching code. A built-in default is used if absent.
"""
from __future__ import annotations

import re
import unicodedata

# ---------------------------------------------------------------------------
# Built-in default taxonomy (curated, 13 groups). Country groups LAST so the
# "International" catch-all only fires when nothing else matched. Genre groups
# listed before country groups => genre > country priority by iteration order.
# ---------------------------------------------------------------------------
DEFAULT_CATEGORIES = {
    "unknown": "Other",
    "genre": {
        "News": ["news", "বার্তা", "cnn", "bbc", "aljazeera", "france24", "sky",
                 "ntv news", "সংবাদ"],
        "Sports": ["sports", "sport", "খেলা", "cricket", "football", "epl", "ufc",
                   "wwe", "psl", "স্পোর্টস", "live sports"],
        "Movies": ["movies", "movie", "cinema", "bollywood", "hollywood",
                   "hd movies", "সিনেমা", "film"],
        "Entertainment": ["ent", "general", "drama", "comedy", "series",
                          "reality", "tv shows", "লাইভ", "entertainment", "গল্প"],
        "Kids": ["kids", "children", "cartoon", "নাটিকা", "animation", "শিশু",
                 "baby"],
        "Music": ["music", "songs", "গান", "mtv", "বাংলা গান", "melody"],
        "Religious": ["islam", "quran", "naat", "christian", "gospel",
                      "spiritual", "ইসলাম", "ধর্ম"],
        "Documentary": ["doc", "documentary", "nature", "discovery",
                        "history", "science"],
        "Education": ["education", "learning", "tutorial", "ক্লাস"],
    },
    "country": {
        "Bangladesh": ["bangla", "banglaiptv", "bangladeshi", "bangladesh", "bd",
                       "bd tv", "bdix", "deshi", "বাংলা", "টিভি", "bangla tv"],
        "India": ["india", "indian", "hindi", "tamil", "telugu", "desi",
                  "बॉलीवुड", "ind"],
        "South Korea and China": ["korea", "korean", "south korea", "china",
                                  "chinese", "cctv", "kbs", "sbs", "hk",
                                  "k-drama", "cdrama"],
        "USA": ["usa", "us", "america", "american", "u.s.", "abc", "nbc",
                "fox", "hbo", "cbs"],
        "International": ["uk", "france", "french", "arabic", "turkey",
                          "turkish", "germany", "russia", "world", "europe",
                          "canada", "spanish", "italy", "japan", "thai"],
    },
}


_PUNCT = re.compile(r"[\"'`’.,:;!?()\[\]{}<>|/\\+=&_~-]")


def _fold(s: str) -> str:
    """Lowercase + strip punctuation + collapse spaces. Keeps unicode (Bangla)."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", str(s)).lower()
    s = _PUNCT.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _hit(aliases, text):
    """Intelligent alias match against `text` (already a label/name).

    Matches when an alias equals a whole token, appears as a word-bounded
    substring, or the alias fully contains a token (e.g. `bd` inside `bdix`,
    `china` inside `chinese`). Bangla aliases match the raw unicode directly.
    Deliberately NOT a loose cross-substring (e.g. `tv` must not match
    `tv shows`); only bounded-substring or token-contained checks are used so
    short aliases don't over-fire.
    """
    folded_text = _fold(text)
    if not folded_text:
        return False
    bounded = " " + folded_text + " "
    tokens = folded_text.split()
    for a in aliases:
        fa = _fold(a)
        if not fa:
            continue
        # exact whole label
        if fa == folded_text:
            return True
        # alias is a whole word in the text
        if (" " + fa + " ") in bounded:
            return True
        # single-word aliases match via PREFIX (either direction) so that
        # "bd" -> "bdix" and "china" -> "chinese", but "tv" does NOT match
        # "mtv" or "tv shows".
        if " " not in fa:
            for w in tokens:
                if w and (fa.startswith(w) or w.startswith(fa)) and w != fa:
                    return True
    return False


class Categorizer:
    """Resolves a stream's canonical category from its metadata."""

    def __init__(self, categories_cfg: dict | None = None):
        cfg = categories_cfg or DEFAULT_CATEGORIES
        self.unknown = cfg.get("unknown", "Other")
        self.genre = cfg.get("genre", DEFAULT_CATEGORIES["genre"])
        self.country = cfg.get("country", DEFAULT_CATEGORIES["country"])
        # stable display order: genre groups, then country groups, then unknown
        self.order = list(self.genre.keys()) + list(self.country.keys()) + [self.unknown]
        self._order_index = {g: i for i, g in enumerate(self.order)}

    def _match_groups(self, text, groups: dict) -> str | None:
        if not text:
            return None
        for canonical, aliases in groups.items():
            if _hit(aliases, text):
                return canonical
        return None

    def resolve(self, group_title: str | None, name: str | None,
                provider_domain: str | None) -> str:
        # 1) genre (from group-title)
        g = self._match_groups(group_title or "", self.genre)
        if g:
            return g
        # 2) country (from group-title)
        g = self._match_groups(group_title or "", self.country)
        if g:
            return g
        # 3) genre (from name, when group-title missing)
        g = self._match_groups(name or "", self.genre)
        if g:
            return g
        # 4) country (from name)
        g = self._match_groups(name or "", self.country)
        if g:
            return g
        # 5) provider domain hints
        dom = (provider_domain or "").lower()
        if "bdix" in dom or dom.endswith(".bd") or "bangla" in dom:
            return "Bangladesh"
        if dom.endswith(".in") or "india" in dom:
            return "India"
        if dom.endswith(".cn") or "china" in dom or "korea" in dom:
            return "South Korea and China"
        if dom.endswith(".us") or "usa" in dom or "american" in dom:
            return "USA"
        return self.unknown

    def order_index(self, group: str) -> int:
        return self._order_index.get(group, len(self.order))


# module-level default instance for callers that don't pass a config
_default = Categorizer()


def categorize(group_title: str | None, name: str | None,
              provider_domain: str | None,
              categories_cfg: dict | None = None) -> str:
    if categories_cfg is None:
        return _default.resolve(group_title, name, provider_domain)
    return Categorizer(categories_cfg).resolve(group_title, name, provider_domain)
