"""Utility helpers: URL normalization, domain extraction, header merging.

Implements §2.3.1 normalization rules (F2/F3/F11) and §24.2 header transform.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse, urlencode, urlunparse

# Rotating-token query params stripped from the *dedupe key* only.
# Single source of truth — keep config.yaml `strip_query_params` in sync (§12).
ROTATING_TOKEN_PARAMS = {
    "token", "sig", "signature", "sign", "token2", "auth",
    "expires", "md5", "hmac", "nonce", "ts", "key", "hdnea",
}

# Static fingerprint params that MUST NOT be stripped (F7) — stripping risks
# merging distinct channels.
STATIC_FINGERPRINT_PARAMS = {
    "deviceid", "devicemodel", "deviceversion", "devicetype", "devicemake",
    "devicednt", "appversion", "appname", "app_name", "advertisingid", "rdid",
    "channel_id", "platform", "content_", "tags", "coppa", "genre", "studio_id",
    "bmodel", "is_lat", "embedpartner",
}


def normalize_url(url: str) -> str:
    """Return the dedupe key: rotating-token params removed, non-kv watermark
    tails dropped, case-insensitive param match, scheme/host lowercased.

    The ORIGINAL url (with tokens) is stored separately in DB; this key is only
    for matching duplicates. See §2.3.1 / F2 / F3 / F11.
    """
    url = url.strip()
    parts = urlparse(url)
    if not parts.scheme or not parts.netloc:
        # Not a parseable URL; return stripped of whitespace for a best-effort key.
        return url

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()

    # Drop non-kv query tails (F3): a raw query segment without '=' (e.g.
    # `?Live-TV™`, `?@Shamim`, `?checkedby:iptvcat.com`) is a watermark, not a
    # key=value pair, and must be dropped so it doesn't fragment dedupe.
    query = parts.query
    if query:
        kept = []
        for seg in query.split("&"):
            if "=" not in seg:
                continue  # non-kv watermark tail -> drop (F3)
            k = seg.split("=", 1)[0]
            if k.lower() in ROTATING_TOKEN_PARAMS:
                continue  # strip rotating token from key (F2)
            kept.append(seg)
        query = "&".join(kept)

    # Reassemble without fragment
    return urlunparse((scheme, netloc, parts.path, parts.params, query, ""))


def is_tokened(url: str) -> bool:
    """True if URL carries a recognized rotating-token param (C1 stale-token path)."""
    parts = urlparse(url)
    if not parts.query:
        return False
    keys = {seg.split("=", 1)[0].lower() for seg in parts.query.split("&") if "=" in seg}
    return bool(keys & ROTATING_TOKEN_PARAMS)


def extract_domain(url: str, aggregate_subdomains: bool = True) -> str:
    """Extract registrable-ish domain from a URL netloc (port stripped).

    aggregate_subdomains collapses cdn.example.com -> example.com when enabled.
    Simple heuristic: keep last two labels unless the TLD is a known multi-label
    ccTLD (co.uk, com.br, etc.).  IP addresses (v4/v6) are returned as-is.
    """
    parts = urlparse(url)
    netloc = parts.netloc.lower()
    if not netloc:
        return ""
    # IPv6 bracket notation — extract host before port stripping
    if netloc.startswith("["):
        # [::1]:8080 → extract "[::1]" then strip brackets
        bracket_end = netloc.find("]")
        if bracket_end != -1:
            return netloc[:bracket_end + 1]
        return netloc
    if ":" in netloc:
        netloc = netloc.split(":", 1)[0]
    # IPv4 addresses — return as-is, don't aggregate.
    if re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", netloc):
        return netloc
    labels = netloc.split(".")
    if len(labels) <= 2:
        return netloc
    if aggregate_subdomains:
        # multi-label TLD handling
        two_label_tlds = {"co", "com", "net", "org", "gov", "ac", "edu"}
        if labels[-2] in two_label_tlds and len(labels) > 2:
            return ".".join(labels[-3:])
        return ".".join(labels[-2:])
    return netloc


def merge_headers(*header_dicts: dict) -> dict:
    """Merge header sources (vlc_options, http_options, pipe_headers, kodi_headers)
    into a canonical dict. Later dicts override earlier. Keys normalized to
    canonical form: User-Agent, Referer, Origin, Cookie, etc. (§24.2).
    """
    canonical = {}
    for d in header_dicts:
        if not d:
            continue
        for k, v in d.items():
            if v is None:
                continue
            ck = k.lower()
            # map common aliases
            if ck in ("http-user-agent", "user-agent", "useragent"):
                canonical["User-Agent"] = v
            elif ck in ("http-referrer", "referrer", "referer"):
                canonical["Referer"] = v
            elif ck in ("http-origin", "origin"):
                canonical["Origin"] = v
            elif ck in ("http-cookie", "cookie"):
                canonical["Cookie"] = v
            else:
                canonical[k] = v
    return canonical


# Ordering priority: transport-critical headers first, then alphabetical.
_HEADER_ORDER = ["User-Agent", "Referer", "Origin", "Cookie"]


def order_headers(headers: dict) -> list:
    """Return keys in a deterministic order: priority headers first, then the
    rest alphabetically (§24.2)."""
    keys = list(headers.keys())
    priority = [k for k in _HEADER_ORDER if k in keys]
    rest = sorted(k for k in keys if k not in _HEADER_ORDER)
    return priority + rest


def headers_to_vlc(headers: dict) -> list[str]:
    """Render canonical headers as `#EXTVLCOPT:` lines (VLC variant, §24.2)."""
    lines = []
    for k, v in headers.items():
        lk = k.lower()
        if lk == "user-agent":
            lines.append(f"#EXTVLCOPT:http-user-agent={v}")
        elif lk == "referer":
            lines.append(f"#EXTVLCOPT:http-referrer={v}")
        elif lk == "origin":
            lines.append(f"#EXTVLCOPT:http-origin={v}")
        elif lk == "cookie":
            lines.append(f"#EXTVLCOPT:http-cookie={v}")
        else:
            lines.append(f"#EXTVLCOPT:{k}={v}")
    return lines


def headers_to_kodi(headers: dict) -> str:
    """Render canonical headers as a single `#KODIPROP:inputstream.adaptive.stream_headers=`
    line (Kodi variant, §24.2). Uses the same http-* key names as VLC."""
    joined = "&".join(f"{_vlc_key(k)}={v}" for k, v in headers.items())
    return f"#KODIPROP:inputstream.adaptive.stream_headers={joined}"


def _vlc_key(k: str) -> str:
    """Map a canonical header name to its http-* transport form used by VLC/Kodi/TiviMate."""
    lk = k.lower()
    if lk == "user-agent":
        return "http-user-agent"
    if lk == "referer":
        return "http-referrer"
    if lk == "origin":
        return "http-origin"
    if lk == "cookie":
        return "http-cookie"
    return k


def headers_to_pipe(headers: dict) -> str:
    """Render canonical headers as `URL|Key=Val&Key=Val` tail (TiviMate, §24.2).
    Uses the same http-* key names as VLC/Kodi."""
    return "&".join(f"{_vlc_key(k)}={v}" for k, v in headers.items())


def split_pipe_url(url_line: str):
    """Split a bare URL line that may carry pipe syntax: `URL|Headers`.
    Returns (base_url, headers_str). Only splits a NON-# line on its FIRST `|`.
    (§2.1 — pipe is appended to URL, not a directive.)"""
    if "|" in url_line:
        base, _, hdr = url_line.partition("|")
        return base.strip(), hdr.strip()
    return url_line.strip(), ""
