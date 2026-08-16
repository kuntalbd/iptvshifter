"""M3U playlist parser (§2.1, F1-F31).

Parses standard + extended M3U, extracting:
- #EXTINF attributes (tvg-*, group-title, lock, custom)
- #EXTM3U x-tvg-url header -> attributes.playlist_epg
- #EXTVLCOPT: -> attributes.vlc_options
- #EXTHTTP:   -> attributes.http_options (OTT Navigator)
- #KODIPROP:  -> attributes.kodi_headers
- pipe syntax URL|Headers -> attributes.pipe_headers
- #EXTGRP:    -> attributes.group_title (M1)
- non-HTTP schemes, relative URLs, robustness guards (F4, F25)

Produces Stream objects (one per distinct normalized_url, honoring winner
logic F11 + multi-token C3).
"""
from __future__ import annotations

import re
from urllib.parse import urlparse, urljoin

from .models import Stream
from .utils import (
    normalize_url,
    extract_domain,
    split_pipe_url,
    ROTATING_TOKEN_PARAMS,
)

# Regex: #EXTINF:-1 [attrs] ,name
EXTINF_RE = re.compile(r"^#EXTINF:(-?\d+(?:\.\d+)?)\s*(.*?),(.*)$")
# attribute key="value" or key=value
ATTR_RE = re.compile(r'(\w[\w-]*)=("([^"]*)"|(\S*))')
VLOPT_RE = re.compile(r"^#EXTVLCOPT:\s*(.+?)=(.*)$")
HTTP_RE = re.compile(r"^#EXTHTTP:\s*(.+?)=(.*)$")
KODI_RE = re.compile(r"^#KODIPROP:\s*(.+?)=(.*)$")
GRP_RE = re.compile(r"^#EXTGRP:\s*(.*)$")

# URL schemes we cannot HTTP-probe (F4)
NON_HTTP_SCHEMES = {"rtmp", "rtsp", "mmsh", "mms", "srt", "udp", "rtmpe", "rtmpt"}


def _parse_attrs(attr_str: str) -> dict:
    """Parse #EXTINF attribute string into a dict (custom preserved)."""
    out = {}
    for m in ATTR_RE.finditer(attr_str):
        key = m.group(1).lower()
        val = m.group(3) if m.group(3) is not None else m.group(4)
        out[key] = val
    return out


def _is_url_line(line: str) -> str | None:
    """Return the URL if line is a bare URL (http/https or non-http scheme).

    Also normalizes the IPTV `@url:` prefix variant (some playlists wrap the
    URL as `@url:` + a backtick-quoted URL). The leading `@url:` and any
    surrounding backticks / quotes / whitespace are stripped so the stored URL
    is a clean absolute URL. Without this, VLC reports
    "unable to open the MRL '@url:`http://...`'" (malformed URL).
    """
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    # strip @url: prefix (case-insensitive)
    m = re.match(r"^@url:\s*(.*)$", s, re.IGNORECASE)
    if m:
        s = m.group(1).strip()
    # strip surrounding backticks / quotes
    s = s.strip("`\"'").strip()
    if not s:
        return None
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://", s):
        return s
    # possibly relative
    if not s.startswith("#"):
        return s
    return None


class PlaylistParser:
    """Generator-based parser (§22 memory-friendly). Call parse_file() or
    parse_text() to yield Stream objects."""

    def __init__(self, aggregate_subdomains: bool = True):
        self.aggregate_subdomains = aggregate_subdomains

    def _resolve(self, url: str, base: str | None) -> str:
        if base and not re.match(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://", url):
            # relative URL (F25) — rare but supported
            try:
                return urljoin(base, url)
            except Exception:
                return url
        return url

    def parse_lines(self, lines, source_type="remote", source_path="", base_url=None):
        """Yield Stream objects from an iterable of lines.

        Multiple #EXTVLCOPT/#EXTHTTP/#KODIPROP lines before a URL line are
        accumulated and attached to the following stream. Pipe syntax is split
        from the URL line itself.
        """
        pending_vlc = {}
        pending_http = {}
        pending_kodi = {}
        pending_grp = None
        pending_extinf = None  # (attrs_dict, name)
        pending_epg = None

        for raw in lines:
            line = raw.rstrip("\n").rstrip("\r")
            stripped = line.strip()
            if not stripped:
                continue

            # --- directives first (order matters: F15 KODIPROP-internal-pipe) ---
            if stripped.startswith("#EXTINF:"):
                m = EXTINF_RE.match(stripped)
                if m:
                    attrs = _parse_attrs(m.group(2))
                    name = m.group(3).strip()
                    pending_extinf = (attrs, name)
                continue

            if stripped.startswith("#EXTM3U"):
                # header line may carry x-tvg-url="..."
                um = re.search(r'x-tvg-url="([^"]*)"', stripped)
                if um:
                    pending_epg = um.group(1)
                continue

            if stripped.startswith("#EXTVLCOPT:"):
                mm = VLOPT_RE.match(stripped)
                if mm:
                    pending_vlc[mm.group(1).lower()] = mm.group(2)
                continue

            if stripped.startswith("#EXTHTTP:"):
                mm = HTTP_RE.match(stripped)
                if mm:
                    pending_http[mm.group(1).lower()] = mm.group(2)
                continue

            if stripped.startswith("#KODIPROP:"):
                mm = KODI_RE.match(stripped)
                if mm:
                    pending_kodi[mm.group(1).lower()] = mm.group(2)
                continue

            if stripped.startswith("#EXTGRP:"):
                mm = GRP_RE.match(stripped)
                if mm:
                    pending_grp = mm.group(1).strip()
                continue

            if stripped.startswith("#"):
                # unknown directive / comment -> skip (robustness, §2.1)
                continue

            # --- bare URL line (may carry pipe syntax) ---
            url_seen = _is_url_line(stripped)
            if url_seen is None:
                continue

            url_seen = self._resolve(url_seen, base_url)
            base_url_part, pipe_hdr = split_pipe_url(url_seen)

            # Build pipe_headers from the pipe tail
            pipe_headers = {}
            if pipe_hdr:
                for seg in pipe_hdr.split("&"):
                    if "=" in seg:
                        k, v = seg.split("=", 1)
                        pipe_headers[k.strip()] = v.strip()

            attrs = {}
            if pending_extinf:
                ext_attrs, name = pending_extinf
                attrs.update(ext_attrs)
            else:
                name = ""

            # group-title resolution: EXTINF group-title > #EXTGRP > ""
            group = attrs.get("group-title") or pending_grp or ""

            attributes = {
                "tvg-id": attrs.get("tvg-id", ""),
                "tvg-name": attrs.get("tvg-name", ""),
                "tvg-logo": attrs.get("tvg-logo", ""),
                "group-title": group,
                "lock": attrs.get("lock", ""),
                "vlc_options": pending_vlc.copy(),
                "http_options": pending_http.copy(),
                "kodi_headers": pending_kodi.copy(),
                "pipe_headers": pipe_headers,
            }
            if pending_epg:
                attributes["playlist_epg"] = pending_epg

            scheme = urlparse(base_url_part).scheme.lower()
            stream = Stream(
                url=normalize_url(base_url_part),
                original_url=base_url_part,  # tokens preserved (F2)
                name=name or attrs.get("tvg-name", ""),
                provider_domain=extract_domain(
                    base_url_part, self.aggregate_subdomains
                ),
                source_type=source_type,
                source_path=source_path,
                extinf_raw=stripped if pending_extinf else "",
                attributes=attributes,
            )
            stream._scheme = scheme  # transient, not persisted
            stream._non_http = scheme in NON_HTTP_SCHEMES

            yield stream

            # reset pending
            pending_vlc = {}
            pending_http = {}
            pending_kodi = {}
            pending_grp = None
            pending_extinf = None

    def parse_text(self, text, **kw):
        return list(self.parse_lines(text.splitlines(), **kw))

    def parse_file(self, path, source_type="local", base_url=None):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return list(
                self.parse_lines(
                    f,
                    source_type=source_type,
                    source_path=path,
                    base_url=base_url,
                )
            )


def merge_into_db(db, streams, run_id="", allow_multi_token=True):
    """Insert/update streams with winner + multi-token logic (§2.3, F11, C3).

    - Dedupe key = normalized url.
    - Winner = prefer tokened/longest original_url (F11).
    - If same normalized key already exists with a DIFFERENT tokened
      original_url, store as additional row only when allow_multi_token
      (C3) — to avoid discarding a working token.
    Returns dict of stats.
    """
    stats = {"inserted": 0, "updated": 0, "duplicates": 0, "multi_token": 0}
    for s in streams:
        norm = s.url
        existing = db.query(
            "SELECT id, original_url, attributes FROM streams WHERE url=?", (norm,)
        )
        if not existing:
            db.execute(
                """INSERT INTO streams
                   (url, original_url, name, provider_domain, source_type, source_path,
                    extinf_raw, attributes, first_seen, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (
                    norm,
                    s.original_url,
                    s.name,
                    s.provider_domain,
                    s.source_type,
                    s.source_path,
                    s.extinf_raw,
                    __import__("json").dumps(s.attributes),
                ),
            )
            stats["inserted"] += 1
            continue

        row = existing[0]
        cur_orig = row["original_url"]
        if cur_orig == s.original_url:
            stats["duplicates"] += 1
            continue

        # Different original_url for same key -> winner / multi-token (F11/C3)
        cur_tokened = any(
            p in (cur_orig.split("?")[1] if "?" in cur_orig else "")
            for p in ROTATING_TOKEN_PARAMS
        )
        new_tokened = any(
            p in (s.original_url.split("?")[1] if "?" in s.original_url else "")
            for p in ROTATING_TOKEN_PARAMS
        )
        if new_tokened and not cur_tokened:
            # new is better (tokened) -> upgrade original_url
            db.execute(
                "UPDATE streams SET original_url=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (s.original_url, row["id"]),
            )
            stats["updated"] += 1
        elif cur_orig.startswith("@") and not s.original_url.startswith("@"):
            # F-fix: existing row has a malformed `@url:` prefixed URL
            # (VLC-MRL bug). A clean re-parsed URL wins so bad data self-heals.
            db.execute(
                "UPDATE streams SET original_url=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (s.original_url, row["id"]),
            )
            stats["updated"] += 1
        elif allow_multi_token and new_tokened and cur_tokened:
            # both tokened but different -> keep both as separate rows (C3)
            # Use a UUID suffix for the secondary key to guarantee uniqueness
            # (abs(hash()) can collide across thousands of streams and crash
            # the whole ingest with a UNIQUE violation — F37).
            import uuid
            db.execute(
                """INSERT INTO streams
                   (url, original_url, name, provider_domain, source_type, source_path,
                    extinf_raw, attributes, first_seen, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (
                    norm + "|" + uuid.uuid4().hex,
                    s.original_url,
                    s.name,
                    s.provider_domain,
                    s.source_type,
                    s.source_path,
                    s.extinf_raw,
                    __import__("json").dumps(s.attributes),
                ),
            )
            stats["multi_token"] += 1
        else:
            stats["duplicates"] += 1
    db.commit()
    return stats
