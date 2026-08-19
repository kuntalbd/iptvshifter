"""Playlist writers: multi-format output (§7.1, §24.2, F13-F22).

Generates:
  - working.m3u         : standard EXTINF + #EXTVLCOPT (VLC)
  - working.kodi.m3u    : #KODIPROP (Kodi)
  - working.tivimate.m3u: URL|Headers (TiviMate/OTT)
All three built from the SAME stream rows; only header syntax differs.
Header rendering delegates to utils (headers_to_vlc/kodi/pipe) so the
VLC-style key names (http-user-agent, http-referrer, ...) are correct.

Categorization (2026-08-15): streams are grouped into a SMALL curated
taxonomy (genre > country) via m3u_processor.categorize, then written
into a SINGLE file with `# === Group ===` section headers. The stream's
group-title attribute is also rewritten to the canonical group so players
display the normalized category.
"""
from __future__ import annotations

import json
import os

from .utils import merge_headers, headers_to_vlc, headers_to_kodi, headers_to_pipe
from .categorize import Categorizer
from .logging_utils import get_logger as _get_logger

_LOG = _get_logger("m3u.writers")


def _as_attrs(stream):
    """Extract the attributes dict from a Stream object or a DB row."""
    if hasattr(stream, "attributes"):
        a = stream.attributes
    else:
        a = stream["attributes"]
    if isinstance(a, str):
        a = json.loads(a or "{}")
    return a or {}


def _as_field(stream, name):
    if hasattr(stream, name):
        return getattr(stream, name)
    # mapping-like (sqlite3.Row / dict): use .get to avoid KeyError on missing
    if isinstance(stream, dict):
        return stream.get(name)
    try:
        return stream[name]
    except (KeyError, IndexError, TypeError):
        return None


def write_streams(streams, base_path: str, formats=("vlc", "kodi", "tivimate"),
                   categories_cfg=None, sort_by_group=True, quality_cfg=None):
    """Write one or more format files. `streams` is a list of Stream objects
    or sqlite3.Row dict-likes. Returns dict fmt->output_path.

    Categorization: each stream is assigned a canonical group (genre > country,
    via Categorizer). Streams are grouped and emitted in a SINGLE file with
    `# === Group ===` section headers, sorted by group then name. The stream's
    group-title attribute is rewritten to the canonical group.

    Quality marking (quality_cfg from config): if `mark_in_group_title` is true,
    a ⭐ (healthy) / 🐢 (slow) prefix is added to the group-title so players can
    surface health. If `separate_healthy_file` is true, a `healthy.<ext>` file
    containing only healthy-tier streams is also written.
    """
    cat = Categorizer(categories_cfg)
    mark_gt = bool((quality_cfg or {}).get("mark_in_group_title", False))

    # attach canonical group + health tier to each stream
    enriched = []
    for s in streams:
        attrs = _as_attrs(s)
        name = _as_field(s, "name")
        gt = attrs.get("group-title")
        domain = _as_field(s, "provider_domain") if hasattr(s, "provider_domain") \
            else attrs.get("provider_domain")
        group = cat.resolve(gt, name, domain)
        health = _as_field(s, "health_tier")
        enriched.append((group, cat.order_index(group), name or "", s, attrs, group, health))

    if sort_by_group:
        enriched.sort(key=lambda t: (t[1], t[2].lower(), t[0]))

    # partition healthy for separate file
    results = {}
    ext_map = {"vlc": "m3u", "kodi": "kodi.m3u", "tivimate": "tivimate.m3u"}
    separate_healthy = bool((quality_cfg or {}).get("separate_healthy_file", False))
    for fmt in formats:
        path = base_path
        if fmt != "vlc":
            path = base_path.rsplit(".", 1)[0] + "." + ext_map[fmt]
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        lines = ["#EXTM3U"]
        last_group = None
        healthy_lines = ["#EXTM3U"] if separate_healthy else None
        last_group_h = None
        for group, _, _, s, attrs, canonical, health in enriched:
            if sort_by_group and group != last_group:
                lines.append(f"# === {group} ===")
                last_group = group
            name = _as_field(s, "name")
            url = _as_field(s, "original_url")
            # rewrite group-title to canonical (so players show normalized category)
            attrs = dict(attrs)
            display_group = canonical
            if mark_gt and health in ("healthy", "medium", "slow"):
                icon = "⭐" if health == "healthy" else ("🐢" if health == "slow" else "⚠️")
                display_group = f"{icon} {canonical}"
            attrs["group-title"] = display_group
            tvg_keys = [
                ("tvg-id", attrs.get("tvg-id")),
                ("tvg-name", attrs.get("tvg-name")),
                ("tvg-logo", attrs.get("tvg-logo")),
                ("group-title", attrs.get("group-title")),
            ]
            tvg_str = " ".join(f'{k}="{v}"' for k, v in tvg_keys if v)
            head = "#EXTINF:-1"
            if tvg_str:
                head += " " + tvg_str
            if attrs.get("lock"):
                head += ' lock="true"'
            head += f",{name}"
            lines.append(head)
            if fmt == "tivimate":
                merged = merge_headers(
                    attrs.get("vlc_options", {}), attrs.get("http_options", {}),
                    attrs.get("kodi_headers", {}), attrs.get("pipe_headers", {}),
                )
                tail = headers_to_pipe(merged)
                lines.append(url + ("|" + tail if tail else ""))
            else:
                merged = merge_headers(
                    attrs.get("vlc_options", {}), attrs.get("http_options", {}),
                    attrs.get("kodi_headers", {}), attrs.get("pipe_headers", {}),
                )
                if fmt == "vlc":
                    for h in headers_to_vlc(merged):
                        lines.append(h)
                else:  # kodi
                    lines.append(headers_to_kodi(merged))
                lines.append(url)
            # healthy-only file
            if separate_healthy and health == "healthy":
                if sort_by_group and group != last_group_h:
                    healthy_lines.append(f"# === {group} ===")
                    last_group_h = group
                healthy_lines.append(head)
                if fmt == "tivimate":
                    healthy_lines.append(url + ("|" + tail if tail else ""))
                else:
                    if fmt == "vlc":
                        for h in headers_to_vlc(merged):
                            healthy_lines.append(h)
                    else:
                        healthy_lines.append(headers_to_kodi(merged))
                    healthy_lines.append(url)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        results[fmt] = path
        if separate_healthy:
            hpath = base_path.rsplit(".", 1)[0] + ".healthy." + (ext_map[fmt] if fmt != "vlc" else "m3u")
            with open(hpath, "w", encoding="utf-8") as f:
                f.write("\n".join(healthy_lines) + "\n")
            results[fmt + "_healthy"] = hpath
        _LOG.info("write_streams wrote fmt=%s path=%s streams=%d",
                  fmt, results[fmt], len(enriched))
    return results


def write_favorites(rows, out_dir: str, formats=("vlc", "kodi", "tivimate")):
    """Write favorite.*.m3u for enabled favorites.

    MIRRORS write_streams: publishes the tokened `original_url` (the playable
    working copy) so favorite channels actually play in the client. Falls back
    to `url` (tokenless) only when `original_url` is empty.

    Includes full EXTINF attributes (tvg-id, tvg-name, tvg-logo, group-title,
    lock) and stream headers (#EXTVLCOPT, #KODIPROP, pipe) matching
    write_streams output quality.
    """
    os.makedirs(out_dir, exist_ok=True)
    entries = []
    for r in rows:
        name = r["name"] or r["url"] or ""
        group = (r["groups"] or "").split(",")[0].strip() or ""
        play_url = r["original_url"] or r["url"] or ""
        if not play_url:
            continue
        attrs = {}
        raw_attrs = r["attributes"] if r["attributes"] is not None else None
        if raw_attrs:
            try:
                attrs = json.loads(raw_attrs) if isinstance(raw_attrs, str) else (raw_attrs or {})
            except Exception:
                attrs = {}
        attrs["group-title"] = group
        entries.append((name, play_url, group, attrs))

    results = {}
    for fmt in formats:
        if fmt == "vlc":
            path = os.path.join(out_dir, "favorite.m3u")
            lines = ["#EXTM3U"]
            for name, url, _, attrs in entries:
                tvg_keys = [
                    ("tvg-id", attrs.get("tvg-id")),
                    ("tvg-name", attrs.get("tvg-name")),
                    ("tvg-logo", attrs.get("tvg-logo")),
                    ("group-title", attrs.get("group-title")),
                ]
                tvg_str = " ".join(f'{k}="{v}"' for k, v in tvg_keys if v)
                head = "#EXTINF:-1"
                if tvg_str:
                    head += " " + tvg_str
                if attrs.get("lock"):
                    head += ' lock="true"'
                head += f",{name}"
                lines.append(head)
                merged = merge_headers(
                    attrs.get("vlc_options", {}), attrs.get("http_options", {}),
                    attrs.get("kodi_headers", {}), attrs.get("pipe_headers", {}),
                )
                for h in headers_to_vlc(merged):
                    lines.append(h)
                lines.append(url)
        elif fmt == "kodi":
            path = os.path.join(out_dir, "favorite.kodi.m3u")
            lines = ["#EXTM3U"]
            for name, url, _, attrs in entries:
                tvg_keys = [
                    ("tvg-id", attrs.get("tvg-id")),
                    ("tvg-name", attrs.get("tvg-name")),
                    ("tvg-logo", attrs.get("tvg-logo")),
                    ("group-title", attrs.get("group-title")),
                ]
                tvg_str = " ".join(f'{k}="{v}"' for k, v in tvg_keys if v)
                head = "#EXTINF:-1"
                if tvg_str:
                    head += " " + tvg_str
                if attrs.get("lock"):
                    head += ' lock="true"'
                head += f",{name}"
                lines.append(head)
                merged = merge_headers(
                    attrs.get("vlc_options", {}), attrs.get("http_options", {}),
                    attrs.get("kodi_headers", {}), attrs.get("pipe_headers", {}),
                )
                lines.append(headers_to_kodi(merged))
                lines.append(url)
        elif fmt == "tivimate":
            path = os.path.join(out_dir, "favorite.tivimate.m3u")
            lines = ["#EXTM3U"]
            for name, url, _, attrs in entries:
                tvg_keys = [
                    ("tvg-id", attrs.get("tvg-id")),
                    ("tvg-name", attrs.get("tvg-name")),
                    ("tvg-logo", attrs.get("tvg-logo")),
                    ("group-title", attrs.get("group-title")),
                ]
                tvg_str = " ".join(f'{k}="{v}"' for k, v in tvg_keys if v)
                head = "#EXTINF:-1"
                if tvg_str:
                    head += " " + tvg_str
                if attrs.get("lock"):
                    head += ' lock="true"'
                head += f",{name}"
                lines.append(head)
                merged = merge_headers(
                    attrs.get("vlc_options", {}), attrs.get("http_options", {}),
                    attrs.get("kodi_headers", {}), attrs.get("pipe_headers", {}),
                )
                tail = headers_to_pipe(merged)
                lines.append(url + ("|" + tail if tail else ""))
        else:
            continue
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
        results[fmt] = path
        _LOG.info("write_favorites wrote fmt=%s path=%s entries=%d",
                  fmt, results[fmt], len(entries))
    return results
