"""Data models (no ORM — plain dataclasses, §14.1)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Stream:
    """A single stream entry (maps to `streams` table, §2.2)."""
    url: str                                # normalized dedupe key
    original_url: str                       # full URL as seen (tokens preserved)
    name: str = ""
    provider_domain: str = ""
    source_type: str = "remote"             # 'remote' | 'local'
    source_path: str = ""
    extinf_raw: str = ""
    attributes: dict = field(default_factory=dict)  # tvg-*, group-title, headers, alt_names[]

    # state
    is_working: Optional[bool] = None       # None=unchecked
    last_checked: Optional[str] = None
    last_working: Optional[str] = None
    consecutive_failures: int = 0
    total_failures: int = 0
    total_successes: int = 0

    # blacklist
    blacklist_tier: str = "none"            # 'none' | 'short' | 'permanent'
    blacklisted_at: Optional[str] = None
    blacklist_reason: str = ""

    # enable/disable
    enabled: bool = True
    disabled_at: Optional[str] = None
    disabled_reason: str = ""
    disabled_by: str = ""

    # ids
    id: Optional[int] = None
    first_seen: Optional[str] = None
    updated_at: Optional[str] = None

    @property
    def group_title(self) -> str:
        return self.attributes.get("group-title", "")

    @property
    def vlc_options(self) -> dict:
        return self.attributes.get("vlc_options", {})

    @property
    def http_options(self) -> dict:
        return self.attributes.get("http_options", {})

    @property
    def kodi_headers(self) -> dict:
        return self.attributes.get("kodi_headers", {})

    @property
    def pipe_headers(self) -> dict:
        return self.attributes.get("pipe_headers", {})


@dataclass
class Provider:
    """Domain-level provider (maps to `providers` table, §6.2)."""
    domain: str
    enabled: bool = True
    disabled_at: Optional[str] = None
    disabled_reason: str = ""
    notes: str = ""
    first_seen: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class Run:
    """Run record (maps to `runs` table, §8)."""
    run_id: str
    mode: str
    started_at: str
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    status: str = "running"
    stats_json: str = ""
    error_message: str = ""
