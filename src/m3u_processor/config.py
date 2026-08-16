"""Configuration loading with precedence: CLI args > env (M3U_*) > config.yaml > defaults.

Implements §18.4. Env mapping uses the M3U_ prefix (e.g. M3U_SHORT_BL_THRESHOLD
-> blacklist.short_threshold).
"""
from __future__ import annotations

import os
import yaml
from dataclasses import dataclass, field
from typing import Any

# Maps env var name -> dotted config path
ENV_MAP = {
    "M3U_DB_PATH": "database.path",
    "M3U_FEED_FILE": "sources.feed_file",
    "M3U_PLAYLIST_DIR": "sources.playlist_dir",
    "M3U_OUTPUT_DIR": "output.dir",
    "M3U_WORKERS": "validation.workers",
    "M3U_SHORT_BL_THRESHOLD": "blacklist.short_threshold",
    "M3U_PERM_INACTIVE_DAYS": "blacklist.permanent_inactive_days",
    "M3U_PERM_FAIL_THRESHOLD": "blacklist.permanent_failure_threshold",
    "M3U_ESCALATE_ENABLED": "blacklist.escalate_enabled",
    "M3U_WEBUI_HOST": "webui.host",
    "M3U_WEBUI_PORT": "webui.port",
    "M3U_VALIDATION_MODE": "validation.mode",
    "M3U_TOKEN_REFRESH": "validation.token_refresh",
}

DEFAULTS: dict = {
    "database": {"path": "./data/m3u.db", "backup_on_start": True, "backup_keep": 7},
    "sources": {
        "feed_file": "./feeds.txt",
        "playlist_dir": "./playlists",
        "recursive_scan": True,
        "file_patterns": ["*.m3u", "*.m3u8", "*.txt"],
    },
    "output": {
        "dir": "./output",
        "working_filename": "working",
        "formats": ["vlc", "kodi", "tivimate"],
        "generate_aux_files": True,
        "sort_by": "group_title",
        "uncategorized_label": "Uncategorized",
    },
    "validation": {
        "mode": "quick",
        "workers": 20,
        "max_workers": 50,
        "timeout_connect": 10,
        "timeout_read": 15,
        "retries": 2,
        "backoff": [5, 15, 30],
        "per_host_limit": 5,
        "user_agent_rotation": True,
        "follow_redirects": True,
        "max_redirects": 5,
        "verify_ssl": True,
        "token_refresh": True,
        "max_token_refetch_per_feed": 1,
        "strip_query_params": [
            "token", "sig", "signature", "sign", "token2", "auth",
            "expires", "md5", "hmac", "nonce", "ts", "key", "hdnea",
        ],
    },
    "blacklist": {
        "short_threshold": 3,
        "permanent_inactive_days": 30,
        "permanent_failure_threshold": 10,
        "escalate_enabled": True,
        "purge_unchecked_days": 90,
    },
    "quality": {
        # Option A: latency-based health scoring (cheap, approximate)
        "latency_check": True,            # enable/disable A entirely
        # thresholds in SECONDS (configurable)
        "healthy_max_ms": 2000,           # elapsed < 2s  -> healthy
        "medium_max_ms": 5000,            # 2s..5s        -> medium
        # > 5s                              -> slow (likely buffer)
        # Option B: throughput sampling (accurate, costs traffic)
        "throughput_check": True,         # enable/disable B entirely
        "throughput_sample_seconds": 3,   # how long to download to measure
        "throughput_min_kbps": 500,       # measured < 500 KB/s -> unhealthy
        # Output marking
        "mark_in_group_title": False,     # prefix ⭐ healthy / 🐢 slow in group-title
        "separate_healthy_file": False,   # also write healthy.m3u (only healthy)
    },
    "providers": {"aggregate_subdomains": True, "auto_create": True},
    "webui": {
        "enabled": True,
        "host": "0.0.0.0",
        "port": 50152,
        "auth_token_file": "./webui_token.txt",
    },
    "scheduler": {"enabled": True, "mode": "quick", "cron_expression": "0 4,16 * * *"},
    "logging": {
        "level": "INFO",
        "json_format": False,
        "file": "./logs/m3u-processor.log",
        "max_bytes": 10485760,
        "backup_count": 5,
    },
    "categories": {
        "unknown": "Other",
        "genre": {
            "News": ["news", "বার্তা", "cnn", "bbc", "aljazeera", "france24", "sky", "ntv news", "সংবাদ"],
            "Sports": ["sports", "sport", "খেলা", "cricket", "football", "epl", "ufc", "wwe", "psl", "স্পোর্টস", "live sports"],
            "Movies": ["movies", "movie", "cinema", "bollywood", "hollywood", "hd movies", "সিনেমা", "film"],
            "Entertainment": ["ent", "general", "drama", "comedy", "series", "reality", "tv shows", "লাইভ", "entertainment", "গল্প"],
            "Kids": ["kids", "children", "cartoon", "নাটিকা", "animation", "শিশু", "baby"],
            "Music": ["music", "songs", "গান", "mtv", "বাংলা গান", "melody"],
            "Religious": ["islam", "quran", "naat", "christian", "gospel", "spiritual", "ইসলাম", "ধর্ম"],
            "Documentary": ["doc", "documentary", "nature", "discovery", "history", "science"],
            "Education": ["education", "learning", "tutorial", "ক্লাস"],
        },
        "country": {
            "Bangladesh": ["bangla", "banglaiptv", "bangladeshi", "bangladesh", "bd", "bd tv", "bdix", "deshi", "বাংলা", "টিভি", "bangla tv"],
            "India": ["india", "indian", "hindi", "tamil", "telugu", "desi", "बॉलीवुड", "ind"],
            "South Korea and China": ["korea", "korean", "south korea", "china", "chinese", "cctv", "kbs", "sbs", "hk", "k-drama", "cdrama"],
            "USA": ["usa", "us", "america", "american", "u.s.", "abc", "nbc", "fox", "hbo", "cbs"],
            "International": ["uk", "france", "french", "arabic", "turkey", "turkish", "germany", "russia", "world", "europe", "canada", "spanish", "italy", "japan", "thai"],
        },
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _set_path(cfg: dict, dotted: str, value: Any):
    parts = dotted.split(".")
    cur = cfg
    for p in parts[:-1]:
        cur = cur.setdefault(p, {})
    cur[parts[-1]] = value


def _coerce(val: str, current: Any) -> Any:
    if isinstance(current, bool):
        return val.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(current, int):
        return int(val)
    if isinstance(current, float):
        return float(val)
    if isinstance(current, list):
        return [x.strip() for x in val.split(",")]
    return val


@dataclass
class Config:
    """Loaded configuration with attribute-style + dict access."""
    _data: dict = field(default_factory=dict)
    config_dir: str = ""  # directory of the loaded config.yaml (anchor for relative paths)
    config_path: str = ""  # path the config was loaded from (for save())

    def get(self, dotted: str, default=None):
        cur = self._data
        for p in dotted.split("."):
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    def set(self, dotted: str, value):
        _set_path(self._data, dotted, value)

    @property
    def data(self) -> dict:
        return self._data

    def as_yaml(self) -> str:
        return yaml.safe_dump(self._data, sort_keys=False)

    def save(self, path: str):
        """Persist the current config back to disk (e.g. scheduler edits)."""
        with open(path, "w", encoding="utf-8") as f:
            f.write(yaml.safe_dump(self._data, sort_keys=False))

    def resolve_path(self, dotted: str) -> str:
        """Resolve a config path key against the config file's directory.

        Relative paths are anchored to ``config_dir`` (the folder containing the
        config.yaml that was loaded) so the whole project can be moved by simply
        relocating that folder — no hardcoded absolute paths anywhere. Absolute
        paths pass through unchanged. Falls back to CWD when no config file was
        used.
        """
        val = self.get(dotted)
        if not isinstance(val, str) or not val:
            return val
        if os.path.isabs(val):
            return val
        base = self.config_dir or os.getcwd()
        return os.path.normpath(os.path.join(base, val))


# Config keys that hold filesystem paths and should be anchored to config_dir
# when given as relative paths.
_PATH_KEYS = (
    "database.path",
    "sources.feed_file",
    "sources.playlist_dir",
    "output.dir",
    "webui.auth_token_file",
    "logging.file",
)


def load_config(
    cli_overrides: dict | None = None,
    config_path: str | None = None,
    env: dict | None = None,
) -> Config:
    """Load config with full precedence chain.

    cli_overrides: flat dotted dict from argparse (highest priority).
    config_path: path to config.yaml (defaults to ./config.yaml).
    env: dict to read env from (defaults to os.environ).
    """
    cfg = _deep_merge(DEFAULTS, {})

    # 1) config.yaml
    path = config_path or "config.yaml"
    config_dir = ""
    if path and os.path.exists(path):
        path = os.path.abspath(path)
        config_dir = os.path.dirname(path)
        with open(path, "r", encoding="utf-8") as f:
            file_cfg = yaml.safe_load(f) or {}
        cfg = _deep_merge(cfg, file_cfg)

    # 2) environment (M3U_*)
    env = env if env is not None else os.environ
    for env_key, dotted in ENV_MAP.items():
        if env_key in env:
            current = cfg
            for p in dotted.split("."):
                if isinstance(current, dict) and p in current:
                    current = current[p]
                else:
                    current = None
                    break
            _set_path(cfg, dotted, _coerce(env[env_key], current))

    # 3) CLI overrides (dotted keys)
    for dotted, value in (cli_overrides or {}).items():
        if value is None:
            continue
        _set_path(cfg, dotted, value)

    # 4) Anchor relative path keys to the config file's directory. This makes
    #    the project relocatable: move the folder, keep config.yaml as the
    #    anchor, and every relative path follows automatically.
    base = config_dir or os.getcwd()
    for dotted in _PATH_KEYS:
        node = cfg
        ok = True
        for p in dotted.split("."):
            if isinstance(node, dict) and p in node:
                node = node[p]
            else:
                ok = False
                break
        if ok and isinstance(node, str) and node and not os.path.isabs(node):
            _set_path(cfg, dotted, os.path.normpath(os.path.join(base, node)))

    return Config(_data=cfg, config_dir=config_dir, config_path=path or "")


def save_config(cfg: "Config", path: str = None):
    """Persist a Config back to disk. If `path` is None, uses cfg.config_path."""
    target = path or cfg.config_path
    if not target:
        raise ValueError("no path given and config has no config_path")
    cfg.save(target)
