"""SQLite engine (stdlib sqlite3, single-file). Implements §18.1, §19, §8.

No SQLAlchemy/Alembic. Pragmas applied per-connection. Schema versioned via a
`config` table key `schema_version` so `init_db` can run migrations idempotently.
"""
from __future__ import annotations

import sqlite3
import os
import gzip
import shutil
from datetime import datetime, timezone

SCHEMA_VERSION = 1

PRAGMAS = [
    "PRAGMA journal_mode = WAL;",
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA cache_size = -8192;",          # 8 MB (§18.1 / §19, fit MemoryMax=400M)
    "PRAGMA temp_store = MEMORY;",
    "PRAGMA journal_size_limit = 6144000;",
    "PRAGMA foreign_keys = ON;",
]

STREAMS_DDL = """
CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    original_url TEXT,
    name TEXT,
    provider_domain TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_path TEXT,
    extinf_raw TEXT,
    attributes JSON,
    is_working BOOLEAN DEFAULT NULL,
    last_checked DATETIME,
    last_working DATETIME,
    consecutive_failures INTEGER DEFAULT 0,
    total_failures INTEGER DEFAULT 0,
    total_successes INTEGER DEFAULT 0,
    blacklist_tier TEXT DEFAULT 'none',
    blacklisted_at DATETIME,
    blacklist_reason TEXT,
    enabled BOOLEAN DEFAULT 1,
    disabled_at DATETIME,
    disabled_reason TEXT,
    disabled_by TEXT,
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    health_score REAL DEFAULT NULL,
    health_tier TEXT DEFAULT NULL
);
"""

INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_streams_provider ON streams(provider_domain);",
    "CREATE INDEX IF NOT EXISTS idx_streams_enabled ON streams(enabled);",
    "CREATE INDEX IF NOT EXISTS idx_streams_blacklist ON streams(blacklist_tier);",
    "CREATE INDEX IF NOT EXISTS idx_streams_working ON streams(is_working);",
    "CREATE INDEX IF NOT EXISTS idx_streams_url_norm ON streams(url);",
]

PROVIDERS_DDL = """
CREATE TABLE IF NOT EXISTS providers (
    domain TEXT PRIMARY KEY,
    enabled BOOLEAN DEFAULT 1,
    disabled_at DATETIME, disabled_reason TEXT, notes TEXT,
    first_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

RUNS_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL, mode TEXT NOT NULL,
    started_at DATETIME NOT NULL, finished_at DATETIME,
    duration_seconds REAL, status TEXT DEFAULT 'running',
    stats_json TEXT, error_message TEXT,
    progress_json TEXT
);
"""

BLACKLIST_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS blacklist_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id INTEGER NOT NULL, url TEXT NOT NULL,
    event_type TEXT NOT NULL, old_tier TEXT, new_tier TEXT,
    reason TEXT, triggered_by TEXT, run_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (stream_id) REFERENCES streams(id)
);
"""

ENABLE_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS enable_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id INTEGER, domain TEXT,
    event_type TEXT NOT NULL, reason TEXT, triggered_by TEXT, run_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

CONFIG_DDL = """
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY, value TEXT NOT NULL,
    description TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    """Thin stdlib sqlite3 wrapper. Single writer connection + read connections."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._writer = None

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        for p in PRAGMAS:
            conn.execute(p)
        return conn

    @property
    def writer(self) -> sqlite3.Connection:
        if self._writer is None:
            self._writer = self._connect()
        return self._writer

    def close(self):
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    def init_db(self, backup: bool = True):
        """Create schema and run migrations. Idempotent."""
        if backup and os.path.exists(self.path):
            self.backup_db()
        c = self.writer
        c.executescript(STREAMS_DDL)
        for idx in INDEX_DDL:
            c.execute(idx)
        c.executescript(PROVIDERS_DDL)
        c.executescript(RUNS_DDL)
        c.executescript(BLACKLIST_EVENTS_DDL)
        c.executescript(ENABLE_EVENTS_DDL)
        c.executescript(CONFIG_DDL)
        c.execute(
            "INSERT OR IGNORE INTO config(key, value, description) VALUES(?,?,?)",
            ("schema_version", str(SCHEMA_VERSION), "DB schema version"),
        )
        c.commit()
        self._run_migrations()

    def _run_migrations(self):
        """Hook for future schema upgrades keyed on stored schema_version."""
        cur = self.writer.execute(
            "SELECT value FROM config WHERE key='schema_version'"
        ).fetchone()
        version = int(cur["value"]) if cur else 0
        # Migration: add quality/health columns if missing (idempotent).
        cols = {r[1] for r in self.writer.execute("PRAGMA table_info(streams)")}
        for col, ctype in (
            ("health_score", "REAL DEFAULT NULL"),
            ("health_tier", "TEXT DEFAULT NULL"),
        ):
            if col not in cols:
                self.writer.execute(f"ALTER TABLE streams ADD COLUMN {col} {ctype}")
        # Migration: add progress_json to runs (idempotent, for Live UI).
        rcols = {r[1] for r in self.writer.execute("PRAGMA table_info(runs)")}
        if "progress_json" not in rcols:
            self.writer.execute("ALTER TABLE runs ADD COLUMN progress_json TEXT")
        if version != SCHEMA_VERSION:
            self.writer.execute(
                "INSERT OR REPLACE INTO config(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self.writer.commit()

    def backup_db(self, output: str | None = None) -> str:
        """Gzip-dump the DB (§22 backup). Returns backup path."""
        os.makedirs("data/backups", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = output or f"data/backups/m3u_{ts}.db.gz"
        # Use sqlite3 .dump via our own connection to avoid shell dependency
        with self._connect() as conn:
            dump = "\n".join(conn.iterdump())
        with gzip.open(out, "wt") as f:
            f.write(dump)
        return out

    def vacuum(self):
        self.writer.execute("VACUUM")
        self.writer.commit()

    # --- convenience helpers used by later phases ---
    def execute(self, sql: str, params=()):
        return self.writer.execute(sql, params)

    def executemany(self, sql: str, seq):
        return self.writer.executemany(sql, seq)

    def commit(self):
        self.writer.commit()

    def query(self, sql: str, params=()):
        return self.writer.execute(sql, params).fetchall()
