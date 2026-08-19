"""SQLite engine (stdlib sqlite3, single-file). Implements §18.1, §19, §8.

No SQLAlchemy/Alembic. Pragmas applied per-connection. Schema versioned via a
`config` table key `schema_version` so `init_db` can run migrations idempotently.
"""
from __future__ import annotations

import sqlite3
import os
import gzip
import shutil
import json
from datetime import datetime, timezone

from .logging_utils import get_logger as _get_logger

_LOG = _get_logger("m3u.database")

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
    source TEXT,                -- specific source: remote feed URL or local playlist path (NOT feeds.txt)
    is_url BOOLEAN DEFAULT 0,   -- 1 if `source` is a remote URL, 0 if local file
    extinf_raw TEXT,
    attributes JSON,
    is_working BOOLEAN DEFAULT NULL,
    last_checked DATETIME,
    last_working DATETIME,
    consecutive_failures INTEGER DEFAULT 0,
    total_failures INTEGER DEFAULT 0,
    consecutive_pass INTEGER DEFAULT 0,
    total_pass INTEGER DEFAULT 0,
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

RUN_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS run_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    occurred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    error_type TEXT NOT NULL,
    message TEXT,
    source TEXT
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

FAVORITES_DDL = """
CREATE TABLE IF NOT EXISTS favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT DEFAULT '',
    url TEXT NOT NULL,
    original_url TEXT DEFAULT '',
    source_path TEXT,
    is_url BOOLEAN DEFAULT 0,
    extinf_raw TEXT,
    attributes JSON,
    is_enabled BOOLEAN DEFAULT 1,
    is_working BOOLEAN DEFAULT NULL,
    is_working_checked DATETIME,
    last_working DATETIME,
    consecutive_failures INTEGER DEFAULT 0,
    total_failures INTEGER DEFAULT 0,
    consecutive_pass INTEGER DEFAULT 0,
    total_pass INTEGER DEFAULT 0,
    total_successes INTEGER DEFAULT 0,
    last_checked DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

FAVORITE_GROUPS_DDL = """
CREATE TABLE IF NOT EXISTS favorite_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);
"""

FAVORITE_MEMBERSHIP_DDL = """
CREATE TABLE IF NOT EXISTS favorite_membership (
    favorite_id INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    PRIMARY KEY (favorite_id, group_id),
    FOREIGN KEY (favorite_id) REFERENCES favorites(id) ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES favorite_groups(id) ON DELETE CASCADE
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
            try:
                self.backup_db()
            except Exception as e:  # noqa: BLE001
                _LOG.warning("backup before init failed (continuing) path=%s err=%s",
                             self.path, e)
        c = self.writer
        c.executescript(STREAMS_DDL)
        for idx in INDEX_DDL:
            c.execute(idx)
        c.executescript(PROVIDERS_DDL)
        c.executescript(RUNS_DDL)
        c.executescript(BLACKLIST_EVENTS_DDL)
        c.executescript(ENABLE_EVENTS_DDL)
        c.executescript(FAVORITES_DDL)
        c.executescript(RUN_ERRORS_DDL)
        c.executescript(FAVORITE_GROUPS_DDL)
        c.executescript(FAVORITE_MEMBERSHIP_DDL)
        c.executescript(CONFIG_DDL)
        c.execute(
            "INSERT OR IGNORE INTO config(key, value, description) VALUES(?,?,?)",
            ("schema_version", str(SCHEMA_VERSION), "DB schema version"),
        )
        c.commit()
        self._run_migrations()
        _LOG.info("init_db path=%s schema_version=%s", self.path, SCHEMA_VERSION)

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
            ("consecutive_pass", "INTEGER DEFAULT 0"),
            ("total_pass", "INTEGER DEFAULT 0"),
        ):
            if col not in cols:
                self.writer.execute(f"ALTER TABLE streams ADD COLUMN {col} {ctype}")
                _LOG.debug("migration streams +col %s", col)
        # Migration: add progress_json to runs (idempotent, for Live UI).
        rcols = {r[1] for r in self.writer.execute("PRAGMA table_info(runs)")}
        if "progress_json" not in rcols:
            self.writer.execute("ALTER TABLE runs ADD COLUMN progress_json TEXT")
        # Migration: add source/is_url columns for batched token refresh
        scols = {r[1] for r in self.writer.execute("PRAGMA table_info(streams)")}
        if "source" not in scols:
            self.writer.execute("ALTER TABLE streams ADD COLUMN source TEXT")
            self.writer.execute("ALTER TABLE streams ADD COLUMN is_url BOOLEAN DEFAULT 0")
            # backfill from existing source_path/source_type
            self.writer.execute(
                "UPDATE streams SET source = source_path WHERE source IS NULL "
                "AND source_path IS NOT NULL AND source_path != ''")
            self.writer.execute(
                "UPDATE streams SET is_url = 1 WHERE source IS NOT NULL "
                "AND source_type = 'remote'")
        # Migration: favorites schema alignment (drop deprecated cols, add new).
        # - rename origin_url -> original_url if needed
        # - drop token_refresh_enabled, token_expires_at
        # - add source_path, is_url, extinf_raw, attributes, consecutive_pass,
        #   total_pass, is_working_checked
        fcols = {r[1] for r in self.writer.execute("PRAGMA table_info(favorites)")}
        if "origin_url" in fcols and "original_url" not in fcols:
            self.writer.execute("ALTER TABLE favorites RENAME COLUMN origin_url TO original_url")
            fcols = {r[1] for r in self.writer.execute("PRAGMA table_info(favorites)")}
        for dcol in ("token_refresh_enabled", "token_expires_at", "group_title"):
            if dcol in fcols:
                self.writer.execute(f"ALTER TABLE favorites DROP COLUMN {dcol}")
        fav_add = {
            "source_path": "TEXT",
            "is_url": "BOOLEAN DEFAULT 0",
            "extinf_raw": "TEXT",
            "attributes": "JSON",
            "consecutive_pass": "INTEGER DEFAULT 0",
            "total_pass": "INTEGER DEFAULT 0",
            "is_working_checked": "DATETIME",
        }
        for col, ctype in fav_add.items():
            if col not in fcols:
                self.writer.execute(f"ALTER TABLE favorites ADD COLUMN {col} {ctype}")
        # Migration: favorites.url must be unique (star toggle / /api/streams join
        # assume one favorite per stream). Dedup existing rows (keep lowest id)
        # before creating the UNIQUE index, so a legacy DB with duplicates
        # doesn't fail the migration.
        f_dups = self.writer.execute(
            "SELECT MIN(id) AS keep_id, url FROM favorites "
            "GROUP BY url HAVING COUNT(*) > 1").fetchall()
        for d in f_dups:
            self.writer.execute(
                "DELETE FROM favorites WHERE url=? AND id != ?",
                (d["url"], d["keep_id"]))
        if f_dups:
            self.writer.commit()
        f_idx = {r[1] for r in self.writer.execute(
            "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='favorites'")}
        if "idx_favorites_url" not in f_idx:
            self.writer.execute("CREATE UNIQUE INDEX idx_favorites_url ON favorites(url)")
        if version != SCHEMA_VERSION:
            self.writer.execute(
                "INSERT OR REPLACE INTO config(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self.writer.commit()
            _LOG.info("migrations applied schema_version %s -> %s", version, SCHEMA_VERSION)

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
        _LOG.info("backup wrote=%s", out)
        return out

    def vacuum(self):
        self.writer.execute("VACUUM")
        self.writer.commit()
        _LOG.info("vacuum path=%s", self.path)

    # --- convenience helpers used by later phases ---
    def execute(self, sql: str, params=()):
        return self.writer.execute(sql, params)

    def executemany(self, sql: str, seq):
        return self.writer.executemany(sql, seq)

    def commit(self):
        self.writer.commit()

    def query(self, sql: str, params=()):
        return self.writer.execute(sql, params).fetchall()

    # --- favorites subsystem (separate from main streams pipeline) ---
    def favorite_add(self, name, url, original_url="",
                     source_path="", is_url=0, extinf_raw="", attributes=None,
                     is_enabled=1):
        """Add a manual favorite. `source_path`/`is_url` are optional: if a
        source is given AND the url carries a token query string, refresh mode
        can re-extract a fresh token from that source."""
        c = self.writer
        existing = c.execute(
            "SELECT id FROM favorites WHERE url=?", (url,)).fetchone()
        if existing:
            return existing["id"]
        cur = c.execute(
            "INSERT INTO favorites(name, url, original_url, "
            "source_path, is_url, extinf_raw, attributes, is_enabled) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (name or "", url, original_url or "",
             source_path or "", 1 if is_url else 0, extinf_raw or "",
             json.dumps(attributes or {}), 1 if is_enabled else 0),
        )
        fid = cur.lastrowid
        self.writer.commit()
        return fid

    def favorite_add_existing(self, stream_url, name="",
                              source_path=None, is_url=None):
        """Add a favorite from an existing streams row, copying its tokened
        origin + source (so refresh can re-extract the token later)."""
        row = self.writer.execute(
            "SELECT url, original_url, source_path, is_url, extinf_raw, attributes "
            "FROM streams WHERE url=? OR original_url=?",
            (stream_url, stream_url),
        ).fetchone()
        if not row:
            return None
        url = row["url"]
        orig = row["original_url"] or ""
        sp = source_path if source_path is not None else (row["source_path"] or "")
        iu = is_url if is_url is not None else (row["is_url"] or 0)
        try:
            attrs = json.loads(row["attributes"] or "{}")
        except Exception:
            attrs = {}
        return self.favorite_add(
            name=name or "",
            url=url,
            original_url=orig,
            source_path=sp,
            is_url=iu,
            extinf_raw=row["extinf_raw"] or "",
            attributes=attrs,
            is_enabled=1,
        )

    def favorite_list(self, group="", working="", q=""):
        sql = ("SELECT f.*, GROUP_CONCAT(g.name) AS groups "
               "FROM favorites f "
               "LEFT JOIN favorite_membership m ON m.favorite_id=f.id "
               "LEFT JOIN favorite_groups g ON g.id=m.group_id ")
        where, params = [], []
        if group:
            sql += ("WHERE f.id IN (SELECT favorite_id FROM favorite_membership mm "
                    "JOIN favorite_groups gg ON gg.id=mm.group_id WHERE gg.name=?) ")
            params.append(group)
            where.append("1")
        if working:
            if working == "working":
                where.append("f.is_working=1")
            elif working == "notworking":
                where.append("f.is_working=0")
            elif working == "unchecked":
                where.append("f.is_working IS NULL")
        if q:
            where.append("(f.name LIKE ? OR f.url LIKE ?)")
            params += [f"%{q}%", f"%{q}%"]
        if where:
            prefix = "AND" if group else "WHERE"
            sql += f" {prefix} " + " AND ".join(where) + " "
        sql += "GROUP BY f.id ORDER BY f.name"
        return self.writer.execute(sql, params).fetchall()

    def favorite_set_enabled(self, fid, enabled):
        self.writer.execute(
            "UPDATE favorites SET is_enabled=?, updated_at=CURRENT_TIMESTAMP "
            "WHERE id=?", (1 if enabled else 0, fid))
        self.writer.commit()

    def favorite_delete(self, fid):
        self.writer.execute("DELETE FROM favorites WHERE id=?", (fid,))
        self.writer.execute(
            "DELETE FROM favorite_groups WHERE id NOT IN "
            "(SELECT DISTINCT group_id FROM favorite_membership)")
        self.writer.commit()

    def favorite_edit(self, fid, name=None, source_path=None,
                      is_url=None, is_enabled=None):
        """Edit mutable fields of a favorite. None values are left unchanged."""
        sets, params = [], []
        if name is not None:
            sets.append("name=?"); params.append(name or "")
        if source_path is not None:
            sets.append("source_path=?"); params.append(source_path or "")
        if is_url is not None:
            sets.append("is_url=?"); params.append(1 if is_url else 0)
        if is_enabled is not None:
            sets.append("is_enabled=?"); params.append(1 if is_enabled else 0)
        if not sets:
            return
        sets.append("updated_at=CURRENT_TIMESTAMP")
        params.append(fid)
        self.writer.execute(
            f"UPDATE favorites SET {', '.join(sets)} WHERE id=?", params)
        self.writer.commit()

    def favorite_record_result(self, fid, ok: bool):
        c = self.writer
        if ok:
            c.execute(
                "UPDATE favorites SET is_working=1, last_working=CURRENT_TIMESTAMP, "
                "last_checked=CURRENT_TIMESTAMP, total_successes=total_successes+1, "
                "consecutive_pass=consecutive_pass+1, total_pass=total_pass+1, "
                "consecutive_failures=0, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (fid,))
        else:
            c.execute(
                "UPDATE favorites SET is_working=0, last_checked=CURRENT_TIMESTAMP, "
                "total_failures=total_failures+1, consecutive_failures=consecutive_failures+1, "
                "consecutive_pass=0, updated_at=CURRENT_TIMESTAMP WHERE id=?", (fid,))
        c.commit()

    def favorite_groups(self):
        return self.writer.execute(
            "SELECT id, name FROM favorite_groups ORDER BY name").fetchall()

    def favorite_set_group(self, fids, group_name):
        """Assign every given favorite to `group_name` (replaces its group
        membership). An empty `group_name` removes membership entirely."""
        c = self.writer
        for fid in fids:
            c.execute("DELETE FROM favorite_membership WHERE favorite_id=?", (fid,))
        if group_name:
            g = c.execute("SELECT id FROM favorite_groups WHERE name=?",
                          (group_name,)).fetchone()
            if not g:
                cur = c.execute("INSERT INTO favorite_groups(name) VALUES(?)", (group_name,))
                gid = cur.lastrowid
            else:
                gid = g["id"]
            for fid in fids:
                c.execute("INSERT OR IGNORE INTO favorite_membership(favorite_id, group_id) "
                          "VALUES(?,?)", (fid, gid))
        c.commit()

    # --- run error log (run-scoped; surfaced in the Web UI) ---
    MAX_ERRORS_PER_RUN = 200  # hard cap so a mass-failure run can't flood the table

    def log_error(self, run_id, error_type, message="", source=""):
        """Record a non-fatal error that occurred during a run (e.g. a source
        fetch failure / rate-limit / timeout). Fatal run errors are captured in
        runs.error_message; this table captures per-event detail. Capped per run
        so a mass-failure run (thousands of tokened sources all down) can't flood
        the table or trigger a commit storm."""
        rid = str(run_id)
        try:
            count = self.writer.execute(
                "SELECT COUNT(*) FROM run_errors WHERE run_id=?", (rid,)
            ).fetchone()[0]
        except Exception:
            count = 0
        if count >= self.MAX_ERRORS_PER_RUN:
            return
        self.writer.execute(
            "INSERT INTO run_errors(run_id, error_type, message, source) "
            "VALUES(?,?,?,?)",
            (rid, str(error_type), str(message)[:2000], str(source)[:500]),
        )
        self.writer.commit()
        _LOG.debug("run error logged run_id=%s type=%s source=%s",
                   rid, error_type, source)

    def get_run_errors(self, run_id=None, limit=200):
        """Return recent run errors. If run_id is given, only that run's errors."""
        if run_id:
            rows = self.query(
                "SELECT id, run_id, occurred_at, error_type, message, source "
                "FROM run_errors WHERE run_id=? ORDER BY id DESC LIMIT ?",
                (str(run_id), limit))
        else:
            rows = self.query(
                "SELECT id, run_id, occurred_at, error_type, message, source "
                "FROM run_errors ORDER BY id DESC LIMIT ?",
                (limit,))
        return rows
