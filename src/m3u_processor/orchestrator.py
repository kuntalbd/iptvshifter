"""Orchestration: end-to-end run flow across 3 modes (§3, §5, §18, C1 hybrid).

Flow:
  1. Parse sources (feeds + local dir) -> merge into DB (parser.merge_into_db)
  2. Select eligible streams by mode (§3.1)
  3. Validate each (validator) with per-host semaphore
  4. Apply blacklist transitions (blacklist.apply_result)
  5. Stale-token hybrid (C1): on suspected_expired, re-extract token from
     local source file / cached feed (max_token_refetch_per_feed cap), retry once.
  6. Persist state; emit report.
  7. Checkpointing: progress saved to runs table; SIGINT -> graceful stop.
"""
from __future__ import annotations

import json
import signal
import sys
import os
from datetime import datetime, timezone

from .parser import PlaylistParser, merge_into_db
from .validator import StreamValidator
from .blacklist import apply_result
from .providers import ensure_provider, provider_enabled
from .models import Stream
from .utils import ROTATING_TOKEN_PARAMS


def _is_tokened(url: str) -> bool:
    """True if the URL carries rotating auth/token query params (expiring)."""
    if "?" not in url:
        return False
    q = url.split("?", 1)[1]
    return any(p in q for p in ROTATING_TOKEN_PARAMS)

def _pid_from_run_id(run_id: str):
    """run_id ends with '-<pid>' (see Orchestrator.run). Return int pid or None."""
    if not run_id:
        return None
    tail = run_id.rsplit("-", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _process_alive(pid: int) -> bool:
    """True if a process with this pid currently exists (probe, don't signal)."""
    if not pid:
        return False
    try:
        os.kill(pid, 0)  # ESRCH if dead, permission error if alive but not ours
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, just not ours
    return True


MODE_ELIGIBILITY = {
    "quick": "blacklist_tier='none' AND enabled=1",
    "regular": "blacklist_tier IN ('none','short') AND enabled=1",
    "full": "enabled=1",  # all, regardless of tier
    "refresh": "blacklist_tier='none' AND enabled=1 AND is_working=1",  # tokened working only
}


class Orchestrator:
    def __init__(self, db, config, http_client=None, progress=None):
        self.db = db
        self.config = config
        self.http_client = http_client
        self.progress = progress
        self._stop = False
        from time import time as _t
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + f"{_t():.4f}".split(".")[1][:4] + f"-{os.getpid()}"
        self.stats = {
            "mode": None, "parsed": 0, "unique": 0, "eligible": 0,
            "checked": 0, "working": 0, "failed": 0, "uncheckable": 0,
            "short_added": 0, "recovered": 0, "escalated": 0, "permanent_added": 0,
            "token_refreshed": 0,
        }
        self._install_signals()

    def _install_signals(self):
        try:
            signal.signal(signal.SIGINT, self._on_signal)
            signal.signal(signal.SIGTERM, self._on_signal)
        except (ValueError, AttributeError):
            pass  # not in main thread

    def _on_signal(self, *a):
        self._stop = True

    # --- source ingestion ---
    def ingest_source(self, path, source_type="local", base_url=None):
        parser = PlaylistParser(
            aggregate_subdomains=bool(self.config.get("providers.aggregate_subdomains", True))
        )
        streams = parser.parse_file(path, source_type=source_type, base_url=base_url)
        stats = merge_into_db(self.db, streams, self.run_id)
        # auto-create providers
        for s in streams:
            ensure_provider(self.db, s.provider_domain,
                            self.config.get("providers.aggregate_subdomains", True))
        self.stats["parsed"] += len(streams)
        return stats

    def ingest_feed(self, url):
        # fetch remote feed text, parse in-memory (no file needed)
        import requests
        import time as _time
        last_err = None
        for attempt in range(3):
            try:
                resp = requests.get(url, timeout=30)
                if resp.status_code == 429:
                    # Rate-limited: back off and retry (transient).
                    _time.sleep(5 * (attempt + 1))
                    last_err = "429 Too Many Requests"
                    continue
                resp.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                if getattr(e.response, "status_code", None) == 429 and attempt < 2:
                    _time.sleep(5 * (attempt + 1))
                    last_err = str(e)
                    continue
                raise
        else:
            # exhausted retries — record and skip so one bad feed can't abort run
            self.db.log_error(self.run_id, "feed_ingest_failed",
                             f"{last_err or 'failed'} (skipped after retries): {url}", url)
            return None
        parser = PlaylistParser(
            aggregate_subdomains=bool(self.config.get("providers.aggregate_subdomains", True))
        )
        streams = parser.parse_text(resp.text, source_type="remote",
                                    source_path=url, base_url=url)
        stats = merge_into_db(self.db, streams, self.run_id)
        for s in streams:
            ensure_provider(self.db, s.provider_domain,
                            self.config.get("providers.aggregate_subdomains", True))
        self.stats["parsed"] += len(streams)
        return stats

    # --- selection ---
    def _eligible_rows(self, mode):
        where = MODE_ELIGIBILITY[mode]
        rows = self.db.query(
            "SELECT id, url, original_url, provider_domain, source_type, source_path, "
            "attributes, enabled, total_failures, consecutive_failures, last_working, "
            "consecutive_pass, total_pass, "
            "blacklist_tier, blacklisted_at, blacklist_reason, is_working "
            f"FROM streams WHERE {where}"
        )
        return rows

    # --- main run ---
    def run(self, mode="quick", run_id=None):
        from time import time as _t
        if run_id:
            self.run_id = str(run_id)
        else:
            self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + \
                f"{_t():.5f}".split(".")[1][:5] + f"-{os.getpid()}"
        # Reaper (rule: two runs of the same mode must never both be 'running').
        # A run left in 'running' whose PROCESS IS ACTUALLY DEAD (killed /
        # crashed / interrupted before _finalize) is a zombie that would shadow
        # the real active run in the UI. Stop only those. A 'running' row whose
        # PID is still alive belongs to a genuine concurrent run and must be
        # left untouched — otherwise a scheduled token-refresh would wrongly
        # kill a long-running full pass that merely overlaps it.
        # Liveness check: parse the trailing -<pid> from run_id and probe it.
        try:
            rows = self.db.query(
                "SELECT run_id FROM runs WHERE status='running' AND run_id<>?",
                (self.run_id,),
            )
            for (rid,) in rows:
                pid = _pid_from_run_id(rid)
                if pid and _process_alive(pid):
                    continue  # still running for real -> keep it
                self.db.execute(
                    "UPDATE runs SET status='stopped', finished_at=CURRENT_TIMESTAMP, "
                    "stats_json=json_set(COALESCE(stats_json,'{}'),'$.interrupted',true) "
                    "WHERE run_id=?",
                    (rid,),
                )
            self.db.commit()
        except Exception:
            pass
        self.stats["mode"] = mode
        self.mode = mode

        # ---- single-run guard: only ONE active run at a time ----
        # If another run's process is still alive (live PID), THIS initiation is
        # discarded: record a 'discarded' run entry (so the UI can show the
        # reason) and return without doing any work. The user can stop the
        # active run from the Web UI; a new run can then be started.
        # Runs BEFORE inserting our own row so the discarded record can safely
        # reuse self.run_id (no unique collision with our own 'running' row).
        try:
            live = self.db.query(
                "SELECT run_id, mode FROM runs WHERE status='running' AND run_id<>?",
                (self.run_id,),
            )
            for (rid, rmode) in live:
                pid = _pid_from_run_id(rid)
                if pid and _process_alive(pid):
                    # record this discarded attempt, then bail
                    self.db.execute(
                        "INSERT INTO runs(run_id, mode, started_at, status, stats_json) "
                        "VALUES(?,?,?,?,?)",
                        (self.run_id, mode, datetime.now(timezone.utc).isoformat(),
                         "discarded",
                         json.dumps({"reason": f"another run already active ({rmode})",
                                     "discarded_by": rid})),
                    )
                    self.db.commit()
                    self.stats["discarded"] = True
                    self.stats["discard_reason"] = f"another run already active ({rmode})"
                    return self.stats
        except Exception:
            pass

        # record THIS run as the active one (after the guard, so a discarded
        # attempt does not leave a stray 'running' row behind).
        self.db.execute(
            "INSERT INTO runs(run_id, mode, started_at, status) VALUES(?,?,?,?)",
            (self.run_id, mode, datetime.now(timezone.utc).isoformat(), "running"),
        )
        self.db.commit()

        rows = self._eligible_rows(mode)
        # refresh mode only targets expiring (tokened) URLs. Pre-filter in SQL
        # via LIKE on the rotating-token params (avoids dragging 16k non-tokened
        # rows just to discard them in Python).
        if mode == "refresh":
            token_params = self.config.get("validation.strip_query_params") or list(ROTATING_TOKEN_PARAMS)
            like = " OR ".join(f"original_url LIKE '%{p}=%'" for p in token_params)
            q = (
                "SELECT id, url, original_url, provider_domain, source_type, source_path, "
                "attributes, enabled, total_failures, consecutive_failures, last_working, "
                "consecutive_pass, total_pass, "
                "blacklist_tier, blacklisted_at, blacklist_reason, is_working "
                f"FROM streams WHERE {MODE_ELIGIBILITY[mode]} AND ({like})"
            )
            rows = self.db.query(q)
        self.stats["eligible"] = len(rows)
        self.stats["unique"] = self.db.query("SELECT COUNT(*) FROM streams")[0][0]

        validator = StreamValidator(
            self.config, http_client=self.http_client
        )
        # quick mode = fast latency-only health (no 3s throughput sampling).
        # Full/regular/refresh keep throughput (Option B) for accurate scoring.
        if mode == "quick":
            validator.throughput_check = False

        # Watchdog: if the run ever hangs (e.g. a stuck network/deadlock), dump
        # all thread stacks every 60s so a stall is diagnosable instead of silent.
        import faulthandler as _fh
        try:
            _fh.dump_traceback_later(60, repeat=True, exit=False)
        except Exception:
            pass

        if mode == "refresh":
            # Refresh: plain token re-extraction ONLY. No active/health check.
            # Batched: group eligible streams by `source`, fetch/read each source
            # ONCE, parse once, and update all matching streams. Fixes the old
            # per-stream re-read (N redundant fetches for shared sources) and
            # supports remote-URL sources (C1).
            try:
                total = len(rows)
                self.stats["token_refreshed"] = self._refresh_tokens_batched()
                done = total
                self._persist_progress(done, total)
                # Part B: refresh favorite tokens, then export favorite.*.m3u
                self.stats["favorite_token_refreshed"] = self._refresh_favorite_tokens()
                self._export_favorites_to_out()
                # record last refresh time (audit; scheduling authority = systemd timer)
                self.db.execute(
                    "INSERT OR REPLACE INTO config(key, value) VALUES('last_refresh_at', ?)",
                    (datetime.now(timezone.utc).isoformat(),),
                )
            except Exception as e:  # noqa: BLE001
                self.db.log_error(self.run_id, "fatal_run_error", f"{type(e).__name__}: {e}")
                self.db.execute(
                    "UPDATE runs SET error_message=?, status='error', finished_at=CURRENT_TIMESTAMP WHERE run_id=?",
                    (f"{type(e).__name__}: {e}"[:2000], self.run_id),
                )
                self.db.commit()
                self.stats["error"] = f"{type(e).__name__}: {e}"
                raise
            self._finalize()
            return self.stats

        # ---- Solution A: 2-phase funnel (parallel cheap HEAD -> deep A+B) ----
        try:
            phase1_rows = rows
            total = len(phase1_rows)
            self._persist_progress(0, total)

            # Phase 1 (health=False) — concurrent
            phase1_results = {}
            done_p1 = 0
            for stream, res in validator.validate_batch(
                [self._row_to_stream(r) for r in phase1_rows],
                progress=lambda d, t: self._persist_progress(d, t),
                health=False,
            ):
                if not provider_enabled(self.db, stream):
                    done_p1 += 1
                    continue
                phase1_results[stream.id] = (stream, res)
                if res.ok:
                    pass  # deep-scored in Phase 2
                else:
                    # record the failure transition exactly once
                    self._process_result(stream, res, validator, mode)
                done_p1 += 1

            phase1_ok = [v[0] for v in phase1_results.values() if v[1].ok]
            self.stats["phase1_active"] = len(phase1_ok)
            self._persist_progress(done_p1, total)

            # Phase 2 (health=True) — concurrent, active subset only
            total2 = len(phase1_ok)
            done2 = 0
            for stream, res in validator.validate_batch(
                phase1_ok,
                progress=lambda d, t: self._persist_progress(done_p1 + d, done_p1 + t),
                health=True,
            ):
                self._process_result(stream, res, validator, mode)
                done2 += 1

            self._persist_progress(done_p1 + done2, total + total2)
        except Exception as e:  # noqa: BLE001
            self.db.log_error(self.run_id, "fatal_run_error", f"{type(e).__name__}: {e}")
            self.db.execute(
                "UPDATE runs SET error_message=?, status='error', finished_at=CURRENT_TIMESTAMP WHERE run_id=?",
                (f"{type(e).__name__}: {e}"[:2000], self.run_id),
            )
            self.db.commit()
            self.stats["error"] = f"{type(e).__name__}: {e}"
            raise
        self._finalize()
        return self.stats

    def _persist_progress(self, done: int, total: int):
        """Write current progress to the runs row (for the Live UI)."""
        pct = round(100.0 * done / total, 1) if total else 100.0
        prog = json.dumps({
            "done": done, "total": total, "percent": pct,
            "checked": self.stats.get("checked", 0),
            "working": self.stats.get("working", 0),
            "failed": self.stats.get("failed", 0),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        try:
            self.db.execute(
                "UPDATE runs SET progress_json=? WHERE run_id=?",
                (prog, self.run_id),
            )
            self.db.commit()
        except Exception:
            pass

    def _row_to_stream(self, row) -> Stream:
        s = Stream(
            url=row["url"], original_url=row["original_url"],
            provider_domain=row["provider_domain"], source_type=row["source_type"] or "remote",
            id=row["id"], enabled=bool(row["enabled"]),
            source_path=row["source_path"] if "source_path" in row.keys() else "",
            attributes=json.loads(row["attributes"] or "{}"),
            total_failures=row["total_failures"] or 0,
            consecutive_failures=row["consecutive_failures"] or 0,
            total_pass=row["total_pass"] or 0,
            consecutive_pass=row["consecutive_pass"] or 0,
            last_working=row["last_working"],
            blacklist_tier=row["blacklist_tier"] or "none",
            blacklisted_at=row["blacklisted_at"],
            blacklist_reason=row["blacklist_reason"] or "",
            is_working=bool(row["is_working"]) if row["is_working"] is not None else None,
        )
        return s

    def _process_result(self, stream, res, validator, mode):
        self.stats["checked"] += 1
        if res.uncheckable:
            self.stats["uncheckable"] += 1
            # leave state unchanged; mark uncheckable list later in writers
            self.db.execute(
                "UPDATE streams SET is_working=NULL, last_checked=CURRENT_TIMESTAMP WHERE id=?",
                (stream.id,),
            )
            self.db.commit()
            return

        # NOTE: token re-extraction is intentionally NOT performed here. It runs
        # only in dedicated refresh mode (scheduled every 2h). quick/regular/full
        # modes just validate; expired tokens are rotated by the refresh run.
        transition = apply_result(stream, res.ok, res.suspected_expired,
                                  self.config, run_id=self.run_id)
        self._count_transition(transition)
        # persist (health is only meaningful when we actually measured it)
        health_score = res.health_score if res.ok else None
        health_tier = res.health_tier if res.ok else None
        self.db.execute(
            """UPDATE streams SET is_working=?, last_checked=CURRENT_TIMESTAMP,
               last_working=?, consecutive_failures=?, total_failures=?, total_successes=?,
               consecutive_pass=?, total_pass=?,
               blacklist_tier=?, blacklisted_at=?, blacklist_reason=?, original_url=?,
               health_score=?, health_tier=?, updated_at=CURRENT_TIMESTAMP WHERE id=?""",
            (stream.is_working, stream.last_working, stream.consecutive_failures,
             stream.total_failures, stream.total_successes, stream.consecutive_pass,
             stream.total_pass, stream.blacklist_tier,
             stream.blacklisted_at, stream.blacklist_reason, stream.original_url,
             health_score, health_tier, stream.id),
        )
        if res.ok:
            self.stats["working"] += 1
        else:
            self.stats["failed"] += 1
        self.db.commit()

    def _norm_url(self, url: str) -> str:
        """Token-stripped normalization key (matches regardless of token presence)."""
        params = self.config.get("validation.strip_query_params") or list(ROTATING_TOKEN_PARAMS)
        if "?" not in url:
            return url
        base, q = url.split("?", 1)
        keep = []
        for pair in q.split("&"):
            key = pair.split("=", 1)[0]
            if key not in params:
                keep.append(pair)
        return base + ("?" + "&".join(keep) if keep else "")

    def _refresh_tokens_batched(self):
        """Batched token refresh for refresh mode.

        Groups eligible (tokened) streams by `source`, then fetches/reads each
        unique source EXACTLY ONCE, parses it once, builds a
        norm_url -> tokened_original_url map, and batch-updates every matching
        stream. This fixes the old per-stream re-read (N redundant fetches for
        shared sources) and supports remote-URL sources (not just local files).
        """
        token_params = self.config.get("validation.strip_query_params") or list(ROTATING_TOKEN_PARAMS)
        like = " OR ".join(f"original_url LIKE '%{p}=%'" for p in token_params)
        rows = self.db.query(
            f"SELECT id, url, original_url, source, is_url FROM streams "
            f"WHERE source IS NOT NULL AND source != '' AND ({like})"
        )
        if not rows:
            return 0

        # group stream ids by source
        by_source = {}
        for r in rows:
            by_source.setdefault((r["source"], r["is_url"]), []).append(r)

        refreshed = 0

        for (src, is_url), srows in by_source.items():
            if self._stop:
                break
            # fetch/read the source ONCE (batched: each unique source = 1 fetch)
            text = None
            if is_url:
                try:
                    import requests
                    resp = requests.get(src, timeout=30)
                    resp.raise_for_status()
                    text = resp.text
                except Exception as e:
                    self.db.log_error(self.run_id, "source_fetch_failed",
                                     f"{type(e).__name__}: {e}", src)
                    continue
            else:
                if not os.path.isfile(src):
                    self.db.log_error(self.run_id, "source_missing", "local file not found", src)
                    continue
                try:
                    with open(src, "r", encoding="utf-8", errors="replace") as f:
                        text = f.read()
                except Exception as e:
                    self.db.log_error(self.run_id, "source_read_failed",
                                     f"{type(e).__name__}: {e}", src)
                    continue
            # parse ONCE -> map norm_url -> tokened original_url
            try:
                parser = PlaylistParser(
                    aggregate_subdomains=bool(self.config.get("providers.aggregate_subdomains", True))
                )
                entries = parser.parse_text(text, source_type="remote" if is_url else "local",
                                            source_path=src, base_url=src if is_url else None)
            except Exception:
                continue
            fresh_map = {}
            for e in entries:
                fresh_map[self._norm_url(e.url)] = e.original_url
            # batch-update matching streams
            for r in srows:
                new = fresh_map.get(self._norm_url(r["url"]))
                if new and new != r["original_url"]:
                    self.db.execute(
                        "UPDATE streams SET original_url=? WHERE id=?",
                        (new, r["id"]),
                    )
                    refreshed += 1
        self.db.commit()
        return refreshed

    def _refresh_favorite_tokens(self) -> int:
        """Refresh-mode Part B(1): re-extract fresh tokens for enabled favorites
        that carry a tokened URL, using each favorite's own source (copied from
        the originating stream). Mirrors _refresh_tokens_batched but for the
        favorites table. Returns count of favorites refreshed."""
        from .parser import PlaylistParser
        # Select favorites that have a source to re-extract from. The join key
        # for matching is the tokenless `url` (the tokened playable value lives
        # in `original_url`). NOTE: do NOT filter on `url LIKE '%?%'` here —
        # in production `url` is the normalized tokenless key, so that filter
        # would match nothing and favorites would never refresh.
        rows = self.db.query(
            "SELECT id, url, original_url, source_path, is_url FROM favorites "
            "WHERE is_enabled=1 AND source_path IS NOT NULL AND source_path != ''"
        )
        if not rows:
            return 0
        # group by source
        by_source: dict = {}
        for r in rows:
            if not r["source_path"]:
                continue  # no source -> final URL, nothing to re-extract
            by_source.setdefault((r["source_path"], r["is_url"]), []).append(r)

        refreshed = 0
        for (src, is_url), frows in by_source.items():
            text = None
            if is_url:
                try:
                    import requests
                    resp = requests.get(src, timeout=30)
                    resp.raise_for_status()
                    text = resp.text
                except Exception as e:
                    self.db.log_error(self.run_id, "fav_source_fetch_failed",
                                     f"{type(e).__name__}: {e}", src)
                    continue
            else:
                if not os.path.isfile(src):
                    self.db.log_error(self.run_id, "fav_source_missing",
                                     "local file not found", src)
                    continue
                try:
                    with open(src, "r", encoding="utf-8", errors="replace") as f:
                        text = f.read()
                except Exception as e:
                    self.db.log_error(self.run_id, "fav_source_read_failed",
                                     f"{type(e).__name__}: {e}", src)
                    continue
            try:
                parser = PlaylistParser(
                    aggregate_subdomains=bool(self.config.get("providers.aggregate_subdomains", True))
                )
                entries = parser.parse_text(text, source_type="remote" if is_url else "local",
                                            source_path=src, base_url=src if is_url else None)
            except Exception:
                continue
            fresh_map = {self._norm_url(e.url): e.original_url for e in entries}
            for r in frows:
                new = fresh_map.get(self._norm_url(r["url"]))
                if new and new != r["original_url"]:
                    self.db.execute(
                        "UPDATE favorites SET original_url=? WHERE id=?",
                        (new, r["id"]),
                    )
                    refreshed += 1
        self.db.commit()
        return refreshed

    def _export_favorites_to_out(self):
        """Refresh-mode Part B(2): write favorite.*.m3u (tokened original_url)
        for enabled favorites into output dir, then let publish push it.

        Mirrors write_streams: publishes the tokened `original_url` so favorites
        stay playable (same token-exposure policy as working.m3u, Decision 33).
        `url` is the tokenless fallback key only.
        """
        from .writers import write_favorites
        out = self.config.get("output.dir", "./out")
        rows = self.db.query(
            "SELECT name, url, original_url, "
            "(SELECT GROUP_CONCAT(g.name) FROM favorite_membership m "
            " JOIN favorite_groups g ON g.id=m.group_id WHERE m.favorite_id=favorites.id) AS groups "
            "FROM favorites WHERE is_enabled=1"
        )
        # Publish the tokened `original_url` (mirrors write_streams / Decision 33)
        # so favorite channels actually play. `url` is the tokenless fallback.
        results = write_favorites(
            rows, out,
            formats=self.config.get("output.formats", ["vlc", "kodi", "tivimate"]),
        )
        self.stats["favorite_files"] = list(results.keys())
        return results

    def _count_transition(self, t):
        e = t.get("event")
        if e == "short_added":
            self.stats["short_added"] += 1
        elif e in ("recovered", "recovered_from_permanent"):
            self.stats["recovered"] += 1
        elif e == "escalated":
            self.stats["escalated"] += 1
        elif e == "permanent_added":
            self.stats["permanent_added"] += 1

    def _finalize(self):
        finished = datetime.now(timezone.utc)
        # compute duration from the run's start time
        duration = None
        try:
            row = self.db.query(
                "SELECT started_at FROM runs WHERE run_id=?", (self.run_id,)
            )
            if row and row[0][0]:
                started = datetime.fromisoformat(row[0][0].replace("Z", "+00:00"))
                duration = (finished - started).total_seconds()
        except Exception:
            duration = None
        self.db.execute(
            "UPDATE runs SET finished_at=CURRENT_TIMESTAMP, status=?, "
            "stats_json=?, duration_seconds=? WHERE run_id=?",
            ("stopped" if self._stop else "completed", json.dumps(self.stats),
             duration, self.run_id),
        )
        self.db.commit()
        # Regenerate output playlists from the (now-updated) DB BEFORE publishing,
        # so the pushed playlist reflects this run's results. Output is built from
        # ALL working streams (is_working=1 OR unverified NULL) regardless of the
        # run mode — a quick/refresh run still produces a complete, current
        # playlist. Best-effort: a regeneration failure must not break publish or
        # the run's report.
        try:
            self._regenerate_outputs()
        except Exception as e:  # noqa: BLE001
            self.stats["generate_error"] = f"output regeneration failed: {e}"
        # Auto-publish finished outputs to the public repo after a run. The output
        # dir is copied as-is (publish does NOT regenerate; regeneration already
        # happened above). Best-effort: never let a publish failure break the
        # validation run or its report.
        try:
            from . import publish as _publish
            pub_result = _publish.publish_outputs(self.config, run_id=self.run_id, mode=getattr(self, "mode", ""))
            if not pub_result.get("published") and pub_result.get("error"):
                # surface as a warning in stats for operator visibility
                self.stats["publish_error"] = pub_result["error"]
        except Exception as e:  # noqa: BLE001
            self.stats["publish_error"] = f"publish hook crashed: {e}"

    def _regenerate_outputs(self):
        """Write working playlists (all working streams) from the DB into the
        configured output dir. Under a file lock so a short run finishing
        mid-way through a long run cannot clobber out/ inconsistently.

        Mirrors the CLI `generate-output` command and the web `/api/generate`.
        """
        from .writers import write_streams
        out = self.config.get("output.dir", "./out") + "/working.m3u"
        rows = self.db.query(
            "SELECT url, original_url, attributes, name, provider_domain, "
            "health_tier, health_score FROM streams "
            "WHERE enabled=1 AND blacklist_tier='none' AND (is_working=1 OR is_working IS NULL)"
        )
        results = write_streams(
            rows, out,
            formats=self.config.get("output.formats", ["vlc", "kodi", "tivimate"]),
            categories_cfg=self.config.get("categories"),
            quality_cfg=self.config.get("quality"),
        )
        self.stats["generated_files"] = list(results.keys())

    def report(self) -> dict:
        return self.stats
