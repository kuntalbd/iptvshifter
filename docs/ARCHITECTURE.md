# iptvshifter — Architecture & Business Logic Specification

> **Living document.** Update this file whenever behavior, config, or data
> model changes. Each change should also append/update an ADR (§7) and the
> test matrix (§8).
>
> Scope: the whole `m3u_processor` package — CLI, orchestrator, validator,
> web UI, publish pipeline. Audience: any engineer who must understand or
> modify the system without reading every source file.

---

## 1. System Context (C4 — System level)

iptvshifter is a **self-hosted IPTV playlist processor**. It ingests M3U
playlists from remote feeds / local files, validates that each stream actually
works (HTTP reachability + latency/throughput), de-duplicates, classifies by
category/country/quality, and publishes clean, playable `.m3u` playlists to a
public git repo so end-users can load them in VLC / Kodi / Tivimate.

```
┌────────────┐   feeds.txt / playlist_dir    ┌──────────────────────┐
│  Remote    │ ───────────────────────────▶  │  ingest (parser)     │
│  M3U feeds │                                │  → merge_into_db     │
└────────────┘                                └──────────┬───────────┘
                                                        │ streams table
                                                        ▼
┌────────────┐   run modes (quick/regular/full)  ┌──────────────────────┐
│  Scheduler │ ───────────────────────────────▶  │  Orchestrator +       │
│ (systemd)  │                                    │  Validator (network) │
└────────────┘                                    └──────────┬───────────┘
                                                        │ is_working set
                                                        ▼
┌────────────┐   run --mode refresh             ┌──────────────────────┐
│  Web UI    │ ───────────────────────────────▶  │  Token refresh +      │
│  (FastAPI) │                                    │  publish → git push  │
└────────────┘                                    └──────────┬───────────┘
                                                        │ out/*.m3u
                                                        ▼
                                                 ┌──────────────────────┐
                                                 │  Public GitHub repo   │
                                                 │  (out/ folder)        │
                                                 └──────────────────────┘
```

**Key principle:** the SQLite `streams` table is the single source of truth.
Everything (validation, publish, UI) reads/writes it. The public `out/` git
repo is a *derived* artifact, regenerated every run.

---

## 2. Functional Modules

| Module | Responsibility | Key entry point |
|--------|----------------|-----------------|
| `parser.py` | Parse M3U text → `Stream` objects; dedupe key = normalized url | `parse_file`, `parse_text`, `merge_into_db` |
| `database.py` | SQLite schema + CRUD + blacklist transitions | `Database` |
| `validator.py` | HTTP reachability, latency, throughput, token handling | `StreamValidator.validate_one/batch` |
| `orchestrator.py` | Run modes, selection, result persistence, publish | `Orchestrator.run` |
| `writers.py` | Render `out/*.m3u` (vlc/kodi/tivimate), favorites | `write_streams`, `write_favorites` |
| `publish.py` | Copy `out/` → repo root `out/`, git commit + push | `publish_outputs` |
| `webui/app.py` | FastAPI UI + REST API | `create_app` |
| `__main__.py` | CLI (`run`, `serve`, `publish`, `stats`, …) + **ingest step** | `run` command |

### 2.1 Ingest (where streams enter the DB)
- Triggered **only** by the CLI `run` command (`__main__.py:144-164`) when
  `mode != "refresh"`.
- Reads `sources.feed_file` (one URL per line) → `ingest_feed(url)`.
- Reads `sources.playlist_dir` (glob `file_patterns`) → `ingest_source(file)`.
- `merge_into_db` inserts new (`is_working` defaults to `NULL`) and upgrades
  tokened `original_url` on conflict (F11 winner / C3 multi-token).
- **Web UI `/api/run` does NOT ingest** — it calls `orch.run()` directly,
  bypassing the CLI ingest step. Known limitation (see §7 ADR-009).

---

## 3. Run Modes — Behavior Matrix

This is the contract. Any deviation is a bug.

| Mode | Ingest feeds? | Selects for validation | Validates? | Sets `is_working`? | Refreshes tokens? | Writes playlists? |
|------|---------------|------------------------|------------|--------------------|-------------------|-------------------|
| `quick` | ✅ (CLI only) | `blacklist_tier='none' AND enabled=1` (any `is_working`) | ✅ latency-only (no throughput) | ✅ | ❌ | ✅ (via `_finalize`) |
| `regular` | ✅ (CLI only) | `blacklist_tier IN ('none','short') AND enabled=1` | ✅ full | ✅ | ❌ | ✅ |
| `full` | ✅ (CLI only) | `enabled=1` (all tiers) | ✅ full | ✅ | ❌ | ✅ |
| `refresh` | ❌ | `blacklist_tier='none' AND enabled=1 AND is_working=1` + tokened | ❌ (token re-extract only) | ❌ | ✅ | ✅ (favorites + working) |

**Critical rules**
- `is_working` is set **only** by `quick`/`regular`/`full` validation.
  `refresh` never touches it.
- `refresh` only targets rows that already have `is_working=1` **and** a
  rotating token param (`md5`/`expires`/…). It re-extracts fresh tokens from
  the source playlist.
- New/unvalidated streams (`is_working IS NULL`) are **only ever validated**
  by `quick`/`regular`/`full`. If those modes never run, NULL streams stay
  NULL forever — yet `write_streams` still publishes them (see §4).
- **Validation is batched & persisted progressively**, NOT all-at-once at the
  end. A long run (15k+ streams) takes 20-30 min; progress shows in the UI
  (`/live`). Mid-run the DB may still show many `is_working IS NULL` rows —
  that is NORMAL until the run reaches those rows and commits them. Do NOT
  assume the run is hung/stuck just because `is_working` looks empty mid-run.
- A large `quick` run over 15k+ mixed-quality streams legitimately takes
  ~25 min (per-host timeouts on dead hosts dominate). Plan scheduler windows
  accordingly.

---

## 4. Publish / Output Pipeline

`write_streams` (writers.py) renders `out/working.m3u` (+ kodi/tivimate) from
`streams WHERE enabled=1 AND blacklist_tier='none' AND (is_working=1 OR
is_working IS NULL)`.

> **Design gap (ADR-009):** `IS NULL` is allowed, so unvalidated streams
> appear in the published playlist even though they were never tested.
> Decision pending: either exclude NULL, or guarantee a validation run before
> publish. **Operational guidance:** the `quick`/`regular`/`full` scheduler jobs
> MUST run on a cadence (e.g. `quick-run` timer) so new ingest rows get validated
> and `is_working` is set. A `refresh`-only schedule leaves new rows NULL forever
> (they are still published, untested) — this is the cause of the "why 2685 NULL"
> investigation (2026-08-17).

`write_favorites` renders `out/favorite.*.m3u` from `favorites WHERE
is_enabled=1`. Per **ADR-008 (Option B)**, the published favorite URL uses the
**tokened `original_url`** (mirrors `write_streams`) so tokens survive.

`publish_outputs` (publish.py):
- Copies `output.dir` → repo-root `out/`.
- **Content-hash / hash guard:** if the output hash is unchanged since the last
  publish, it skips the commit (stops commit storms). Hash state lives in
  `output.dir/.last_publish_hash` (private, never pushed).
- Audit log line: `[publish] run_id=.. mode=.. source=.. hash=.. changed=..`
- Commits + pushes via throwaway `GIT_ASKPASS` (creds from `auth_file`/env,
  never logged).

---

## 5. Data Model (core tables)

```
streams(
  id PK, url TEXT (tokenless key), original_url TEXT (tokened, playable),
  name, provider_domain, source_type, source_path, source, is_url,
  extinf_raw TEXT, attributes JSON, enabled, blacklist_tier, is_working NULL|0|1,
  last_checked, last_working, consecutive_failures, total_failures,
  total_pass, consecutive_pass, total_successes, health_score, health_tier,
  first_seen, updated_at
)
runs(run_id PK, mode, started_at, finished_at, status, stats_json, error_message)
favorites(id PK, name, url UNIQUE, original_url, source_path, is_url,
  extinf_raw, attributes JSON, is_enabled, is_working NULL|0|1,
  last_working, consecutive_failures, total_failures,
  consecutive_pass, total_pass, total_successes, last_checked, …)
favorite_groups(id PK, name, …)
favorite_membership(favorite_id, group_id)
providers(domain PK, enabled, state, …)
config(key PK, value)        -- schema_version, last_refresh_at
run_errors, blacklist_events, enable_events  -- audit logs
```

---

## 6. Configuration Reference (`prod/config.yaml`)

| Section | Key | Meaning | Default / note |
|---------|-----|---------|----------------|
| `database` | `path` | SQLite file | must be absolute |
| | `backup_on_start` | gzip backup on init | |
| `sources` | `feed_file` | newline URL list | ingested by CLI `run` (not UI) |
| | `playlist_dir` | local `.m3u` dir | ingested by CLI `run` |
| | `recursive_scan`, `file_patterns` | glob rules | |
| `output` | `dir` | working output dir (private) | copied to repo `out/` |
| | `formats` | vlc/kodi/tivimate | |
| | `sort_by`, `generate_aux_files` | grouping, healthy file | |
| `validation` | `mode` | default run mode | quick |
| | `workers`, `max_concurrent` | HTTP concurrency | 150 / 40 |
| | `timeout_connect/read`, `retries`, `backoff` | network bounds | |
| | `per_host_limit` | per-host semaphore | 8 |
| | `strip_query_params` | token params for dedup/refresh | md5, expires, token, … |
| | `token_refresh` | re-extract expired tokens | true |
| `blacklist` | `short_threshold`, `permanent_*` | auto-blacklist rules | |
| `quality` | `healthy_max_ms`, `throughput_min_kbps` | scoring | |
| `providers` | `aggregate_subdomains`, `auto_create` | provider handling | |
| `webui` | `host/port/auth_token_file` | UI bind | |
| `scheduler` | `jobs[]` | systemd timer jobs | token-refresh(refresh), quick-run(quick) |
| `publish` | `enabled`, `git.*` | git push target | auth_file = `.env` |
| `categories` | `genre`, `country` | classification keywords | Bangla labels |

---

## 7. Architecture Decision Records (ADRs)

- **ADR-001** (db is source of truth): all state in SQLite; `out/` is derived.
- **ADR-002** (tokenless `url` as dedupe key): stable identity despite token
  rotation; `original_url` carries the live token.
- **ADR-003** (C3 multi-token): keep both tokened variants as separate rows
  rather than discard a possibly-working token.
- **ADR-004** (mode split): `quick`=latency-only, `regular`/`full`=throughput,
  `refresh`=token-only (no health check).
- **ADR-005** (publish only `is_working=1 OR NULL`): unvalidated streams
  published too — **see gap §4**.
- **ADR-008 (Option B)**: favorites publish **tokened `original_url`** (not
  tokenless), mirroring `write_streams`.
- **ADR-009 (OPEN)**: Web UI `/api/run` bypasses ingest → empty-DB UI run
  does nothing. Either (a) make UI ingest, or (b) document that UI only
  re-validates existing rows. **Not yet fixed.**
- **ADR-010**: content-hash guard in `publish_outputs` stops commit storms
  when output is unchanged.
- **ADR-011**: test-suite loads `examples/config.example.yaml` with
  `publish: disabled` + `conftest.py` strips `GITHUB_*` env, so `pytest` /
  `hermes verify` never pushes to the real repo.
- **ADR-012 (SEGV-proof validation)**: network validation runs in **child
  processes** (`validation.isolate_subprocess`, default True). A native SEGV
  in the TLS/DNS stack (OpenSSL 3.5 + glibc resolver under thread concurrency)
  previously killed the whole pipeline (exit 139, partial data, ~75% stall).
  Now each chunk is a separate process; a crash/timeout marks that chunk as
  failed and the run continues. Root cause of the 2026-08-17/18 SEGVs.
- **ADR-013 (structured logging)**: all modules use `logging_utils.get_logger`
  (level/file/json via config `logging:`), replacing ad-hoc `print()`. The
  validator also arms `socket.setdefaulttimeout` + `faulthandler` as native
  crash/stall safety nets.
- **ADR-014 (hang-proof batch)**: `validate_batch` now acquires the global
  network semaphore in the **abandonable pool worker** (not the unkillable
  daemon thread), with a hard wall-clock per-link deadline — so a native stall
  can never exhaust the semaphore and deadlock the run.

---

## 8. Test Matrix (end-to-end)

| TC | Name | Precondition | Action | Expected | Status |
|----|------|--------------|--------|----------|--------|
| TC-1 | CLI ingest+validate | empty DB | `run --mode quick` | parsed>0, checked>0, is_working set (not all NULL), out/working.m3u written | ✅ PASS (small: 5 parsed/4 checked/1 working/3 failed/0 NULL; SCALE: 15435 checked, 764 working, 14661 failed, exit 0, no SEGV/no hang; subprocess isolation spawn+parallel+hard_timeout+os._exit) |
| TC-2 | Web UI run ingests | empty DB | POST /api/run {quick} | UI run ingests + validates (previously ingested nothing) | ✅ FIXED (worker now ingests feed_file+playlist_dir before orch.run; verified: empty small DB 0→4 streams after API call, 1 working/3 failed) |
| TC-3 | refresh no-ingest | tokened rows exist | `run --mode refresh` | no new rows; token_refreshed≥0 | ✅ PASS (total 15435 unchanged; eligible=15, checked=0, token_refreshed=10 — refresh re-tokens only, no ingest) |
| TC-4 | generate-output | working rows | CLI `generate-output` | out/working.* written | ✅ PASS (working.m3u 1683 lines; vlc/kodi/tivimate + healthy variants written) |
| TC-5 | publish pushes | out/ changed | CLI `publish` | git commit+push, no secret leak | ✅ PASS (verified 2026-08-17; auto-publisher live, pushes to public origin/main) |
| TC-6 | UI pages load | service up | GET /,/streams,/favorites,… | all 200 | ✅ PASS (all 11 SPA routes + 6 API endpoints return 200) |
| TC-7 | favorites CRUD | streams exist | POST /api/favorites/add-existing | row created; in out/favorite.m3u | ✅ PASS (add→id=1, list shows, validate-now checked 1) |
| TC-8 | favorite validate-now | favorite exists | POST /api/favorites/validate-now | is_working updated | ✅ PASS (validate-now returns {ok,checked:1}) |
| TC-9 | CLI info cmds | any | `stats`,`list-feeds`,`list-providers` | non-error, reflects DB | ✅ PASS (partial) |
| TC-10 | Option B token publish | favorites set | refresh+generate | favorite.m3u uses tokened original_url | ✅ PASS (intended — ADR-008; test_phase14 enforces) |

Legend: ✅ PASS · ❌ KNOWN FAIL · ⏳ pending

---

## 9. Operating Notes

- **Scheduler**: `token-refresh.timer` (refresh, every 2h) + `quick-run.timer`
  (quick, every 2 days, 03:00). Web UI "Run" button also starts runs.
- **Ingest only via CLI**: to populate from feeds, run
  `python -m m3u_processor --config prod/config.yaml run --mode quick`
  (Web UI alone will NOT pull new feeds — ADR-009).
- **Clean DB**: `DELETE FROM` all tables + `sqlite_sequence`, then re-ingest
  via CLI. `id` restarts at 1.
- **Secrets**: GitHub PAT in `/bd/soft-data/.env/github/.env`; never in repo.
```
