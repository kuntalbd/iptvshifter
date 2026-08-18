# End-to-End Test Plan — iptvshifter

Goal: verify the FULL pipeline works from empty DB, via real feeds, through
validation, publish, and the Web UI — not just unit tests.

## Critical gap found (2026-08-17)
Web UI `/api/run` calls `orch.run(mode)` directly, BYPASSING the ingest step
that lives in `__main__.py` (`run` command ingests feeds/playlists only when
`mode != "refresh"`). So a Web-UI "quick" run validates the EXISTING streams
table but never ingests new feeds. With an empty DB, a Web-UI run does nothing.

## Test cases (TC)

### TC-1: CLI `run --mode quick` ingests + validates (the real ingest path)
- Pre: empty streams table (just cleaned).
- Action: `python -m m3u_processor --config prod/config.yaml run --mode quick`
- Expect:
  - `parsed` > 0 (feeds.txt 6 feeds ingested)
  - `checked` > 0 (validation ran)
  - streams table populated, ids start at 1
  - is_working set (1 or 0) on ingested rows — NOT all NULL
  - out/working.m3u regenerated with playable (tokened) urls

### TC-2: Web UI full flow (the bug we found)
- Pre: empty DB.
- Action: POST /api/run {mode:quick} from browser/UI.
- Expect: streams table stays EMPTY (reproduces the bug — UI does NOT ingest).
- This is a KNOWN FAILURE to document; fix = make Web UI ingest too.

### TC-3: `run --mode refresh` does NOT ingest, only refreshes tokens
- Pre: streams table has tokened rows.
- Action: `run --mode refresh`
- Expect: no new rows inserted; token_refreshed >= 0; out/favorite.* written.

### TC-4: `generate-output` writes playlists (no network)
- Action: CLI `generate-output`
- Expect: out/working.m3u + kodi/tivimate written; line count == working rows.

### TC-5: CLI `publish` copies out/ to repo out/ and pushes (git)
- Action: CLI `publish`
- Expect: git commit + push to origin/main; no secret leak.

### TC-6: Web UI pages load (HTTP 200)
- Action: GET /, /streams, /providers, /favorites, /run, /runs, /settings
- Expect: 200 for each.

### TC-7: Web UI favorites CRUD (add / add-existing / edit / delete / set-enabled)
- Action: POST /api/favorites/add-existing with a real stream id.
- Expect: favorites row created; out/favorite.m3u includes it after generate.

### TC-8: Web UI favorite validate-now
- Action: POST /api/favorites/validate-now {include_disabled:true}
- Expect: favorite is_working updated.

### TC-9: stats / list-feeds / list-providers CLI
- Action: `stats`, `list-feeds`, `list-providers`
- Expect: non-error output reflecting current DB.

### TC-10: `refresh` token publish uses tokened original_url (Option B)
- Action: generate after refresh; inspect favorite.m3u
- Expect: favorite url = tokened original_url (not tokenless).

## Execution order
1. TC-1 (CLI ingest) — the foundation. If this fails, nothing else matters.
2. TC-2 (Web UI ingest bug) — document.
3. TC-3..TC-10 — dependent flows.

## Verdict format
Each TC: PASS / FAIL / KNOWN-FAIL with evidence (cmd + actual output).
