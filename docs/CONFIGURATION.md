# Configuration Guide — M3U Playlist Processor v1.2

> Every key in `config.yaml`, its default, and what it does. Precedence:
> **CLI flag > environment (`M3U_*`) > config.yaml > built-in default** (§18.4).

Copy `config.yaml.example` → `config.yaml` and edit. Unknown keys are ignored;
missing keys fall back to built-in defaults.

---

## Top-level sections
```
database      # SQLite location + backups
sources       # where playlists come from
output        # generated playlist files
validation    # HTTP probing behavior
blacklist     # tiered ban thresholds
providers     # domain aggregation + gating
categories    # group normalization (2026-08-15)
webui         # dashboard host/port/auth
scheduler     # when to run
logging       # log file + level
```

---

## database
| Key | Default | Notes |
|-----|---------|-------|
| `path` | `./data/m3u.db` | SQLite file. Parent dir auto-created. |
| `backup_on_start` | `true` | gzip-copy DB before each run. |
| `backup_keep` | `7` | Keep last N backups in `data/backups/`. |

---

## sources
| Key | Default | Notes |
|-----|---------|-------|
| `feed_file` | `./feeds.txt` | One playlist URL or local path per line. |
| `playlist_dir` | `./playlists` | Local `.m3u` files ingested on run. |
| `recursive_scan` | `true` | Recurse into subdirs of `playlist_dir`. |
| `file_patterns` | `["*.m3u","*.m3u8","*.txt"]` | Which files to ingest. |

---

## output
| Key | Default | Notes |
|-----|---------|-------|
| `dir` | `./output` | Where playlists are written. |
| `working_filename` | `working` | Base name; `.m3u`/`.kodi.m3u`/`.tivimate.m3u` appended. |
| `formats` | `["vlc","kodi","tivimate"]` | Which player variants to generate. |
| `generate_aux_files` | `true` | Write the 3 variants. |
| `sort_by` | `group_title` | `group_title` \| `name` \| `domain`. |
| `uncategorized_label` | `Uncategorized` | Group for streams with no group-title. |

Only `enabled` + (`is_working` OR `uncheckable`) + `blacklist_tier='none'`
streams are included (§7.1). Output is **categorized into a single file** with
`# === Group ===` section headers — see `categories` below.

---

## validation  ← the performance/accuracy knobs
| Key | Default | Notes |
|-----|---------|-------|
| `mode` | `quick` | Default mode if `run` is called without `--mode`. |
| `workers` | `20` | Concurrent validation threads. |
| `max_workers` | `50` | Hard cap. |
| `timeout_connect` | `10` | Seconds to establish TCP/TLS. |
| `timeout_read` | `15` | Seconds to receive first bytes. |
| `retries` | `2` | Retries on 5xx/timeout only (4xx = deterministic, no retry). |
| `backoff` | `[2,4,8]` | Seconds between retries. |
| `per_host_limit` | `5` | Max concurrent requests per host (§4.3). |
| `user_agent_rotation` | `true` | Rotate UA pool per stream. |
| `follow_redirects` | `true` | Follow up to `max_redirects`. |
| `max_redirects` | `5` | Redirect cap. |
| `verify_ssl` | `true` | Set `false` only if a proxy MITMs TLS. |
| `token_refresh` | `true` | C1 HYBRID: on 403 of a tokened URL, re-extract a fresh token from the local source and re-validate before blacklisting. OFF in `quick` mode. |
| `max_token_refetch_per_feed` | `1` | Cap remote re-fetches per source per run. |
| `strip_query_params` | `[token,sig,signature,sign,token2,auth,expires,md5,hmac,nonce,ts,key,hdnea]` | Rotating-token params stripped from the **dedupe key only**. |

> **Content-Type note (F33):** a response is accepted unless its `Content-Type`
> is `text/html` (error/login page). `text/plain`, empty, and `video/*` are all
> treated as valid.

---

## blacklist  (§5, F26)
| Key | Default | Notes |
|-----|---------|-------|
| `short_threshold` | `3` | Consecutive failures → `short` tier. |
| `permanent_inactive_days` | `30` | Days inactive → `permanent`. |
| `permanent_failure_threshold` | `10` | Total failures (no `last_working`) → `permanent` (F26). |
| `escalate_enabled` | `true` | Allow tier escalation. |
| `purge_unchecked_days` | `90` | Delete streams never validated after N days. |

State machine: `none → short → permanent`. A stream with a known `last_working`
date escalates more slowly; one **never worked** hits permanent at
`permanent_failure_threshold` (F26).

---

## providers  (§6)
| Key | Default | Notes |
|-----|---------|-------|
| `aggregate_subdomains` | `true` | `cdn.x.com` + `x.com` → one provider `x.com`. |
| `auto_create` | `true` | Discover providers from stream domains automatically. |

Disable a whole provider: `disable-provider example.com` or the Providers page.

---

## quality  (buffer / health detection, 2026-08-15)
Two independent checks, each toggleable in `config.yaml`. **Disabled = bypassed**
(no extra probe, no health data stored). Both measured only when a stream is
reachable (ok=True); failed streams get `health_tier=NULL`.

| Key | Default | Notes |
|-----|---------|-------|
| `latency_check` | `true` | **Option A** on/off (cheap, approximate). |
| `healthy_max_ms` | `2000` | elapsed ≤ 2000ms → **healthy**. In SECONDS-as-ms — configurable. |
| `medium_max_ms` | `5000` | 2000–5000ms → **medium**; >5000ms → **slow** (likely buffer). |
| `throughput_check` | `true` | **Option B** on/off (accurate, costs traffic). |
| `throughput_sample_seconds` | `3` | How long to download for throughput measure. |
| `throughput_min_kbps` | `500` | measured < 500 KB/s → **unhealthy** (buffer). |
| `mark_in_group_title` | `false` | Prefix ⭐ (healthy) / 🐢 (slow) / ⚠️ (medium) in `group-title`. |
| `separate_healthy_file` | `false` | Also write `working.healthy.m3u` (healthy-only). |

**Option A (latency):** measures request elapsed time. Tier from the two
thresholds above (all configurable). Cheap — reuses the validation request.

**Option B (throughput):** downloads for `throughput_sample_seconds` and
computes measured KB/s. Below `throughput_min_kbps` → slow/unhealthy. This is
the true buffer predictor but costs real traffic per stream.

**Combined score** (both on): `score = latency*0.4 + throughput*0.6`; tier =
worst of the two (slow dominates). Stored in DB columns `health_score`,
`health_tier`. Use in `generate-output` via `mark_in_group_title` /
`separate_healthy_file`, or filter in the Web UI / `stats`.

```yaml
quality:
  latency_check: true
  healthy_max_ms: 2000
  medium_max_ms: 5000
  throughput_check: true
  throughput_sample_seconds: 3
  throughput_min_kbps: 500
  mark_in_group_title: false
  separate_healthy_file: false
```

---

## categories  (group normalization, 2026-08-15)
Collapses messy playlist labels into a SMALL curated taxonomy (<=20 groups).
Genre groups are matched BEFORE country groups (**genre > country**), so
`"Bangladesh News"` lands in **News**. Unknown → `unknown` group (default `Other`).

| Key | Default | Notes |
|-----|---------|-------|
| `unknown` | `Other` | Catch-all for unmatched streams. |
| `genre` | see below | Genre groups + their aliases (matched first). |
| `country` | see below | Country groups + aliases (matched after genre). |

Built-in groups (fully editable in config — tune aliases without code changes):
- **Genre:** News, Sports, Movies, Entertainment, Kids, Music, Religious, Documentary, Education
- **Country:** Bangladesh, India, South Korea and China, USA, International (all other countries)

Matching is multi-signal: group-title → name → provider domain (`.bd`/`bdix`
→ Bangladesh). Aliases support Bangla unicode, whole-word, and prefix matches
(`bd`→`bdix`, `china`→`chinese`) without loose over-matching (`tv`≠`mtv`).

The `generate-output` command writes a **single file** with `# === Group ===`
section headers, sorted by group then name, and rewrites each stream's
`group-title` to the canonical group so players display the normalized category.

```yaml
categories:
  unknown: "Other"
  genre:
    News: ["news", "বার্তা", "cnn", "bbc", "aljazeera", "সংবাদ"]
    Sports: ["sports", "cricket", "football", "খেলা"]
    # ... (see config.yaml.example for the full list)
  country:
    Bangladesh: ["bangla", "bangladesh", "bd", "bdix", "বাংলা", "টিভি"]
    India: ["india", "hindi", "tamil", "desi"]
    # ...
```

---

## webui  ← **port is configurable here**
| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | Start UI on `serve`. |
| `host` | `0.0.0.0` | Bind address. Override with `serve --host`. |
| `port` | `50152` | **Bind port.** Override with `serve --port`. Precedence: CLI flag > this > 50152. |
| `auth_token_file` | `./webui_token.txt` | Optional bearer token file for the UI (if present, API requires it). |

```yaml
webui:
  host: "0.0.0.0"
  port: 50152          # ← change me (e.g. 50153)
```

---

## scheduler
| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | |
| `mode` | `quick` | Mode used by scheduled runs. |
| `cron_expression` | `0 4,16 * * *` | Twice daily (04:00, 16:00). |

---

## logging
| Key | Default | Notes |
|-----|---------|-------|
| `level` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR`. |
| `json_format` | `false` | Structured logs if true. |
| `file` | `./logs/m3u-processor.log` | Log path. |
| `max_bytes` | `10485760` | Rotate at 10 MB. |
| `backup_count` | `5` | Keep 5 rotated logs. |

---

## Environment overrides
Any key can be set via env with `M3U_` prefix and `__` for nesting, e.g.:
```bash
export M3U_DATABASE__PATH=/srv/m3u.db
export M3U_WEBUI__PORT=9000
export M3U_VALIDATION__WORKERS=40
```
CLI flags (e.g. `serve --port`) win over env over config.
