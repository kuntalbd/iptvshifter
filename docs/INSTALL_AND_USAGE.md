# Install & Usage Guide — M3U Playlist Processor v1.2

> Prereqs, install, and every command with real examples. Source:
> `src/m3u_processor/`. See `PROJECT_STRUCTURE.md` for what each file does and
> `CONFIGURATION.md` for all config keys.

---

## 1. Requirements (no compile needed)
- Python **3.10+**
- OS: Linux / macOS / Raspberry Pi OS (Pi 4 recommended)
- Pip packages (already declared in `pyproject.toml`, auto-installed):
  `fastapi uvicorn httpx jinja2 pyyaml requests tqdm python-multipart`
- **No Docker.** Native + systemd only (§22).

---

## 2. Install

### Option A — editable install (recommended for dev / Pi)
```bash
cd iptvshifter
pip install -e .
```
This installs the `m3u-processor` console command.

### Option B — without install (run module directly)
```bash
cd iptvshifter
PYTHONPATH=src python -m m3u_processor <command>
```
All examples below use `m3u-processor`; replace with the `python -m` form if
you skipped the install.

### Create your config + feeds
```bash
cp examples/config.example.yaml config.yaml   # then edit (see CONFIGURATION.md)
cp feeds.txt.example feeds.txt         # one playlist URL per line
mkdir -p data output playlists
```

---

## 3. First run (happy path)
```bash
m3u-processor init-db                       # create SQLite DB + pragmas
m3u-processor run --mode quick              # validate a subset (fast)
m3u-processor run --mode regular            # full re-check of eligible streams
m3u-processor generate-output --formats vlc,kodi,tivimate
m3u-processor serve --host 0.0.0.0 --port 50152
```
Then open `http://<host>:50152/`.

> Tip: any uncommon port works — e.g. `50153` — via `--port` or `webui.port`.
> See §5.

---

## 4. Commands reference

| Command | Purpose | Key flags |
|---------|---------|-----------|
| `init-db` | Create DB schema + pragmas + gzip backup helper | — |
| `run` | Ingest sources + validate streams (§3) | `--mode quick\|regular\|full\|refresh`, `--config PATH` |
| `generate-output` | Write player playlists (§7.1) | `--formats vlc,kodi,tivimate`, `--out BASE` (or global `--output-dir DIR` before the subcommand) |
| `list-feeds` | Show configured feed sources | — |
| `add-feed URL` | Append a playlist URL to `feeds.txt` | — |
| `list-providers` | List discovered provider domains + state | — |
| `enable-provider DOMAIN` / `disable-provider DOMAIN` | Toggle a provider (§6.2) | — |
| `stats` | DB summary (counts, tiers, last run) | — |
| `blacklist` | Show blacklisted streams (blacklist tier) | `--tier short\|permanent` (default permanent) |
| `serve` | Start FastAPI web UI (§11) | `--host`, `--port` (both **configurable**, see §5) |
| `--version` / `--help` | Version / help | — |

### `run` modes (§3)
- `quick` — latency-only health (throughput sampling OFF) over all enabled
  streams not permanently blacklisted (tier=none, incl. unverified). Fast,
  low-traffic.
- `regular` — full health scoring (latency + 3s throughput sampling) over all
  enabled tier=none **and** short streams (re-check + rehab).
- `full` — everything enabled, including permanent-blacklisted (rehabilitation
  attempt).
- `refresh` — **token re-extraction only** for tokened working streams (C1
  hybrid); no active/health check. This is the ONLY mode that rotates tokens;
  `quick`/`regular`/`full` just validate and leave expired tokens to the next
  refresh run.

### Output is categorized (single file)
`generate-output` writes **one file per format**, but inside each file streams
are grouped into a SMALL taxonomy (genre > country, ≤20 groups) with
`# === Group ===` section headers, sorted by group then name. Each stream's
`group-title` is rewritten to the canonical group so players show the
normalized category. Tune the taxonomy via the `categories:` block in
`config.yaml` (see CONFIGURATION.md).

### Health / buffer detection (quality block)
Two independent checks mark streams **healthy / medium / slow** so you can spot
buffering channels:
- **Option A (latency):** request elapsed time vs `healthy_max_ms` / `medium_max_ms`
  (configurable, seconds-as-ms).
- **Option B (throughput):** samples real download speed vs `throughput_min_kbps`.
- Each is **enable/disable in `config.yaml`** (`quality.latency_check`,
  `quality.throughput_check`). Disabled = bypassed.
- Output: set `quality.mark_in_group_title: true` to prefix ⭐/🐢 in the
  player, or `quality.separate_healthy_file: true` to also write
  `working.healthy.m3u` (healthy-only). Stored in DB as `health_score`/`health_tier`.

### Examples
```bash
# ingest a one-off playlist without touching feeds.txt
m3u-processor run --mode regular

# token refresh is a dedicated mode, not a run flag:
#   run --mode refresh   # re-extract tokens only (scheduled every 2h)

# generate only VLC + TiviMate, to a custom base file
m3u-processor generate-output --formats vlc,tivimate --out /mnt/usb/playlists/working.m3u

# ban a bad provider entirely
m3u-processor disable-provider bad-cdn.example

# list streams blacklisted permanently
m3u-processor blacklist --tier permanent
```

---

## 5. Web UI & port (port is configurable ✅)
The UI binds to a **configurable host + port**, with this precedence:
**CLI flag > `config.yaml` `webui.port`/`webui.host` > built-in default (50152 / 0.0.0.0)**.

```bash
# via CLI flag
m3u-processor serve --host 127.0.0.1 --port 50153

# OR via config (webui section):
#   webui:
#     host: "0.0.0.0"
#     port: 50153
m3u-processor serve
```
Pages (§11): Dashboard, Streams, Providers, Blacklist, Run, Settings.
Live run progress streams over SSE to the Run page.

---

## 6. Pi deployment (systemd, §22)
```bash
# on the Pi, as root:
export WORKDIR=/opt/m3u-processor
mkdir -p $WORKDIR && cp -r . $WORKDIR/
cp examples/config.example.yaml $WORKDIR/config.yaml   # edit DB path, port, feeds
cd $WORKDIR
pip install -e .
sudo ./scripts/install.sh        # generates + enables systemd units
```
Units created: `m3u-processor.service` (daily run), `m3u-processor.timer`
(daily schedule), `m3u-processor-web.service` (UI). All apply Pi hardening
(`MemoryMax=1G` web / `2G` quick-run & token-refresh, `Nice=10`,
`NoNewPrivileges`, `ProtectSystem=strict`).
Or generate units manually:
```bash
python -m m3u_processor.deploy --mode regular --out /etc/systemd/system
```

---

## 7. Scheduling
- **systemd timer** (recommended on Pi): `m3u-processor.timer` runs daily.
- Or any cron: `0 4,16 * * * m3u-processor run --mode quick`.

---

## 8. Troubleshooting
- **DB locked / busy** → ensure only one `run`/`serve` writes at a time; WAL mode
  is on by default.
- **UI won't start** → check `webui.enabled: true` and that the port is free.
- **All streams "dead"** → a strict proxy/firewall may block HEAD/GET; lower
  `validation.per_host_limit` or set `verify_ssl: false` for testing.
- **Too slow** → reduce `validation.workers` or run `quick` mode more often.
- **VLC: "unable to open the MRL '@url:`http://...`'"** → some source playlists
  wrap URLs as `@url:` + backtick-quoted. The parser now strips that prefix at
  ingest. If you have **old data** with the malformed URLs, re-run
  `m3u-processor run` (or `generate-output` after a fresh `run`) — the parser
  re-parses the feeds and self-heals the stored URLs to clean form.
- **Logs** → `logs/m3u-processor.log` (configurable under `logging:`).

---

## Docker (optional)

The image is **multi-arch** (`linux/amd64` + `linux/arm64`) and **stateless** —
all state lives on mounted volumes, so the container can be replaced/upgraded
without losing data.

### Build (multi-arch → Docker Hub)
```bash
./docker/build.sh latest        # docker buildx build --platform ... --push
```

### Run
```bash
docker run -d --name iptvshifter \
  -v ~/iptvshifter/config:/config \   # config.yaml + feeds.txt + .env
  -v ~/iptvshifter/data:/data \       # m3u.db (sqlite)
  -v ~/iptvshifter/out:/out \         # output (pushed to GitHub after each run)
  -p 50152:50152 \
  kuntalbd/iptvshifter:latest serve
```

- `serve` → long-running web UI + built-in scheduler (default).
- `run --mode quick` → one-shot validation + publish, then exit.
- GitHub publish reads `GITHUB_USER` / `GITHUB_PAT` / `GITHUB_REPO_URL` from the
  mounted `/config/.env` (see `examples/.env.example`). The repo is auto-init +
  remote-set on first run, so no manual `git clone` is needed.
- Override paths via env: `M3U_DB_PATH`, `M3U_FEED_FILE`, `M3U_OUTPUT_DIR`,
  `M3U_CONFIG`, `M3U_WEBUI_PORT`.
.
