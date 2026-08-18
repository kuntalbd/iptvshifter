# M3U Playlist Processor

Parse, deduplicate, validate, and generate player-ready IPTV playlists.

- **Local-only**, single-file SQLite (stdlib `sqlite3`, no ORM)
- **Multi-format output**: VLC (`#EXTVLCOPT`), Kodi (`#KODIPROP`), TiviMate (pipe `|`) variants
- **2-phase funnel** (Solution A+B): quick reachability → deep latency/throughput health
- **Quality A+B**: latency scoring + optional throughput sampling (buffer detection)
- **Tiered blacklist** + provider/URL enable-disable
- **Auto-publish** validated playlists to a public GitHub repo (out/ folder)
- **Pi-friendly**: WAL, vendored CSS, no JS framework
- **Docker** multi-arch image (`linux/amd64` + `linux/arm64`) — fully stateless, all state on mounted volumes

## Quick start (native)

```bash
pip install -e .                 # installs deps from pyproject.toml
cp examples/config.example.yaml config.yaml
cp examples/feeds.example.txt feeds.txt
python -m m3u_processor init-db
python -m m3u_processor run --mode quick
```

## Quick start (Docker)

```bash
mkdir -p ~/iptvshifter/config ~/iptvshifter/data ~/iptvshifter/out
cp examples/config.example.yaml ~/iptvshifter/config/config.yaml
cp examples/feeds.example.txt   ~/iptvshifter/config/feeds.txt
cp examples/.env.example         ~/iptvshifter/config/.env   # fill in GITHUB_*
docker run -d --name iptvshifter \
  -v ~/iptvshifter/config:/config \
  -v ~/iptvshifter/data:/data \
  -v ~/iptvshifter/out:/out \
  -p 50152:50152 \
  kuntalbd/iptvshifter:latest serve
```

Open http://localhost:50152 — the web UI runs the scheduler (refresh every 2h,
quick run every 2 days) and publishes results to your GitHub repo after each run.

## Docs

- `docs/DOC_INDEX.md` — **start here**: document map + update workflow (live docs)
- `requirement/requirement_overview_v1.2.md` — business requirements (§1–§25)
- `requirement/qa_test_plan_v1.md` — unit/integration test plan (P1–P10, B0–B8)
- `docs/END_TO_END_TESTING.md` — **full project-wide E2E**: feature inventory + test matrix (incl. Web UI)
- `docs/ARCHITECTURE.md` — component contracts, ADRs, data model
- `docs/CONFIGURATION.md` — all config keys
- `docs/INSTALL_AND_USAGE.md` — full install + every command
- `docs/PROJECT_STRUCTURE.md` — source layout
- `examples/` — `config.example.yaml`, `feeds.example.txt`, `.env.example`
- `docker/` — `Dockerfile`, `entrypoint.sh`, `build.sh` (multi-arch)
- `systemd/` — `install.sh`, `setup_autostart.sh`, example unit
