"""CLI entry point (argparse). Implements §9 commands.

Config precedence (§18.4): CLI args > env (M3U_*) > config.yaml > defaults.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

SYS_PATH_READY = False


def _ensure_path():
    global SYS_PATH_READY
    if not SYS_PATH_READY:
        src = str(Path(__file__).resolve().parent.parent)
        if src not in sys.path:
            sys.path.insert(0, src)
        SYS_PATH_READY = True


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="m3u-processor",
        description="M3U Playlist Processor v1.2 — parse, dedupe, validate, output.",
    )
    p.add_argument("--db", help="Path to SQLite DB")
    p.add_argument("--config", help="Path to config.yaml")
    p.add_argument("--feed-file", help="Path to feeds.txt")
    p.add_argument("--playlist-dir", help="Path to local playlist dir")
    p.add_argument("--output-dir", help="Path to output dir")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("-q", "--quiet", action="store_true")
    p.add_argument("--json-logs", action="store_true")
    p.add_argument("--version", action="store_true", help="Print version and exit")

    sub = p.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run a validation pass (§3)")
    run_p.add_argument("--mode", choices=["quick", "regular", "full", "refresh"])
    run_p.add_argument("--job", help="Run a named scheduler job (reads mode+cron from config.scheduler.jobs)")
    run_p.add_argument("--workers", type=int)
    run_p.add_argument("--timeout", type=int)
    run_p.add_argument("--resume", action="store_true")

    sub.add_parser("init-db", help="Initialize the database schema")
    sub.add_parser("vacuum", help="VACUUM the database")
    sub.add_parser("backup", help="Backup the database (gzip dump)")

    go_p = sub.add_parser("generate-output", help="Write working playlists (§7.1)")
    go_p.add_argument("--formats", default="vlc,kodi,tivimate",
                      help="Comma list: vlc,kodi,tivimate")
    go_p.add_argument("--out", help="Base output path (e.g. /out/working.m3u)")

    sub.add_parser("list-feeds", help="List configured feed URLs")
    af_p = sub.add_parser("add-feed", help="Append a feed URL to feeds.txt")
    af_p.add_argument("url")

    sub.add_parser("list-providers", help="List discovered providers + state")
    ep_p = sub.add_parser("disable-provider", help="Disable a provider domain")
    ep_p.add_argument("domain")
    ep_p.add_argument("--reason", default="manual")
    ep2_p = sub.add_parser("enable-provider", help="Re-enable a provider domain")
    ep2_p.add_argument("domain")

    sub.add_parser("stats", help="Print DB statistics")
    bl_p = sub.add_parser("blacklist", help="Show blacklisted streams")
    bl_p.add_argument("--tier", choices=["short", "permanent"], default="permanent")

    pub_p = sub.add_parser("publish", help="Copy outputs to repo out/ and push to git")
    pub_p.add_argument("--run-id", default="", help="Label for the commit message")

    serve_p = sub.add_parser("serve", help="Start the FastAPI web UI (§11)")
    serve_p.add_argument("--host", default=None,
                         help="Bind host (default: cfg webui.host or 0.0.0.0)")
    serve_p.add_argument("--port", type=int, default=None,
                         help="Bind port (default: cfg webui.port or 50152)")
    serve_p.add_argument("--no-reload", action="store_true")
    return p


def _cli_overrides(args) -> dict:
    o = {}
    if args.db: o["database.path"] = args.db
    if args.feed_file: o["sources.feed_file"] = args.feed_file
    if args.playlist_dir: o["sources.playlist_dir"] = args.playlist_dir
    if args.output_dir: o["output.dir"] = args.output_dir
    if getattr(args, "workers", None): o["validation.workers"] = args.workers
    if getattr(args, "mode", None): o["validation.mode"] = args.mode
    return o


def _load_cfg(args):
    from m3u_processor import config as cfg_mod
    overrides = _cli_overrides(args)
    # --config wins; fall back to M3U_CONFIG env (used by the Docker image when
    # no --config is supplied), then the default ./config.yaml.
    config_path = args.config or os.environ.get("M3U_CONFIG") or None
    return cfg_mod.load_config(cli_overrides=overrides, config_path=config_path)


def _db(args, cfg):
    from m3u_processor.database import Database
    db = Database(cfg.get("database.path"))
    return db


def main(argv=None):
    _ensure_path()
    from m3u_processor import __version__
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print(f"m3u-processor {__version__}")
        return 0

    cfg = _load_cfg(args)

    if args.command == "init-db":
        db = _db(args, cfg)
        db.init_db(backup=cfg.get("database.backup_on_start"))
        print(f"[init-db] schema ready at {cfg.get('database.path')}")
        db.close()
        return 0

    if args.command == "vacuum":
        db = _db(args, cfg)
        db.vacuum()
        print("[vacuum] done")
        db.close()
        return 0

    if args.command == "backup":
        db = _db(args, cfg)
        path = db.backup_db()
        print(f"[backup] wrote {path}")
        db.close()
        return 0

    if args.command == "run":
        from m3u_processor.orchestrator import Orchestrator
        db = _db(args, cfg)
        db.init_db(backup=False)
        orch = Orchestrator(db, cfg)
        # Refresh mode only re-extracts tokens from EXISTING db rows — it must
        # NOT re-ingest feeds (that would re-parse everything and is the source
        # of rate-limit/429 crashes). Skip ingest entirely for refresh.
        run_mode = getattr(args, "mode", None)
        if run_mode != "refresh":
            # ingest configured sources
            feed_file = cfg.get("sources.feed_file")
            if feed_file and Path(feed_file).is_file():
                for line in open(feed_file):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        orch.ingest_feed(line)
            pdir = cfg.get("sources.playlist_dir")
            if pdir and Path(pdir).is_dir():
                for f in sorted(Path(pdir).glob("*.m3u*")):
                    orch.ingest_source(str(f))
        # resolve mode: explicit --mode > --job's mode > config validation.mode
        mode = getattr(args, "mode", None)
        job_name = getattr(args, "job", None)
        if not mode and job_name:
            jobs = (cfg.get("scheduler", {}) or {}).get("jobs", []) or []
            job = next((j for j in jobs if j.get("name") == job_name), None)
            if not job:
                print(f"[run] unknown job: {job_name}", file=sys.stderr)
                return 2
            mode = job.get("mode", "quick")
        if not mode:
            mode = cfg.get("validation.mode", "quick")
        stats = orch.run(mode=mode)
        print(json.dumps(stats, indent=2))
        db.close()
        return 0

    if args.command == "generate-output":
        from m3u_processor.writers import write_streams
        db = _db(args, cfg)
        db.init_db(backup=False)
        formats = [x.strip() for x in args.formats.split(",") if x.strip()]
        out = args.out or cfg.get("output.dir", "./out") + "/working.m3u"
        rows = db.query(
            "SELECT url, original_url, attributes, name, provider_domain, "
            "health_tier, health_score FROM streams "
            "WHERE enabled=1 AND blacklist_tier='none' AND (is_working=1 OR is_working IS NULL)"
        )
        categories_cfg = cfg.get("categories")
        quality_cfg = cfg.get("quality")
        results = write_streams(rows, out, formats=formats, categories_cfg=categories_cfg, quality_cfg=quality_cfg)
        for fmt, path in results.items():
            print(f"[generate-output] wrote {fmt}: {path}")
        db.close()
        return 0

    if args.command == "list-feeds":
        feed_file = cfg.get("sources.feed_file")
        if feed_file and Path(feed_file).is_file():
            for line in open(feed_file):
                line = line.strip()
                if line and not line.startswith("#"):
                    print(line)
        else:
            print("(no feeds.txt configured)")
        return 0

    if args.command == "add-feed":
        feed_file = cfg.get("sources.feed_file")
        with open(feed_file, "a") as f:
            f.write(args.url + "\n")
        print(f"[add-feed] appended to {feed_file}")
        return 0

    if args.command == "list-providers":
        db = _db(args, cfg)
        db.init_db(backup=False)
        for r in db.query("SELECT domain, enabled, disabled_reason FROM providers ORDER BY domain"):
            flag = "ENABLED" if r["enabled"] else f"DISABLED({r['disabled_reason']})"
            print(f"{r['domain']:40s} {flag}")
        db.close()
        return 0

    if args.command in ("disable-provider", "enable-provider"):
        from m3u_processor.providers import set_provider_enabled
        db = _db(args, cfg)
        db.init_db(backup=False)
        enabled = args.command == "enable-provider"
        set_provider_enabled(db, args.domain, enabled,
                             reason=getattr(args, "reason", "manual"), by="cli")
        print(f"[{args.command}] {args.domain} -> {'enabled' if enabled else 'disabled'}")
        db.close()
        return 0

    if args.command == "stats":
        db = _db(args, cfg)
        db.init_db(backup=False)
        s = db.query(
            "SELECT "
            "(SELECT COUNT(*) FROM streams) AS total, "
            "(SELECT COUNT(*) FROM streams WHERE blacklist_tier='none') AS ok, "
            "(SELECT COUNT(*) FROM streams WHERE blacklist_tier='short') AS short, "
            "(SELECT COUNT(*) FROM streams WHERE blacklist_tier='permanent') AS perm, "
            "(SELECT COUNT(*) FROM streams WHERE is_working=1) AS working, "
            "(SELECT COUNT(*) FROM providers) AS providers"
        )[0]
        print(json.dumps({k: s[k] for k in s.keys()}, indent=2))
        db.close()
        return 0

    if args.command == "blacklist":
        db = _db(args, cfg)
        db.init_db(backup=False)
        for r in db.query(
            "SELECT name, url, provider_domain, blacklist_tier, blacklist_reason "
            "FROM streams WHERE blacklist_tier=?", (args.tier,)
        ):
            print(f"{r['blacklist_tier']:9s} {r['name'][:30]:30s} {r['provider_domain']}")
        db.close()
        return 0

    if args.command == "publish":
        from m3u_processor.publish import publish_outputs
        res = publish_outputs(cfg, run_id=getattr(args, "run_id", "") or "manual")
        print(json.dumps(res, indent=2))
        return 0 if res.get("published") else 1

    if args.command == "serve":
        from m3u_processor.webui.app import run_app
        # Port/host configurable: CLI flag wins; else config (webui.port/host);
        # else built-in defaults (50152 / 0.0.0.0).
        host = args.host or cfg.get("webui.host", "0.0.0.0")
        port = args.port if args.port is not None else int(cfg.get("webui.port", 50152))
        run_app(cfg, host=host, port=port, reload=not args.no_reload)
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
