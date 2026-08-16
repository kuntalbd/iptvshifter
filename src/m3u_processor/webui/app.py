"""FastAPI web UI (§11, §12). Zero JS framework — vendored CSS + vanilla JS + SSE.

Pages (server-rendered Jinja2):
  /            Dashboard (stats + last run)
  /streams     Stream browser (filter by tier/working)
  /providers   Provider domains + enable/disable
  /blacklist   Blacklisted streams
  /run         Trigger/monitor a run (SSE progress)
  /settings    Config view + token-refresh toggle

API (JSON):
  GET /api/stats
  GET /api/streams?tier=&working=&q=
  GET /api/providers
  POST /api/provider/disable  {domain, reason}
  POST /api/provider/enable   {domain}
  POST /api/run  {mode}  -> starts background run, returns run_id
  GET /api/events?run_id=...  SSE stream of progress
  POST /api/generate  {formats}  -> writes output files
"""
from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import threading

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from .. import __version__
from ..database import Database
from .. import config as cfg_mod
from ..providers import set_provider_enabled
from ..writers import write_streams

HERE = Path(__file__).resolve().parent
TEMPLATES_DIR = HERE / "templates"
STATIC_DIR = HERE / "static"

# Event bus for SSE progress per run
_RUN_EVENTS = {}
_RUN_LOCK = threading.Lock()


def _pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _get_db(cfg):
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    return db


def create_app(cfg):
    app = FastAPI(title="M3U Playlist Processor", version=__version__)

    # templates (lazy import to avoid hard dep if not used)
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
    )

    def render(name, **ctx):
        ctx.setdefault("version", __version__)
        return env.get_template(name).render(**ctx)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # ---------- pages ----------
    @app.get("/", response_class=HTMLResponse)
    def page_dashboard():
        db = _get_db(cfg)
        try:
            stats = db.query(
                "SELECT "
                "(SELECT COUNT(*) FROM streams) AS total, "
                "(SELECT COUNT(*) FROM streams WHERE blacklist_tier='none') AS ok, "
                "(SELECT COUNT(*) FROM streams WHERE blacklist_tier='short') AS short, "
                "(SELECT COUNT(*) FROM streams WHERE blacklist_tier='permanent') AS perm, "
                "(SELECT COUNT(*) FROM streams WHERE is_working=1) AS working, "
                "(SELECT COUNT(*) FROM providers) AS providers"
            )[0]
            last = db.query(
                "SELECT run_id, mode, status, started_at, stats_json FROM runs "
                "ORDER BY started_at DESC LIMIT 1"
            )
            last_run = last[0] if last else None
            last_refresh = db.query("SELECT value FROM config WHERE key='last_refresh_at'")
            last_refresh = last_refresh[0][0] if last_refresh else None
        finally:
            db.close()
        return HTMLResponse(render("dashboard.html",
                                   stats=dict(stats), last_run=last_run,
                                   last_refresh_at=last_refresh))

    @app.get("/streams", response_class=HTMLResponse)
    def page_streams():
        return HTMLResponse(render("streams.html"))

    @app.get("/providers", response_class=HTMLResponse)
    def page_providers():
        return HTMLResponse(render("providers.html"))

    @app.get("/blacklist", response_class=HTMLResponse)
    def page_blacklist():
        return HTMLResponse(render("blacklist.html"))

    @app.get("/run", response_class=HTMLResponse)
    def page_run():
        return HTMLResponse(render("run.html"))

    @app.get("/settings", response_class=HTMLResponse)
    def page_settings():
        return HTMLResponse(render("settings.html", cfg=cfg.data))

    @app.get("/runs", response_class=HTMLResponse)
    def page_runs():
        return HTMLResponse(render("runs.html", cfg=cfg.data))

    @app.get("/schedules", response_class=HTMLResponse)
    def page_schedules():
        return HTMLResponse(render("schedules.html", cfg=cfg.data))

    @app.get("/live", response_class=HTMLResponse)
    def page_live():
        return HTMLResponse(render("live.html", cfg=cfg.data))

    @app.get("/api/live")
    def api_live():
        db = _get_db(cfg)
        try:
            row = db.query(
                "SELECT run_id, mode, started_at, status, progress_json "
                "FROM runs WHERE status='running' ORDER BY started_at DESC LIMIT 1"
            )
            if not row:
                return {"active": False}
            r = row[0]
            prog = {}
            try:
                prog = json.loads(r["progress_json"] or "{}")
            except Exception:
                prog = {}
            return {
                "active": True,
                "run_id": r["run_id"],
                "mode": r["mode"],
                "started_at": r["started_at"],
                "status": r["status"],
                "progress": prog,
            }
        finally:
            db.close()

    @app.get("/api/run-status")
    def api_run_status():
        """Lightweight: is a run currently active (for discard messaging)?"""
        db = _get_db(cfg)
        try:
            row = db.query(
                "SELECT run_id, mode, started_at FROM runs "
                "WHERE status='running' ORDER BY started_at DESC LIMIT 1"
            )
            if not row:
                return {"active": False}
            r = row[0]
            pid = None
            try:
                pid = int(r["run_id"].rsplit("-", 1)[-1])
            except Exception:
                pid = None
            alive = bool(pid) and _pid_alive(pid)
            return {"active": alive, "run_id": r["run_id"], "mode": r["mode"],
                    "started_at": r["started_at"]}
        finally:
            db.close()

    @app.post("/api/run/stop")
    def api_run_stop():
        """Stop the currently active run (web-spawned or systemd cron job).

        For a web-spawned run we signal the live PID. For a systemd cron job we
        also attempt `systemctl --user stop <job>.service`. Either way the
        process dies -> the Orchestrator's SIGTERM handler sets _stop and the run
        finalizes as 'stopped'.
        """
        db = _get_db(cfg)
        try:
            row = db.query(
                "SELECT run_id, mode FROM runs WHERE status='running' "
                "ORDER BY started_at DESC LIMIT 1"
            )
            if not row:
                return {"ok": False, "error": "no active run"}
            r = row[0]
            run_id = r["run_id"]
            pid = None
            try:
                pid = int(run_id.rsplit("-", 1)[-1])
            except Exception:
                pid = None
            stopped = False
            if pid and _pid_alive(pid):
                try:
                    os.kill(pid, signal.SIGTERM)
                    stopped = True
                except Exception:
                    stopped = False
            # also try systemctl for cron-scheduled jobs
            jobs = (cfg.get("scheduler", {}) or {}).get("jobs", []) or []
            job = next((j for j in jobs if j.get("mode") == r["mode"]), None)
            if job:
                svc = f"m3u-processor-{job['name']}.service"
                try:
                    subprocess.run(["systemctl", "--user", "stop", svc],
                                   capture_output=True, timeout=15)
                    stopped = True
                except Exception:
                    pass
            return {"ok": stopped, "run_id": run_id,
                    "pid": pid, "method": "signal+systemctl"}
        finally:
            db.close()

    @app.get("/api/runs")
    def api_runs(limit: int = 100, mode: str = "", status: str = ""):
        db = _get_db(cfg)
        try:
            wheres = ["1=1"]
            params = []
            if mode:
                wheres.append("mode=?")
                params.append(mode)
            if status:
                wheres.append("status=?")
                params.append(status)
            rows = db.query(
                f"SELECT run_id, mode, started_at, finished_at, duration_seconds, "
                f"status, stats_json FROM runs "
                f"WHERE {' AND '.join(wheres)} "
                f"ORDER BY started_at DESC LIMIT ?",
                tuple(params + [limit]),
            )
            out = []
            for r in rows:
                stats = {}
                try:
                    stats = json.loads(r["stats_json"] or "{}")
                except Exception:
                    stats = {}
                out.append({
                    "run_id": r["run_id"],
                    "mode": r["mode"],
                    "started_at": r["started_at"],
                    "finished_at": r["finished_at"],
                    "duration_seconds": r["duration_seconds"],
                    "status": r["status"],
                    "stats": stats,
                    "publish_error": stats.get("publish_error"),
                })
            return out
        finally:
            db.close()

    # ---------- scheduler (CRUD via config.yaml) ----------
    def _regenerate_units():
        """Regenerate systemd units from the (now-updated) scheduler config.

        IMPORTANT: we `enable` (not `enable --now`) so adding/editing a job does
        NOT immediately fire a run. A freshly-created timer with `--now` would
        trigger the job's service at once, spawning a background run that then
        collides with the single-run guard and pollutes the runs table. The
        timer simply activates on its next scheduled tick. Stale units (renamed
        / removed jobs) are disabled + removed first to avoid dangling symlinks.
        """
        try:
            from ..deploy import generate
            cfg_path = cfg.config_path or "config.yaml"
            workdir = cfg.config_dir or os.getcwd()
            jobs = (cfg.get("scheduler", {}) or {}).get("jobs", []) or []
            if not jobs:
                return
            outdir = os.path.join(workdir, ".systemd-units")
            os.makedirs(outdir, exist_ok=True)
            target = "default.target" if os.environ.get("XDG_RUNTIME_DIR") else "multi-user.target"
            generate(user=os.getlogin() if _safe_getlogin() else os.environ.get("USER", "kuntalbd"),
                     workdir=workdir, config=cfg_path, port=int(cfg.get("webui.port", 50152)),
                     jobs=jobs, outdir=outdir, service_target=target, timer_target="timers.target")
            # remove stale units not in the current job set
            keep = {"m3u-processor-web"} | {f"m3u-processor-{j['name']}" for j in jobs}
            for f in os.listdir(outdir):
                if not (f.endswith(".service") or f.endswith(".timer")):
                    continue
                stem = f[:-len(".service")] if f.endswith(".service") else f[:-len(".timer")]
                if stem not in keep:
                    try:
                        subprocess.run(["systemctl", "--user", "disable", stem],
                                       capture_output=True, timeout=15)
                    except Exception:
                        pass
                    os.remove(os.path.join(outdir, f))
            # install + reload (enable, NOT enable --now, so jobs don't auto-fire)
            for f in os.listdir(outdir):
                if f.endswith((".service", ".timer")):
                    subprocess.run(["systemctl", "--user", "enable",
                                    os.path.join(outdir, f)], capture_output=True, timeout=30)
            subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True, timeout=30)
        except Exception:
            pass

    def _safe_getlogin():
        try:
            return os.getlogin()
        except Exception:
            return None

    @app.get("/api/scheduler")
    def api_scheduler():
        sched = (cfg.get("scheduler", {}) or {})
        jobs = sched.get("jobs", []) or []
        return {"enabled": sched.get("enabled", True), "jobs": jobs}

    @app.post("/api/scheduler")
    async def api_scheduler_add(req: Request):
        from .. import config as _cfg_mod
        body = await req.json()
        name = (body.get("name") or "").strip()
        mode = body.get("mode")
        cron = (body.get("cron") or "").strip()
        if not name or not mode or not cron:
            raise HTTPException(400, "name, mode, cron required")
        if mode not in ("quick", "regular", "full", "refresh"):
            raise HTTPException(400, "invalid mode")
        sched = cfg.data.setdefault("scheduler", {})
        jobs = sched.setdefault("jobs", [])
        existing = next((j for j in jobs if j.get("name") == name), None)
        if existing:
            existing["mode"] = mode
            existing["cron"] = cron
        else:
            jobs.append({"name": name, "mode": mode, "cron": cron})
        _cfg_mod.save_config(cfg)
        _regenerate_units()
        return {"ok": True, "jobs": jobs}

    @app.delete("/api/scheduler")
    async def api_scheduler_del(req: Request):
        from .. import config as _cfg_mod
        body = await req.json()
        name = (body.get("name") or "").strip()
        sched = cfg.data.setdefault("scheduler", {})
        jobs = sched.setdefault("jobs", [])
        sched["jobs"] = [j for j in jobs if j.get("name") != name]
        _cfg_mod.save_config(cfg)
        _regenerate_units()
        return {"ok": True, "jobs": sched["jobs"]}

    # ---------- api ----------
    @app.get("/api/health-stats")
    def api_health_stats():
        db = _get_db(cfg)
        try:
            rows = db.query(
                "SELECT health_tier, COUNT(*) AS n FROM streams "
                "WHERE is_working=1 GROUP BY health_tier"
            )
            last = db.query("SELECT value FROM config WHERE key='last_refresh_at'")
        finally:
            db.close()
        d = {r["health_tier"]: r["n"] for r in rows}
        last_refresh = last[0][0] if last else None
        return {"healthy": d.get("healthy", 0), "medium": d.get("medium", 0),
                "slow": d.get("slow", 0), "unknown": d.get("unknown", 0),
                "last_refresh_at": last_refresh}

    @app.get("/api/streams")
    def api_streams(tier: str = "", working: str = "", q: str = "",
                    health: str = "", limit: int = 200, offset: int = 0):
        db = _get_db(cfg)
        try:
            wheres = ["1=1"]
            params = []
            if tier:
                wheres.append("blacklist_tier=?")
                params.append(tier)
            if working == "1":
                wheres.append("(is_working=1 OR is_working IS NULL)")
            elif working == "0":
                wheres.append("is_working=0")
            if health:
                wheres.append("health_tier=?")
                params.append(health)
            if q:
                wheres.append("(name LIKE ? OR url LIKE ?)")
                params.extend([f"%{q}%", f"%{q}%"])
            sql = ("SELECT id, name, url, original_url, provider_domain, blacklist_tier, "
                   "is_working, health_tier, health_score, last_checked FROM streams WHERE "
                   + " AND ".join(wheres) +
                   " ORDER BY id LIMIT ? OFFSET ?")
            params.extend([limit, offset])
            rows = db.query(sql, params)
        finally:
            db.close()
        return [dict(r) for r in rows]

    @app.get("/api/providers")
    def api_providers():
        db = _get_db(cfg)
        try:
            rows = db.query(
                "SELECT domain, enabled, disabled_reason, first_seen FROM providers ORDER BY domain"
            )
        finally:
            db.close()
        return [dict(r) for r in rows]

    @app.post("/api/provider/disable")
    async def api_disable(req: Request):
        body = await req.json()
        db = _get_db(cfg)
        try:
            set_provider_enabled(db, body["domain"], False,
                                 reason=body.get("reason", "manual"), by="web")
        finally:
            db.close()
        return {"ok": True}

    @app.post("/api/provider/enable")
    async def api_enable(req: Request):
        body = await req.json()
        db = _get_db(cfg)
        try:
            set_provider_enabled(db, body["domain"], True, by="web")
        finally:
            db.close()
        return {"ok": True}

    @app.post("/api/run")
    async def api_run(req: Request):
        from ..orchestrator import Orchestrator
        body = await req.json()
        job_name = body.get("job")
        mode = body.get("mode", cfg.get("validation.mode", "quick"))
        if job_name:
            jobs = (cfg.get("scheduler", {}) or {}).get("jobs", []) or []
            job = next((j for j in jobs if j.get("name") == job_name), None)
            if job:
                mode = job.get("mode", mode)
        token_refresh = body.get("token_refresh")
        q = queue.Queue()

        def progress(done, total):
            q.put({"type": "progress", "done": done, "total": total})

        # Stable web-side run id used both for SSE and DB row (passed to the
        # Orchestrator so the discard reason / live lookups line up). Ends with
        # the real PROCESS pid (not thread id) so the live-pid stop/status logic
        # in api_run_status / api_run_stop keeps working for web-spawned runs.
        run_id = f"web-{mode}-{os.getpid()}"

        # Orchestrator + its DB live entirely in the worker thread (sqlite
        # connections are not thread-safe).
        def worker():
            from ..database import Database as _DB
            db = _DB(cfg.get("database.path"))
            db.init_db(backup=False)
            orch = Orchestrator(db, cfg)
            orch.progress = progress
            try:
                stats = orch.run(mode=mode, token_refresh=token_refresh, run_id=run_id)
                if stats.get("discarded"):
                    q.put({"type": "discarded",
                           "reason": stats.get("discard_reason", "another run active"),
                           "run_id": orch.run_id})
                else:
                    q.put({"type": "done", "stats": stats, "run_id": orch.run_id})
            except Exception as e:
                q.put({"type": "error", "message": str(e)})
            finally:
                db.close()

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        with _RUN_LOCK:
            _RUN_EVENTS[run_id] = q
        return {"run_id": run_id, "mode": mode}

    @app.get("/api/events")
    def api_events(run_id: str):
        with _RUN_LOCK:
            q = _RUN_EVENTS.get(run_id)
        if q is None:
            raise HTTPException(404, "unknown run_id")

        def gen():
            while True:
                try:
                    evt = q.get(timeout=30)
                except queue.Empty:
                    yield "data: " + json.dumps({"type": "ping"}) + "\n\n"
                    continue
                yield "data: " + json.dumps(evt) + "\n\n"
                if evt["type"] in ("done", "error"):
                    break

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post("/api/generate")
    async def api_generate(req: Request):
        from pathlib import Path as _P
        body = await req.json()
        formats = body.get("formats", ["vlc", "kodi", "tivimate"])
        db = _get_db(cfg)
        try:
            out = cfg.get("output.dir", "./out") + "/working.m3u"
            rows = db.query(
                "SELECT url, original_url, attributes, name, provider_domain, "
                "health_tier FROM streams "
                "WHERE enabled=1 AND blacklist_tier='none' "
                "AND (is_working=1 OR is_working IS NULL)"
            )
            results = write_streams(
                rows, out, formats=formats,
                categories_cfg=cfg.get("categories"),
                quality_cfg=cfg.get("quality"),
            )
        finally:
            db.close()
        return {"ok": True, "files": results}

    return app


def run_app(cfg, host="0.0.0.0", port=50152, reload=False):
    import uvicorn
    app = create_app(cfg)
    uvicorn.run(app, host=host, port=port)
