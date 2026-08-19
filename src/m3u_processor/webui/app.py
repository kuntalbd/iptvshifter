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
import time

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from ..logging_utils import get_logger as _get_logger

_LOG = _get_logger("m3u.webui")
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


def _const_time_eq(a: str, b: str) -> bool:
    """Constant-time string equality (token comparison)."""
    if len(a) != len(b):
        return False
    res = 0
    for ca, cb in zip(a.encode("utf-8"), b.encode("utf-8")):
        res |= ca ^ cb
    return res == 0


def _get_db(cfg):
    db = Database(cfg.get("database.path"))
    db.init_db(backup=False)
    return db


def create_app(cfg):
    app = FastAPI(title="M3U Playlist Processor", version=__version__)
    app.state.cfg = cfg

    # templates (lazy import to avoid hard dep if not used)
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
    )

    def render(name, **ctx):
        ctx.setdefault("version", __version__)
        return env.get_template(name).render(**ctx)

    async def _json_body(req: Request) -> dict:
        """Safely parse a JSON request body.

        Malformed/empty bodies raise 422 (not 500) so clients get a clear
        signal instead of an unhandled JSONDecodeError traceback.
        """
        try:
            data = await req.json()
        except Exception:  # JSONDecodeError or empty body
            raise HTTPException(status_code=422, detail="invalid or missing JSON body")
        if not isinstance(data, dict):
            raise HTTPException(status_code=422, detail="JSON body must be an object")
        return data

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # ---------- optional API auth ----------
    # When `webui.auth_token_file` points at a file containing a non-empty
    # token, every /api/* call must present it (Authorization: Bearer <token>,
    # X-Auth-Token header, ?token= query, or m3u_token cookie). Constant-time
    # comparison avoids timing side-channels. Read per request (the operator can
    # rotate the token file without restarting the daemon).
    _auth_token = None
    _auth_token_mtime = None

    @app.middleware("http")
    async def _api_auth(request: Request, call_next):
        nonlocal _auth_token, _auth_token_mtime
        path = request.url.path
        if not (path == "/api" or path.startswith("/api/")):
            return await call_next(request)
        tf = cfg.get("webui.auth_token_file", "")
        if not tf:
            return await call_next(request)
        try:
            st = os.stat(tf)
            if _auth_token is None or st.st_mtime != _auth_token_mtime:
                with open(tf, "r", encoding="utf-8") as fh:
                    _auth_token = fh.read().strip()
                _auth_token_mtime = st.st_mtime
        except OSError:
            _auth_token = None
        expected = _auth_token or ""
        if not expected:
            return await call_next(request)
        provided = request.headers.get("Authorization", "")
        if provided.lower().startswith("bearer "):
            provided = provided[len("bearer "):].strip()
        elif not provided:
            provided = request.headers.get("X-Auth-Token", "") or request.query_params.get("token", "") or request.cookies.get("m3u_token", "")
        provided = provided.strip()
        if not provided or not _const_time_eq(provided, expected):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return await call_next(request)

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

    @app.get("/favorites", response_class=HTMLResponse)
    def page_favorites():
        return HTMLResponse(render("favorites.html"))

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

    @app.get("/errors", response_class=HTMLResponse)
    def page_errors():
        return HTMLResponse(render("errors.html", cfg=cfg.data))

    @app.get("/api/run-errors")
    def api_run_errors(run_id: str = None, limit: int = 200):
        db = _get_db(cfg)
        try:
            rows = db.get_run_errors(run_id=run_id, limit=limit)
            return {"errors": [
                {"id": r["id"], "run_id": r["run_id"], "occurred_at": r["occurred_at"],
                 "error_type": r["error_type"], "message": r["message"], "source": r["source"]}
                for r in rows]}
        finally:
            db.close()

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
                return {"is_run_active": False}
            r = row[0]
            prog = {}
            try:
                prog = json.loads(r["progress_json"] or "{}")
            except Exception:
                prog = {}
            return {
                "is_run_active": True,
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
        """Lightweight: is a run currently active (for discard messaging)?

        Keys off the DB `runs` row (status='running'), NOT a pid embedded in the
        run_id. The web service may be restarted mid-run, which would invalidate
        any pid-based liveness check and wrongly report 'idle' while the
        background worker thread is still validating.
        """
        db = _get_db(cfg)
        try:
            row = db.query(
                "SELECT run_id, mode, started_at FROM runs "
                "WHERE status='running' ORDER BY started_at DESC LIMIT 1"
            )
            if not row:
                return {"is_run_active": False}
            r = row[0]
            return {"is_run_active": True, "run_id": r["run_id"], "mode": r["mode"],
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
            # The web-spawned run lives in a daemon worker thread of THIS process;
            # it cannot be reached by pid (run_id no longer embeds one). Signal it
            # via the Orchestrator's stop flag by setting a DB marker the worker
            # polls. Cron/systemd jobs are stopped via systemctl.
            stopped = False
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
            # Mark stop-requested for the in-process worker thread (best effort).
            try:
                db.execute(
                    "UPDATE runs SET status='stopping' WHERE run_id=?", (run_id,)
                )
                db.commit()
                stopped = True
            except Exception:
                pass
            return {"ok": stopped, "run_id": run_id,
                    "method": "db-stop-flag+systemctl"}
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
        body = await _json_body(req)
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
        _LOG.info("scheduler add/update job=%s mode=%s cron=%s",
                  name, mode, cron)
        return {"ok": True, "jobs": jobs}

    @app.delete("/api/scheduler")
    async def api_scheduler_del(req: Request):
        from .. import config as _cfg_mod
        body = await _json_body(req)
        name = (body.get("name") or "").strip()
        sched = cfg.data.setdefault("scheduler", {})
        jobs = sched.setdefault("jobs", [])
        sched["jobs"] = [j for j in jobs if j.get("name") != name]
        _cfg_mod.save_config(cfg)
        _regenerate_units()
        _LOG.info("scheduler delete job=%s", name)
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
                wheres.append("s.blacklist_tier=?")
                params.append(tier)
            if working == "1":
                wheres.append("(s.is_working=1 OR s.is_working IS NULL)")
            elif working == "0":
                wheres.append("s.is_working=0")
            if health:
                wheres.append("s.health_tier=?")
                params.append(health)
            if q:
                wheres.append("(s.name LIKE ? OR s.url LIKE ?)")
                params.extend([f"%{q}%", f"%{q}%"])
            sql = ("SELECT s.id, s.name, s.url, s.original_url, s.provider_domain, "
                   "s.blacklist_tier, s.is_working, s.health_tier, s.health_score, "
                   "s.last_checked, s.last_working, s.total_failures, "
                   "s.consecutive_failures, s.consecutive_pass, s.total_pass, "
                   "CASE WHEN f.fid IS NULL THEN 0 ELSE 1 END AS is_favorite, "
                   "COALESCE(f.fid, 0) AS favorite_id "
                   "FROM streams s "
                   "LEFT JOIN (SELECT id AS fid, url FROM favorites) f "
                   "ON f.url = s.url WHERE "
                   + " AND ".join(wheres) +
                   " ORDER BY s.id LIMIT ? OFFSET ?")
            params.extend([limit, offset])
            rows = db.query(sql, params)
        finally:
            db.close()
        return [dict(r) for r in rows]

    @app.get("/api/providers")
    def api_providers(
        search: str = "",
        state: str = "",
        sort: str = "streams-desc",
        page: int = 1,
        per_page: int = 50,
    ):
        db = _get_db(cfg)
        try:
            params = []
            where = ""
            if search:
                where = " WHERE p.domain LIKE ?"
                params.append(f"%{search}%")
            if state == "enabled":
                where += (" AND " if where else " WHERE ") + " p.enabled = 1"
            elif state == "disabled":
                where += (" AND " if where else " WHERE ") + " p.enabled = 0"

            sort_map = {
                "streams-desc": "total_streams DESC, p.domain",
                "streams-asc": "total_streams ASC, p.domain",
                "working-desc": "working DESC, p.domain",
                "name-asc": "p.domain ASC",
                "name-desc": "p.domain DESC",
            }
            order = sort_map.get(sort, "total_streams DESC, p.domain")

            totals_row = db.query("""
                SELECT
                    COUNT(DISTINCT p.domain) AS total_providers,
                    COALESCE(SUM(sub.total), 0) AS total_streams,
                    COALESCE(SUM(sub.working), 0) AS working,
                    COALESCE(SUM(sub.failed), 0) AS failed,
                    COALESCE(SUM(sub.unchecked), 0) AS unchecked,
                    COALESCE(SUM(sub.blacklist_short), 0) AS blacklist_short,
                    COALESCE(SUM(sub.blacklist_permanent), 0) AS blacklist_permanent
                FROM providers p
                LEFT JOIN (
                    SELECT
                        provider_domain,
                        COUNT(*) AS total,
                        SUM(CASE WHEN is_working = 1 THEN 1 ELSE 0 END) AS working,
                        SUM(CASE WHEN is_working = 0 THEN 1 ELSE 0 END) AS failed,
                        SUM(CASE WHEN is_working IS NULL THEN 1 ELSE 0 END) AS unchecked,
                        SUM(CASE WHEN blacklist_tier = 'short' THEN 1 ELSE 0 END) AS blacklist_short,
                        SUM(CASE WHEN blacklist_tier = 'permanent' THEN 1 ELSE 0 END) AS blacklist_permanent
                    FROM streams GROUP BY provider_domain
                ) sub ON sub.provider_domain = p.domain
                {where}
            """.format(where=where), params)[0]
            totals = dict(totals_row)

            offset = max(0, (page - 1) * per_page)

            count_sql = f"""
                SELECT COUNT(*) AS cnt
                FROM providers p
                LEFT JOIN (
                    SELECT provider_domain, COUNT(*) AS total
                    FROM streams GROUP BY provider_domain
                ) s ON s.provider_domain = p.domain
                {where}
            """
            total = db.query(count_sql, params)[0]["cnt"]

            rows = db.query(f"""
                SELECT
                    p.domain,
                    p.enabled,
                    p.disabled_reason,
                    p.first_seen,
                    COALESCE(s.total, 0) AS total_streams,
                    COALESCE(s.working, 0) AS working,
                    COALESCE(s.failed, 0) AS failed,
                    COALESCE(s.unchecked, 0) AS unchecked,
                    COALESCE(s.blacklist_short, 0) AS blacklist_short,
                    COALESCE(s.blacklist_permanent, 0) AS blacklist_permanent,
                    COALESCE(s.healthy, 0) AS healthy,
                    COALESCE(s.medium, 0) AS medium,
                    COALESCE(s.slow, 0) AS slow
                FROM providers p
                LEFT JOIN (
                    SELECT
                        provider_domain,
                        COUNT(*) AS total,
                        SUM(CASE WHEN is_working = 1 THEN 1 ELSE 0 END) AS working,
                        SUM(CASE WHEN is_working = 0 THEN 1 ELSE 0 END) AS failed,
                        SUM(CASE WHEN is_working IS NULL THEN 1 ELSE 0 END) AS unchecked,
                        SUM(CASE WHEN blacklist_tier = 'short' THEN 1 ELSE 0 END) AS blacklist_short,
                        SUM(CASE WHEN blacklist_tier = 'permanent' THEN 1 ELSE 0 END) AS blacklist_permanent,
                        SUM(CASE WHEN health_tier = 'healthy' THEN 1 ELSE 0 END) AS healthy,
                        SUM(CASE WHEN health_tier = 'medium' THEN 1 ELSE 0 END) AS medium,
                        SUM(CASE WHEN health_tier = 'slow' THEN 1 ELSE 0 END) AS slow
                    FROM streams
                    GROUP BY provider_domain
                ) s ON s.provider_domain = p.domain
                {where}
                ORDER BY {order}
                LIMIT ? OFFSET ?
            """, params + [per_page, offset])
        finally:
            db.close()
        return {
            "providers": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
            "totals": totals,
        }

    @app.post("/api/provider/disable")
    async def api_disable(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            set_provider_enabled(db, body["domain"], False,
                                 reason=body.get("reason", "manual"), by="web")
            _LOG.info("web provider disabled domain=%s reason=%s",
                      body["domain"], body.get("reason", "manual"))
        finally:
            db.close()
        return {"ok": True}

    @app.post("/api/provider/enable")
    async def api_enable(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            set_provider_enabled(db, body["domain"], True, by="web")
            _LOG.info("web provider enabled domain=%s", body["domain"])
        finally:
            db.close()
        return {"ok": True}

    @app.post("/api/run")
    async def api_run(req: Request):
        from ..orchestrator import Orchestrator
        body = await _json_body(req)
        job_name = body.get("job")
        mode = body.get("mode", cfg.get("validation.mode", "quick"))
        if job_name:
            jobs = (cfg.get("scheduler", {}) or {}).get("jobs", []) or []
            job = next((j for j in jobs if j.get("name") == job_name), None)
            if job:
                mode = job.get("mode", mode)
        q = queue.Queue()

        _LOG.info("api_run requested mode=%s job=%s", mode, job_name)
        def progress(done, total):
            q.put({"type": "progress", "done": done, "total": total})

        # Stable web-side run id used both for SSE and DB row (passed to the
        # Orchestrator so the discard reason / live lookups line up). Uses a
        # timestamp+random suffix (NOT the process pid) so the run stays
        # identifiable as 'running' in the DB even if the web service is
        # restarted mid-run — api_run_status keys off the DB row, not a pid.
        run_id = f"web-{mode}-{int(time.time()*1000)}-{os.urandom(3).hex()}"

        # Orchestrator + its DB live entirely in the worker thread (sqlite
        # connections are not thread-safe).
        def worker():
            from ..database import Database as _DB
            from pathlib import Path as _P
            db = _DB(cfg.get("database.path"))
            db.init_db(backup=False)
            orch = Orchestrator(db, cfg)
            orch.progress = progress
            try:
                # Ingest configured sources first (mirrors the CLI `run` path),
                # so a UI run on an empty DB actually populates streams instead
                # of validating nothing. (TC-2 fix: Web UI previously skipped
                # ingest and only re-validated existing rows.)
                if mode != "refresh":
                    feed_file = cfg.get("sources.feed_file")
                    if feed_file and _P(feed_file).is_file():
                        for line in open(feed_file):
                            line = line.strip()
                            if line and not line.startswith("#"):
                                try:
                                    orch.ingest_feed(line)
                                except Exception as e:
                                    _LOG.warning("api_run ingest_feed failed: %s", e)
                    pdir = cfg.get("sources.playlist_dir")
                    if pdir and _P(pdir).is_dir():
                        for f in sorted(_P(pdir).glob("*.m3u*")):
                            try:
                                orch.ingest_source(str(f))
                            except Exception as e:
                                _LOG.warning("api_run ingest_source failed: %s", e)
                stats = orch.run(mode=mode, run_id=run_id)
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

    # ---------- favorites subsystem ----------
    @app.get("/api/favorite-groups")
    def api_favorite_groups():
        db = _get_db(cfg)
        try:
            return [{"id": r["id"], "name": r["name"]} for r in db.favorite_groups()]
        finally:
            db.close()

    @app.get("/api/favorites")
    def api_favorites(group: str = "", working: str = "", q: str = ""):
        db = _get_db(cfg)
        try:
            rows = db.favorite_list(group=group, working=working, q=q)
            out = []
            for r in rows:
                out.append({
                    "id": r["id"], "name": r["name"], "url": r["url"],
                    "original_url": r["original_url"], "groups": r["groups"] or "",
                    "is_enabled": bool(r["is_enabled"]),
                    "is_working": r["is_working"],
                    "last_working": r["last_working"],
                    "source_path": r["source_path"] or "",
                    "is_url": bool(r["is_url"]),
                    "total_failures": r["total_failures"],
                    "consecutive_failures": r["consecutive_failures"],
                    "total_pass": r["total_pass"],
                    "consecutive_pass": r["consecutive_pass"],
                    "total_successes": r["total_successes"],
                })
            return out
        finally:
            db.close()

    @app.post("/api/favorites/add")
    async def api_favorites_add(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            fid = db.favorite_add(
                name=body.get("name", ""),
                url=body["url"],
                original_url=body.get("original_url", ""),
                source_path=body.get("source_path", ""),
                is_url=bool(body.get("is_url", False)),
                is_enabled=bool(body.get("is_enabled", True)),
            )
            if body.get("group"):
                db.favorite_set_group([fid], body["group"])
            _LOG.info("web favorite added id=%s name=%s", fid, body.get("name", ""))
            return {"ok": True, "id": fid}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/favorites/add-existing")
    async def api_favorites_add_existing(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            fid = db.favorite_add_existing(
                stream_url=body["url"],
                name=body.get("name", ""),
            )
            if fid and body.get("group"):
                db.favorite_set_group([fid], body["group"])
            _LOG.info("web favorite add-existing id=%s name=%s", fid, body.get("name", ""))
            return {"ok": True, "id": fid} if fid else {"ok": False, "error": "stream not found"}
        finally:
            db.close()

    @app.post("/api/favorites/edit")
    async def api_favorites_edit(req: Request):
        """Edit an existing favorite (name, group, source_path, is_url, is_enabled)."""
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            fid = body["id"]
            db.favorite_edit(
                fid,
                name=body.get("name"),
                source_path=body.get("source_path"),
                is_url=body.get("is_url"),
                is_enabled=body.get("is_enabled"),
            )
            if "group" in body:
                db.favorite_set_group([fid], body["group"])
            return {"ok": True, "id": fid}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/favorites/delete")
    async def api_favorites_delete(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            db.favorite_delete(body["id"])
            _LOG.info("web favorite deleted id=%s", body["id"])
            return {"ok": True}
        finally:
            db.close()

    @app.post("/api/favorites/set-enabled")
    async def api_favorites_set_enabled(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            db.favorite_set_enabled(body["id"], bool(body.get("enabled", True)))
            _LOG.info("web favorite set-enabled id=%s enabled=%s",
                      body["id"], body.get("enabled", True))
            return {"ok": True}
        finally:
            db.close()

    @app.post("/api/favorites/set-group")
    async def api_favorites_set_group(req: Request):
        body = await _json_body(req)
        fids = body.get("ids", [])
        group = body.get("group", "")
        if not group:
            return {"ok": False, "error": "group required"}
        db = _get_db(cfg)
        try:
            db.favorite_set_group(fids, group)
            _LOG.info("web favorite set-group count=%d group=%s", len(fids), group)
            return {"ok": True}
        finally:
            db.close()

    @app.post("/api/favorites/validate-now")
    async def api_favorites_validate_now(req: Request = None):
        """Validate favorites (separate from main pipeline runs). By default only
        enabled items are checked; pass include_disabled=true to also check
        disabled items."""
        db = _get_db(cfg)
        try:
            include_disabled = False
            if req is not None:
                try:
                    _b = await req.json() if hasattr(req, "json") else {}
                    if _b.get("include_disabled"):
                        include_disabled = True
                except Exception:
                    pass
            sql = "SELECT id, url, original_url FROM favorites"
            if not include_disabled:
                sql += " WHERE is_enabled=1"
            rows = db.query(sql)
            if not rows:
                return {"ok": True, "checked": 0}
            from ..validator import StreamValidator
            validator = StreamValidator(cfg)
            checked = 0
            for r in rows:
                # Validate the PLAYABLE url (may carry a token) — that is what
                # must actually be reachable. Prefer original_url (tokened) since
                # tokenless url may not be directly reachable.
                url = r["original_url"] or r["url"]
                res = validator.validate_one(
                    type("S", (), {"url": url, "original_url": url,
                                  "attributes": {}})())
                if res.uncheckable:
                    # Non-http scheme (rtmp/rtsp/etc): can't validate over HTTP.
                    # Leave status untouched — do NOT count as a failure.
                    checked += 1
                    continue
                db.favorite_record_result(r["id"], res.ok)
                checked += 1
            return {"ok": True, "checked": checked}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/favorites/toggle")
    async def api_favorites_toggle(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            url = body.get("url", "")
            if not url:
                return {"ok": False, "error": "url required"}
            existing = db.query(
                "SELECT id FROM favorites WHERE url=?", (url,))
            if existing:
                db.favorite_delete(existing[0]["id"])
                _LOG.info("web favorite toggled OFF url=%s", url)
                return {"ok": True, "action": "removed"}
            else:
                fid = db.favorite_add_existing(stream_url=url)
                if fid:
                    _LOG.info("web favorite toggled ON url=%s id=%s", url, fid)
                    return {"ok": True, "action": "added", "id": fid}
                return {"ok": False, "error": "stream not found"}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/favorites/batch-add")
    async def api_favorites_batch_add(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            urls = body.get("urls", [])
            added = 0
            for url in urls:
                existing = db.query("SELECT id FROM favorites WHERE url=?", (url,))
                if not existing:
                    fid = db.favorite_add_existing(stream_url=url)
                    if fid:
                        added += 1
            return {"ok": True, "added": added}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/favorites/batch-remove")
    async def api_favorites_batch_remove(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            ids = body.get("ids", [])
            for fid in ids:
                db.favorite_delete(fid)
            return {"ok": True, "removed": len(ids)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/streams/batch-blacklist")
    async def api_streams_batch_blacklist(req: Request):
        body = await _json_body(req)
        db = _get_db(cfg)
        try:
            ids = body.get("ids", [])
            tier = body.get("tier", "none")
            if tier not in ("none", "short", "permanent"):
                return {"ok": False, "error": f"invalid tier: {tier}"}
            for sid in ids:
                db.execute(
                    "UPDATE streams SET blacklist_tier=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (tier, sid))
            db.commit()
            return {"ok": True, "updated": len(ids)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            db.close()

    @app.post("/api/generate")
    async def api_generate(req: Request):
        from pathlib import Path as _P
        body = await _json_body(req)
        formats = body.get("formats", ["vlc", "kodi", "tivimate"])
        db = _get_db(cfg)
        try:
            out = cfg.get("output.dir", "./out") + "/working.m3u"
            rows = db.query(
                "SELECT url, original_url, attributes, name, provider_domain, "
                "health_tier FROM streams "
                "WHERE enabled=1 AND blacklist_tier='none' "
                "AND is_working=1"
            )
            results = write_streams(
                rows, out, formats=formats,
                categories_cfg=cfg.get("categories"),
                quality_cfg=cfg.get("quality"),
            )
            _LOG.info("web generate files=%s", list(results.keys()))
        finally:
            db.close()
        return {"ok": True, "files": results}

    return app


def run_app(cfg, host="0.0.0.0", port=50152, reload=False):
    import uvicorn
    app = create_app(cfg)
    uvicorn.run(app, host=host, port=port)
