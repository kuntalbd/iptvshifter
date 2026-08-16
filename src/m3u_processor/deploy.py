"""Deployment helpers: generate systemd units + install script (§22).

Generates one timer+service PER scheduler job, plus the web UI service.
Each run job unit executes `m3u-processor run --job <name>`, which reads its
mode from config.scheduler.jobs. systemd timers are REBOOT-PERSISTENT: the
next-trigger is tracked by systemd, so the per-job interval counter does NOT
reset on reboot.

Also accepts a legacy single --mode/--calendar (backward compatible) which
emits a single m3u-processor.service/.timer.
All units apply Pi hardening: MemoryMax, Nice=10, NoNewPrivileges.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

SERVICE_TMPL = """[Unit]
Description=M3U Playlist Processor — {job} ({mode} mode)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User={user}
WorkingDirectory={workdir}
ExecStart={python} -m m3u_processor --config {config} run --job {job}
Nice=10
MemoryMax=400M
MemoryHigh=300M
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths={workdir}
Restart=no
StandardOutput=journal
StandardError=journal

[Install]
WantedBy={service_target}
"""

TIMER_TMPL = """[Unit]
Description=M3U Playlist Processor — {job} timer

[Timer]
OnCalendar={oncalendar}
Persistent=true
RandomizedDelaySec=30

[Install]
WantedBy={timer_target}
"""

WEB_TMPL = """[Unit]
Description=M3U Playlist Processor — Web UI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={user}
WorkingDirectory={workdir}
ExecStart={python} -m m3u_processor --config {config} serve --host 0.0.0.0 --port {port}
Nice=10
MemoryMax=400M
MemoryHigh=300M
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths={workdir}
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy={service_target}
"""


# Map common cron expressions to systemd OnCalendar. Falls back to a best-effort
# conversion for the simple 5-field cron used by config.scheduler.jobs[*].cron.
def cron_to_oncalendar(cron: str) -> str:
    """Convert a 5-field cron string to a valid systemd OnCalendar spec.

    Produces forms like:
      "0 2 * * *"      -> "*-*-* 02:00:00"
      "7 */2 * * *"     -> "*-*-* 0/2:07:00"   (every 2h at :07)
      "0 3 * * FRI"     -> "Fri *-*-* 03:00:00"
    Falls back to the raw cron string if it cannot be parsed.
    """
    parts = cron.strip().split()
    if len(parts) != 5:
        return cron
    minute, hour, dom, month, dow = parts

    def _num(field, step_base):
        if field == "*":
            return "*", None
        if field.startswith("*/"):
            return f"{step_base}/{field[2:]}", None
        return field, None

    min_part, _ = _num(minute, 0)
    hr_part, _ = _num(hour, 0)
    # zero-pad simple numeric minute/hour for systemd
    if min_part.isdigit():
        min_part = min_part.zfill(2)
    if hr_part.isdigit():
        hr_part = hr_part.zfill(2)
    time = f"{hr_part}:{min_part}:00"

    dow_token = ""
    if dow != "*":
        wmap = {"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed", "4": "Thu",
                "5": "Fri", "6": "Sat", "7": "Sun"}
        toks = [wmap.get(d.strip(), d.strip()) for d in dow.split(",")]
        dow_token = ",".join(toks) + " "
    return f"{dow_token}*-*-* {time}"


def generate(user="m3u", workdir="/opt/m3u-processor", config="./config.yaml",
              port=50152, jobs=None, outdir=".", service_target="multi-user.target",
              timer_target="timers.target"):
    """Generate units. `jobs` is a list of dicts {name, mode, cron}.
    If None, a single legacy m3u-processor unit is emitted (mode=regular).
    `service_target` controls WantedBy for .service units (use
    `default.target` for user-scope installs, `multi-user.target` for
    system-scope). `timer_target` is where timers hook (usually timers.target).
    """
    python = sys.executable
    os.makedirs(outdir, exist_ok=True)
    paths = {}

    if not jobs:
        jobs = [{"name": "m3u-processor", "mode": "regular", "cron": "daily"}]

    for job in jobs:
        name = job["name"]
        mode = job.get("mode", "regular")
        cron = job.get("cron", "daily")
        oncal = cron_to_oncalendar(cron) if cron != "daily" else "*-*-* 04:00:00"
        svc = SERVICE_TMPL.format(job=name, mode=mode, user=user,
                                  workdir=workdir, python=python, config=config,
                                  service_target=service_target)
        tmr = TIMER_TMPL.format(job=name, oncalendar=oncal,
                                timer_target=timer_target)
        svc_path = os.path.join(outdir, f"m3u-processor-{name}.service")
        tmr_path = os.path.join(outdir, f"m3u-processor-{name}.timer")
        with open(svc_path, "w") as f:
            f.write(svc)
        with open(tmr_path, "w") as f:
            f.write(tmr)
        paths[f"m3u-processor-{name}.service"] = svc_path
        paths[f"m3u-processor-{name}.timer"] = tmr_path

    # web UI service
    web = WEB_TMPL.format(user=user, workdir=workdir, python=python,
                          config=config, port=port,
                          service_target=service_target)
    with open(os.path.join(outdir, "m3u-processor-web.service"), "w") as f:
        f.write(web)
    paths["m3u-processor-web.service"] = os.path.join(outdir, "m3u-processor-web.service")
    return list(paths.keys())


def main():
    ap = argparse.ArgumentParser(description="Generate systemd units for Pi deployment (§22)")
    ap.add_argument("--user", default="m3u")
    ap.add_argument("--workdir", default="/opt/m3u-processor")
    ap.add_argument("--config", default="./config.yaml")
    ap.add_argument("--port", type=int, default=50152)
    ap.add_argument("--jobs", default=None,
                   help="JSON list of {name,mode,cron}; if omitted, reads config.scheduler.jobs")
    ap.add_argument("--out", default=".")
    ap.add_argument("--service-target", default="multi-user.target",
                   help="WantedBy target for service units (default.target for user scope)")
    ap.add_argument("--timer-target", default="timers.target")
    args = ap.parse_args()

    jobs = None
    if args.jobs:
        import json
        jobs = json.loads(args.jobs)
    elif os.path.exists(args.config):
        import yaml
        with open(args.config) as f:
            cfg = yaml.safe_load(f) or {}
        sched = cfg.get("scheduler", {}) or {}
        jobs = sched.get("jobs")
    files = generate(args.user, args.workdir, args.config, args.port, jobs,
                     args.out, args.service_target, args.timer_target)
    for f in files:
        print(f"[deploy] wrote {f}")


if __name__ == "__main__":
    main()
