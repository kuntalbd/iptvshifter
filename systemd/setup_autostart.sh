#!/usr/bin/env bash
# =============================================================================
# setup_autostart.sh — install M3U Playlist Processor as boot-persistent
# systemd services (web UI + scheduler). Login-independent: once installed,
# the PC boots -> everything starts with NO user login required.
#
# Two scopes:
#   --scope user     (default)  user-level units + linger. Root NOT required.
#                               Works on this machine right now.
#   --scope system               system-wide units in /etc/systemd/system.
#                               REQUIRES root (run with sudo). Truly login-
#                               independent and not tied to a user session.
#
# Reusable on any machine: edit the variables below or pass flags.
# Idempotent: safe to re-run (regenerates units + re-enables).
#
# What it installs (from config.scheduler.jobs):
#   m3u-processor-<job>.service + .timer   for each scheduler job
#   m3u-processor-web.service              web UI (restarts on failure)
# Each run auto-publishes outputs to git (publish.py) on completion.
# =============================================================================
set -euo pipefail

# ---- configurable defaults (override via flags/env) -------------------------
REPO_DIR="${REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
CONFIG="${CONFIG:-$REPO_DIR/prod/config.yaml}"
RUN_USER="${RUN_USER:-$(id -un)}"
PORT="${PORT:-50152}"
SCOPE="${SCOPE:-user}"
PY="${PY:-$(command -v python3)}"

# ---- parse flags ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --scope)        SCOPE="$2"; shift 2;;
    --repo)         REPO_DIR="$2"; shift 2;;
    --config)       CONFIG="$2"; shift 2;;
    --user)         RUN_USER="$2"; shift 2;;
    --port)         PORT="$2"; shift 2;;
    --python)       PY="$2"; shift 2;;
    -h|--help)      sed -n '2,20p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 1;;
  esac
done

[[ "$SCOPE" == "user" || "$SCOPE" == "system" ]] || { echo "scope must be user|system" >&2; exit 1; }
[[ -f "$CONFIG" ]] || { echo "config not found: $CONFIG" >&2; exit 1; }

# verify the package/CLI is importable from the chosen python
"$PY" -c "import m3u_processor" 2>/dev/null || {
  echo "m3u_processor not importable from $PY" >&2
  echo "activate its venv first, e.g.  source $REPO_DIR/.venv/bin/activate" >&2
  exit 1
}

if [[ "$SCOPE" == "user" ]]; then
  UNIT_DIR="$HOME/.config/systemd/user"
  SVC_TARGET="default.target"
  TMR_TARGET="timers.target"
  CTL="systemctl --user"
  echo "==> user-scope install for user '$RUN_USER'"
else
  UNIT_DIR="/etc/systemd/system"
  SVC_TARGET="multi-user.target"
  TMR_TARGET="timers.target"
  CTL="systemctl"
  # system-wide needs root
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "system scope requires root — re-run with: sudo $0 --scope system ..." >&2
    exit 1
  fi
fi

mkdir -p "$UNIT_DIR"

# Determine the set of unit base-names that SHOULD exist for the current jobs,
# so we can prune any stale units left over from an older scheduler config
# (e.g. renamed/removed jobs like daily-full, weekly-heavy).
KEEP_NAMES="m3u-processor-web"
while IFS= read -r jname; do
  [ -n "$jname" ] && KEEP_NAMES="$KEEP_NAMES m3u-processor-$jname"
done < <("$PY" -c "
import yaml,sys
with open('$CONFIG') as f: c=yaml.safe_load(f) or {}
for j in (c.get('scheduler',{}) or {}).get('jobs',[]) or []:
    print(j.get('name',''))
")

echo "==> generating units into $UNIT_DIR"
"$PY" -m m3u_processor.deploy \
  --user "$RUN_USER" \
  --workdir "$REPO_DIR" \
  --config "$CONFIG" \
  --port "$PORT" \
  --out "$UNIT_DIR" \
  --service-target "$SVC_TARGET" \
  --timer-target "$TMR_TARGET"

# Prune stale units: disable + remove any m3u-processor-*.service/.timer whose
# stem is not in KEEP_NAMES (handles renamed/removed scheduler jobs).
echo "==> pruning stale units"
for f in "$UNIT_DIR"/m3u-processor-*.service "$UNIT_DIR"/m3u-processor-*.timer; do
  [ -e "$f" ] || [ -L "$f" ] || continue
  stem=$(basename "$f" .service); stem=$(basename "$stem" .timer)
  if ! echo " $KEEP_NAMES " | grep -q " $stem "; then
    $CTL disable "$(basename "$f")" 2>/dev/null || true
    rm -f "$f"
    echo "  pruned: $(basename "$f")"
  fi
done

# enable + start
echo "==> reloading + enabling units"
$CTL daemon-reload

# enable all generated timer + service units
for unit in "$UNIT_DIR"/m3u-processor-*.timer "$UNIT_DIR"/m3u-processor-web.service; do
  [[ -e "$unit" ]] || continue
  $CTL enable --now "$(basename "$unit")" >/dev/null && echo "  enabled: $(basename "$unit")"
done

if [[ "$SCOPE" == "user" ]]; then
  echo "==> enabling linger (keeps user services alive across reboot w/o login)"
  loginctl enable-linger "$RUN_USER" 2>/dev/null || \
    echo "  (linger needs root once: sudo loginctl enable-linger $RUN_USER)"
fi

echo
echo "==> DONE. Status:"
$CTL list-units 'm3u-processor-*' --no-legend 2>/dev/null || true
echo
echo "Web UI: http://0.0.0.0:$PORT  (starts on boot, no login needed)"
echo "On reboot the scheduler timers + web UI come up automatically."
