#!/usr/bin/env bash
# Install M3U Playlist Processor as systemd services (native + systemd, §22).
# Run as root (sudo) on the Pi. Requires: python3, the package installed
# (pip install -e .), and config.yaml present at WORKDIR.
# Each scheduler job (config.scheduler.jobs) becomes its own .timer + .service.
set -euo pipefail

WORKDIR="${WORKDIR:-/opt/m3u-processor}"
SERVICE_USER="${SERVICE_USER:-m3u}"
PORT="${PORT:-50152}"

echo "[install] workdir=$WORKDIR user=$SERVICE_USER port=$PORT"

if [ ! -f "$WORKDIR/config.yaml" ]; then
  echo "[install] ERROR: $WORKDIR/config.yaml not found. Copy examples/config.example.yaml first." >&2
  exit 1
fi

id "$SERVICE_USER" >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
mkdir -p "$WORKDIR/data" "$WORKDIR/out"
chown -R "$SERVICE_USER":"$SERVICE_USER" "$WORKDIR"

# generate one timer+service per scheduler job (+ web UI service)
python3 -m m3u_processor.deploy --user "$SERVICE_USER" \
  --workdir "$WORKDIR" --config "$WORKDIR/config.yaml" --port "$PORT" \
  --out /etc/systemd/system

systemctl daemon-reload

# enable + start every generated run timer
for svc in /etc/systemd/system/m3u-processor-*.timer; do
  [ -e "$svc" ] || continue
  systemctl enable "$(basename "$svc")"
  systemctl start "$(basename "$svc")"
  echo "[install] enabled timer: $(basename "$svc")"
done

# web UI
systemctl enable m3u-processor-web.service
systemctl start m3u-processor-web.service
echo "[install] done. Web UI at http://localhost:$PORT"
echo "[install] per-job timers enabled (reboot-persistent). Check: systemctl list-timers 'm3u-processor-*'"
