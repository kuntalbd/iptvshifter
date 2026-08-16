#!/bin/bash
# Entrypoint for the M3U Playlist Processor container.
# - Sets a git identity (needed for the auto-publish step).
# - Applies M3U_* env overrides (already honoured by the config loader).
# - Runs the requested mode: `serve` (default) or `run ...`.
set -e

# git identity for the publish step (override via GIT_USER / GIT_EMAIL).
export GIT_AUTHOR_NAME="${GIT_USER:-iptvshifter}"
export GIT_AUTHOR_EMAIL="${GIT_EMAIL:-iptvshifter@users.noreply.github.com}"
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
export GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"

CONFIG="${M3U_CONFIG:-/config/config.yaml}"

# If a config was not mounted, generate a minimal one from the example so the
# container still starts (user should mount their own /config).
if [ ! -f "$CONFIG" ]; then
    echo "[entrypoint] no config at $CONFIG — using built-in example defaults."
    CONFIG_ARG=""
else
    CONFIG_ARG="--config $CONFIG"
fi

case "$1" in
    serve)
        exec python -m m3u_processor $CONFIG_ARG serve --host 0.0.0.0 --port "${M3U_WEBUI_PORT:-50152}"
        ;;
    run)
        shift
        exec python -m m3u_processor $CONFIG_ARG run "$@"
        ;;
    *)
        # pass through anything else
        exec python -m m3u_processor $CONFIG_ARG "$@"
        ;;
esac
