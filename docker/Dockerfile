# M3U Playlist Processor — multi-arch image (linux/amd64 + linux/arm64)
# Build:  docker buildx build --platform linux/amd64,linux/arm64 \
#           -t kuntalbd/iptvshifter:latest --push .
FROM python:3.11-slim

# git is required for the auto-publish-to-GitHub step.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN useradd -m -u 10001 appuser
WORKDIR /app

# Install Python deps first (better layer caching).
COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

# Data / config / output live on the host (mounted). Nothing sensitive in image.
VOLUME ["/config", "/data", "/out"]

# Default mounts assumed at these paths.
# Working output (generated playlists) lives at /data/out; the publish target
# repo is a SEPARATE mount at /out (see config publish.target_dir: ../out which
# resolves to /out relative to /config). Keeping them separate avoids the
# working dir and the git repo being the same folder.
ENV M3U_CONFIG=/config/config.yaml \
    M3U_DB_PATH=/data/m3u.db \
    M3U_FEED_FILE=/config/feeds.txt \
    M3U_OUTPUT_DIR=/data/out

EXPOSE 50152

# The container is stateless: all state is on mounted volumes.
# Modes:
#   serve  -> long-running web UI + built-in scheduler (recommended)
#   run --mode quick -> one-shot validation + publish, then exit
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
USER appuser

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["serve"]
