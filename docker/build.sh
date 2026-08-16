#!/usr/bin/env bash
# Build & push the multi-arch (amd64 + arm64) image to Docker Hub.
# Usage: ./docker/build.sh [tag]
set -euo pipefail

TAG="${1:-latest}"
IMAGE="kuntalbd/iptvshifter:${TAG}"

if ! docker buildx version >/dev/null 2>&1; then
    echo "docker buildx is required. Enable it: docker buildx create --use" >&2
    exit 1
fi

# Ensure a multi-arch builder exists.
docker buildx create --name m3u-builder --driver docker-container --use 2>/dev/null || \
    docker buildx use m3u-builder 2>/dev/null || true

echo "==> Building + pushing ${IMAGE} for linux/amd64,linux/arm64"
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --file docker/Dockerfile \
    --tag "${IMAGE}" \
    --push \
    .

echo "==> Done. Pull with: docker pull ${IMAGE}"
