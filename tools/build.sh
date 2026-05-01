#!/usr/bin/env bash
# tools/build.sh — build the mbd-history compose service with git metadata baked in.
#
# Usage:
#   bash tools/build.sh                        # rebuild mbd-history
#   bash tools/build.sh mbd-history            # explicit
#   bash tools/build.sh --no-build             # dry-run
#
# Mirror of the MBD repo's tools/build.sh (PR #144). Default service is
# mbd-history rather than `api`.

set -euo pipefail

SERVICES=()
NO_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --no-build) NO_BUILD=1 ;;
    *) SERVICES+=("$arg") ;;
  esac
done
if [ ${#SERVICES[@]} -eq 0 ]; then
  SERVICES=("mbd-history")
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

GIT_COMMIT="$(git rev-parse HEAD)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [ "$GIT_BRANCH" = "HEAD" ]; then
  GIT_BRANCH="detached"
fi
BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

export GIT_COMMIT GIT_BRANCH BUILD_TIME

echo "Repo root:    $REPO_ROOT"
echo "GIT_COMMIT:   $GIT_COMMIT"
echo "GIT_BRANCH:   $GIT_BRANCH"
echo "BUILD_TIME:   $BUILD_TIME"
echo "Services:     ${SERVICES[*]}"

if [ "$NO_BUILD" -eq 1 ]; then
  echo "(dry-run; --no-build specified, skipping docker build)"
  exit 0
fi

echo
echo "Running: docker compose build ${SERVICES[*]}"
docker compose build "${SERVICES[@]}"

echo
echo "Build complete. To deploy:"
echo "  docker compose up -d --no-deps --force-recreate ${SERVICES[*]}"
