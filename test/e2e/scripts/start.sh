#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

if [ "${1:-}" != "--reuse" ]; then
    compose down --volumes --remove-orphans
fi

compose up -d --wait source target
compose up -d traffic
"$E2E_DIR/scripts/assert-source.sh"
"$E2E_DIR/scripts/assert-traffic.sh"

echo "e2e bed is ready"
echo "source: postgres://app:app@localhost:${SOURCE_PORT:-55432}/app"
echo "target: postgres://app:app@localhost:${TARGET_PORT:-55433}/app"
