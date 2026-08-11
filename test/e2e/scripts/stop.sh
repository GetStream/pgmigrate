#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

if [ "${1:-}" = "--keep-data" ]; then
    compose down --remove-orphans
else
    compose down --volumes --remove-orphans
fi
