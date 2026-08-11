#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

compose stop traffic >/dev/null

before=$(source_sql -Atqc "SELECT steps FROM e2e.traffic_metrics WHERE id = 1")
sleep "${TRAFFIC_QUIET_WINDOW:-2}"
after=$(source_sql -Atqc "SELECT steps FROM e2e.traffic_metrics WHERE id = 1")

if [ "$before" != "$after" ]; then
    echo "traffic did not quiesce: before=$before after=$after" >&2
    exit 1
fi

echo "traffic=paused steps=$after"
