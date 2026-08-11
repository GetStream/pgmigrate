#!/bin/sh

set -eu

interval=${TRAFFIC_INTERVAL:-0.10}

until psql "$SOURCE_URL" -X -v ON_ERROR_STOP=1 -Atqc "SELECT 1 FROM e2e.traffic_metrics WHERE id = 1" >/dev/null 2>&1; do
    sleep 1
done

while :; do
    if ! psql "$SOURCE_URL" -X -v ON_ERROR_STOP=1 -Atqc "SELECT e2e.run_traffic_step()" >/dev/null; then
        echo "traffic transaction failed; retrying" >&2
        sleep 1
        continue
    fi
    sleep "$interval"
done
