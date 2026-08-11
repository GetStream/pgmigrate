#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

sample() {
    source_sql -At -F '|' -c \
        "SELECT steps, inserts, updates, deletes, extract(epoch FROM (clock_timestamp() - last_traffic_at))::integer FROM e2e.traffic_metrics WHERE id = 1"
}

before=$(sample)
sleep "${TRAFFIC_ASSERT_WINDOW:-2}"
after=$(sample)

IFS='|' read -r before_steps _rest <<EOF
$before
EOF
IFS='|' read -r after_steps inserts updates deletes age <<EOF
$after
EOF

if [ "$after_steps" -le "$before_steps" ]; then
    echo "traffic did not advance: before=$before_steps after=$after_steps" >&2
    exit 1
fi

if [ "$inserts" -le 0 ] || [ "$updates" -le 0 ] || [ "$deletes" -le 0 ]; then
    echo "mixed traffic is incomplete: inserts=$inserts updates=$updates deletes=$deletes" >&2
    exit 1
fi

if [ "$age" -gt 10 ]; then
    echo "traffic heartbeat is stale: ${age}s" >&2
    exit 1
fi

echo "traffic=ok steps=$after_steps inserts=$inserts updates=$updates deletes=$deletes"
