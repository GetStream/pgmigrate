#!/bin/sh
#
# Asserts that target tuning both happens and is undone.
#
# The target container runs at stock settings, so a tuned max_wal_size and
# checkpoint_timeout can only have come from pgmigrate. The reverted check looks
# at postgresql.auto.conf rather than at the running values, because ALTER SYSTEM
# writes that file without activating anything: a setting left there is inert now
# and live after the next reload, which is exactly the kind of change an operator
# would never find.

set -eu
. "$(dirname -- "$0")/common.sh"

mode=${1:-}

auto_conf_pins() {
    target_sql -Atqc "
        SELECT coalesce(string_agg(name || '=' || setting, ',' ORDER BY name), '')
        FROM pg_file_settings
        WHERE sourcefile LIKE '%postgresql.auto.conf'"
}

case "$mode" in
applied)
    max_wal_size=$(target_sql -Atqc "SELECT current_setting('max_wal_size')")
    checkpoint_timeout=$(target_sql -Atqc "SELECT current_setting('checkpoint_timeout')")
    if [ "$max_wal_size" = "1GB" ]; then
        echo "max_wal_size is still the stock 1GB, so the target was never tuned" >&2
        exit 1
    fi
    if [ "$checkpoint_timeout" = "5min" ]; then
        echo "checkpoint_timeout is still the stock 5min, so the target was never tuned" >&2
        exit 1
    fi
    pins=$(auto_conf_pins)
    case "$pins" in
        *max_wal_size*) ;;
        *) echo "max_wal_size is not pinned in postgresql.auto.conf: '$pins'" >&2; exit 1 ;;
    esac
    echo "tuning-applied=$max_wal_size|$checkpoint_timeout"
    ;;
reverted)
    pins=$(auto_conf_pins)
    if [ -n "$pins" ]; then
        echo "cutover left target settings pinned in postgresql.auto.conf: $pins" >&2
        exit 1
    fi
    max_wal_size=$(target_sql -Atqc "SELECT current_setting('max_wal_size')")
    checkpoint_timeout=$(target_sql -Atqc "SELECT current_setting('checkpoint_timeout')")
    if [ "$max_wal_size" != "1GB" ] || [ "$checkpoint_timeout" != "5min" ]; then
        echo "target settings were not restored: max_wal_size=$max_wal_size checkpoint_timeout=$checkpoint_timeout" >&2
        exit 1
    fi
    echo "tuning-reverted=$max_wal_size|$checkpoint_timeout"
    ;;
*)
    echo "usage: assert-tuning.sh applied|reverted" >&2
    exit 1
    ;;
esac
