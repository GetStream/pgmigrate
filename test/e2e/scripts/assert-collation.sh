#!/bin/sh
#
# Asserts the collation gate on databases a real server created.
#
# Two cases matter and they pull in opposite directions. One locale reaches
# pgmigrate under more than one name, so a target that spells the source's own
# locale differently has to be accepted: RDS reports en_US.UTF-8 where Cloud SQL
# reports en_US.UTF8, and refusing that pair would teach operators to pass
# --allow-collation-change without reading it. A target that genuinely collates
# differently has to be refused until they do pass it.
#
# Both databases are created on the target container, so the locale is the only
# thing that differs between them and the target the migration will really use.
# The refusal is also checked for stopping preflight where it happens, since the
# check that follows it samples the WAL rate for a minute.

set -eu
. "$(dirname -- "$0")/common.sh"

repo_root=$(CDPATH='' cd -- "$E2E_DIR/../.." && pwd)
binary=${PGMIGRATE_BIN:-"$repo_root/pgmigrate"}
source_url=${PGMIGRATE_SOURCE:-"postgres://app:app@localhost:${SOURCE_PORT:-55432}/app?sslmode=disable"}
pg_dump_path=${PGMIGRATE_PG_DUMP:-pg_dump}
pg_restore_path=${PGMIGRATE_PG_RESTORE:-pg_restore}

alias_db=collation_alias
other_db=collation_other

cleanup() {
    for name in "$alias_db" "$other_db"; do
        target_sql -Atqc "DROP DATABASE IF EXISTS $name" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT INT TERM

# template0 is required, because a new database may only take a locale differing
# from its template's.
create_database() {
    target_sql -Atq \
        -c "SET client_min_messages=warning" \
        -c "DROP DATABASE IF EXISTS $1" >/dev/null
    target_sql -Atqc "CREATE DATABASE $1 TEMPLATE template0 ENCODING 'UTF8'
        LC_COLLATE '$2' LC_CTYPE '$3'" >/dev/null
}

target_url_for() {
    printf 'postgres://app:app@localhost:%s/%s?sslmode=disable' "${TARGET_PORT:-55433}" "$1"
}

run_preflight() {
    dir=$(mktemp -d "${TMPDIR:-/tmp}/pgmigrate-collation.XXXXXX")
    preflight_status=0
    # shellcheck disable=SC2086
    "$binary" preflight \
        --source "$source_url" \
        --target "$(target_url_for "$1")" \
        --dir "$dir" \
        --pg-dump "$pg_dump_path" \
        --pg-restore "$pg_restore_path" \
        --wal-sample-duration 250ms \
        --ack-warnings ${2:-} >"$dir/preflight.log" 2>&1 || preflight_status=$?
    preflight_output=$(cat "$dir/preflight.log")
    rm -rf "$dir"
}

source_collate=$(source_sql -Atqc \
    "SELECT datcollate FROM pg_database WHERE datname=current_database()")
case "$source_collate" in
    *.utf8) alias_collate="${source_collate%.utf8}.UTF-8" ;;
    *.UTF-8) alias_collate="${source_collate%.UTF-8}.utf8" ;;
    *)
        echo "source locale '$source_collate' has no UTF-8 spelling to vary" >&2
        exit 1
        ;;
esac

create_database "$alias_db" "$alias_collate" "$alias_collate"
run_preflight "$alias_db"
if [ "$preflight_status" -ne 0 ]; then
    echo "preflight refused '$alias_collate' against the source's own '$source_collate'" >&2
    printf '%s\n' "$preflight_output" >&2
    exit 1
fi
case "$preflight_output" in
    *collation-*)
        echo "an alias of the source locale was reported as a collation finding" >&2
        printf '%s\n' "$preflight_output" >&2
        exit 1
        ;;
esac
echo "collation-alias-accepted=$source_collate|$alias_collate"

create_database "$other_db" C C
run_preflight "$other_db"
if [ "$preflight_status" -eq 0 ]; then
    echo "preflight accepted a target collating with C" >&2
    printf '%s\n' "$preflight_output" >&2
    exit 1
fi
for expected in collation-locale "$source_collate" --allow-collation-change "stopped here"; do
    case "$preflight_output" in
        *"$expected"*) ;;
        *)
            echo "the refusal does not mention '$expected'" >&2
            printf '%s\n' "$preflight_output" >&2
            exit 1
            ;;
    esac
done
# Stopping at the collation check is the point of running it first: the check
# after it is the one that samples the WAL rate, and its finding is the marker
# that it ran.
case "$preflight_output" in
    *wal-retention*)
        echo "preflight went on sampling the WAL rate after refusing the collation" >&2
        printf '%s\n' "$preflight_output" >&2
        exit 1
        ;;
esac

run_preflight "$other_db" --allow-collation-change
if [ "$preflight_status" -ne 0 ]; then
    echo "--allow-collation-change did not allow a C target" >&2
    printf '%s\n' "$preflight_output" >&2
    exit 1
fi
case "$preflight_output" in
    *info\ collation-locale*) ;;
    *)
        echo "the allowed change is not reported at all" >&2
        printf '%s\n' "$preflight_output" >&2
        exit 1
        ;;
esac

echo "collation-change-refused-then-allowed=ok"
