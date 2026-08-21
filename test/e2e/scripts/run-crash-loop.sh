#!/bin/sh
set -eu
. "$(dirname -- "$0")/common.sh"

repo_root=$(CDPATH='' cd -- "$E2E_DIR/../.." && pwd)
binary=${PGMIGRATE_BIN:-"$repo_root/pgmigrate"}
source_url=${PGMIGRATE_SOURCE:-"postgres://app:app@localhost:${SOURCE_PORT:-55432}/app?sslmode=disable"}
target_url=${PGMIGRATE_TARGET:-"postgres://app:app@localhost:${TARGET_PORT:-55433}/app?sslmode=disable"}
migration_dir=$(mktemp -d "${TMPDIR:-/tmp}/pgmigrate-crash-e2e.XXXXXX")
run_pid=

cleanup() {
    if [ -n "$run_pid" ] && kill -0 "$run_pid" 2>/dev/null; then
        kill "$run_pid" 2>/dev/null || true
        wait "$run_pid" 2>/dev/null || true
    fi
    if [ "${KEEP_MIGRATION_DIR:-0}" != "1" ]; then
        rm -rf "$migration_dir"
    else
        echo "migration directory: $migration_dir"
    fi
}
trap cleanup EXIT INT TERM

tool_dir="$migration_dir/tools"
mkdir -p "$tool_dir"
make_pg_tool() {
    tool=$1
    path="$tool_dir/$tool"
    cat >"$path" <<EOF
#!/bin/bash
set -e
args=()
for arg in "\$@"; do args+=("\${arg//localhost/host.docker.internal}"); done
exec docker run --rm -v "$migration_dir:$migration_dir" postgres:17-bookworm "$tool" "\${args[@]}"
EOF
    chmod 700 "$path"
}
make_pg_tool pg_dump
make_pg_tool pg_restore

# The seed is far smaller than the default 1 GiB threshold, so without this every
# table would copy as one unsplit part and the split path would never run here.
split_threshold=${SPLIT_THRESHOLD:-65536}

common_args="--source $source_url --target $target_url --dir $migration_dir --pg-dump $tool_dir/pg_dump --pg-restore $tool_dir/pg_restore --wal-sample-duration 250ms --split-threshold $split_threshold --ack-warnings"

"$E2E_DIR/scripts/start.sh"
# shellcheck disable=SC2086
"$binary" preflight $common_args

# Resuming into a phase re-runs the schema archive steps, each of which is a
# separate containerized client-tool invocation, before any resume work starts.
# The budget therefore has to absorb container startup latency; too tight a value
# fails on a slow host rather than on a resume defect.
phase_timeout=${PHASE_TIMEOUT:-60}

wait_phase() {
    wanted=$1
    attempts=0
    while [ "$attempts" -lt "$phase_timeout" ]; do
        status=$("$binary" status --dir "$migration_dir" --json 2>/dev/null || true)
        case "$status" in
            *"\"phase\": \"$wanted\""*) return 0 ;;
        esac
        if ! kill -0 "$run_pid" 2>/dev/null; then
            wait "$run_pid" || true
            echo "run exited while waiting for $wanted" >&2
            awk '{print}' "$migration_dir/run.log" >&2
            return 1
        fi
        attempts=$((attempts + 1))
        sleep 1
    done
    echo "phase $wanted not reached in ${phase_timeout}s" >&2
    awk '{print}' "$migration_dir/run.log" >&2
    return 1
}

crash_at() {
    phase=$1
    echo "crash-loop: starting for $phase"
    # shellcheck disable=SC2086
    PGMIGRATE_TEST_PAUSE_PHASE=$phase "$binary" run $common_args >>"$migration_dir/run.log" 2>&1 &
    run_pid=$!
    wait_phase "$phase"
    kill -9 "$run_pid"
    wait "$run_pid" 2>/dev/null || true
    run_pid=
    echo "crash-loop: killed at $phase"
}

crash_at copy
crash_at indexes
crash_at catchup
crash_at follow
replay_before_resume=$(target_sql -Atqc "
    SELECT transactions_applied::text || '|' || rows_applied::text
    FROM pgmigrate_internal.replication_progress
    LIMIT 1
")
before_txns=${replay_before_resume%%|*}
before_rows=${replay_before_resume#*|}

# shellcheck disable=SC2086
"$binary" run $common_args >>"$migration_dir/run.log" 2>&1 &
run_pid=$!
wait_phase follow
replay_after_resume=$(target_sql -Atqc "
    SELECT transactions_applied::text || '|' || rows_applied::text
    FROM pgmigrate_internal.replication_progress
    LIMIT 1
")
after_txns=${replay_after_resume%%|*}
after_rows=${replay_after_resume#*|}
if [ "$after_txns" -lt "$before_txns" ] || [ "$after_rows" -lt "$before_rows" ]; then
    echo "replay counters regressed across resume: $replay_before_resume -> $replay_after_resume" >&2
    exit 1
fi
echo "replay counters survived resume: $replay_before_resume -> $replay_after_resume"
# Four kills and four resumes have each re-derived the tuning. The target must
# still be tuned, and the recorded originals must still be the pre-migration
# values rather than a bulk-load value recorded over them by a resume.
"$E2E_DIR/scripts/assert-tuning.sh" applied
# Same reasoning for the replica identities: each resume re-inspects the source,
# and by now the needy relations are already at FULL. A resume that recorded FULL
# as the original would revert to it and leave production inflated for good.
"$E2E_DIR/scripts/assert-replica-identity.sh" applied
"$E2E_DIR/scripts/pause-traffic.sh"
"$binary" verify --source "$source_url" --target "$target_url" --dir "$migration_dir" >/dev/null
"$binary" cutover --source "$source_url" --target "$target_url" --dir "$migration_dir" >/dev/null
wait "$run_pid"
run_pid=
"$E2E_DIR/scripts/assert-tuning.sh" reverted
"$E2E_DIR/scripts/assert-replica-identity.sh" reverted
"$E2E_DIR/scripts/assert-data.sh"
echo "migration-crash-e2e=ok"
