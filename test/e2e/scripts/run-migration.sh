#!/bin/sh

set -eu
. "$(dirname -- "$0")/common.sh"

repo_root=$(CDPATH='' cd -- "$E2E_DIR/../.." && pwd)
binary=${PGMIGRATE_BIN:-"$repo_root/pgmigrate"}
source_url=${PGMIGRATE_SOURCE:-"postgres://app:app@localhost:${SOURCE_PORT:-55432}/app?sslmode=disable"}
target_url=${PGMIGRATE_TARGET:-"postgres://app:app@localhost:${TARGET_PORT:-55433}/app?sslmode=disable"}
timeout=${MIGRATION_TIMEOUT:-300}
driver=${PGMIGRATE_DRIVER:-cli}
created_migration_dir=0
run_pid=
controller_pid=
controller_url=${PGMIGRATE_CONTROLLER_URL:-http://127.0.0.1:19188}
controller_listen=${PGMIGRATE_CONTROLLER_LISTEN:-127.0.0.1:19188}
controller_token=${PGMIGRATE_CONTROLLER_TOKEN:-pgmigrate-e2e-token}
controller_revision=

case "$driver" in
    cli|controller) ;;
    *) echo "unknown PGMIGRATE_DRIVER: $driver (want cli or controller)" >&2; exit 1 ;;
esac

if [ ! -x "$binary" ]; then
    echo "pgmigrate binary is not executable: $binary" >&2
    echo "build it first or set PGMIGRATE_BIN" >&2
    exit 1
fi

if [ -n "${MIGRATION_DIR:-}" ]; then
    migration_dir=$MIGRATION_DIR
    mkdir -p "$migration_dir"
else
    migration_dir=$(mktemp -d "${TMPDIR:-/tmp}/pgmigrate-e2e.XXXXXX")
    created_migration_dir=1
fi

tool_dir="$migration_dir/tools"
mkdir -p "$tool_dir"
make_pg_tool() {
    tool=$1
    path="$tool_dir/$tool"
    cat >"$path" <<EOF
#!/bin/bash
set -e
args=()
for arg in "\$@"; do
    args+=("\${arg//localhost/host.docker.internal}")
done
exec docker run --rm -v "$migration_dir:$migration_dir" postgres:17-bookworm "$tool" "\${args[@]}"
EOF
    chmod 700 "$path"
}
make_pg_tool pg_dump
make_pg_tool pg_restore
pg_dump_path="$tool_dir/pg_dump"
pg_restore_path="$tool_dir/pg_restore"

cleanup() {
    if [ -n "$run_pid" ] && kill -0 "$run_pid" 2>/dev/null; then
        kill "$run_pid" 2>/dev/null || true
        wait "$run_pid" 2>/dev/null || true
    fi
    if [ -n "$controller_pid" ] && kill -0 "$controller_pid" 2>/dev/null; then
        kill "$controller_pid" 2>/dev/null || true
        wait "$controller_pid" 2>/dev/null || true
    fi
    if [ "$created_migration_dir" -eq 1 ] && [ "${KEEP_MIGRATION_DIR:-0}" != "1" ]; then
        rm -rf "$migration_dir"
    else
        echo "migration directory: $migration_dir"
    fi
}
trap cleanup EXIT INT TERM

controller_status() {
    curl -fsS -H "X-PGMigrate-Token: $controller_token" "$controller_url/api/status"
}

controller_action() {
    action=$1
    curl -fsS -X POST \
        -H "X-PGMigrate-Token: $controller_token" \
        -H "X-PGMigrate-Confirm: $action" \
        -H "X-PGMigrate-Config-Revision: $controller_revision" \
        "$controller_url/api/actions/$action" >/dev/null
}

controller_configure() {
    response=$(curl -fsS -X PUT \
        -H "X-PGMigrate-Token: $controller_token" \
        -H "Content-Type: application/json" \
        --data-binary @- "$controller_url/api/config" <<EOF
{
  "source": "$source_url",
  "target": "$target_url",
  "table_filter": "",
  "workers": 2,
  "split_threshold": $split_threshold,
  "restore_jobs": 2,
  "ack_warnings": true,
  "allow_collation_change": false,
  "pg_dump_path": "$pg_dump_path",
  "pg_restore_path": "$pg_restore_path",
  "metrics": "",
  "wal_sample_duration": "250ms",
  "segment_prune_interval": "1m",
  "retry_base_copy": false,
  "skip_target_tuning": false,
  "warn_on_tuning_errors": false,
  "target_memory": "",
  "maintenance_work_mem": "",
  "max_parallel_maintenance_workers": 0,
  "max_wal_size": "",
  "checkpoint_timeout": "",
  "verify_workers": 1,
  "verify_sample_rows": 1000000,
  "verify_sample_windows": 128,
  "verify_batch_rows": 5000,
  "verify_duty_cycle": 1,
  "verify_table_timeout": "20m",
  "verify_converge_timeout": "1m",
  "verify_cdc_rows": 100000,
  "cdc_sample_rows": 100000
}
EOF
)
    case "$response" in
        *'"source_configured":true'*'"target_configured":true'*) ;;
        *) echo "controller did not accept database configuration" >&2
           printf '%s\n' "$response" >&2
           exit 1 ;;
    esac
    controller_revision=$(printf '%s\n' "$response" | sed -n 's/.*"revision":"\([^"]*\)".*/\1/p')
    if [ -z "$controller_revision" ]; then
        echo "controller configuration response did not include a revision" >&2
        printf '%s\n' "$response" >&2
        exit 1
    fi
    case "$response" in
        *"$source_url"*|*"$target_url"*)
            echo "controller configuration response exposed a DSN" >&2
            exit 1
            ;;
    esac
    for response in \
        "$(curl -fsS -H "X-PGMigrate-Token: $controller_token" "$controller_url/api/config")" \
        "$(controller_status)"
    do
        case "$response" in
            *"$source_url"*|*"$target_url"*)
                echo "controller API response exposed a DSN" >&2
                exit 1
                ;;
        esac
    done
}

controller_operation_state() {
    slot=$1
    controller_status | sed -n "s/.*\"$slot\":{[^}]*\"state\":\"\([^\"]*\)\".*/\1/p"
}

wait_controller_operation() {
    slot=$1
    action=$2
    deadline=$(( $(date +%s) + timeout ))
    while :; do
        state=$(controller_operation_state "$slot")
        case "$state" in
            succeeded) return ;;
            failed|stopped)
                echo "controller $action $state" >&2
                controller_status >&2 || true
                exit 1
                ;;
        esac
        if ! kill -0 "$controller_pid" 2>/dev/null; then
            echo "controller exited while waiting for $action" >&2
            awk '{print}' "$migration_dir/controller.log" >&2
            exit 1
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "timed out waiting for controller $action after ${timeout}s" >&2
            exit 1
        fi
        sleep 1
    done
}

"$E2E_DIR/scripts/start.sh"

# The seed is far smaller than the default 1 GiB threshold, so without this every
# table would copy as one unsplit part and the split path would never run here.
split_threshold=${SPLIT_THRESHOLD:-65536}

echo "checking the collation gate"
PGMIGRATE_BIN="$binary" PGMIGRATE_SOURCE="$source_url" \
    PGMIGRATE_PG_DUMP="$pg_dump_path" PGMIGRATE_PG_RESTORE="$pg_restore_path" \
    "$E2E_DIR/scripts/assert-collation.sh"

echo "running preflight"
if [ "$driver" = controller ]; then
    PGMIGRATE_SOURCE= PGMIGRATE_TARGET= "$binary" controller \
        --dir "$migration_dir" \
        --listen "$controller_listen" \
        --token "$controller_token" >"$migration_dir/controller.log" 2>&1 &
    controller_pid=$!
    deadline=$(( $(date +%s) + timeout ))
    until controller_status >/dev/null 2>&1; do
        if ! kill -0 "$controller_pid" 2>/dev/null; then
            echo "controller exited during startup" >&2
            awk '{print}' "$migration_dir/controller.log" >&2
            exit 1
        fi
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "timed out waiting for controller startup" >&2
            exit 1
        fi
        sleep 1
    done
    initial_config=$(curl -fsS -H "X-PGMigrate-Token: $controller_token" "$controller_url/api/config")
    case "$initial_config" in
        *'"source_configured":false'*'"target_configured":false'*) ;;
        *) echo "controller unexpectedly started with database configuration" >&2
           printf '%s\n' "$initial_config" >&2
           exit 1 ;;
    esac
    controller_configure
    controller_action preflight
    wait_controller_operation migration preflight
else
    "$binary" preflight \
        --source "$source_url" \
        --target "$target_url" \
        --dir "$migration_dir" \
        --pg-dump "$pg_dump_path" \
        --pg-restore "$pg_restore_path" \
        --wal-sample-duration 250ms \
        --ack-warnings
fi

echo "starting migration"
if [ "$driver" = controller ]; then
    controller_action run
else
    "$binary" run \
        --source "$source_url" \
        --target "$target_url" \
        --dir "$migration_dir" \
        --pg-dump "$pg_dump_path" \
        --pg-restore "$pg_restore_path" \
        --wal-sample-duration 250ms \
        --split-threshold "$split_threshold" \
        --ack-warnings >"$migration_dir/run.log" 2>&1 &
    run_pid=$!
fi

deadline=$(( $(date +%s) + timeout ))
while :; do
    if [ "$driver" = controller ]; then
        state=$(controller_operation_state migration)
        case "$state" in
            failed|stopped|succeeded)
                echo "controller run became $state before follow phase" >&2
                controller_status >&2 || true
                exit 1
                ;;
        esac
    elif ! kill -0 "$run_pid" 2>/dev/null; then
            wait "$run_pid" || true
            echo "pgmigrate run exited before follow phase" >&2
            printf '%s\n' "--- pgmigrate run log ---" >&2
            awk '{print}' "$migration_dir/run.log" >&2
            exit 1
    fi

    status=$("$binary" status --dir "$migration_dir" --json 2>/dev/null || true)
    case "$status" in
        *'"phase":"follow"'*|*'"phase": "follow"'*) break ;;
    esac

    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "timed out waiting for follow phase after ${timeout}s" >&2
        exit 1
    fi
    sleep 1
done

echo "migration reached follow phase"
# Prove the lowered threshold actually split something. A seed that grows or
# shrinks must not silently stop covering the split-part copy path, which is
# where a part is written into its table alongside its completion marker.
copied_parts=$(target_sql -Atqc "SELECT count(*) FROM pgmigrate_internal.copy_parts")
copied_tables=$(target_sql -Atqc "SELECT count(DISTINCT table_oid) FROM pgmigrate_internal.copy_parts")
if [ "$copied_parts" -le "$copied_tables" ]; then
    echo "no table was split: $copied_parts part(s) across $copied_tables table(s)" >&2
    exit 1
fi
echo "copied $copied_parts part(s) across $copied_tables table(s)"
"$E2E_DIR/scripts/assert-tuning.sh" applied
"$E2E_DIR/scripts/assert-replica-identity.sh" applied
"$E2E_DIR/scripts/assert-traffic.sh"

# Replay counters are committed in the same target transaction as the DML and
# remote LSN. They are the durable source for the dashboard's rolling changes/s
# and transactions/s rates, not an estimate from WAL bytes.
replay_stats=$(target_sql -Atqc "
    SELECT transactions_applied::text || '|' || rows_applied::text
    FROM pgmigrate_internal.replication_progress
    LIMIT 1
")
replay_txns=${replay_stats%%|*}
replay_rows=${replay_stats#*|}
if [ -z "$replay_txns" ] || [ -z "$replay_rows" ] ||
   [ "$replay_txns" -le 0 ] || [ "$replay_rows" -le 0 ]; then
    echo "replay counters did not advance: $replay_stats" >&2
    exit 1
fi
if [ "$driver" = controller ]; then
    replay_status=$(controller_status)
    controller_replay=$(printf '%s\n' "$replay_status" |
        sed -n 's/.*"apply":{[^}]*"transactions":\([0-9][0-9]*\),"rows":\([0-9][0-9]*\).*/\1|\2/p')
    controller_txns=${controller_replay%%|*}
    controller_rows=${controller_replay#*|}
    if [ -z "$controller_replay" ] || [ "$controller_txns" -le 0 ] || [ "$controller_rows" -le 0 ]; then
        echo "controller did not expose positive durable replay counters" >&2
        printf '%s\n' "$replay_status" >&2
        exit 1
    fi
fi
echo "replayed $replay_rows row changes in $replay_txns source transactions"

# The production controller runs every action as a child process. Kill the
# replay worker outright in follow, prove the HTTP controller survives, then
# resume from the exact durable target LSN/counters. This exercises a failure
# that panic recovery inside one process cannot contain.
if [ "$driver" = controller ]; then
    worker_pid=$(ps -axo pid=,ppid=,command= |
        awk -v parent="$controller_pid" '$2 == parent && /__controller-worker run/ { print $1; exit }')
    if [ -z "$worker_pid" ]; then
        echo "controller run worker process was not found" >&2
        ps -axo pid=,ppid=,command= >&2
        exit 1
    fi
    kill -KILL "$worker_pid"
    deadline=$(( $(date +%s) + timeout ))
    while :; do
        if ! kill -0 "$controller_pid" 2>/dev/null || ! controller_status >/dev/null 2>&1; then
            echo "controller did not survive replay worker failure" >&2
            awk '{print}' "$migration_dir/controller.log" >&2
            exit 1
        fi
        state=$(controller_operation_state migration)
        case "$state" in
            failed) break ;;
            stopped|succeeded)
                echo "killed replay worker became $state, want failed" >&2
                controller_status >&2 || true
                exit 1
                ;;
        esac
        if [ "$(date +%s)" -ge "$deadline" ]; then
            echo "timed out waiting for killed replay worker to fail" >&2
            exit 1
        fi
        sleep 1
    done

    if [ "${PGMIGRATE_TEST_DROP_SLOT_RESTART:-0}" = 1 ]; then
        lost_slot=$(source_sql -Atqc "
            SELECT slot_name FROM pg_catalog.pg_replication_slots
            WHERE slot_name LIKE 'pgmigrate_slot_%'
            ORDER BY slot_name LIMIT 1
        ")
        if [ -z "$lost_slot" ]; then
            echo "test logical slot was not found" >&2
            exit 1
        fi
        source_sql -Atqc "SELECT pg_catalog.pg_drop_replication_slot('$lost_slot')" >/dev/null

        # A normal resume must fail closed before scanning the local CDC queue;
        # recreating this slot at the current WAL position would lose writes.
        controller_action run
        deadline=$(( $(date +%s) + timeout ))
        while [ "$(controller_operation_state migration)" != failed ]; do
            if [ "$(date +%s)" -ge "$deadline" ]; then
                echo "timed out waiting for missing-slot resume to fail" >&2
                controller_status >&2 || true
                exit 1
            fi
            sleep 1
        done
        lost_status=$(controller_status)
        case "$lost_status" in
            *'"fresh_snapshot_required":true'*) ;;
            *) echo "controller did not classify the lost stream for fresh-snapshot recovery" >&2
               printf '%s\n' "$lost_status" >&2
               exit 1 ;;
        esac

        controller_action restart-base-copy
        deadline=$(( $(date +%s) + timeout ))
        fresh_slot_seen=0
        while :; do
            state=$(controller_operation_state migration)
            case "$state" in
                failed|stopped|succeeded)
                    echo "fresh-snapshot restart became $state before follow" >&2
                    controller_status >&2 || true
                    exit 1 ;;
            esac
            status=$("$binary" status --dir "$migration_dir" --json 2>/dev/null || true)
            slot_count=$(source_sql -Atqc "
                SELECT count(*) FROM pg_catalog.pg_replication_slots
                WHERE slot_name='$lost_slot'
            ")
            if [ "$slot_count" -eq 1 ]; then
                fresh_slot_seen=1
            fi
            case "$status" in
                *'"phase":"follow"'*|*'"phase": "follow"'*)
                    if [ "$fresh_slot_seen" -eq 1 ]; then
                        break
                    fi ;;
            esac
            if [ "$(date +%s)" -ge "$deadline" ]; then
                echo "timed out waiting for fresh-snapshot restart to reach follow" >&2
                controller_status >&2 || true
                exit 1
            fi
            sleep 1
        done
        resumed_stats=$(target_sql -Atqc "
            SELECT transactions_applied::text || '|' || rows_applied::text
            FROM pgmigrate_internal.replication_progress
            LIMIT 1
        ")
        echo "controller rejected the lost slot and restarted losslessly at $resumed_stats"
    else
        controller_action run
        sleep 1
        state=$(controller_operation_state migration)
        if [ "$state" != running ]; then
            echo "resumed replay worker is $state, want running" >&2
            controller_status >&2 || true
            exit 1
        fi
        resumed_stats=$(target_sql -Atqc "
            SELECT transactions_applied::text || '|' || rows_applied::text
            FROM pgmigrate_internal.replication_progress
            LIMIT 1
        ")
        resumed_txns=${resumed_stats%%|*}
        resumed_rows=${resumed_stats#*|}
        if [ "$resumed_txns" -lt "$replay_txns" ] || [ "$resumed_rows" -lt "$replay_rows" ]; then
            echo "replay counters regressed across worker resume: $replay_stats -> $resumed_stats" >&2
            exit 1
        fi
        echo "controller survived replay worker kill; resumed at $resumed_stats"
    fi
fi

# Verification while the source is still taking writes. A row read from a live
# source and a target that is still applying is expected to differ, and this is
# where the rule that tells that apart from a real divergence is exercised end to
# end: verify marks a source WAL position, waits for the running pgmigrate process
# to apply past it, and reads the row again. Without it, a table under write reports
# a divergence that is not there, so a failure here is a real regression rather than
# flakiness.
echo "verifying against live traffic"
applied_before=$(target_sql -Atqc "SELECT remote_lsn FROM pgmigrate_internal.replication_progress LIMIT 1")
if [ "$driver" = controller ]; then
    controller_action verify
    wait_controller_operation verification verify
    controller_status >"$migration_dir/controller-verify-live.json"
else
    if ! "$binary" verify \
        --source "$source_url" \
        --target "$target_url" \
        --dir "$migration_dir" >"$migration_dir/verify-live.json" 2>"$migration_dir/verify-live.log"; then
        echo "verification under live traffic failed" >&2
        awk '{print}' "$migration_dir/verify-live.json" >&2
        awk '{print}' "$migration_dir/verify-live.log" >&2
        exit 1
    fi
fi
applied_after=$(target_sql -Atqc "SELECT remote_lsn FROM pgmigrate_internal.replication_progress LIMIT 1")
verify_result=$migration_dir/verify-live.json
if [ "$driver" = controller ]; then
    verify_result=$migration_dir/controller-verify-live.json
fi
case $(cat "$verify_result") in
    *'"converged":true'*) ;;
    *) echo "live verification did not report convergence" >&2
       awk '{print}' "$verify_result" >&2
       exit 1 ;;
esac
# Rows have to have been read and looked up. Verification samples, so it cannot
# claim to have compared everything, but a result that reported convergence without
# reading anything would pass a bare "converged" check and prove nothing.
if [ "$driver" = controller ]; then
    claims='"sampled_rows" "estimated_rows" "cdc_observed"'
else
    claims='"source":{"pages" "estimated_rows" "target":{"batches"'
fi
for claim in $claims; do
    case $(cat "$verify_result") in
        *"$claim"*) ;;
        *) echo "live verification result is missing $claim" >&2
           awk '{print}' "$verify_result" >&2
           exit 1 ;;
    esac
done
# The rows replication wrote are checked separately, from keys the applier
# recorded as it wrote them, and the heap sample cannot reach them: it samples by
# physical position, which says nothing about when a row was written. This asserts
# the whole chain end to end — the running applier recorded keys, they reached
# state.db, and verify read them — because every part of it is silent when it
# fails, and a run that checked nothing still reports convergence.
if [ "$driver" = cli ]; then
    case $(cat "$migration_dir/verify-live.log") in
        *'applied rows checked'*) ;;
        *) echo "verification did not check the replication path: no applier-recorded keys reached it" >&2
           awk '{print}' "$migration_dir/verify-live.log" >&2
           exit 1 ;;
    esac
fi
# The fixture's keyless table cannot be checked at all: a sampled row is found on
# the target by key. That has to be said out loud and must not fail the run, or a
# cutover would be blocked for good by a table nothing can verify.
if [ "$driver" = cli ]; then
    case $(cat "$migration_dir/verify-live.json") in
        *'was not compared'*) ;;
        *) echo "the keyless fixture table was not reported as skipped" >&2
           awk '{print}' "$migration_dir/verify-live.json" >&2
           exit 1 ;;
    esac
fi
# Verification must never hold apply. Traffic is running throughout, so apply has to
# have advanced across the comparison rather than only after it.
if [ "$applied_before" = "$applied_after" ]; then
    echo "target apply did not advance while verifying: still at $applied_after" >&2
    exit 1
fi
echo "apply advanced during verification: $applied_before -> $applied_after"

"$E2E_DIR/scripts/pause-traffic.sh"

# Verification is the operator's decision to make before cutting over, and this is
# where the fixture makes it: cutover itself checks nothing, so a divergence found
# here is the only thing standing between a bad copy and production.
echo "running verification before cutover"
if [ "$driver" = controller ]; then
    controller_action verify
    wait_controller_operation verification verify
else
    "$binary" verify \
        --source "$source_url" \
        --target "$target_url" \
        --dir "$migration_dir" >/dev/null
fi

"$binary" cutover \
    --source "$source_url" \
    --target "$target_url" \
    --dir "$migration_dir"

if [ "$driver" = controller ]; then
    wait_controller_operation migration run
else
    if ! wait "$run_pid"; then
        echo "pgmigrate run did not exit cleanly after cutover" >&2
        awk '{print}' "$migration_dir/run.log" >&2
        exit 1
    fi
    run_pid=
fi
if [ "$(source_sql -Atqc "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgmigrate_slot_%'")" -ne 0 ]; then
    echo "migration replication slot was not cleaned up" >&2
    exit 1
fi
if [ "$(target_sql -Atqc "SELECT to_regnamespace('pgmigrate_internal') IS NULL")" != "t" ]; then
    echo "target migration metadata was not cleaned up" >&2
    exit 1
fi
"$E2E_DIR/scripts/assert-tuning.sh" reverted
"$E2E_DIR/scripts/assert-replica-identity.sh" reverted
"$binary" cutover \
    --source "$source_url" \
    --target "$target_url" \
    --dir "$migration_dir" >/dev/null
"$E2E_DIR/scripts/assert-data.sh"

echo "migration-e2e=ok"
