# pgmigrate

## Introduction

pgmigrate copies one live PostgreSQL database into another and follows logical
changes until an operator-controlled cutover. It is a single Go process that
supervises the whole procedure and can be stopped and restarted at any point.

The offline version of this job is `pg_dump | pg_restore`. That produces a copy
of one moment, and a database still taking writes has moved on by the time the
restore finishes. Closing the gap by hand means creating a replication slot
before a snapshot is exported, restoring the schema against that snapshot,
copying every table inside it, replaying everything written since, and keeping
enough state on durable disk that a crash halfway through does not force a
restart from zero. pgmigrate is that procedure, made restartable and auditable.

pgmigrate uses `pg_dump` and `pg_restore` for the schema parts of the work and
implements its own data copy, streaming each table part directly from a source
`COPY` into a target `COPY` with no intermediate file. It holds the exported
snapshot open for the whole base copy, and captures every change made while that
copy runs.

pgmigrate migrates one database per run, on PostgreSQL 16, 17, or 18, where the
target major version is the same as or newer than the source's.

## Base copy and change data capture

`pgmigrate run` drives six phases in one process and stays in the last of them
until cutover completes:

1. **setup** creates a deterministic publication and logical slot on the source
   with an exported snapshot. A command-idle connection holds the snapshot open
   while a second connection watches that backend.
2. **schema** filters the `pg_dump` custom-format archive to the durable
   selected-table inventory and its cataloged dependencies, and restores the
   pre-data section in `pg_dump`'s own dependency order.
3. **copy** streams snapshot-consistent table parts from source `COPY` to target
   `COPY`. A table larger than `--split-threshold` is split into at most
   `--workers` parts. Equal-major migrations whose columns all use allowlisted
   built-in types copy in binary; everything else copies as text.
4. **indexes** restores the managed indexes and constraints, driving the builds
   itself so they run concurrently, restores the deferred post-data objects one
   at a time behind durable markers, and vacuums the loaded tables.
5. **catchup** applies the transactions staged during the copy with atomic
   progress and source-order fallbacks.
6. **follow** applies live changes and watches source and slot health.

Change data capture uses `pgoutput`, the logical decoding plugin built into
PostgreSQL, so there is no extension to install on the source. Decoded
transactions are written to append-only checksummed segment files under the
migration directory and fsynced at each transaction boundary; a transaction over
256 MiB spills to temporary files beneath `cdc/spill`. Apply is serial, and
target DML commits in the same transaction as
`pgmigrate_internal.replication_progress` on the target, which is the
authoritative apply position. Finalized segments that have been applied are
pruned every `--segment-prune-interval`, retaining one safety segment.

`pgmigrate cutover` then emits a logical boundary message, drains exactly through
it, advances target sequences with headroom, reverts what the migration changed on
both servers, and atomically writes `cutover-report.json`. It moves data and
metadata and judges neither: freezing application writes and deciding the copy is
good enough to serve are the operator's, done before it runs.

`pgmigrate sequences` runs that one step on its own, from `follow` onwards, for a
cutover that moves traffic before it moves the database. Sequences are set
absolutely, so it is rerunnable and `cutover` redoes it against the source's final
values. `--sequence-offset` is the room the source keeps: whatever it allocates
beyond that collides with the target.

## Documentation

This README is the documentation. [Design considerations](#design-considerations-why-oh-why)
explains why the mechanism is what it is and what each choice cost,
[Verification](#verification) what a check does and does not establish,
[Crash recovery](#crash-recovery) what a restart keeps and what it discards, and
[Limitations](#limitations) what the tool does not do. Test patterns and
environment controls are in [test/README.md](test/README.md).

There are seven commands:

| command | what it does |
|---|---|
| `preflight` | checks whether a migration can succeed, and persists its findings |
| `run` | starts or resumes the migration, and waits in `follow` until cutover completes |
| `status` | reads local state only, so it is safe to run beside `run` |
| `controller` | serves a guarded local dashboard for status, preflight, run, and verification |
| `verify` | samples each table against the target and checks what replication wrote |
| `sequences` | advances target sequences alone, so the target can take writes before the cutover |
| `cutover` | performs the rerunnable, durably stepped cutover |

Every command takes `--dir`. Database actions need source and target connection
strings; `status` and the controller's read-only dashboard do not. `pgmigrate
<command> --help` prints the defaults as resolved on the host, which for
`--workers` and `--restore-jobs` depend on its CPU count.

## Example

```bash
$ export PGMIGRATE_SOURCE="postgres://migrator@source.host.dev/app?sslmode=verify-full"
$ export PGMIGRATE_TARGET="postgres://migrator@target.host.dev/app?sslmode=verify-full"

$ pgmigrate preflight --dir ./migration
```

Preflight prints its findings, and exits non-zero while any warning is
unacknowledged:

```
warning table-16582-replica-identity: e2e.audit_log (16384 bytes, 60 recorded UPDATE/DELETE rows) cannot identify UPDATE/DELETE rows: it has no valid primary key, and a unique index is not a replica identity until REPLICA IDENTITY USING INDEX designates it, so pgmigrate will set REPLICA IDENTITY FULL on it for the duration of the migration and restore the original at cleanup. Until then every source UPDATE/DELETE on it writes all old column values to WAL and apply matches target rows on all columns. Adding a primary key, or designating a unique non-partial index over NOT NULL columns with REPLICA IDENTITY USING INDEX, avoids that
info target-tuning: target will be tuned for the bulk load and restored at cutover, sized for 512MB of memory (estimated from shared_buffers): max_parallel_maintenance_workers 2 to 4 (session), synchronous_commit on to off (session), max_wal_size 1GB to 64GB (system), checkpoint_timeout 5min to 30min (system)
```

With those understood, start the migration. It keeps running after the base copy
finishes, following changes until you cut over:

```bash
$ pgmigrate run --dir ./migration --ack-warnings --workers 8 --restore-jobs 4 --metrics :9187
```

`run` writes almost nothing to the terminal. Phase, progress, health, and error
events go to `log/pgmigrate.log` inside the migration directory as JSON:

```
{"event":"phase","phase":"setup","time":"2026-08-11T07:39:17.978941Z"}
{"event":"phase","phase":"schema","time":"2026-08-11T07:39:18.088792Z"}
{"event":"phase","phase":"copy","time":"2026-08-11T07:39:19.227451Z"}
{"event":"phase","phase":"indexes","time":"2026-08-11T07:39:19.37051Z"}
{"event":"phase","phase":"catchup","time":"2026-08-11T07:39:21.569899Z"}
{"event":"phase","phase":"follow","time":"2026-08-11T07:39:21.912798Z"}
{"catchup_boundary":"0/1B11730","event":"phase","phase":"follow","time":"2026-08-11T07:39:21.912931Z"}
```

From another terminal, `status` reads that state without touching either
database. This one is a finished migration, with most of its per-table
verification lines removed:

```
$ pgmigrate status --dir ./migration
phase: complete
end position: 0/1BE9D08
tables: 12/12
parts: 21/21
indexes: 25/25
constraints: 10/10
verify: 12/12
verify e2e.accounts: done 100/100 rows sampled (100.00%), 2/2 source pages, 100 target rows, 78/78 applied rows checked
verify e2e.audit_log: done 166/166 rows sampled (100.00%), 2/2 source pages, 166 target rows, 165/165 applied rows checked
verify e2e.metrics: done 0/0 rows sampled (0.00%), 0/0 source pages, 0 target rows
verify e2e.order_items: done 3059/3059 rows sampled (100.00%), 24/24 source pages, 3059 target rows, 123/123 applied rows checked
findings: 4 open
steps: 33 complete
apply: 0/1BE9D08 staged, 0/1BE9D08 applied, 0 txns, 0 rows
lag: 0 bytes, 30.489s stale
```

`verify` can be run whenever you want an answer, including while the source is
still taking writes. It prints a line per table as it works, and a summary that
reports its two checks separately. Again with most tables removed:

```
$ pgmigrate verify --dir ./migration >verify.json
verify e2e.orders: sampling 0 of 1115 rows (0.0%), 0 pages read, measuring rate
verify e2e.orders: checking cdc, 119 of 119 applied rows
verify e2e.orders: done 1115 of 1115 rows (100.0%), 12 pages read, measuring rate
verify e2e.metrics: done 0 of 0 rows (0.0%), 0 pages read, measuring rate
warning: "e2e"."metrics" was not compared, because it has no primary key and no usable unique index, and a sampled row can only be found on the target by key
verified 11 tables: 5236 of 5236 rows sampled, 978 of 978 applied rows checked (101 deletions), 0 divergent, 1 not compared for want of a key
```

The progress lines and the summary go to standard error; the full result goes to
standard output as JSON, which is why the command above redirects it to a file.

Finally, freeze application writes on the source and cut over. `run` must still
be following, because it is the process that drains the stream:

```bash
$ pgmigrate cutover --dir ./migration
```

Cutover prints the report it wrote to `cutover-report.json`. Formatted, with the
step log trimmed:

```json
{
  "version": 1,
  "tool_version": "v0.1.0",
  "completed_at": "2026-08-11T07:40:10.07482Z",
  "end_position": "0/1BE9D08",
  "sequences": [
    {"schema": "e2e", "name": "order_id_seq", "source_value": 1143, "target_value": 1001143, "is_called": true}
  ],
  "configuration": {"sequence_offset": 1000000, "values": {"workers": "16"}},
  "steps": []
}
```

`tool_version` is the build that ran the cutover: a released binary names its
version, and one built from a checkout names the commit it came from and whether
that tree was clean. `end_position` is the boundary the target was drained
through, and it is the line between what this migration carried and what it did
not: anything the source wrote after it stayed behind. `sequences` records that
`order_id_seq` was left 1,000,000 ahead of the source, so the application cannot
collide with an existing key. `steps` is the durable log the cutover resumed
against.

## Installing pgmigrate

With Go 1.25 or newer, from a checkout:

```bash
$ go build -o pgmigrate ./cmd/pgmigrate
```

Or without one:

```bash
$ go install github.com/GetStream/pgmigrate/cmd/pgmigrate@main
```

## Commands

Every flag is defined once, on the root command, so any command accepts any
flag. The table for each command lists the flags that command acts on; the
others are ignored. `--source` and `--target` default to `PGMIGRATE_SOURCE` and
`PGMIGRATE_TARGET`, and are never written to disk.

### pgmigrate preflight

Inventories the selected tables and checks server versions, logical-replication
settings, replica identity, sequence headroom, WAL headroom, collations,
extensions, target state, privileges, and client-tool versions. Findings are
persisted in the migration directory, so `status` shows them later. Warnings block
until acknowledged, and `run` repeats the same checks with the same gate.

Sequence headroom is checked against `--sequence-offset`, for every sequence a
selected table owns or draws a column default from. A sequence with fewer than ten
million values left before its maximum, or its minimum when it counts down, is a
warning: what remains has to cover both databases until traffic moves. One with
less room than the offset is an error, because `setval` refuses a value past the
bound and the cutover would fail at its sequence step.

Inherited `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`,
and `idle_session_timeout` on the source or target are warnings. pgmigrate sets
those GUCs to 0 on every SQL session it opens, including `pg_dump` and
`pg_restore`, so a COPY that would otherwise die at 60s can finish. The warning
is so the inherited values are visible before a long run, not a request to
`ALTER ROLE` for pgmigrate.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory, created if absent |
| `--source <dsn>` | `PGMIGRATE_SOURCE` | source connection string |
| `--target <dsn>` | `PGMIGRATE_TARGET` | target connection string |
| `--table-filter <file>` | none, meaning every ordinary table | ordered include/exclude `schema.table` globs; see [Table filters](#table-filters) |
| `--ack-warnings` | false | accept every current warning, so preflight exits zero. Does not cover a collation change |
| `--allow-collation-change` | false | proceed to a target that collates text differently from the source; see [Collation](#collation) |
| `--pg-dump <path>` | found on `PATH` | `pg_dump` executable, whose version is checked here |
| `--pg-restore <path>` | found on `PATH` | `pg_restore` executable, whose version is checked here |
| `--wal-sample-duration <duration>` | `1m` | how long to sample the source WAL rate when judging slot retention headroom |
| `--sequence-offset <n>` | `1000000` | the gap the cutover will leave, which is the room each selected sequence is checked for |
| `--workers <n>` | host CPU count | index-build concurrency the tuning plan is sized for, so preflight reports the plan `run` would apply |
| `--skip-target-tuning` | false | report no tuning plan, because the run will not tune |
| `--target-memory <size>` | estimated from `shared_buffers` | target memory the plan is sized against, for example `64GB` |
| `--maintenance-work-mem <size>` | derived | plan this value per index-build session instead of deriving one |
| `--max-parallel-maintenance-workers <n>` | derived | plan this value per index-build session instead of deriving one |
| `--max-wal-size <size>` | derived | plan this `max_wal_size` for the bulk load |
| `--checkpoint-timeout <duration>` | derived | plan this `checkpoint_timeout` for the bulk load |

### pgmigrate run

Starts a new migration or resumes an existing one, and does not return until
cutover completes or something fails. One `run` process owns the migration
directory's writer lock.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory; bound to the source cluster, database, and filter |
| `--source <dsn>` | `PGMIGRATE_SOURCE` | source connection string |
| `--target <dsn>` | `PGMIGRATE_TARGET` | target connection string |
| `--table-filter <file>` | none | ordered include/exclude globs. On a resume this must match the run that created the directory |
| `--ack-warnings` | false | accept every current preflight warning, including consenting to `REPLICA IDENTITY FULL` where it is needed |
| `--allow-collation-change` | false | proceed to a target that collates text differently from the source |
| `--workers <n>` | host CPU count | parallel copy and index-build workers, and the cap on parts per table |
| `--split-threshold <bytes>` | `1073741824` (1 GiB) | desired bytes per copy part. A table is split into at most `--workers` parts, so a table far larger than the threshold produces larger parts |
| `--restore-jobs <n>` | half the host CPU count, at least 1 | parallel `pg_restore` jobs for the schema restore |
| `--pg-dump <path>` | found on `PATH` | `pg_dump` executable |
| `--pg-restore <path>` | found on `PATH` | `pg_restore` executable |
| `--metrics <address>` | off | serve Prometheus metrics at `/metrics` on this address, for example `:9187` |
| `--segment-prune-interval <duration>` | `1m` | minimum interval between passes that delete applied CDC segments |
| `--wal-sample-duration <duration>` | `1m` | source WAL-rate sample for the preflight checks `run` repeats |
| `--retry-base-copy` | false | restart the base copy even though the last attempts failed the same way; see [Restarting a failed base copy](#restarting-a-failed-base-copy) |
| `--cdc-sample-rows <n>` | `100000` | applied keys the applier keeps per relation, so `verify` can check the replication path. `0` records none |
| `--endpos <LSN>` | none | inclusive end position for the stream. Must resolve to an exact durable transaction or boundary, or it is rejected |
| `--skip-target-tuning` | false | leave target settings alone during the bulk load |
| `--warn-on-tuning-errors` | false | continue when a setting cannot be changed, instead of stopping and undoing the tuning |
| `--target-memory <size>` | estimated from `shared_buffers` | target memory the tuning is sized against |
| `--maintenance-work-mem <size>` | derived | apply this value per index-build session |
| `--max-parallel-maintenance-workers <n>` | derived | apply this value per index-build session |
| `--max-wal-size <size>` | derived | apply this `max_wal_size` for the bulk load |
| `--checkpoint-timeout <duration>` | derived | apply this `checkpoint_timeout` for the bulk load |

### pgmigrate status

Opens `state.db` read-only and reports progress, so it is safe to run repeatedly
beside an active `run`. It needs no database connection and no DSNs.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory to read |
| `--json` | false | render the snapshot as JSON instead of text |
| `--watch <duration>` | off | re-render at this interval until interrupted. Must be zero or at least `10ms` |

### pgmigrate controller

Serves an embedded web dashboard backed by the same durable state as `status`.
It shows the lifecycle stage, exact object completion counts, copied rows and
bytes, apply lag and staleness, per-table verification coverage and rates,
findings, failures, and action output. The lifecycle bar is stage progress, not
an elapsed-time estimate; the object and verification bars use the recorded
completed and total work.

The controller starts idle. After authentication, the dashboard loads all
non-secret preflight, run, copy, tuning, and verification defaults. Save a valid
configuration before using an action. Controls track the durable lifecycle and
remain disabled while configuration has unsaved changes, when an action is not
valid, or when the migration is complete. Configuration is locked while either
a migration or verification operation is active. Verification is permitted only
while `run` is following.

Source and target DSNs can be supplied through the dashboard, but are
write-only: config and status API responses contain only configured/not-configured
flags. The password inputs are cleared after every save or reload, and DSNs are
never placed in browser storage, migration state, or logs. Controller token,
listener, and migration directory remain startup-only. The dashboard deliberately
does not expose `sequences` or `cutover`; starting a migration still requires an
explicit in-page browser confirmation and creates or reuses logical-replication
state on the source.

```bash
$ pgmigrate controller --dir ./migration
pgmigrate controller listening on http://127.0.0.1:9188
```

The default listener is loopback-only. For a pod, bind to all interfaces and
provide a token through a secret, then use a port-forward or another
authenticated private path to reach it:

```bash
$ export PGMIGRATE_CONTROLLER_TOKEN="$(secret-tool-or-platform-command)"
$ pgmigrate controller --dir /work/migration --listen :9188
```

The browser sends the token in `X-PGMigrate-Token`; only this controller token is
kept in the tab's session storage, and it is not written into migration state. A
non-loopback listener is rejected when no token is configured.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory to display and control |
| `--listen <address>` | `127.0.0.1:9188` | HTTP listen address |
| `--token <value>` | `PGMIGRATE_CONTROLLER_TOKEN` | required for any non-loopback listener |
| `--source <dsn>` | `PGMIGRATE_SOURCE` | optional initial source connection string; it can instead be entered write-only in the dashboard |
| `--target <dsn>` | `PGMIGRATE_TARGET` | optional initial target connection string; it can instead be entered write-only in the dashboard |

Run the isolated authenticated-controller migration test with
`make controller-e2e`. It drives preflight, run, live and final verification
through the API, keeps cutover CLI-only, and independently compares source and
target contents.

### pgmigrate verify

Samples each selected table on the source and looks those rows up on the target,
and separately checks the rows replication reported writing. See
[Verification](#verification) for what a pass does and does not mean. The result
is written to standard output as JSON; progress, warnings, and the summary go to
standard error. A named divergence, or a table stopped early, exits non-zero.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory holding the table inventory |
| `--source <dsn>` | `PGMIGRATE_SOURCE` | source connection string |
| `--target <dsn>` | `PGMIGRATE_TARGET` | target connection string |
| `--verify-workers <n>` | `1` | tables checked in parallel. Each one reads the live source, which is why this is not `--workers` |
| `--verify-sample-rows <n>` | `1000000` | rows per table read from the source and looked up on the target. A table smaller than this is read whole. `0` is rejected: there is no exhaustive mode |
| `--verify-sample-windows <n>` | `128` | page intervals those rows are drawn from, spread across the heap with the last pinned to its end |
| `--verify-batch-rows <n>` | `5000` | keys per target lookup statement, clamped down for a very wide key to stay under the bind-parameter limit |
| `--verify-duty-cycle <fraction>` | `1` | fraction of the wall clock verification may spend querying, sleeping between windows to stay under it. Must be greater than 0 and at most 1 |
| `--verify-table-timeout <duration>` | `20m` | time one table's check may take. `0` disables it. A table stopped here reports incomplete and cannot report convergence |
| `--verify-converge-timeout <duration>` | `1m` | how long a row that appears to differ is given to settle against a fixed WAL position before it is reported |
| `--verify-cdc-rows <n>` | `100000` | applier-recorded keys per table checked alongside the heap sample. `0` falls back to the default |

### pgmigrate sequences

Runs the cutover's sequence step on its own, from the `follow` phase onwards, for
a cutover that moves traffic before it moves the database. It reads each selected
sequence's next value on the source and sets the target's copy that far past it, so
the target can accept writes while the source is still serving. Every sequence a
selected table owns or draws a column default from is included. The results are
written to standard output as JSON.

Values are set absolutely rather than advanced, so running it again is harmless,
and `cutover` runs the same step against the source's final values. The offset is
the room the source keeps: whatever it allocates beyond that collides with what the
target has already handed out, so size it above what the source can consume before
traffic moves.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory holding the schema selection |
| `--source <dsn>` | `PGMIGRATE_SOURCE` | source connection string |
| `--target <dsn>` | `PGMIGRATE_TARGET` | target connection string |
| `--sequence-offset <n>` | `1000000` | values each target sequence is set past the source's. `0` leaves no gap, which is only safe once the source will never allocate again |

### pgmigrate cutover

Performs the cutover as a sequence of durably recorded steps: validate the
target identity, emit and record an end position, wait for `run` to drain exactly
through it, advance sequences, clean up, and write the report. Re-running resumes
at the first incomplete step and reuses the recorded end position. It requires an
active `run` in the `follow` phase, and prints the report as JSON when it
finishes. Run against an already complete migration, it re-prints the stored
report.

**It checks nothing.** Writes made on the source after the end position are not
migrated and nothing here will notice, so freeze application writes before running
it. It does not verify: run `pgmigrate verify` while `run` is still following, read
the result, and decide for yourself.

| flag | default | what it does |
|---|---|---|
| `--dir <path>` | required | migration state directory |
| `--source <dsn>` | `PGMIGRATE_SOURCE` | source connection string |
| `--target <dsn>` | `PGMIGRATE_TARGET` | target connection string |
| `--endpos <LSN>` | the boundary cutover emits | explicit inclusive end position, for advanced use. Must resolve to an exact durable transaction or boundary |
| `--sequence-offset <n>` | `1000000` | values each target sequence is set past the source's; see [pgmigrate sequences](#pgmigrate-sequences) |
| `--no-cleanup` | false | retain the source replication objects and target migration metadata. Target tuning and target replica identities are still reverted, because the target is about to serve production |

## Dependencies

At run time pgmigrate needs `pg_dump` and `pg_restore` on the `PATH`, or their
paths given with `--pg-dump` and `--pg-restore`. Their major version must be at
least the source's; preflight checks this and says so when it is not.

The source must run with `wal_level=logical` and have a free replication slot
and walsender, WAL retention and disk to cover the run, and a role that can
replicate, create a publication, and read every selected table. Preflight proves
the replication right by opening a replication connection rather than by reading
role attributes, so managed services work: grant `rds_replication` on Amazon RDS
or Aurora, or `ALTER USER <role> WITH REPLICATION` on Cloud SQL. A non-superuser
role is enough.

The target must be fresh, and its role able to create schemas and tables,
restore objects, write data, and set `session_replication_role=replica`.
Replication-origin privileges are not required.

The host needs durable local disk for CDC staged but not yet applied during
copy, indexing, catch-up, and follow, plus temporary space for exceptionally
large source transactions.

DDL is neither replicated nor detected, so the source schema must be frozen for
the whole run. Application writes may continue until you freeze them for cutover.

## Table filters

`--table-filter` takes one `schema.table` glob per line:

```text
# Include application tables.
public.*
sales.orders
!public.audit_*
```

Blank lines and comments are ignored, and the last matching rule wins. With any
positive rule, unmatched tables are excluded; with only exclusions, unmatched
tables are included. Selection carries every descendant of a partitioned table
at any depth. A normalized SHA-256 fingerprint binds the rule set to the
migration directory, so comments and formatting do not change its identity.

## Migration directory

```text
migration/
├── LOCK / CONTROL           # exclusive run and control-command locks
├── state.db                 # SQLite control plane and durable step markers
├── snapshot.json            # source fingerprint, publication, slot, snapshot
├── dump/
│   ├── schema.dump          # custom-format schema archive
│   ├── predata.list         # restored pre-data TOC
│   └── postdata.list        # restored deferred post-data TOC
├── cdc/
│   ├── *.seg                # finalized checksummed transaction segments
│   ├── *.seg.partial        # recoverable active tail
│   └── spill/               # temporary oversized-transaction files
├── log/pgmigrate.log        # JSON phase, progress, health, and error events
└── cutover-report.json      # final audit artifact
```

The directory is bound to the source cluster and database and to the normalized
table filter. Place it on durable local storage, restrict access to it, and
retain it with the cutover report according to your audit policy.

## Design considerations (why oh why)

### A copy part is either wholly present or wholly absent

Each part is written into its target table in the same transaction that records
the part complete. Nothing else makes a base copy resumable: a marker written
after the data can be lost while the data survives, and a marker written before
it can name rows that never arrived. This way a crash leaves a part either
finished and marked, or absent and unmarked, and a resume needs no reasoning
about what a partial part left behind.

### Apply progress lives on the target, not beside the tool

`pgmigrate_internal.replication_progress` on the target is the authoritative
apply position, and it commits in the same transaction as the DML it describes.
The local SQLite database is a low-rate control plane whose apply LSN is
display-only. A position recorded anywhere but next to the rows can disagree
with them after a crash, and then replay either loses transactions or repeats
them. A source-and-filter-derived stream generation binds copied data to that
progress, and a resume refuses progress that is missing or belongs to another
stream.

During catch-up, the applier coalesces an available ordered prefix of small
source transactions into one bounded target transaction. It never waits to fill
a group, so follow-mode latency stays low when traffic is light. A group is
capped by transaction count, row changes, and decoded data bytes; spilled or
large source transactions are replayed on their own. The final source EndLSN is
committed atomically with the whole group, so a crash or replay error leaves
either all grouped changes and their progress or neither.

For plain built-in relations, catalog checks prove that replica-mode writes have
no cross-relation behavior: no replica/always triggers or rules, RLS, checks,
generated columns, domains, or expression/partial indexes. The applier can then
preserve exact per-relation order while grouping independent relation lanes,
using binary `COPY` for large insert runs and ordinal-checked array operations
for keyed updates and deletes. Any relation outside that conservative set keeps
exact source order and the scalar fallback. This removes most SQL, commit/fsync,
and progress overhead while the target is still offline for migration.

Every connection that reads or executes a catalog definition pins `search_path`
to the empty path, so definitions are fully qualified and mean the same thing on
both sides. pgmigrate also records how the target rendered each index and
constraint it created, and compares a later resume against that recording rather
than against the source's text: server-deparsed SQL depends on the reading
session's `search_path` and is not a fixed point, so comparing the two databases'
text refused objects that pgmigrate had itself just created.

### Verification reads pages, not keys

A window is a range of heap pages (`ctid >= '(a,0)' AND ctid < '(b,0)'`), which
PostgreSQL answers with a `Tid Range Scan`: no index is involved, and no window
predicate can be mis-planned. On a bloated production source, hashing rows found
by key range measured 2,452 rows/s against **128,000 rows/s** for the same rows
read as a page range, because a key range on a bloated heap costs one random page
fetch per row. Page numbers only mean something within one heap, so a partitioned
source table is read as its leaf partitions, each given a share of the row budget
by size, while the target lookups go through the parent.

### Sampling is what keeps verification affordable

The exhaustive comparison this replaced read both heaps end to end: on one
production shard, 163.7 GB and 330M row hashes for a 66-minute answer, 57 minutes
of it a single table. Sampling stops the cost of a table's check from scaling
with the size of the table.

There is no exhaustive mode, and `--verify-sample-rows 0` is rejected rather
than read as "read everything": the target side is index lookups, so an
exhaustive run would be one random probe per row and slower than the sequential
comparison it replaced.

### Verification also checks what replication wrote

The heap sample cannot reach the rows the applier wrote. It samples by physical
position, and on a bloated heap position says nothing about write time: measured
on a production shard, none of the sample's 128 windows intersected the band
holding nine hours of applied changes, so the sample was validating `pg_restore`
and skipping the applier, which is the new code. Deletions make the case
strongest, because a row the source deleted and the target kept is absent from
every read of the source: on that shard, 8,201 of 8,984 recorded changes on the
busiest table were deletions from a retention job.

So the applier records the identity of the rows it writes, as a reservoir sample
capped at `--cdc-sample-rows` per relation, kept uniform over the whole change
stream rather than over its most recent part — the catch-up burst is where the
spill and out-of-order paths run, so a window of recent changes would never look
at the riskiest period. Acceptance falls off as `cap/k`, so a relation seeing ten
million changes does about 460,000 writes rather than ten million, and keys are
batched into one SQLite transaction every 1,000 samples or two seconds. Keys are
recorded only after the apply transaction commits, so a rolled-back apply never
names a row.

### Reading a live source

A row read from a live source, on a target that is still applying, is *expected*
to differ, and waiting for it to settle would never terminate for a row written
to constantly. What settles it is fixing a position rather than a moment: the
source rows are re-read, a decodable marker names a WAL position at or after that
read, and the target rows are read once apply has passed it. A row that still
differs then is genuine divergence, because the target has seen everything the
source held. `--verify-converge-timeout` covers the millisecond window between
the read and the marker; it is a deadline rather than a count of attempts because
what is being waited out is replication latency, which is measured in time.

Apply is never paused or held. Verification only ever waits for it, and only for
rows that already appear to differ, which is normally none. A marker names
nothing until it reaches disk, so on PostgreSQL 17 and later it is written with
`pg_logical_emit_message(flush => true)`, and on 16, which has no such argument,
a small committed message immediately after forces the same flush.

### The target is tuned for a bulk load, and put back

Stock checkpoint settings are the dominant cost of a large load: at the default
`max_wal_size` of 1 GB, a 200 GB copy checkpoints almost continuously. Values are
derived from what the target reports about itself, because `maintenance_work_mem`
is per-session and `--workers` sessions build indexes at once, so the product has
to fit target memory. Target memory is estimated from `shared_buffers`, and
`--target-memory` replaces the estimate. A setting already at or above the load
value is left alone, so a hand-tuned target is not touched.

Settings that need `ALTER SYSTEM` are recorded in `state.db` before they change
and reverted at cutover, so even a crashed run can be put back. Amazon RDS and
Cloud SQL refuse `ALTER SYSTEM`; that is detected in preflight, reported as a
warning naming each setting, and the session-scoped tuning still applies. A
setting the tool tries and cannot change stops the run, undoing anything already
applied so the target is never left half tuned; `--warn-on-tuning-errors`
continues best-effort instead, and `--skip-target-tuning` does not try at all.
`status` shows an open finding for as long as the target is in its bulk-load
configuration.

`synchronous_commit=off` is applied to the copy and index-build sessions only,
never to change apply, because CDC segment pruning keys off applied progress and
a lost commit could discard a segment still needed for replay.

### Replica identity

Logical replication needs a way to identify the row an `UPDATE` or `DELETE`
changed. A relation with no primary key, no designated replica identity index, or
`REPLICA IDENTITY NOTHING` has none, and PostgreSQL rejects every `UPDATE` and
`DELETE` on it as soon as it is published — so this is not a slow migration but an
unwritable source.

Preflight reports each such relation as a warning naming its size and its
lifetime `UPDATE`/`DELETE` count, and `run` sets `REPLICA IDENTITY FULL` on
**only** those, before creating the publication, restoring the original at
cleanup. A default `run` stops until `--ack-warnings`, which is where you
consent: while `FULL` is in place, every source `UPDATE` and `DELETE` on those
relations writes all old column values to WAL, and apply matches target rows on
all columns. Adding a primary key, or designating a unique non-partial index over
`NOT NULL` columns with `REPLICA IDENTITY USING INDEX`, avoids it entirely.

Partitioned tables are expanded to their leaves, because the publication names
the parent but the replication stream carries the partitions, and `ALTER TABLE`
on a parent does not reach them. A relation the migration role does not own is an
**error** rather than a warning: only an owner can change a replica identity, so
no acknowledgement would make it work. Under `--no-cleanup` the source identities
are deliberately *not* restored, because the retained publication still names
those relations and restoring them would make every source write on them fail;
`status` keeps an open finding listing them.

### Collation

Source and target must collate text the same way. This is the first thing
preflight checks, and a difference stops it there without running the rest,
because the check immediately after it samples the source WAL rate for a minute
and nothing later can inform a decision about the target's locale.

What a difference costs is not correctness inside pgmigrate: every target index
is rebuilt with the target's collation, and verification never orders or
range-compares text, since it reaches rows by page and looks them up by key. What
it costs is the application, whose queries order and compare strings differently
after cutover. That makes it a decision to take deliberately rather than a
warning to wave through, so it has its own flag, `--allow-collation-change`, and
`--ack-warnings` does not cover it.

Comparison is by locale rather than by spelling, because one locale reaches
pgmigrate under several names. Codeset spelling (`UTF-8`, `utf-8`, `utf8`),
letter case, and `POSIX` as glibc's alias of `C` are folded first, so an RDS
source reporting `en_US.UTF-8` and a Cloud SQL target reporting `en_US.UTF8` are
compatible. Nothing else is folded: `en-US` and `en_US` are names in different
naming schemes rather than two spellings of one name, and `C.UTF8` is not `C`.
Both `LC_COLLATE` and `LC_CTYPE` are compared, since character classification
decides `upper()`, `lower()`, and regex character classes. Under the ICU and
builtin providers the provider's own locale is compared too, because there it and
not `datcollate` is what collates.

Two neighbouring differences are treated differently. Two providers reporting
different versions of the same locale is an acknowledgeable warning: the locale
has not changed, and migrations between managed services cross glibc versions
routinely, so demanding the flag every time would drain it of meaning. Two cases
cannot be allowed through at all, because collation there decides structure
rather than output: a nondeterministic collation backing a unique index or
primary key, where text equality itself changes, and a range-partitioned table
with a collatable partition key, where rows can route to a different partition.

## Verification

`verify` checks each selected table two ways, and reports them separately
because they answer different questions.

It **samples the heap**: it reads about a million rows from the source and looks
those exact rows up on the target by key. Both sides return a hash of the whole
row, computed by PostgreSQL rather than by pgmigrate, so a bug in pgmigrate's own
type handling cannot cancel itself out across the comparison, and a missing row
and a wrongly applied column value are the same finding.

It also **checks the rows replication wrote**, by key, from what the applier
recorded as it wrote them, using the same recheck rule against a fixed WAL
position. `--verify-cdc-rows` bounds how many of those keys one table's check
looks at, and the check reports what it looked at against what the applier saw,
so a truncated check says so rather than reading as complete. An empty reservoir
reports "no applied rows recorded", never "0 checked", because those mean
opposite things. A relation whose recorded key does not cover the columns
`verify` keys rows on — the applier keys a change on the replica identity, which
may differ from the primary key — is skipped with that reason.

**Read this for what a pass does and does not mean.** It is a smoke test, not a
proof: it finds divergence and never proves its absence. The heap sample is also
blind in one direction, because it walks the source, so a row the target holds and
the source does not — an unapplied delete, or a duplicate — is never looked at. A
*recorded* delete is asked about on both sides and so is caught; a target-only row
nobody recorded a change for is still invisible, and finding it would need a
target-side scan.

The row budget is spread over `--verify-sample-windows` evenly spaced places in
the heap, with the last window pinned to the end of it, because that is where
appended rows land and where a replication fault appears first. Spreading them is
the point: row density varied between 0.09 and 17.2 rows per page between regions
of the same production table, so a budget spent in one place is a sample of that
place rather than of the table. Each window is bounded twice, by its page interval
and by a row limit, so a dense region stops early and a sparse one returns less. A
table small enough to fit inside the budget is read whole, so `verify` on a small
database compares everything it holds.

A table with no primary key and no `NOT NULL` unique index **cannot be checked at
all**, because there is nothing to look its rows up on the target by. It is
skipped with a warning naming it and reported with zero coverage. It does not fail
the run: an unverifiable table is not evidence of a broken copy.

Two workers took a 2-vCPU production source from 16% to sustained 100% CPU in
three minutes, so verification has its own concurrency, `--verify-workers`,
separate from `--workers`, and `--verify-duty-cycle` sleeps between windows to
hold querying under a fraction of the wall clock. On a small source, also set
`max_parallel_workers_per_gather = 0` for the migration role: intra-query
parallelism buys nothing here and defeats the duty cycle. Adding the replication
check cost little at these magnitudes: three runs over 110 tables, against
reservoirs drawn from 12k, 52k and 93k applied changes, each took about 5m25s,
against 7m56s for the heap sample alone before it existed.

**Cutover does not run this, or consult it.** Nothing stops a cutover over a
divergence, so the decision is yours: run `verify` while `run` is still following,
read the result, and cut over or don't. Verification is also worth more there than
it would be inside a cutover, because it costs no downtime and can be repeated as
often as you like. What it can never do, wherever it runs, is prove the copy is
sound — it samples, so it finds divergence and reports what it compared.

## Crash recovery

Re-run `pgmigrate run` with the same DSNs, filter, and directory.

- A torn `.partial` CDC tail is scanned and truncated to the last valid frame.
  Receiving resumes from the latest fsynced transaction EndLSN.
- Target DML and authoritative progress commit atomically, so a reconnect or
  restart skips transactions already recorded on the target. Missing or
  mismatched stream generation or progress is fatal once copied data exists.
- Restarts from `indexes`, `catchup`, or `follow` retain the completed base copy
  and recover staged CDC.
- Restarts from `setup`, `schema`, or `copy` deliberately discard **all**
  base-copy progress and start with a fresh slot and snapshot. An exported
  snapshot cannot survive its holder connection, and mixing snapshots would be
  unsafe.
- Copy parts retry classified connection failures up to five times. CDC
  transport failures reconnect automatically; corruption, protocol errors,
  divergence, and prolonged handoff backpressure stop the run for diagnosis.
  SQLSTATE 57014 is not a connection failure: after pgmigrate has set
  `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`,
  and `idle_session_timeout` to 0 on its SQL sessions, a cancel is external
  (`pg_cancel_backend`, a proxy idle timeout) and stops the part.
- Cutover records each successful step and resumes at the first incomplete one,
  reusing the end position the first attempt recorded. It never moves that
  boundary: the target has already been drained to it, and a fresh one would
  silently redefine what the migration carried.

Cleanup validates ownership and expected object shape before removing source or
target objects, and refuses to adopt or drop unexpected objects.

### Restarting a failed base copy

Because a restart from `setup`, `schema`, or `copy` discards the whole base copy,
a failure that always happens at the same point can never finish: under a
supervisor the run keeps re-copying from zero and keeps re-running destructive
work. `run` therefore records how each attempt died, and refuses to restart the
base copy once two consecutive attempts have failed in the same phase for the
same reason:

```
refusing to restart the base copy after repeated identical failures: the last 2
runs all failed in phase copy with the same error, and restarting from phase copy
would drop the target tables and copy again from a new snapshot. Fix the cause,
then re-run with --retry-base-copy to try again. Last error: ...
```

The refusal is durable, so a supervisor restarting the process keeps reporting
the same reason rather than resuming the loop. Resolve the cause and pass
`--retry-base-copy` once; the record clears by itself as soon as the run reaches
`indexes`, after which restarts resume instead of discarding work. A process
killed outright, or stopped with a signal, is not a failed attempt.
`--retry-base-copy` is not the fix for SQLSTATE 57014: pgmigrate already
disables the session timeouts that cancel long COPY, dump, and restore. If a
cancel still happens, it is external, and retrying the whole base copy will
hit it again.

## Security

DSNs are **not stored** in `state.db`, `snapshot.json`, logs, or the cutover
report. Supply them again for `run`, `verify`, and `cutover`; environment
variables keep them out of interactive shell history. DSNs and credentials still
exist in process memory and may be visible through local process or environment
inspection, so protect the host and use dedicated, least-privilege migration
credentials.

Snapshot metadata, lock files, logs, and reports are created with restrictive
modes, and the migration directory is created as owner and group accessible. Its
effective protection can still depend on pre-existing permissions and the process
umask, so directory ownership and backups remain the operator's responsibility.
Use TLS verification for remote databases. The optional metrics endpoint has no
authentication or TLS; bind it to a trusted interface or protect it with network
controls.

## Testing

See [test/README.md](test/README.md) for test patterns and environment controls.

```sh
make fmt
make vet
make test
make race
PGTEST_MAJORS=17 make integration
PGTEST_MAJORS=16,17,18 make integration
make bench
make cdc-bench
make e2e
make crash-e2e
```

Integration tests use Testcontainers and need a working Docker-compatible
runtime. The PostgreSQL 17 Compose e2e harness independently checks final table
inventory, row counts, and canonical row digests, including one digest per leaf
partition and a comparison of every index and constraint definition in the
schema.

`make cdc-bench` times only replay of a durable 500,000-change backlog and
compares full source/target table digests. Use
`PGMIGRATE_CDC_BENCH_BARRIER_EVERY=N` to add a check-constrained ordering
barrier every N source transactions, and `PGMIGRATE_CDC_BENCH_ACCOUNT_COUNT=N`
to exercise hot-key skew. Transaction count and the minimum accepted rate are
controlled by `PGMIGRATE_CDC_BENCH_TRANSACTIONS` and
`PGMIGRATE_CDC_BENCH_MIN_CHANGES_PER_SECOND`.

## Limitations

- One database per run; no fleet orchestration, bidirectional replication, or
  conflict resolution.
- PostgreSQL 16 to 18 only, and no downgrade migration.
- DDL is neither replicated nor automatically detected. Large objects are
  preflight-blocked; foreign and unlogged tables and materialized views produce
  findings and need an operator plan.
- The target is assumed not to receive independent application traffic before
  cutover. Replay divergence stops the run.
- Apply is serial.
- The delivered e2e bed is PostgreSQL 17 to 17. Cross-major compatibility has
  focused integration probes but no full cross-major Compose migration.
- Verification samples, and reports 64-bit server-side hashes rather than a
  cryptographic proof. It cannot see a target-only row nobody recorded a change
  for, cannot check a table with no primary key and no `NOT NULL` unique index at
  all, and checks the rows replication wrote as a capped sample of what the
  applier reported rather than exhaustively from the decoded stream.
- Cutover enforces nothing. It does not check that application writes stopped and
  does not verify the copy, so writes made after the end position are left behind
  silently and a divergent table will not stop it. Both are the operator's to
  establish beforehand, the second with `verify`.
- Local microbenchmarks (`make bench`) cover in-memory and file primitives only.
  They establish no sustainable CDC rate, cutover duration, or managed-cloud
  performance.

## License

Apache License 2.0. The full text is in [LICENSE](LICENSE).
