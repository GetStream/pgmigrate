# Testing pgmigrate

The repository separates fast package tests, container-backed integration
tests, and shell-driven end-to-end tests. Run commands from the repository root.

## Unit tests

Unit tests live beside the package under test as `*_test.go` and do not require
PostgreSQL. They cover pure logic and controlled I/O, including filters,
fingerprints, phase transitions, TOC classification, COPY planning and retry
classification, pgoutput decoding, transaction codecs, segment recovery,
spill cleanup, verification result reporting, cutover step resumption, status,
and metrics.

```sh
make test
make race
go test ./internal/cdc -run 'TestRecovery'
go test ./internal/cutover -run 'TestResume'
```

`make test` also compiles packages that have integration tests, but excludes
those tests because they use the `integration` build tag. `make race` runs the
ordinary suite with the Go race detector; it does not include tagged
integration tests.

When adding tests:

- keep package-specific fixtures close to the package, using `testdata` where
  files are useful;
- put reusable helpers in `internal/testutil`, accept `testing.TB`, call
  `Helper`, and fail the current test on setup errors;
- use table-driven cases for parsers, planners, and error classification;
- test crash consistency at both sides of each durable boundary, including a
  failure after an external effect but before its local marker;
- assert permanent errors are not accidentally placed in reconnect/retry loops.

## Integration tests

Integration tests use Testcontainers, carry the `integration` build tag, and
require a working Docker-compatible runtime. They exercise actual PostgreSQL
behavior: exported snapshots, publication/slot ownership, preflight probes,
binary and text COPY, indexes/FKs, pgoutput staging and replay, transactional
target progress, range verification, sequence synchronization, and PostgreSQL
version assumptions.

```sh
make integration
PGTEST_MAJORS=17 make integration
PGTEST_MAJORS=16,18 go test -tags=integration ./internal/postgres -v
go test -tags=integration ./internal/cdc -run TestPG17LiveWALStageApplyCrashRetry -v
```

`PGTEST_MAJORS` is a comma-separated subset of `16,17,18`. When unset, the
version-matrix helper selects all three. Duplicate values are ignored and any
other major fails the test. The matrix is used by PostgreSQL-assumption tests;
several focused integration tests intentionally start PostgreSQL 17 directly,
so setting `PGTEST_MAJORS` does not rewrite every test's server version.

Select two majors when exercising the cross-major capability branch, for
example `PGTEST_MAJORS=16,18`. Cross-major COPY selection has focused coverage;
the repository does not currently run a complete cross-major e2e migration.

## End-to-end tests

The isolated Compose bed under `test/e2e` starts PostgreSQL 17 source and target
instances plus mixed INSERT/UPDATE/DELETE traffic.

```sh
make e2e
```

The harness builds/runs preflight, waits for `follow`, confirms traffic, freezes
writes, verifies, cuts over, checks cleanup, and independently compares table
inventory, exact row counts, and order-independent canonical row digests. It
does not use pgmigrate's verifier for the final data assertion. The default run
uses four replay workers and 64-transaction replay batches. CI precedes it with
focused real-PostgreSQL concurrency and batching throughput tests under the race
detector.

The seed also carries objects whose `pg_dump` archive descriptions are multi-word
or word-prefixed by a shorter description: a text-search configuration reached by
a dependent GIN index, and an operator class. Keep them. A source containing a
text-search configuration once aborted the schema phase, and the fixture gap is
why that reached a production rehearsal.

Three further properties of the seed are load-bearing, and each one covers a bug
that a `public`-only fixture let through to that same rehearsal:

- the extension lives in `e2e`, not `public`, so the restore list has to keep
  `CREATE SCHEMA e2e` ahead of `CREATE EXTENSION ... WITH SCHEMA e2e`. Because
  `public` always exists, a fixture that installs extensions there cannot fail
  this way;
- the extension is reachable only through the dependency closure, by way of the
  text-search configuration's dictionary mapping, rather than by providing a
  column type. That keeps the run on binary COPY while still selecting the
  extension and the schema holding it;
- comments on a schema, table, column, type, and function exercise the deferred
  post-data path. `pg_dump` writes comments with no catalog identity and names
  the object without its schema, so a fixture without comments never resolves
  one.

Two later additions are load-bearing for the same reason, and
`assert-source.sh` fails if either is lost:

- the source database sets `search_path` to `e2e, public`, which is what a real
  source does and what no earlier fixture did. PostgreSQL renders an object
  reference bare when its schema is on the reading session's path and qualified
  when it is not, so the same index read from this source and from the
  default-path target came back as two different strings and was reported as
  drift. The seed's GIN index over `e2e.order_search` is the object whose
  rendering flips. It also means nothing pgmigrate runs against the source may
  rely on the default path;
- `e2e.events` is `RANGE`-partitioned two levels deep with rows in every leaf, a
  primary key and a partitioned index on the parent, a child-only index, a
  foreign key out, and `e2e.event_notes` holding a foreign key in. Traffic
  inserts, updates, deletes, and moves a row across the partition boundary.
  Partition children were once dropped from the restore list, leaving a target
  whose partitioned table rejected every insert; a fixture without a populated
  partitioned table cannot see this. `assert-data.sh` therefore digests every
  leaf separately — a target that put every row in one default partition answers
  every parent query correctly — and compares the partition tree, every index
  and constraint definition, and that nothing is left invalid or unvalidated.

Both beds also run with `--split-threshold 65536`, because the seed is orders of
magnitude below the 1 GiB default and every table would otherwise be copied as a
single unsplit part, leaving the split path — a different write path, taken for
every large table in production — untested end to end. The migration bed asserts
it copied more parts than tables, so a resized seed cannot silently stop covering
it. `order_items` has a composite primary key and therefore splits by source
block rather than by key range, covering both.

Useful controls:

- `PGMIGRATE_BIN`: test a different binary;
- `MIGRATION_TIMEOUT`: seconds to wait for follow (default 300);
- `SPLIT_THRESHOLD`: bytes per copy part (default 65536);
- `REPLAY_WORKERS`: concurrent target replay sessions (default 4);
- `REPLAY_BATCH_SIZE`: dependent transactions per target commit (default 64);
- `REPLAY_WINDOW`: source transactions searched for parallel work (default 128);
- `MIGRATION_DIR`: caller-owned state directory;
- `KEEP_MIGRATION_DIR=1`: retain temporary state and logs;
- `SOURCE_PORT` / `TARGET_PORT`: override host ports.

See [e2e/README.md](e2e/README.md) for manual test-bed commands.

## Crash and recovery tests

```sh
make crash-e2e
```

The crash harness sends `SIGKILL` after the run reports `copy`, `indexes`,
`catchup`, and `follow`, restarting from the same migration directory after
each kill. It then verifies, cuts over, waits for a clean `run` exit, and runs
the independent data comparison.

`PHASE_TIMEOUT` (default 60) is the seconds to wait for each phase. Every resume
re-runs the schema archive steps first, and each client-tool invocation starts a
container, so a tight budget fails on host latency rather than on a resume
defect.

This proves the exercised phase-boundary recovery paths. It is not random fault
injection and does not claim coverage for every instruction, fsync window,
network partition, or disk failure. Segment unit tests separately cover torn
and corrupt active tails; cutover unit tests cover report/marker crash windows.

## Benchmarks

```sh
make bench
go test -run='^$' -bench=. -benchmem ./internal/cdc
```

Benchmarks cover codec encode/decode, pgoutput decode, apply preparation,
segment append, and large-record reading. Results are machine-local
microbenchmarks; do not report them as database migration throughput.

## Before delivery

For documentation-only changes, `go test ./...` is sufficient to catch
accidental repository issues. For code changes, the expected baseline is:

```sh
make fmt
make vet
make test
make race
```

Run integration, e2e, and crash-e2e in proportion to the affected PostgreSQL,
orchestration, persistence, and recovery paths. Network-chaos/toxiproxy and
managed-cloud smoke suites are not currently present.
