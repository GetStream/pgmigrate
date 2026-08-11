# PostgreSQL 17 end-to-end bed

This isolated Compose project starts:

- a PostgreSQL 17 source configured for logical replication;
- an empty PostgreSQL 17 target;
- a traffic worker running transactional INSERT, UPDATE, and DELETE activity.

The source is initialized with deterministic customers, accounts, orders, order
items, and a two-level range-partitioned `e2e.events` with rows in every leaf. Every
replicated table has a primary key, and the source database runs on a non-default
`search_path`. The traffic worker maintains counters and a heartbeat so the
assertions can distinguish real mixed traffic from a merely running container.

## Use

From the repository root:

```sh
test/e2e/scripts/start.sh
test/e2e/scripts/assert-source.sh
test/e2e/scripts/assert-traffic.sh
test/e2e/scripts/pause-traffic.sh
test/e2e/scripts/stop.sh
```

`start.sh` removes the prior Compose volumes by default, guaranteeing a fresh,
deterministic seed. Pass `--reuse` to retain the current databases. `stop.sh` also
removes volumes by default; pass `--keep-data` to preserve them.

The source and target URLs are:

```text
postgres://app:app@localhost:55432/app?sslmode=disable
postgres://app:app@localhost:55433/app?sslmode=disable
```

Override the host ports with `SOURCE_PORT` and `TARGET_PORT`.

## Full migration

After `./pgmigrate` has been built:

```sh
test/e2e/scripts/run-migration.sh
```

The harness creates a temporary migration directory, starts a fresh test bed, checks
source health and mixed traffic, runs `preflight`, starts `run`, waits for the JSON
status to report the `follow` phase, stops traffic, invokes `cutover`, and compares all
source and target tables.

Set `PGMIGRATE_BIN` to use another binary. `MIGRATION_TIMEOUT` controls the wait for
follow mode. `MIGRATION_DIR` uses a caller-owned state directory; otherwise temporary
state is deleted. Set `KEEP_MIGRATION_DIR=1` to retain temporary state and logs.
The harness acknowledges expected preflight warnings for this controlled fixture.

`assert-data.sh` is intentionally independent of pgmigrate's verifier. It compares the
table inventory and, for each seeded table and each leaf partition, an exact row count
plus an order-independent MD5 digest of canonical JSON rows. It also compares the
partition tree with its bounds and every index and constraint definition, reading both
sides on a pinned `search_path` so the two renderings are comparable. Run it only after
traffic is paused and replication has drained.
