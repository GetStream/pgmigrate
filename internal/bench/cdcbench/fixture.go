package cdcbench

import (
	"context"
	"fmt"

	"github.com/GetStream/pgmigrate/internal/cdc"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	benchSchema      = "pgmigrate_bench"
	benchTable       = "hot"
	benchPublication = "pgmigrate_bench_pub"
	benchSlot        = "pgmigrate_bench_slot"
	benchGeneration  = "pgmigrate-bench-v1"
)

const fixtureSchema = `
	CREATE SCHEMA pgmigrate_bench;
	CREATE TABLE pgmigrate_bench.hot (
		id bigint PRIMARY KEY,
		revision bigint NOT NULL,
		payload text NOT NULL,
		touched bigint NOT NULL,
		bucket integer NOT NULL
	) WITH (fillfactor=90, autovacuum_enabled=false);
	CREATE TABLE pgmigrate_bench.maintenance (
		id bigint PRIMARY KEY,
		payload text NOT NULL,
		category integer NOT NULL,
		amount bigint NOT NULL,
		padding text NOT NULL
	) WITH (autovacuum_enabled=false);
`

const seedHot = `
	INSERT INTO pgmigrate_bench.hot (id, revision, payload, touched, bucket)
	SELECT id, 0, md5(id::text || ':0'), 0, 0
	FROM generate_series(1, $1::bigint) AS id;
`

const seedMaintenance = `
	INSERT INTO pgmigrate_bench.maintenance (id, payload, category, amount, padding)
	SELECT id, md5(id::text), (id % 4096)::integer, id * 17, repeat('x', 96)
	FROM generate_series(1, $1::bigint) AS id;
`

const sourceIndexes = `
	CREATE INDEX hot_payload_idx ON pgmigrate_bench.hot (payload);
	CREATE INDEX hot_revision_idx ON pgmigrate_bench.hot (revision);
	CREATE INDEX hot_touched_idx ON pgmigrate_bench.hot (touched);
	CREATE INDEX hot_bucket_payload_idx ON pgmigrate_bench.hot (bucket, payload);
	CREATE INDEX maintenance_payload_idx ON pgmigrate_bench.maintenance (payload);
	CREATE INDEX maintenance_category_idx ON pgmigrate_bench.maintenance (category);
	CREATE INDEX maintenance_amount_idx ON pgmigrate_bench.maintenance (amount);
	CREATE INDEX maintenance_category_payload_idx ON pgmigrate_bench.maintenance (category, payload);
`

func setupFixture(ctx context.Context, sourceURI, targetURI string, rows int64) (cdc.LSN, error) {
	source, err := postgres.Connect(ctx, sourceURI)
	if err != nil {
		return 0, fmt.Errorf("connect source fixture: %w", err)
	}
	defer source.Close(context.Background())
	target, err := postgres.Connect(ctx, targetURI)
	if err != nil {
		return 0, fmt.Errorf("connect target fixture: %w", err)
	}
	defer target.Close(context.Background())

	for name, conn := range map[string]interface {
		Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	}{"source": source, "target": target} {
		if _, err := conn.Exec(ctx, fixtureSchema); err != nil {
			return 0, fmt.Errorf("create %s fixture: %w", name, err)
		}
		if _, err := conn.Exec(ctx, seedHot, rows); err != nil {
			return 0, fmt.Errorf("seed %s hot fixture: %w", name, err)
		}
	}
	// The maintenance table is deliberately larger than the hot replay table.
	// Its indexes keep target CPU and I/O busy for long enough to measure overlap
	// without multiplying the source-side CDC workload.
	if _, err := source.Exec(ctx, seedMaintenance, 0); err != nil {
		return 0, fmt.Errorf("seed source maintenance fixture: %w", err)
	}
	if _, err := target.Exec(ctx, seedMaintenance, rows*4); err != nil {
		return 0, fmt.Errorf("seed target maintenance fixture: %w", err)
	}
	if _, err := source.Exec(ctx, sourceIndexes); err != nil {
		return 0, fmt.Errorf("create source fixture indexes: %w", err)
	}
	if _, err := source.Exec(
		ctx,
		"CREATE PUBLICATION "+benchPublication+" FOR TABLE "+benchSchema+"."+benchTable,
	); err != nil {
		return 0, fmt.Errorf("create benchmark publication: %w", err)
	}

	config, err := pgconn.ParseConfig(sourceURI)
	if err != nil {
		return 0, fmt.Errorf("parse replication connection: %w", err)
	}
	config.RuntimeParams["replication"] = "database"
	replication, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return 0, fmt.Errorf("connect replication protocol: %w", err)
	}
	defer replication.Close(context.Background())
	slot, err := pglogrepl.CreateReplicationSlot(
		ctx,
		replication,
		benchSlot,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "NOEXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		return 0, fmt.Errorf("create benchmark replication slot: %w", err)
	}
	start, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse benchmark consistent point: %w", err)
	}
	return cdc.LSN(start), nil
}

type checksum struct {
	Rows     int64
	Revision int64
	Hash     int64
}

func readChecksum(ctx context.Context, uri string) (checksum, error) {
	conn, err := postgres.Connect(ctx, uri)
	if err != nil {
		return checksum{}, err
	}
	defer conn.Close(context.Background())
	var result checksum
	err = conn.QueryRow(ctx, `
		SELECT count(*)::bigint,
		       coalesce(sum(revision), 0)::bigint,
		       coalesce(bit_xor(hashtextextended(
		           id::text || ':' || revision::text || ':' || payload || ':' ||
		           touched::text || ':' || bucket::text, 0
		       )), 0)::bigint
		FROM pgmigrate_bench.hot
	`).Scan(&result.Rows, &result.Revision, &result.Hash)
	return result, err
}

func verifyFixture(ctx context.Context, sourceURI, targetURI string, generated, applied int64) error {
	if generated != applied {
		return fmt.Errorf("committed update accounting mismatch: generated=%d applied=%d", generated, applied)
	}
	source, err := readChecksum(ctx, sourceURI)
	if err != nil {
		return fmt.Errorf("read source checksum: %w", err)
	}
	target, err := readChecksum(ctx, targetURI)
	if err != nil {
		return fmt.Errorf("read target checksum: %w", err)
	}
	if source != target {
		return fmt.Errorf("source/target checksum mismatch: source=%+v target=%+v", source, target)
	}
	if source.Revision != generated {
		return fmt.Errorf("source revision total=%d, generated updates=%d", source.Revision, generated)
	}

	conn, err := postgres.Connect(ctx, targetURI)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var invalid int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM pg_index i
		JOIN pg_class x ON x.oid=i.indexrelid
		JOIN pg_namespace n ON n.oid=x.relnamespace
		WHERE n.nspname='pgmigrate_bench' AND (NOT i.indisvalid OR NOT i.indisready)
	`).Scan(&invalid); err != nil {
		return fmt.Errorf("inspect target indexes: %w", err)
	}
	if invalid != 0 {
		return fmt.Errorf("target has %d invalid benchmark indexes", invalid)
	}
	return nil
}
