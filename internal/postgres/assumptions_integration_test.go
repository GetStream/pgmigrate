//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgreSQLMigrationAssumptions(t *testing.T) {
	instances := make([]*pgtest.Instance, 0, 3)
	reportedMajors := make(map[*pgtest.Instance]int, 3)
	for _, major := range pgtest.Majors(t) {
		instance := pgtest.Start(t, major)
		instances = append(instances, instance)
		reportedMajors[instance] = serverMajor(t, instance)
		if reportedMajors[instance] != major {
			t.Fatalf(
				"postgres:%d image reported PostgreSQL major %d",
				major,
				reportedMajors[instance],
			)
		}
	}

	for _, instance := range instances {
		major := reportedMajors[instance]
		t.Run(fmt.Sprintf("PG%d/version capability gates", major), func(t *testing.T) {
			probeVersionCapabilities(t, instance, major)
		})
		t.Run(fmt.Sprintf("PG%d/replication origin rollback behavior", major), func(t *testing.T) {
			probeReplicationOriginRollbackBehavior(t, instance)
		})
		t.Run(fmt.Sprintf("PG%d/transactional progress atomicity", major), func(t *testing.T) {
			probeTransactionalProgressAtomicity(t, instance)
		})
		t.Run(fmt.Sprintf("PG%d/COPY FREEZE eligibility", major), func(t *testing.T) {
			probeCopyFreezeEligibility(t, instance)
		})
		t.Run(fmt.Sprintf("PG%d/exported snapshot command idle", major), func(t *testing.T) {
			probeExportedSnapshotRequiresIdle(t, instance)
		})
	}

	t.Run("COPY format decision uses actual majors", func(t *testing.T) {
		for _, source := range instances {
			for _, target := range instances {
				sourceMajor := reportedMajors[source]
				targetMajor := reportedMajors[target]
				want := postgres.CopyFormatText
				if sourceMajor == targetMajor {
					want = postgres.CopyFormatBinary
				}
				if got := postgres.CopyFormatForMajors(sourceMajor, targetMajor); got != want {
					t.Errorf(
						"source PG%d to target PG%d: got %q, want %q",
						sourceMajor,
						targetMajor,
						got,
						want,
					)
				}
			}
		}

		if len(instances) < 2 {
			t.Log("cross-major branch requires two selected PGTEST_MAJORS")
		}
	})
}

func serverMajor(t testing.TB, instance *pgtest.Instance) int {
	t.Helper()
	conn := instance.Connect(t)

	var major int
	if err := conn.QueryRow(
		context.Background(),
		"SELECT current_setting('server_version_num')::integer / 10000",
	).Scan(&major); err != nil {
		t.Fatalf("read PostgreSQL server major: %v", err)
	}
	return major
}

func probeVersionCapabilities(t testing.TB, instance *pgtest.Instance, major int) {
	t.Helper()
	capabilities, err := postgres.CapabilitiesForMajor(major)
	if err != nil {
		t.Fatalf("capabilities for PostgreSQL %d: %v", major, err)
	}

	conn := instance.Connect(t)
	var serverHasFailoverSlots bool
	if err := conn.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_proc
			WHERE proname = 'pg_create_logical_replication_slot'
			  AND pronargs = 5
		)
	`).Scan(&serverHasFailoverSlots); err != nil {
		t.Fatalf("probe failover-slot function signature: %v", err)
	}
	if capabilities.ReplicationSlotFailover != serverHasFailoverSlots {
		t.Errorf(
			"PG%d failover-slot capability = %v, server signature says %v",
			major,
			capabilities.ReplicationSlotFailover,
			serverHasFailoverSlots,
		)
	}
	if !capabilities.SequenceSyncRequired {
		t.Errorf("PG%d unexpectedly claims logical replication synchronizes sequences", major)
	}
	if !capabilities.ExportedSnapshotRequiresIdle {
		t.Errorf("PG%d unexpectedly permits commands while exporting a snapshot", major)
	}
}

func probeReplicationOriginRollbackBehavior(t testing.TB, instance *pgtest.Instance) {
	t.Helper()
	ctx := context.Background()
	control := instance.Connect(t)
	origin := fmt.Sprintf("pgmigrate_probe_%d", instance.Major)

	if _, err := control.Exec(ctx, "DROP TABLE IF EXISTS pgmigrate_origin_probe"); err != nil {
		t.Fatalf("drop old replication-origin probe table: %v", err)
	}
	if _, err := control.Exec(ctx, "CREATE TABLE pgmigrate_origin_probe (id integer PRIMARY KEY)"); err != nil {
		t.Fatalf("create replication-origin probe table: %v", err)
	}
	if _, err := control.Exec(ctx, "SELECT pg_replication_origin_create($1)", origin); err != nil {
		t.Fatalf("create replication origin: %v", err)
	}
	t.Cleanup(func() {
		_, _ = control.Exec(context.Background(), "SELECT pg_replication_origin_drop($1)", origin)
	})

	apply := connectOriginApply(t, instance)
	if _, err := apply.Exec(ctx, "SELECT pg_replication_origin_session_setup($1)", origin); err != nil {
		t.Fatalf("set up replication-origin session: %v", err)
	}

	rolledBackLSN := pglogrepl.LSN(0x20)
	tx, err := apply.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rolled-back origin transaction: %v", err)
	}
	if _, err := tx.Exec(
		ctx,
		"SELECT pg_replication_origin_xact_setup($1::pg_lsn, clock_timestamp())",
		rolledBackLSN.String(),
	); err != nil {
		t.Fatalf("set rolled-back origin progress: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO pgmigrate_origin_probe VALUES (1)"); err != nil {
		t.Fatalf("insert rolled-back row: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("roll back origin transaction: %v", err)
	}
	resetAndCloseOriginApply(t, apply)

	rolledBackProgress := originProgress(t, control, origin)
	if rolledBackProgress != rolledBackLSN {
		t.Errorf(
			"durable rolled-back origin progress after reconnect = %s, want %s",
			rolledBackProgress,
			rolledBackLSN,
		)
	}
	var rows int
	if err := control.QueryRow(ctx, "SELECT count(*) FROM pgmigrate_origin_probe").Scan(&rows); err != nil {
		t.Fatalf("count rows after rollback: %v", err)
	}
	if rows != 0 {
		t.Fatalf("row count after rollback = %d, want 0", rows)
	}
	t.Logf("rollback evidence: durable progress=%s rows=%d", rolledBackProgress, rows)

	apply = connectOriginApply(t, instance)
	if _, err := apply.Exec(ctx, "SELECT pg_replication_origin_session_setup($1)", origin); err != nil {
		t.Fatalf("set up reconnected replication-origin session: %v", err)
	}
	committedLSN := pglogrepl.LSN(0x30)
	tx, err = apply.Begin(ctx)
	if err != nil {
		t.Fatalf("begin committed origin transaction: %v", err)
	}
	if _, err := tx.Exec(
		ctx,
		"SELECT pg_replication_origin_xact_setup($1::pg_lsn, clock_timestamp())",
		committedLSN.String(),
	); err != nil {
		t.Fatalf("set committed origin progress: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO pgmigrate_origin_probe VALUES (2)"); err != nil {
		t.Fatalf("insert committed row: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit origin transaction: %v", err)
	}
	resetAndCloseOriginApply(t, apply)

	progress := originProgress(t, control, origin)
	if progress != committedLSN {
		t.Fatalf("committed origin progress = %s, want %s", progress, committedLSN)
	}
	if err := control.QueryRow(ctx, "SELECT count(*) FROM pgmigrate_origin_probe").Scan(&rows); err != nil {
		t.Fatalf("count committed rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("committed row count = %d, want 1", rows)
	}
	t.Logf("commit evidence: durable progress=%s rows=%d", progress, rows)
}

func probeTransactionalProgressAtomicity(t testing.TB, instance *pgtest.Instance) {
	t.Helper()
	ctx := context.Background()
	control := instance.Connect(t)
	streamID := fmt.Sprintf("pgmigrate_stream_%d", instance.Major)

	if err := postgres.EnsureProgressTable(ctx, control); err != nil {
		t.Fatalf("ensure transactional progress table: %v", err)
	}
	if _, err := control.Exec(ctx, "DROP TABLE IF EXISTS pgmigrate_progress_probe"); err != nil {
		t.Fatalf("drop old transactional progress probe table: %v", err)
	}
	if _, err := control.Exec(ctx, "CREATE TABLE pgmigrate_progress_probe (id integer PRIMARY KEY)"); err != nil {
		t.Fatalf("create transactional progress probe table: %v", err)
	}

	baselineLSN := pglogrepl.LSN(0x10)
	apply := connectOriginApply(t, instance)
	tx, err := apply.Begin(ctx)
	if err != nil {
		t.Fatalf("begin baseline progress transaction: %v", err)
	}
	if err := postgres.UpdateProgress(ctx, tx, streamID, baselineLSN); err != nil {
		t.Fatalf("record baseline progress: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit baseline progress: %v", err)
	}
	closeDirectConnection(t, apply)

	rolledBackLSN := pglogrepl.LSN(0x20)
	apply = connectOriginApply(t, instance)
	tx, err = apply.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rolled-back progress transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO pgmigrate_progress_probe VALUES (1)"); err != nil {
		t.Fatalf("insert rolled-back progress row: %v", err)
	}
	if err := postgres.UpdateProgress(ctx, tx, streamID, rolledBackLSN); err != nil {
		t.Fatalf("record rolled-back progress: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("roll back data and progress: %v", err)
	}
	closeDirectConnection(t, apply)

	observer := connectOriginApply(t, instance)
	progress, exists, err := postgres.ReadProgress(ctx, observer, streamID)
	if err != nil {
		t.Fatalf("read progress after rollback: %v", err)
	}
	if !exists || progress != baselineLSN {
		t.Fatalf(
			"progress after rollback = %s (exists=%v), want %s",
			progress,
			exists,
			baselineLSN,
		)
	}
	var rows int
	if err := observer.QueryRow(ctx, "SELECT count(*) FROM pgmigrate_progress_probe").Scan(&rows); err != nil {
		t.Fatalf("count rows after transactional rollback: %v", err)
	}
	if rows != 0 {
		t.Fatalf("row count after transactional rollback = %d, want 0", rows)
	}
	closeDirectConnection(t, observer)

	committedLSN := pglogrepl.LSN(0x30)
	apply = connectOriginApply(t, instance)
	tx, err = apply.Begin(ctx)
	if err != nil {
		t.Fatalf("begin committed progress transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO pgmigrate_progress_probe VALUES (2)"); err != nil {
		t.Fatalf("insert committed progress row: %v", err)
	}
	if err := postgres.UpdateProgress(ctx, tx, streamID, committedLSN); err != nil {
		t.Fatalf("record committed progress: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit data and progress: %v", err)
	}
	closeDirectConnection(t, apply)

	observer = connectOriginApply(t, instance)
	progress, exists, err = postgres.ReadProgress(ctx, observer, streamID)
	if err != nil {
		t.Fatalf("read progress after commit: %v", err)
	}
	if !exists || progress != committedLSN {
		t.Fatalf(
			"progress after commit = %s (exists=%v), want %s",
			progress,
			exists,
			committedLSN,
		)
	}
	if err := observer.QueryRow(ctx, "SELECT count(*) FROM pgmigrate_progress_probe").Scan(&rows); err != nil {
		t.Fatalf("count rows after transactional commit: %v", err)
	}
	if rows != 1 {
		t.Fatalf("row count after transactional commit = %d, want 1", rows)
	}
	if !postgres.ShouldSkipRemoteTransaction(progress, rolledBackLSN) {
		t.Error("transaction below durable custom progress was not skipped")
	}
	if !postgres.ShouldSkipRemoteTransaction(progress, committedLSN) {
		t.Error("transaction equal to durable custom progress was not skipped")
	}
	if postgres.ShouldSkipRemoteTransaction(progress, pglogrepl.LSN(0x40)) {
		t.Error("transaction above durable custom progress was skipped")
	}
	closeDirectConnection(t, observer)

	t.Logf(
		"transactional evidence: rollback progress=%s rows=0; commit progress=%s rows=%d",
		baselineLSN,
		committedLSN,
		rows,
	)
}

func connectOriginApply(t testing.TB, instance *pgtest.Instance) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), instance.URI)
	if err != nil {
		t.Fatalf("connect replication-origin apply session: %v", err)
	}
	return conn
}

func closeDirectConnection(t testing.TB, conn *pgx.Conn) {
	t.Helper()
	if err := conn.Close(context.Background()); err != nil {
		t.Fatalf("close direct PostgreSQL connection: %v", err)
	}
}

func resetAndCloseOriginApply(t testing.TB, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(
		context.Background(),
		"SELECT pg_replication_origin_session_reset()",
	); err != nil {
		t.Fatalf("reset replication-origin apply session: %v", err)
	}
	closeDirectConnection(t, conn)
}

func originProgress(t testing.TB, conn *pgx.Conn, origin string) pglogrepl.LSN {
	t.Helper()
	var value string
	if err := conn.QueryRow(
		context.Background(),
		"SELECT pg_replication_origin_progress($1, true)::text",
		origin,
	).Scan(&value); err != nil {
		t.Fatalf("read replication-origin progress: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		t.Fatalf("parse replication-origin progress %q: %v", value, err)
	}
	return lsn
}

func probeCopyFreezeEligibility(t testing.TB, instance *pgtest.Instance) {
	t.Helper()
	ctx := context.Background()
	conn := instance.Connect(t)

	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin COPY FREEZE transaction: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE pgmigrate_copy_freeze (id integer)"); err != nil {
		t.Fatalf("create COPY FREEZE table: %v", err)
	}
	if _, err := conn.PgConn().CopyFrom(
		ctx,
		strings.NewReader("1\n2\n"),
		"COPY pgmigrate_copy_freeze FROM STDIN WITH (FREEZE)",
	); err != nil {
		t.Fatalf("COPY FREEZE into table created in current transaction: %v", err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit eligible COPY FREEZE: %v", err)
	}

	_, err := conn.PgConn().CopyFrom(
		ctx,
		strings.NewReader("3\n"),
		"COPY pgmigrate_copy_freeze FROM STDIN WITH (FREEZE)",
	)
	if err == nil {
		t.Fatal("COPY FREEZE into a previously committed table unexpectedly succeeded")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
		t.Fatalf("ineligible COPY FREEZE error = %v, want SQLSTATE 55000", err)
	}
}

func probeExportedSnapshotRequiresIdle(t testing.TB, instance *pgtest.Instance) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repl := instance.ReplicationConnect(t)
	sqlConn := instance.Connect(t)

	idleSlot := fmt.Sprintf("pgmigrate_idle_%d", instance.Major)
	idleResult, err := pglogrepl.CreateReplicationSlot(
		ctx,
		repl,
		idleSlot,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "EXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		t.Fatalf("create slot with exported snapshot: %v", err)
	}
	importSnapshot(t, sqlConn, idleResult.SnapshotName, true)
	if err := pglogrepl.DropReplicationSlot(
		ctx,
		repl,
		idleSlot,
		pglogrepl.DropReplicationSlotOptions{},
	); err != nil {
		t.Fatalf("drop idle snapshot slot: %v", err)
	}

	activeSlot := fmt.Sprintf("pgmigrate_active_%d", instance.Major)
	activeResult, err := pglogrepl.CreateReplicationSlot(
		ctx,
		repl,
		activeSlot,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "EXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		t.Fatalf("create second slot with exported snapshot: %v", err)
	}
	if _, err := pglogrepl.IdentifySystem(ctx, repl); err != nil {
		t.Fatalf("issue command after snapshot export: %v", err)
	}
	importSnapshot(t, sqlConn, activeResult.SnapshotName, false)
	if err := pglogrepl.DropReplicationSlot(
		ctx,
		repl,
		activeSlot,
		pglogrepl.DropReplicationSlotOptions{},
	); err != nil {
		t.Fatalf("drop active snapshot slot: %v", err)
	}
}

func importSnapshot(t testing.TB, conn *pgx.Conn, snapshot string, wantSuccess bool) {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatalf("begin snapshot importer: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	statement := "SET TRANSACTION SNAPSHOT '" + strings.ReplaceAll(snapshot, "'", "''") + "'"
	_, err = tx.Exec(ctx, statement)
	if wantSuccess {
		if err != nil {
			t.Fatalf("import snapshot while exporter was command-idle: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit snapshot importer: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("snapshot remained importable after exporter issued another command")
	}
}
