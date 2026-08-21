//go:build integration

package setup_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/setup"
	"github.com/jackc/pgx/v5"
)

type snapshotState struct {
	slot, snapshot, point string
}

func (s *snapshotState) SetSnapshot(_ context.Context, slot, snapshot, point string) error {
	s.slot, s.snapshot, s.point = slot, snapshot, point
	return nil
}

func TestPG17SnapshotLifecycleAndFailoverGate(t *testing.T) {
	instance := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	control := instance.Connect(t)
	if _, err := control.Exec(ctx, "ALTER SYSTEM SET wal_sender_timeout = '1s'"); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, "ALTER SYSTEM SET idle_in_transaction_session_timeout = '1s'"); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, "ALTER SYSTEM SET idle_session_timeout = '1s'"); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	for {
		var walSender, idleTransaction, idleSession string
		if err := control.QueryRow(ctx, `
			SELECT current_setting('wal_sender_timeout'),
			       current_setting('idle_in_transaction_session_timeout'),
			       current_setting('idle_session_timeout')
		`).Scan(&walSender, &idleTransaction, &idleSession); err != nil {
			t.Fatal(err)
		}
		if walSender == "1s" && idleTransaction == "1s" && idleSession == "1s" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Keep the test's long-lived control connection out of the experiment. New
	// connections still inherit the one-second server defaults, including the
	// replication connection created by setup.Run below.
	if _, err := control.Exec(ctx, `
		SELECT set_config('idle_in_transaction_session_timeout', '0', false),
		       set_config('idle_session_timeout', '0', false)
	`); err != nil {
		t.Fatal(err)
	}

	for _, failover := range []bool{false, true} {
		t.Run(fmt.Sprintf("failover=%v", failover), func(t *testing.T) {
			table := fmt.Sprintf("setup_source_%v", failover)
			if _, err := control.Exec(ctx, "CREATE TABLE "+table+" (id integer PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			state := &snapshotState{}
			holder, err := setup.Run(ctx, setup.Config{
				SourceDSN:      instance.URI,
				TargetDSN:      instance.URI,
				Dir:            t.TempDir(),
				MigrationID:    table,
				Tables:         []setup.Table{{Schema: "public", Name: table}},
				EnableFailover: failover,
			}, state)
			if err != nil {
				t.Fatalf("setup: %v", err)
			}
			t.Cleanup(func() {
				_ = holder.Close(context.Background())
				dropStaleArtifacts(t, context.Background(), control,
					holder.Snapshot.Publication, holder.Snapshot.Slot)
			})

			if state.slot != holder.Snapshot.Slot || state.snapshot != holder.Snapshot.Name ||
				state.point != holder.Snapshot.ConsistentPoint {
				t.Fatalf("state snapshot = %+v, holder = %+v", state, holder.Snapshot)
			}
			// All three server-wide timeouts are deliberately shorter than this
			// wait. The snapshot holder overrides them in the startup packet,
			// before exporting a snapshot that would be invalidated by any later
			// SET command.
			time.Sleep(1500 * time.Millisecond)
			alive, err := holder.Alive(ctx)
			if err != nil || !alive {
				t.Fatalf("snapshot holder alive = %v, error = %v", alive, err)
			}

			importer, err := pgx.Connect(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := importer.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+holder.Snapshot.Name+"'"); err != nil {
				t.Fatalf("import exported snapshot: %v", err)
			}
			_ = tx.Rollback(ctx)
			_ = importer.Close(ctx)

			var gotFailover bool
			if err := control.QueryRow(ctx,
				"SELECT failover FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
				holder.Snapshot.Slot).Scan(&gotFailover); err != nil {
				t.Fatal(err)
			}
			if gotFailover != failover {
				t.Errorf("slot failover = %v, want %v", gotFailover, failover)
			}

			cancelCtx, cancelWatch := context.WithCancel(ctx)
			cancelWatch()
			if err := <-holder.Watchdog(cancelCtx, 5*time.Millisecond); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled watchdog error = %v", err)
			}
			lost := holder.Watchdog(ctx, 5*time.Millisecond)
			var terminated bool
			if err := control.QueryRow(
				ctx,
				"SELECT pg_catalog.pg_terminate_backend($1)", int32(holder.Snapshot.BackendPID),
			).Scan(&terminated); err != nil || !terminated {
				t.Fatalf("terminate snapshot holder = %v, error = %v", terminated, err)
			}
			select {
			case err := <-lost:
				if !errors.Is(err, setup.ErrSnapshotHolderLost) {
					t.Fatalf("lost watchdog error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("watchdog did not detect terminated snapshot holder")
			}
		})
	}
}

func TestPG17RecoverStaleSetupSafely(t *testing.T) {
	instance := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	control := instance.Connect(t)
	for _, statement := range []string{
		"CREATE TABLE expected_recovery (id integer PRIMARY KEY)",
		"CREATE TABLE foreign_recovery (id integer PRIMARY KEY)",
	} {
		if _, err := control.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	var systemID, database string
	if err := control.QueryRow(ctx, `
		SELECT system_identifier::text, current_database()
		FROM pg_catalog.pg_control_system()
	`).Scan(&systemID, &database); err != nil {
		t.Fatal(err)
	}

	cfg := setup.Config{
		SourceDSN:   instance.URI,
		Dir:         t.TempDir(),
		MigrationID: "stale-safe",
		Tables:      []setup.Table{{Schema: "public", Name: "expected_recovery"}},
	}
	publication, slot := setup.Names(setup.SourceFingerprint(systemID, database), cfg.MigrationID)
	createStaleArtifacts(t, ctx, control, publication, slot, "expected_recovery")
	if err := os.WriteFile(filepath.Join(cfg.Dir, "snapshot.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := setup.RecoverStale(ctx, cfg, setup.ResumeConfirmation{}); err == nil {
		t.Fatal("recovery without durable no-snapshot confirmation succeeded")
	}
	assertArtifactsExist(t, ctx, control, publication, slot)
	if err := setup.RecoverStale(ctx, cfg, setup.ResumeConfirmation{NoSnapshot: true}); err != nil {
		t.Fatalf("recover matching stale artifacts: %v", err)
	}
	assertArtifactsAbsent(t, ctx, control, publication, slot)
	if _, err := os.Stat(filepath.Join(cfg.Dir, "snapshot.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale snapshot metadata remains: %v", err)
	}

	foreignCfg := cfg
	foreignCfg.Dir = t.TempDir()
	foreignCfg.MigrationID = "stale-foreign"
	foreignPublication, foreignSlot := setup.Names(
		setup.SourceFingerprint(systemID, database), foreignCfg.MigrationID,
	)
	createStaleArtifacts(t, ctx, control, foreignPublication, foreignSlot, "foreign_recovery")
	if err := os.WriteFile(filepath.Join(foreignCfg.Dir, "snapshot.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := setup.RecoverStale(ctx, foreignCfg, setup.ResumeConfirmation{NoSnapshot: true}); err == nil {
		t.Fatal("recovery adopted foreign publication collision")
	}
	assertArtifactsExist(t, ctx, control, foreignPublication, foreignSlot)
	if _, err := os.Stat(filepath.Join(foreignCfg.Dir, "snapshot.json")); err != nil {
		t.Fatalf("foreign collision metadata was removed: %v", err)
	}
	dropStaleArtifacts(t, ctx, control, foreignPublication, foreignSlot)

	slotForeignCfg := cfg
	slotForeignCfg.Dir = t.TempDir()
	slotForeignCfg.MigrationID = "stale-foreign-slot"
	slotForeignPublication, slotForeign := setup.Names(
		setup.SourceFingerprint(systemID, database), slotForeignCfg.MigrationID,
	)
	if _, err := control.Exec(ctx,
		`CREATE PUBLICATION "`+slotForeignPublication+`" FOR TABLE public.expected_recovery`); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx,
		"SELECT * FROM pg_catalog.pg_create_physical_replication_slot($1)", slotForeign); err != nil {
		t.Fatal(err)
	}
	if err := setup.RecoverStale(
		ctx, slotForeignCfg, setup.ResumeConfirmation{NoSnapshot: true},
	); err == nil {
		t.Fatal("recovery adopted foreign physical slot collision")
	}
	assertArtifactsExist(t, ctx, control, slotForeignPublication, slotForeign)
	dropStaleArtifacts(t, ctx, control, slotForeignPublication, slotForeign)
}

func createStaleArtifacts(
	t testing.TB,
	ctx context.Context,
	conn *pgx.Conn,
	publication, slot, table string,
) {
	t.Helper()
	if _, err := conn.Exec(ctx,
		`CREATE PUBLICATION "`+publication+`" FOR TABLE public."`+table+`"`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx,
		"SELECT * FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatal(err)
	}
}

// dropStaleArtifacts tears a fixture down. Production removal goes through
// setup.CleanupOwned, which validates ownership before dropping anything.
func dropStaleArtifacts(t testing.TB, ctx context.Context, conn *pgx.Conn, publication, slot string) {
	t.Helper()
	if _, err := conn.Exec(ctx,
		`SELECT pg_catalog.pg_drop_replication_slot($1)
		 WHERE EXISTS (SELECT FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)`,
		slot); err != nil {
		t.Fatalf("drop fixture slot %s: %v", slot, err)
	}
	if _, err := conn.Exec(ctx, `DROP PUBLICATION IF EXISTS "`+publication+`"`); err != nil {
		t.Fatalf("drop fixture publication %s: %v", publication, err)
	}
}

func assertArtifactsExist(t testing.TB, ctx context.Context, conn *pgx.Conn, publication, slot string) {
	t.Helper()
	var publications, slots int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_publication WHERE pubname=$1", publication).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1", slot).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if publications != 1 || slots != 1 {
		t.Fatalf("artifact counts publication=%d slot=%d, want 1/1", publications, slots)
	}
}

func assertArtifactsAbsent(t testing.TB, ctx context.Context, conn *pgx.Conn, publication, slot string) {
	t.Helper()
	var publications, slots int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_publication WHERE pubname=$1", publication).Scan(&publications); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1", slot).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if publications != 0 || slots != 0 {
		t.Fatalf("artifact counts publication=%d slot=%d, want 0/0", publications, slots)
	}
}
