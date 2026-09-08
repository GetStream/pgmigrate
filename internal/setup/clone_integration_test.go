//go:build integration

package setup_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/setup"
)

func TestPG17CloneRejectsWrongEndpointAndCleansSetup(t *testing.T) {
	source := pgtest.Start(t, 17)
	clone := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	control := source.Connect(t)
	if _, err := control.Exec(ctx, "CREATE TABLE clone_test(id int PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, dsn, want string }{
		{"unrelated", clone.URI, "stale or unrelated"},
		{"primary", source.URI, "no physical recovery seed LSN"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &snapshotState{}
			holder, err := setup.Run(ctx, setup.Config{
				SourceDSN: source.URI, TargetDSN: target.URI, CopySourceDSN: test.dsn,
				Dir: t.TempDir(), MigrationID: test.name,
				Tables: []setup.Table{{Schema: "public", Name: "clone_test"}},
			}, state)
			if holder != nil || err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("holder=%v err=%v, want %q", holder, err, test.want)
			}
			if state.slot != "" {
				t.Fatal("unvalidated clone was persisted")
			}
			var count int
			if err := control.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgmigrate_slot_%'").Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed clone left %d slots: %v", count, err)
			}
			if err := control.QueryRow(ctx, "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'pgmigrate_pub_%'").Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed clone left %d publications: %v", count, err)
			}
		})
	}
	t.Run("resume before first feedback", func(t *testing.T) {
		cfg := setup.Config{
			SourceDSN: source.URI, TargetDSN: target.URI, Dir: t.TempDir(), MigrationID: "resume",
			Tables: []setup.Table{{Schema: "public", Name: "clone_test"}},
		}
		holder, err := setup.Run(ctx, cfg, &snapshotState{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = holder.Close(context.Background())
			_ = setup.CleanupOwned(context.Background(), source.URI, holder.Snapshot.Publication, holder.Snapshot.Slot, cfg.Tables, false)
		}()
		if _, err := control.Exec(ctx, "INSERT INTO clone_test VALUES(1)"); err != nil {
			t.Fatal(err)
		}
		snapshot := holder.Snapshot
		snapshot.CopySource = true
		snapshot.SlotConsistentPoint = snapshot.ConsistentPoint
		if err := control.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&snapshot.ConsistentPoint); err != nil {
			t.Fatal(err)
		}
		if snapshot.ConsistentPoint == snapshot.SlotConsistentPoint {
			t.Fatal("fixture did not advance past slot creation")
		}
		_ = holder.Close(ctx)
		if err := setup.ValidateResume(ctx, cfg, snapshot); err != nil {
			t.Fatalf("slot still at creation cannot resume clone stream: %v", err)
		}
		snapshot.SlotConsistentPoint = ""
		if err := setup.ValidateResume(ctx, cfg, snapshot); err == nil {
			t.Fatal("clone without original slot position accepted")
		}
	})
	t.Run("cancel while waiting", func(t *testing.T) {
		waitCtx, stop := context.WithCancel(ctx)
		defer stop()
		_, err := setup.Run(waitCtx, setup.Config{
			SourceDSN: source.URI, TargetDSN: target.URI,
			CopySourceDSN:   "postgres://app:app@127.0.0.1:1/app?sslmode=disable",
			CopySourceReady: stop, Dir: t.TempDir(), MigrationID: "cancel",
			Tables: []setup.Table{{Schema: "public", Name: "clone_test"}},
		}, &snapshotState{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting cancellation = %v", err)
		}
		var count int
		if err := control.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'pgmigrate_slot_%'").Scan(&count); err != nil || count != 0 {
			t.Fatalf("cancelled clone left %d slots: %v", count, err)
		}
	})
}
