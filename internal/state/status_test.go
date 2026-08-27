package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestMigrationDoesNotReadProgress(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := openTestStore(t, t.TempDir())
	want, err := store.Migration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Metadata reads must not depend on the dashboard's progress rows.
	if _, err := store.db.ExecContext(ctx, "DELETE FROM apply_progress"); err != nil {
		t.Fatal(err)
	}
	got, err := store.Migration(ctx)
	if err != nil || got != want {
		t.Fatalf("Migration() = %+v, %v; want %+v", got, err, want)
	}
	if _, err := store.Snapshot(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Snapshot() error = %v; want missing progress row", err)
	}
}

func TestMigrationSeesControlBoundaryAndSurvivesReopen(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dir := t.TempDir()
	store := openTestStore(t, dir)
	if err := store.SetSnapshot(ctx, "slot", "snapshot", "1/A"); err != nil {
		t.Fatal(err)
	}
	control, err := OpenControl(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { control.Close() })
	reader, err := OpenReadOnly(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	for _, s := range []*Store{store, reader} {
		before, err := s.Migration(ctx)
		if err != nil || before.EndPosition != "" {
			t.Fatalf("initial boundary = %+v, %v", before, err)
		}
	}
	if err := control.SetEndPosition(ctx, "2/F"); err != nil {
		t.Fatal(err)
	}
	status, err := control.Snapshot(ctx)
	if err != nil || status.Migration.EndPosition != "2/F" {
		t.Fatalf("control snapshot = %+v, %v", status, err)
	}
	for _, s := range []*Store{store, reader} {
		got, err := s.Migration(ctx)
		if err != nil || got != status.Migration {
			t.Fatalf("Migration() = %+v, %v; want %+v", got, err, status.Migration)
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := openTestStore(t, dir).Migration(ctx)
	if err != nil || got != status.Migration {
		t.Fatalf("reopened Migration() = %+v, %v; want %+v", got, err, status.Migration)
	}
}

func TestMigrationReadErrors(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store := openTestStore(t, t.TempDir())
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Migration(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Migration() error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, "DELETE FROM migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Migration(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing Migration() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Migration(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Migration() error = %v", err)
	}
}

func BenchmarkMigrationMetadata(b *testing.B) {
	ctx := b.Context()
	store, err := Open(ctx, b.TempDir(), testFingerprints)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	// Synthetic inventory with the same object counts as the c3 migration.
	for _, statement := range []string{
		`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<113)
		 INSERT INTO tables(oid,schema_name,table_name,completed) SELECT x,'public','table_'||x,1 FROM n`,
		`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<276)
		 INSERT INTO parts(table_oid,part_id,completed) SELECT 1,x,1 FROM n`,
		`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<509)
		 INSERT INTO indexes(oid,table_oid,name,completed) SELECT x,1,'index_'||x,1 FROM n`,
		`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<12)
		 INSERT INTO constraints(oid,table_oid,name,completed) SELECT x,1,'constraint_'||x,1 FROM n`,
	} {
		if _, err := store.db.ExecContext(ctx, statement); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("Migration", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := store.Migration(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := store.Snapshot(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}
