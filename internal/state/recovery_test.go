package state

import (
	"context"
	"testing"
	"time"
)

func TestOpenUpgradesRecoveryProgress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openTestStore(t, dir)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := openTestDB(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"DROP TABLE recovery_progress",
		"PRAGMA user_version = 7",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := openTestStore(t, dir)
	version, err := schemaVersionOf(ctx, upgraded.db)
	if err != nil || version != schemaVersion {
		t.Fatalf("version after upgrade = %d (want %d): %v", version, schemaVersion, err)
	}
	status, err := upgraded.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Recovery != (RecoveryProgress{}) {
		t.Fatalf("upgraded recovery progress = %#v", status.Recovery)
	}
	var rows int
	if err := upgraded.db.QueryRowContext(
		ctx,
		"SELECT count(*) FROM recovery_progress",
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("recovery progress rows = %d, want 1", rows)
	}
}

func TestUpdateRecoveryProgressReplacesSingleton(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())
	first := RecoveryProgress{
		TotalBytes:         32 << 20,
		TrustedBytes:       24 << 20,
		ScannedBytes:       8 << 20,
		TotalSegments:      8,
		TrustedSegments:    6,
		ScannedSegments:    2,
		Elapsed:            2 * time.Second,
		ScanBytesPerSecond: 4 << 20,
		FallbackReason:     "catalog checksum mismatch",
		ManifestRebuilt:    true,
	}
	if err := store.UpdateRecoveryProgress(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateRecoveryProgress(ctx, first); err != nil {
		t.Fatalf("idempotent update: %v", err)
	}

	replacement := RecoveryProgress{
		TotalBytes:         40 << 20,
		TrustedBytes:       40 << 20,
		TotalSegments:      10,
		TrustedSegments:    10,
		Elapsed:            1250 * time.Millisecond,
		ScanBytesPerSecond: 0,
	}
	if err := store.UpdateRecoveryProgress(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	status, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Recovery != replacement {
		t.Fatalf("recovery progress = %#v, want %#v", status.Recovery, replacement)
	}
	var rows int
	if err := store.db.QueryRowContext(
		ctx,
		"SELECT count(*) FROM recovery_progress",
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("recovery progress rows = %d, want 1", rows)
	}
}
