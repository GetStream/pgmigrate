package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

var testFingerprints = Fingerprints{Source: "source-system-123", Filter: "filter-sha256"}

func openTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	store, err := Open(context.Background(), dir, testFingerprints)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return store
}

// TestOpenUpgradesAnOlderStateDirectory covers the cost of having no migration
// path at all. A directory written by an earlier binary kept its old columns and
// its old version, so a new binary opened it happily and failed later, mid-run —
// and the only way to resume a copy that had taken hours was to add the columns
// by hand.
func TestOpenUpgradesAnOlderStateDirectory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openTestStore(t, dir)
	if err := store.UpsertTable(ctx, Table{OID: 42, Schema: "public", Name: "widgets"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIndex(ctx, Index{
		OID: 100, TableOID: 42, Name: "widgets_pkey", Definition: "CREATE UNIQUE INDEX ...",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordIndexTargetDefinition(ctx, 100, "CREATE UNIQUE INDEX ..."); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Recreate what version 1 left on disk: the pre-upgrade columns, the rows an
	// interrupted migration had already recorded, and the old version number.
	downgrade := func(t *testing.T) {
		t.Helper()
		db, err := openTestDB(t, dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, statement := range []string{
			"ALTER TABLE indexes DROP COLUMN target_definition",
			"ALTER TABLE constraints DROP COLUMN target_definition",
			"PRAGMA user_version = 1",
		} {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("%s: %v", statement, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	downgrade(t)

	upgraded := openTestStore(t, dir)
	if version, err := schemaVersionOf(ctx, upgraded.db); err != nil || version != schemaVersion {
		t.Fatalf("version after upgrade = %d (want %d): %v", version, schemaVersion, err)
	}
	// The rows an older run recorded survive, with no recorded target rendering,
	// which is exactly how the indexes phase treats an object it has not yet seen.
	indexes, err := upgraded.ListIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].Name != "widgets_pkey" {
		t.Fatalf("indexes after upgrade = %#v", indexes)
	}
	rendering, err := upgraded.IndexTargetDefinition(ctx, 100)
	if err != nil || rendering != "" {
		t.Fatalf("recorded target rendering after upgrade = %q: %v", rendering, err)
	}
	if err := upgraded.RecordIndexTargetDefinition(ctx, 100, "CREATE UNIQUE INDEX ..."); err != nil {
		t.Fatalf("record target rendering into upgraded column: %v", err)
	}
	// Upgrading twice must be a no-op rather than an error.
	if err := upgraded.Close(); err != nil {
		t.Fatal(err)
	}
	openTestStore(t, dir)
}

// TestVerifyTablesCarryForwardFromTheTwoSidedSchema covers the migration that
// reshapes verification's own records. A directory left by a run of the exhaustive
// comparison has to open, because the alternative is redoing a copy that took
// hours, and its progress rows have to survive losing the columns the sample no
// longer produces.
func TestVerifyTablesCarryForwardFromTheTwoSidedSchema(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openTestStore(t, dir)
	if err := store.UpsertTable(ctx, Table{OID: 42, Schema: "public", Name: "widgets"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := openTestDB(t, dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"ALTER TABLE verify_tables DROP COLUMN sampled_rows",
		"ALTER TABLE verify_tables DROP COLUMN estimated_rows",
		"ALTER TABLE verify_tables DROP COLUMN target_rows",
		"ALTER TABLE verify_tables DROP COLUMN candidate_rows",
		"ALTER TABLE verify_tables ADD COLUMN target_pages INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE verify_tables ADD COLUMN target_pages_total INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE verify_tables ADD COLUMN divergent_buckets INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE verify_tables ADD COLUMN rows_done INTEGER NOT NULL DEFAULT 0",
		`INSERT INTO verify_tables
			(table_oid, stage, source_pages, source_pages_total, target_pages,
			 target_pages_total, rows_done, divergent_buckets, coverage, complete, updated_at)
		 VALUES (42, 'target pass', 900, 900, 400, 400, 20000, 2, 1, 1, 0)`,
		"PRAGMA user_version = 4",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	upgraded := openTestStore(t, dir)
	if version, err := schemaVersionOf(ctx, upgraded.db); err != nil || version != schemaVersion {
		t.Fatalf("version after upgrade = %d (want %d): %v", version, schemaVersion, err)
	}
	tables, err := upgraded.ListVerifyTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || tables[0].TableOID != 42 || tables[0].SourcePages != 900 {
		t.Fatalf("verify tables after upgrade = %#v", tables)
	}
	// The sampled figures start at zero rather than inheriting a count that meant
	// something else, and the next observation of the table overwrites them.
	if tables[0].Sampled != 0 || tables[0].Estimated != 0 || tables[0].Candidates != 0 {
		t.Fatalf("sampled figures should start empty: %#v", tables[0])
	}
	if err := upgraded.UpsertVerifyTable(ctx, VerifyTable{
		TableOID: 42, Stage: "done", Sampled: 1000, Estimated: 20000, Coverage: 0.05,
		Converged: true, Complete: true,
	}); err != nil {
		t.Fatalf("write into the upgraded columns: %v", err)
	}
}

// TestOpenRefusesNewerStateDirectory is the other half: a directory this binary
// cannot read must say so at open time, naming both versions.
func TestOpenRefusesNewerStateDirectory(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for name, open := range map[string]func() error{
		"Open":         func() error { _, err := Open(ctx, dir, testFingerprints); return err },
		"OpenReadOnly": func() error { _, err := OpenReadOnly(ctx, dir); return err },
		"OpenControl":  func() error { _, err := OpenControl(ctx, dir); return err },
	} {
		var mismatch *SchemaVersionError
		err := open()
		if !errors.As(err, &mismatch) {
			t.Fatalf("%s error = %v, want SchemaVersionError", name, err)
		}
		if mismatch.Have != schemaVersion+1 || mismatch.Want != schemaVersion {
			t.Fatalf("%s reported versions %d/%d", name, mismatch.Have, mismatch.Want)
		}
		if !strings.Contains(err.Error(), "newer pgmigrate") {
			t.Fatalf("%s message is not actionable: %v", name, err)
		}
	}
}

func TestOpenWaitsForConcurrentSQLiteStartupWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	initial, err := Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}

	// This connection deliberately bypasses sqliteDSN and holds SQLite's writer
	// lock across the next Open. It models a controller or prior writer finishing
	// one transaction at the same instant a crashed run restarts.
	blocker, err := sql.Open("sqlite", filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	blocker.SetMaxOpenConns(1)
	conn, err := blocker.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "UPDATE migration SET updated_at = updated_at WHERE id = 1"); err != nil {
		t.Fatal(err)
	}

	released := make(chan error, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			_, err := conn.ExecContext(ctx, "COMMIT")
			released <- err
		case <-ctx.Done():
			released <- ctx.Err()
		}
	}()
	<-ready
	started := time.Now()
	resumed, openErr := Open(ctx, dir, testFingerprints)
	waited := time.Since(started)
	if err := <-released; err != nil {
		t.Fatalf("release startup writer: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}
	if openErr != nil {
		t.Fatalf("Open() after startup writer released its lock: %v", openErr)
	}
	if waited < 100*time.Millisecond {
		_ = resumed.Close()
		t.Fatalf("Open() waited only %s for a writer held for 200ms", waited)
	}
	if err := resumed.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDSNInstallsBusyTimeoutOnFirstConnection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	initial, err := Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}

	for _, readOnly := range []bool{false, true} {
		name := "read-write"
		if readOnly {
			name = "read-only"
		}
		t.Run(name, func(t *testing.T) {
			dsn, err := sqliteDSN(filepath.Join(dir, "state.db"), readOnly)
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.PingContext(ctx); err != nil {
				t.Fatal(err)
			}
			var timeout int
			if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeout); err != nil {
				t.Fatal(err)
			}
			if timeout != 5000 {
				t.Fatalf("busy_timeout = %d, want 5000", timeout)
			}
		})
	}
}

func openTestDB(t *testing.T, dir string) (*sql.DB, error) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, "state.db"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func TestOpenInitializesDirectoryAndSchema(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "migration")
	store := openTestStore(t, dir)

	for _, name := range []string{"LOCK", "state.db", "dump", "cdc", "log"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s was not initialized: %v", name, err)
		}
	}

	var mode string
	if err := store.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal mode = %q, want wal", mode)
	}

	wantTables := []string{
		"migration", "tables", "parts", "indexes", "constraints",
		"apply_progress", "verify_tables", "findings", "steps", "failed_attempt",
	}
	for _, name := range wantTables {
		var count int
		if err := store.db.QueryRow(
			"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name,
		).Scan(&count); err != nil {
			t.Fatalf("inspect schema table %s: %v", name, err)
		}
		if count != 1 {
			t.Errorf("schema table %s count = %d, want 1", name, count)
		}
	}

	status, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if status.Migration.Phase != PhasePreflight {
		t.Errorf("initial phase = %q, want %q", status.Migration.Phase, PhasePreflight)
	}
	if status.Migration.SourceFingerprint != testFingerprints.Source ||
		status.Migration.FilterFingerprint != testFingerprints.Filter {
		t.Errorf("initial fingerprints = %#v", status.Migration)
	}
}

func TestOpenLockContentionAndCloseUnlock(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(context.Background(), dir, testFingerprints)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}

	_, err = Open(context.Background(), dir, testFingerprints)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("contending Open() error = %v, want ErrLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, err := Open(context.Background(), dir, testFingerprints)
	if err != nil {
		t.Fatalf("Open() after unlock error = %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("idempotent Close() error = %v", err)
	}
}

func TestFingerprintMismatch(t *testing.T) {
	dir := t.TempDir()
	store := openTestStore(t, dir)
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	tests := []struct {
		name  string
		value Fingerprints
		field string
	}{
		{"source", Fingerprints{Source: "other-source", Filter: testFingerprints.Filter}, "source"},
		{"filter", Fingerprints{Source: testFingerprints.Source, Filter: "other-filter"}, "filter"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Open(context.Background(), dir, test.value)
			var mismatch *FingerprintMismatchError
			if !errors.As(err, &mismatch) {
				t.Fatalf("Open() error = %v, want FingerprintMismatchError", err)
			}
			if mismatch.Field != test.field {
				t.Errorf("mismatch field = %q, want %q", mismatch.Field, test.field)
			}
		})
	}
}

func TestPersistenceProgressAndIdempotency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err := store.SetSnapshot(ctx, "slot", "snapshot", "1/A"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSnapshot(ctx, "slot", "snapshot", "1/A"); err != nil {
		t.Fatalf("idempotent SetSnapshot() error = %v", err)
	}
	if err := store.UpsertTable(ctx, Table{
		OID: 42, Schema: "public", Name: "orders", EstimatedRows: 100, Bytes: 4096, PartsTotal: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPart(ctx, Part{TableOID: 42, ID: "all"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompletePart(ctx, 42, "all", 100, 4096, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.CompletePart(ctx, 42, "all", 999, 999, time.Hour); err != nil {
		t.Fatalf("idempotent CompletePart() error = %v", err)
	}
	if err := store.CompleteTable(ctx, 42); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTable(ctx, 42); err != nil {
		t.Fatalf("idempotent CompleteTable() error = %v", err)
	}
	if err := store.UpsertIndex(ctx, Index{OID: 100, TableOID: 42, Name: "orders_pkey"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteIndex(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertConstraint(ctx, Constraint{
		OID: 101, TableOID: 42, Name: "orders_pkey", Kind: "primary",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteConstraint(ctx, 101); err != nil {
		t.Fatal(err)
	}
	completionChecks := []struct {
		name string
		fn   func() (bool, error)
	}{
		{"table", func() (bool, error) { return store.TableCompleted(ctx, 42) }},
		{"part", func() (bool, error) { return store.PartCompleted(ctx, 42, "all") }},
		{"index", func() (bool, error) { return store.IndexCompleted(ctx, 100) }},
		{"constraint", func() (bool, error) { return store.ConstraintCompleted(ctx, 101) }},
	}
	for _, check := range completionChecks {
		done, err := check.fn()
		if err != nil || !done {
			t.Errorf("%s completion = %t, %v; want true, nil", check.name, done, err)
		}
	}
	applyUpdatedAt := time.Date(2026, time.August, 21, 12, 34, 56, 789, time.UTC)
	if err := store.UpdateApplyProgress(ctx, ApplyProgress{
		StagedLSN: "1/C", AppliedLSN: "1/B", Txns: 7, Rows: 23, UpdatedAt: applyUpdatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertVerifyTable(ctx, VerifyTable{
		TableOID: 42, Stage: "done", SourcePages: 8, SourcePagesTotal: 8,
		Sampled: 100, Estimated: 100, TargetRows: 100, Coverage: 1,
		Converged: true, Complete: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertFinding(ctx, Finding{
		ID: "wal-level", Kind: "preflight", Severity: "red", Message: "wrong wal_level",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveFinding(ctx, "wal-level"); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteStep(ctx, "schema-dump", "schema.dump"); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteStep(ctx, "schema-dump", "different"); err != nil {
		t.Fatalf("idempotent CompleteStep() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	store, err = Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer store.Close()
	status, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if status.Tables != (Counts{Done: 1, Total: 1}) ||
		status.Parts != (Counts{Done: 1, Total: 1}) ||
		status.Indexes != (Counts{Done: 1, Total: 1}) ||
		status.Constraints != (Counts{Done: 1, Total: 1}) ||
		status.VerifyTables != (Counts{Done: 1, Total: 1}) {
		t.Errorf("unexpected persisted counts: %#v", status)
	}
	if status.Apply.AppliedLSN != "1/B" || status.Apply.StagedLSN != "1/C" ||
		status.Apply.Txns != 7 || status.Apply.Rows != 23 || !status.Apply.UpdatedAt.Equal(applyUpdatedAt) {
		t.Errorf("unexpected persisted apply progress: %#v", status.Apply)
	}
	if status.OpenFindings != 0 || status.CompletedSteps != 1 {
		t.Errorf("finding/step status = %d/%d", status.OpenFindings, status.CompletedSteps)
	}
	if status.Migration.SlotName != "slot" || status.Migration.ConsistentPoint != "1/A" {
		t.Errorf("unexpected persisted migration: %#v", status.Migration)
	}
	var rows, bytes, duration int64
	if err := store.db.QueryRow(
		`
		SELECT rows_copied, bytes_copied, duration_ns
		FROM parts WHERE table_oid=42 AND part_id='all'`,
	).Scan(&rows, &bytes, &duration); err != nil {
		t.Fatal(err)
	}
	if rows != 100 || bytes != 4096 || duration != int64(3*time.Second) {
		t.Errorf("completion was overwritten: rows=%d bytes=%d duration=%s",
			rows, bytes, time.Duration(duration))
	}
}

func TestPhaseTransitionsAndEndPosition(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())
	phases := []Phase{
		PhaseSetup, PhaseSchema, PhaseCopy, PhaseIndexes, PhaseCatchup,
		PhaseFollow, PhaseDrained, PhaseCutover, PhaseComplete,
	}
	for _, phase := range phases {
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatalf("TransitionPhase(%s) error = %v", phase, err)
		}
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatalf("repeated TransitionPhase(%s) error = %v", phase, err)
		}
	}
	if err := store.TransitionPhase(ctx, PhaseFollow); !errors.Is(err, ErrInvalidPhaseTransition) {
		t.Errorf("backward transition error = %v, want ErrInvalidPhaseTransition", err)
	}

	dir := t.TempDir()
	second := openTestStore(t, dir)
	if err := second.TransitionPhase(ctx, PhaseSchema); !errors.Is(err, ErrInvalidPhaseTransition) {
		t.Errorf("skipped transition error = %v, want ErrInvalidPhaseTransition", err)
	}
	if err := second.SetEndPosition(ctx, "2/F"); err != nil {
		t.Fatal(err)
	}
	if err := second.SetEndPosition(ctx, "2/F"); err != nil {
		t.Fatalf("repeated SetEndPosition() error = %v", err)
	}
	if err := second.SetEndPosition(ctx, "3/A"); err == nil {
		t.Error("changing end position unexpectedly succeeded")
	}
}

func TestFailedAttemptCountsOnlyConsecutiveIdenticalFailures(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openTestStore(t, dir)
	attempt, err := store.FailedAttempt(ctx)
	if err != nil || attempt.Consecutive != 0 {
		t.Fatalf("fresh state attempt = %#v err = %v", attempt, err)
	}
	for want := 1; want <= 3; want++ {
		if err := store.RecordFailedAttempt(ctx, PhaseCopy, "sqlstate:53000", "no empty local buffer available"); err != nil {
			t.Fatal(err)
		}
		attempt, err := store.FailedAttempt(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if attempt.Consecutive != want || attempt.Phase != PhaseCopy || attempt.ObservedAt.IsZero() {
			t.Fatalf("attempt %d = %#v", want, attempt)
		}
	}
	// A different reason, or the same reason in a different phase, starts over.
	if err := store.RecordFailedAttempt(ctx, PhaseCopy, "sqlstate:57P01", "terminating connection"); err != nil {
		t.Fatal(err)
	}
	attempt, err = store.FailedAttempt(ctx)
	if err != nil || attempt.Consecutive != 1 || attempt.Detail != "terminating connection" {
		t.Fatalf("attempt after a new reason = %#v err = %v", attempt, err)
	}
	if err := store.RecordFailedAttempt(ctx, PhaseSchema, "sqlstate:57P01", "terminating connection"); err != nil {
		t.Fatal(err)
	}
	if attempt, err = store.FailedAttempt(ctx); err != nil || attempt.Consecutive != 1 {
		t.Fatalf("attempt after a new phase = %#v err = %v", attempt, err)
	}
	// The record has to outlive the reset it exists to guard, and the process
	// that wrote it.
	for _, phase := range []Phase{PhaseSetup, PhaseSchema, PhaseCopy} {
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordFailedAttempt(ctx, PhaseCopy, "sqlstate:53000", "no empty local buffer available"); err != nil {
		t.Fatal(err)
	}
	if err := store.ResetBaseCopy(ctx); err != nil {
		t.Fatal(err)
	}
	store.Close()
	reopened := openTestStore(t, dir)
	if attempt, err = reopened.FailedAttempt(ctx); err != nil || attempt.Consecutive != 1 || attempt.Signature != "sqlstate:53000" {
		t.Fatalf("attempt after reset and reopen = %#v err = %v", attempt, err)
	}
	if err := reopened.ClearFailedAttempt(ctx); err != nil {
		t.Fatal(err)
	}
	if attempt, err = reopened.FailedAttempt(ctx); err != nil || attempt.Consecutive != 0 {
		t.Fatalf("cleared attempt = %#v err = %v", attempt, err)
	}
}

func TestResolveFailedAttemptDoesNotHideALaterFailure(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())
	const findingID = "cdc-divergence"
	record := func() {
		t.Helper()
		if err := store.UpsertFinding(ctx, Finding{
			ID: findingID, Kind: "divergence", Severity: "error", Message: "replay diverged",
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordFailedAttempt(
			ctx, PhaseCatchup, "error:divergence", "replay diverged",
		); err != nil {
			t.Fatal(err)
		}
	}

	record()
	baseline, err := store.FailedAttempt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A recurrence before the old baseline is resolved is a newer failure and
	// must not be erased by progress belonging to the resumed attempt.
	record()
	cleared, err := store.ResolveFailedAttempt(ctx, baseline, findingID)
	if err != nil {
		t.Fatal(err)
	}
	if cleared {
		t.Fatal("stale failure baseline cleared a later recurrence")
	}
	current, err := store.FailedAttempt(ctx)
	if err != nil || current.Consecutive != 2 {
		t.Fatalf("later failure=%#v err=%v, want consecutive=2", current, err)
	}
	if findings, err := store.PendingFindings(ctx); err != nil || len(findings) != 1 {
		t.Fatalf("pending divergence after stale clear=%#v err=%v", findings, err)
	}

	cleared, err = store.ResolveFailedAttempt(ctx, current, findingID)
	if err != nil || !cleared {
		t.Fatalf("current failure cleared=%t err=%v", cleared, err)
	}
	if attempt, err := store.FailedAttempt(ctx); err != nil || attempt.Consecutive != 0 {
		t.Fatalf("resolved failure=%#v err=%v", attempt, err)
	}
	if findings, err := store.PendingFindings(ctx); err != nil || len(findings) != 0 {
		t.Fatalf("resolved divergence remains pending=%#v err=%v", findings, err)
	}

	// A recurrence after proven progress is a new blocker, not historical state.
	record()
	if attempt, err := store.FailedAttempt(ctx); err != nil || attempt.Consecutive != 1 {
		t.Fatalf("new failure=%#v err=%v, want consecutive=1", attempt, err)
	}
	if findings, err := store.PendingFindings(ctx); err != nil || len(findings) != 1 {
		t.Fatalf("new divergence blocker=%#v err=%v", findings, err)
	}
}

func TestConcurrentWritesAreSerialized(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())

	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.CompleteStep(ctx, "shared-step", "done")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent CompleteStep() error = %v", err)
		}
	}
	status, err := store.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.CompletedSteps != 1 {
		t.Errorf("completed steps = %d, want 1", status.CompletedSteps)
	}
}
