package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenReadOnlyWhileWriterHoldsLock(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writer, err := Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer writer.Close()

	reader, err := OpenReadOnly(ctx, dir)
	if err != nil {
		t.Fatalf("OpenReadOnly() while writer is locked error = %v", err)
	}
	defer reader.Close()

	status, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("reader Snapshot() error = %v", err)
	}
	if status.Migration.Phase != PhasePreflight {
		t.Errorf("reader phase = %q, want %q", status.Migration.Phase, PhasePreflight)
	}

	if err := writer.TransitionPhase(ctx, PhaseSetup); err != nil {
		t.Fatalf("writer TransitionPhase() error = %v", err)
	}
	migration, err := reader.Migration(ctx)
	if err != nil {
		t.Fatalf("reader Migration() after write error = %v", err)
	}
	if migration.Phase != PhaseSetup {
		t.Errorf("reader phase after write = %q, want %q", migration.Phase, PhaseSetup)
	}
	if err := reader.CompleteStep(ctx, "not-allowed", ""); !errors.Is(err, ErrReadOnly) {
		t.Errorf("read-only mutation error = %v, want ErrReadOnly", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader Close() error = %v", err)
	}
	if _, err := reader.Snapshot(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("Snapshot() after close error = %v, want ErrClosed", err)
	}
}

func TestOpenReadOnlyMissingState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, err := OpenReadOnly(ctx, dir)
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("OpenReadOnly() error = %v, want ErrStateNotFound", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.db")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("OpenReadOnly() created state.db: stat error = %v", err)
	}

	missingDir := filepath.Join(t.TempDir(), "absent")
	_, err = OpenReadOnly(ctx, missingDir)
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("OpenReadOnly() missing directory error = %v, want ErrStateNotFound", err)
	}
	if _, err := os.Stat(missingDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("OpenReadOnly() created directory: stat error = %v", err)
	}
}

func TestInventoryListsPersistAndFilterPending(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writer, err := Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatal(err)
	}

	for _, table := range []Table{
		{OID: 1, Schema: "public", Name: "done", Bytes: 10, PartsTotal: 1},
		{OID: 2, Schema: "public", Name: "pending", Bytes: 20, PartsTotal: 1},
	} {
		if err := writer.UpsertTable(ctx, table); err != nil {
			t.Fatal(err)
		}
	}
	for _, part := range []Part{
		{TableOID: 1, ID: "all", RangeStart: "0", RangeEnd: "10"},
		{TableOID: 2, ID: "all", RangeStart: "10", RangeEnd: "20"},
	} {
		if err := writer.UpsertPart(ctx, part); err != nil {
			t.Fatal(err)
		}
	}
	for _, index := range []Index{
		{OID: 11, TableOID: 1, Name: "done_idx", Definition: "CREATE INDEX done_idx", Bytes: 5},
		{OID: 12, TableOID: 2, Name: "pending_idx", Definition: "CREATE INDEX pending_idx", Bytes: 6},
	} {
		if err := writer.UpsertIndex(ctx, index); err != nil {
			t.Fatal(err)
		}
	}
	for _, constraint := range []Constraint{
		{OID: 21, TableOID: 1, Name: "done_pk", Kind: "p", Definition: "PRIMARY KEY"},
		{OID: 22, TableOID: 2, Name: "pending_pk", Kind: "p", Definition: "PRIMARY KEY"},
	} {
		if err := writer.UpsertConstraint(ctx, constraint); err != nil {
			t.Fatal(err)
		}
	}
	for _, verified := range []VerifyTable{
		{
			TableOID: 1, Stage: "done", SourcePages: 4, SourcePagesTotal: 4,
			Sampled: 40, Estimated: 40, Converged: true, Complete: true,
		},
		{TableOID: 2, Stage: "sampling", SourcePages: 1, SourcePagesTotal: 4, Sampled: 10, Estimated: 40},
	} {
		if err := writer.UpsertVerifyTable(ctx, verified); err != nil {
			t.Fatal(err)
		}
	}
	for _, finding := range []Finding{
		{ID: "resolved", Kind: "preflight", Severity: "yellow", Message: "acknowledged"},
		{ID: "open", Kind: "divergence", Severity: "red", Message: "mismatch"},
	} {
		if err := writer.UpsertFinding(ctx, finding); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.ResolveFinding(ctx, "resolved"); err != nil {
		t.Fatal(err)
	}
	if err := writer.UpsertStep(ctx, Step{Name: "done-step", Detail: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompleteStep(ctx, "done-step", "one"); err != nil {
		t.Fatal(err)
	}
	if err := writer.UpsertStep(ctx, Step{Name: "pending-step", Detail: "two"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompletePart(ctx, 1, "all", 10, 100, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompleteTable(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompleteIndex(ctx, 11); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompleteConstraint(ctx, 21); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenReadOnly(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	assertLengths := func(name string, allLen, pendingLen int, all func() (int, error), pending func() (int, error)) {
		t.Helper()
		gotAll, err := all()
		if err != nil {
			t.Errorf("%s list error = %v", name, err)
		}
		gotPending, err := pending()
		if err != nil {
			t.Errorf("%s pending error = %v", name, err)
		}
		if gotAll != allLen || gotPending != pendingLen {
			t.Errorf("%s lengths = %d/%d, want %d/%d", name, gotAll, gotPending, allLen, pendingLen)
		}
	}
	assertLengths("tables", 2, 1,
		func() (int, error) { v, e := reader.ListTables(ctx); return len(v), e },
		func() (int, error) { v, e := reader.PendingTables(ctx); return len(v), e })
	assertLengths("parts", 2, 1,
		func() (int, error) { v, e := reader.ListParts(ctx); return len(v), e },
		func() (int, error) { v, e := reader.PendingParts(ctx); return len(v), e })
	assertLengths("indexes", 2, 1,
		func() (int, error) { v, e := reader.ListIndexes(ctx); return len(v), e },
		func() (int, error) { v, e := reader.PendingIndexes(ctx); return len(v), e })
	assertLengths("constraints", 2, 1,
		func() (int, error) { v, e := reader.ListConstraints(ctx); return len(v), e },
		func() (int, error) { v, e := reader.PendingConstraints(ctx); return len(v), e })
	assertLengths("findings", 2, 1,
		func() (int, error) { v, e := reader.ListFindings(ctx); return len(v), e },
		func() (int, error) { v, e := reader.PendingFindings(ctx); return len(v), e })
	assertLengths("steps", 2, 1,
		func() (int, error) { v, e := reader.ListSteps(ctx); return len(v), e },
		func() (int, error) { v, e := reader.PendingSteps(ctx); return len(v), e })

	tables, err := reader.PendingTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tables[0].OID != 2 || tables[0].Name != "pending" || tables[0].Bytes != 20 {
		t.Errorf("pending table did not round-trip: %#v", tables[0])
	}
	parts, err := reader.ListParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !parts[0].Completed || parts[0].Rows != 10 || parts[0].Bytes != 100 {
		t.Errorf("completed part did not round-trip: %#v", parts[0])
	}
}
