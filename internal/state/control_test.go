package state

import (
	"context"
	"errors"
	"testing"
)

func TestOpenControlWhileRunWriterIsOpen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writer, err := Open(ctx, dir, testFingerprints)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	control, err := OpenControl(ctx, dir)
	if err != nil {
		t.Fatalf("OpenControl() error = %v", err)
	}
	defer control.Close()
	if err := control.CompleteStep(ctx, "controller", "active"); err != nil {
		t.Fatalf("control write error = %v", err)
	}
	if _, err := OpenControl(ctx, dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second OpenControl() error = %v, want ErrLocked", err)
	}
	done, err := writer.StepCompleted(ctx, "controller")
	if err != nil || !done {
		t.Fatalf("writer did not observe control write: done=%v err=%v", done, err)
	}
}

func TestResetBaseCopyForcesFreshSnapshotState(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir(), testFingerprints)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.TransitionPhase(ctx, PhaseSetup); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSnapshot(ctx, "old_slot", "old_snapshot", "0/10"); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTable(ctx, Table{OID: 1, Schema: "public", Name: "items"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPart(ctx, Part{TableOID: 1, ID: "all"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompletePart(ctx, 1, "all", 1, 10, 1); err != nil {
		t.Fatal(err)
	}

	if err := store.ResetBaseCopy(ctx); err != nil {
		t.Fatal(err)
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if migration.Phase != PhasePreflight || migration.SlotName != "" ||
		migration.SnapshotName != "" || migration.ConsistentPoint != "" {
		t.Fatalf("reset migration = %#v", migration)
	}
	parts, err := store.ListParts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 0 {
		t.Fatalf("reset retained %d copy parts", len(parts))
	}
	if err := store.TransitionPhase(ctx, PhaseSetup); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSnapshot(ctx, "new_slot", "new_snapshot", "0/20"); err != nil {
		t.Fatalf("fresh snapshot after reset error = %v", err)
	}
}

func TestRecoverCutoverClearsBoundaryAndSteps(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir(), testFingerprints)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, phase := range []Phase{PhaseSetup, PhaseSchema, PhaseCopy, PhaseIndexes, PhaseCatchup, PhaseFollow, PhaseDrained} {
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SetEndPosition(ctx, "0/100"); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteStep(ctx, "cutover.end_position", "0/100"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverCutover(ctx); err != nil {
		t.Fatal(err)
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if migration.Phase != PhaseFollow || migration.EndPosition != "" {
		t.Fatalf("recovered migration = %#v", migration)
	}
	steps, err := store.ListSteps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		if len(step.Name) >= len("cutover.") && step.Name[:len("cutover.")] == "cutover." {
			t.Fatalf("cutover step survived recovery: %#v", step)
		}
	}
}

func TestTargetCleanupRequestCanBeDurablyCleared(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir(), testFingerprints)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetTargetCleanupRequested(ctx, true); err != nil {
		t.Fatal(err)
	}
	if requested, err := store.StepCompleted(ctx, "target.cleanup.requested"); err != nil || !requested {
		t.Fatalf("requested=%v err=%v", requested, err)
	}
	if err := store.SetTargetCleanupRequested(ctx, false); err != nil {
		t.Fatal(err)
	}
	if requested, err := store.StepCompleted(ctx, "target.cleanup.requested"); err != nil || requested {
		t.Fatalf("cleared request=%v err=%v", requested, err)
	}
}
