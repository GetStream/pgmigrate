package cdc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegmentPrunerBoundsSustainedAppliedSegments(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var lastEnd LSN
	for i := 1; i <= 30; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		lastEnd = transaction.EndLSN
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
		if err := pruner.OnProgress(context.Background(), lastEnd); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegments(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) > 2 {
		t.Fatalf("segments after sustained apply = %d, want at most safety plus latest", len(segments))
	}
	recovery, err := Recover(directory)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.DurableLSN != lastEnd {
		t.Fatalf("recovered durable LSN = %x, want %x", recovery.DurableLSN, lastEnd)
	}
	if err := pruner.OnProgress(context.Background(), lastEnd+1); err != nil {
		t.Fatal(err)
	}
	segments, err = listSegments(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("segments after all became eligible = %d, want one safety segment", len(segments))
	}
	if _, err := Recover(directory); err != nil {
		t.Fatalf("recover retained safety segment: %v", err)
	}
}

func TestSegmentPrunerErrorsAreActionable(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	err = pruner.OnProgress(context.Background(), 10)
	if err == nil {
		t.Fatal("expected prune error")
	}
	if isConnectionError(err) {
		t.Fatalf("maintenance error was incorrectly retryable: %v", err)
	}
}

func TestNormalizeEndPositionDetectsExactAndFloorsManualLSN(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct{ commit, end LSN }{
		{0x10, 0x18},
		{0x20, 0x2f},
		{0x30, 0x3a},
	} {
		transaction := testTransaction(pair.commit)
		transaction.EndLSN = pair.end
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	exact, err := NormalizeEndPosition(directory, 0x2f, 0x3a)
	if err != nil {
		t.Fatal(err)
	}
	if !exact.Exact || exact.Boundary != 0x2f {
		t.Fatalf("exact resolution = %#v", exact)
	}
	floored, err := NormalizeEndPosition(directory, 0x25, 0x3a)
	if err != nil {
		t.Fatal(err)
	}
	if floored.Exact || floored.Boundary != 0x18 {
		t.Fatalf("floored resolution = %#v", floored)
	}
	ahead, err := NormalizeEndPosition(directory, 0x50, 0x3a)
	if err != nil {
		t.Fatal(err)
	}
	if ahead.Exact || ahead.Boundary != 0x3a {
		t.Fatalf("ahead-of-durable resolution = %#v", ahead)
	}

	watermark := new(DurableWatermark)
	watermark.Publish(0x3a)
	applier := &Applier{config: ApplierConfig{
		Directory: directory,
		Durable:   watermark,
		EndPosition: func(context.Context) (LSN, bool, error) {
			return 0x25, true, nil
		},
	}}
	end, set, err := applier.effectiveEndPosition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !set || end != 0x18 {
		t.Fatalf("effective applier end = %x (set %v), want 18", end, set)
	}
}

// TestEffectiveEndPositionResolvesTheBoundaryOnce guards the cost of consulting
// the cutover boundary. NormalizeEndPosition decodes every staged transaction
// from the start of the retained set, and the apply loop consults the boundary
// once per transaction, so resolving it each time made apply cost grow with the
// square of the staged stream and burned whole cores on a real shard.
//
// Emptying the directory after the first call is the observable proof: a second
// call that rescans cannot find a boundary at all, while a cached one is unmoved.
func TestEffectiveEndPositionResolvesTheBoundaryOnce(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	transaction := testTransaction(0x10)
	transaction.EndLSN = 0x18
	if err := writer.Append(&transaction); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	watermark := new(DurableWatermark)
	watermark.Publish(0x3a)
	applier := &Applier{config: ApplierConfig{
		Directory: directory,
		Durable:   watermark,
		EndPosition: func(context.Context) (LSN, bool, error) {
			return 0x25, true, nil
		},
	}}
	first, set, err := applier.effectiveEndPosition(context.Background())
	if err != nil || !set || first != 0x18 {
		t.Fatalf("first resolution = %x (set %v): %v", first, set, err)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(directory, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	second, set, err := applier.effectiveEndPosition(context.Background())
	if err != nil || !set || second != first {
		t.Fatalf("second resolution = %x (set %v): %v", second, set, err)
	}
}

func TestNormalizeEndPositionRejectsUnavailableBoundary(t *testing.T) {
	t.Parallel()
	_, err := NormalizeEndPosition(t.TempDir(), 0x20, 0x10)
	var unavailable *EndPositionUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("error = %v, want EndPositionUnavailableError", err)
	}
}
