package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
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

func TestCatalogPrunerDeletesInBatchesAndKeepsSafety(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	var lastEnd LSN
	for i := 1; i <= 5; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		lastEnd = transaction.EndLSN
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory:         directory,
		Interval:          time.Hour,
		Catalog:           writer.SegmentCatalog(),
		MaxRemoveSegments: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pruner.OnProgress(context.Background(), lastEnd+1); err != nil {
		t.Fatal(err)
	}
	if got := len(writer.SegmentCatalog().snapshot()); got != 3 {
		t.Fatalf("catalog after first batch=%d, want 3", got)
	}
	if err := pruner.OnProgress(context.Background(), lastEnd+1); err != nil {
		t.Fatal(err)
	}
	ranges := writer.SegmentCatalog().snapshot()
	if len(ranges) != 1 || ranges[0].LastEnd != lastEnd {
		t.Fatalf("catalog after second batch=%#v, want last safety segment", ranges)
	}
	segments, err := listSegments(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].start != 0x50 {
		t.Fatalf("remaining segments=%#v, want safety segment 0x50", segments)
	}
	if _, err := Recover(directory); err != nil {
		t.Fatalf("recover after catalog prune: %v", err)
	}
}

func TestSegmentPrunerRunDrainsBatchesWithoutNewProgress(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	var lastEnd LSN
	for i := 1; i <= 7; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		lastEnd = transaction.EndLSN
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory:         directory,
		Interval:          time.Hour,
		Catalog:           writer.SegmentCatalog(),
		MaxRemoveSegments: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pruner.OnProgress(context.Background(), lastEnd+1); err != nil {
		t.Fatal(err)
	}
	if got := len(writer.SegmentCatalog().snapshot()); got != 5 {
		t.Fatalf("catalog after callback batch=%d, want 5", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pruner.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for len(writer.SegmentCatalog().snapshot()) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(writer.SegmentCatalog().snapshot()); got != 1 {
		cancel()
		t.Fatalf("catalog after background batches=%d, want 1", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("pruner Run error=%v, want context cancellation", err)
	}
	segments, err := listSegments(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].start != 0x70 {
		t.Fatalf("remaining segments=%#v, want final safety segment", segments)
	}
}

func TestCatalogPrunerRejectsSegmentChangedAfterValidation(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	ranges := writer.SegmentCatalog().snapshot()
	file, err := os.OpenFile(ranges[0].Path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Nanosecond,
		Catalog:   writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = pruner.OnProgress(context.Background(), 0x40)
	if err == nil || !strings.Contains(err.Error(), "changed after validation") {
		t.Fatalf("changed segment prune error=%v", err)
	}
	if got := len(writer.SegmentCatalog().snapshot()); got != 3 {
		t.Fatalf("catalog after rejected prune=%d, want 3", got)
	}
}

func TestCatalogPrunerRefusesAnUnexplainedMissingSegment(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	ranges := writer.SegmentCatalog().snapshot()
	if err := os.Remove(ranges[0].Path); err != nil {
		t.Fatal(err)
	}
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Nanosecond,
		Catalog:   writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = pruner.OnProgress(context.Background(), 0x40)
	if err == nil || !strings.Contains(err.Error(), "missing from disk") {
		t.Fatalf("missing segment prune error=%v", err)
	}
	if got := len(writer.SegmentCatalog().snapshot()); got != 3 {
		t.Fatalf("catalog after missing segment=%d, want 3", got)
	}
	if _, err := os.Stat(ranges[1].Path); err != nil {
		t.Fatalf("pruner deleted another segment after missing file: %v", err)
	}
}

func TestCatalogPrunerRevalidatesAChangedFinalizedSegment(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	first := writer.SegmentCatalog().snapshot()[0]
	extra := testTransaction(0x15)
	extra.EndLSN = 0x16
	payload, err := MarshalTransaction(&extra)
	if err != nil {
		t.Fatal(err)
	}
	var header [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], crc32.Checksum(payload, castagnoliTable))
	file, err := os.OpenFile(first.Path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFull(file, header[:]); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := writeFull(file, payload); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Hour,
		Catalog:   writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pruner.OnProgress(context.Background(), 0x40); err != nil {
		t.Fatal(err)
	}
	ranges := writer.SegmentCatalog().snapshot()
	if len(ranges) != 3 || ranges[0].LastCommit != 0x15 || ranges[0].LastEnd != 0x16 {
		t.Fatalf("refreshed catalog=%#v", ranges)
	}
	if err := pruner.OnProgress(context.Background(), 0x40); err != nil {
		t.Fatal(err)
	}
	ranges = writer.SegmentCatalog().snapshot()
	if len(ranges) != 1 || ranges[0].StartCommit != 0x30 {
		t.Fatalf("catalog after refreshed prune=%#v", ranges)
	}
	if _, err := Recover(directory); err != nil {
		t.Fatalf("recover after refreshed prune: %v", err)
	}
}

func TestCatalogPruneAllowsOpenReaderToReachSafetySegment(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(directory, 0x10, writer.DurableEndLSN())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	transaction, err := reader.Next()
	if err != nil || transaction.CommitLSN != 0x20 {
		t.Fatalf("first reader transaction=%x err=%v, want 20", transaction.CommitLSN, err)
	}
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Nanosecond,
		Catalog:   writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pruner.OnProgress(context.Background(), 0x40); err != nil {
		t.Fatal(err)
	}
	transaction, err = reader.Next()
	if err != nil || transaction.CommitLSN != 0x30 {
		t.Fatalf("reader after unlink=%x err=%v, want safety transaction 30", transaction.CommitLSN, err)
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
