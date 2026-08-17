package cdc

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCatalogRecoveryReadsNoFinalizedPayload(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 64; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	recovery, recoveredCatalog, err := RecoverWithCatalog(directory)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.TotalSegments != 64 || recovery.TrustedSegments != 64 {
		t.Fatalf(
			"recovery segments = total %d/trusted %d, want 64/64",
			recovery.TotalSegments, recovery.TrustedSegments,
		)
	}
	if recovery.ScannedBytes != 0 || recovery.ScannedSegments != 0 {
		t.Fatalf(
			"catalog recovery scanned %d bytes in %d segments",
			recovery.ScannedBytes, recovery.ScannedSegments,
		)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:     directory,
		AfterEndLSN:   testTransaction(63 * 0x10).EndLSN,
		DurableEndLSN: testTransaction(64 * 0x10).EndLSN,
		Catalog:       recoveredCatalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if transaction.CommitLSN != 64*0x10 {
		t.Fatalf("indexed resume commit = %x, want %x", transaction.CommitLSN, 64*0x10)
	}
	combinedRead := recovery.ScannedBytes + reader.BytesRead()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if recovery.TotalBytes/max(combinedRead, 1) < 50 {
		t.Fatalf(
			"combined recovery and replay work reduction = %dx, want at least 50x",
			recovery.TotalBytes/max(combinedRead, 1),
		)
	}
}

func TestRecoveryBootstrapsMissingAndCorruptCatalog(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		damage func(*testing.T, string)
		reason string
	}{
		{
			name: "missing",
			damage: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.Remove(segmentCatalogPath(directory)); err != nil {
					t.Fatal(err)
				}
			},
			reason: "manifest_missing",
		},
		{
			name: "corrupt",
			damage: func(t *testing.T, directory string) {
				t.Helper()
				path := segmentCatalogPath(directory)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				data[len(data)-1] ^= 0xff
				if err := os.WriteFile(path, data, 0o640); err != nil {
					t.Fatal(err)
				}
			},
			reason: "manifest_invalid",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			writer, _, err := OpenWriter(WriterConfig{
				Directory: directory, RotationBytes: 1,
			})
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
			test.damage(t, directory)
			recovery, err := Recover(directory)
			if err != nil {
				t.Fatal(err)
			}
			if !recovery.ManifestRebuilt || recovery.FallbackReason != test.reason {
				t.Fatalf(
					"fallback = rebuilt %t/reason %q, want true/%q",
					recovery.ManifestRebuilt, recovery.FallbackReason, test.reason,
				)
			}
			if recovery.ScannedSegments != 3 ||
				recovery.ScannedBytes != recovery.TotalBytes {
				t.Fatalf(
					"bootstrap work = %d/%d bytes in %d segments",
					recovery.ScannedBytes, recovery.TotalBytes,
					recovery.ScannedSegments,
				)
			}
			second, err := Recover(directory)
			if err != nil {
				t.Fatal(err)
			}
			if second.ScannedBytes != 0 || second.TrustedSegments != 3 {
				t.Fatalf(
					"second recovery = scanned %d/trusted %d",
					second.ScannedBytes, second.TrustedSegments,
				)
			}
		})
	}
}

func TestRecoveryCountsOnlyBytesReadFromMalformedTail(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "0000000000000010.seg.partial")
	var header [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:4], 21)
	data := append(header[:], make([]byte, 21+(1<<20))...)
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
	recovery, err := Recover(directory)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.ScannedBytes != frameHeaderSize+21 {
		t.Fatalf(
			"malformed-tail scan bytes = %d, want %d actually read",
			recovery.ScannedBytes, frameHeaderSize+21,
		)
	}
	if recovery.TruncatedBytes != int64(len(data)) {
		t.Fatalf(
			"malformed-tail truncation = %d, want %d",
			recovery.TruncatedBytes, len(data),
		)
	}
}

func TestRecoveryAdoptsSegmentRenamedBeforeCatalogPersistence(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := testTransaction(0x10)
	if err := writer.Append(&first); err != nil {
		t.Fatal(err)
	}
	oldCatalog, err := os.ReadFile(segmentCatalogPath(directory))
	if err != nil {
		t.Fatal(err)
	}
	second := testTransaction(0x20)
	if err := writer.Append(&second); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	// This is the filesystem image after segment rename and directory fsync,
	// but before publishing the new catalog generation.
	if err := os.WriteFile(
		segmentCatalogPath(directory), oldCatalog, 0o640,
	); err != nil {
		t.Fatal(err)
	}

	recovery, err := Recover(directory)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.TrustedSegments != 1 || recovery.ScannedSegments != 1 ||
		recovery.DurableLSN != second.EndLSN {
		t.Fatalf(
			"suffix recovery = trusted %d/scanned %d/durable %x",
			recovery.TrustedSegments, recovery.ScannedSegments,
			recovery.DurableLSN,
		)
	}
	next, err := Recover(directory)
	if err != nil {
		t.Fatal(err)
	}
	if next.TrustedSegments != 2 || next.ScannedSegments != 0 {
		t.Fatalf(
			"adopted recovery = trusted %d/scanned %d",
			next.TrustedSegments, next.ScannedSegments,
		)
	}
}

func TestRecoveryFinishesDurablyIntendedPrune(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []LSN{0x10, 0x20, 0x30} {
		transaction := testTransaction(lsn)
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	catalog := writer.SegmentCatalog()
	segments := catalog.snapshot()
	catalog.mu.RLock()
	generation := catalog.generation
	catalog.mu.RUnlock()
	if err := persistPruneWatermark(
		directory, generation+1, segments[1].LastEnd,
	); err != nil {
		t.Fatal(err)
	}

	recovery, err := Recover(directory)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.TrustedSegments != 1 || recovery.DurableLSN != segments[2].LastEnd {
		t.Fatalf(
			"recovery = trusted %d/durable %x",
			recovery.TrustedSegments, recovery.DurableLSN,
		)
	}
	for _, removed := range segments[:2] {
		if _, err := os.Stat(removed.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("durably pruned segment %s still exists: %v", removed.Path, err)
		}
	}
}

func TestCatalogRebuildPreservesPrunedPrefix(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	transactions := make([]Transaction, 4)
	for i := range transactions {
		transactions[i] = testTransaction(LSN((i + 1) * 0x10))
		if err := writer.Append(&transactions[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	catalog := writer.SegmentCatalog()
	if _, _, err := catalog.prune(transactions[3].EndLSN+1, 4); err != nil {
		t.Fatal(err)
	}
	prunedThrough := catalog.prunedEndLSN()
	if err := os.Remove(segmentCatalogPath(directory)); err != nil {
		t.Fatal(err)
	}

	recovery, rebuilt, err := RecoverWithCatalog(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !recovery.ManifestRebuilt ||
		recovery.prunedThrough != prunedThrough ||
		rebuilt.prunedEndLSN() != prunedThrough {
		t.Fatalf(
			"rebuilt prune state = rebuilt %t/recovery %x/catalog %x, want %x",
			recovery.ManifestRebuilt, recovery.prunedThrough,
			rebuilt.prunedEndLSN(), prunedThrough,
		)
	}
	_, err = NewReaderWithConfig(ReaderConfig{
		Directory:     directory,
		AfterEndLSN:   transactions[1].EndLSN,
		DurableEndLSN: recovery.DurableLSN,
		Catalog:       rebuilt,
	})
	if err == nil || !strings.Contains(err.Error(), "precedes pruned CDC prefix") {
		t.Fatalf("reader error = %v, want pruned-prefix rejection", err)
	}
}

func TestCatalogRecoveryFailsClosedForChangedSegmentSet(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		damage func(*testing.T, string, []SegmentRange)
		want   string
	}{
		{
			name: "missing",
			damage: func(t *testing.T, _ string, segments []SegmentRange) {
				t.Helper()
				if err := os.Remove(segments[0].Path); err != nil {
					t.Fatal(err)
				}
			},
			want: "missing from disk",
		},
		{
			name: "resized",
			damage: func(t *testing.T, _ string, segments []SegmentRange) {
				t.Helper()
				file, err := os.OpenFile(segments[0].Path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write([]byte{0}); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
			},
			want: "changed size",
		},
		{
			name: "unexpected interior",
			damage: func(t *testing.T, directory string, segments []SegmentRange) {
				t.Helper()
				data, err := os.ReadFile(segments[0].Path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(
					filepath.Join(directory, "0000000000000018.seg"), data, 0o640,
				); err != nil {
					t.Fatal(err)
				}
			},
			want: "inside cataloged range",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			writer, _, err := OpenWriter(WriterConfig{
				Directory: directory, RotationBytes: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, lsn := range []LSN{0x10, 0x20} {
				transaction := testTransaction(lsn)
				if err := writer.Append(&transaction); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			segments := writer.SegmentCatalog().snapshot()
			test.damage(t, directory, segments)
			_, err = Recover(directory)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Recover error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestReaderFailsOnPermanentlyMissingCatalogedSegment(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction := testTransaction(0x10)
	if err := writer.Append(&transaction); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	segment := writer.SegmentCatalog().snapshot()[0]
	if err := os.Remove(segment.Path); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:     directory,
		DurableEndLSN: transaction.EndLSN,
		Catalog:       writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err == nil ||
		!strings.Contains(err.Error(), "missing from disk") {
		t.Fatalf("reader error = %v, want missing segment", err)
	}
}

func TestReaderSeeksFromAppliedEndLSN(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 64<<10)
	var after LSN
	for i := 1; i <= 160; i++ {
		tuple := Tuple{{Kind: DatumText, Data: []byte(payload)}}
		transaction := Transaction{
			CommitLSN: LSN(i * 0x10),
			EndLSN:    LSN(i*0x10 + 1),
			Relations: []Relation{{
				OID: 1, Namespace: "public", Name: "seek",
				Columns: []Column{{Name: "payload", Type: 25}},
			}},
			Changes: []Change{{
				RelationOID: 1, Kind: ChangeInsert, New: &tuple,
			}},
		}
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
		if i == 120 {
			after = transaction.EndLSN
		}
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:     directory,
		AfterEndLSN:   after,
		DurableEndLSN: writer.DurableEndLSN(),
		Catalog:       writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if transaction.CommitLSN != 121*0x10 {
		t.Fatalf("first commit = %x, want %x", transaction.CommitLSN, 121*0x10)
	}
	if reader.BytesRead() > segmentSeekInterval+int64(len(payload))+frameHeaderSize+256 {
		t.Fatalf("reader consumed %d bytes after indexed seek", reader.BytesRead())
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeEndPositionSeeksCatalogSuffix(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	var last Transaction
	for i := 1; i <= 64; i++ {
		last = testTransaction(LSN(i * 0x10))
		if err := writer.Append(&last); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	resolution, err := NormalizeEndPositionWithCatalog(
		directory,
		writer.SegmentCatalog(),
		last.EndLSN+5,
		last.EndLSN,
	)
	if err != nil {
		t.Fatal(err)
	}
	if resolution.Boundary != last.EndLSN || resolution.Exact {
		t.Fatalf(
			"resolution = boundary %x/exact %t, want %x/false",
			resolution.Boundary, resolution.Exact, last.EndLSN,
		)
	}
}

func TestReaderSeekSurvivesRotationAndPrunedPrefix(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	transactions := make([]Transaction, 64)
	for i := range transactions {
		transactions[i] = testTransaction(LSN((i + 1) * 0x10))
		if err := writer.Append(&transactions[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	catalog := writer.SegmentCatalog()
	if _, _, err := catalog.prune(transactions[59].EndLSN+1, 64); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:     directory,
		AfterEndLSN:   transactions[59].EndLSN,
		DurableEndLSN: transactions[63].EndLSN,
		Catalog:       catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	next, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if next.CommitLSN != transactions[60].CommitLSN {
		t.Fatalf(
			"first retained commit = %x, want %x",
			next.CommitLSN, transactions[60].CommitLSN,
		)
	}
	payload, err := MarshalTransaction(&transactions[60])
	if err != nil {
		t.Fatal(err)
	}
	if reader.BytesRead() != int64(frameHeaderSize+len(payload)) {
		t.Fatalf(
			"reader consumed %d bytes after rotated seek, want one %d-byte frame",
			reader.BytesRead(), frameHeaderSize+len(payload),
		)
	}
}

func TestReaderSeeksPastAppliedEmptyTransaction(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	empty := Transaction{CommitLSN: 0x10, EndLSN: 0x11}
	next := testTransaction(0x20)
	for _, transaction := range []*Transaction{&empty, &next} {
		if err := writer.Append(transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:     directory,
		AfterEndLSN:   empty.EndLSN,
		DurableEndLSN: next.EndLSN,
		Catalog:       writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	transaction, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if transaction.CommitLSN != next.CommitLSN {
		t.Fatalf(
			"first transaction after empty boundary = %x, want %x",
			transaction.CommitLSN, next.CommitLSN,
		)
	}
}

func BenchmarkCatalogRecovery64Segments(b *testing.B) {
	directory := b.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(strings.Repeat("x", 8<<20))
	for i := 1; i <= 64; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		(*transaction.Changes[0].New)[0] = TupleDatum{
			Kind: DatumBinary, Data: payload,
		}
		if err := writer.Append(&transaction); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	var bootstrapElapsed, metadataElapsed time.Duration
	for range b.N {
		b.StopTimer()
		if err := os.Remove(segmentCatalogPath(directory)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		started := time.Now()
		bootstrap, err := Recover(directory)
		bootstrapElapsed += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		started = time.Now()
		recovery, err := Recover(directory)
		metadataElapsed += time.Since(started)
		if err != nil {
			b.Fatal(err)
		}
		if recovery.ScannedBytes != 0 {
			b.Fatalf("scanned %d finalized bytes", recovery.ScannedBytes)
		}
		b.ReportMetric(
			float64(bootstrap.TotalBytes)/float64(max(recovery.ScannedBytes, 1)),
			"work_reduction_x",
		)
	}
	if metadataElapsed > 0 {
		b.ReportMetric(
			float64(bootstrapElapsed)/float64(metadataElapsed),
			"wall_speedup_x",
		)
	}
}

func BenchmarkExternalCatalogRecovery(b *testing.B) {
	directory := os.Getenv("PGMIGRATE_CDC_RECOVERY_DIR")
	if directory == "" {
		b.Skip("set PGMIGRATE_CDC_RECOVERY_DIR to a CDC segment directory")
	}
	b.ResetTimer()
	for range b.N {
		recovery, err := Recover(directory)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(recovery.TotalBytes), "total_bytes")
		b.ReportMetric(float64(recovery.ScannedBytes), "scanned_bytes")
		b.ReportMetric(
			float64(recovery.TotalBytes)/float64(max(recovery.ScannedBytes, 1)),
			"work_reduction_x",
		)
	}
}
