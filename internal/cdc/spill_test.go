package cdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestDecoderSpillStreamsCompatibleSegmentAndRecovery(t *testing.T) {
	t.Parallel()
	spillDirectory := t.TempDir()
	spilled := decodeSpillTestTransaction(t, PGOutputDecoderConfig{
		SpillThreshold: 64,
		SpillDirectory: spillDirectory,
	})
	if !spilled.IsSpilled() || len(spilled.Changes) != 0 {
		t.Fatalf("transaction spill state = spilled %v/resident changes %d", spilled.IsSpilled(), len(spilled.Changes))
	}
	if spilled.ChangeCount() != 3 {
		t.Fatalf("spilled change count = %d, want 3", spilled.ChangeCount())
	}
	spillPath := spilled.Spill.Path()
	if _, err := os.Stat(spillPath); err != nil {
		t.Fatal(err)
	}

	resident := decodeSpillTestTransaction(t, PGOutputDecoderConfig{
		SpillThreshold: 1 << 20,
		SpillDirectory: spillDirectory,
	})
	var spilledPayload, residentPayload bytes.Buffer
	if _, err := WriteTransaction(&spilledPayload, spilled); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteTransaction(&residentPayload, resident); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(spilledPayload.Bytes(), residentPayload.Bytes()) {
		t.Fatal("spilled and resident deterministic payloads differ")
	}

	segmentDirectory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: segmentDirectory})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.AppendFrame(spilled); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(spillPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill still exists after successful append: %v", err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	recovery, err := Recover(segmentDirectory)
	if err != nil {
		t.Fatal(err)
	}
	transactions, err := ReadTransactionsAfter(segmentDirectory, 0, recovery.DurableLSN)
	if err != nil {
		t.Fatal(err)
	}
	if len(transactions) != 1 || !reflect.DeepEqual(transactions[0].Changes, resident.Changes) {
		t.Fatalf("recovered transactions = %#v", transactions)
	}
}

func TestDecoderAbortRemovesActiveSpill(t *testing.T) {
	t.Parallel()
	decoder, err := NewPGOutputDecoderWithConfig(PGOutputDecoderConfig{
		SpillThreshold: 64,
		SpillDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	startSpillTestTransaction(t, decoder)
	if decoder.current == nil || decoder.current.Spill == nil {
		t.Fatal("transaction did not spill")
	}
	path := decoder.current.Spill.Path()
	if _, err := decoder.Decode([]byte{'?'}); err == nil {
		t.Fatal("expected protocol error")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active spill survived decoder abort: %v", err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDecoderCloseRemovesActiveSpill(t *testing.T) {
	t.Parallel()
	decoder, err := NewPGOutputDecoderWithConfig(PGOutputDecoderConfig{
		SpillThreshold: 64,
		SpillDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	startSpillTestTransaction(t, decoder)
	path := decoder.current.Spill.Path()
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active spill survived decoder close: %v", err)
	}
}

func TestFailedSegmentAppendRetainsSpillForOrphanCleanup(t *testing.T) {
	t.Parallel()
	spillDirectory := t.TempDir()
	transaction := decodeSpillTestTransaction(t, PGOutputDecoderConfig{
		SpillThreshold: 64,
		SpillDirectory: spillDirectory,
	})
	path := transaction.Spill.Path()
	transaction.EndLSN = 0
	writer, _, err := OpenWriter(WriterConfig{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.AppendFrame(transaction); err == nil {
		t.Fatal("expected invalid transaction append failure")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("failed append removed spill: %v", err)
	}
	if err := transaction.Spill.closeKeepFile(); err != nil {
		t.Fatal(err)
	}
	if err := CleanupOrphanSpills(spillDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan spill was not cleaned: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOrphanCleanupSkipsLockedActiveSpill(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	active, err := newTransactionSpill(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer active.closeAndRemove()
	orphan := filepath.Join(directory, spillFilePrefix+"orphan.tmp")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CleanupOrphanSpills(directory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(active.Path()); err != nil {
		t.Fatalf("active locked spill was removed: %v", err)
	}
	if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan spill remains: %v", err)
	}
}

func TestWriterRejectsPayloadAboveUint32WithoutReadingSpill(t *testing.T) {
	t.Parallel()
	writer, _, err := OpenWriter(WriterConfig{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	transaction := &Transaction{
		CommitLSN: 1,
		EndLSN:    2,
		Spill: &TransactionSpill{
			changeBytes: maxPayloadSize,
			changeCount: 1,
		},
	}
	if _, err := writer.AppendFrame(transaction); err == nil {
		t.Fatal("expected payload above uint32 limit to fail")
	}
}

func TestApplierSkipPathsRemoveReaderSpills(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name        string
		progress    LSN
		endPosition func(context.Context) (LSN, bool, error)
	}{
		{
			name:     "already applied",
			progress: 20,
		},
		{
			name: "past end boundary",
			endPosition: func(context.Context) (LSN, bool, error) {
				return 5, true, nil
			},
		},
		{
			name: "end position error",
			endPosition: func(context.Context) (LSN, bool, error) {
				return 0, false, errors.New("end position failed")
			},
		},
		{
			name: "canceled end position",
			endPosition: func(ctx context.Context) (LSN, bool, error) {
				return 0, false, ctx.Err()
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			directory := t.TempDir()
			spill, err := newTransactionSpill(directory)
			if err != nil {
				t.Fatal(err)
			}
			spillPath := spill.path
			reader := &Reader{
				durableEndLSN: 20,
				pending: &Transaction{
					CommitLSN: 10,
					EndLSN:    11,
					Spill:     spill,
				},
			}
			ctx := context.Background()
			if testCase.name == "canceled end position" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			applier := &Applier{config: ApplierConfig{
				Durable:     &DurableWatermark{},
				EndPosition: testCase.endPosition,
			}}
			_, _, _ = applier.applyFromReader(ctx, nil, reader, newTargetRelationCache(), testCase.progress)
			if _, err := os.Stat(spillPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("reader spill remains after skip: %v", err)
			}
		})
	}
}

func TestReaderUsesDedicatedSpillDirectoryAndCleansOrphans(t *testing.T) {
	t.Parallel()
	segmentDirectory := t.TempDir()
	spillDirectory := t.TempDir()
	orphan, err := newTransactionSpill(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	orphanPath := orphan.path
	if err := orphan.closeKeepFile(); err != nil {
		t.Fatal(err)
	}
	cleanedSpillDirectories.Delete(filepath.Clean(spillDirectory))
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:      segmentDirectory,
		SpillDirectory: spillDirectory,
		DurableEndLSN:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := os.Stat(orphanPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan reader spill remains: %v", err)
	}

	transaction := testTransaction(1)
	payload, err := MarshalTransaction(&transaction)
	if err != nil {
		t.Fatal(err)
	}
	payloadFile, err := os.CreateTemp(t.TempDir(), "payload-*")
	if err != nil {
		t.Fatal(err)
	}
	defer payloadFile.Close()
	if _, err := payloadFile.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := payloadFile.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	reader.file = payloadFile
	spilled, err := reader.readLargeTransaction(
		uint32(len(payload)),
		crc32.Checksum(payload, castagnoliTable),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer spilled.CleanupSpill()
	if filepath.Dir(spilled.Spill.path) != spillDirectory {
		t.Fatalf("reader spill directory = %s, want %s", filepath.Dir(spilled.Spill.path), spillDirectory)
	}
}

func TestReaderCleansLargeSpillWhenCommitAlreadyApplied(t *testing.T) {
	t.Parallel()
	segmentDirectory := t.TempDir()
	spillDirectory := t.TempDir()
	transaction := testTransaction(1)
	transaction.Changes[0].New = &Tuple{{
		Kind: DatumBinary,
		Data: make([]byte, streamPayloadBytes),
	}}
	writer, _, err := OpenWriter(WriterConfig{Directory: segmentDirectory})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err := writer.Append(&transaction); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:      segmentDirectory,
		SpillDirectory: spillDirectory,
		AfterCommitLSN: transaction.CommitLSN,
		DurableEndLSN:  transaction.EndLSN,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("already-applied large transaction = %v", err)
	}
	entries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("reader spill directory contains %d files after skip", len(entries))
	}
}

func decodeSpillTestTransaction(t testing.TB, config PGOutputDecoderConfig) *Transaction {
	t.Helper()
	decoder, err := NewPGOutputDecoderWithConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	startSpillTestTransaction(t, decoder)
	transaction, err := decoder.Decode(commitMessage(0x100, 0x118, time.Unix(1_700_000_001, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if err := decoder.Close(); err != nil {
		t.Fatal(err)
	}
	return transaction
}

func startSpillTestTransaction(t testing.TB, decoder *PGOutputDecoder) {
	t.Helper()
	messages := [][]byte{
		relationMessage(42, "public", "spill_items", 'd', []Column{
			{Name: "id", Type: 23, Flags: 1},
			{Name: "payload", Type: 25},
		}),
		beginMessage(0x100, time.Unix(1_700_000_000, 0)),
		insertMessage(42, Tuple{
			{Kind: DatumText, Data: []byte("1")},
			{Kind: DatumText, Data: bytes.Repeat([]byte("a"), 128)},
		}),
		insertMessage(42, Tuple{
			{Kind: DatumText, Data: []byte("2")},
			{Kind: DatumNull},
		}),
		updateMessage(42, nil, Tuple{
			{Kind: DatumText, Data: []byte("1")},
			{Kind: DatumText, Data: bytes.Repeat([]byte("b"), 128)},
		}),
	}
	for _, message := range messages {
		if _, err := decoder.Decode(message); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkReaderLargeRecord(b *testing.B) {
	const payloadBytes = 20 << 20
	directory := b.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory})
	if err != nil {
		b.Fatal(err)
	}
	tuple := Tuple{{Kind: DatumText, Data: bytes.Repeat([]byte("x"), payloadBytes)}}
	transaction := Transaction{
		CommitLSN: 1,
		EndLSN:    2,
		Changes: []Change{{
			RelationOID: 1,
			Kind:        ChangeInsert,
			New:         &tuple,
		}},
	}
	if err := writer.Append(&transaction); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(payloadBytes)
	b.ResetTimer()
	for range b.N {
		reader, err := NewReader(directory, 0, 2)
		if err != nil {
			b.Fatal(err)
		}
		transaction, err := reader.Next()
		if err != nil {
			b.Fatal(err)
		}
		_ = transaction.CleanupSpill()
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderManyTransactions(b *testing.B) {
	for _, transactionCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("%d", transactionCount), func(b *testing.B) {
			benchmarkReaderTransactions(b, transactionCount)
		})
	}
}

func benchmarkReaderTransactions(b *testing.B, transactionCount int) {
	directory := b.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory:     directory,
		RotationBytes: 256 << 10,
	})
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= transactionCount; i++ {
		transaction := testTransaction(LSN(i * 0x10))
		if err := writer.Append(&transaction); err != nil {
			b.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	durable := writer.DurableEndLSN()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reader, err := NewReader(directory, 0, durable)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for {
			if _, err := reader.Next(); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				b.Fatal(err)
			}
			count++
		}
		if count != transactionCount {
			b.Fatalf("read %d transactions", count)
		}
		_ = reader.Close()
	}
}
