package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriterSyncAdvancesCompleteTransactionBoundary(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, recovery, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	if recovery.DurableLSN != 0 {
		t.Fatalf("recovered durable LSN = %x, want 0", recovery.DurableLSN)
	}
	tx := testTransaction(0x10)
	tx.EndLSN = 0x1f
	if err := writer.Append(&tx); err != nil {
		t.Fatal(err)
	}
	if got := writer.DurableEndLSN(); got != 0 {
		t.Fatalf("durable LSN advanced before sync: %x", got)
	}
	reader, err := NewReader(dir, 0, writer.DurableEndLSN())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("unsynced transaction error = %v, want io.EOF", err)
	}
	if got, err := writer.Sync(); err != nil {
		t.Fatal(err)
	} else if got != tx.EndLSN {
		t.Fatalf("synced LSN = %x, want EndLSN %x", got, tx.EndLSN)
	}
	if got := writer.DurableEndLSN(); got != tx.EndLSN {
		t.Fatalf("durable EndLSN = %x, want %x", got, tx.EndLSN)
	}
	if err := reader.AdvanceDurableEndLSN(writer.DurableEndLSN()); err != nil {
		t.Fatal(err)
	}
	visible, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if visible.CommitLSN != tx.CommitLSN || visible.EndLSN != tx.EndLSN {
		t.Fatalf("visible transaction LSNs = %x/%x, want %x/%x",
			visible.CommitLSN, visible.EndLSN, tx.CommitLSN, tx.EndLSN)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendFrameReturnsEncodedByteCount(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	tx := testTransaction(0x10)
	frameBytes, err := writer.AppendFrame(&tx)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(frameHeaderSize + len(payload)); frameBytes != want {
		t.Fatalf("frame bytes = %d, want %d", frameBytes, want)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderTreatsStreamPlaceholderAsIncompleteTail(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000001.seg.partial")
	if err := os.WriteFile(path, make([]byte, frameHeaderSize), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(dir, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("placeholder read error = %v, want io.EOF", err)
	}
}

func TestReaderRefreshFollowsPartialTailWithoutRescan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	first := testTransaction(0x10)
	if err := writer.Append(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(dir, 0, writer.DurableEndLSN())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if transaction, err := reader.Next(); err != nil || transaction.EndLSN != first.EndLSN {
		t.Fatalf("first transaction = %#v, %v", transaction, err)
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("tail error = %v", err)
	}
	second := testTransaction(0x20)
	if err := writer.Append(&second); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Refresh(writer.DurableEndLSN()); err != nil {
		t.Fatal(err)
	}
	if transaction, err := reader.Next(); err != nil || transaction.EndLSN != second.EndLSN {
		t.Fatalf("second transaction = %#v, %v", transaction, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderKeepsRotatedDescriptorUntilActualEOF(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	first := testTransaction(0x10)
	if err := writer.Append(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(dir, 0, writer.DurableEndLSN())
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if transaction, err := reader.Next(); err != nil || transaction.EndLSN != first.EndLSN {
		t.Fatalf("first = %#v, %v", transaction, err)
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("initial tail = %v", err)
	}

	second := testTransaction(0x20)
	if err := writer.Append(&second); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Rotate(); err != nil {
		t.Fatal(err)
	}
	third := testTransaction(0x30)
	if err := writer.Append(&third); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Refresh(writer.DurableEndLSN()); err != nil {
		t.Fatal(err)
	}
	var got []LSN
	for {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, transaction.CommitLSN)
	}
	if !reflect.DeepEqual(got, []LSN{second.CommitLSN, third.CommitLSN}) {
		t.Fatalf("transactions after rotation = %x", got)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderRetriesFinalizedStreamingPlaceholder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000010.seg.partial")
	if err := os.WriteFile(path, make([]byte, frameHeaderSize), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(dir, 0, 0x20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("placeholder error = %v", err)
	}
	transaction := testTransaction(0x10)
	payload, err := MarshalTransaction(&transaction)
	if err != nil {
		t.Fatal(err)
	}
	frame := encodedFrame(payload)
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(frame, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	finalPath := strings.TrimSuffix(path, ".partial")
	if err := os.Rename(path, finalPath); err != nil {
		t.Fatal(err)
	}
	if err := reader.Refresh(0x20); err != nil {
		t.Fatal(err)
	}
	got, err := reader.Next()
	if err != nil || got.CommitLSN != transaction.CommitLSN {
		t.Fatalf("finalized placeholder transaction = %#v, %v", got, err)
	}
}

func TestReaderRetriesCompletedResidentPayload(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000010.seg.partial")
	transaction := testTransaction(0x10)
	payload, err := MarshalTransaction(&transaction)
	if err != nil {
		t.Fatal(err)
	}
	frame := encodedFrame(payload)
	cut := frameHeaderSize + len(payload)/2
	if err := os.WriteFile(path, frame[:cut], 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(dir, 0, transaction.EndLSN)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("short payload error = %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(frame[cut:]); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Refresh(transaction.EndLSN); err != nil {
		t.Fatal(err)
	}
	got, err := reader.Next()
	if err != nil || got.CommitLSN != transaction.CommitLSN {
		t.Fatalf("completed resident transaction = %#v, %v", got, err)
	}
}

func TestReaderRetriesCompletedPartialHeader(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "0000000000000010.seg.partial")
	transaction := testTransaction(0x10)
	payload, err := MarshalTransaction(&transaction)
	if err != nil {
		t.Fatal(err)
	}
	frame := encodedFrame(payload)
	if err := os.WriteFile(path, frame[:4], 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReader(dir, 0, transaction.EndLSN)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("short header error = %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(frame[4:]); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := reader.Next()
	if err != nil || got.CommitLSN != transaction.CommitLSN {
		t.Fatalf("completed-header transaction = %#v, %v", got, err)
	}
}

func encodedFrame(payload []byte) []byte {
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Checksum(payload, castagnoliTable))
	copy(frame[frameHeaderSize:], payload)
	return frame
}

func TestNewPartialDirectorySyncPrecedesDurableFileSync(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	var events []string
	writer, _, err := OpenWriter(WriterConfig{
		Directory: dir,
		DirectorySync: func(string) error {
			events = append(events, "directory")
			return nil
		},
		FileSync: func(*os.File) error {
			events = append(events, "file")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction := testTransaction(0x10)
	if err := writer.Append(&transaction); err != nil {
		t.Fatal(err)
	}
	if got := writer.DurableEndLSN(); got != 0 {
		t.Fatalf("durable LSN before sync = %x", got)
	}
	if _, err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"directory", "file"}) {
		t.Fatalf("sync order = %v, want directory then file", events)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewPartialDirectorySyncFailurePreventsAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: dir,
		DirectorySync: func(string) error {
			return errors.New("injected directory sync failure")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction := testTransaction(0x10)
	if err := writer.Append(&transaction); err == nil {
		t.Fatal("expected append to fail before segment use")
	}
	if writer.DurableEndLSN() != 0 || writer.LastEndLSN() != 0 {
		t.Fatal("failed directory sync advanced writer positions")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed new segment left %d directory entries", len(entries))
	}
}

func TestRotationAndReaderAfterLSN(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	pairs := []struct {
		commit LSN
		end    LSN
	}{{0x10, 0x18}, {0x20, 0x2f}, {0x30, 0x45}}
	for _, pair := range pairs {
		tx := testTransaction(pair.commit)
		tx.EndLSN = pair.end
		if err := writer.Append(&tx); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []LSN{0x10, 0x20, 0x30} {
		name := fmt.Sprintf("%016x.seg", uint64(lsn))
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected rotated segment %s: %v", name, err)
		}
	}

	if got := writer.DurableEndLSN(); got != 0x45 {
		t.Fatalf("durable EndLSN after rotation = %x, want 45", got)
	}
	recovery, err := Recover(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.LastCommitLSN != 0x30 || recovery.DurableLSN != 0x45 {
		t.Fatalf("recovered LSNs = commit %x/end %x, want 30/45",
			recovery.LastCommitLSN, recovery.DurableLSN)
	}

	reader, err := NewReader(dir, 0x10, recovery.DurableLSN)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, want := range pairs[1:] {
		tx, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		if tx.CommitLSN != want.commit || tx.EndLSN != want.end {
			t.Fatalf("transaction LSNs = %x/%x, want %x/%x",
				tx.CommitLSN, tx.EndLSN, want.commit, want.end)
		}
	}
	if _, err := reader.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("reader end error = %v, want io.EOF", err)
	}
}

func TestSegmentCatalogTracksRotationAndRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		commit LSN
		end    LSN
	}{{0x10, 0x18}, {0x20, 0x2f}, {0x30, 0x45}} {
		tx := testTransaction(pair.commit)
		tx.EndLSN = pair.end
		if err := writer.Append(&tx); err != nil {
			t.Fatal(err)
		}
	}
	ranges := writer.SegmentCatalog().snapshot()
	if len(ranges) != 3 {
		t.Fatalf("catalog ranges=%d, want 3", len(ranges))
	}
	for i, want := range []struct {
		start LSN
		end   LSN
	}{{0x10, 0x18}, {0x20, 0x2f}, {0x30, 0x45}} {
		if ranges[i].StartCommit != want.start || ranges[i].LastEnd != want.end {
			t.Fatalf("catalog range %d=%x/%x, want %x/%x",
				i, ranges[i].StartCommit, ranges[i].LastEnd, want.start, want.end)
		}
		info, err := os.Stat(ranges[i].Path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != ranges[i].ValidatedSize {
			t.Fatalf("catalog size %d=%d, want %d", i, ranges[i].ValidatedSize, info.Size())
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, recovery, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if recovery.LastCommitLSN != 0x30 || recovery.DurableLSN != 0x45 {
		t.Fatalf("recovery LSNs=%x/%x, want 30/45", recovery.LastCommitLSN, recovery.DurableLSN)
	}
	recovered := reopened.SegmentCatalog().snapshot()
	if !reflect.DeepEqual(recovered, ranges) {
		t.Fatalf("recovered catalog=%#v, want %#v", recovered, ranges)
	}
}

func TestSegmentCatalogFinalizesRecoveredPartialRange(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: dir, RotationBytes: int64(^uint64(0) >> 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	first := testTransaction(0x10)
	first.EndLSN = 0x18
	if err := writer.Append(&first); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, recovery, err := OpenWriter(WriterConfig{
		Directory: dir, RotationBytes: int64(^uint64(0) >> 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if recovery.PartialPath == "" || len(reopened.SegmentCatalog().snapshot()) != 0 {
		t.Fatalf("recovered partial=%q catalog=%#v", recovery.PartialPath, reopened.SegmentCatalog().snapshot())
	}
	second := testTransaction(0x20)
	second.EndLSN = 0x2f
	if err := reopened.Append(&second); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Finalize(); err != nil {
		t.Fatal(err)
	}
	ranges := reopened.SegmentCatalog().snapshot()
	if len(ranges) != 1 || ranges[0].StartCommit != 0x10 ||
		ranges[0].LastCommit != 0x20 || ranges[0].LastEnd != 0x2f {
		t.Fatalf("finalized recovered partial catalog=%#v", ranges)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryTruncatesTornTailAndContinues(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	tx := testTransaction(0x10)
	tx.EndLSN = 0x1f
	if err := writer.Append(&tx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	partial := onlySegment(t, dir, ".seg.partial")
	info, err := os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	validSize := info.Size()
	file, err := os.OpenFile(partial, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovery, err := Recover(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.LastCommitLSN != 0x10 || recovery.DurableLSN != 0x1f || recovery.TruncatedBytes != 4 {
		t.Fatalf("recovery = %#v, want commit/end 10/1f and four truncated bytes", recovery)
	}
	info, err = os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != validSize {
		t.Fatalf("repaired size = %d, want %d", info.Size(), validSize)
	}

	writer, recovery, err = OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	if recovery.DurableLSN != 0x1f {
		t.Fatalf("durable LSN = %x, want 1f", recovery.DurableLSN)
	}
	next := testTransaction(0x20)
	next.EndLSN = 0x35
	if err := writer.Append(&next); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	transactions, err := ReadTransactionsAfter(dir, 0, writer.DurableEndLSN())
	if err != nil {
		t.Fatal(err)
	}
	if got := commitLSNs(transactions); !reflect.DeepEqual(got, []LSN{0x10, 0x20}) {
		t.Fatalf("commit LSNs = %x", got)
	}
}

func TestRecoveryTruncatesCorruptTailRecord(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []LSN{0x10, 0x20} {
		tx := testTransaction(lsn)
		if err := writer.Append(&tx); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	partial := onlySegment(t, dir, ".seg.partial")
	data, err := os.ReadFile(partial)
	if err != nil {
		t.Fatal(err)
	}
	firstSize := frameHeaderSize + int(binary.LittleEndian.Uint32(data[:4]))
	data[firstSize+frameHeaderSize] ^= 0xff
	if err := os.WriteFile(partial, data, 0o640); err != nil {
		t.Fatal(err)
	}

	recovery, err := Recover(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.LastCommitLSN != 0x10 || recovery.DurableLSN != 0x11 {
		t.Fatalf("recovered LSNs = commit %x/end %x, want 10/11",
			recovery.LastCommitLSN, recovery.DurableLSN)
	}
	info, err := os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(firstSize) {
		t.Fatalf("repaired size = %d, want %d", info.Size(), firstSize)
	}
}

func TestRecoveryTracksCommitOrderingSeparatelyFromEndLSN(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	first := testTransaction(0x20)
	first.EndLSN = 0x30
	if err := writer.Append(&first); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	outOfOrder := testTransaction(0x10)
	outOfOrder.EndLSN = 0x40
	payload, err := MarshalTransaction(&outOfOrder)
	if err != nil {
		t.Fatal(err)
	}
	var header [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], crc32.Checksum(payload, castagnoliTable))
	partial := onlySegment(t, dir, ".seg.partial")
	file, err := os.OpenFile(partial, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFull(file, header[:]); err != nil {
		t.Fatal(err)
	}
	if err := writeFull(file, payload); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovery, err := Recover(dir)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.LastCommitLSN != first.CommitLSN || recovery.DurableLSN != first.EndLSN {
		t.Fatalf("recovered LSNs = commit %x/end %x, want %x/%x",
			recovery.LastCommitLSN, recovery.DurableLSN, first.CommitLSN, first.EndLSN)
	}
	if recovery.TruncatedBytes != int64(frameHeaderSize+len(payload)) {
		t.Fatalf("truncated bytes = %d, want %d", recovery.TruncatedBytes, frameHeaderSize+len(payload))
	}
}

func TestRecoveryRejectsCorruptFinalizedSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	tx := testTransaction(0x10)
	if err := writer.Append(&tx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	segment := onlySegment(t, dir, ".seg")
	file, err := os.OpenFile(segment, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, frameHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Recover(dir); err == nil {
		t.Fatal("expected finalized segment corruption to fail")
	}
}

func TestParallelRecoveryMatchesSerialAndReportsMonotonicProgress(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for lsn := LSN(0x10); lsn <= 0x80; lsn += 0x10 {
		transaction := testTransaction(lsn)
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	serial, serialRecovery, err := OpenWriter(WriterConfig{
		Directory: dir, RotationBytes: 1, RecoveryWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantCatalog := serial.SegmentCatalog().snapshot()
	if err := serial.Close(); err != nil {
		t.Fatal(err)
	}

	var (
		callbackActive atomic.Int32
		callbackRaced  atomic.Bool
		progressMu     sync.Mutex
		progress       []RecoveryProgress
	)
	parallel, parallelRecovery, err := OpenWriter(WriterConfig{
		Directory:       dir,
		RotationBytes:   1,
		RecoveryWorkers: MaxRecoveryWorkers,
		RecoveryProgress: func(current RecoveryProgress) {
			if callbackActive.Add(1) != 1 {
				callbackRaced.Store(true)
			}
			// Make overlapping worker completions contend for the callback. The
			// callback contract still requires them to be serialized.
			time.Sleep(2 * time.Millisecond)
			progressMu.Lock()
			progress = append(progress, current)
			progressMu.Unlock()
			callbackActive.Add(-1)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer parallel.Close()
	if callbackRaced.Load() {
		t.Fatal("recovery progress callback ran concurrently")
	}
	if !reflect.DeepEqual(parallelRecovery, serialRecovery) {
		t.Fatalf("parallel recovery = %#v, serial = %#v", parallelRecovery, serialRecovery)
	}
	if got := parallel.SegmentCatalog().snapshot(); !reflect.DeepEqual(got, wantCatalog) {
		t.Fatalf("parallel catalog = %#v, serial = %#v", got, wantCatalog)
	}

	progressMu.Lock()
	snapshots := append([]RecoveryProgress(nil), progress...)
	progressMu.Unlock()
	if len(snapshots) < len(wantCatalog)+2 {
		t.Fatalf("progress snapshots = %d, want initial, per-file, and final reports", len(snapshots))
	}
	for index, current := range snapshots {
		if current.FilesTotal != len(wantCatalog) {
			t.Fatalf("progress[%d] files total = %d, want %d", index, current.FilesTotal, len(wantCatalog))
		}
		if current.BytesScanned > current.BytesTotal {
			t.Fatalf("progress[%d] scanned %d > total %d", index, current.BytesScanned, current.BytesTotal)
		}
		if index == 0 {
			continue
		}
		previous := snapshots[index-1]
		if current.FilesChecked < previous.FilesChecked ||
			current.BytesScanned < previous.BytesScanned || current.Elapsed < previous.Elapsed {
			t.Fatalf("progress regressed from %#v to %#v", previous, current)
		}
	}
	last := snapshots[len(snapshots)-1]
	if last.FilesChecked != last.FilesTotal || last.BytesScanned != last.BytesTotal {
		t.Fatalf("final progress = %#v, want all files and bytes", last)
	}
}

func TestParallelRecoveryRejectsAnyFinalizedCorruptionBeforeRepairingPartial(t *testing.T) {
	for _, corruptIndex := range []int{0, 1, 2} {
		t.Run(fmt.Sprintf("segment %d", corruptIndex), func(t *testing.T) {
			dir, finalized, partial := finalizedWithTornPartial(t)
			before, err := os.ReadFile(partial)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(finalized[corruptIndex].path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte{0xff}, frameHeaderSize); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}

			var reports []RecoveryProgress
			_, _, err = OpenWriter(WriterConfig{
				Directory: dir, RecoveryWorkers: MaxRecoveryWorkers,
				RecoveryProgress: func(progress RecoveryProgress) {
					reports = append(reports, progress)
				},
			})
			if err == nil || !strings.Contains(err.Error(), filepath.Base(finalized[corruptIndex].path)) {
				t.Fatalf("parallel recovery error = %v, want corrupted segment name", err)
			}
			after, readErr := os.ReadFile(partial)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatal("parallel finalized-segment failure repaired the partial tail")
			}
			last := reports[len(reports)-1]
			if last.FilesChecked >= last.FilesTotal {
				t.Fatalf("failed validation reported every file checked: %#v", last)
			}
		})
	}
}

func TestParallelRecoveryEnforcesCrossSegmentOrdering(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  Transaction
		second Transaction
		want   string
	}{
		{
			name:   "commit LSN",
			first:  transactionWithLSNs(0x20, 0x30),
			second: transactionWithLSNs(0x10, 0x40),
			want:   "non-monotonic commit LSN",
		},
		{
			name:   "end LSN",
			first:  transactionWithLSNs(0x10, 0x50),
			second: transactionWithLSNs(0x20, 0x30),
			want:   "non-monotonic end LSN",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFinalizedTransaction(t, dir, 0x10, test.first)
			writeFinalizedTransaction(t, dir, 0x20, test.second)
			_, _, err := OpenWriter(WriterConfig{
				Directory: dir, RecoveryWorkers: MaxRecoveryWorkers,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("parallel recovery error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestParallelRecoveryRepairsTornPartialAfterFinalizedSegments(t *testing.T) {
	t.Parallel()
	dir, _, partial := finalizedWithTornPartial(t)
	info, err := os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	tornSize := info.Size()

	var reports []RecoveryProgress
	writer, recovery, err := OpenWriter(WriterConfig{
		Directory: dir, RecoveryWorkers: MaxRecoveryWorkers,
		RecoveryProgress: func(progress RecoveryProgress) {
			reports = append(reports, progress)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if recovery.LastCommitLSN != 0x40 || recovery.DurableLSN != 0x41 || recovery.TruncatedBytes != 4 {
		t.Fatalf("parallel recovery = %#v, want commit/end 40/41 and four truncated bytes", recovery)
	}
	if last := reports[len(reports)-1]; last.BytesTruncated != 4 || last.FilesChecked != last.FilesTotal {
		t.Fatalf("repaired-tail progress = %#v", last)
	}
	info, err = os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != tornSize-4 {
		t.Fatalf("repaired partial size = %d, want %d", info.Size(), tornSize-4)
	}
	next := testTransaction(0x50)
	if err := writer.Append(&next); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	transactions, err := ReadTransactionsAfter(dir, 0, writer.DurableEndLSN())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := commitLSNs(transactions), []LSN{0x10, 0x20, 0x30, 0x40, 0x50}; !reflect.DeepEqual(got, want) {
		t.Fatalf("commit LSNs after repair = %x, want %x", got, want)
	}
}

func TestOpenWriterValidatesRecoveryWorkerLimit(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{-1, MaxRecoveryWorkers + 1} {
		if _, _, err := OpenWriter(WriterConfig{
			Directory: t.TempDir(), RecoveryWorkers: workers,
		}); err == nil || !strings.Contains(err.Error(), "0 uses the default") {
			t.Errorf("OpenWriter workers=%d error = %v", workers, err)
		}
	}
}

func TestPruneKeepsOneSafetySegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []LSN{0x10, 0x20, 0x30} {
		tx := testTransaction(lsn)
		if err := writer.Append(&tx); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	removed, err := Prune(dir, 0x40)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 {
		t.Fatalf("removed %d segments, want 2", len(removed))
	}
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].start != 0x30 {
		t.Fatalf("remaining segments = %#v, want safety segment starting at 30", segments)
	}
}

func TestPruneStopsBeforeUnappliedCorruptSuffix(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []LSN{0x10, 0x20, 0x30} {
		tx := testTransaction(lsn)
		if err := writer.Append(&tx); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(segments[1].path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, frameHeaderSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	removed, err := Prune(dir, 0x10)
	if err != nil {
		t.Fatalf("prune read unapplied corrupt suffix: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("pruned %d unapplied segments", len(removed))
	}
}

func TestFinalizeRenamesPartialSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	tx := testTransaction(0xabc)
	if err := writer.Append(&tx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "0000000000000abc.seg")); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkSegmentAppend(b *testing.B) {
	dir := b.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory:     dir,
		RotationBytes: int64(^uint64(0) >> 1),
	})
	if err != nil {
		b.Fatal(err)
	}
	tx := testTransaction(1)
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		b.Fatal(err)
	}
	if err := writer.Append(&tx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload) + frameHeaderSize))
	b.ResetTimer()
	for i := range b.N {
		tx.CommitLSN = LSN(i + 2)
		tx.EndLSN = tx.CommitLSN + 1
		if err := writer.Append(&tx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
}

func finalizedWithTornPartial(t *testing.T) (string, []segmentFile, string) {
	t.Helper()
	dir := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: dir, RotationBytes: 1})
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
	writer, _, err = OpenWriter(WriterConfig{
		Directory: dir, RotationBytes: int64(^uint64(0) >> 1), RecoveryWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	tail := testTransaction(0x40)
	if err := writer.Append(&tail); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	partial := onlySegment(t, dir, ".seg.partial")
	file, err := os.OpenFile(partial, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3, 4}); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	finalized := segments[:len(segments)-1]
	if len(finalized) != 3 || !segments[len(segments)-1].partial {
		t.Fatalf("fixture segments = %#v", segments)
	}
	return dir, finalized, partial
}

func transactionWithLSNs(commit, end LSN) Transaction {
	transaction := testTransaction(commit)
	transaction.EndLSN = end
	return transaction
}

func writeFinalizedTransaction(t *testing.T, dir string, start LSN, transaction Transaction) {
	t.Helper()
	payload, err := MarshalTransaction(&transaction)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%016x.seg", uint64(start)))
	if err := os.WriteFile(path, encodedFrame(payload), 0o600); err != nil {
		t.Fatal(err)
	}
}

func onlySegment(t *testing.T, dir, suffix string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var matches []string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == suffix || len(entry.Name()) >= len(suffix) && entry.Name()[len(entry.Name())-len(suffix):] == suffix {
			matches = append(matches, filepath.Join(dir, entry.Name()))
		}
	}
	if len(matches) != 1 {
		t.Fatalf("found %d %s files, want 1", len(matches), suffix)
	}
	return matches[0]
}

func commitLSNs(transactions []Transaction) []LSN {
	result := make([]LSN, len(transactions))
	for i := range transactions {
		result[i] = transactions[i].CommitLSN
	}
	return result
}
