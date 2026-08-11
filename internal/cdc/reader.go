package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Reader reads complete transactions in commit order from a recovered
// snapshot of the segment directory. Small records reuse an in-memory payload;
// large records stream their changes into the dedicated spill directory.
type Reader struct {
	directory      string
	spillDirectory string
	segments       []segmentFile
	index          int
	file           *os.File
	after          LSN
	durableEndLSN  LSN
	previousCommit LSN
	previousEnd    LSN
	pending        *Transaction
	payload        []byte
	header         [frameHeaderSize]byte
	closed         bool
}

// NewReader creates a reader positioned after afterCommitLSN. Transactions
// whose EndLSN is above durableEndLSN are held back until the gate advances.
func NewReader(directory string, afterCommitLSN, durableEndLSN LSN) (*Reader, error) {
	return NewReaderWithConfig(ReaderConfig{
		Directory:      directory,
		AfterCommitLSN: afterCommitLSN,
		DurableEndLSN:  durableEndLSN,
	})
}

type ReaderConfig struct {
	Directory      string
	SpillDirectory string
	AfterCommitLSN LSN
	DurableEndLSN  LSN
}

func NewReaderWithConfig(config ReaderConfig) (*Reader, error) {
	if config.SpillDirectory == "" {
		config.SpillDirectory = filepath.Join(config.Directory, "reader-spill")
	}
	if err := mkdirAllDurable(config.SpillDirectory, 0o750); err != nil {
		return nil, err
	}
	if err := cleanupOrphanSpillsOnce(config.SpillDirectory); err != nil {
		return nil, err
	}
	segments, err := listSegments(config.Directory)
	if err != nil {
		return nil, err
	}
	return &Reader{
		directory:      config.Directory,
		spillDirectory: config.SpillDirectory,
		segments:       segments,
		after:          config.AfterCommitLSN,
		durableEndLSN:  config.DurableEndLSN,
	}, nil
}

// Next returns the next complete transaction whose CommitLSN is greater than
// the reader's starting LSN and whose EndLSN is durable. It returns io.EOF
// when no durable transaction is currently available.
func (r *Reader) Next() (Transaction, error) {
	if r.closed {
		return Transaction{}, errors.New("cdc: read from closed segment reader")
	}
	if r.pending != nil {
		if r.pending.EndLSN > r.durableEndLSN {
			return Transaction{}, io.EOF
		}
		tx := *r.pending
		r.pending = nil
		return tx, nil
	}
	for {
		if r.file == nil {
			if r.index >= len(r.segments) {
				return Transaction{}, io.EOF
			}
			var err error
			r.file, err = os.Open(r.segments[r.index].path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					missingStart := r.segments[r.index].start
					updated, refreshErr := listSegments(r.directory)
					if refreshErr != nil {
						return Transaction{}, refreshErr
					}
					r.segments = updated
					r.index = len(updated)
					for i := range updated {
						if updated[i].start >= missingStart {
							r.index = i
							break
						}
					}
					continue
				}
				return Transaction{}, fmt.Errorf("cdc: open segment %s: %w", filepath.Base(r.segments[r.index].path), err)
			}
		}
		tx, err := r.readTransaction()
		if err == io.EOF {
			if r.segments[r.index].partial {
				return Transaction{}, io.EOF
			}
			if closeErr := r.file.Close(); closeErr != nil {
				return Transaction{}, fmt.Errorf("cdc: close segment reader: %w", closeErr)
			}
			r.file = nil
			r.index++
			continue
		}
		if err != nil {
			return Transaction{}, err
		}
		if tx.CommitLSN <= r.after {
			if err := tx.CleanupSpill(); err != nil {
				return Transaction{}, fmt.Errorf("cdc: cleanup skipped reader spill: %w", err)
			}
			continue
		}
		if tx.EndLSN > r.durableEndLSN {
			r.pending = &tx
			return Transaction{}, io.EOF
		}
		return tx, nil
	}
}

func (r *Reader) readTransaction() (Transaction, error) {
	segment := r.segments[r.index]
	frameStart, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return Transaction{}, fmt.Errorf("cdc: locate frame start in %s: %w", filepath.Base(segment.path), err)
	}
	_, err = io.ReadFull(r.file, r.header[:])
	if err == io.EOF {
		return Transaction{}, r.rewindIncompleteFrame(frameStart, segment.path)
	}
	if err != nil {
		if segment.partial && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
			return Transaction{}, r.rewindIncompleteFrame(frameStart, segment.path)
		}
		return Transaction{}, fmt.Errorf("cdc: read frame header from %s: %w", filepath.Base(segment.path), err)
	}
	length := binary.LittleEndian.Uint32(r.header[0:4])
	// Streamed appends reserve an all-zero header and replace it only after
	// the payload is complete. A concurrent reader of the partial tail must
	// treat that placeholder as an incomplete frame, not corrupt data.
	if segment.partial && length == 0 {
		return Transaction{}, r.rewindIncompleteFrame(frameStart, segment.path)
	}
	if uint64(length) > maxPayloadSize {
		return Transaction{}, fmt.Errorf("cdc: frame length %d exceeds limit in %s", length, filepath.Base(segment.path))
	}
	expected := binary.LittleEndian.Uint32(r.header[4:8])
	if uint64(length) > streamPayloadBytes {
		tx, err := r.readLargeTransaction(length, expected)
		if err != nil {
			if segment.partial && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
				return Transaction{}, r.rewindIncompleteFrame(frameStart, segment.path)
			}
			return Transaction{}, fmt.Errorf("cdc: stream transaction from %s: %w", filepath.Base(segment.path), err)
		}
		if err := r.validateOrder(&tx); err != nil {
			return Transaction{}, errors.Join(err, tx.CleanupSpill())
		}
		return tx, nil
	}
	if cap(r.payload) < int(length) {
		r.payload = make([]byte, int(length))
	} else {
		r.payload = r.payload[:int(length)]
	}
	if _, err := io.ReadFull(r.file, r.payload); err != nil {
		if segment.partial && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
			return Transaction{}, r.rewindIncompleteFrame(frameStart, segment.path)
		}
		return Transaction{}, fmt.Errorf("cdc: read frame payload from %s: %w", filepath.Base(segment.path), err)
	}
	if actual := crc32.Checksum(r.payload, castagnoliTable); actual != expected {
		return Transaction{}, fmt.Errorf("cdc: checksum mismatch in %s", filepath.Base(segment.path))
	}
	tx, err := UnmarshalTransaction(r.payload)
	if err != nil {
		return Transaction{}, fmt.Errorf("cdc: decode transaction in %s: %w", filepath.Base(segment.path), err)
	}
	if err := r.validateOrder(&tx); err != nil {
		return Transaction{}, err
	}
	return tx, nil
}

func (r *Reader) rewindIncompleteFrame(frameStart int64, path string) error {
	if _, err := r.file.Seek(frameStart, io.SeekStart); err != nil {
		return fmt.Errorf("cdc: rewind incomplete frame in %s: %w", filepath.Base(path), err)
	}
	return io.EOF
}

func (r *Reader) validateOrder(tx *Transaction) error {
	if tx.CommitLSN <= r.previousCommit || tx.EndLSN < tx.CommitLSN {
		return fmt.Errorf("cdc: non-monotonic commit LSN %x after %x", tx.CommitLSN, r.previousCommit)
	}
	if tx.EndLSN <= r.previousEnd {
		return fmt.Errorf("cdc: non-monotonic end LSN %x after %x", tx.EndLSN, r.previousEnd)
	}
	r.previousCommit = tx.CommitLSN
	r.previousEnd = tx.EndLSN
	return nil
}

func (r *Reader) readLargeTransaction(length, expected uint32) (transaction Transaction, err error) {
	limited := &io.LimitedReader{R: r.file, N: int64(length)}
	checksum := crc32.New(castagnoliTable)
	hashed := io.TeeReader(limited, checksum)
	bounded := &boundedEncodedReader{reader: hashed, remaining: int64(length)}
	prefix, err := decodeTransactionPrefix(bounded)
	if err != nil {
		return Transaction{}, err
	}
	spill, err := newTransactionSpill(r.spillDirectory)
	if err != nil {
		return Transaction{}, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			err = errors.Join(err, spill.closeAndRemove())
		}
	}()
	changeReader := &teeEncodedReader{reader: bounded, writer: spill.file}
	before := bounded.Remaining()
	for i := uint32(0); i < prefix.changes; i++ {
		if _, err := decodeChangeReader(changeReader, prefix.version); err != nil {
			return Transaction{}, fmt.Errorf("change %d: %w", i, err)
		}
	}
	if bounded.Remaining() != 0 {
		var trailing [1]byte
		if _, err := io.ReadFull(bounded, trailing[:]); err != nil {
			return Transaction{}, err
		}
		return Transaction{}, fmt.Errorf("transaction has %d trailing bytes", bounded.Remaining()+1)
	}
	if checksum.Sum32() != expected {
		return Transaction{}, fmt.Errorf("checksum mismatch: got %08x, want %08x", checksum.Sum32(), expected)
	}
	spill.changeCount = prefix.changes
	spill.changeBytes = uint64(before)
	spill.version = prefix.version
	prefix.transaction.Spill = spill
	cleanup = false
	return prefix.transaction, nil
}

// AdvanceDurableEndLSN raises the reader's durability gate. The watermark
// cannot move backward.
func (r *Reader) AdvanceDurableEndLSN(durableEndLSN LSN) error {
	if durableEndLSN < r.durableEndLSN {
		return fmt.Errorf("cdc: durable EndLSN cannot move backward from %x to %x", r.durableEndLSN, durableEndLSN)
	}
	r.durableEndLSN = durableEndLSN
	return nil
}

// Refresh follows newly appended bytes, rotations, and newly created segments
// without rewinding the current cursor.
func (r *Reader) Refresh(durableEndLSN LSN) error {
	if err := r.AdvanceDurableEndLSN(durableEndLSN); err != nil {
		return err
	}
	updated, err := listSegments(r.directory)
	if err != nil {
		return err
	}
	if r.file != nil && r.index < len(r.segments) {
		current := r.segments[r.index]
		for i := range updated {
			if updated[i].start != current.start {
				continue
			}
			// A rename does not mean the open descriptor is exhausted. Keep
			// reading it from the current offset and only advance after EOF.
			r.segments = updated
			r.index = i
			return nil
		}
		// A pruner may unlink a consumed open segment. Keep the old snapshot
		// until EOF; the open descriptor remains valid and the next refresh
		// will map the following retained segment.
		return nil
	}
	if r.file == nil && r.index >= len(r.segments) {
		next := 0
		if len(r.segments) != 0 {
			last := r.segments[len(r.segments)-1]
			next = len(updated)
			for i := range updated {
				if updated[i].start > last.start {
					next = i
					break
				}
				if updated[i].start == last.start && last.partial && updated[i].partial {
					next = i
					break
				}
			}
		}
		r.segments = updated
		r.index = next
		return nil
	}
	r.segments = updated
	return nil
}

// Close closes the current segment file.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	var cleanupErr error
	if r.pending != nil {
		cleanupErr = r.pending.CleanupSpill()
		r.pending = nil
	}
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("cdc: close segment reader: %w", err))
		}
	}
	return cleanupErr
}

// ReadTransactionsAfter returns all durable complete transactions after a
// commit LSN.
func ReadTransactionsAfter(directory string, afterCommitLSN, durableEndLSN LSN) ([]Transaction, error) {
	reader, err := NewReader(directory, afterCommitLSN, durableEndLSN)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var transactions []Transaction
	for {
		tx, err := reader.Next()
		if err == io.EOF {
			return transactions, nil
		}
		if err != nil {
			return nil, err
		}
		if err := tx.materializeChanges(); err != nil {
			return nil, errors.Join(err, tx.CleanupSpill())
		}
		transactions = append(transactions, tx)
	}
}

// Prune removes finalized segments whose end LSN is below appliedLSN while
// retaining the newest eligible segment as a safety segment.
func Prune(directory string, appliedLSN LSN) ([]string, error) {
	segments, err := listSegments(directory)
	if err != nil {
		return nil, err
	}

	var candidates []string
	var previousCommit LSN
	var previousEnd LSN
	for _, segment := range segments {
		if segment.partial {
			break
		}
		scan, err := scanSegment(segment.path, false, previousCommit, previousEnd, nil)
		if err != nil {
			return nil, err
		}
		previousCommit = scan.lastCommitLSN
		previousEnd = scan.lastEndLSN
		if scan.lastEndLSN < appliedLSN {
			candidates = append(candidates, segment.path)
		}
	}
	if len(candidates) <= 1 {
		return nil, nil
	}

	removed := make([]string, 0, len(candidates)-1)
	for _, candidate := range candidates[:len(candidates)-1] {
		if err := os.Remove(candidate); err != nil {
			return removed, fmt.Errorf("cdc: prune segment %s: %w", filepath.Base(candidate), err)
		}
		removed = append(removed, candidate)
	}
	if err := syncDirectory(directory); err != nil {
		return removed, err
	}
	return removed, nil
}
