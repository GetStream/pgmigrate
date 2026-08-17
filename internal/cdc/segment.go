package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	frameHeaderSize      = 8
	DefaultRotationBytes = int64(1 << 30)
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// WriterConfig configures an append-only segment writer.
type WriterConfig struct {
	Directory        string
	RotationBytes    int64
	FileSync         func(*os.File) error
	DirectorySync    func(string) error
	RecoveryProgress func(Recovery)
}

// Writer appends complete transactions to one .seg.partial tail.
type Writer struct {
	mu sync.Mutex

	dir           string
	rotationBytes int64
	catalog       *SegmentCatalog
	file          *os.File
	partialPath   string
	size          int64
	lastCommitLSN LSN
	lastEndLSN    LSN
	pendingEndLSN LSN
	durableEndLSN LSN
	payload       []byte
	header        [frameHeaderSize]byte
	seekPoints    []SegmentSeekPoint
	lastSeek      int64
	fileSync      func(*os.File) error
	directorySync func(string) error
	closed        bool
}

// SegmentRange is the immutable LSN range of one finalized segment. It is
// populated only after the segment has been validated by recovery or durably
// finalized by Writer.
type SegmentRange struct {
	Path          string
	StartCommit   LSN
	LastCommit    LSN
	LastEnd       LSN
	ValidatedSize int64
	SeekPoints    []SegmentSeekPoint
}

// SegmentCatalog caches validated finalized-segment bounds for pruning. Disk is
// authoritative: OpenWriter rebuilds the catalog through recovery after every
// restart, and the mutable partial tail is never included.
type SegmentCatalog struct {
	mu            sync.RWMutex
	directory     string
	generation    uint64
	prunedThrough LSN
	finalized     []SegmentRange
	tail          *SegmentRange
}

func newSegmentCatalog(directory string, finalized []SegmentRange) *SegmentCatalog {
	return newSegmentCatalogState(directory, finalized, 0, 0, nil)
}

func newSegmentCatalogState(
	directory string,
	finalized []SegmentRange,
	generation uint64,
	prunedThrough LSN,
	tail *SegmentRange,
) *SegmentCatalog {
	var clonedTail *SegmentRange
	if tail != nil {
		value := cloneSegmentRanges([]SegmentRange{*tail})[0]
		clonedTail = &value
	}
	return &SegmentCatalog{
		directory:     directory,
		generation:    generation,
		prunedThrough: prunedThrough,
		finalized:     cloneSegmentRanges(finalized),
		tail:          clonedTail,
	}
}

func (c *SegmentCatalog) snapshot() []SegmentRange {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneSegmentRanges(c.finalized)
}

func (c *SegmentCatalog) addFinalized(segment SegmentRange) error {
	if c == nil {
		return errors.New("cdc: finalized segment catalog is missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addFinalizedLocked(segment)
}

func (c *SegmentCatalog) addFinalizedLocked(segment SegmentRange) error {
	if len(c.finalized) != 0 {
		previous := c.finalized[len(c.finalized)-1]
		if segment.StartCommit <= previous.StartCommit ||
			segment.LastCommit <= previous.LastCommit ||
			segment.LastEnd <= previous.LastEnd {
			return fmt.Errorf(
				"cdc: finalized segment %s range %x/%x does not follow %s range %x/%x",
				filepath.Base(segment.Path), segment.LastCommit, segment.LastEnd,
				filepath.Base(previous.Path), previous.LastCommit, previous.LastEnd,
			)
		}
	} else if segment.LastEnd <= c.prunedThrough {
		return fmt.Errorf(
			"cdc: finalized segment %s end %x does not follow pruned prefix %x",
			filepath.Base(segment.Path), segment.LastEnd, c.prunedThrough,
		)
	}
	proposed := append(cloneSegmentRanges(c.finalized), segment)
	if err := persistDiskSegmentCatalog(
		c.directory, c.generation+1, c.prunedThrough, proposed,
	); err != nil {
		return err
	}
	c.generation++
	c.finalized = proposed
	c.tail = nil
	return nil
}

func (c *SegmentCatalog) updateTail(segment *SegmentRange) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if segment == nil {
		c.tail = nil
		return
	}
	cloned := cloneSegmentRanges([]SegmentRange{*segment})[0]
	c.tail = &cloned
}

func (c *SegmentCatalog) readerSnapshot() ([]SegmentRange, LSN) {
	if c == nil {
		return nil, 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := cloneSegmentRanges(c.finalized)
	if c.tail != nil {
		result = append(result, cloneSegmentRanges([]SegmentRange{*c.tail})[0])
	}
	return result, c.prunedThrough
}

func (c *SegmentCatalog) prunedEndLSN() LSN {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.prunedThrough
}

// OpenWriter recovers the segment directory and opens its partial tail.
func OpenWriter(config WriterConfig) (*Writer, Recovery, error) {
	if config.Directory == "" {
		return nil, Recovery{}, errors.New("cdc: segment directory is required")
	}
	if config.RotationBytes < 0 {
		return nil, Recovery{}, errors.New("cdc: rotation threshold must not be negative")
	}
	if config.RotationBytes == 0 {
		config.RotationBytes = DefaultRotationBytes
	}
	if config.FileSync == nil {
		config.FileSync = func(file *os.File) error { return file.Sync() }
	}
	if config.DirectorySync == nil {
		config.DirectorySync = syncDirectory
	}
	if err := mkdirAllDurable(config.Directory, 0o750); err != nil {
		return nil, Recovery{}, err
	}
	recovery, finalized, err := recoverDirectoryWithProgress(
		config.Directory, config.RecoveryProgress,
	)
	if err != nil {
		return nil, Recovery{}, err
	}

	catalog := newSegmentCatalogState(
		config.Directory,
		finalized,
		recovery.catalogGeneration,
		recovery.prunedThrough,
		recovery.tailRange,
	)
	w := &Writer{
		dir:           config.Directory,
		rotationBytes: config.RotationBytes,
		catalog:       catalog,
		lastCommitLSN: recovery.LastCommitLSN,
		lastEndLSN:    recovery.DurableLSN,
		pendingEndLSN: recovery.DurableLSN,
		durableEndLSN: recovery.DurableLSN,
		partialPath:   recovery.PartialPath,
		fileSync:      config.FileSync,
		directorySync: config.DirectorySync,
	}
	if recovery.tailRange != nil {
		w.seekPoints = slices.Clone(recovery.tailRange.SeekPoints)
		if len(w.seekPoints) != 0 {
			w.lastSeek = w.seekPoints[len(w.seekPoints)-1].Offset
		}
	}
	if recovery.PartialPath != "" {
		w.file, err = os.OpenFile(recovery.PartialPath, os.O_RDWR, 0)
		if err != nil {
			return nil, Recovery{}, fmt.Errorf("cdc: open segment tail: %w", err)
		}
		info, statErr := w.file.Stat()
		if statErr != nil {
			_ = w.file.Close()
			return nil, Recovery{}, fmt.Errorf("cdc: stat segment tail: %w", statErr)
		}
		w.size = info.Size()
		if _, err := w.file.Seek(w.size, io.SeekStart); err != nil {
			_ = w.file.Close()
			return nil, Recovery{}, fmt.Errorf("cdc: seek segment tail: %w", err)
		}
	}
	return w, recovery, nil
}

// SegmentCatalog returns the writer's validated finalized-segment catalog.
// The catalog remains current as this writer rotates new segments.
func (w *Writer) SegmentCatalog() *SegmentCatalog {
	if w == nil {
		return nil
	}
	return w.catalog
}

// Append writes one complete transaction frame. It does not make the
// transaction durable until Sync is called, unless it triggers rotation.
func (w *Writer) Append(tx *Transaction) error {
	_, err := w.AppendFrame(tx)
	return err
}

// AppendFrame writes one complete transaction frame and returns its encoded
// byte count, including the frame header. It preserves Append's durability
// semantics and lets hot-path callers account bytes without encoding twice.
func (w *Writer) AppendFrame(tx *Transaction) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, errors.New("cdc: append to closed segment writer")
	}
	if tx == nil {
		return 0, errors.New("cdc: append nil transaction")
	}
	if tx.CommitLSN == 0 || tx.EndLSN < tx.CommitLSN {
		return 0, errors.New("cdc: invalid transaction LSNs")
	}
	if tx.CommitLSN <= w.lastCommitLSN {
		return 0, fmt.Errorf("cdc: commit LSN %x does not follow %x", tx.CommitLSN, w.lastCommitLSN)
	}
	if tx.EndLSN <= w.lastEndLSN {
		return 0, fmt.Errorf("cdc: end LSN %x does not follow %x", tx.EndLSN, w.lastEndLSN)
	}
	payloadSize, err := transactionPayloadSize(tx)
	if err != nil {
		return 0, err
	}
	if payloadSize > maxPayloadSize {
		return 0, fmt.Errorf("cdc: payload exceeds %d bytes", maxPayloadSize)
	}
	if w.file == nil {
		if err := w.openSegment(tx.CommitLSN); err != nil {
			return 0, err
		}
	}

	frameStart := w.size
	previousCommit := w.lastCommitLSN
	previousEnd := w.lastEndLSN
	var frameBytes int64
	if tx.Spill != nil || payloadSize > streamPayloadBytes {
		frameBytes, err = w.appendStreamedFrameLocked(tx, payloadSize)
	} else {
		frameBytes, err = w.appendResidentFrameLocked(tx)
	}
	if err != nil {
		return 0, err
	}
	if len(w.seekPoints) == 0 || frameStart-w.lastSeek >= segmentSeekInterval {
		w.seekPoints = append(w.seekPoints, SegmentSeekPoint{
			Offset:         frameStart,
			PreviousCommit: previousCommit,
			PreviousEnd:    previousEnd,
		})
		w.lastSeek = frameStart
	}
	w.size += frameBytes
	w.lastCommitLSN = tx.CommitLSN
	w.lastEndLSN = tx.EndLSN
	w.pendingEndLSN = tx.EndLSN
	w.catalog.updateTail(&SegmentRange{
		Path:          w.partialPath,
		StartCommit:   segmentStart(w.partialPath),
		LastCommit:    w.lastCommitLSN,
		LastEnd:       w.lastEndLSN,
		ValidatedSize: w.size,
		SeekPoints:    w.seekPoints,
	})

	if w.size >= w.rotationBytes {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}
	return frameBytes, nil
}

func (w *Writer) appendResidentFrameLocked(tx *Transaction) (int64, error) {
	var err error
	w.payload, err = AppendTransaction(w.payload[:0], tx)
	if err != nil {
		return 0, err
	}
	frameStart := w.size
	binary.LittleEndian.PutUint32(w.header[0:4], uint32(len(w.payload)))
	binary.LittleEndian.PutUint32(w.header[4:8], crc32.Checksum(w.payload, castagnoliTable))
	if err := writeFull(w.file, w.header[:]); err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: write frame header: %w", err)
	}
	if err := writeFull(w.file, w.payload); err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: write frame payload: %w", err)
	}
	return int64(frameHeaderSize + len(w.payload)), nil
}

func (w *Writer) appendStreamedFrameLocked(tx *Transaction, payloadSize uint64) (int64, error) {
	frameStart := w.size
	clear(w.header[:])
	if err := writeFull(w.file, w.header[:]); err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: write placeholder frame header: %w", err)
	}
	checksum := crc32.New(castagnoliTable)
	payloadBytes, err := WriteTransaction(io.MultiWriter(w.file, checksum), tx)
	if err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: stream frame payload: %w", err)
	}
	if payloadBytes != payloadSize {
		w.rollbackFrameLocked(frameStart)
		return 0, errors.New("cdc: streamed payload size changed")
	}
	binary.LittleEndian.PutUint32(w.header[0:4], uint32(payloadSize))
	binary.LittleEndian.PutUint32(w.header[4:8], checksum.Sum32())
	if _, err := w.file.Seek(frameStart, io.SeekStart); err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: seek streamed frame header: %w", err)
	}
	if err := writeFull(w.file, w.header[:]); err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: finalize streamed frame header: %w", err)
	}
	frameBytes := int64(frameHeaderSize) + int64(payloadSize)
	if _, err := w.file.Seek(frameStart+frameBytes, io.SeekStart); err != nil {
		w.rollbackFrameLocked(frameStart)
		return 0, fmt.Errorf("cdc: seek streamed frame end: %w", err)
	}
	_ = tx.CleanupSpill()
	return frameBytes, nil
}

func (w *Writer) rollbackFrameLocked(frameStart int64) {
	_ = w.file.Truncate(frameStart)
	_, _ = w.file.Seek(frameStart, io.SeekStart)
}

func (w *Writer) openSegment(start LSN) error {
	name := fmt.Sprintf("%016x.seg.partial", uint64(start))
	w.partialPath = filepath.Join(w.dir, name)
	file, err := os.OpenFile(w.partialPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o640)
	if err != nil {
		return fmt.Errorf("cdc: create segment: %w", err)
	}
	w.file = file
	w.size = 0
	w.seekPoints = nil
	w.lastSeek = 0
	w.catalog.updateTail(nil)
	if err := w.directorySync(w.dir); err != nil {
		_ = file.Close()
		w.file = nil
		_ = os.Remove(w.partialPath)
		w.partialPath = ""
		return fmt.Errorf("cdc: fsync new segment directory entry: %w", err)
	}
	return nil
}

// Sync group-fsyncs all appended transactions and returns the latest durable
// complete transaction EndLSN.
func (w *Writer) Sync() (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.durableEndLSN, errors.New("cdc: sync closed segment writer")
	}
	if err := w.syncLocked(); err != nil {
		return w.durableEndLSN, err
	}
	return w.durableEndLSN, nil
}

func (w *Writer) syncLocked() error {
	if w.file == nil || w.pendingEndLSN == w.durableEndLSN {
		return nil
	}
	if err := w.fileSync(w.file); err != nil {
		return fmt.Errorf("cdc: fsync segment: %w", err)
	}
	w.durableEndLSN = w.pendingEndLSN
	return nil
}

// DurableEndLSN returns the highest complete transaction EndLSN successfully
// fsynced. It is safe to report as PostgreSQL feedback.
func (w *Writer) DurableEndLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.durableEndLSN
}

// LastEndLSN returns the latest complete transaction appended, whether or not
// it has been fsynced. It is used only for local duplicate suppression; it
// must never be reported to PostgreSQL as durable feedback.
func (w *Writer) LastEndLSN() LSN {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastEndLSN
}

// Rotate fsyncs and finalizes the current partial segment. The next Append
// starts a new partial segment.
func (w *Writer) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("cdc: rotate closed segment writer")
	}
	return w.rotateLocked()
}

func (w *Writer) rotateLocked() error {
	if w.file == nil {
		return nil
	}
	finalized := SegmentRange{
		StartCommit:   segmentStart(w.partialPath),
		LastCommit:    w.lastCommitLSN,
		LastEnd:       w.lastEndLSN,
		ValidatedSize: w.size,
		SeekPoints:    slices.Clone(w.seekPoints),
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("cdc: close segment before rotation: %w", err)
	}
	w.file = nil
	w.catalog.mu.Lock()
	defer w.catalog.mu.Unlock()
	finalPath := strings.TrimSuffix(w.partialPath, ".partial")
	if err := os.Rename(w.partialPath, finalPath); err != nil {
		file, reopenErr := os.OpenFile(w.partialPath, os.O_RDWR, 0)
		if reopenErr == nil {
			w.file = file
			_, reopenErr = w.file.Seek(w.size, io.SeekStart)
		}
		if reopenErr != nil {
			return fmt.Errorf("cdc: finalize segment: %w (reopen tail: %v)", err, reopenErr)
		}
		return fmt.Errorf("cdc: finalize segment: %w", err)
	}
	w.partialPath = ""
	w.size = 0
	if err := w.directorySync(w.dir); err != nil {
		return err
	}
	finalized.Path = finalPath
	if err := w.catalog.addFinalizedLocked(finalized); err != nil {
		return err
	}
	w.seekPoints = nil
	w.lastSeek = 0
	return nil
}

func segmentStart(path string) LSN {
	start, _, ok := parseSegmentName(filepath.Base(path))
	if !ok {
		return 0
	}
	return start
}

// Finalize fsyncs and renames the current tail to .seg.
func (w *Writer) Finalize() error {
	return w.Rotate()
}

// Close fsyncs pending transactions and closes the partial tail without
// finalizing it, so a subsequent writer can recover and continue it.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	syncErr := w.syncLocked()
	var closeErr error
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			closeErr = fmt.Errorf("cdc: close segment: %w", err)
		}
		w.file = nil
	}
	return errors.Join(syncErr, closeErr)
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := writer.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("cdc: open segment directory for fsync: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("cdc: fsync segment directory: %w", err)
	}
	return nil
}

func mkdirAllDurable(directory string, mode os.FileMode) error {
	clean := filepath.Clean(directory)
	var created []string
	for current := clean; ; current = filepath.Dir(current) {
		_, err := os.Stat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cdc: stat directory %s: %w", current, err)
		}
		created = append(created, current)
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if err := os.MkdirAll(clean, mode); err != nil {
		return fmt.Errorf("cdc: create directory %s: %w", clean, err)
	}
	for i := len(created) - 1; i >= 0; i-- {
		if err := syncDirectory(created[i]); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(created[i])); err != nil {
			return err
		}
	}
	return nil
}

// Recover verifies finalized segments and truncates a torn or corrupt partial
// tail at its first invalid frame.
func Recover(directory string) (Recovery, error) {
	recovery, _, err := RecoverWithCatalog(directory)
	return recovery, err
}

// RecoverWithCatalog recovers a standalone CDC directory and returns the same
// immutable metadata view used by Writer and Reader.
func RecoverWithCatalog(
	directory string,
) (Recovery, *SegmentCatalog, error) {
	recovery, finalized, err := recoverDirectory(directory)
	if err != nil {
		return Recovery{}, nil, err
	}
	return recovery, newSegmentCatalogState(
		directory,
		finalized,
		recovery.catalogGeneration,
		recovery.prunedThrough,
		recovery.tailRange,
	), nil
}

func recoverDirectory(directory string) (Recovery, []SegmentRange, error) {
	return recoverDirectoryWithProgress(directory, nil)
}

func recoverDirectoryWithProgress(
	directory string,
	progress func(Recovery),
) (Recovery, []SegmentRange, error) {
	segments, err := listSegments(directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Recovery{}, nil, nil
		}
		return Recovery{}, nil, err
	}
	tracker := newRecoveryTracker(segments, progress)
	var (
		finalizedFiles []segmentFile
		partial        *segmentFile
	)
	for i := range segments {
		segment := segments[i]
		if segment.partial {
			if partial != nil {
				return Recovery{}, nil, errors.New("cdc: multiple partial segments")
			}
			partial = &segment
			continue
		}
		if partial != nil {
			return Recovery{}, nil, errors.New("cdc: finalized segment follows partial tail")
		}
		finalizedFiles = append(finalizedFiles, segment)
	}

	diskCatalog, catalogExists, catalogErr := loadDiskSegmentCatalog(directory)
	prunedThrough, pruneGeneration, pruneExists, pruneErr := loadPruneWatermark(directory)
	if catalogErr == nil && catalogExists {
		if pruneErr != nil ||
			prunedThrough < LSN(diskCatalog.PrunedThrough) ||
			(!pruneExists && diskCatalog.PrunedThrough != 0) {
			if err := persistPruneWatermark(
				directory, diskCatalog.Generation, LSN(diskCatalog.PrunedThrough),
			); err != nil {
				return Recovery{}, nil, err
			}
			prunedThrough = LSN(diskCatalog.PrunedThrough)
			pruneGeneration = diskCatalog.Generation
		}
		if prunedThrough > LSN(diskCatalog.PrunedThrough) {
			var err error
			diskCatalog, err = finishInterruptedCatalogPrune(
				directory, diskCatalog, prunedThrough, pruneGeneration,
			)
			if err != nil {
				return Recovery{}, nil, err
			}
		}
		return recoverFromDiskCatalog(
			directory, finalizedFiles, partial, diskCatalog, tracker,
		)
	}
	if pruneErr != nil {
		return Recovery{}, nil, fmt.Errorf(
			"cdc: recover without a valid catalog and pruned-prefix watermark: %w",
			pruneErr,
		)
	}
	// A missing catalog is the one-time compatibility path. A corrupt catalog is
	// also rebuilt from authoritative segment bytes rather than guessed.
	reason := "manifest_missing"
	if catalogErr != nil {
		reason = "manifest_invalid"
	}
	tracker.setFallback(reason, true)
	return rebuildDiskSegmentCatalog(
		directory, finalizedFiles, partial, prunedThrough, tracker,
	)
}

func finishInterruptedCatalogPrune(
	directory string,
	catalog diskSegmentCatalog,
	prunedThrough LSN,
	pruneGeneration uint64,
) (diskSegmentCatalog, error) {
	ranges, err := diskCatalogRanges(directory, catalog)
	if err != nil {
		return diskSegmentCatalog{}, err
	}
	cut := 0
	exact := LSN(catalog.PrunedThrough) == prunedThrough
	for cut < len(ranges) && ranges[cut].LastEnd <= prunedThrough {
		if ranges[cut].LastEnd == prunedThrough {
			exact = true
		}
		cut++
	}
	if !exact {
		return diskSegmentCatalog{}, fmt.Errorf(
			"cdc: pruned-prefix watermark %x is not a cataloged segment boundary",
			prunedThrough,
		)
	}
	generation := max(catalog.Generation, pruneGeneration) + 1
	if err := persistDiskSegmentCatalog(
		directory, generation, prunedThrough, ranges[cut:],
	); err != nil {
		return diskSegmentCatalog{}, err
	}
	updated, exists, err := loadDiskSegmentCatalog(directory)
	if err != nil {
		return diskSegmentCatalog{}, err
	}
	if !exists {
		return diskSegmentCatalog{}, errors.New(
			"cdc: reconciled segment catalog disappeared",
		)
	}
	return updated, nil
}

func rebuildDiskSegmentCatalog(
	directory string,
	finalizedFiles []segmentFile,
	partial *segmentFile,
	prunedThrough LSN,
	tracker *recoveryTracker,
) (Recovery, []SegmentRange, error) {
	if prunedThrough != 0 {
		kept := finalizedFiles[:0]
		removed := false
		for _, file := range finalizedFiles {
			if file.start <= prunedThrough {
				if err := os.Remove(file.path); err != nil &&
					!errors.Is(err, os.ErrNotExist) {
					return Recovery{}, nil, fmt.Errorf(
						"cdc: remove durably pruned segment %s: %w",
						filepath.Base(file.path), err,
					)
				}
				removed = true
				continue
			}
			kept = append(kept, file)
		}
		finalizedFiles = kept
		if removed {
			if err := syncDirectory(directory); err != nil {
				return Recovery{}, nil, err
			}
		}
	}
	scans, err := scanFinalizedSegments(finalizedFiles, tracker)
	if err != nil {
		return Recovery{}, nil, err
	}
	finalized := make([]SegmentRange, len(finalizedFiles))
	var result Recovery
	var previousCommit LSN
	previousEnd := prunedThrough
	for i, segment := range finalizedFiles {
		scan := scans[i]
		if scan.firstCommitLSN == 0 || scan.firstCommitLSN != segment.start {
			return Recovery{}, nil, fmt.Errorf(
				"cdc: finalized segment %s starts at %x, filename says %x",
				filepath.Base(segment.path), scan.firstCommitLSN, segment.start,
			)
		}
		if scan.firstCommitLSN <= previousCommit ||
			scan.firstEndLSN <= previousEnd ||
			scan.lastCommitLSN <= previousCommit ||
			scan.lastEndLSN <= previousEnd {
			return Recovery{}, nil, fmt.Errorf(
				"cdc: finalized segment %s does not follow its predecessor",
				filepath.Base(segment.path),
			)
		}
		if len(scan.seekPoints) != 0 {
			scan.seekPoints[0].PreviousCommit = previousCommit
			scan.seekPoints[0].PreviousEnd = previousEnd
		}
		finalized[i] = SegmentRange{
			Path:          segment.path,
			StartCommit:   segment.start,
			LastCommit:    scan.lastCommitLSN,
			LastEnd:       scan.lastEndLSN,
			ValidatedSize: scan.size,
			SeekPoints:    scan.seekPoints,
		}
		previousCommit = scan.lastCommitLSN
		previousEnd = scan.lastEndLSN
	}
	if prunedThrough != 0 {
		if err := persistPruneWatermark(directory, 1, prunedThrough); err != nil {
			return Recovery{}, nil, err
		}
	}
	if err := persistDiskSegmentCatalog(
		directory, 1, prunedThrough, finalized,
	); err != nil {
		return Recovery{}, nil, err
	}
	result.catalogGeneration = 1
	result.prunedThrough = prunedThrough
	if partial != nil {
		result.PartialPath = partial.path
		scan, err := scanSegmentObserved(
			partial.path, true, previousCommit, previousEnd, nil, tracker.scanned,
		)
		if err != nil {
			return Recovery{}, nil, err
		}
		result.TruncatedBytes = scan.truncated
		tracker.segmentScanned()
		previousCommit = scan.lastCommitLSN
		previousEnd = scan.lastEndLSN
		if scan.size != 0 {
			result.tailRange = &SegmentRange{
				Path:          partial.path,
				StartCommit:   partial.start,
				LastCommit:    scan.lastCommitLSN,
				LastEnd:       scan.lastEndLSN,
				ValidatedSize: scan.size,
				SeekPoints:    scan.seekPoints,
			}
		}
	}
	result.LastCommitLSN = previousCommit
	result.DurableLSN = previousEnd
	tracker.finish(&result)
	return result, finalized, nil
}

func recoverFromDiskCatalog(
	directory string,
	finalizedFiles []segmentFile,
	partial *segmentFile,
	diskCatalog diskSegmentCatalog,
	tracker *recoveryTracker,
) (Recovery, []SegmentRange, error) {
	finalized, err := diskCatalogRanges(directory, diskCatalog)
	if err != nil {
		// Structurally invalid metadata is never trusted.
		tracker.setFallback("manifest_invalid", true)
		return rebuildDiskSegmentCatalog(
			directory, finalizedFiles, partial,
			LSN(diskCatalog.PrunedThrough), tracker,
		)
	}
	var trustedBytes int64
	for _, segment := range finalized {
		trustedBytes += segment.ValidatedSize
	}
	tracker.trust(trustedBytes, int64(len(finalized)))
	filesByStart := make(map[LSN]segmentFile, len(finalizedFiles))
	for _, file := range finalizedFiles {
		filesByStart[file.start] = file
	}
	known := make(map[LSN]struct{}, len(finalized))
	for _, segment := range finalized {
		known[segment.StartCommit] = struct{}{}
		file, ok := filesByStart[segment.StartCommit]
		if !ok {
			return Recovery{}, nil, fmt.Errorf(
				"cdc: cataloged segment %s is missing from disk",
				filepath.Base(segment.Path),
			)
		}
		info, err := os.Stat(file.path)
		if err != nil {
			return Recovery{}, nil, fmt.Errorf(
				"cdc: stat cataloged segment %s: %w", filepath.Base(file.path), err,
			)
		}
		if !info.Mode().IsRegular() || info.Size() != segment.ValidatedSize {
			return Recovery{}, nil, fmt.Errorf(
				"cdc: immutable cataloged segment %s changed size from %d to %d",
				filepath.Base(file.path), segment.ValidatedSize, info.Size(),
			)
		}
	}

	var stalePrefix []string
	var suffix []segmentFile
	lastStart := LSN(0)
	if len(finalized) != 0 {
		lastStart = finalized[len(finalized)-1].StartCommit
	}
	for _, file := range finalizedFiles {
		if _, ok := known[file.start]; ok {
			continue
		}
		switch {
		case file.start <= LSN(diskCatalog.PrunedThrough):
			stalePrefix = append(stalePrefix, file.path)
		case len(finalized) == 0 || file.start > lastStart:
			suffix = append(suffix, file)
		default:
			return Recovery{}, nil, fmt.Errorf(
				"cdc: unexpected finalized segment %s inside cataloged range",
				filepath.Base(file.path),
			)
		}
	}
	if len(stalePrefix) != 0 {
		for _, path := range stalePrefix {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return Recovery{}, nil, fmt.Errorf(
					"cdc: remove durably pruned segment %s: %w", filepath.Base(path), err,
				)
			}
		}
		if err := syncDirectory(directory); err != nil {
			return Recovery{}, nil, err
		}
	}

	var previousCommit, previousEnd LSN
	if len(finalized) != 0 {
		previous := finalized[len(finalized)-1]
		previousCommit = previous.LastCommit
		previousEnd = previous.LastEnd
	} else {
		previousEnd = LSN(diskCatalog.PrunedThrough)
	}
	for _, file := range suffix {
		scan, err := scanSegmentObserved(
			file.path, false, previousCommit, previousEnd, nil, tracker.scanned,
		)
		if err != nil {
			return Recovery{}, nil, err
		}
		if scan.firstCommitLSN != file.start {
			return Recovery{}, nil, fmt.Errorf(
				"cdc: finalized suffix %s starts at %x, filename says %x",
				filepath.Base(file.path), scan.firstCommitLSN, file.start,
			)
		}
		finalized = append(finalized, SegmentRange{
			Path:          file.path,
			StartCommit:   file.start,
			LastCommit:    scan.lastCommitLSN,
			LastEnd:       scan.lastEndLSN,
			ValidatedSize: scan.size,
			SeekPoints:    scan.seekPoints,
		})
		tracker.segmentScanned()
		previousCommit = scan.lastCommitLSN
		previousEnd = scan.lastEndLSN
	}
	generation := diskCatalog.Generation
	if len(suffix) != 0 {
		generation++
		if err := persistDiskSegmentCatalog(
			directory,
			generation,
			LSN(diskCatalog.PrunedThrough),
			finalized,
		); err != nil {
			return Recovery{}, nil, err
		}
	}
	result := Recovery{
		catalogGeneration: generation,
		prunedThrough:     LSN(diskCatalog.PrunedThrough),
	}
	if partial != nil {
		result.PartialPath = partial.path
		scan, err := scanSegmentObserved(
			partial.path, true, previousCommit, previousEnd, nil, tracker.scanned,
		)
		if err != nil {
			return Recovery{}, nil, err
		}
		result.TruncatedBytes = scan.truncated
		tracker.segmentScanned()
		previousCommit = scan.lastCommitLSN
		previousEnd = scan.lastEndLSN
		if scan.size != 0 {
			result.tailRange = &SegmentRange{
				Path:          partial.path,
				StartCommit:   partial.start,
				LastCommit:    scan.lastCommitLSN,
				LastEnd:       scan.lastEndLSN,
				ValidatedSize: scan.size,
				SeekPoints:    scan.seekPoints,
			}
		}
	}
	result.LastCommitLSN = previousCommit
	result.DurableLSN = previousEnd
	tracker.finish(&result)
	return result, finalized, nil
}

type segmentFile struct {
	path    string
	start   LSN
	partial bool
}

func listSegments(directory string) ([]segmentFile, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("cdc: read segment directory: %w", err)
	}
	segments := make([]segmentFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		start, partial, ok := parseSegmentName(entry.Name())
		if !ok {
			continue
		}
		segments = append(segments, segmentFile{
			path:    filepath.Join(directory, entry.Name()),
			start:   start,
			partial: partial,
		})
	}
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].start == segments[j].start {
			return !segments[i].partial && segments[j].partial
		}
		return segments[i].start < segments[j].start
	})
	return segments, nil
}

func parseSegmentName(name string) (LSN, bool, bool) {
	partial := strings.HasSuffix(name, ".seg.partial")
	suffix := ".seg"
	if partial {
		suffix = ".seg.partial"
	}
	if !strings.HasSuffix(name, suffix) {
		return 0, false, false
	}
	base := strings.TrimSuffix(name, suffix)
	if len(base) != 16 {
		return 0, false, false
	}
	value, err := strconv.ParseUint(base, 16, 64)
	if err != nil {
		return 0, false, false
	}
	return LSN(value), partial, true
}

type scanResult struct {
	firstCommitLSN LSN
	firstEndLSN    LSN
	lastCommitLSN  LSN
	lastEndLSN     LSN
	size           int64
	truncated      int64
	seekPoints     []SegmentSeekPoint
}

type observedSegmentReader struct {
	reader  io.Reader
	observe func(int64)
	pending int64
}

func (r *observedSegmentReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if n > 0 && r.observe != nil {
		r.pending += int64(n)
		if r.pending >= 1<<20 {
			r.observe(r.pending)
			r.pending = 0
		}
	}
	return n, err
}

func (r *observedSegmentReader) flush() {
	if r.observe != nil && r.pending != 0 {
		r.observe(r.pending)
		r.pending = 0
	}
}

func scanSegment(
	path string,
	repairTail bool,
	previousCommit, previousEnd LSN,
	visit func(Transaction) error,
) (scanResult, error) {
	return scanSegmentObserved(
		path, repairTail, previousCommit, previousEnd, visit, nil,
	)
}

func scanSegmentObserved(
	path string,
	repairTail bool,
	previousCommit, previousEnd LSN,
	visit func(Transaction) error,
	observe func(int64),
) (scanResult, error) {
	flags := os.O_RDONLY
	if repairTail {
		flags = os.O_RDWR
	}
	file, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return scanResult{}, fmt.Errorf("cdc: open segment %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return scanResult{}, fmt.Errorf("cdc: stat segment %s: %w", filepath.Base(path), err)
	}

	result := scanResult{lastCommitLSN: previousCommit, lastEndLSN: previousEnd}
	observed := &observedSegmentReader{reader: file, observe: observe}
	defer observed.flush()
	payload := make([]byte, 0, 64<<10)
	streamBuffer := make([]byte, 64<<10)
	var header [frameHeaderSize]byte
	var invalid error
	for {
		frameStart := result.size
		previousFrameCommit := result.lastCommitLSN
		previousFrameEnd := result.lastEndLSN
		n, readErr := io.ReadFull(observed, header[:])
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			invalid = fmt.Errorf("short frame header: %w", readErr)
			result.size = frameStart
			_ = n
			break
		}
		length := binary.LittleEndian.Uint32(header[0:4])
		if uint64(length) > maxPayloadSize {
			invalid = fmt.Errorf("frame length %d exceeds limit", length)
			result.size = frameStart
			break
		}
		expected := binary.LittleEndian.Uint32(header[4:8])
		var tx Transaction
		if visit == nil {
			tx, readErr = scanTransactionMetadata(observed, length, expected, streamBuffer)
			if readErr != nil {
				invalid = readErr
				result.size = frameStart
				break
			}
		} else {
			if cap(payload) < int(length) {
				payload = make([]byte, int(length))
			} else {
				payload = payload[:int(length)]
			}
			if _, readErr = io.ReadFull(observed, payload); readErr != nil {
				invalid = fmt.Errorf("short frame payload: %w", readErr)
				result.size = frameStart
				break
			}
			if actual := crc32.Checksum(payload, castagnoliTable); actual != expected {
				invalid = fmt.Errorf("checksum mismatch: got %08x, want %08x", actual, expected)
				result.size = frameStart
				break
			}
			var decodeErr error
			tx, decodeErr = UnmarshalTransaction(payload)
			if decodeErr != nil {
				invalid = fmt.Errorf("decode transaction: %w", decodeErr)
				result.size = frameStart
				break
			}
		}
		if tx.CommitLSN <= result.lastCommitLSN || tx.EndLSN < tx.CommitLSN {
			invalid = fmt.Errorf("non-monotonic commit LSN %x after %x", tx.CommitLSN, result.lastCommitLSN)
			result.size = frameStart
			break
		}
		if tx.EndLSN <= result.lastEndLSN {
			invalid = fmt.Errorf("non-monotonic end LSN %x after %x", tx.EndLSN, result.lastEndLSN)
			result.size = frameStart
			break
		}
		if result.firstCommitLSN == 0 {
			result.firstCommitLSN = tx.CommitLSN
			result.firstEndLSN = tx.EndLSN
		}
		if len(result.seekPoints) == 0 ||
			frameStart-result.seekPoints[len(result.seekPoints)-1].Offset >= segmentSeekInterval {
			result.seekPoints = append(result.seekPoints, SegmentSeekPoint{
				Offset:         frameStart,
				PreviousCommit: previousFrameCommit,
				PreviousEnd:    previousFrameEnd,
			})
		}
		result.lastCommitLSN = tx.CommitLSN
		result.lastEndLSN = tx.EndLSN
		result.size += int64(frameHeaderSize) + int64(length)
		if visit != nil {
			if err := visit(tx); err != nil {
				return result, err
			}
		}
	}

	if invalid == nil {
		if repairTail {
			if err := file.Sync(); err != nil {
				return scanResult{}, fmt.Errorf("cdc: fsync recovered segment %s: %w", filepath.Base(path), err)
			}
		}
		return result, nil
	}
	if !repairTail {
		return scanResult{}, fmt.Errorf("cdc: corrupt finalized segment %s at byte %d: %w", filepath.Base(path), result.size, invalid)
	}
	result.truncated = info.Size() - result.size
	if err := file.Truncate(result.size); err != nil {
		return scanResult{}, fmt.Errorf("cdc: truncate segment %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		return scanResult{}, fmt.Errorf("cdc: fsync repaired segment %s: %w", filepath.Base(path), err)
	}
	return result, nil
}

func scanTransactionMetadata(
	file io.Reader,
	length uint32,
	expected uint32,
	buffer []byte,
) (Transaction, error) {
	const metadataBytes = 21
	if length < metadataBytes {
		return Transaction{}, errors.New("frame payload is shorter than transaction metadata")
	}
	checksum := crc32.New(castagnoliTable)
	var metadata [metadataBytes]byte
	if _, err := io.ReadFull(file, metadata[:]); err != nil {
		return Transaction{}, fmt.Errorf("short frame payload: %w", err)
	}
	_, _ = checksum.Write(metadata[:])
	remaining := uint64(length) - metadataBytes
	for remaining != 0 {
		chunk := uint64(len(buffer))
		if chunk > remaining {
			chunk = remaining
		}
		if _, err := io.ReadFull(file, buffer[:int(chunk)]); err != nil {
			return Transaction{}, fmt.Errorf("short frame payload: %w", err)
		}
		_, _ = checksum.Write(buffer[:int(chunk)])
		remaining -= chunk
	}
	if actual := checksum.Sum32(); actual != expected {
		return Transaction{}, fmt.Errorf("checksum mismatch: got %08x, want %08x", actual, expected)
	}
	if string(metadata[:4]) != string(payloadMagic[:]) {
		return Transaction{}, errors.New("invalid payload magic")
	}
	if metadata[4] != 1 && metadata[4] != payloadVersion {
		return Transaction{}, fmt.Errorf("unsupported payload version %d", metadata[4])
	}
	return Transaction{
		CommitLSN: LSN(binary.LittleEndian.Uint64(metadata[5:13])),
		EndLSN:    LSN(binary.LittleEndian.Uint64(metadata[13:21])),
	}, nil
}
