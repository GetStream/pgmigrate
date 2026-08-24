package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameHeaderSize          = 8
	DefaultRotationBytes     = int64(1 << 30)
	DefaultRecoveryWorkers   = 4
	MaxRecoveryWorkers       = 4
	recoveryProgressInterval = time.Second
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// WriterConfig configures an append-only segment writer.
type WriterConfig struct {
	Directory        string
	RotationBytes    int64
	RecoveryWorkers  int
	RecoveryProgress func(RecoveryProgress)
	FileSync         func(*os.File) error
	DirectorySync    func(string) error
}

// RecoveryProgress reports the read-only validation that precedes reopening a
// durable CDC stream. The counters are monotonic. BytesTotal is the size
// observed before validation; the segment checks themselves remain authoritative.
type RecoveryProgress struct {
	FilesChecked   int
	FilesTotal     int
	BytesScanned   int64
	BytesTotal     int64
	BytesTruncated int64
	Elapsed        time.Duration
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
	fileSync      func(*os.File) error
	directorySync func(string) error
	closed        bool
}

// Recovery reports the result of scanning the segment directory.
type Recovery struct {
	LastCommitLSN  LSN
	DurableLSN     LSN
	PartialPath    string
	TruncatedBytes int64
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
}

// SegmentCatalog caches validated finalized-segment bounds for pruning. Disk is
// authoritative: OpenWriter rebuilds the catalog through recovery after every
// restart, and the mutable partial tail is never included.
type SegmentCatalog struct {
	mu        sync.RWMutex
	directory string
	finalized []SegmentRange
}

func newSegmentCatalog(directory string, finalized []SegmentRange) *SegmentCatalog {
	return &SegmentCatalog{
		directory: directory,
		finalized: append([]SegmentRange(nil), finalized...),
	}
}

func (c *SegmentCatalog) snapshot() []SegmentRange {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]SegmentRange(nil), c.finalized...)
}

func (c *SegmentCatalog) addFinalized(segment SegmentRange) error {
	if c == nil {
		return errors.New("cdc: finalized segment catalog is missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
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
	}
	c.finalized = append(c.finalized, segment)
	return nil
}

func (c *SegmentCatalog) removeFinalized(paths []string) {
	if c == nil || len(paths) == 0 {
		return
	}
	removed := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		removed[path] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := c.finalized[:0]
	for _, segment := range c.finalized {
		if _, ok := removed[segment.Path]; !ok {
			kept = append(kept, segment)
		}
	}
	c.finalized = kept
}

func (c *SegmentCatalog) replaceFinalized(replacement SegmentRange) error {
	if c == nil {
		return errors.New("cdc: finalized segment catalog is missing")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.finalized {
		if c.finalized[i].Path != replacement.Path {
			continue
		}
		if replacement.StartCommit != c.finalized[i].StartCommit {
			return fmt.Errorf("cdc: cataloged segment %s changed start LSN", filepath.Base(replacement.Path))
		}
		if i > 0 {
			previous := c.finalized[i-1]
			if replacement.LastCommit <= previous.LastCommit || replacement.LastEnd <= previous.LastEnd {
				return fmt.Errorf("cdc: refreshed segment %s no longer follows %s",
					filepath.Base(replacement.Path), filepath.Base(previous.Path))
			}
		}
		if i+1 < len(c.finalized) {
			next := c.finalized[i+1]
			if replacement.LastCommit >= next.LastCommit || replacement.LastEnd >= next.LastEnd {
				return fmt.Errorf("cdc: refreshed segment %s no longer precedes %s",
					filepath.Base(replacement.Path), filepath.Base(next.Path))
			}
		}
		c.finalized[i] = replacement
		return nil
	}
	return fmt.Errorf("cdc: refreshed segment %s is absent from catalog", filepath.Base(replacement.Path))
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
	if config.RecoveryWorkers < 0 || config.RecoveryWorkers > MaxRecoveryWorkers {
		return nil, Recovery{}, fmt.Errorf(
			"cdc: recovery workers must be between 0 and %d (0 uses the default)", MaxRecoveryWorkers,
		)
	}
	if config.RecoveryWorkers == 0 {
		config.RecoveryWorkers = DefaultRecoveryWorkers
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
	recovery, finalized, err := recoverDirectoryWithConfig(
		config.Directory, config.RecoveryWorkers, config.RecoveryProgress,
	)
	if err != nil {
		return nil, Recovery{}, err
	}

	w := &Writer{
		dir:           config.Directory,
		rotationBytes: config.RotationBytes,
		catalog:       newSegmentCatalog(config.Directory, finalized),
		lastCommitLSN: recovery.LastCommitLSN,
		lastEndLSN:    recovery.DurableLSN,
		pendingEndLSN: recovery.DurableLSN,
		durableEndLSN: recovery.DurableLSN,
		partialPath:   recovery.PartialPath,
		fileSync:      config.FileSync,
		directorySync: config.DirectorySync,
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

	var frameBytes int64
	if tx.Spill != nil || payloadSize > streamPayloadBytes {
		frameBytes, err = w.appendStreamedFrameLocked(tx, payloadSize)
	} else {
		frameBytes, err = w.appendResidentFrameLocked(tx)
	}
	if err != nil {
		return 0, err
	}
	w.size += frameBytes
	w.lastCommitLSN = tx.CommitLSN
	w.lastEndLSN = tx.EndLSN
	w.pendingEndLSN = tx.EndLSN

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
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("cdc: close segment before rotation: %w", err)
	}
	w.file = nil
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
	if err := w.catalog.addFinalized(finalized); err != nil {
		return err
	}
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
	recovery, _, err := recoverDirectory(directory)
	return recovery, err
}

func recoverDirectory(directory string) (Recovery, []SegmentRange, error) {
	return recoverDirectoryWithConfig(directory, DefaultRecoveryWorkers, nil)
}

type recoverySegmentResult struct {
	scan scanResult
	err  error
}

func recoverDirectoryWithConfig(
	directory string,
	workers int,
	progress func(RecoveryProgress),
) (Recovery, []SegmentRange, error) {
	segments, err := listSegments(directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Recovery{}, nil, nil
		}
		return Recovery{}, nil, err
	}
	if workers < 1 || workers > MaxRecoveryWorkers {
		return Recovery{}, nil, fmt.Errorf(
			"cdc: recovery workers must be between 1 and %d", MaxRecoveryWorkers,
		)
	}

	partialIndex := len(segments)
	for i, segment := range segments {
		if !segment.partial {
			if partialIndex != len(segments) {
				return Recovery{}, nil, errors.New("cdc: finalized segment follows partial tail")
			}
			continue
		}
		if partialIndex != len(segments) {
			return Recovery{}, nil, errors.New("cdc: multiple partial segments")
		}
		partialIndex = i
	}

	totalBytes, err := prepareRecoverySegments(segments)
	if err != nil {
		return Recovery{}, nil, err
	}
	reporter := newRecoveryProgressReporter(len(segments), totalBytes, progress)
	defer reporter.finish()

	finalizedSegments := segments[:partialIndex]
	results := make([]recoverySegmentResult, len(finalizedSegments))
	if len(finalizedSegments) != 0 {
		workerCount := min(workers, len(finalizedSegments))
		jobs := make(chan int)
		var group sync.WaitGroup
		group.Add(workerCount)
		for range workerCount {
			go func() {
				defer group.Done()
				for index := range jobs {
					segment := finalizedSegments[index]
					results[index].scan, results[index].err = scanSegmentWithProgress(
						segment.path, false, 0, 0, nil, reporter.addBytes, segment.size,
					)
					if results[index].err == nil {
						reporter.completeFile()
					}
				}
			}()
		}
		for index := range finalizedSegments {
			jobs <- index
		}
		close(jobs)
		group.Wait()
	}

	var result Recovery
	finalized := make([]SegmentRange, 0, len(segments))
	var previousCommit LSN
	var previousEnd LSN
	for index, segment := range finalizedSegments {
		item := results[index]
		if item.err != nil {
			return Recovery{}, nil, item.err
		}
		scan := item.scan
		if scan.frames != 0 {
			if scan.firstCommitLSN <= previousCommit {
				return Recovery{}, nil, fmt.Errorf(
					"cdc: corrupt finalized segment %s at byte 0: non-monotonic commit LSN %x after %x",
					filepath.Base(segment.path), scan.firstCommitLSN, previousCommit,
				)
			}
			if scan.firstEndLSN <= previousEnd {
				return Recovery{}, nil, fmt.Errorf(
					"cdc: corrupt finalized segment %s at byte 0: non-monotonic end LSN %x after %x",
					filepath.Base(segment.path), scan.firstEndLSN, previousEnd,
				)
			}
			previousCommit = scan.lastCommitLSN
			previousEnd = scan.lastEndLSN
		}
		finalized = append(finalized, SegmentRange{
			Path:          segment.path,
			StartCommit:   segment.start,
			LastCommit:    previousCommit,
			LastEnd:       previousEnd,
			ValidatedSize: scan.size,
		})
	}

	if partialIndex < len(segments) {
		partial := segments[partialIndex]
		result.PartialPath = partial.path
		scan, scanErr := scanSegmentWithProgress(
			partial.path, true, previousCommit, previousEnd, nil, reporter.addBytes, partial.size,
		)
		if scanErr != nil {
			return Recovery{}, nil, scanErr
		}
		reporter.repairedTail(scan.truncated)
		reporter.completeFile()
		previousCommit = scan.lastCommitLSN
		previousEnd = scan.lastEndLSN
		result.TruncatedBytes = scan.truncated
	}
	result.LastCommitLSN = previousCommit
	result.DurableLSN = previousEnd
	return result, finalized, nil
}

func prepareRecoverySegments(segments []segmentFile) (int64, error) {
	var total int64
	for index := range segments {
		segment := &segments[index]
		info, err := os.Stat(segment.path)
		if err != nil {
			return 0, fmt.Errorf("cdc: stat segment %s: %w", filepath.Base(segment.path), err)
		}
		if !info.Mode().IsRegular() {
			return 0, fmt.Errorf("cdc: segment %s is not a regular file", filepath.Base(segment.path))
		}
		if info.Size() > (1<<63-1)-total {
			return 0, errors.New("cdc: segment byte total overflows int64")
		}
		segment.size = info.Size()
		total += info.Size()
	}
	return total, nil
}

type recoveryProgressReporter struct {
	callback   func(RecoveryProgress)
	started    time.Time
	filesTotal int
	bytesTotal int64
	filesDone  atomic.Int64
	bytesDone  atomic.Int64
	bytesTorn  atomic.Int64
	nextReport atomic.Int64
	callbackMu sync.Mutex
}

func newRecoveryProgressReporter(
	filesTotal int,
	bytesTotal int64,
	callback func(RecoveryProgress),
) *recoveryProgressReporter {
	started := time.Now()
	reporter := &recoveryProgressReporter{
		callback: callback, started: started, filesTotal: filesTotal, bytesTotal: bytesTotal,
	}
	reporter.nextReport.Store(started.Add(recoveryProgressInterval).UnixNano())
	reporter.report()
	return reporter
}

func (r *recoveryProgressReporter) addBytes(count int64) {
	if count <= 0 {
		return
	}
	for {
		previous := r.bytesDone.Load()
		next := previous + count
		if next < previous || next > r.bytesTotal {
			next = r.bytesTotal
		}
		if r.bytesDone.CompareAndSwap(previous, next) {
			break
		}
	}
	if r.callback == nil {
		return
	}
	now := time.Now()
	for {
		next := r.nextReport.Load()
		if now.UnixNano() < next {
			return
		}
		if r.nextReport.CompareAndSwap(next, now.Add(recoveryProgressInterval).UnixNano()) {
			r.report()
			return
		}
	}
}

func (r *recoveryProgressReporter) completeFile() {
	r.filesDone.Add(1)
	r.report()
}

func (r *recoveryProgressReporter) repairedTail(count int64) {
	if count > 0 {
		r.bytesTorn.Add(count)
	}
}

func (r *recoveryProgressReporter) finish() {
	r.report()
}

func (r *recoveryProgressReporter) report() {
	if r.callback == nil {
		return
	}
	r.callbackMu.Lock()
	defer r.callbackMu.Unlock()
	r.callback(RecoveryProgress{
		FilesChecked:   int(r.filesDone.Load()),
		FilesTotal:     r.filesTotal,
		BytesScanned:   r.bytesDone.Load(),
		BytesTotal:     r.bytesTotal,
		BytesTruncated: r.bytesTorn.Load(),
		Elapsed:        time.Since(r.started),
	})
}

type segmentFile struct {
	path    string
	start   LSN
	partial bool
	size    int64
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
	frames         int64
}

func scanSegment(path string, repairTail bool, previousCommit, previousEnd LSN, visit func(Transaction) error) (scanResult, error) {
	return scanSegmentWithProgress(path, repairTail, previousCommit, previousEnd, visit, nil, -1)
}

func scanSegmentWithProgress(
	path string,
	repairTail bool,
	previousCommit, previousEnd LSN,
	visit func(Transaction) error,
	onRead func(int64),
	expectedSize int64,
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
	if expectedSize >= 0 && info.Size() != expectedSize {
		return scanResult{}, fmt.Errorf(
			"cdc: segment %s changed before recovery validation: size is %d, expected %d",
			filepath.Base(path), info.Size(), expectedSize,
		)
	}
	var reader io.Reader = file
	if onRead != nil {
		reader = recoveryCountingReader{reader: file, onRead: onRead}
	}

	result := scanResult{lastCommitLSN: previousCommit, lastEndLSN: previousEnd}
	payload := make([]byte, 0, 64<<10)
	streamBuffer := make([]byte, 64<<10)
	var header [frameHeaderSize]byte
	var invalid error
	for {
		frameStart := result.size
		n, readErr := io.ReadFull(reader, header[:])
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
			tx, readErr = scanTransactionMetadata(reader, length, expected, streamBuffer)
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
			if _, readErr = io.ReadFull(reader, payload); readErr != nil {
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
		if result.frames == 0 {
			result.firstCommitLSN = tx.CommitLSN
			result.firstEndLSN = tx.EndLSN
		}
		result.lastCommitLSN = tx.CommitLSN
		result.lastEndLSN = tx.EndLSN
		result.size += int64(frameHeaderSize) + int64(length)
		result.frames++
		if visit != nil {
			if err := visit(tx); err != nil {
				return result, err
			}
		}
	}

	if invalid == nil {
		if expectedSize >= 0 {
			if err := requireUnchangedRecoverySize(file, path, info.Size()); err != nil {
				return scanResult{}, err
			}
		}
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
	if expectedSize >= 0 {
		if err := requireUnchangedRecoverySize(file, path, info.Size()); err != nil {
			return scanResult{}, err
		}
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

func requireUnchangedRecoverySize(file *os.File, path string, expected int64) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cdc: restat segment %s: %w", filepath.Base(path), err)
	}
	if info.Size() != expected {
		return fmt.Errorf(
			"cdc: segment %s changed during recovery validation: size is %d, expected %d",
			filepath.Base(path), info.Size(), expected,
		)
	}
	return nil
}

type recoveryCountingReader struct {
	reader io.Reader
	onRead func(int64)
}

func (r recoveryCountingReader) Read(buffer []byte) (int, error) {
	read, err := r.reader.Read(buffer)
	if read > 0 {
		r.onRead(int64(read))
	}
	return read, err
}

func scanTransactionMetadata(
	reader io.Reader,
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
	if _, err := io.ReadFull(reader, metadata[:]); err != nil {
		return Transaction{}, fmt.Errorf("short frame payload: %w", err)
	}
	_, _ = checksum.Write(metadata[:])
	remaining := uint64(length) - metadataBytes
	for remaining != 0 {
		chunk := uint64(len(buffer))
		if chunk > remaining {
			chunk = remaining
		}
		if _, err := io.ReadFull(reader, buffer[:int(chunk)]); err != nil {
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
