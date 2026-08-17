package cdc

import (
	"os"
	"sync"
	"time"
)

// Recovery reports payload work performed while opening the CDC directory.
// Trusted counters are metadata-only; scanned counters are bytes that were
// actually read and checksummed.
type Recovery struct {
	LastCommitLSN  LSN
	DurableLSN     LSN
	PartialPath    string
	TruncatedBytes int64

	TotalBytes      int64
	TrustedBytes    int64
	ScannedBytes    int64
	TotalSegments   int64
	TrustedSegments int64
	ScannedSegments int64
	Elapsed         time.Duration
	FallbackReason  string
	ManifestRebuilt bool

	catalogGeneration uint64
	prunedThrough     LSN
	tailRange         *SegmentRange
}

type recoveryTracker struct {
	mu       sync.Mutex
	started  time.Time
	lastEmit time.Time
	callback func(Recovery)
	progress Recovery
}

func newRecoveryTracker(
	files []segmentFile,
	callback func(Recovery),
) *recoveryTracker {
	tracker := &recoveryTracker{
		started:  time.Now(),
		callback: callback,
	}
	for _, file := range files {
		info, err := os.Stat(file.path)
		if err == nil {
			tracker.progress.TotalBytes += info.Size()
		}
		tracker.progress.TotalSegments++
	}
	tracker.emitLocked(true)
	return tracker
}

func (t *recoveryTracker) setFallback(reason string, rebuilt bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.FallbackReason = reason
	t.progress.ManifestRebuilt = rebuilt
	t.progress.TrustedBytes = 0
	t.progress.TrustedSegments = 0
	t.emitLocked(true)
}

func (t *recoveryTracker) trust(bytes, segments int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.TrustedBytes = bytes
	t.progress.TrustedSegments = segments
	t.emitLocked(false)
}

func (t *recoveryTracker) scanned(bytes int64) {
	if bytes <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.ScannedBytes += bytes
	t.emitLocked(false)
}

func (t *recoveryTracker) segmentScanned() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.ScannedSegments++
	t.emitLocked(false)
}

func (t *recoveryTracker) finish(result *Recovery) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.LastCommitLSN = result.LastCommitLSN
	t.progress.DurableLSN = result.DurableLSN
	t.progress.PartialPath = result.PartialPath
	t.progress.TruncatedBytes = result.TruncatedBytes
	t.progress.Elapsed = time.Since(t.started)
	t.progress.catalogGeneration = result.catalogGeneration
	t.progress.prunedThrough = result.prunedThrough
	t.progress.tailRange = result.tailRange
	*result = t.progress
	t.emitLocked(true)
}

func (t *recoveryTracker) emitLocked(force bool) {
	if t.callback == nil {
		return
	}
	now := time.Now()
	if !force && now.Sub(t.lastEmit) < 250*time.Millisecond {
		return
	}
	snapshot := t.progress
	snapshot.Elapsed = now.Sub(t.started)
	t.lastEmit = now
	t.callback(snapshot)
}
