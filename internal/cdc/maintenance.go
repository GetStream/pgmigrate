package cdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ProgressCallback runs after target data and progress commit atomically.
// Returning an error terminates the applier.
type ProgressCallback func(context.Context, LSN) error

type SegmentPrunerConfig struct {
	Directory         string
	Interval          time.Duration
	Catalog           *SegmentCatalog
	MaxRemoveSegments int
}

// SegmentPruner provides an applier progress callback that periodically prunes
// finalized segments while retaining Prune's safety segment.
type SegmentPruner struct {
	directory string
	interval  time.Duration
	catalog   *SegmentCatalog
	maxRemove int
	mu        sync.Mutex
	next      time.Time
	latest    LSN
	wake      chan struct{}
}

func NewSegmentPruner(config SegmentPrunerConfig) (*SegmentPruner, error) {
	if config.Directory == "" {
		return nil, errors.New("cdc: segment pruner directory is required")
	}
	if config.Interval < 0 {
		return nil, errors.New("cdc: segment prune interval must not be negative")
	}
	if config.Interval == 0 {
		config.Interval = time.Minute
	}
	if config.MaxRemoveSegments < 0 {
		return nil, errors.New("cdc: segment prune removal batch must not be negative")
	}
	if config.MaxRemoveSegments == 0 {
		config.MaxRemoveSegments = 128
	}
	if config.Catalog != nil && filepath.Clean(config.Catalog.directory) != filepath.Clean(config.Directory) {
		return nil, errors.New("cdc: segment pruner catalog directory does not match")
	}
	return &SegmentPruner{
		directory: config.Directory,
		interval:  config.Interval,
		catalog:   config.Catalog,
		maxRemove: config.MaxRemoveSegments,
		wake:      make(chan struct{}, 1),
	}, nil
}

// OnProgress is suitable for ApplierConfig.AfterProgress.
func (p *SegmentPruner) OnProgress(_ context.Context, applied LSN) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if applied > p.latest {
		p.latest = applied
	}
	now := time.Now()
	if !p.next.IsZero() && now.Before(p.next) {
		return nil
	}
	more, err := p.pruneLocked(p.latest, now)
	if more {
		p.wakeLocked()
	}
	return err
}

// Run continues bounded pruning batches independently of apply progress. It
// must be supervised alongside the applier so maintenance failures stop the
// migration instead of becoming detached background errors.
func (p *SegmentPruner) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-p.wake:
		}
		for {
			p.mu.Lock()
			more, err := p.pruneLocked(p.latest, time.Now())
			p.mu.Unlock()
			if err != nil {
				return err
			}
			if !more {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
}

func (p *SegmentPruner) pruneLocked(applied LSN, now time.Time) (bool, error) {
	var (
		more bool
		err  error
	)
	if p.catalog != nil {
		_, more, err = p.catalog.prune(applied, p.maxRemove)
	} else {
		_, err = Prune(p.directory, applied)
	}
	if err != nil {
		return false, fmt.Errorf("cdc: prune applied segments through %x: %w", applied, err)
	}
	if more {
		p.next = time.Time{}
	} else {
		p.next = now.Add(p.interval)
	}
	return more, nil
}

func (p *SegmentPruner) wakeLocked() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// prune removes a bounded oldest prefix of applied finalized segments using
// bounds already validated by recovery or writer rotation. The newest eligible
// segment is retained as the same safety segment kept by scan-based Prune.
func (c *SegmentCatalog) prune(applied LSN, maxRemove int) ([]string, bool, error) {
	finalized := c.snapshot()
	entries, err := os.ReadDir(c.directory)
	if err != nil {
		return nil, false, fmt.Errorf("read segment directory before catalog prune: %w", err)
	}
	present := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, _, ok := parseSegmentName(entry.Name()); ok {
			present[filepath.Join(c.directory, entry.Name())] = struct{}{}
		}
	}
	for _, segment := range finalized {
		if _, ok := present[segment.Path]; !ok {
			// A writer only adds files and this pruner is serialized, so a
			// missing cataloged file is not a benign rotation race. Do not
			// delete anything else from an unexplained directory state.
			return nil, false, fmt.Errorf(
				"cataloged segment %s is missing from disk", filepath.Base(segment.Path),
			)
		}
	}
	eligible := 0
	for eligible < len(finalized) && finalized[eligible].LastEnd < applied {
		eligible++
	}
	if eligible <= 1 {
		return nil, false, nil
	}
	candidates := finalized[:eligible-1]
	more := len(candidates) > maxRemove
	if more {
		candidates = candidates[:maxRemove]
	}

	for i, segment := range candidates {
		info, err := os.Stat(segment.Path)
		if err != nil {
			return nil, more, fmt.Errorf("stat cataloged segment %s: %w", filepath.Base(segment.Path), err)
		}
		if !info.Mode().IsRegular() || info.Size() != segment.ValidatedSize {
			refreshed, err := revalidateCatalogedSegment(finalized, i)
			if err != nil {
				return nil, more, fmt.Errorf(
					"cataloged segment %s changed after validation: %w",
					filepath.Base(segment.Path), err,
				)
			}
			if err := c.replaceFinalized(refreshed); err != nil {
				return nil, more, err
			}
			// Recompute the eligible prefix from the refreshed catalog before
			// deleting anything. This call remains conservative and bounded to
			// one fallback segment scan.
			return nil, true, nil
		}
	}

	removed := make([]string, 0, len(candidates))
	var removeErr error
	for _, segment := range candidates {
		if err := os.Remove(segment.Path); err != nil {
			removeErr = fmt.Errorf("remove segment %s: %w", filepath.Base(segment.Path), err)
			break
		}
		removed = append(removed, segment.Path)
	}
	if len(removed) == 0 {
		return nil, more, removeErr
	}
	directoryErr := syncDirectory(c.directory)
	if directoryErr == nil {
		c.removeFinalized(removed)
	}
	if removeErr != nil || directoryErr != nil {
		return removed, more, errors.Join(removeErr, directoryErr)
	}
	return removed, more, nil
}

func revalidateCatalogedSegment(finalized []SegmentRange, index int) (SegmentRange, error) {
	if index < 0 || index >= len(finalized) {
		return SegmentRange{}, errors.New("cdc: catalog segment index is out of range")
	}
	var previousCommit, previousEnd LSN
	if index > 0 {
		previousCommit = finalized[index-1].LastCommit
		previousEnd = finalized[index-1].LastEnd
	}
	segment := finalized[index]
	scan, err := scanSegment(segment.Path, false, previousCommit, previousEnd, nil)
	if err != nil {
		return SegmentRange{}, err
	}
	return SegmentRange{
		Path:          segment.Path,
		StartCommit:   segment.StartCommit,
		LastCommit:    scan.lastCommitLSN,
		LastEnd:       scan.lastEndLSN,
		ValidatedSize: scan.size,
	}, nil
}
