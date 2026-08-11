package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ProgressCallback runs after target data and progress commit atomically.
// Returning an error terminates the applier.
type ProgressCallback func(context.Context, LSN) error

type SegmentPrunerConfig struct {
	Directory string
	Interval  time.Duration
}

// SegmentPruner provides an applier progress callback that periodically prunes
// finalized segments while retaining Prune's safety segment.
type SegmentPruner struct {
	directory string
	interval  time.Duration
	mu        sync.Mutex
	next      time.Time
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
	return &SegmentPruner{directory: config.Directory, interval: config.Interval}, nil
}

// OnProgress is suitable for ApplierConfig.AfterProgress.
func (p *SegmentPruner) OnProgress(_ context.Context, applied LSN) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if !p.next.IsZero() && now.Before(p.next) {
		return nil
	}
	if _, err := Prune(p.directory, applied); err != nil {
		return fmt.Errorf("cdc: prune applied segments through %x: %w", applied, err)
	}
	p.next = now.Add(p.interval)
	return nil
}
