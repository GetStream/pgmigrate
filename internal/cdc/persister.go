package cdc

import (
	"context"
	"errors"
	"time"
)

type PersisterConfig struct {
	Writer       *Writer
	Transactions <-chan Transaction
	Durable      *DurableWatermark
	SyncBytes    int64
	SyncInterval time.Duration
}

// Persister appends complete transactions and group-fsyncs by byte and time
// thresholds. It publishes EndLSN only after Writer.Sync succeeds.
type Persister struct {
	config PersisterConfig
}

func NewPersister(config PersisterConfig) (*Persister, error) {
	if config.Writer == nil {
		return nil, errors.New("cdc: persister writer is required")
	}
	if config.Transactions == nil {
		return nil, errors.New("cdc: persister transaction channel is required")
	}
	if config.Durable == nil {
		return nil, errors.New("cdc: persister durable watermark is required")
	}
	if config.SyncBytes <= 0 {
		config.SyncBytes = 4 << 20
	}
	if config.SyncInterval <= 0 {
		config.SyncInterval = 100 * time.Millisecond
	}
	config.Durable.Publish(config.Writer.DurableEndLSN())
	return &Persister{config: config}, nil
}

func (p *Persister) Run(ctx context.Context) error {
	timer := time.NewTimer(p.config.SyncInterval)
	defer timer.Stop()
	var unsyncedBytes int64
	if p.config.Writer.LastEndLSN() > p.config.Writer.DurableEndLSN() {
		// A persister can be recreated around a writer with an unsynced tail.
		// Its exact byte count is immaterial; force the next timer/close sync.
		unsyncedBytes = 1
	}
	sync := func() error {
		if unsyncedBytes == 0 {
			return nil
		}
		durable, err := p.config.Writer.Sync()
		if err != nil {
			return err
		}
		p.config.Durable.Publish(durable)
		unsyncedBytes = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case transaction, ok := <-p.config.Transactions:
			if !ok {
				return sync()
			}
			if transaction.EndLSN <= p.config.Writer.LastEndLSN() {
				// START_REPLICATION correctly resumes at the durable LSN, so a
				// reconnect can replay transactions already appended locally
				// but not yet fsynced. Keep the first frame and discard replay.
				p.config.Durable.Publish(p.config.Writer.DurableEndLSN())
				_ = transaction.CleanupSpill()
				continue
			}
			frameBytes, err := p.config.Writer.AppendFrame(&transaction)
			if err != nil {
				if transaction.Spill != nil {
					_ = transaction.Spill.closeKeepFile()
				}
				return err
			}
			unsyncedBytes += frameBytes
			if unsyncedBytes >= p.config.SyncBytes {
				if err := sync(); err != nil {
					return err
				}
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(p.config.SyncInterval)
			}
		case <-timer.C:
			if err := sync(); err != nil {
				return err
			}
			timer.Reset(p.config.SyncInterval)
		}
	}
}
