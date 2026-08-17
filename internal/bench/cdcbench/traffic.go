package cdcbench

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"golang.org/x/sync/errgroup"
)

const updateBatchSQL = `
	UPDATE pgmigrate_bench.hot
	SET revision = revision + 1,
	    payload = md5(id::text || ':' || (revision + 1)::text),
	    touched = touched + 1,
	    bucket = ((revision + 1) % 4096)::integer
	WHERE id >= $1 AND id < $2
`

func runFixedTraffic(
	ctx context.Context,
	uri string,
	rows int64,
	batch, workers int,
	updates int64,
	committed *atomic.Int64,
) error {
	return runTraffic(ctx, uri, rows, batch, workers, updates, 0, committed)
}

func runRealtimeTraffic(
	ctx context.Context,
	uri string,
	rows int64,
	batch, workers, updatesPerSecond int,
	committed *atomic.Int64,
) error {
	if updatesPerSecond == 0 {
		<-ctx.Done()
		return nil
	}
	return runTraffic(ctx, uri, rows, batch, workers, 0, updatesPerSecond, committed)
}

func runTraffic(
	ctx context.Context,
	uri string,
	rows int64,
	batch, workers int,
	total int64,
	updatesPerSecond int,
	committed *atomic.Int64,
) error {
	// Stopping live traffic is graceful: stop scheduling new batches, then let
	// every batch already handed to PostgreSQL finish. Canceling an in-flight
	// autocommit query can commit server-side while returning context.Canceled
	// client-side, which would undercount generated changes and make the
	// benchmark's exact accounting check lie.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()
	stopWatch := context.AfterFunc(ctx, func() {
		time.AfterFunc(30*time.Second, cancelWorkers)
	})
	defer stopWatch()
	group, groupCtx := errgroup.WithContext(workerCtx)
	jobs := make(chan int64, workers*2)
	group.Go(func() error {
		defer close(jobs)
		cursor := int64(1)
		send := func() error {
			start := cursor
			cursor += int64(batch)
			if cursor > rows {
				cursor = 1
			}
			select {
			case jobs <- start:
				return nil
			case <-ctx.Done():
				return nil
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}
		if updatesPerSecond == 0 {
			for remaining := total; remaining > 0; remaining -= int64(batch) {
				if err := send(); err != nil {
					return err
				}
			}
			return nil
		}
		interval := time.Duration(float64(time.Second) * float64(batch) / float64(updatesPerSecond))
		if interval < time.Microsecond {
			interval = time.Microsecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := send(); err != nil {
					return err
				}
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}
	})
	for range workers {
		group.Go(func() error {
			conn, err := postgres.Connect(groupCtx, uri)
			if err != nil {
				return fmt.Errorf("connect traffic worker: %w", err)
			}
			defer conn.Close(context.Background())
			for start := range jobs {
				tag, err := conn.Exec(groupCtx, updateBatchSQL, start, start+int64(batch))
				if err != nil {
					return fmt.Errorf("execute source update batch: %w", err)
				}
				if got := tag.RowsAffected(); got != int64(batch) {
					return fmt.Errorf("source update batch affected %d rows, want %d", got, batch)
				}
				committed.Add(int64(batch))
			}
			return nil
		})
	}
	err := group.Wait()
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return nil
	}
	return err
}
