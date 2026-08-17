package cdcbench

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"golang.org/x/sync/errgroup"
)

type maintenanceResult struct {
	duration       time.Duration
	indexDuration  time.Duration
	vacuumDuration time.Duration
}

type indexJob struct {
	table string
	sql   string
}

var benchmarkIndexes = []indexJob{
	{"hot", "CREATE INDEX CONCURRENTLY hot_payload_idx ON pgmigrate_bench.hot (payload)"},
	{"maintenance", "CREATE INDEX CONCURRENTLY maintenance_payload_idx ON pgmigrate_bench.maintenance (payload)"},
	{"hot", "CREATE INDEX CONCURRENTLY hot_revision_idx ON pgmigrate_bench.hot (revision)"},
	{"maintenance", "CREATE INDEX CONCURRENTLY maintenance_category_idx ON pgmigrate_bench.maintenance (category)"},
	{"hot", "CREATE INDEX CONCURRENTLY hot_touched_idx ON pgmigrate_bench.hot (touched)"},
	{"maintenance", "CREATE INDEX CONCURRENTLY maintenance_amount_idx ON pgmigrate_bench.maintenance (amount)"},
	{"hot", "CREATE INDEX CONCURRENTLY hot_bucket_payload_idx ON pgmigrate_bench.hot (bucket, payload)"},
	{"maintenance", "CREATE INDEX CONCURRENTLY maintenance_category_payload_idx ON pgmigrate_bench.maintenance (category, payload)"},
}

func runMaintenance(ctx context.Context, targetURI string, indexWorkers int) (maintenanceResult, error) {
	started := time.Now()
	var result maintenanceResult
	locks := map[string]*sync.Mutex{
		"hot":         {},
		"maintenance": {},
	}
	indexStarted := time.Now()
	if err := buildBenchmarkIndexes(ctx, targetURI, indexWorkers, locks); err != nil {
		return maintenanceResult{}, err
	}
	result.indexDuration = time.Since(indexStarted)
	vacuumStarted := time.Now()
	if err := vacuumBenchmarkTables(ctx, targetURI, locks); err != nil {
		return maintenanceResult{}, err
	}
	result.vacuumDuration = time.Since(vacuumStarted)
	result.duration = time.Since(started)
	return result, nil
}

func buildBenchmarkIndexes(
	ctx context.Context,
	targetURI string,
	workers int,
	locks map[string]*sync.Mutex,
) error {
	jobs := make(chan indexJob)
	group, groupCtx := errgroup.WithContext(ctx)
	for range min(workers, len(benchmarkIndexes)) {
		group.Go(func() error {
			conn, err := postgres.Connect(groupCtx, targetURI)
			if err != nil {
				return fmt.Errorf("connect index worker: %w", err)
			}
			defer conn.Close(context.Background())
			for job := range jobs {
				lock := locks[job.table]
				lock.Lock()
				_, err := conn.Exec(groupCtx, job.sql)
				lock.Unlock()
				if err != nil {
					return fmt.Errorf("build benchmark index on %s: %w", job.table, err)
				}
			}
			return nil
		})
	}
	group.Go(func() error {
		defer close(jobs)
		for _, job := range benchmarkIndexes {
			select {
			case jobs <- job:
			case <-groupCtx.Done():
				return groupCtx.Err()
			}
		}
		return nil
	})
	return group.Wait()
}

func vacuumBenchmarkTables(
	ctx context.Context,
	targetURI string,
	locks map[string]*sync.Mutex,
) error {
	group, groupCtx := errgroup.WithContext(ctx)
	for _, table := range []string{"hot", "maintenance"} {
		table := table
		group.Go(func() error {
			conn, err := postgres.Connect(groupCtx, targetURI)
			if err != nil {
				return fmt.Errorf("connect vacuum worker: %w", err)
			}
			defer conn.Close(context.Background())
			lock := locks[table]
			lock.Lock()
			if _, err := conn.Exec(
				groupCtx,
				"VACUUM (ANALYZE) pgmigrate_bench."+table,
			); err != nil {
				lock.Unlock()
				return fmt.Errorf("vacuum benchmark table %s: %w", table, err)
			}
			lock.Unlock()
			return nil
		})
	}
	return group.Wait()
}
