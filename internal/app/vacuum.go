package app

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/GetStream/pgmigrate/internal/tuning"
)

// vacuumStepPrefix namespaces the durable per-table vacuum steps.
const vacuumStepPrefix = "target.vacuum."

// vacuumTarget runs VACUUM (ANALYZE) over every copied table.
//
// This replaces the bare ANALYZE the index build used to run. A bulk load leaves
// the target with no statistics and a heap full of line pointers no one has
// tidied, and the ANALYZE alone addressed only the first. Verification does not
// depend on this — it reads pages in physical order and takes its page counts from
// pg_relation_size — but every query the application runs after cutover does.
//
// Each table is a durable step, so an interrupted run resumes at the table it was
// on rather than vacuuming the whole target again.
func vacuumTarget(
	ctx context.Context,
	cfg config.Config,
	store *state.Store,
	sessionGUCs map[string]string,
) error {
	tables, err := store.ListTables(ctx)
	if err != nil {
		return err
	}
	pending := make([]state.Table, 0, len(tables))
	for _, table := range tables {
		done, stepErr := store.StepCompleted(ctx, vacuumStepPrefix+qualifiedTable(table))
		if stepErr != nil {
			return stepErr
		}
		if !done {
			pending = append(pending, table)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	started := time.Now()
	logEvent(cfg.Dir, "vacuum_start", map[string]any{"tables": len(pending)})
	workers := max(cfg.Workers, 1)
	jobs := make(chan state.Table)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	for range min(workers, len(pending)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for table := range jobs {
				err := vacuumOne(runCtx, cfg, store, sessionGUCs, table)
				if err == nil {
					continue
				}
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
				cancel()
				return
			}
		}()
	}
	for _, table := range pending {
		select {
		case jobs <- table:
		case <-runCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	logEvent(cfg.Dir, "vacuum_done", map[string]any{
		"tables": len(pending), "duration": time.Since(started).String(),
	})
	return nil
}

// vacuumOne vacuums one table and records it, in that order: a step recorded
// before the work would let a crash between the two skip the table forever.
func vacuumOne(
	ctx context.Context,
	cfg config.Config,
	store *state.Store,
	sessionGUCs map[string]string,
	table state.Table,
) error {
	name := qualifiedTable(table)
	started := time.Now()
	conn, err := vacuumSession(ctx, cfg, sessionGUCs)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	// VACUUM cannot run inside a transaction block, so this is deliberately a
	// bare Exec on its own connection rather than part of any batch.
	if _, err := conn.Exec(ctx, "VACUUM (ANALYZE) "+identifier(table)); err != nil {
		return fmt.Errorf("vacuum %s: %w", name, err)
	}
	logEvent(cfg.Dir, "vacuum_table", map[string]any{
		"table": name, "duration": time.Since(started).String(),
	})
	return store.CompleteStep(ctx, vacuumStepPrefix+name, time.Since(started).String())
}

// vacuumSession opens a target connection carrying the tuned maintenance
// settings, which is what lets one vacuum use the memory and parallel workers the
// target was sized for.
func vacuumSession(ctx context.Context, cfg config.Config, sessionGUCs map[string]string) (*pgx.Conn, error) {
	conn, err := postgres.Connect(ctx, cfg.Target)
	if err != nil {
		return nil, err
	}
	if err := postgres.PinSearchPath(ctx, conn); err != nil {
		conn.Close(context.Background())
		return nil, err
	}
	names := make([]string, 0, len(sessionGUCs))
	for name := range sessionGUCs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := tuning.SetSession(ctx, conn, name, sessionGUCs[name]); err != nil {
			conn.Close(context.Background())
			return nil, err
		}
	}
	return conn, nil
}

func qualifiedTable(table state.Table) string { return table.Schema + "." + table.Name }

func identifier(table state.Table) string {
	return pgx.Identifier{table.Schema, table.Name}.Sanitize()
}
