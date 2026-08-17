package cdcbench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GetStream/pgmigrate/internal/cdc"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"golang.org/x/sync/errgroup"
)

type updateCounterSampler struct {
	updates atomic.Int64
}

func (s *updateCounterSampler) Observe(sample cdc.KeySample) {
	if sample.Schema == benchSchema && sample.Table == benchTable && sample.Kind == cdc.ChangeUpdate {
		s.updates.Add(1)
	}
}

type workerResult struct {
	name string
	err  error
}

type counterSnapshot struct {
	at        time.Time
	applied   int64
	generated int64
	durable   cdc.LSN
}

type maintenanceOutcome struct {
	result maintenanceResult
	start  counterSnapshot
	end    counterSnapshot
	err    error
}

// Run executes one end-to-end benchmark in two disposable PostgreSQL containers.
func Run(ctx context.Context, cfg Config) (result Result, runErr error) {
	if err := cfg.validate(); err != nil {
		return Result{}, err
	}
	if cfg.Output == nil {
		cfg.Output = io.Discard
	}
	workDir := cfg.WorkDir
	removeWorkDir := false
	if workDir == "" {
		var err error
		workDir, err = os.MkdirTemp("", "pgmigrate-cdc-bench-")
		if err != nil {
			return Result{}, err
		}
		removeWorkDir = true
	} else {
		if err := os.MkdirAll(workDir, 0o700); err != nil {
			return Result{}, err
		}
		entries, err := os.ReadDir(workDir)
		if err != nil {
			return Result{}, err
		}
		if len(entries) != 0 {
			return Result{}, fmt.Errorf("work directory %s is not empty", workDir)
		}
	}
	if removeWorkDir {
		defer os.RemoveAll(workDir)
	}
	cdcDir := filepath.Join(workDir, "cdc")
	if err := os.MkdirAll(filepath.Join(cdcDir, "spill"), 0o700); err != nil {
		return Result{}, err
	}

	logf(cfg.Output, "starting PostgreSQL %d source and target", cfg.PostgresMajor)
	var source, target *postgresInstance
	startGroup, startCtx := errgroup.WithContext(ctx)
	startGroup.Go(func() error {
		var err error
		source, err = startPostgres(startCtx, cfg.PostgresMajor)
		return err
	})
	startGroup.Go(func() error {
		var err error
		target, err = startPostgres(startCtx, cfg.PostgresMajor)
		return err
	})
	if err := startGroup.Wait(); err != nil {
		cleanupErr := error(nil)
		if source != nil {
			cleanupErr = errors.Join(cleanupErr, source.close())
		}
		if target != nil {
			cleanupErr = errors.Join(cleanupErr, target.close())
		}
		return Result{}, errors.Join(err, cleanupErr)
	}
	defer func() {
		runErr = errors.Join(runErr, source.close(), target.close())
	}()

	logf(cfg.Output, "seeding %d rows per benchmark table", cfg.Rows)
	startLSN, err := setupFixture(ctx, source.URI, target.URI, cfg.Rows)
	if err != nil {
		return Result{}, err
	}
	writer, recovery, err := cdc.OpenWriter(cdc.WriterConfig{Directory: cdcDir})
	if err != nil {
		return Result{}, err
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			runErr = errors.Join(runErr, writer.Close())
		}
	}()
	durable := new(cdc.DurableWatermark)
	durable.Publish(recovery.DurableLSN)
	transactions := make(chan cdc.Transaction, max(64, cfg.TrafficWorkers*8))
	persister, err := cdc.NewPersister(cdc.PersisterConfig{
		Writer: writer, Transactions: transactions, Durable: durable,
	})
	if err != nil {
		return Result{}, err
	}
	receiver, err := cdc.NewReceiver(cdc.ReceiverConfig{
		ConnString: source.URI, Slot: benchSlot, Publication: benchPublication,
		StartLSN: startLSN, Transactions: transactions, Durable: durable,
		FeedbackInterval: 100 * time.Millisecond,
		Backpressure:     30 * time.Second,
		SpillDirectory:   filepath.Join(cdcDir, "spill"),
	})
	if err != nil {
		return Result{}, err
	}

	pipelineCtx, stopPipeline := context.WithCancel(ctx)
	defer stopPipeline()
	workers := make(chan workerResult, 4)
	var pipelineWorkers sync.WaitGroup
	startPipelineWorker := func(name string, run func() error) {
		pipelineWorkers.Add(1)
		go func() {
			defer pipelineWorkers.Done()
			workers <- workerResult{name, run()}
		}()
	}
	startPipelineWorker("receiver", func() error { return receiver.Run(pipelineCtx) })
	startPipelineWorker("persister", func() error { return persister.Run(pipelineCtx) })
	defer func() {
		stopPipeline()
		runErr = errors.Join(runErr, waitForWorkers("CDC pipeline", &pipelineWorkers, 30*time.Second))
	}()

	var generated atomic.Int64
	logf(cfg.Output, "staging %d update changes before replay", cfg.BacklogUpdates)
	if err := runFixedTraffic(
		ctx, source.URI, cfg.Rows, cfg.UpdateBatch, cfg.TrafficWorkers,
		cfg.BacklogUpdates, &generated,
	); err != nil {
		return Result{}, err
	}
	prefillLSN, err := emitBoundaryLSN(ctx, source.URI)
	if err != nil {
		return Result{}, err
	}
	if err := waitForDurable(ctx, durable, prefillLSN, workers); err != nil {
		return Result{}, err
	}

	pruner, err := cdc.NewSegmentPruner(cdc.SegmentPrunerConfig{
		Directory: cdcDir, Interval: time.Second, Catalog: writer.SegmentCatalog(),
	})
	if err != nil {
		return Result{}, err
	}
	sampler := new(updateCounterSampler)
	var appliedAtBoundary atomic.Int64
	applier, err := cdc.NewApplier(cdc.ApplierConfig{
		ConnString: target.URI, Directory: cdcDir,
		ReaderSpillDirectory: filepath.Join(cdcDir, "spill"),
		StreamID:             benchSlot, StreamGeneration: benchGeneration,
		FreshSetup: true, TargetHasCopiedData: true,
		Durable: durable, PollInterval: 5 * time.Millisecond,
		AfterProgress: func(ctx context.Context, lsn cdc.LSN) error {
			// The sampler flushes a whole source transaction before this callback.
			// Publishing its count here keeps benchmark snapshots from splitting
			// one transaction's row samples across a measurement boundary.
			appliedAtBoundary.Store(sampler.updates.Load())
			return pruner.OnProgress(ctx, lsn)
		},
		Sampler: sampler,
	})
	if err != nil {
		return Result{}, err
	}
	startPipelineWorker("applier", func() error { return applier.Run(pipelineCtx) })
	startPipelineWorker("pruner", func() error { return pruner.Run(pipelineCtx) })

	trafficCtx, stopTraffic := context.WithCancel(ctx)
	defer stopTraffic()
	trafficDone := make(chan error, 1)
	var trafficWorker sync.WaitGroup
	trafficWorker.Add(1)
	go func() {
		defer trafficWorker.Done()
		trafficDone <- runRealtimeTraffic(
			trafficCtx, source.URI, cfg.Rows, cfg.UpdateBatch, cfg.TrafficWorkers,
			cfg.RealtimeUpdates, &generated,
		)
	}()
	defer func() {
		stopTraffic()
		runErr = errors.Join(runErr, waitForWorkers("source traffic", &trafficWorker, 30*time.Second))
	}()

	logf(cfg.Output, "warming replay for %s", cfg.Warmup)
	if err := waitHealthy(ctx, cfg.Warmup, workers, trafficDone); err != nil {
		stopTraffic()
		return Result{}, err
	}

	maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
	defer stopMaintenance()
	capture := func() counterSnapshot {
		return counterSnapshot{
			at: time.Now(), applied: appliedAtBoundary.Load(),
			generated: generated.Load(), durable: durable.Load(),
		}
	}
	maintenanceDone := make(chan maintenanceOutcome, 1)
	var maintenanceWorker sync.WaitGroup
	var measureStart counterSnapshot
	if cfg.Maintenance {
		logf(cfg.Output, "starting concurrent indexes followed by VACUUM (ANALYZE)")
		maintenanceStarted := make(chan counterSnapshot, 1)
		maintenanceWorker.Add(1)
		go func() {
			defer maintenanceWorker.Done()
			start := capture()
			maintenanceStarted <- start
			value, err := runMaintenance(maintenanceCtx, target.URI, cfg.IndexWorkers)
			maintenanceDone <- maintenanceOutcome{
				result: value, start: start, end: capture(), err: err,
			}
		}()
		defer func() {
			stopMaintenance()
			runErr = errors.Join(runErr, waitForWorkers("target maintenance", &maintenanceWorker, 30*time.Second))
		}()
		select {
		case measureStart = <-maintenanceStarted:
		case worker := <-workers:
			stopTraffic()
			return Result{}, unexpectedWorker(worker)
		case err := <-trafficDone:
			stopTraffic()
			if err == nil {
				err = errors.New("realtime traffic stopped unexpectedly")
			}
			return Result{}, err
		case <-ctx.Done():
			stopTraffic()
			return Result{}, ctx.Err()
		}
	} else {
		measureStart = capture()
	}

	measureStarted := measureStart.at
	appliedStarted := measureStart.applied
	generatedStarted := measureStart.generated
	maintenanceActive := cfg.Maintenance
	var maintenance maintenanceResult
	var overlapApplied int64
	var overlapGenerated int64
	var overlapDuration time.Duration
	var overlapBacklog bool
	lastBucketAt := measureStarted
	lastBucketApplied := appliedStarted
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	timer := time.NewTimer(cfg.Duration)
	defer timer.Stop()
	var maintenanceChannel <-chan maintenanceOutcome
	if cfg.Maintenance {
		maintenanceChannel = maintenanceDone
	}
measure:
	for {
		select {
		case <-ctx.Done():
			stopTraffic()
			return Result{}, ctx.Err()
		case value := <-maintenanceChannel:
			maintenanceChannel = nil
			if value.err != nil {
				stopTraffic()
				return Result{}, value.err
			}
			maintenance = value.result
			if maintenanceActive {
				maintenanceActive = false
				overlapDuration = value.end.at.Sub(value.start.at)
				overlapApplied = value.end.applied - value.start.applied
				overlapGenerated = value.end.generated - value.start.generated
				overlapBacklog, err = replayBacklogBefore(
					ctx, target.URI, value.end.durable,
				)
				if err != nil {
					stopTraffic()
					return Result{}, err
				}
				logf(cfg.Output, "maintenance finished in %s", maintenance.duration.Round(time.Millisecond))
			}
		case worker := <-workers:
			stopTraffic()
			return Result{}, unexpectedWorker(worker)
		case err := <-trafficDone:
			stopTraffic()
			if err == nil {
				err = errors.New("realtime traffic stopped unexpectedly")
			}
			return Result{}, err
		case now := <-ticker.C:
			applied := sampler.updates.Load()
			elapsed := now.Sub(lastBucketAt)
			bucket := Bucket{
				StartedAt:         lastBucketAt.Sub(measureStarted),
				Duration:          elapsed,
				AppliedUpdates:    applied - lastBucketApplied,
				UpdatesPerSecond:  rate(applied-lastBucketApplied, elapsed),
				MaintenanceActive: maintenanceActive,
			}
			result.Buckets = append(result.Buckets, bucket)
			logf(cfg.Output, "apply %.0f updates/s maintenance=%t", bucket.UpdatesPerSecond, maintenanceActive)
			lastBucketAt, lastBucketApplied = now, applied
		case <-timer.C:
			break measure
		}
	}
	measureEnd := capture()
	appliedEnded := measureEnd.applied
	generatedEnded := measureEnd.generated
	measureEnded := measureEnd.at
	if maintenanceActive {
		overlapDuration = measureEnd.at.Sub(measureStart.at)
		overlapApplied = measureEnd.applied - measureStart.applied
		overlapGenerated = measureEnd.generated - measureStart.generated
		overlapBacklog, err = replayBacklogBefore(ctx, target.URI, measureEnd.durable)
		if err != nil {
			stopTraffic()
			return Result{}, err
		}
	} else if !cfg.Maintenance {
		overlapBacklog, err = replayBacklogBefore(ctx, target.URI, measureEnd.durable)
		if err != nil {
			stopTraffic()
			return Result{}, err
		}
	}
	result.PostgresMajor = cfg.PostgresMajor
	result.Rows = cfg.Rows
	result.UpdateBatch = cfg.UpdateBatch
	result.TrafficWorkers = cfg.TrafficWorkers
	result.RequestedRealtimeRate = cfg.RealtimeUpdates
	result.Warmup = cfg.Warmup
	result.Duration = cfg.Duration
	result.IndexWorkers = cfg.IndexWorkers
	result.Maintenance = cfg.Maintenance
	result.MinimumApplyRate = cfg.MinUpdatesPerSecond
	result.BacklogUpdates = cfg.BacklogUpdates
	result.MeasuredAppliedUpdates = appliedEnded - appliedStarted
	result.MeasuredSourceUpdates = generatedEnded - generatedStarted
	result.AppliedUpdatesPerSecond = rate(result.MeasuredAppliedUpdates, measureEnded.Sub(measureStarted))
	result.SourceUpdatesPerSecond = rate(result.MeasuredSourceUpdates, measureEnded.Sub(measureStarted))
	result.NetDrainUpdatesPerSecond = result.AppliedUpdatesPerSecond - result.SourceUpdatesPerSecond
	result.OverlapAppliedUpdates = overlapApplied
	result.OverlapSourceUpdates = overlapGenerated
	result.OverlapDuration = overlapDuration
	result.OverlapUpdatesPerSecond = rate(overlapApplied, overlapDuration)
	result.OverlapSourcePerSecond = rate(overlapGenerated, overlapDuration)
	result.BacklogAtOverlapEnd = overlapBacklog

	stopTraffic()
	if err := <-trafficDone; err != nil && !errors.Is(err, context.Canceled) {
		return Result{}, err
	}
	logf(cfg.Output, "traffic stopped; draining durable CDC")
	stagedLSN, err := emitBoundaryLSN(ctx, source.URI)
	if err != nil {
		return Result{}, err
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, cfg.DrainTimeout)
	defer cancelDrain()
	if err := waitForDurable(drainCtx, durable, stagedLSN, workers); err != nil {
		return Result{}, err
	}
	boundary := durable.Load()
	drainStarted := time.Now()
	caughtUp := make(chan error, 1)
	go func() { caughtUp <- applier.WaitUntil(drainCtx, boundary) }()
	select {
	case err := <-caughtUp:
		if err != nil {
			return Result{}, err
		}
	case worker := <-workers:
		return Result{}, unexpectedWorker(worker)
	case <-drainCtx.Done():
		return Result{}, drainCtx.Err()
	}
	result.DrainDuration = time.Since(drainStarted)

	if maintenanceChannel != nil {
		select {
		case value := <-maintenanceChannel:
			if value.err != nil {
				return Result{}, value.err
			}
			maintenance = value.result
		case worker := <-workers:
			return Result{}, unexpectedWorker(worker)
		case <-drainCtx.Done():
			return Result{}, drainCtx.Err()
		}
	}
	result.MaintenanceDuration = maintenance.duration
	result.IndexDuration = maintenance.indexDuration
	result.VacuumDuration = maintenance.vacuumDuration

	stopPipeline()
	for range 4 {
		worker := <-workers
		if worker.err != nil && !errors.Is(worker.err, context.Canceled) {
			return Result{}, fmt.Errorf("%s failed during shutdown: %w", worker.name, worker.err)
		}
	}
	// Progress commits before the applier publishes its row samples. Joining the
	// applier closes that small window before exact accounting is captured.
	result.TotalGeneratedUpdates = generated.Load()
	result.TotalAppliedUpdates = sampler.updates.Load()
	if err := writer.Close(); err != nil {
		return Result{}, err
	}
	writerClosed = true
	if err := verifyFixture(
		ctx, source.URI, target.URI,
		result.TotalGeneratedUpdates, result.TotalAppliedUpdates,
	); err != nil {
		return result, err
	}

	gatedRate := result.AppliedUpdatesPerSecond
	if cfg.Maintenance {
		if result.OverlapDuration <= 0 {
			return Result{}, errors.New("maintenance completed before overlap could be measured")
		}
		gatedRate = result.OverlapUpdatesPerSecond
	}
	if !result.BacklogAtOverlapEnd {
		window := "measurement"
		if cfg.Maintenance {
			window = "target maintenance"
		}
		return result, fmt.Errorf("CDC backlog drained before %s ended", window)
	}
	sustainedSourceRate := result.SourceUpdatesPerSecond
	if cfg.Maintenance {
		sustainedSourceRate = result.OverlapSourcePerSecond
	}
	if cfg.RealtimeUpdates > 0 &&
		sustainedSourceRate < float64(cfg.RealtimeUpdates)*0.95 {
		return result, fmt.Errorf(
			"source sustained only %.0f of requested %d updates/s",
			sustainedSourceRate, cfg.RealtimeUpdates,
		)
	}
	if cfg.MinUpdatesPerSecond > 0 && gatedRate < cfg.MinUpdatesPerSecond {
		return result, fmt.Errorf(
			"CDC apply %.0f updates/s is below required %.0f updates/s",
			gatedRate, cfg.MinUpdatesPerSecond,
		)
	}
	comparedSourceRate := result.SourceUpdatesPerSecond
	if cfg.Maintenance {
		comparedSourceRate = result.OverlapSourcePerSecond
	}
	if cfg.RealtimeUpdates > 0 && gatedRate <= comparedSourceRate {
		return result, fmt.Errorf(
			"CDC apply %.0f updates/s did not outrun source traffic %.0f updates/s",
			gatedRate, comparedSourceRate,
		)
	}
	return result, nil
}

func emitBoundaryLSN(ctx context.Context, uri string) (cdc.LSN, error) {
	conn, err := postgres.Connect(ctx, uri)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	var value string
	if err := conn.QueryRow(
		ctx,
		"SELECT pg_logical_emit_message(true, 'pgmigrate_bench_boundary', '')::text",
	).Scan(&value); err != nil {
		return 0, err
	}
	lsn, err := pglogrepl.ParseLSN(value)
	return cdc.LSN(lsn), err
}

func replayBacklogBefore(ctx context.Context, targetURI string, boundary cdc.LSN) (bool, error) {
	conn, err := postgres.Connect(ctx, targetURI)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	progress, exists, err := postgres.ReadProgress(ctx, conn, benchSlot)
	if err != nil {
		return false, err
	}
	return !exists || cdc.LSN(progress) < boundary, nil
}

func waitForDurable(
	ctx context.Context,
	durable *cdc.DurableWatermark,
	want cdc.LSN,
	workers <-chan workerResult,
) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for durable.Load() < want {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case worker := <-workers:
			return unexpectedWorker(worker)
		case <-ticker.C:
		}
	}
	return nil
}

func waitHealthy(
	ctx context.Context,
	duration time.Duration,
	workers <-chan workerResult,
	traffic <-chan error,
) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case worker := <-workers:
		return unexpectedWorker(worker)
	case err := <-traffic:
		if err == nil {
			err = errors.New("realtime traffic stopped unexpectedly")
		}
		return err
	case <-timer.C:
		return nil
	}
}

func unexpectedWorker(worker workerResult) error {
	if worker.err == nil {
		return fmt.Errorf("%s stopped unexpectedly", worker.name)
	}
	return fmt.Errorf("%s stopped unexpectedly: %w", worker.name, worker.err)
}

func rate(count int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func waitForWorkers(name string, workers *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("timed out stopping %s", name)
	}
}

func logf(output io.Writer, format string, args ...any) {
	fmt.Fprintf(output, "[cdc-bench] "+format+"\n", args...)
}
