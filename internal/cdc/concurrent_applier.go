package cdc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pgx/v5"
)

// runConcurrentConnection drains each durable reader snapshot through a pool
// of target sessions. A snapshot is finite; after it is drained the outer loop
// refreshes the reader or waits for the persister to publish more WAL.
func (a *Applier) runConcurrentConnection(
	ctx context.Context,
	pool *applyWorkerPool,
	reader *Reader,
	progress LSN,
) error {
	for {
		if err := reader.Refresh(a.config.Durable.Load()); err != nil {
			return err
		}
		if a.config.EndPosition != nil {
			end, set, err := a.effectiveEndPosition(ctx)
			if err != nil {
				return err
			}
			if set && progress >= end {
				return nil
			}
		}
		applied, next, err := a.applyConcurrentAvailable(ctx, pool, reader, progress)
		if err != nil {
			return err
		}
		if applied {
			progress = next
			continue
		}
		timer := time.NewTimer(a.config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type replayJob struct {
	transactions []Transaction
	relations    []uint32
	payloadBytes uint64
	waiting      int
	dependents   []*replayJob
	committed    bool
	submitted    bool
	sealed       bool
}

const maxReplayBatchBytes = uint64(16 << 20)

func newReplayJob(transaction Transaction, payloadBytes uint64, batchSize int) *replayJob {
	seen := make(map[uint32]struct{}, len(transaction.Relations))
	relations := make([]uint32, 0, len(transaction.Relations))
	for _, relation := range transaction.Relations {
		if _, ok := seen[relation.OID]; ok {
			continue
		}
		seen[relation.OID] = struct{}{}
		relations = append(relations, relation.OID)
	}
	return &replayJob{
		transactions: []Transaction{transaction},
		relations:    relations,
		payloadBytes: payloadBytes,
		sealed:       batchSize == 1 || payloadBytes >= maxReplayBatchBytes,
	}
}

func (j *replayJob) append(transaction Transaction, payloadBytes uint64, batchSize int) bool {
	if j.submitted || j.sealed {
		return false
	}
	if len(j.transactions) >= batchSize || j.payloadBytes+payloadBytes > maxReplayBatchBytes {
		j.sealed = true
		return false
	}
	for _, relation := range transaction.Relations {
		found := false
		for _, existing := range j.relations {
			if relation.OID == existing {
				found = true
				break
			}
		}
		if !found {
			// The new table could have a different predecessor. Start another
			// job so the dependency graph, and its parallelism, remain exact.
			j.sealed = true
			return false
		}
	}
	j.transactions = append(j.transactions, transaction)
	j.payloadBytes += payloadBytes
	j.sealed = len(j.transactions) >= batchSize || j.payloadBytes >= maxReplayBatchBytes
	return true
}

func (j *replayJob) endLSN() LSN {
	return j.transactions[len(j.transactions)-1].EndLSN
}

func (j *replayJob) cleanupSpills() error {
	var result error
	for i := range j.transactions {
		result = errors.Join(result, j.transactions[i].CleanupSpill())
	}
	return result
}

// linkReplayJob records the immediately preceding transaction for every table.
// A multi-table transaction is released only after every unique predecessor
// commits, preventing a later change from observing an older table snapshot.
func linkReplayJob(job *replayJob, tails map[uint32]*replayJob) {
	predecessors := make(map[*replayJob]struct{}, len(job.relations))
	for _, relation := range job.relations {
		if predecessor := tails[relation]; predecessor != nil {
			// A fast independent lane can commit while the reader is still
			// discovering later source transactions. It is already a satisfied
			// dependency and will not emit another completion event.
			if !predecessor.committed {
				predecessors[predecessor] = struct{}{}
			}
		}
		tails[relation] = job
	}
	for predecessor := range predecessors {
		job.waiting++
		predecessor.dependents = append(predecessor.dependents, job)
	}
}

type applyWorkerEvent struct {
	job *replayJob
	err error
}

type applyWorkerPool struct {
	ctx      context.Context
	cancel   context.CancelFunc
	applier  *Applier
	jobs     chan *replayJob
	results  chan applyWorkerEvent
	progress *pgx.Conn
	extra    []*pgx.Conn
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func newApplyWorkerPool(
	ctx context.Context,
	applier *Applier,
	first *pgx.Conn,
) (*applyWorkerPool, error) {
	connections := make([]*pgx.Conn, 0, applier.config.Workers)
	for worker := 0; worker < applier.config.Workers; worker++ {
		conn, err := postgres.Connect(ctx, applier.config.ConnString)
		if err != nil {
			closeApplyConnections(connections)
			return nil, fmt.Errorf("cdc: connect applier worker %d: %w", worker+1, err)
		}
		if err := configureApplySession(ctx, conn); err != nil {
			conn.Close(context.Background())
			closeApplyConnections(connections)
			return nil, fmt.Errorf("cdc: configure applier worker %d: %w", worker+1, err)
		}
		connections = append(connections, conn)
	}

	poolCtx, cancel := context.WithCancel(ctx)
	pool := &applyWorkerPool{
		ctx:      poolCtx,
		cancel:   cancel,
		applier:  applier,
		jobs:     make(chan *replayJob, applier.config.Workers),
		results:  make(chan applyWorkerEvent, applier.config.Workers),
		progress: first,
		extra:    connections,
	}
	for _, conn := range connections {
		pool.wg.Add(1)
		go pool.runWorker(conn)
	}
	return pool, nil
}

func closeApplyConnections(connections []*pgx.Conn) {
	for _, conn := range connections {
		conn.Close(context.Background())
	}
}

func (p *applyWorkerPool) runWorker(conn *pgx.Conn) {
	defer p.wg.Done()
	relations := newTargetRelationCache()
	statements := newApplyStatementCache(applyStatementCacheCapacity)
	for {
		select {
		case <-p.ctx.Done():
			return
		case job := <-p.jobs:
			prepared, err := p.applier.prepareTransactions(
				p.ctx, conn, relations, statements, job.transactions,
			)
			if err == nil {
				err = p.applier.commitPreparedReplay(prepared, job.transactions)
			}
			if !p.send(applyWorkerEvent{job: job, err: err}) {
				_ = prepared.abort()
				return
			}
		}
	}
}

func (p *applyWorkerPool) send(event applyWorkerEvent) bool {
	select {
	case p.results <- event:
		return true
	case <-p.ctx.Done():
		return false
	}
}

func (p *applyWorkerPool) submit(job *replayJob) error {
	select {
	case p.jobs <- job:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *applyWorkerPool) stop() {
	p.stopOnce.Do(func() {
		p.cancel()
		p.wg.Wait()
		closeApplyConnections(p.extra)
	})
}

type replayReadEvent struct {
	transaction Transaction
	err         error
}

// startReplayReadPump keeps a large on-disk transaction from starving target
// completion events. Its single-item channel bounds read-ahead to one decoded
// transaction beyond the scheduler window.
func startReplayReadPump(
	ctx context.Context,
	reader *Reader,
) (<-chan replayReadEvent, context.CancelFunc, *sync.WaitGroup) {
	readCtx, cancel := context.WithCancel(ctx)
	events := make(chan replayReadEvent, 1)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(events)
		for {
			select {
			case <-readCtx.Done():
				return
			default:
			}
			transaction, err := reader.Next()
			select {
			case events <- replayReadEvent{transaction: transaction, err: err}:
			case <-readCtx.Done():
				_ = transaction.CleanupSpill()
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return events, cancel, wg
}

// applyConcurrentAvailable reads ahead by a bounded amount and commits a job
// as soon as every preceding transaction for its tables is durable. Each worker
// atomically records replay receipts with its DML. The coordinator checkpoints
// only the contiguous receipt prefix, so independent commits can finish out of
// source order without weakening crash recovery.
func (a *Applier) applyConcurrentAvailable(
	ctx context.Context,
	pool *applyWorkerPool,
	reader *Reader,
	progress LSN,
) (bool, LSN, error) {
	maxPending := a.config.Window
	compactAt := max(a.config.Workers*4, a.config.Workers)
	checkpointEvery := max(a.config.Workers*4, a.config.BatchSize)
	checkpointEvery = min(checkpointEvery, maxPending)
	jobs := make([]*replayJob, 0, min(maxPending, compactAt))
	front := 0
	active := 0
	pending := 0
	exhausted := false
	applied := false
	next := progress
	durableNext := progress
	tails := make(map[uint32]*replayJob)
	runnable := make([]*replayJob, 0, a.config.Workers)
	checkpointLSNs := make([]LSN, 0, checkpointEvery)
	receipts, err := loadStreamReplayReceipts(
		ctx, pool.progress, a.config.StreamID, a.config.StreamGeneration, progress,
	)
	if err != nil {
		return false, progress, fmt.Errorf("cdc: load durable replay receipts: %w", err)
	}
	receiptIndex := 0
	readEvents, cancelRead, readWG := startReplayReadPump(ctx, reader)

	stopReader := func() error {
		cancelRead()
		readWG.Wait()
		var result error
		for event := range readEvents {
			result = errors.Join(result, event.transaction.CleanupSpill())
		}
		return result
	}

	fail := func(err error) (bool, LSN, error) {
		pool.stop()
		err = errors.Join(err, stopReader())
		for i := front; i < len(jobs); i++ {
			err = errors.Join(err, jobs[i].cleanupSpills())
		}
		return applied, next, err
	}
	succeed := func() (bool, LSN, error) {
		return applied, next, stopReader()
	}

	sealLast := func() {
		if len(jobs) > front {
			jobs[len(jobs)-1].sealed = true
		}
	}

	checkpoint := func() error {
		if len(checkpointLSNs) == 0 {
			return nil
		}
		if err := checkpointStreamProgress(
			ctx, pool.progress, a.config.StreamID, a.config.StreamGeneration, durableNext,
		); err != nil {
			return fmt.Errorf("cdc: checkpoint replay progress: %w", err)
		}
		next = durableNext
		applied = true
		if a.config.AfterProgress != nil {
			for _, checkpointLSN := range checkpointLSNs {
				if err := a.config.AfterProgress(ctx, checkpointLSN); err != nil {
					return err
				}
			}
		}
		checkpointLSNs = checkpointLSNs[:0]
		return nil
	}

	advanceCommittedPrefix := func() error {
		for front < len(jobs) && jobs[front].committed {
			job := jobs[front]
			if err := job.cleanupSpills(); err != nil {
				return fmt.Errorf("cdc: cleanup applied reader spill: %w", err)
			}
			for i := range job.transactions {
				checkpointLSNs = append(checkpointLSNs, job.transactions[i].EndLSN)
			}
			durableNext = job.endLSN()
			pending -= len(job.transactions)
			for _, relation := range job.relations {
				if tails[relation] == job {
					delete(tails, relation)
				}
			}
			front++
			if front >= compactAt {
				remaining := copy(jobs, jobs[front:])
				clear(jobs[remaining:])
				jobs = jobs[:remaining]
				front = 0
			}
		}
		return nil
	}

	for {
		if exhausted || pending >= maxPending {
			sealLast()
		}
		for active < a.config.Workers {
			selected := -1
			for i, job := range runnable {
				if job.sealed {
					selected = i
					break
				}
			}
			if selected < 0 {
				break
			}
			job := runnable[selected]
			copy(runnable[selected:], runnable[selected+1:])
			runnable[len(runnable)-1] = nil
			runnable = runnable[:len(runnable)-1]
			if err := pool.submit(job); err != nil {
				return fail(err)
			}
			job.submitted = true
			active++
		}

		if err := advanceCommittedPrefix(); err != nil {
			return fail(err)
		}
		if len(checkpointLSNs) >= checkpointEvery || exhausted && front == len(jobs) {
			if err := checkpoint(); err != nil {
				return fail(err)
			}
		}
		if exhausted && front == len(jobs) && active == 0 {
			return succeed()
		}
		if active == 0 && exhausted && len(runnable) == 0 {
			return fail(errors.New("cdc: concurrent replay scheduler has pending work but no runnable transaction"))
		}

		var availableReads <-chan replayReadEvent
		if !exhausted && pending < maxPending {
			availableReads = readEvents
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case event, ok := <-availableReads:
			if !ok {
				exhausted = true
				continue
			}
			if errors.Is(event.err, io.EOF) {
				exhausted = true
				continue
			}
			if event.err != nil {
				return fail(event.err)
			}
			transaction := event.transaction
			if transaction.EndLSN <= progress {
				if err := transaction.CleanupSpill(); err != nil {
					return fail(fmt.Errorf("cdc: cleanup already-applied reader spill: %w", err))
				}
				continue
			}
			if a.config.EndPosition != nil {
				end, set, err := a.effectiveEndPosition(ctx)
				if err != nil {
					return fail(errors.Join(err, transaction.CleanupSpill()))
				}
				if set && transaction.EndLSN > end {
					if err := transaction.CleanupSpill(); err != nil {
						return fail(fmt.Errorf("cdc: cleanup post-boundary reader spill: %w", err))
					}
					exhausted = true
					continue
				}
			}

			for receiptIndex < len(receipts) && transaction.EndLSN > receipts[receiptIndex].last {
				receiptIndex++
			}
			recovered := receiptIndex < len(receipts) &&
				transaction.EndLSN >= receipts[receiptIndex].first
			if recovered {
				sealLast()
				for _, relation := range transaction.Relations {
					if tails[relation.OID] != nil {
						return fail(fmt.Errorf(
							"cdc: durable receipt %x precedes an unapplied transaction for table %d",
							transaction.EndLSN, relation.OID,
						))
					}
				}
				job := newReplayJob(transaction, 0, 1)
				job.committed = true
				job.submitted = true
				jobs = append(jobs, job)
				pending++
				continue
			}

			payloadBytes := uint64(0)
			if a.config.BatchSize > 1 {
				var err error
				payloadBytes, err = transactionPayloadSize(&transaction)
				if err != nil {
					return fail(errors.Join(err, transaction.CleanupSpill()))
				}
				if len(jobs) > front && jobs[len(jobs)-1].append(
					transaction, payloadBytes, a.config.BatchSize,
				) {
					pending++
					continue
				}
			}

			job := newReplayJob(transaction, payloadBytes, a.config.BatchSize)
			linkReplayJob(job, tails)
			jobs = append(jobs, job)
			pending++
			if job.waiting == 0 {
				runnable = append(runnable, job)
			}
		case event := <-pool.results:
			if event.err != nil {
				return fail(event.err)
			}
			active--
			event.job.committed = true
			for _, dependent := range event.job.dependents {
				dependent.waiting--
				if dependent.waiting == 0 {
					runnable = append(runnable, dependent)
				}
			}
		}
	}
}
