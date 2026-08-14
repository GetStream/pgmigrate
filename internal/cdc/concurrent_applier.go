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
	prepared     bool
	committing   bool
	submitted    bool
	sealed       bool
	full         bool
	commit       chan struct{}
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
		full:         batchSize > 1 && payloadBytes >= maxReplayBatchBytes,
		commit:       make(chan struct{}),
	}
}

func (j *replayJob) append(transaction Transaction, payloadBytes uint64, batchSize int) bool {
	if j.submitted || j.sealed {
		return false
	}
	if len(j.transactions) >= batchSize || j.payloadBytes+payloadBytes > maxReplayBatchBytes {
		j.sealed = true
		j.full = true
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
	j.full = len(j.transactions) >= batchSize || j.payloadBytes >= maxReplayBatchBytes
	j.sealed = j.full
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
			predecessors[predecessor] = struct{}{}
		}
		tails[relation] = job
	}
	for predecessor := range predecessors {
		job.waiting++
		predecessor.dependents = append(predecessor.dependents, job)
	}
}

const (
	workerPrepared = iota + 1
	workerCommitted
)

type applyWorkerEvent struct {
	job   *replayJob
	phase int
	err   error
}

type applyWorkerPool struct {
	ctx      context.Context
	cancel   context.CancelFunc
	applier  *Applier
	jobs     chan *replayJob
	results  chan applyWorkerEvent
	extra    []*pgx.Conn
	wg       sync.WaitGroup
	stopOnce sync.Once
}

func newApplyWorkerPool(
	ctx context.Context,
	applier *Applier,
	first *pgx.Conn,
) (*applyWorkerPool, error) {
	connections := make([]*pgx.Conn, 1, applier.config.Workers)
	connections[0] = first
	for worker := 1; worker < applier.config.Workers; worker++ {
		conn, err := postgres.Connect(ctx, applier.config.ConnString)
		if err != nil {
			closeApplyConnections(connections[1:])
			return nil, fmt.Errorf("cdc: connect applier worker %d: %w", worker+1, err)
		}
		if err := configureApplySession(ctx, conn); err != nil {
			conn.Close(context.Background())
			closeApplyConnections(connections[1:])
			return nil, fmt.Errorf("cdc: configure applier worker %d: %w", worker+1, err)
		}
		connections = append(connections, conn)
	}

	poolCtx, cancel := context.WithCancel(ctx)
	pool := &applyWorkerPool{
		ctx:     poolCtx,
		cancel:  cancel,
		applier: applier,
		jobs:    make(chan *replayJob, applier.config.Workers),
		results: make(chan applyWorkerEvent, applier.config.Workers*2),
		extra:   connections[1:],
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
			if !p.send(applyWorkerEvent{job: job, phase: workerPrepared, err: err}) {
				_ = prepared.abort()
				return
			}
			if err != nil {
				continue
			}
			select {
			case <-p.ctx.Done():
				_ = prepared.abort()
				return
			case <-job.commit:
			}
			err = p.applier.commitPreparedTransaction(prepared, job.endLSN())
			if !p.send(applyWorkerEvent{job: job, phase: workerCommitted, err: err}) {
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

// applyConcurrentAvailable reads ahead by a bounded amount, runs transactions
// as soon as all preceding transactions for their tables have committed, and
// grants commit permission strictly in source order. Progress is updated in the
// same target transaction as its data, preserving crash-safe exactly-once replay.
func (a *Applier) applyConcurrentAvailable(
	ctx context.Context,
	pool *applyWorkerPool,
	reader *Reader,
	progress LSN,
) (bool, LSN, error) {
	maxPending := a.config.Window
	compactAt := max(a.config.Workers*4, a.config.Workers)
	jobs := make([]*replayJob, 0, min(maxPending, compactAt))
	front := 0
	active := 0
	pending := 0
	exhausted := false
	applied := false
	next := progress
	tails := make(map[uint32]*replayJob)
	runnable := make([]*replayJob, 0, a.config.Workers)

	fail := func(err error) (bool, LSN, error) {
		pool.stop()
		for i := front; i < len(jobs); i++ {
			err = errors.Join(err, jobs[i].cleanupSpills())
		}
		return applied, next, err
	}

	for {
		// Start target work as soon as every idle worker has a runnable job.
		// Keep scanning to maxPending only when table dependencies hide parallel
		// work behind transactions that cannot run yet.
		for !exhausted && pending < maxPending &&
			active+len(runnable) < a.config.Workers && !fullRunnable(runnable) {
			transaction, err := reader.Next()
			if errors.Is(err, io.EOF) {
				exhausted = true
				break
			}
			if err != nil {
				return fail(err)
			}
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
					break
				}
			}

			payloadBytes := uint64(0)
			if a.config.BatchSize > 1 {
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
		}

		for active < a.config.Workers && len(runnable) != 0 {
			job := runnable[0]
			runnable[0] = nil
			runnable = runnable[1:]
			if len(runnable) == 0 {
				runnable = nil
			}
			if err := pool.submit(job); err != nil {
				return fail(err)
			}
			job.submitted = true
			active++
		}

		if front < len(jobs) && jobs[front].prepared && !jobs[front].committing {
			jobs[front].committing = true
			close(jobs[front].commit)
		}
		if exhausted && front == len(jobs) {
			return applied, next, nil
		}
		if active == 0 {
			return fail(errors.New("cdc: concurrent replay scheduler has pending work but no runnable transaction"))
		}

		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case event := <-pool.results:
			switch event.phase {
			case workerPrepared:
				if event.err != nil {
					return fail(event.err)
				}
				event.job.prepared = true

			case workerCommitted:
				if event.job != jobs[front] {
					return fail(fmt.Errorf(
						"cdc: transaction %x committed ahead of %x",
						event.job.endLSN(), jobs[front].endLSN(),
					))
				}
				if event.err != nil {
					return fail(event.err)
				}
				active--
				job := jobs[front]
				if err := job.cleanupSpills(); err != nil {
					return fail(fmt.Errorf("cdc: cleanup applied reader spill: %w", err))
				}
				next = job.endLSN()
				pending -= len(job.transactions)
				applied = true
				if a.config.AfterProgress != nil {
					if err := a.config.AfterProgress(ctx, next); err != nil {
						return fail(err)
					}
				}
				for _, relation := range job.relations {
					if tails[relation] == job {
						delete(tails, relation)
					}
				}
				for _, dependent := range job.dependents {
					dependent.waiting--
					if dependent.waiting == 0 {
						runnable = append(runnable, dependent)
					}
				}
				front++
				// A durable backlog can contain millions of transactions. Keep the
				// scheduler window bounded instead of retaining every committed job
				// in the slice backing array until the reader reaches EOF.
				if front >= compactAt {
					remaining := copy(jobs, jobs[front:])
					clear(jobs[remaining:])
					jobs = jobs[:remaining]
					front = 0
				}

			default:
				return fail(fmt.Errorf("cdc: unknown apply worker event %d", event.phase))
			}
		}
	}
}

func fullRunnable(runnable []*replayJob) bool {
	for _, job := range runnable {
		if job.full {
			return true
		}
	}
	return false
}
