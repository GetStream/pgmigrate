package cdc

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

type applyWorker struct {
	conn       *pgx.Conn
	statements *applyStatementCache
}

func openApplyWorkers(
	ctx context.Context,
	primary *pgx.Conn,
	primaryStatements *applyStatementCache,
	connString string,
	count int,
) ([]*applyWorker, error) {
	if primary == nil || count < 1 {
		return nil, errors.New("cdc: primary apply connection and positive worker count are required")
	}
	workers := make([]*applyWorker, 0, count)
	workers = append(workers, &applyWorker{conn: primary, statements: primaryStatements})
	for len(workers) < count {
		conn, err := postgres.Connect(ctx, connString)
		if err != nil {
			closeApplyWorkers(workers[1:])
			return nil, fmt.Errorf("cdc: connect replay worker %d: %w", len(workers), err)
		}
		if err := configureApplySession(ctx, conn); err != nil {
			conn.Close(context.Background())
			closeApplyWorkers(workers[1:])
			return nil, fmt.Errorf("cdc: configure replay worker %d: %w", len(workers), err)
		}
		if _, err := conn.Exec(
			ctx, "SELECT set_config('application_name', $1, false)",
			fmt.Sprintf("pgmigrate-replay-%d", len(workers)),
		); err != nil {
			conn.Close(context.Background())
			closeApplyWorkers(workers[1:])
			return nil, fmt.Errorf("cdc: name replay worker %d: %w", len(workers), err)
		}
		workers = append(workers, &applyWorker{
			conn: conn, statements: newApplyStatementCache(applyStatementCacheCapacity),
		})
	}
	return workers, nil
}

func closeApplyWorkers(workers []*applyWorker) {
	for _, worker := range workers {
		if worker != nil && worker.conn != nil {
			worker.conn.Close(context.Background())
		}
	}
}

func (a *Applier) executeReplayPlan(
	ctx context.Context,
	workers []*applyWorker,
	plan replayPlan,
	transactions []Transaction,
	relations []map[uint32]*targetRelation,
) error {
	if len(workers) == 0 {
		return errors.New("cdc: replay claim has no target workers")
	}
	if err := validateReplayPlanExecution(plan, transactions); err != nil {
		return err
	}
	for _, step := range plan.Steps {
		if step.SerialTransaction >= 0 {
			work, exists := replayPlanWork(plan, step.Index, 0)
			if !exists || work.Kind != replayWorkSerial {
				return fmt.Errorf("cdc: replay serial step %d has no exact work manifest", step.Index)
			}
			transactionIndex := step.SerialTransaction
			if transactionIndex < 0 || transactionIndex >= len(transactions) {
				return fmt.Errorf("cdc: replay serial step %d has invalid transaction", step.Index)
			}
			if err := a.executeReplayWork(
				ctx, workers[0], plan.Claim, work,
				func(replay *applyPipeline) error {
					return a.queueTransactionChanges(
						replay, relations[transactionIndex], &transactions[transactionIndex], nil,
					)
				},
			); err != nil {
				return err
			}
			continue
		}

		buckets := make([][]replayPlanLane, len(workers))
		loads := make([]int64, len(workers))
		lanes := append([]replayPlanLane(nil), step.Lanes...)
		slices.SortStableFunc(lanes, func(left, right replayPlanLane) int {
			if left.Work.ExpectedChanges != right.Work.ExpectedChanges {
				return -cmp.Compare(left.Work.ExpectedChanges, right.Work.ExpectedChanges)
			}
			return cmp.Compare(left.Lane, right.Lane)
		})
		for _, lane := range lanes {
			if err := validateReplayPlanLane(lane, transactions); err != nil {
				return err
			}
			workerIndex := 0
			for candidate := 1; candidate < len(loads); candidate++ {
				if loads[candidate] < loads[workerIndex] {
					workerIndex = candidate
				}
			}
			buckets[workerIndex] = append(buckets[workerIndex], lane)
			loads[workerIndex] += lane.Work.ExpectedChanges
		}
		group, groupCtx := errgroup.WithContext(ctx)
		for workerIndex := range buckets {
			if len(buckets[workerIndex]) == 0 {
				continue
			}
			worker := workers[workerIndex]
			lanes := buckets[workerIndex]
			group.Go(func() error {
				for _, lane := range lanes {
					lane := lane
					if err := a.executeReplayWork(
						groupCtx, worker, plan.Claim, lane.Work,
						func(replay *applyPipeline) error {
							return queueParallelReplayLane(replay, lane.Items)
						},
					); err != nil {
						return err
					}
				}
				return nil
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}
	}
	if a.config.beforeReplayFinalize != nil {
		if err := a.config.beforeReplayFinalize(plan.Claim); err != nil {
			return err
		}
	}
	if err := finalizeReplayClaim(ctx, workers[0].conn, plan.Claim); err != nil {
		return err
	}
	for i := range transactions {
		collector := newSampleCollector(a.config.Sampler, &transactions[i])
		collector.addAll(transactions[i].Changes)
		collector.flush()
	}
	return nil
}

func validateReplayPlanLane(lane replayPlanLane, transactions []Transaction) error {
	itemIndex := 0
	var expectedChanges int64
	previousTransaction := -1
	for _, transactionIndex := range lane.TransactionIndexes {
		if transactionIndex <= previousTransaction || transactionIndex < 0 ||
			transactionIndex >= len(transactions) {
			return fmt.Errorf("cdc: replay lane %d has invalid transaction order", lane.Lane)
		}
		previousTransaction = transactionIndex
		transaction := &transactions[transactionIndex]
		expectedChanges += int64(transaction.ChangeCount())
		if transaction.Spill != nil {
			return fmt.Errorf("cdc: replay lane %d contains a spilled transaction", lane.Lane)
		}
		for changeIndex := range transaction.Changes {
			if itemIndex >= len(lane.Items) {
				return fmt.Errorf("cdc: replay lane %d omits a source change", lane.Lane)
			}
			item := lane.Items[itemIndex]
			if item.transactionIndex != transactionIndex || item.changeIndex != changeIndex ||
				item.change != &transaction.Changes[changeIndex] || item.relation == nil {
				return fmt.Errorf("cdc: replay lane %d source change order is invalid", lane.Lane)
			}
			itemIndex++
		}
	}
	if itemIndex != len(lane.Items) {
		return fmt.Errorf("cdc: replay lane %d contains an unclaimed source change", lane.Lane)
	}
	if lane.Work.ExpectedTransactions != int64(len(lane.TransactionIndexes)) ||
		lane.Work.ExpectedChanges != expectedChanges {
		return fmt.Errorf("cdc: replay lane %d receipt counters do not cover its source transactions", lane.Lane)
	}
	return nil
}

func validateReplayPlanExecution(plan replayPlan, transactions []Transaction) error {
	seen := make([]bool, len(transactions))
	nextTransaction := 0
	for stepIndex, step := range plan.Steps {
		if step.Index != stepIndex {
			return errors.New("cdc: replay plan step indexes are not contiguous")
		}
		if step.SerialTransaction >= 0 {
			transactionIndex := step.SerialTransaction
			if len(step.Lanes) != 0 || transactionIndex >= len(transactions) ||
				transactionIndex < 0 || seen[transactionIndex] || transactionIndex != nextTransaction {
				return fmt.Errorf("cdc: replay serial step %d has invalid coverage", step.Index)
			}
			work, exists := replayPlanWork(plan, step.Index, 0)
			if !exists || work.ExpectedTransactions != 1 ||
				work.ExpectedChanges != int64(transactions[transactionIndex].ChangeCount()) {
				return fmt.Errorf("cdc: replay serial step %d has invalid receipt counters", step.Index)
			}
			seen[transactionIndex] = true
			nextTransaction++
			continue
		}
		if len(step.Lanes) == 0 {
			return fmt.Errorf("cdc: replay parallel step %d is empty", step.Index)
		}
		stepTransactions := make(map[int]struct{})
		for _, lane := range step.Lanes {
			for _, transactionIndex := range lane.TransactionIndexes {
				if transactionIndex < 0 || transactionIndex >= len(transactions) || seen[transactionIndex] {
					return fmt.Errorf("cdc: replay step %d has duplicate or invalid transaction coverage", step.Index)
				}
				seen[transactionIndex] = true
				stepTransactions[transactionIndex] = struct{}{}
			}
		}
		for offset := range len(stepTransactions) {
			if _, exists := stepTransactions[nextTransaction+offset]; !exists {
				return fmt.Errorf("cdc: replay step %d does not cover one contiguous source epoch", step.Index)
			}
		}
		nextTransaction += len(stepTransactions)
	}
	for transactionIndex, covered := range seen {
		if !covered {
			return fmt.Errorf("cdc: replay plan omits source transaction %d", transactionIndex)
		}
	}
	if replayPlanWorkTransactions(plan.Works) != plan.Claim.Transactions ||
		replayPlanWorkChanges(plan.Works) != plan.Claim.Changes {
		return errors.New("cdc: replay work manifest does not cover the claim counters")
	}
	return nil
}

func (a *Applier) executeReplayWork(
	ctx context.Context,
	worker *applyWorker,
	claim replayClaim,
	work replayClaimWork,
	queueDML func(*applyPipeline) error,
) error {
	committed, err := beginReplayClaimWork(ctx, worker.conn, claim, work)
	if err != nil || committed {
		return err
	}
	replay := newApplyPipeline(ctx, worker.conn.PgConn(), worker.statements)
	replay.syncWindow = applyBatchPipelineWindow
	replayErr := queueDML(replay)
	if replayErr == nil {
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'T' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after replay work is %q, want %q",
			replay.conn.TxStatus(), 'T',
		)
	}
	if replayErr == nil {
		replay.queueUnprepared(
			replayWorkCompletionSQL,
			replayWorkCompletionParams(claim, work),
			applyExpectation{
				description: "commit exact replay work receipt", expectedRows: 1,
			},
		)
		replay.commit()
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'I' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after replay work commit is %q, want %q",
			replay.conn.TxStatus(), 'I',
		)
	}
	if replayErr != nil {
		return errors.Join(replayErr, replay.abort())
	}
	if err := replay.close(); err != nil {
		return err
	}
	if a.config.afterReplayWork != nil {
		if err := a.config.afterReplayWork(claim, work); err != nil {
			return err
		}
	}
	return nil
}

func queueParallelReplayLane(replay *applyPipeline, items []relationBatchedChange) error {
	// Source transactions commonly interleave several relations. Preserve the
	// first-seen relation order and exact per-relation change order, but collect
	// each homogeneous relation into one lane before invoking the existing set
	// DML batcher. Without this stable grouping, every source transaction breaks
	// into several tiny SQL statements and target concurrency loses to the
	// single-session batched path.
	relationIndexes := make(map[*targetRelation]int)
	relationLanes := make([][]relationBatchedChange, 0)
	for _, item := range items {
		index, exists := relationIndexes[item.relation]
		if !exists {
			index = len(relationLanes)
			relationIndexes[item.relation] = index
			relationLanes = append(relationLanes, nil)
		}
		relationLanes[index] = append(relationLanes[index], item)
	}
	for _, lane := range relationLanes {
		if err := queueRelationReplayLane(replay, lane); err != nil {
			return err
		}
	}
	return nil
}
