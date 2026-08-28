package cdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	migrationconfig "github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// DivergenceError reports source/target state that prevents exactly-once
// replay. It is permanent and is not retried by Applier.Run.
type DivergenceError struct {
	Relation string
	Change   ChangeKind
	Reason   string
}

func (e *DivergenceError) Error() string {
	return fmt.Sprintf("cdc: divergence applying %s change to %s: %s", changeKindName(e.Change), e.Relation, e.Reason)
}

type ApplierConfig struct {
	ConnString           string
	Directory            string
	ReaderSpillDirectory string
	StreamID             string
	StreamGeneration     string
	FreshSetup           bool
	TargetHasCopiedData  bool
	Durable              *DurableWatermark
	PollInterval         time.Duration
	ReconnectDelay       time.Duration
	// ReplayWorkers is the maximum number of target sessions that execute one
	// durable replay claim concurrently. A zero value keeps direct library users
	// on the legacy single-session path; the application supplies its explicit
	// operator-configured default.
	ReplayWorkers     int
	BatchMaxDataBytes int64
	BatchMaxChanges   int
	// EndPosition returns the optional inclusive cutover boundary. Transactions
	// beyond it are never applied.
	EndPosition func(context.Context) (LSN, bool, error)
	// AfterProgress runs after target data and progress commit. Maintenance
	// failures are terminal rather than hidden behind reconnect.
	AfterProgress ProgressCallback
	// afterReplayWork and beforeReplayFinalize are deterministic crash-test
	// hooks. Production leaves them nil.
	afterReplayWork      func(replayClaim, replayClaimWork) error
	beforeReplayFinalize func(replayClaim) error
	// Sampler, when set, is told which rows each committed transaction wrote, so
	// that verification can check the replication path rather than only the rows
	// the base copy left in the heap.
	Sampler KeySampler
}

type Applier struct {
	config ApplierConfig
	// streamGeneration is the current durable target-side generation token.
	// The configured generation remains immutable; successful replay claims move
	// this token monotonically so transactions started by older binaries can
	// never pass the progress guard after a claim finalizes.
	streamGeneration string

	// endPosition caches the normalized cutover boundary. NormalizeEndPosition
	// decodes every staged transaction from the start of the retained set, and
	// the apply loop consults the boundary once per transaction, so resolving it
	// each time made apply cost grow with the square of the staged stream.
	endPositionMu        sync.Mutex
	endPositionRequested LSN
	endPositionBoundary  LSN
	endPositionResolved  bool
}

func NewApplier(config ApplierConfig) (*Applier, error) {
	if config.ConnString == "" || config.Directory == "" || config.StreamID == "" {
		return nil, errors.New("cdc: applier connection string, directory, and stream ID are required")
	}
	if config.Durable == nil {
		return nil, errors.New("cdc: applier durable watermark is required")
	}
	if config.StreamGeneration == "" {
		config.StreamGeneration = config.StreamID
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.ReconnectDelay <= 0 {
		config.ReconnectDelay = time.Second
	}
	if config.ReplayWorkers < 0 || config.BatchMaxDataBytes < 0 || config.BatchMaxChanges < 0 {
		return nil, errors.New("cdc: replay workers and batch limits must not be negative")
	}
	if config.ReplayWorkers == 0 {
		config.ReplayWorkers = 1
	}
	if err := migrationconfig.ValidateReplayWorkers(config.ReplayWorkers); err != nil {
		return nil, fmt.Errorf("cdc: %w", err)
	}
	if config.BatchMaxDataBytes == 0 {
		config.BatchMaxDataBytes = applyBatchDefaultDataBytes
	}
	if config.BatchMaxChanges == 0 {
		config.BatchMaxChanges = applyBatchDefaultChanges
	}
	return &Applier{config: config}, nil
}

// Run continuously applies durable complete transactions. Connection failures
// reconnect; divergence and segment corruption are returned to the caller.
func (a *Applier) Run(ctx context.Context) error {
	for {
		err := a.runConnection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var divergence *DivergenceError
		if errors.As(err, &divergence) {
			return err
		}
		if !isConnectionError(err) {
			return err
		}
		timer := time.NewTimer(a.config.ReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// WaitUntil blocks until the authoritative target progress reaches boundary.
// Only Applier.Run advances that progress, so the caller must supervise Run
// concurrently and stop waiting if it exits; otherwise this polls forever.
//
// boundary must already be a transaction EndLSN. Catchup passes the durable
// watermark, which the persister published from a commit. A manual cutover LSN
// is normalized before it is stored. This wait must not scan the segment
// directory: NormalizeEndPosition reads every staged transaction, and after a
// long copy that is the whole backlog.
func (a *Applier) WaitUntil(ctx context.Context, boundary LSN) error {
	if boundary == 0 {
		return nil
	}
	for {
		conn, err := postgres.Connect(ctx, a.config.ConnString)
		if err != nil {
			return fmt.Errorf("cdc: connect catch-up observer: %w", err)
		}
		progress, _, readErr := postgres.ReadProgress(ctx, conn, a.config.StreamID)
		conn.Close(context.Background())
		if readErr != nil {
			return fmt.Errorf("cdc: read catch-up progress: %w", readErr)
		}
		if LSN(progress) >= boundary {
			return nil
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

func (a *Applier) runConnection(ctx context.Context) error {
	conn, err := postgres.Connect(ctx, a.config.ConnString)
	if err != nil {
		return fmt.Errorf("cdc: connect applier: %w", err)
	}
	defer conn.Close(context.Background())
	if err := postgres.EnsureProgressTable(ctx, conn); err != nil {
		return fmt.Errorf("cdc: ensure apply progress: %w", err)
	}
	if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
		StreamID:            a.config.StreamID,
		Generation:          a.config.StreamGeneration,
		FreshSetup:          a.config.FreshSetup,
		TargetHasCopiedData: a.config.TargetHasCopiedData,
	}); err != nil {
		return err
	}
	effectiveGeneration, err := resolveStreamEffectiveGeneration(
		ctx, conn, a.config.StreamID, a.config.StreamGeneration,
	)
	if err != nil {
		return err
	}
	a.streamGeneration = effectiveGeneration
	if err := ensureReplayClaimTables(ctx, conn); err != nil {
		return err
	}

	progress, progressExists, err := postgres.ReadProgress(ctx, conn, a.config.StreamID)
	if err != nil {
		return fmt.Errorf("cdc: read apply progress: %w", err)
	}
	if err := configureApplySession(ctx, conn); err != nil {
		return err
	}
	statementCache := newApplyStatementCache(applyStatementCacheCapacity)
	workers, err := openApplyWorkers(
		ctx, conn, statementCache, a.config.ConnString, a.config.ReplayWorkers,
	)
	if err != nil {
		return err
	}
	defer closeApplyWorkers(workers[1:])
	if progressExists && a.config.AfterProgress != nil {
		if err := a.config.AfterProgress(ctx, LSN(progress)); err != nil {
			return err
		}
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:      a.config.Directory,
		SpillDirectory: a.config.ReaderSpillDirectory,
		DurableEndLSN:  a.config.Durable.Load(),
	})
	if err != nil {
		return err
	}
	defer reader.Close()
	relationCache := newTargetRelationCache()
	for {
		if err := reader.Refresh(a.config.Durable.Load()); err != nil {
			return err
		}
		if a.config.EndPosition != nil {
			end, set, err := a.effectiveEndPosition(ctx)
			if err != nil {
				return err
			}
			if set && LSN(progress) >= end {
				return nil
			}
		}
		applied, next, err := a.applyFromReader(
			ctx, conn, reader, relationCache, statementCache, workers, LSN(progress),
		)
		if err != nil {
			return err
		}
		if applied {
			progress = pglogrepl.LSN(next)
			if a.config.AfterProgress != nil {
				if err := a.config.AfterProgress(ctx, next); err != nil {
					return err
				}
			}
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

func (a *Applier) effectiveStreamGeneration() string {
	if a.streamGeneration != "" {
		return a.streamGeneration
	}
	return a.config.StreamGeneration
}

func configureApplySession(ctx context.Context, conn *pgx.Conn) error {
	// This connection is dedicated to logical replay. Set replica role once so
	// every source transaction suppresses target triggers and referential
	// actions without paying an extra target round trip per transaction. Force
	// synchronous durability here as well: target bulk-load tuning may have set
	// the database default to off, but CDC pruning advances from committed apply
	// progress and must never discard a segment before its target commit is
	// crash-durable.
	if _, err := conn.Exec(ctx, "SET session_replication_role = replica"); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: disable target replication triggers: %w", err))
	}
	if _, err := conn.Exec(ctx, "SET synchronous_commit = on"); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: require durable target commits: %w", err))
	}
	return nil
}

func (a *Applier) applyAvailable(ctx context.Context, conn *pgx.Conn, progress LSN) (bool, LSN, error) {
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:      a.config.Directory,
		SpillDirectory: a.config.ReaderSpillDirectory,
		DurableEndLSN:  a.config.Durable.Load(),
	})
	if err != nil {
		return false, progress, err
	}
	defer reader.Close()
	return a.applyFromReader(
		ctx, conn, reader, newTargetRelationCache(),
		newApplyStatementCache(applyStatementCacheCapacity), nil, progress,
	)
}

const (
	// Catch-up scheduling slices are bounded independently by source transaction
	// count, row changes, and decoded payload size. A four-slice durable window
	// gives the key-affine executor enough work to keep every target session busy.
	// A resident source transaction is never split: when it alone exceeds the
	// window it becomes a one-transaction replay claim. Only disk-spilled
	// transactions keep the streaming standalone path.
	applyBatchMaxTransactions  = 16384
	applyBatchDefaultChanges   = 131072
	applyBatchDefaultDataBytes = 32 << 20

	// Eight sessions is one replay scheduling slice. Four configured slices form
	// the default durable target wave; higher session counts extend the wave so
	// per-session work does not shrink. The claim format does not change, so an
	// older binary can reconstruct and finish an active window from its stored
	// EndLSN, lane count, manifest, and receipts.
	replayWorkersPerClaimSlice = 8
)

func replayClaimWindowSlices(workers int) int {
	if workers < replayWorkersPerClaimSlice {
		return 1
	}
	// Match crdb-to-pg's four-claim target wave even at the conservative default
	// of eight sessions. More than 32 sessions add one slice per additional
	// eight sessions so per-session work does not shrink as concurrency grows.
	return max(4, (workers+replayWorkersPerClaimSlice-1)/replayWorkersPerClaimSlice)
}

func replayClaimLaneCount(workers int) int {
	if workers <= 1 {
		return 1
	}
	// Extra logical lanes smooth hash-skew tails while worker counts are small.
	// Once there are 32 real sessions, retain one deterministic lane per session
	// instead of multiplying receipt transactions and synchronous commits.
	return min(max(workers, workers*4), max(32, workers))
}

func replayClaimWindowLimits(workers, changes int, dataBytes int64) (int, int, int64) {
	slices := replayClaimWindowSlices(workers)
	maxInt := int(^uint(0) >> 1)
	transactionsLimit := applyBatchMaxTransactions
	changesLimit := changes
	dataBytesLimit := dataBytes
	if slices > 1 {
		if transactionsLimit > maxInt/slices {
			transactionsLimit = maxInt
		} else {
			transactionsLimit *= slices
		}
		if changesLimit > maxInt/slices {
			changesLimit = maxInt
		} else {
			changesLimit *= slices
		}
		if dataBytesLimit > int64(^uint64(0)>>1)/int64(slices) {
			dataBytesLimit = int64(^uint64(0) >> 1)
		} else {
			dataBytesLimit *= int64(slices)
		}
	}
	return transactionsLimit, changesLimit, dataBytesLimit
}

func (a *Applier) applyFromReader(
	ctx context.Context,
	conn *pgx.Conn,
	reader *Reader,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	workers []*applyWorker,
	progress LSN,
) (bool, LSN, error) {
	if len(workers) == 0 {
		workers = []*applyWorker{{conn: conn, statements: statementCache}}
	}
	var activeClaim replayClaim
	claimExists := false
	if conn != nil && a.config.StreamID != "" {
		var err error
		activeClaim, claimExists, err = readReplayClaim(ctx, conn, a.config.StreamID)
		if err != nil {
			return false, progress, err
		}
	}
	if claimExists && (activeClaim.StreamID != a.config.StreamID ||
		activeClaim.Generation != a.config.StreamGeneration ||
		activeClaim.FenceGeneration != a.effectiveStreamGeneration() ||
		activeClaim.StartLSN != progress) {
		return false, progress, errors.New("cdc: active replay claim does not match target progress identity")
	}
	applyBatch := func(batch []Transaction, claim *replayClaim) (bool, LSN, error) {
		return a.applyTransactionBatchWithWorkers(
			ctx, conn, relationCache, statementCache, workers, batch, progress, claim,
		)
	}
	transactionsLimit, changesLimit, dataBytesLimit := replayClaimWindowLimits(
		a.config.ReplayWorkers, a.config.BatchMaxChanges, a.config.BatchMaxDataBytes,
	)
	batch := make([]Transaction, 0, transactionsLimit)
	batchChanges := 0
	var batchDataBytes int64
	for {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			if claimExists {
				return false, progress, fmt.Errorf(
					"cdc: durable replay claim ends at %s but retained input ended first",
					pglogrepl.LSN(activeClaim.EndLSN),
				)
			}
			if len(batch) == 0 {
				return false, progress, nil // nothing staged is left to apply
			}
			return applyBatch(batch, nil)
		}
		if err != nil {
			if claimExists {
				return false, progress, err
			}
			// Publish the verified prefix before surfacing a corrupt or otherwise
			// unreadable suffix. The next apply pass resumes at the committed
			// progress and reports the same suffix error.
			if len(batch) != 0 {
				return applyBatch(batch, nil)
			}
			return false, progress, err
		}
		// Target progress is an EndLSN. Re-scan is deliberate: it remains
		// correct even when commit and end positions are not interchangeable.
		if transaction.EndLSN <= progress {
			if err := transaction.CleanupSpill(); err != nil {
				return false, progress, fmt.Errorf("cdc: cleanup already-applied reader spill: %w", err)
			}
			continue
		}
		if claimExists {
			if transaction.EndLSN > activeClaim.EndLSN {
				return false, progress, errors.Join(
					fmt.Errorf(
						"cdc: retained replay transaction ends at %s beyond active claim %s",
						pglogrepl.LSN(transaction.EndLSN), pglogrepl.LSN(activeClaim.EndLSN),
					),
					transaction.CleanupSpill(), cleanupTransactionBatch(batch),
				)
			}
			batch = append(batch, transaction)
			if transaction.EndLSN == activeClaim.EndLSN {
				return applyBatch(batch, &activeClaim)
			}
			continue
		}
		if a.config.EndPosition != nil {
			end, set, err := a.effectiveEndPosition(ctx)
			if err != nil {
				return false, progress, errors.Join(err, transaction.CleanupSpill())
			}
			if set && transaction.EndLSN > end {
				if err := transaction.CleanupSpill(); err != nil {
					return false, progress, fmt.Errorf("cdc: cleanup post-boundary reader spill: %w", err)
				}
				if len(batch) == 0 {
					return false, progress, nil
				}
				return applyBatch(batch, nil)
			}
		}
		if transaction.IsSpilled() {
			if len(batch) != 0 {
				// The reader already advanced over this transaction. Hold it for
				// the next call so the completed small batch can publish progress
				// and maintenance callbacks before a large transaction starts.
				reader.pending = &transaction
				return applyBatch(batch, nil)
			}
			applyErr := a.applyTransaction(
				ctx, conn, relationCache, statementCache, progress, &transaction,
			)
			cleanupErr := transaction.CleanupSpill()
			if applyErr != nil {
				return false, progress, errors.Join(applyErr, cleanupErr)
			}
			if cleanupErr != nil {
				return false, progress, fmt.Errorf("cdc: cleanup applied reader spill: %w", cleanupErr)
			}
			return true, transaction.EndLSN, nil
		}
		transactionChanges := int(transaction.ChangeCount())
		transactionDataBytes := int64(transactionApplyDataBytes(&transaction))
		if len(batch) != 0 &&
			(batchChanges+transactionChanges > changesLimit ||
				batchDataBytes+transactionDataBytes > dataBytesLimit) {
			// Keep the configured wave bound without splitting this source
			// transaction. The next pass will claim it alone.
			reader.pending = &transaction
			return applyBatch(batch, nil)
		}
		batchChanges += transactionChanges
		batchDataBytes += transactionDataBytes
		batch = append(batch, transaction)
		if len(batch) >= transactionsLimit ||
			batchChanges >= changesLimit ||
			batchDataBytes >= dataBytesLimit {
			return applyBatch(batch, nil)
		}
	}
}

func transactionApplyDataBytes(transaction *Transaction) int {
	if transaction == nil {
		return 0
	}
	bytes := 0
	for i := range transaction.Relations {
		bytes += len(transaction.Relations[i].Namespace) + len(transaction.Relations[i].Name)
		for j := range transaction.Relations[i].Columns {
			bytes += len(transaction.Relations[i].Columns[j].Name)
		}
	}
	addTuple := func(tuple *Tuple) {
		if tuple == nil {
			return
		}
		for i := range *tuple {
			bytes += len((*tuple)[i].Data)
		}
	}
	for i := range transaction.Changes {
		addTuple(transaction.Changes[i].Old)
		addTuple(transaction.Changes[i].New)
	}
	return bytes
}

func (a *Applier) effectiveEndPosition(ctx context.Context) (LSN, bool, error) {
	requested, set, err := a.config.EndPosition(ctx)
	if err != nil || !set {
		return requested, set, err
	}
	durable := a.config.Durable.Load()
	if durable < requested {
		return requested, true, nil
	}
	boundary, err := a.resolveEndPosition(requested, durable)
	if err != nil {
		return 0, false, err
	}
	return boundary, true, nil
}

// resolveEndPosition normalizes requested at most once per requested value. The
// result is stable: durable is at or past requested, so every transaction that
// could end at or before requested is already staged and durable.
func (a *Applier) resolveEndPosition(requested, durable LSN) (LSN, error) {
	a.endPositionMu.Lock()
	defer a.endPositionMu.Unlock()
	if a.endPositionResolved && a.endPositionRequested == requested {
		return a.endPositionBoundary, nil
	}
	resolution, err := NormalizeEndPosition(a.config.Directory, requested, durable)
	if err != nil {
		return 0, err
	}
	a.endPositionRequested = requested
	a.endPositionBoundary = resolution.Boundary
	a.endPositionResolved = true
	return resolution.Boundary, nil
}

type targetRelation struct {
	source           Relation
	quoted           string
	heapBytes        int64
	heapBlocksRead   int64
	heapBlocksHit    int64
	columns          []targetColumn
	mappedColumns    []targetColumn
	generatedColumns []targetColumn
	overrideIdentity bool
	capabilities     targetRelationCapabilities
}

// targetRelationCapabilities separates ordering safety from the available
// transport. A relation that cannot cross an ordering barrier may still admit
// set-based DML inside its original source position (for example, an enum table
// whose values need a typed COPY stage instead of built-in binary arrays).
// Keeping these decisions independent prevents one slow relation from forcing
// every otherwise-independent relation in a catch-up batch onto the scalar path.
type targetRelationCapabilities struct {
	// relationLane permits hashing independent primary-key rows across target
	// sessions. relationOrderedLane is the strictly weaker guarantee that every
	// write for this relation may share one relation-scoped lane. The latter
	// keeps non-PK UNIQUE conflicts and tables without a canonical PK
	// in source order without turning them into a global replay barrier.
	relationLane        bool
	relationOrderedLane bool
	// relationOrderedLaneV4 freezes plan-v4 admission. Plan v5 may admit a
	// custom text-search config only after proving its complete unaccent closure.
	relationOrderedLaneV4 bool
	// relationOrderedLaneV3 freezes the stricter plan-v3 catalog admission so
	// an active v3 claim reconstructs exactly after a rolling binary restart.
	// Plan v4 separates relation-local ordering from set-DML transport safety.
	relationOrderedLaneV3 bool
	// primaryKeyArbiter is true only when PostgreSQL can use the target primary
	// key as an ON CONFLICT arbiter. DEFERRABLE primary keys still identify rows
	// and order replay safely, but PostgreSQL rejects them as conflict arbiters.
	primaryKeyArbiter bool
	keyedSetDML       bool
	binaryCopy        bool
	textCopyStage     bool
	selectiveUpdates  bool
	// crossKeyConflicts is true when distinct primary-key rows can conflict
	// through an ordinary non-primary UNIQUE index. Relations with exclusion
	// indexes remain global replay barriers. A cross-key UNIQUE relation
	// remains safe for set DML inside one target transaction, but is not eligible
	// for primary-key-sharded target transactions.
	crossKeyConflicts bool
}

type targetColumn struct {
	name                      string
	quoted                    string
	oid                       uint32
	arrayOID                  uint32
	nondeterministicCollation bool
	key                       bool
	primary                   bool
	primaryPos                int
	replayKeySafe             bool
	lanePayloadTextOnly       bool
	identity                  string
	sourceIndex               int
	generated                 bool
	notNull                   bool
	conflicting               bool
}

type targetRelationCache struct {
	relations map[uint32]*targetRelation
}

type targetRelationQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type targetRelationLoader func(context.Context, targetRelationQuerier, *Relation) (*targetRelation, error)

func newTargetRelationCache() *targetRelationCache {
	return &targetRelationCache{relations: make(map[uint32]*targetRelation)}
}

func (c *targetRelationCache) resolve(
	ctx context.Context,
	db targetRelationQuerier,
	source *Relation,
	loader targetRelationLoader,
) (*targetRelation, error) {
	if c == nil {
		return loader(ctx, db, source)
	}
	if cached := c.relations[source.OID]; cached != nil &&
		sameRelationDefinition(&cached.source, source) {
		return cached, nil
	}
	relation, err := loader(ctx, db, source)
	if err != nil {
		return nil, err
	}
	relation.source = cloneRelation(*source)
	c.relations[source.OID] = relation
	return relation, nil
}

func sameRelationDefinition(left, right *Relation) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.OID == right.OID &&
		left.Namespace == right.Namespace &&
		left.Name == right.Name &&
		left.ReplicaIdentity == right.ReplicaIdentity &&
		slices.Equal(left.Columns, right.Columns)
}

func (a *Applier) applyTransaction(
	ctx context.Context,
	conn *pgx.Conn,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	progress LSN,
	transaction *Transaction,
) error {
	relations, err := resolveTargetRelations(ctx, conn, relationCache, transaction)
	if err != nil {
		return err
	}

	collector := newSampleCollector(a.config.Sampler, transaction)
	replay := newApplyPipeline(ctx, conn.PgConn(), statementCache)
	replay.begin()
	replayErr := a.queueTransactionChanges(replay, relations, transaction, collector)
	if replayErr == nil {
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'T' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after replay is %q, want %q",
			replay.conn.TxStatus(), 'T',
		)
	}
	if replayErr == nil {
		replay.queueProgress(
			a.config.StreamID,
			a.effectiveStreamGeneration(),
			progress,
			transaction.EndLSN,
			1,
			int64(transaction.ChangeCount()),
		)
		replay.commit()
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'I' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after commit is %q, want %q",
			replay.conn.TxStatus(), 'I',
		)
	}
	if replayErr != nil {
		return errors.Join(replayErr, replay.abort())
	}
	if err := replay.close(); err != nil {
		return err
	}
	collector.flush()
	return nil
}

// applyTransactionBatch replays a bounded prefix of source transactions in one
// target transaction. The final source EndLSN is committed atomically with all
// DML, so a crash leaves either the whole prefix and its progress present or
// neither. Plain relations whose catalog proves that replica-mode writes have
// no cross-relation behavior may be grouped into relation-local lanes; every
// other batch retains exact source order.
func (a *Applier) applyTransactionBatch(
	ctx context.Context,
	conn *pgx.Conn,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	transactions []Transaction,
	progress LSN,
) (bool, LSN, error) {
	return a.applyTransactionBatchWithWorkers(
		ctx, conn, relationCache, statementCache,
		[]*applyWorker{{conn: conn, statements: statementCache}},
		transactions, progress, nil,
	)
}

func (a *Applier) applyTransactionBatchWithWorkers(
	ctx context.Context,
	conn *pgx.Conn,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	workers []*applyWorker,
	transactions []Transaction,
	progress LSN,
	resume *replayClaim,
) (bool, LSN, error) {
	if len(transactions) == 0 {
		return false, progress, nil
	}
	relations := make([]map[uint32]*targetRelation, len(transactions))
	for i := range transactions {
		resolved, err := resolveTargetRelations(ctx, conn, relationCache, &transactions[i])
		if err != nil {
			return false, progress, errors.Join(err, cleanupTransactionBatch(transactions))
		}
		relations[i] = resolved
	}

	laneCount := a.config.ReplayWorkers
	if resume != nil {
		laneCount = resume.LaneCount
	} else if laneCount > 1 {
		// Extra logical lanes reduce hash-skew tails at small worker counts. The
		// executor size-balances those deterministic lanes across real sessions.
		laneCount = replayClaimLaneCount(laneCount)
	}
	if laneCount > 1 && (resume != nil || len(workers) > 1) {
		startGeneration := a.effectiveStreamGeneration()
		if resume != nil {
			startGeneration = resume.StartGeneration
		}
		planVersion := replayClaimPlanVersion
		if resume != nil {
			planVersion = resume.PlanVersion
		}
		plan, err := buildReplayPlanForGenerationVersion(
			a.config.StreamID, a.config.StreamGeneration, startGeneration, progress,
			laneCount, transactions, relations, planVersion,
		)
		if err != nil {
			return false, progress, errors.Join(err, cleanupTransactionBatch(transactions))
		}
		// A fresh batch containing any true serial barrier stays on the proven
		// one-transaction relation-batched path below. Turning alternating safe
		// epochs and barriers into a concurrent claim creates hundreds of tiny
		// synchronous commits and is strictly worse than that bounded fallback.
		// An existing claim must always reconstruct and finish its exact manifest,
		// including plan-version-2 claims left by an older binary.
		if shouldUseConcurrentReplayPlan(resume, plan) {
			if resume != nil && !replayClaimsEqual(plan.Claim, *resume) {
				return false, progress, errors.Join(
					errors.New("cdc: reconstructed replay claim digest does not match target claim"),
					cleanupTransactionBatch(transactions),
				)
			}
			claim, err := ensureReplayClaim(ctx, conn, plan.Claim, plan.Works)
			if err != nil {
				return false, progress, errors.Join(err, cleanupTransactionBatch(transactions))
			}
			plan.Claim = claim
			if err := a.executeReplayPlan(ctx, workers, plan, transactions, relations); err != nil {
				return false, progress, errors.Join(err, cleanupTransactionBatch(transactions))
			}
			a.streamGeneration = claim.FenceGeneration
			if err := cleanupTransactionBatch(transactions); err != nil {
				return false, progress, err
			}
			return true, claim.EndLSN, nil
		}
	}

	replay := newApplyPipeline(ctx, conn.PgConn(), statementCache)
	replay.syncWindow = applyBatchPipelineWindow
	replay.begin()
	collectors := make([]*sampleCollector, len(transactions))
	for i := range transactions {
		collectors[i] = newSampleCollector(a.config.Sampler, &transactions[i])
	}
	var replayErr error
	if reordered, err := queueRelationBatchedChanges(
		replay, relations, transactions, collectors,
	); reordered {
		replayErr = err
	} else {
		for i := range transactions {
			if err := a.queueTransactionChanges(
				replay, relations[i], &transactions[i], collectors[i],
			); err != nil {
				replayErr = err
				break
			}
		}
	}
	// A successful SQL command can still be a replay divergence when an UPDATE
	// or DELETE affects the wrong number of rows. Observe all DML results while
	// the coalesced target transaction can still be rolled back.
	if replayErr == nil {
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'T' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after batched replay is %q, want %q",
			replay.conn.TxStatus(), 'T',
		)
	}
	if replayErr == nil {
		last := transactions[len(transactions)-1].EndLSN
		var rows int64
		for i := range transactions {
			rows += int64(transactions[i].ChangeCount())
		}
		replay.queueProgress(
			a.config.StreamID,
			a.effectiveStreamGeneration(),
			progress,
			last,
			int64(len(transactions)),
			rows,
		)
		replay.commit()
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'I' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after batched commit is %q, want %q",
			replay.conn.TxStatus(), 'I',
		)
	}
	if replayErr != nil {
		return false, progress, errors.Join(
			replayErr, replay.abort(), cleanupTransactionBatch(transactions),
		)
	}
	if err := replay.close(); err != nil {
		return false, progress, errors.Join(err, cleanupTransactionBatch(transactions))
	}
	if err := cleanupTransactionBatch(transactions); err != nil {
		return false, progress, err
	}
	for _, collector := range collectors {
		collector.flush()
	}
	return true, transactions[len(transactions)-1].EndLSN, nil
}

func shouldUseConcurrentReplayPlan(resume *replayClaim, plan replayPlan) bool {
	// executeReplayPlan treats every unsafe source transaction as an ordered
	// barrier between parallel epochs. Keeping those barriers inside the durable
	// window lets safe work on either side use all target sessions without moving
	// the frontier past the barrier or splitting a source transaction.
	return resume != nil || plan.HasParallel
}

type relationBatchedChange struct {
	transactionIndex int
	changeIndex      int
	change           *Change
	relation         *targetRelation
	collector        *sampleCollector
}

type relationReplayStep struct {
	ordered     bool
	items       []relationBatchedChange
	lanes       [][]relationBatchedChange
	laneIndexes map[*targetRelation]int
}

// planRelationBatchedChanges partitions one target transaction into alternating
// safe epochs and ordered barriers. Safe epochs retain exact order inside each
// relation while allowing independent relations to be grouped. An unsafe row
// no longer disables batching for the entire prefix: it drains the preceding
// epoch, remains in source order with adjacent unsafe work, and starts a fresh
// epoch after it. The target transaction still wraps every step and progress.
func planRelationBatchedChanges(
	relations []map[uint32]*targetRelation,
	transactions []Transaction,
	collectors []*sampleCollector,
) ([]relationReplayStep, bool, error) {
	for i := range transactions {
		if transactions[i].Spill != nil {
			return nil, false, nil
		}
	}
	steps := make([]relationReplayStep, 0, 3)
	for transactionIndex := range transactions {
		for changeIndex := range transactions[transactionIndex].Changes {
			change := &transactions[transactionIndex].Changes[changeIndex]
			relation := relations[transactionIndex][change.RelationOID]
			if relation == nil {
				return nil, true, divergenceFor(nil, change.Kind, "required relation metadata is missing")
			}
			item := relationBatchedChange{
				transactionIndex: transactionIndex,
				changeIndex:      changeIndex,
				change:           change,
				relation:         relation,
				collector:        collectors[transactionIndex],
			}
			laneSafe := relation.capabilities.relationLane && change.Kind != ChangeTruncate
			if !laneSafe {
				if len(steps) == 0 || !steps[len(steps)-1].ordered {
					steps = append(steps, relationReplayStep{ordered: true})
				}
				step := &steps[len(steps)-1]
				step.items = append(step.items, item)
				continue
			}

			if len(steps) == 0 || steps[len(steps)-1].ordered {
				steps = append(steps, relationReplayStep{
					laneIndexes: make(map[*targetRelation]int),
				})
			}
			step := &steps[len(steps)-1]
			laneIndex, exists := step.laneIndexes[relation]
			if !exists {
				laneIndex = len(step.lanes)
				step.laneIndexes[relation] = laneIndex
				step.lanes = append(step.lanes, nil)
			}
			step.lanes[laneIndex] = append(step.lanes[laneIndex], item)
		}
	}
	return steps, true, nil
}

func queueRelationReplayLane(replay *applyPipeline, lane []relationBatchedChange) error {
	for start := 0; start < len(lane); {
		end := start + 1
		for end < len(lane) && lane[end].change.Kind == lane[start].change.Kind {
			end++
		}
		if err := queueRelationReplayRun(replay, nil, lane[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func queueOrderedReplayStep(
	replay *applyPipeline,
	relations []map[uint32]*targetRelation,
	items []relationBatchedChange,
) error {
	for start := 0; start < len(items); {
		end := start + 1
		for end < len(items) && orderedReplayItemsShareStatement(items[start], items[end]) {
			end++
		}
		if err := queueRelationReplayRun(replay, relations, items[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func orderedReplayItemsShareStatement(left, right relationBatchedChange) bool {
	if left.transactionIndex != right.transactionIndex || left.change.Kind != right.change.Kind {
		return false
	}
	if left.change.Kind == ChangeTruncate {
		return sameTruncateOptions(*left.change, *right.change)
	}
	return left.relation == right.relation
}

func queueRelationReplayRun(
	replay *applyPipeline,
	relations []map[uint32]*targetRelation,
	run []relationBatchedChange,
) error {
	if len(run) == 0 {
		return nil
	}
	changes := make([]Change, len(run))
	for i := range run {
		changes[i] = *run[i].change
	}
	var err error
	switch changes[0].Kind {
	case ChangeInsert:
		err = applyInserts(replay, run[0].relation, changes)
	case ChangeUpdate:
		err = applyUpdates(replay, run[0].relation, changes)
	case ChangeDelete:
		err = applyDeletes(replay, run[0].relation, changes)
	case ChangeTruncate:
		if relations == nil {
			return divergenceFor(run[0].relation, ChangeTruncate, "truncate escaped its ordering barrier")
		}
		err = applyTruncates(replay, relations[run[0].transactionIndex], changes)
	default:
		return divergenceFor(run[0].relation, changes[0].Kind, "unknown change kind")
	}
	if err != nil {
		return err
	}
	for i := range run {
		run[i].collector.add(run[i].change)
	}
	return nil
}

func queueRelationBatchedChanges(
	replay *applyPipeline,
	relations []map[uint32]*targetRelation,
	transactions []Transaction,
	collectors []*sampleCollector,
) (bool, error) {
	steps, planned, err := planRelationBatchedChanges(relations, transactions, collectors)
	if err != nil || !planned {
		return planned, err
	}
	for _, step := range steps {
		if step.ordered {
			if err := queueOrderedReplayStep(replay, relations, step.items); err != nil {
				return true, err
			}
			continue
		}
		for _, lane := range step.lanes {
			if err := queueRelationReplayLane(replay, lane); err != nil {
				return true, err
			}
		}
	}
	return true, nil
}

func cleanupTransactionBatch(transactions []Transaction) error {
	var result error
	for i := range transactions {
		if err := transactions[i].CleanupSpill(); err != nil {
			result = errors.Join(result, fmt.Errorf(
				"cdc: cleanup batched transaction %x spill: %w", transactions[i].EndLSN, err,
			))
		}
	}
	return result
}

func resolveTargetRelations(
	ctx context.Context,
	conn *pgx.Conn,
	relationCache *targetRelationCache,
	transaction *Transaction,
) (map[uint32]*targetRelation, error) {
	relations := make(map[uint32]*targetRelation, len(transaction.Relations))
	for i := range transaction.Relations {
		relation, err := relationCache.resolve(ctx, conn, &transaction.Relations[i], loadTargetRelation)
		if err != nil {
			return nil, err
		}
		relations[relation.source.OID] = relation
	}
	return relations, nil
}

func (a *Applier) queueTransactionChanges(
	replay *applyPipeline,
	relations map[uint32]*targetRelation,
	transaction *Transaction,
	collector *sampleCollector,
) error {
	var replayErr error
	if transaction.Spill != nil {
		replayErr = a.applySpilledChanges(replay, relations, transaction.Spill, collector)
	} else {
		for i := 0; i < len(transaction.Changes); {
			change := &transaction.Changes[i]
			relation := relations[change.RelationOID]
			if relation == nil {
				replayErr = divergenceFor(nil, change.Kind, "required relation metadata is missing")
				break
			}
			switch change.Kind {
			case ChangeInsert:
				end := i + 1
				for end < len(transaction.Changes) &&
					transaction.Changes[end].Kind == ChangeInsert &&
					transaction.Changes[end].RelationOID == change.RelationOID {
					end++
				}
				if err := applyInserts(replay, relation, transaction.Changes[i:end]); err != nil {
					replayErr = err
					break
				}
				collector.addAll(transaction.Changes[i:end])
				i = end
			case ChangeUpdate:
				end := i + 1
				for end < len(transaction.Changes) &&
					transaction.Changes[end].Kind == ChangeUpdate &&
					transaction.Changes[end].RelationOID == change.RelationOID {
					end++
				}
				if err := applyUpdates(replay, relation, transaction.Changes[i:end]); err != nil {
					replayErr = err
					break
				}
				collector.addAll(transaction.Changes[i:end])
				i = end
			case ChangeDelete:
				if err := applyDelete(replay, relation, change); err != nil {
					replayErr = err
					break
				}
				collector.add(change)
				i++
			case ChangeTruncate:
				end := i + 1
				for end < len(transaction.Changes) &&
					transaction.Changes[end].Kind == ChangeTruncate &&
					sameTruncateOptions(transaction.Changes[end], *change) {
					end++
				}
				if err := applyTruncates(replay, relations, transaction.Changes[i:end]); err != nil {
					replayErr = err
					break
				}
				i = end
			default:
				replayErr = divergenceFor(relation, change.Kind, "unknown change kind")
			}
			if replayErr != nil {
				break
			}
		}
	}
	return replayErr
}

func loadTargetRelation(ctx context.Context, db targetRelationQuerier, source *Relation) (*targetRelation, error) {
	rows, err := db.Query(ctx, `
		WITH trusted_unaccent_extension AS (
		  SELECT extension_row.oid, extension_row.extnamespace
		  FROM pg_catalog.pg_extension extension_row
		  WHERE extension_row.extname = 'unaccent'
		    AND extension_row.extversion = '1.1'
		), trusted_unaccent_init_function AS (
		  SELECT init_function.oid, trusted_extension.oid AS extension_oid
		  FROM trusted_unaccent_extension trusted_extension
		  JOIN pg_catalog.pg_namespace function_namespace
		    ON function_namespace.oid = trusted_extension.extnamespace
		  JOIN pg_catalog.pg_proc init_function
		    ON init_function.pronamespace = function_namespace.oid
		   AND init_function.proname = 'unaccent_init'
		  JOIN pg_catalog.pg_language function_language
		    ON function_language.oid = init_function.prolang
		   AND function_language.oid = 13
		   AND function_language.lanname = 'c'
		  JOIN pg_catalog.pg_depend extension_dependency
		    ON extension_dependency.classid = 'pg_catalog.pg_proc'::regclass
		   AND extension_dependency.objid = init_function.oid
		   AND extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
		   AND extension_dependency.refobjid = trusted_extension.oid
		   AND extension_dependency.deptype = 'e'
		  WHERE init_function.prorettype = 'internal'::regtype
		    AND init_function.pronargs = 1
		    AND init_function.proargtypes[0] = 'internal'::regtype
		    AND init_function.probin = '$libdir/unaccent'
		    AND init_function.prosrc = 'unaccent_init'
		    AND init_function.provolatile = 'v'
		    AND init_function.proparallel = 's'
		    AND init_function.prokind = 'f'
		    AND NOT init_function.proretset
		    AND NOT init_function.proisstrict
		    AND NOT init_function.prosecdef
		    AND NOT init_function.proleakproof
		    AND init_function.proconfig IS NULL
		    AND init_function.prosupport = 0
		), trusted_unaccent_lexize_function AS (
		  SELECT lexize_function.oid, trusted_extension.oid AS extension_oid
		  FROM trusted_unaccent_extension trusted_extension
		  JOIN pg_catalog.pg_namespace function_namespace
		    ON function_namespace.oid = trusted_extension.extnamespace
		  JOIN pg_catalog.pg_proc lexize_function
		    ON lexize_function.pronamespace = function_namespace.oid
		   AND lexize_function.proname = 'unaccent_lexize'
		  JOIN pg_catalog.pg_language function_language
		    ON function_language.oid = lexize_function.prolang
		   AND function_language.oid = 13
		   AND function_language.lanname = 'c'
		  JOIN pg_catalog.pg_depend extension_dependency
		    ON extension_dependency.classid = 'pg_catalog.pg_proc'::regclass
		   AND extension_dependency.objid = lexize_function.oid
		   AND extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
		   AND extension_dependency.refobjid = trusted_extension.oid
		   AND extension_dependency.deptype = 'e'
		  WHERE lexize_function.prorettype = 'internal'::regtype
		    AND lexize_function.pronargs = 4
		    AND lexize_function.proargtypes[0] = 'internal'::regtype
		    AND lexize_function.proargtypes[1] = 'internal'::regtype
		    AND lexize_function.proargtypes[2] = 'internal'::regtype
		    AND lexize_function.proargtypes[3] = 'internal'::regtype
		    AND lexize_function.probin = '$libdir/unaccent'
		    AND lexize_function.prosrc = 'unaccent_lexize'
		    AND lexize_function.provolatile = 'v'
		    AND lexize_function.proparallel = 's'
		    AND lexize_function.prokind = 'f'
		    AND NOT lexize_function.proretset
		    AND NOT lexize_function.proisstrict
		    AND NOT lexize_function.prosecdef
		    AND NOT lexize_function.proleakproof
		    AND lexize_function.proconfig IS NULL
		    AND lexize_function.prosupport = 0
		), trusted_unaccent_template AS (
		  SELECT template_row.oid, trusted_extension.oid AS extension_oid
		  FROM trusted_unaccent_extension trusted_extension
		  JOIN pg_catalog.pg_namespace template_namespace
		    ON template_namespace.oid = trusted_extension.extnamespace
		  JOIN trusted_unaccent_init_function init_function
		    ON init_function.extension_oid = trusted_extension.oid
		  JOIN trusted_unaccent_lexize_function lexize_function
		    ON lexize_function.extension_oid = trusted_extension.oid
		  JOIN pg_catalog.pg_ts_template template_row
		    ON template_row.tmplnamespace = template_namespace.oid
		   AND template_row.tmplname = 'unaccent'
		   AND template_row.tmplinit = init_function.oid
		   AND template_row.tmpllexize = lexize_function.oid
		  JOIN pg_catalog.pg_depend extension_dependency
		    ON extension_dependency.classid = 'pg_catalog.pg_ts_template'::regclass
		   AND extension_dependency.objid = template_row.oid
		   AND extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
		   AND extension_dependency.refobjid = trusted_extension.oid
		   AND extension_dependency.deptype = 'e'
		), trusted_unaccent_dictionary AS (
		  SELECT dictionary_row.oid
		  FROM trusted_unaccent_extension trusted_extension
		  JOIN pg_catalog.pg_namespace dictionary_namespace
		    ON dictionary_namespace.oid = trusted_extension.extnamespace
		  JOIN trusted_unaccent_template template_row
		    ON template_row.extension_oid = trusted_extension.oid
		  JOIN pg_catalog.pg_ts_dict dictionary_row
		    ON dictionary_row.dictnamespace = dictionary_namespace.oid
		   AND dictionary_row.dictname = 'unaccent'
		   AND dictionary_row.dicttemplate = template_row.oid
		   AND dictionary_row.dictinitoption = 'rules = ''unaccent'''
		  JOIN pg_catalog.pg_depend extension_dependency
		    ON extension_dependency.classid = 'pg_catalog.pg_ts_dict'::regclass
		   AND extension_dependency.objid = dictionary_row.oid
		   AND extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
		   AND extension_dependency.refobjid = trusted_extension.oid
		   AND extension_dependency.deptype = 'e'
		), trusted_unaccent_text_search_config AS (
		  SELECT config_row.oid
		  FROM pg_catalog.pg_ts_config config_row
		  JOIN pg_catalog.pg_ts_parser parser_row
		    ON parser_row.oid = config_row.cfgparser
		   AND parser_row.oid < 16384
		  JOIN pg_catalog.pg_namespace parser_namespace
		    ON parser_namespace.oid = parser_row.prsnamespace
		   AND parser_namespace.nspname = 'pg_catalog'
		  JOIN pg_catalog.pg_ts_config_map config_map
		    ON config_map.mapcfg = config_row.oid
		  JOIN pg_catalog.pg_ts_dict dictionary_row
		    ON dictionary_row.oid = config_map.mapdict
		  LEFT JOIN trusted_unaccent_dictionary trusted_dictionary
		    ON trusted_dictionary.oid = dictionary_row.oid
		  GROUP BY config_row.oid
		  HAVING count(*) > 0
		     AND pg_catalog.bool_and(
		       dictionary_row.oid < 16384 OR trusted_dictionary.oid IS NOT NULL
		     )
		)
		SELECT a.attname,
		       a.atttypid,
		       t.typarray,
		       a.attcollation <> 0 AND NOT column_collation.collisdeterministic
		         AS nondeterministic_collation,
		       a.attidentity::text,
		       a.attgenerated <> '',
		       a.attnotnull,
		       coalesce(primary_key.position, 0) AS primary_key_position,
		       coalesce(primary_key.catalog_safe, false) AS replay_key_catalog_safe,
		       EXISTS (
		         SELECT 1 FROM pg_catalog.pg_index primary_arbiter
		         WHERE primary_arbiter.indrelid = c.oid
		           AND primary_arbiter.indisprimary
		           AND primary_arbiter.indimmediate
		       ) AS primary_key_arbiter,
		       EXISTS (
		         SELECT 1 FROM pg_catalog.pg_index conflict_index
		         WHERE conflict_index.indrelid = c.oid
		           AND (conflict_index.indisunique OR conflict_index.indisexclusion)
		           AND a.attnum = ANY(conflict_index.indkey)
		       ) AS conflict_sensitive,
		       EXISTS (
		         SELECT 1 FROM pg_catalog.pg_index selective_index
		         WHERE selective_index.indrelid = c.oid
		           AND NOT (selective_index.indisunique OR selective_index.indisexclusion)
		           AND (selective_index.indexprs IS NOT NULL OR selective_index.indpred IS NOT NULL)
		       ) AS selective_updates,
		       EXISTS (
		         SELECT 1 FROM pg_catalog.pg_index cross_key_index
		         WHERE cross_key_index.indrelid = c.oid
		           AND (cross_key_index.indisunique OR cross_key_index.indisexclusion)
		           AND NOT cross_key_index.indisprimary
		       ) AS cross_key_conflicts,
		       c.relkind = 'r'
		       AND c.relpersistence = 'p'
		       AND NOT c.relhassubclass
		       AND NOT c.relispartition
		       AND NOT c.relrowsecurity
		       AND NOT c.relforcerowsecurity
		       AND c.relam = (
		         SELECT heap_am.oid FROM pg_catalog.pg_am heap_am WHERE heap_am.amname = 'heap'
		       )
		       AND NOT EXISTS (
		         SELECT 1 FROM pg_catalog.pg_trigger trigger_row
		         WHERE trigger_row.tgrelid = c.oid AND trigger_row.tgenabled IN ('R', 'A')
		       )
		       AND NOT EXISTS (
		         SELECT 1 FROM pg_catalog.pg_rewrite rule_row
		         WHERE rule_row.ev_class = c.oid
		           AND rule_row.rulename <> '_RETURN'
		           AND rule_row.ev_enabled IN ('R', 'A')
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_constraint check_constraint
		         CROSS JOIN LATERAL regexp_matches(
		           check_constraint.conbin::text,
		           '\{([A-Z][A-Z0-9_]*)[[:space:]]',
		           'g'
		         ) AS node_match
		         WHERE check_constraint.conrelid = c.oid
		           AND check_constraint.contype = 'c'
		           AND node_match[1] <> ALL (ARRAY[
		             'ARRAYEXPR',
		             'BOOLEXPR',
		             'CONST',
		             'FUNCEXPR',
		             'NULLTEST',
		             'OPEXPR',
		             'RELABELTYPE',
		             'SCALARARRAYOPEXPR',
		             'VAR'
		           ])
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_constraint check_constraint
		         CROSS JOIN LATERAL regexp_matches(
		           check_constraint.conbin::text,
		           ':([a-z_]*funcid) ([0-9]+)',
		           'g'
		         ) AS function_match
		         JOIN pg_catalog.pg_proc check_function
		           ON check_function.oid = function_match[2]::oid
		         JOIN pg_catalog.pg_namespace check_function_namespace
		           ON check_function_namespace.oid = check_function.pronamespace
		         WHERE check_constraint.conrelid = c.oid
		           AND check_constraint.contype = 'c'
		           AND function_match[2]::oid <> 0
		           AND (
		             check_function.oid >= 16384
		             OR
		             check_function_namespace.nspname <> 'pg_catalog'
		             OR check_function.provolatile <> 'i'
		           )
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_constraint check_constraint
		         JOIN pg_catalog.pg_depend check_dependency
		           ON check_dependency.classid = 'pg_catalog.pg_constraint'::regclass
		          AND check_dependency.objid = check_constraint.oid
		         LEFT JOIN pg_catalog.pg_operator check_operator
		           ON check_dependency.refclassid = 'pg_catalog.pg_operator'::regclass
		          AND check_operator.oid = check_dependency.refobjid
		         LEFT JOIN pg_catalog.pg_namespace check_operator_namespace
		           ON check_operator_namespace.oid = check_operator.oprnamespace
		         LEFT JOIN pg_catalog.pg_collation check_collation
		           ON check_dependency.refclassid = 'pg_catalog.pg_collation'::regclass
		          AND check_collation.oid = check_dependency.refobjid
		         LEFT JOIN pg_catalog.pg_namespace check_collation_namespace
		           ON check_collation_namespace.oid = check_collation.collnamespace
		         LEFT JOIN pg_catalog.pg_type check_type
		           ON check_dependency.refclassid = 'pg_catalog.pg_type'::regclass
		          AND check_type.oid = check_dependency.refobjid
		         WHERE check_constraint.conrelid = c.oid
		           AND check_constraint.contype = 'c'
		           AND (
		             (
		               check_dependency.refclassid = 'pg_catalog.pg_class'::regclass
		               AND check_dependency.refobjid <> c.oid
		             )
		             OR (
		               check_dependency.refclassid = 'pg_catalog.pg_operator'::regclass
		               AND (
		                 check_operator.oid >= 16384
		                 OR check_operator_namespace.nspname <> 'pg_catalog'
		               )
		             )
		             OR (
		               check_dependency.refclassid = 'pg_catalog.pg_collation'::regclass
		               AND NOT check_collation.collisdeterministic
		             )
		             OR (
		               check_dependency.refclassid = 'pg_catalog.pg_type'::regclass
		               AND check_type.oid >= 16384
		               AND check_type.typtype <> 'e'
		             )
		             OR check_dependency.refclassid NOT IN (
		               'pg_catalog.pg_class'::regclass,
		               'pg_catalog.pg_proc'::regclass,
		               'pg_catalog.pg_operator'::regclass,
		               'pg_catalog.pg_collation'::regclass,
		               'pg_catalog.pg_type'::regclass
		             )
		           )
		       )
		       AND NOT EXISTS (
		         SELECT 1 FROM pg_catalog.pg_attribute generated_attribute
		         WHERE generated_attribute.attrelid = c.oid
		           AND generated_attribute.attnum > 0
		           AND NOT generated_attribute.attisdropped
		           AND generated_attribute.attgenerated <> ''
		       )
		       AND NOT EXISTS (
		         SELECT 1 FROM pg_catalog.pg_index exclusion_index
		         WHERE exclusion_index.indrelid = c.oid
		           AND exclusion_index.indisexclusion
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_index maintained_index
		         JOIN LATERAL unnest(
		           maintained_index.indclass::oid[],
		           maintained_index.indcollation::oid[]
		         ) WITH ORDINALITY
		           AS maintained_entry(opclass_oid, collation_oid, ordinality) ON
		             maintained_entry.ordinality <= maintained_index.indnkeyatts
		         JOIN pg_catalog.pg_opclass maintained_opclass
		           ON maintained_opclass.oid = maintained_entry.opclass_oid
		         JOIN pg_catalog.pg_namespace maintained_opclass_namespace
		           ON maintained_opclass_namespace.oid = maintained_opclass.opcnamespace
		         LEFT JOIN pg_catalog.pg_collation maintained_collation
		           ON maintained_collation.oid = maintained_entry.collation_oid
		         LEFT JOIN pg_catalog.pg_namespace maintained_collation_namespace
		           ON maintained_collation_namespace.oid = maintained_collation.collnamespace
		         WHERE maintained_index.indrelid = c.oid
		           AND (
		             (
		               (
		                 maintained_opclass.oid >= 16384
		                 OR maintained_opclass_namespace.nspname <> 'pg_catalog'
		               )
		               AND NOT EXISTS (
		                 SELECT 1
		                 FROM pg_catalog.pg_depend extension_dependency
		                 JOIN pg_catalog.pg_extension trusted_extension
		                   ON extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
		                  AND trusted_extension.oid = extension_dependency.refobjid
		                 WHERE extension_dependency.classid = 'pg_catalog.pg_opclass'::regclass
		                   AND extension_dependency.objid = maintained_opclass.oid
		                   AND extension_dependency.deptype = 'e'
		                   AND trusted_extension.extname = 'btree_gin'
		               )
		             )
		             OR (
		               maintained_entry.collation_oid <> 0
		               AND NOT maintained_collation.collisdeterministic
		             )
		           )
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_index dependency_index
		         JOIN pg_catalog.pg_depend dependency
		           ON dependency.classid = 'pg_catalog.pg_class'::regclass
		          AND dependency.objid = dependency_index.indexrelid
		         LEFT JOIN pg_catalog.pg_proc dependency_function
		           ON dependency.refclassid = 'pg_catalog.pg_proc'::regclass
		          AND dependency_function.oid = dependency.refobjid
		         LEFT JOIN pg_catalog.pg_namespace dependency_function_namespace
		           ON dependency_function_namespace.oid = dependency_function.pronamespace
		         LEFT JOIN pg_catalog.pg_operator dependency_operator
		           ON dependency.refclassid = 'pg_catalog.pg_operator'::regclass
		          AND dependency_operator.oid = dependency.refobjid
		         LEFT JOIN pg_catalog.pg_namespace dependency_operator_namespace
		           ON dependency_operator_namespace.oid = dependency_operator.oprnamespace
		         LEFT JOIN pg_catalog.pg_collation dependency_collation
		           ON dependency.refclassid = 'pg_catalog.pg_collation'::regclass
		          AND dependency_collation.oid = dependency.refobjid
		         LEFT JOIN pg_catalog.pg_type dependency_type
		           ON dependency.refclassid = 'pg_catalog.pg_type'::regclass
		          AND dependency_type.oid = dependency.refobjid
		         LEFT JOIN pg_catalog.pg_ts_config dependency_ts_config
		           ON dependency.refclassid = 'pg_catalog.pg_ts_config'::regclass
		          AND dependency_ts_config.oid = dependency.refobjid
		         WHERE dependency_index.indrelid = c.oid
		           AND (
		             (
		               dependency_function.oid IS NOT NULL
		               AND (
		                 dependency_function.oid >= 16384
		                 OR dependency_function_namespace.nspname <> 'pg_catalog'
		               )
		             )
		             OR (
		               dependency_operator.oid IS NOT NULL
		               AND (
		                 dependency_operator.oid >= 16384
		                 OR dependency_operator_namespace.nspname <> 'pg_catalog'
		               )
		             )
		             OR (
		               dependency.refclassid = 'pg_catalog.pg_class'::regclass
		               AND dependency.refobjid <> c.oid
		             )
		             OR (
		               dependency.refclassid = 'pg_catalog.pg_collation'::regclass
		               AND NOT dependency_collation.collisdeterministic
		             )
		             OR (
		               dependency.refclassid = 'pg_catalog.pg_type'::regclass
		               AND dependency_type.oid >= 16384
		               AND dependency_type.typtype <> 'e'
		             )
		             OR (
		               dependency.refclassid = 'pg_catalog.pg_ts_config'::regclass
		               AND dependency_ts_config.oid >= 16384
		               AND NOT EXISTS (
		                 SELECT 1
		                 FROM trusted_unaccent_text_search_config trusted_config
		                 WHERE trusted_config.oid = dependency_ts_config.oid
		               )
		             )
		             OR dependency.refclassid NOT IN (
		               'pg_catalog.pg_class'::regclass,
		               'pg_catalog.pg_constraint'::regclass,
		               'pg_catalog.pg_opclass'::regclass,
		               'pg_catalog.pg_proc'::regclass,
		               'pg_catalog.pg_operator'::regclass,
		               'pg_catalog.pg_collation'::regclass,
		               'pg_catalog.pg_type'::regclass,
		               'pg_catalog.pg_ts_config'::regclass
		             )
		           )
		       ) AS relation_ordered_lane_safe,
		       NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_index plan_v4_index
		         JOIN pg_catalog.pg_depend plan_v4_dependency
		           ON plan_v4_dependency.classid = 'pg_catalog.pg_class'::regclass
		          AND plan_v4_dependency.objid = plan_v4_index.indexrelid
		         JOIN pg_catalog.pg_ts_config plan_v4_ts_config
		           ON plan_v4_dependency.refclassid =
		                'pg_catalog.pg_ts_config'::regclass
		          AND plan_v4_ts_config.oid = plan_v4_dependency.refobjid
		         WHERE plan_v4_index.indrelid = c.oid
		           AND plan_v4_ts_config.oid >= 16384
		       ) AS relation_ordered_lane_v4_ts_safe,
		       c.relpersistence = 'p'
		       AND NOT c.relhassubclass
		       AND NOT c.relispartition
		       AND c.relam = (
		         SELECT heap_am.oid FROM pg_catalog.pg_am heap_am WHERE heap_am.amname = 'heap'
		       )
		       AND NOT EXISTS (
		         SELECT 1 FROM pg_catalog.pg_attribute generated_attribute
		         WHERE generated_attribute.attrelid = c.oid
		           AND generated_attribute.attnum > 0
		           AND NOT generated_attribute.attisdropped
		           AND generated_attribute.attgenerated <> ''
		       )
		       AND NOT EXISTS (
		         SELECT 1 FROM pg_catalog.pg_index exclusion_index
		         WHERE exclusion_index.indrelid = c.oid
		           AND exclusion_index.indisexclusion
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_index maintained_index
		         JOIN LATERAL unnest(
		           maintained_index.indclass::oid[],
		           maintained_index.indcollation::oid[]
		         ) WITH ORDINALITY
		           AS maintained_entry(opclass_oid, collation_oid, ordinality) ON
		             maintained_entry.ordinality <= maintained_index.indnkeyatts
		         JOIN pg_catalog.pg_opclass maintained_opclass
		           ON maintained_opclass.oid = maintained_entry.opclass_oid
		         JOIN pg_catalog.pg_namespace maintained_opclass_namespace
		           ON maintained_opclass_namespace.oid = maintained_opclass.opcnamespace
		         LEFT JOIN pg_catalog.pg_collation maintained_collation
		           ON maintained_collation.oid = maintained_entry.collation_oid
		         LEFT JOIN pg_catalog.pg_namespace maintained_collation_namespace
		           ON maintained_collation_namespace.oid = maintained_collation.collnamespace
		         WHERE maintained_index.indrelid = c.oid
		           AND (
		             (
		               maintained_opclass_namespace.nspname <> 'pg_catalog'
		               AND NOT EXISTS (
		                 SELECT 1
		                 FROM pg_catalog.pg_depend extension_dependency
		                 JOIN pg_catalog.pg_extension trusted_extension
		                   ON extension_dependency.refclassid = 'pg_catalog.pg_extension'::regclass
		                  AND trusted_extension.oid = extension_dependency.refobjid
		                 WHERE extension_dependency.classid = 'pg_catalog.pg_opclass'::regclass
		                   AND extension_dependency.objid = maintained_opclass.oid
		                   AND extension_dependency.deptype = 'e'
		                   AND trusted_extension.extname = 'btree_gin'
		               )
		             )
		             OR (
		               maintained_entry.collation_oid <> 0
		               AND (
		                 maintained_collation_namespace.nspname <> 'pg_catalog'
		                 OR NOT maintained_collation.collisdeterministic
		               )
		             )
		           )
		       )
		       AND NOT EXISTS (
		         SELECT 1
		         FROM pg_catalog.pg_index dependency_index
		         JOIN pg_catalog.pg_depend dependency
		           ON dependency.classid = 'pg_catalog.pg_class'::regclass
		          AND dependency.objid = dependency_index.indexrelid
		         LEFT JOIN pg_catalog.pg_proc dependency_function
		           ON dependency.refclassid = 'pg_catalog.pg_proc'::regclass
		          AND dependency_function.oid = dependency.refobjid
		         LEFT JOIN pg_catalog.pg_namespace dependency_function_namespace
		           ON dependency_function_namespace.oid = dependency_function.pronamespace
		         LEFT JOIN pg_catalog.pg_operator dependency_operator
		           ON dependency.refclassid = 'pg_catalog.pg_operator'::regclass
		          AND dependency_operator.oid = dependency.refobjid
		         LEFT JOIN pg_catalog.pg_namespace dependency_operator_namespace
		           ON dependency_operator_namespace.oid = dependency_operator.oprnamespace
		         WHERE dependency_index.indrelid = c.oid
		           AND (
		             (
		               dependency_function.oid IS NOT NULL
		               AND dependency_function_namespace.nspname <> 'pg_catalog'
		             )
		             OR (
		               dependency_operator.oid IS NOT NULL
		               AND dependency_operator_namespace.nspname <> 'pg_catalog'
		             )
		           )
		       ) AS relation_ordered_lane_v3_safe,
		       c.relkind = 'r'
		         AND NOT c.relrowsecurity
		         AND NOT c.relforcerowsecurity
		         AND a.attgenerated = ''
		         AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_trigger trigger_row
		           WHERE trigger_row.tgrelid = c.oid AND trigger_row.tgenabled IN ('R', 'A')
		         )
		         AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_rewrite rule_row
		           WHERE rule_row.ev_class = c.oid
		             AND rule_row.rulename <> '_RETURN'
		             AND rule_row.ev_enabled IN ('R', 'A')
		         )
		         AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_constraint constraint_row
		           WHERE constraint_row.conrelid = c.oid AND constraint_row.contype = 'c'
		         )
		         AND NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_index index_row
		           WHERE index_row.indrelid = c.oid
		             AND (index_row.indisunique OR index_row.indisexclusion)
		             AND (index_row.indexprs IS NOT NULL OR index_row.indpred IS NOT NULL)
		         ) AS set_dml_safe,
		       t.oid < 16384 AS built_in_type,
		       (t.oid < 16384 OR t.typtype = 'e' OR (
		          t.typtype = 'b'
		          AND t.typcategory = 'A'
		          AND t.typinput = 'pg_catalog.array_in'::regproc
		          AND t.typoutput = 'pg_catalog.array_out'::regproc
		          AND element_type.typtype = 'e'
		       ))
		         AS replay_lane_payload_safe,
		       pg_catalog.pg_relation_size(c.oid) AS heap_bytes,
		       coalesce(io.heap_blks_read, 0) AS heap_blocks_read,
		       coalesce(io.heap_blks_hit, 0) AS heap_blocks_hit
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_catalog.pg_type element_type ON element_type.oid = t.typelem
		LEFT JOIN pg_catalog.pg_collation column_collation
		  ON column_collation.oid = a.attcollation
		LEFT JOIN LATERAL (
		         SELECT primary_entry.ordinality::integer AS position,
		                opclass.opcdefault
		                  AND (primary_entry.collation_oid = 0 OR pk_collation.collisdeterministic)
		                  AS catalog_safe
		         FROM pg_catalog.pg_index primary_index
		         JOIN LATERAL unnest(
		           primary_index.indkey::smallint[],
		           primary_index.indclass::oid[],
		           primary_index.indcollation::oid[]
		         ) WITH ORDINALITY
		           AS primary_entry(attnum, opclass_oid, collation_oid, ordinality) ON true
		         JOIN pg_catalog.pg_opclass opclass ON opclass.oid = primary_entry.opclass_oid
		         LEFT JOIN pg_catalog.pg_collation pk_collation
		           ON pk_collation.oid = primary_entry.collation_oid
		         WHERE primary_index.indrelid = c.oid
		           AND primary_index.indisprimary
		           AND primary_entry.attnum = a.attnum
		           AND primary_entry.ordinality <= primary_index.indnkeyatts
		       ) primary_key ON true
		LEFT JOIN pg_catalog.pg_statio_all_tables io ON io.relid = c.oid
		WHERE n.nspname = $1 AND c.relname = $2
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`, source.Namespace, source.Name)
	if err != nil {
		return nil, fmt.Errorf("cdc: inspect target relation %s.%s: %w", source.Namespace, source.Name, err)
	}
	defer rows.Close()
	result := &targetRelation{
		source: *source,
		quoted: pgx.Identifier{source.Namespace, source.Name}.Sanitize(),
		capabilities: targetRelationCapabilities{
			relationLane:          true,
			relationOrderedLane:   true,
			relationOrderedLaneV4: true,
			relationOrderedLaneV3: true,
			primaryKeyArbiter:     true,
			keyedSetDML:           true,
			binaryCopy:            true,
			textCopyStage:         true,
		},
	}
	hasSelectiveUpdates := false
	for rows.Next() {
		var column targetColumn
		var replayKeyCatalogSafe, primaryKeyArbiter, setDMLSafe, builtIn, lanePayloadSafe bool
		var selectiveUpdates, crossKeyConflicts bool
		var relationOrderedLaneSafe, relationOrderedLaneV4TSSafe, relationOrderedLaneV3Safe bool
		var heapBytes, heapBlocksRead, heapBlocksHit int64
		if err := rows.Scan(
			&column.name, &column.oid, &column.arrayOID, &column.nondeterministicCollation,
			&column.identity,
			&column.generated, &column.notNull, &column.primaryPos, &replayKeyCatalogSafe,
			&primaryKeyArbiter,
			&column.conflicting, &selectiveUpdates, &crossKeyConflicts,
			&relationOrderedLaneSafe, &relationOrderedLaneV4TSSafe,
			&relationOrderedLaneV3Safe, &setDMLSafe,
			&builtIn, &lanePayloadSafe, &heapBytes,
			&heapBlocksRead, &heapBlocksHit,
		); err != nil {
			return nil, err
		}
		// This is a relation-level admission decision repeated on every catalog
		// row. Apply it even when the target has only generated columns; those
		// columns are intentionally skipped by the writable-column gates below.
		result.capabilities.relationOrderedLane =
			result.capabilities.relationOrderedLane && relationOrderedLaneSafe
		// Plan v5 changes only the custom text-search-config dependency gate.
		// Combining the v5 result with the old unconditional custom-config
		// rejection reconstructs the complete plan-v4 catalog decision exactly.
		result.capabilities.relationOrderedLaneV4 =
			result.capabilities.relationOrderedLaneV4 &&
				relationOrderedLaneSafe && relationOrderedLaneV4TSSafe
		result.capabilities.relationOrderedLaneV3 =
			result.capabilities.relationOrderedLaneV3 && relationOrderedLaneV3Safe
		result.capabilities.primaryKeyArbiter =
			result.capabilities.primaryKeyArbiter && primaryKeyArbiter
		// Generated columns are omitted from every target INSERT/UPDATE column
		// list and maintained by PostgreSQL. Their own non-writability must not
		// disable set DML or selective updates for the writable relation columns.
		if !column.generated {
			// Cross-transaction ordering depends only on the target primary key,
			// but payload type input must also be free of user-defined side effects.
			// Built-ins and enums (including enum arrays) satisfy that invariant;
			// domains and arbitrary extension/base types retain serial source order.
			setLaneSafe := setDMLSafe && lanePayloadSafe
			result.capabilities.relationLane =
				result.capabilities.relationLane && setLaneSafe
			result.capabilities.relationOrderedLane =
				result.capabilities.relationOrderedLane && lanePayloadSafe
			result.capabilities.relationOrderedLaneV4 =
				result.capabilities.relationOrderedLaneV4 && lanePayloadSafe
			result.capabilities.relationOrderedLaneV3 =
				result.capabilities.relationOrderedLaneV3 && setLaneSafe
			result.capabilities.keyedSetDML = result.capabilities.keyedSetDML && setDMLSafe
			result.capabilities.binaryCopy = result.capabilities.binaryCopy && setDMLSafe && builtIn
			result.capabilities.textCopyStage = result.capabilities.textCopyStage && setDMLSafe
		}
		hasSelectiveUpdates = hasSelectiveUpdates || selectiveUpdates
		result.capabilities.crossKeyConflicts =
			result.capabilities.crossKeyConflicts || crossKeyConflicts
		result.heapBytes = heapBytes
		result.heapBlocksRead = heapBlocksRead
		result.heapBlocksHit = heapBlocksHit
		if column.identity == "a" {
			result.overrideIdentity = true
		}
		column.quoted = pgx.Identifier{column.name}.Sanitize()
		column.lanePayloadTextOnly = !builtIn
		column.primary = column.primaryPos > 0
		column.replayKeySafe = replayKeyCatalogSafe && replayKeyTargetTypeSafe(column.oid)
		if column.generated {
			result.generatedColumns = append(result.generatedColumns, column)
		} else {
			result.columns = append(result.columns, column)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Selective updates only require keyed set DML inside the transaction's
	// original source-order position. Custom types can make a relation unsafe
	// for a cross-transaction lane because they need typed/text transport, but
	// they do not make the compare-first update itself unsafe. Coupling this to
	// relationLane caused complete-row pgoutput tuples on enum-bearing tables to
	// rewrite every partial/expression index even when only one column changed.
	result.capabilities.selectiveUpdates =
		hasSelectiveUpdates && result.capabilities.keyedSetDML
	if len(result.columns) == 0 && len(result.generatedColumns) == 0 {
		// Naming the absent relation matters most for a partition: publishing a
		// partitioned table streams changes identified by the partition, so a
		// target missing one reports it here on the first write to it.
		return nil, divergenceFor(result, 0, "target relation does not exist or has no columns")
	}
	sourceIndexes := make(map[string]int, len(source.Columns))
	for i := range source.Columns {
		sourceIndexes[source.Columns[i].Name] = i
	}
	for i := range result.columns {
		sourceIndex, ok := sourceIndexes[result.columns[i].name]
		if !ok {
			return nil, divergenceFor(result, 0, fmt.Sprintf(
				"writable target column %q is absent from pgoutput relation", result.columns[i].name,
			))
		}
		result.columns[i].sourceIndex = sourceIndex
		result.columns[i].key = source.Columns[sourceIndex].Flags&1 != 0
		result.columns[i].replayKeySafe = result.columns[i].replayKeySafe &&
			source.Columns[sourceIndex].Type == result.columns[i].oid
	}
	hasReplayPrimaryKey := false
	for i := range result.columns {
		if !result.columns[i].primary {
			continue
		}
		hasReplayPrimaryKey = true
		result.capabilities.relationLane =
			result.capabilities.relationLane && result.columns[i].replayKeySafe
	}
	result.capabilities.relationLane =
		result.capabilities.relationLane && hasReplayPrimaryKey
	targetColumns := make(map[string]targetColumn, len(result.columns)+len(result.generatedColumns))
	for _, column := range result.columns {
		targetColumns[column.name] = column
	}
	for _, column := range result.generatedColumns {
		targetColumns[column.name] = column
	}
	result.mappedColumns = make([]targetColumn, 0, len(source.Columns))
	for sourceIndex, sourceColumn := range source.Columns {
		column, ok := targetColumns[sourceColumn.Name]
		if !ok {
			return nil, divergenceFor(result, 0, fmt.Sprintf(
				"pgoutput column %q is absent from target relation", sourceColumn.Name,
			))
		}
		column.sourceIndex = sourceIndex
		column.key = sourceColumn.Flags&1 != 0
		result.mappedColumns = append(result.mappedColumns, column)
	}
	return result, nil
}

// replayKeyTargetTypeSafe admits only built-in types whose pgoutput text is a
// canonical representative of PostgreSQL equality. Numeric scale, bpchar
// padding, floating-point signed zero, timetz offsets, and custom types can
// produce distinct bytes that compare equal through a primary-key index.
func replayKeyTargetTypeSafe(oid uint32) bool {
	switch oid {
	case pgtype.BoolOID,
		pgtype.ByteaOID,
		pgtype.Int2OID,
		pgtype.Int4OID,
		pgtype.Int8OID,
		pgtype.TextOID,
		pgtype.VarcharOID,
		pgtype.DateOID,
		pgtype.TimeOID,
		pgtype.TimestampOID,
		pgtype.TimestamptzOID,
		pgtype.UUIDOID:
		return true
	default:
		return false
	}
}

func (a *Applier) applySpilledChanges(
	replay *applyPipeline,
	relations map[uint32]*targetRelation,
	spill *TransactionSpill,
	collector *sampleCollector,
) error {
	var pending []Change
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		relation := relations[pending[0].RelationOID]
		var err error
		switch pending[0].Kind {
		case ChangeInsert:
			err = applyInserts(replay, relation, pending)
			if err == nil {
				collector.addAll(pending)
			}
		case ChangeTruncate:
			err = applyTruncates(replay, relations, pending)
		}
		pending = pending[:0]
		return err
	}
	err := spill.forEachChange(func(change Change) error {
		relation := relations[change.RelationOID]
		if relation == nil {
			return divergenceFor(nil, change.Kind, "required relation metadata is missing")
		}
		switch change.Kind {
		case ChangeInsert:
			if len(pending) != 0 &&
				(pending[0].Kind != ChangeInsert || pending[0].RelationOID != change.RelationOID) {
				if err := flush(); err != nil {
					return err
				}
			}
			pending = append(pending, change)
			if len(pending) >= insertChunkRows(len(relation.columns)) {
				return flush()
			}
			return nil
		case ChangeTruncate:
			if len(pending) != 0 &&
				(pending[0].Kind != ChangeTruncate || !sameTruncateOptions(pending[0], change)) {
				if err := flush(); err != nil {
					return err
				}
			}
			pending = append(pending, change)
			return nil
		case ChangeUpdate:
			if err := flush(); err != nil {
				return err
			}
			if err := applyUpdate(replay, relation, &change); err != nil {
				return err
			}
			collector.add(&change)
			return nil
		case ChangeDelete:
			if err := flush(); err != nil {
				return err
			}
			if err := applyDelete(replay, relation, &change); err != nil {
				return err
			}
			collector.add(&change)
			return nil
		default:
			return divergenceFor(relation, change.Kind, "unknown change kind")
		}
	})
	if err != nil {
		return err
	}
	return flush()
}

// rawParam is one bind parameter for the extended query protocol. isNull is
// carried separately because the protocol distinguishes a nil value (SQL NULL)
// from a non-nil zero-length one (a zero-length value), while a Go []byte does
// not distinguish nil from empty at every producer. Inferring nullness from the
// data pointer silently turned an empty string into NULL.
type rawParam struct {
	data   []byte
	oid    uint32
	format int16
	isNull bool
}

func arrayParamForColumn(
	relation *targetRelation,
	column targetColumn,
	datums []TupleDatum,
	kind ChangeKind,
) (rawParam, bool, error) {
	if column.arrayOID == 0 || len(datums) == 0 {
		return rawParam{}, false, nil
	}
	format := DatumNull
	hasNull := false
	for _, datum := range datums {
		if _, err := datumParamForColumn(relation, column, datum, kind); err != nil {
			return rawParam{}, false, err
		}
		if datum.Kind == DatumNull {
			hasNull = true
			continue
		}
		if format == DatumNull {
			format = datum.Kind
		} else if format != datum.Kind {
			return rawParam{}, false, nil
		}
	}
	if format == DatumNull || format == DatumBinary {
		data := make([]byte, 0, 20+len(datums)*8)
		data = binary.BigEndian.AppendUint32(data, 1)
		if hasNull {
			data = binary.BigEndian.AppendUint32(data, 1)
		} else {
			data = binary.BigEndian.AppendUint32(data, 0)
		}
		data = binary.BigEndian.AppendUint32(data, column.oid)
		data = binary.BigEndian.AppendUint32(data, uint32(len(datums)))
		data = binary.BigEndian.AppendUint32(data, 1)
		for _, datum := range datums {
			if datum.Kind == DatumNull {
				data = binary.BigEndian.AppendUint32(data, ^uint32(0))
				continue
			}
			data = binary.BigEndian.AppendUint32(data, uint32(len(datum.Data)))
			data = append(data, datum.Data...)
		}
		return rawParam{data: data, oid: column.arrayOID, format: 1}, true, nil
	}

	var data strings.Builder
	data.Grow(2 + len(datums)*8)
	data.WriteByte('{')
	for i, datum := range datums {
		if i != 0 {
			data.WriteByte(',')
		}
		if datum.Kind == DatumNull {
			data.WriteString("NULL")
			continue
		}
		data.WriteByte('"')
		for _, value := range datum.Data {
			if value == '\\' || value == '"' {
				data.WriteByte('\\')
			}
			data.WriteByte(value)
		}
		data.WriteByte('"')
	}
	data.WriteByte('}')
	return rawParam{data: []byte(data.String()), oid: column.arrayOID}, true, nil
}

const (
	applyPipelineWindow      = 256
	applyBatchPipelineWindow = 65536
)

type applyResultKind byte

const (
	applyCommandResult applyResultKind = iota
	applyPrepareResult
	applyDeallocateResult
)

type applyExpectation struct {
	resultKind       applyResultKind
	relation         *targetRelation
	kind             ChangeKind
	description      string
	expectedRows     int64
	expectedOrdinals int
	allowMissingRows bool // Only for DELETEs using a catalog-validated primary key.
	expectedTag      string
	progressGuard    bool
	statement        string
	paramOIDs        []uint32
	consumeRows      func(*pgconn.ResultReader) error
}

type applyPipeline struct {
	ctx          context.Context
	conn         *pgconn.PgConn
	pipeline     *pgconn.Pipeline
	statements   *applyStatementCache
	expectations []applyExpectation
	commands     int
	syncWindow   int
	closed       bool
}

func newApplyPipeline(
	ctx context.Context,
	conn *pgconn.PgConn,
	statements *applyStatementCache,
) *applyPipeline {
	return &applyPipeline{
		ctx:        ctx,
		conn:       conn,
		pipeline:   conn.StartPipeline(ctx),
		statements: statements,
		syncWindow: applyPipelineWindow,
	}
}

func (p *applyPipeline) begin() {
	p.queueUnprepared("BEGIN", nil, applyExpectation{
		description: "begin target transaction", expectedRows: -1, expectedTag: "BEGIN",
	})
}

func (p *applyPipeline) commit() {
	p.queueUnprepared("COMMIT", nil, applyExpectation{
		description: "commit target transaction", expectedRows: -1, expectedTag: "COMMIT",
	})
}

func (p *applyPipeline) queueProgress(
	streamID, generation string,
	expectedLSN LSN,
	remoteLSN LSN,
	transactions, rows int64,
) {
	p.queueUnprepared(
		streamProgressSQL,
		streamProgressParams(streamID, generation, expectedLSN, remoteLSN, transactions, rows),
		applyExpectation{
			description: "update transactional apply progress", expectedRows: 1,
			progressGuard: true,
		},
	)
}

func (p *applyPipeline) queue(
	sql string,
	params []rawParam,
	expectation applyExpectation,
) error {
	values, oids, formats := rawParamArrays(params)
	statement, added, evicted := p.statements.acquire(sql, oids)
	if evicted != nil {
		p.pipeline.SendDeallocate(evicted.name)
		p.expectations = append(p.expectations, applyExpectation{
			resultKind:  applyDeallocateResult,
			description: "deallocate replay statement " + evicted.name,
			statement:   evicted.name,
		})
	}
	if statement == nil {
		p.pipeline.SendQueryParams(sql, values, oids, formats, nil)
	} else {
		if added {
			p.pipeline.SendPrepare(statement.name, sql, oids)
			p.expectations = append(p.expectations, applyExpectation{
				resultKind:  applyPrepareResult,
				relation:    expectation.relation,
				kind:        expectation.kind,
				description: "prepare " + expectation.description,
				statement:   statement.name,
				paramOIDs:   append([]uint32(nil), oids...),
			})
		}
		p.pipeline.SendQueryPrepared(statement.name, values, formats, nil)
	}
	p.expectations = append(p.expectations, expectation)
	p.commands++
	if p.syncWindow > 0 && p.commands >= p.syncWindow {
		return p.sync()
	}
	return nil
}

func (p *applyPipeline) queueUnprepared(
	sql string,
	params []rawParam,
	expectation applyExpectation,
) {
	values, oids, formats := rawParamArrays(params)
	p.pipeline.SendQueryParams(sql, values, oids, formats, nil)
	p.expectations = append(p.expectations, expectation)
	p.commands++
}

func rawParamArrays(params []rawParam) ([][]byte, []uint32, []int16) {
	values := paramValues(params)
	oids := make([]uint32, len(params))
	formats := make([]int16, len(params))
	for i := range params {
		oids[i] = params[i].oid
		formats[i] = params[i].format
	}
	return values, oids, formats
}

func (p *applyPipeline) sync() error {
	if len(p.expectations) == 0 {
		return nil
	}
	expectations := p.expectations
	p.expectations = nil
	p.commands = 0
	if err := p.pipeline.Sync(); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: synchronize replay pipeline: %w", err))
	}

	resultIndex := 0
	var firstErr error
	for {
		result, err := p.pipeline.GetResults()
		if err != nil {
			if firstErr == nil {
				firstErr = pipelineResultError(expectations, resultIndex, err)
			}
			if resultIndex < len(expectations) {
				resultIndex++
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				return firstErr
			}
			continue
		}
		if _, ok := result.(*pgconn.PipelineSync); ok {
			if resultIndex != len(expectations) && firstErr == nil {
				firstErr = fmt.Errorf(
					"cdc: replay pipeline returned %d results, expected %d",
					resultIndex, len(expectations),
				)
			}
			return firstErr
		}
		if resultIndex >= len(expectations) {
			closeErr := closeUnexpectedPipelineResult(result)
			if firstErr == nil {
				firstErr = errors.Join(
					fmt.Errorf("cdc: replay pipeline returned unexpected result type %T", result),
					closeErr,
				)
			}
			continue
		}
		expectation := expectations[resultIndex]
		resultIndex++
		switch expectation.resultKind {
		case applyCommandResult:
			reader, ok := result.(*pgconn.ResultReader)
			if !ok {
				if firstErr == nil {
					firstErr = errors.Join(
						fmt.Errorf(
							"cdc: %s returned result type %T, expected command result",
							expectation.description, result,
						),
						closeUnexpectedPipelineResult(result),
					)
				}
				continue
			}
			var ordinalErr error
			if expectation.consumeRows != nil {
				ordinalErr = expectation.consumeRows(reader)
			} else {
				ordinalErr = expectation.validateOrdinals(reader)
			}
			tag, closeErr := reader.Close()
			if closeErr != nil {
				if firstErr == nil {
					firstErr = expectation.classify(closeErr)
				}
				continue
			}
			if ordinalErr != nil && firstErr == nil {
				firstErr = ordinalErr
			}
			if expectation.expectedRows >= 0 && tag.RowsAffected() != expectation.expectedRows &&
				(!expectation.allowMissingRows || tag.RowsAffected() > expectation.expectedRows) && firstErr == nil {
				firstErr = divergenceFor(expectation.relation, expectation.kind, fmt.Sprintf(
					"affected %d rows, expected %d", tag.RowsAffected(), expectation.expectedRows,
				))
			}
			if expectation.expectedTag != "" && tag.String() != expectation.expectedTag && firstErr == nil {
				firstErr = fmt.Errorf(
					"cdc: %s returned command tag %q, expected %q",
					expectation.description, tag.String(), expectation.expectedTag,
				)
			}
		case applyPrepareResult:
			description, ok := result.(*pgconn.StatementDescription)
			if !ok {
				if firstErr == nil {
					firstErr = errors.Join(
						fmt.Errorf(
							"cdc: %s returned result type %T, expected statement description",
							expectation.description, result,
						),
						closeUnexpectedPipelineResult(result),
					)
				}
				continue
			}
			if !slices.Equal(description.ParamOIDs, expectation.paramOIDs) && firstErr == nil {
				firstErr = fmt.Errorf(
					"cdc: prepared replay statement %s parameter OIDs %v, expected %v",
					expectation.statement, description.ParamOIDs, expectation.paramOIDs,
				)
			}
		case applyDeallocateResult:
			if _, ok := result.(*pgconn.CloseComplete); !ok && firstErr == nil {
				firstErr = errors.Join(
					fmt.Errorf(
						"cdc: %s returned result type %T, expected close completion",
						expectation.description, result,
					),
					closeUnexpectedPipelineResult(result),
				)
			}
		}
	}
}

func (expectation applyExpectation) validateOrdinals(reader *pgconn.ResultReader) error {
	if expectation.expectedOrdinals == 0 {
		return nil
	}
	seen := make([]bool, expectation.expectedOrdinals)
	var result error
	for reader.NextRow() {
		values := reader.Values()
		if len(values) != 1 {
			if result == nil {
				result = divergenceFor(expectation.relation, expectation.kind, fmt.Sprintf(
					"batched replay returned %d identity columns, expected 1", len(values),
				))
			}
			continue
		}
		ordinal, err := strconv.Atoi(string(values[0]))
		if err != nil || ordinal < 0 || ordinal >= len(seen) {
			if result == nil {
				result = divergenceFor(expectation.relation, expectation.kind, fmt.Sprintf(
					"batched replay returned invalid identity ordinal %q", values[0],
				))
			}
			continue
		}
		if seen[ordinal] {
			if result == nil {
				result = divergenceFor(expectation.relation, expectation.kind, fmt.Sprintf(
					"batched replay matched identity ordinal %d more than once", ordinal,
				))
			}
			continue
		}
		seen[ordinal] = true
	}
	if result != nil {
		return result
	}
	for ordinal, matched := range seen {
		if !matched && !expectation.allowMissingRows {
			return divergenceFor(expectation.relation, expectation.kind, fmt.Sprintf(
				"batched replay did not match identity ordinal %d", ordinal,
			))
		}
	}
	return nil
}

func closeUnexpectedPipelineResult(result any) error {
	if reader, ok := result.(*pgconn.ResultReader); ok {
		_, err := reader.Close()
		return err
	}
	return nil
}

func pipelineResultError(expectations []applyExpectation, index int, err error) error {
	if index < len(expectations) {
		return expectations[index].classify(err)
	}
	return classifyApplyError(nil, 0, fmt.Errorf("cdc: read replay pipeline result: %w", err))
}

func (expectation applyExpectation) classify(err error) error {
	if expectation.progressGuard && isProgressGuardError(err) {
		return fmt.Errorf("%w: %v", ErrStreamGenerationMismatch, err)
	}
	return classifyApplyError(expectation.relation, expectation.kind, fmt.Errorf(
		"%s: %w", expectation.description, err,
	))
}

func (p *applyPipeline) close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	closeErr := p.pipeline.Close()
	if closeErr != nil {
		closeErr = classifyApplyError(nil, 0, fmt.Errorf("cdc: close replay pipeline: %w", closeErr))
	}
	return closeErr
}

func (p *applyPipeline) suspend() error {
	if len(p.expectations) != 0 {
		if err := p.sync(); err != nil {
			return err
		}
	}
	if err := p.pipeline.Close(); err != nil {
		p.pipeline = nil
		p.resume()
		return classifyApplyError(nil, 0, fmt.Errorf(
			"cdc: suspend replay pipeline: %w", err,
		))
	}
	p.pipeline = nil
	return nil
}

func (p *applyPipeline) resume() {
	if p.pipeline == nil {
		p.pipeline = p.conn.StartPipeline(p.ctx)
	}
}

func (p *applyPipeline) copyFrom(
	relation *targetRelation,
	kind ChangeKind,
	description string,
	copySQL string,
	data []byte,
	expectedRows int,
) error {
	if err := p.suspend(); err != nil {
		return err
	}
	if p.conn.TxStatus() != 'T' {
		p.resume()
		return fmt.Errorf(
			"cdc: target transaction status before %s is %q, want %q",
			description, p.conn.TxStatus(), 'T',
		)
	}
	tag, copyErr := p.conn.CopyFrom(p.ctx, bytes.NewReader(data), copySQL)
	p.resume()
	if copyErr != nil {
		return classifyApplyError(relation, kind, fmt.Errorf("%s: %w", description, copyErr))
	}
	if tag.RowsAffected() != int64(expectedRows) {
		return divergenceFor(relation, kind, fmt.Sprintf(
			"%s affected %d rows, expected %d", description, tag.RowsAffected(), expectedRows,
		))
	}
	return nil
}

const minimumTextCopyStageRows = 64

const minimumBinaryCopyStageRows = 64

// loadBinaryCopyStage is the built-in-type counterpart of loadTextCopyStage.
// It mirrors crdb-to-pg's fast path: COPY a lane group into a transaction-local
// typed stage, then consume it with one set-based statement. The stage and DML
// live in the same replay-work transaction as the durable receipt.
func (p *applyPipeline) loadBinaryCopyStage(
	relation *targetRelation,
	kind ChangeKind,
	columns []targetColumn,
	values []TupleDatum,
	rowCount int,
) (string, bool, error) {
	if rowCount < minimumBinaryCopyStageRows || len(columns) == 0 ||
		!relation.capabilities.binaryCopy {
		return "", false, nil
	}
	data, supported, err := binaryCopyStageData(relation, kind, columns, values, rowCount)
	if err != nil || !supported {
		return "", supported, err
	}
	stage := textCopyStageName(relation, kind, columns)
	var create strings.Builder
	create.WriteString("CREATE TEMP TABLE IF NOT EXISTS ")
	create.WriteString(stage)
	create.WriteString(" ON COMMIT DELETE ROWS AS SELECT 0::bigint AS ordinal")
	for i, column := range columns {
		create.WriteByte(',')
		create.WriteString("pgmigrate_target.")
		create.WriteString(column.quoted)
		fmt.Fprintf(&create, " AS column_%d", i)
	}
	create.WriteString(" FROM ")
	create.WriteString(relation.quoted)
	create.WriteString(" AS pgmigrate_target WITH NO DATA")
	p.queueUnprepared(create.String(), nil, applyExpectation{
		relation: relation, kind: kind,
		description: "create binary replay stage for " + relation.quoted, expectedRows: -1,
	})
	p.queueUnprepared("TRUNCATE "+stage, nil, applyExpectation{
		relation: relation, kind: kind,
		description: "clear binary replay stage for " + relation.quoted, expectedRows: -1,
	})

	var copySQL strings.Builder
	copySQL.WriteString("COPY ")
	copySQL.WriteString(stage)
	copySQL.WriteString(" (ordinal")
	for i := range columns {
		fmt.Fprintf(&copySQL, ",column_%d", i)
	}
	copySQL.WriteString(") FROM STDIN BINARY")
	if err := p.copyFrom(
		relation, kind, "binary copy into replay stage for "+relation.quoted,
		copySQL.String(), data, rowCount,
	); err != nil {
		return "", true, err
	}
	return stage, true, nil
}

func binaryCopyStageData(
	relation *targetRelation,
	kind ChangeKind,
	columns []targetColumn,
	values []TupleDatum,
	rowCount int,
) ([]byte, bool, error) {
	if rowCount < 0 || len(values) != rowCount*len(columns) {
		return nil, true, divergenceFor(relation, kind, fmt.Sprintf(
			"binary stage has %d values for %d rows of %d columns",
			len(values), rowCount, len(columns),
		))
	}
	estimatedBytes := 21 + rowCount*(14+len(columns)*4)
	for row := 0; row < rowCount; row++ {
		for columnIndex, column := range columns {
			datum := values[row*len(columns)+columnIndex]
			switch datum.Kind {
			case DatumNull:
			case DatumBinary:
				if _, err := datumParamForColumn(relation, column, datum, kind); err != nil {
					return nil, true, err
				}
				estimatedBytes += len(datum.Data)
			default:
				return nil, false, nil
			}
		}
	}
	data := make([]byte, 0, estimatedBytes)
	data = append(data, []byte("PGCOPY\n\xff\r\n\x00")...)
	data = binary.BigEndian.AppendUint32(data, 0)
	data = binary.BigEndian.AppendUint32(data, 0)
	for row := 0; row < rowCount; row++ {
		data = binary.BigEndian.AppendUint16(data, uint16(len(columns)+1))
		data = binary.BigEndian.AppendUint32(data, 8)
		data = binary.BigEndian.AppendUint64(data, uint64(row))
		for columnIndex := range columns {
			datum := values[row*len(columns)+columnIndex]
			if datum.Kind == DatumNull {
				data = binary.BigEndian.AppendUint32(data, ^uint32(0))
				continue
			}
			data = binary.BigEndian.AppendUint32(data, uint32(len(datum.Data)))
			data = append(data, datum.Data...)
		}
	}
	data = binary.BigEndian.AppendUint16(data, ^uint16(0))
	return data, true, nil
}

// loadTextCopyStage copies text pgoutput values into a target-typed temporary
// relation. This is the escape hatch that parameter arrays cannot provide
// efficiently for user-defined types: PostgreSQL's COPY input functions do the
// conversion once, then one set-based target statement consumes the stage.
// The stage lives in the same target transaction as DML and progress.
func (p *applyPipeline) loadTextCopyStage(
	relation *targetRelation,
	kind ChangeKind,
	columns []targetColumn,
	values []TupleDatum,
	rowCount int,
) (string, bool, error) {
	if rowCount < minimumTextCopyStageRows || len(columns) == 0 ||
		!relation.capabilities.textCopyStage || !textCopyStagePreferred(columns) {
		return "", false, nil
	}
	data, supported, err := textCopyStageData(relation, kind, columns, values, rowCount)
	if err != nil || !supported {
		return "", supported, err
	}

	stage := textCopyStageName(relation, kind, columns)
	var create strings.Builder
	create.WriteString("CREATE TEMP TABLE IF NOT EXISTS ")
	create.WriteString(stage)
	create.WriteString(" ON COMMIT DELETE ROWS AS SELECT 0::bigint AS ordinal")
	for i, column := range columns {
		create.WriteByte(',')
		create.WriteString("pgmigrate_target.")
		create.WriteString(column.quoted)
		fmt.Fprintf(&create, " AS column_%d", i)
	}
	create.WriteString(" FROM ")
	create.WriteString(relation.quoted)
	create.WriteString(" AS pgmigrate_target WITH NO DATA")
	p.queueUnprepared(create.String(), nil, applyExpectation{
		relation: relation, kind: kind,
		description: "create typed replay stage for " + relation.quoted, expectedRows: -1,
	})
	p.queueUnprepared("TRUNCATE "+stage, nil, applyExpectation{
		relation: relation, kind: kind,
		description: "clear typed replay stage for " + relation.quoted, expectedRows: -1,
	})

	var copySQL strings.Builder
	copySQL.WriteString("COPY ")
	copySQL.WriteString(stage)
	copySQL.WriteString(" (ordinal")
	for i := range columns {
		fmt.Fprintf(&copySQL, ",column_%d", i)
	}
	copySQL.WriteString(") FROM STDIN")
	if err := p.copyFrom(
		relation, kind, "text copy into typed stage for "+relation.quoted,
		copySQL.String(), data, rowCount,
	); err != nil {
		return "", true, err
	}
	return stage, true, nil
}

func textCopyStagePreferred(columns []targetColumn) bool {
	for _, column := range columns {
		if column.oid >= 16384 {
			return true
		}
	}
	return false
}

func textCopyStageName(
	relation *targetRelation,
	kind ChangeKind,
	columns []targetColumn,
) string {
	hash := sha256.New()
	_, _ = io.WriteString(hash, relation.quoted)
	_, _ = hash.Write([]byte{byte(kind)})
	var encoded [8]byte
	for _, column := range columns {
		binary.BigEndian.PutUint32(encoded[:4], column.oid)
		binary.BigEndian.PutUint32(encoded[4:], uint32(column.sourceIndex))
		_, _ = hash.Write(encoded[:])
		_, _ = io.WriteString(hash, column.name)
		_, _ = hash.Write([]byte{0})
	}
	sum := hash.Sum(nil)
	name := fmt.Sprintf("pgmigrate_stage_%x", sum[:12])
	return pgx.Identifier{"pg_temp", name}.Sanitize()
}

func textCopyStageData(
	relation *targetRelation,
	kind ChangeKind,
	columns []targetColumn,
	values []TupleDatum,
	rowCount int,
) ([]byte, bool, error) {
	if rowCount < 0 || len(values) != rowCount*len(columns) {
		return nil, true, divergenceFor(relation, kind, fmt.Sprintf(
			"typed stage has %d values for %d rows of %d columns",
			len(values), rowCount, len(columns),
		))
	}
	data := make([]byte, 0, rowCount*(16+len(columns)*8))
	for ordinal := 0; ordinal < rowCount; ordinal++ {
		data = strconv.AppendInt(data, int64(ordinal), 10)
		for i := range columns {
			datum := values[ordinal*len(columns)+i]
			if _, err := datumParamForColumn(relation, columns[i], datum, kind); err != nil {
				return nil, true, err
			}
			data = append(data, '\t')
			switch datum.Kind {
			case DatumNull:
				data = append(data, '\\', 'N')
			case DatumText:
				data = appendTextCopyValue(data, datum.Data)
			default:
				return nil, false, nil
			}
		}
		data = append(data, '\n')
	}
	return data, true, nil
}

func appendTextCopyValue(dst, value []byte) []byte {
	for _, character := range value {
		switch character {
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\v':
			dst = append(dst, '\\', 'v')
		default:
			dst = append(dst, character)
		}
	}
	return dst
}

func (p *applyPipeline) abort() error {
	if p.closed {
		return nil
	}
	var result error
	if len(p.expectations) != 0 {
		result = errors.Join(result, p.sync())
	}
	if status := p.conn.TxStatus(); status == 'T' || status == 'E' {
		p.queueUnprepared("ROLLBACK", nil, applyExpectation{
			description: "roll back target transaction", expectedRows: -1, expectedTag: "ROLLBACK",
		})
		result = errors.Join(result, p.sync())
	}
	if status := p.conn.TxStatus(); status != 'I' && !p.conn.IsClosed() {
		result = errors.Join(result, fmt.Errorf(
			"cdc: target transaction status after rollback is %q, want %q", status, 'I',
		))
	}
	closeErr := p.close()
	p.statements.invalidate()
	return errors.Join(result, closeErr)
}

// emptyParamValue is a non-nil zero-length parameter value, which the extended
// query protocol reads as a zero-length value rather than as NULL.
var emptyParamValue = []byte{}

func applyInserts(replay *applyPipeline, relation *targetRelation, changes []Change) error {
	if copied, err := applyInsertCopy(replay, relation, changes); copied || err != nil {
		return err
	}
	for arrayStart := 0; arrayStart < len(changes); arrayStart += applyArrayChunkRows {
		arrayEnd := arrayStart + applyArrayChunkRows
		if arrayEnd > len(changes) {
			arrayEnd = len(changes)
		}
		arrayChanges := changes[arrayStart:arrayEnd]
		if len(relation.columns) != 0 {
			if applied, err := applyInsertTextStage(replay, relation, arrayChanges); applied {
				if err != nil {
					return err
				}
				continue
			} else if err != nil {
				return err
			}
			if applied, err := applyInsertArrayChunk(replay, relation, arrayChanges); applied {
				if err != nil {
					return err
				}
				continue
			} else if err != nil {
				return err
			}
		}
		chunkRows := insertChunkRows(len(relation.columns))
		for start := 0; start < len(arrayChanges); start += chunkRows {
			end := start + chunkRows
			if end > len(arrayChanges) {
				end = len(arrayChanges)
			}
			if err := applyInsertChunk(replay, relation, arrayChanges[start:end]); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	applyArrayChunkRows          = 8192
	applySelectiveProbeChunkRows = 512
	selectiveBitmapMinHeapBytes  = 1 << 30
	selectiveDirectMinHeapBlocks = 1_000_000
)

// useSelectiveBitmap separates cold, restored heaps from hot application
// tables. BitmapOr avoids one synchronous random heap read per WAL row on a
// cold multi-hundred-GB relation, but its OR planning and materialization are
// needless work when the target heap is already resident. PostgreSQL's own I/O
// counters make that distinction without naming tables or guessing from size.
func useSelectiveBitmap(relation *targetRelation) bool {
	if relation.heapBytes < selectiveBitmapMinHeapBytes {
		return false
	}
	totalBlocks := relation.heapBlocksRead + relation.heapBlocksHit
	if totalBlocks >= selectiveDirectMinHeapBlocks && relation.heapBlocksRead <= totalBlocks/100 {
		return false
	}
	return true
}

// useExactIdentityMembership adds a scalar exact-key BitmapOr guard only for a
// cold heap. The batch join is already semantically exact for composite keys;
// forcing a hundreds-of-terms OR on a cache-resident table makes replay read
// every target row twice and can cost more than the update itself.
func useExactIdentityMembership(
	relation *targetRelation,
	identityColumns []targetColumn,
) bool {
	return len(identityColumns) == 1 && useSelectiveBitmap(relation)
}

// writeDirectSelectiveTargetJoin performs one parameterized exact lookup per
// composite identity. OFFSET 0 keeps PostgreSQL from flattening the lateral
// subquery into a broad row-bound join that can choose an unrelated index.
// Unlike a hundreds-of-terms BitmapOr it has negligible planning cost and lets
// the primary key serve each lookup directly.
func writeDirectSelectiveTargetJoin(
	sql *strings.Builder,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
) {
	sql.WriteString(" JOIN LATERAL (SELECT ")
	for i, columnIndex := range setColumns {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("pgmigrate_lookup.")
		sql.WriteString(relation.columns[columnIndex].quoted)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_lookup WHERE ")
	for i, column := range identityColumns {
		if i != 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString("pgmigrate_lookup.")
		sql.WriteString(column.quoted)
		fmt.Fprintf(sql, "=pgmigrate_batch.identity_%d", i)
	}
	sql.WriteString(" OFFSET 0) AS pgmigrate_target ON true")
}

func applyInsertCopy(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) (bool, error) {
	const minimumCopyRows = 256
	if len(changes) < minimumCopyRows || len(relation.columns) == 0 || !relation.capabilities.binaryCopy {
		return false, nil
	}
	data, supported, err := binaryCopyData(relation, changes)
	if err != nil || !supported {
		return supported, err
	}
	var sql strings.Builder
	sql.WriteString("COPY ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" (")
	for i, column := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
	}
	sql.WriteString(") FROM STDIN BINARY")
	return true, replay.copyFrom(
		relation, ChangeInsert, "binary copy into "+relation.quoted,
		sql.String(), data, len(changes),
	)
}

func binaryCopyData(relation *targetRelation, changes []Change) ([]byte, bool, error) {
	estimatedBytes := 21 + len(changes)*(2+len(relation.columns)*4)
	for row := range changes {
		if err := validateTuple(relation, changes[row].New, ChangeInsert); err != nil {
			return nil, true, err
		}
		for _, column := range relation.columns {
			datum := (*changes[row].New)[column.sourceIndex]
			switch datum.Kind {
			case DatumNull:
			case DatumBinary:
				if _, err := datumParamForColumn(relation, column, datum, ChangeInsert); err != nil {
					return nil, true, err
				}
				estimatedBytes += len(datum.Data)
			default:
				return nil, false, nil
			}
		}
	}
	data := make([]byte, 0, estimatedBytes)
	data = append(data, []byte("PGCOPY\n\xff\r\n\x00")...)
	data = binary.BigEndian.AppendUint32(data, 0)
	data = binary.BigEndian.AppendUint32(data, 0)
	for row := range changes {
		data = binary.BigEndian.AppendUint16(data, uint16(len(relation.columns)))
		for _, column := range relation.columns {
			datum := (*changes[row].New)[column.sourceIndex]
			if datum.Kind == DatumNull {
				data = binary.BigEndian.AppendUint32(data, ^uint32(0))
				continue
			}
			data = binary.BigEndian.AppendUint32(data, uint32(len(datum.Data)))
			data = append(data, datum.Data...)
		}
	}
	data = binary.BigEndian.AppendUint16(data, ^uint16(0))
	return data, true, nil
}

func applyInsertTextStage(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) (bool, error) {
	if len(changes) < minimumTextCopyStageRows ||
		!relation.capabilities.textCopyStage || !textCopyStagePreferred(relation.columns) {
		return false, nil
	}
	values := make([]TupleDatum, 0, len(changes)*len(relation.columns))
	for row := range changes {
		if err := validateTuple(relation, changes[row].New, ChangeInsert); err != nil {
			return true, err
		}
		for _, column := range relation.columns {
			values = append(values, (*changes[row].New)[column.sourceIndex])
		}
	}
	stage, applied, err := replay.loadTextCopyStage(
		relation, ChangeInsert, relation.columns, values, len(changes),
	)
	if err != nil || !applied {
		return applied, err
	}

	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" (")
	for i, column := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
	}
	sql.WriteByte(')')
	if relation.overrideIdentity {
		sql.WriteString(" OVERRIDING SYSTEM VALUE")
	}
	sql.WriteString(" SELECT ")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "column_%d", i)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(stage)
	sql.WriteString(" ORDER BY ordinal")
	return true, replay.queue(sql.String(), nil, applyExpectation{
		relation: relation, kind: ChangeInsert,
		description: "staged insert into " + relation.quoted, expectedRows: int64(len(changes)),
	})
}

func insertChunkRows(columnCount int) int {
	const (
		maxBindParameters = 65535
		maxSQLRows        = 1000
	)
	if columnCount <= 0 {
		return 1
	}
	rows := maxBindParameters / columnCount
	if rows > maxSQLRows {
		rows = maxSQLRows
	}
	if rows < 1 {
		rows = 1
	}
	return rows
}

func applyInsertChunk(replay *applyPipeline, relation *targetRelation, changes []Change) error {
	if len(relation.columns) == 0 {
		sql := "INSERT INTO " + relation.quoted + " DEFAULT VALUES"
		for i := range changes {
			if err := validateTuple(relation, changes[i].New, ChangeInsert); err != nil {
				return err
			}
			if err := replay.queue(sql, nil, applyExpectation{
				relation: relation, kind: ChangeInsert,
				description: "insert default row into " + relation.quoted, expectedRows: 1,
			}); err != nil {
				return err
			}
		}
		return nil
	}
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" (")
	for i, column := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
	}
	sql.WriteByte(')')
	if relation.overrideIdentity {
		sql.WriteString(" OVERRIDING SYSTEM VALUE")
	}
	sql.WriteString(" VALUES ")
	params := make([]rawParam, 0, len(relation.columns)*len(changes))
	for row := range changes {
		if row != 0 {
			sql.WriteByte(',')
		}
		tuple := changes[row].New
		if err := validateTuple(relation, tuple, ChangeInsert); err != nil {
			return err
		}
		sql.WriteByte('(')
		for column := range relation.columns {
			if column != 0 {
				sql.WriteByte(',')
			}
			param, err := datumParam(relation, column, (*tuple)[relation.columns[column].sourceIndex], ChangeInsert)
			if err != nil {
				return err
			}
			params = append(params, param)
			fmt.Fprintf(&sql, "$%d", len(params))
		}
		sql.WriteByte(')')
	}
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeInsert,
		description: "insert into " + relation.quoted, expectedRows: int64(len(changes)),
	})
}

func applyInsertArrayChunk(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) (bool, error) {
	params := make([]rawParam, 0, len(relation.columns))
	for _, column := range relation.columns {
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			if err := validateTuple(relation, changes[row].New, ChangeInsert); err != nil {
				return true, err
			}
			datums[row] = (*changes[row].New)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeInsert)
		if err != nil {
			return true, err
		}
		if !supported {
			return false, nil
		}
		params = append(params, param)
	}
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" (")
	for i, column := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
	}
	sql.WriteByte(')')
	if relation.overrideIdentity {
		sql.WriteString(" OVERRIDING SYSTEM VALUE")
	}
	sql.WriteString(" SELECT ")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "pgmigrate_batch.column_%d", i)
	}
	sql.WriteString(" FROM unnest(")
	for i := range params {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "$%d", i+1)
	}
	sql.WriteString(") AS pgmigrate_batch(")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "column_%d", i)
	}
	sql.WriteByte(')')
	return true, replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeInsert,
		description: "array insert into " + relation.quoted, expectedRows: int64(len(changes)),
	})
}

// PostgreSQL logical replication supplies a complete new row for ordinary
// columns (apart from unchanged TOAST values). For those rows, use the same
// primary-key upsert shape as crdb-to-pg: PostgreSQL resolves the conflict
// through the exact primary key and performs the update in one operation. This
// avoids a compare-first target read and avoids UPDATE ... FROM plans whose
// join order can select an unrelated secondary index on very large tables.
func canPrimaryKeyUpsert(relation *targetRelation, change *Change) bool {
	return canPrimaryKeyUpsertForPlan(relation, change, true)
}

// canPrimaryKeyUpsertV2 freezes the scheduler admission used by plan version
// 2. A newer executor may use the legacy UPDATE shape for an uncommitted work
// row, but it must reconstruct the exact v2 lane manifest before doing so.
func canPrimaryKeyUpsertV2(relation *targetRelation, change *Change) bool {
	return canPrimaryKeyUpsertForPlan(relation, change, false)
}

func canPrimaryKeyUpsertForPlan(
	relation *targetRelation,
	change *Change,
	requireImmediateArbiter bool,
) bool {
	if relation == nil || change == nil || change.New == nil ||
		len(*change.New) != len(relation.source.Columns) || len(relation.columns) == 0 ||
		!relation.capabilities.keyedSetDML ||
		(requireImmediateArbiter && !relation.capabilities.primaryKeyArbiter) {
		return false
	}
	primary := primaryKeyColumns(relation)
	if len(primary) == 0 {
		return false
	}
	for _, column := range relation.columns {
		if (*change.New)[column.sourceIndex].Kind == DatumUnchangedToast {
			return false
		}
	}
	if change.Old == nil || len(*change.Old) != len(relation.source.Columns) {
		return true
	}
	for _, column := range primary {
		oldDatum := (*change.Old)[column.sourceIndex]
		newDatum := (*change.New)[column.sourceIndex]
		if oldDatum.Kind == DatumUnchangedToast || !tupleDatumEqual(oldDatum, newDatum) {
			return false
		}
	}
	return true
}

func tupleDatumEqual(left, right TupleDatum) bool {
	return left.Kind == right.Kind && bytes.Equal(left.Data, right.Data)
}

func primaryKeyColumns(relation *targetRelation) []targetColumn {
	columns := make([]targetColumn, 0, len(relation.columns))
	for _, column := range relation.columns {
		if column.primary {
			columns = append(columns, column)
		}
	}
	slices.SortFunc(columns, func(left, right targetColumn) int {
		return left.primaryPos - right.primaryPos
	})
	return columns
}

func primaryKeyTupleKey(relation *targetRelation, tuple *Tuple) (string, error) {
	if err := validateTuple(relation, tuple, ChangeUpdate); err != nil {
		return "", err
	}
	var key strings.Builder
	for _, column := range primaryKeyColumns(relation) {
		datum := (*tuple)[column.sourceIndex]
		key.WriteByte(byte(datum.Kind))
		fmt.Fprintf(&key, ":%d:", len(datum.Data))
		key.Write(datum.Data)
	}
	return key.String(), nil
}

func applyPrimaryKeyUpsertChunk(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) error {
	if len(changes) == 0 {
		return nil
	}
	if applied, err := applyPrimaryKeyUpsertBinaryStage(replay, relation, changes); applied || err != nil {
		return err
	}
	if applied, err := applyPrimaryKeyUpsertTextStage(replay, relation, changes); applied || err != nil {
		return err
	}
	if applied, err := applyPrimaryKeyUpsertArrayChunk(replay, relation, changes); applied || err != nil {
		return err
	}
	chunkRows := insertChunkRows(len(relation.columns))
	for start := 0; start < len(changes); start += chunkRows {
		end := min(start+chunkRows, len(changes))
		if err := applyPrimaryKeyUpsertValueChunk(replay, relation, changes[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func applyPrimaryKeyUpsertBinaryStage(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) (bool, error) {
	values := make([]TupleDatum, 0, len(changes)*len(relation.columns))
	for row := range changes {
		if err := validateTuple(relation, changes[row].New, ChangeUpdate); err != nil {
			return true, err
		}
		for _, column := range relation.columns {
			values = append(values, (*changes[row].New)[column.sourceIndex])
		}
	}
	stage, applied, err := replay.loadBinaryCopyStage(
		relation, ChangeUpdate, relation.columns, values, len(changes),
	)
	if err != nil || !applied {
		return applied, err
	}
	var sql strings.Builder
	writePrimaryKeyUpsertPrefix(&sql, relation)
	sql.WriteString(" SELECT ")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "column_%d", i)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(stage)
	sql.WriteString(" ORDER BY ordinal")
	appendPrimaryKeyConflictClause(&sql, relation)
	return true, replay.queue(sql.String(), nil, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "binary-staged primary-key upsert into " + relation.quoted,
		expectedRows: int64(len(changes)),
	})
}

func applyPrimaryKeyUpsertTextStage(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) (bool, error) {
	values := make([]TupleDatum, 0, len(changes)*len(relation.columns))
	for row := range changes {
		if err := validateTuple(relation, changes[row].New, ChangeUpdate); err != nil {
			return true, err
		}
		for _, column := range relation.columns {
			values = append(values, (*changes[row].New)[column.sourceIndex])
		}
	}
	stage, applied, err := replay.loadTextCopyStage(
		relation, ChangeUpdate, relation.columns, values, len(changes),
	)
	if err != nil || !applied {
		return applied, err
	}
	var sql strings.Builder
	writePrimaryKeyUpsertPrefix(&sql, relation)
	sql.WriteString(" SELECT ")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "column_%d", i)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(stage)
	sql.WriteString(" ORDER BY ordinal")
	appendPrimaryKeyConflictClause(&sql, relation)
	return true, replay.queue(sql.String(), nil, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "staged primary-key upsert into " + relation.quoted,
		expectedRows: int64(len(changes)),
	})
}

func applyPrimaryKeyUpsertArrayChunk(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) (bool, error) {
	params := make([]rawParam, 0, len(relation.columns))
	for _, column := range relation.columns {
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			if err := validateTuple(relation, changes[row].New, ChangeUpdate); err != nil {
				return true, err
			}
			datums[row] = (*changes[row].New)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeUpdate)
		if err != nil || !supported {
			return supported, err
		}
		params = append(params, param)
	}
	var sql strings.Builder
	writePrimaryKeyUpsertPrefix(&sql, relation)
	sql.WriteString(" SELECT ")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "pgmigrate_batch.column_%d", i)
	}
	sql.WriteString(" FROM unnest(")
	for i := range params {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "$%d", i+1)
	}
	sql.WriteString(") AS pgmigrate_batch(")
	for i := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "column_%d", i)
	}
	sql.WriteString(") WHERE true")
	appendPrimaryKeyConflictClause(&sql, relation)
	return true, replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "array primary-key upsert into " + relation.quoted,
		expectedRows: int64(len(changes)),
	})
}

func applyPrimaryKeyUpsertValueChunk(
	replay *applyPipeline,
	relation *targetRelation,
	changes []Change,
) error {
	var sql strings.Builder
	writePrimaryKeyUpsertPrefix(&sql, relation)
	sql.WriteString(" VALUES ")
	params := make([]rawParam, 0, len(changes)*len(relation.columns))
	for row := range changes {
		if err := validateTuple(relation, changes[row].New, ChangeUpdate); err != nil {
			return err
		}
		if row != 0 {
			sql.WriteByte(',')
		}
		sql.WriteByte('(')
		for columnIndex, column := range relation.columns {
			if columnIndex != 0 {
				sql.WriteByte(',')
			}
			param, err := datumParamForColumn(
				relation, column, (*changes[row].New)[column.sourceIndex], ChangeUpdate,
			)
			if err != nil {
				return err
			}
			params = append(params, param)
			fmt.Fprintf(&sql, "$%d", len(params))
		}
		sql.WriteByte(')')
	}
	appendPrimaryKeyConflictClause(&sql, relation)
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "primary-key upsert into " + relation.quoted,
		expectedRows: int64(len(changes)),
	})
}

func writePrimaryKeyUpsertPrefix(sql *strings.Builder, relation *targetRelation) {
	sql.WriteString("INSERT INTO ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" (")
	for i, column := range relation.columns {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
	}
	sql.WriteByte(')')
	if relation.overrideIdentity {
		sql.WriteString(" OVERRIDING SYSTEM VALUE")
	}
}

func appendPrimaryKeyConflictClause(sql *strings.Builder, relation *targetRelation) {
	primary := primaryKeyColumns(relation)
	sql.WriteString(" ON CONFLICT (")
	for i, column := range primary {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
	}
	sql.WriteString(") DO UPDATE SET ")
	assignments := 0
	for _, column := range relation.columns {
		if column.primary {
			continue
		}
		if assignments != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(column.quoted)
		sql.WriteString("=EXCLUDED.")
		sql.WriteString(column.quoted)
		assignments++
	}
	if assignments == 0 {
		sql.WriteString(primary[0].quoted)
		sql.WriteString("=EXCLUDED.")
		sql.WriteString(primary[0].quoted)
	}
}

func applyUpdates(replay *applyPipeline, relation *targetRelation, changes []Change) error {
	for start := 0; start < len(changes); {
		if !canPrimaryKeyUpsert(relation, &changes[start]) {
			end := start + 1
			for end < len(changes) && !canPrimaryKeyUpsert(relation, &changes[end]) {
				end++
			}
			if err := applyLegacyUpdates(replay, relation, changes[start:end]); err != nil {
				return err
			}
			start = end
			continue
		}

		seen := make(map[string]struct{})
		end := start
		for end < len(changes) && end-start < applyArrayChunkRows &&
			canPrimaryKeyUpsert(relation, &changes[end]) {
			key, err := primaryKeyTupleKey(relation, changes[end].New)
			if err != nil {
				return err
			}
			if _, duplicate := seen[key]; duplicate {
				break
			}
			seen[key] = struct{}{}
			end++
		}
		if err := applyPrimaryKeyUpsertChunk(replay, relation, changes[start:end]); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func applyLegacyUpdates(replay *applyPipeline, relation *targetRelation, changes []Change) error {
	identityColumns := batchUpdateIdentityColumns(relation)
	if len(changes) < 2 || len(identityColumns) == 0 || len(relation.columns) == 0 {
		for i := range changes {
			if err := applyUpdate(replay, relation, &changes[i]); err != nil {
				return err
			}
		}
		return nil
	}

	for start := 0; start < len(changes); {
		firstKey, err := batchUpdateIdentityKey(relation, identityColumns, &changes[start])
		if err != nil {
			return err
		}
		setColumns := updateSetColumnIndexes(relation, &changes[start])
		if !relation.capabilities.selectiveUpdates &&
			!updateSetColumnsBatchSafe(relation, setColumns) {
			if err := applyUpdate(replay, relation, &changes[start]); err != nil {
				return err
			}
			start++
			continue
		}
		chunkRows := applyArrayChunkRows
		if relation.capabilities.selectiveUpdates {
			chunkRows = updateChunkRows(len(setColumns) + len(identityColumns))
		}
		if useExactIdentityMembership(relation, identityColumns) && chunkRows > applySelectiveProbeChunkRows {
			chunkRows = applySelectiveProbeChunkRows
		}
		seen := map[string]struct{}{firstKey: {}}
		end := start + 1
		for end < len(changes) && end-start < chunkRows {
			key, err := batchUpdateIdentityKey(relation, identityColumns, &changes[end])
			if err != nil {
				return err
			}
			candidateSetColumns := updateSetColumnIndexes(relation, &changes[end])
			if !slices.Equal(setColumns, candidateSetColumns) ||
				(!relation.capabilities.selectiveUpdates &&
					!updateSetColumnsBatchSafe(relation, candidateSetColumns)) {
				break
			}
			if _, duplicate := seen[key]; duplicate {
				break
			}
			seen[key] = struct{}{}
			end++
		}
		if relation.capabilities.selectiveUpdates {
			if err := applySelectiveUpdateChunk(
				replay, relation, identityColumns, setColumns, changes[start:end],
			); err != nil {
				return err
			}
		} else if end-start == 1 {
			if err := applyUpdate(replay, relation, &changes[start]); err != nil {
				return err
			}
		} else if err := applyUpdateChunk(
			replay, relation, identityColumns, setColumns, changes[start:end],
		); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// applySelectiveUpdateChunk avoids the write amplification caused by pgoutput
// update tuples containing the complete new row. It first compares those values
// with the target inside the open replay transaction, then groups rows by their
// exact changed-column mask. Each target row is still updated once, but indexes
// that do not depend on a changed column are no longer needlessly maintained.
func applySelectiveUpdateChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) error {
	masks, err := inspectSelectiveUpdateMasks(
		replay, relation, identityColumns, setColumns, changes,
	)
	if err != nil {
		return err
	}
	for start := 0; start < len(changes); {
		mask := masks[start]
		if len(mask) == 0 {
			start++
			continue
		}
		if !updateSetColumnsBatchSafe(relation, mask) {
			exact := selectiveChange(relation, &changes[start], mask)
			if err := applyUpdate(replay, relation, &exact); err != nil {
				return err
			}
			start++
			continue
		}
		firstKey, err := batchUpdateIdentityKey(relation, identityColumns, &changes[start])
		if err != nil {
			return err
		}
		seen := map[string]struct{}{firstKey: {}}
		end := start + 1
		for end < len(changes) && end-start < applyArrayChunkRows && slices.Equal(mask, masks[end]) {
			key, err := batchUpdateIdentityKey(relation, identityColumns, &changes[end])
			if err != nil {
				return err
			}
			if _, duplicate := seen[key]; duplicate {
				break
			}
			seen[key] = struct{}{}
			end++
		}
		exact := make([]Change, end-start)
		for i := range exact {
			exact[i] = selectiveChange(relation, &changes[start+i], mask)
		}
		if len(exact) == 1 {
			if err := applyUpdate(replay, relation, &exact[0]); err != nil {
				return err
			}
		} else if err := applyUpdateChunk(replay, relation, identityColumns, mask, exact); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func selectiveChange(relation *targetRelation, change *Change, setColumns []int) Change {
	exact := *change
	if exact.Old == nil {
		exact.Old = change.New
	}
	newTuple := make(Tuple, len(*change.New))
	for i := range newTuple {
		newTuple[i] = TupleDatum{Kind: DatumUnchangedToast}
	}
	for _, columnIndex := range setColumns {
		sourceIndex := relation.columns[columnIndex].sourceIndex
		newTuple[sourceIndex] = (*change.New)[sourceIndex]
	}
	exact.New = &newTuple
	return exact
}

func selectiveIdentityParams(
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) ([]rawParam, bool, error) {
	params := make([]rawParam, 0, len(identityColumns))
	for _, column := range identityColumns {
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			predicate := changes[row].Old
			if predicate == nil {
				predicate = changes[row].New
			}
			datums[row] = (*predicate)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeUpdate)
		if err != nil || !supported {
			return nil, supported, err
		}
		params = append(params, param)
	}
	return params, true, nil
}

func appendSelectiveIdentityScalarParams(
	params []rawParam,
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) ([]rawParam, [][]int, error) {
	positions := make([][]int, len(changes))
	for row := range changes {
		predicate := changes[row].Old
		if predicate == nil {
			predicate = changes[row].New
		}
		positions[row] = make([]int, len(identityColumns))
		for i, column := range identityColumns {
			param, err := datumParamForColumn(
				relation, column, (*predicate)[column.sourceIndex], ChangeUpdate,
			)
			if err != nil {
				return nil, nil, err
			}
			params = append(params, param)
			positions[row][i] = len(params)
		}
	}
	return params, positions, nil
}

func appendDeleteIdentityScalarParams(
	params []rawParam,
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) ([]rawParam, [][]int, error) {
	positions := make([][]int, len(changes))
	for row := range changes {
		positions[row] = make([]int, len(identityColumns))
		for i, column := range identityColumns {
			param, err := datumParamForColumn(
				relation, column, (*changes[row].Old)[column.sourceIndex], ChangeDelete,
			)
			if err != nil {
				return nil, nil, err
			}
			params = append(params, param)
			positions[row][i] = len(params)
		}
	}
	return params, positions, nil
}

// writeSelectiveTargetRowsCTE forces PostgreSQL to collect all exact
// replica-identity matches into a bitmap before it touches the heap. A nested
// loop issues one synchronous random heap read per WAL row, which is
// catastrophic when a restored table is much larger than cache. BitmapOr keeps
// the same primary-key predicates but visits matching heap pages physically.
func writeSelectiveTargetRowsCTE(
	sql *strings.Builder,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	identityParamPositions [][]int,
) {
	sql.WriteString("WITH pgmigrate_target_rows AS MATERIALIZED (SELECT ")
	selected := make(map[string]struct{}, len(setColumns)+len(identityColumns))
	written := 0
	writeColumn := func(column targetColumn) {
		if _, exists := selected[column.name]; exists {
			return
		}
		selected[column.name] = struct{}{}
		if written != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("pgmigrate_bitmap_target.")
		sql.WriteString(column.quoted)
		written++
	}
	for _, columnIndex := range setColumns {
		writeColumn(relation.columns[columnIndex])
	}
	for _, column := range identityColumns {
		writeColumn(column)
	}
	sql.WriteString(" FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_bitmap_target WHERE ")
	writeExactIdentityDisjunction(
		sql, "pgmigrate_bitmap_target", identityColumns, identityParamPositions,
	)
	sql.WriteString(") ")
}

func writeExactIdentityDisjunction(
	sql *strings.Builder,
	targetAlias string,
	identityColumns []targetColumn,
	identityParamPositions [][]int,
) {
	for row, positions := range identityParamPositions {
		if row != 0 {
			sql.WriteString(" OR ")
		}
		sql.WriteByte('(')
		for i, column := range identityColumns {
			if i != 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(targetAlias)
			sql.WriteByte('.')
			sql.WriteString(column.quoted)
			fmt.Fprintf(sql, "=$%d", positions[i])
		}
		sql.WriteByte(')')
	}
}

func writeSelectiveDifference(
	sql *strings.Builder,
	relation *targetRelation,
	columnIndex int,
	batchColumn int,
) {
	sql.WriteByte(',')
	column := relation.columns[columnIndex]
	if column.nondeterministicCollation {
		// A nondeterministic collation can report two byte-distinct values as
		// equal. Skipping that assignment would leave an observably different
		// scalar or array value on the target while advancing durable progress.
		// Treat it as changed without invoking its collation-aware equality.
		sql.WriteString("true")
		return
	}
	sql.WriteString("(pgmigrate_target.")
	sql.WriteString(column.quoted)
	fmt.Fprintf(sql, " IS DISTINCT FROM pgmigrate_batch.set_%d)", batchColumn)
}

func inspectSelectiveUpdateMasks(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) ([][]int, error) {
	params := make([]rawParam, 0, len(setColumns)+len(identityColumns))
	for _, columnIndex := range setColumns {
		column := relation.columns[columnIndex]
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			datums[row] = (*changes[row].New)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeUpdate)
		if err != nil {
			return nil, err
		}
		if !supported {
			return inspectSelectiveUpdateMasksValues(
				replay, relation, identityColumns, setColumns, changes,
			)
		}
		params = append(params, param)
	}
	identityParams, supported, err := selectiveIdentityParams(relation, identityColumns, changes)
	if err != nil {
		return nil, err
	}
	if !supported {
		return inspectSelectiveUpdateMasksValues(
			replay, relation, identityColumns, setColumns, changes,
		)
	}
	params = append(params, identityParams...)
	batchParamCount := len(params)
	var sql strings.Builder
	targetRows := relation.quoted
	if useExactIdentityMembership(relation, identityColumns) {
		var identityParamPositions [][]int
		params, identityParamPositions, err = appendSelectiveIdentityScalarParams(
			params, relation, identityColumns, changes,
		)
		if err != nil {
			return nil, err
		}
		writeSelectiveTargetRowsCTE(
			&sql, relation, identityColumns, setColumns, identityParamPositions,
		)
		targetRows = "pgmigrate_target_rows"
	}
	sql.WriteString("SELECT pgmigrate_batch.ordinal - 1")
	for i, columnIndex := range setColumns {
		writeSelectiveDifference(&sql, relation, columnIndex, i)
	}
	sql.WriteString(" FROM unnest(")
	for i := 0; i < batchParamCount; i++ {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "$%d", i+1)
	}
	sql.WriteString(") WITH ORDINALITY AS pgmigrate_batch(")
	for i := range setColumns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "set_%d", i)
	}
	for i := range identityColumns {
		if len(setColumns) != 0 || i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "identity_%d", i)
	}
	sql.WriteString(",ordinal)")
	if targetRows == "pgmigrate_target_rows" {
		sql.WriteString(" JOIN ")
		sql.WriteString(targetRows)
		sql.WriteString(" AS pgmigrate_target ON ")
		writeBatchIdentityPredicate(&sql, identityColumns, "identity_", 0)
	} else {
		writeDirectSelectiveTargetJoin(&sql, relation, identityColumns, setColumns)
	}
	masks := make([][]int, len(changes))
	seen := make([]bool, len(changes))
	replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "inspect selective update " + relation.quoted,
		expectedRows: int64(len(changes)), expectedOrdinals: len(changes),
		consumeRows: func(reader *pgconn.ResultReader) error {
			for reader.NextRow() {
				values := reader.Values()
				if len(values) != len(setColumns)+1 {
					return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
						"selective inspection returned %d columns, expected %d", len(values), len(setColumns)+1,
					))
				}
				ordinal, err := strconv.Atoi(string(values[0]))
				if err != nil || ordinal < 0 || ordinal >= len(changes) || seen[ordinal] {
					return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
						"selective inspection returned invalid ordinal %q", values[0],
					))
				}
				seen[ordinal] = true
				for i, value := range values[1:] {
					switch string(value) {
					case "t":
						masks[ordinal] = append(masks[ordinal], setColumns[i])
					case "f":
					default:
						return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
							"selective inspection returned invalid difference flag %q", value,
						))
					}
				}
			}
			for ordinal, found := range seen {
				if !found {
					return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
						"selective inspection did not match source row %d", ordinal,
					))
				}
			}
			return nil
		},
	})
	if err := replay.sync(); err != nil {
		return nil, err
	}
	return masks, nil
}

func inspectSelectiveUpdateMasksValues(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) ([][]int, error) {
	params := make([]rawParam, 0, len(changes)*(len(setColumns)+len(identityColumns)))
	identityParamPositions := make([][]int, len(changes))
	var values strings.Builder
	for row := range changes {
		if row != 0 {
			values.WriteByte(',')
		}
		fmt.Fprintf(&values, "(%d", row)
		for _, columnIndex := range setColumns {
			column := relation.columns[columnIndex]
			param, err := datumParam(
				relation, columnIndex, (*changes[row].New)[column.sourceIndex], ChangeUpdate,
			)
			if err != nil {
				return nil, err
			}
			params = append(params, param)
			fmt.Fprintf(&values, ",$%d", len(params))
		}
		predicate := changes[row].Old
		if predicate == nil {
			predicate = changes[row].New
		}
		identityParamPositions[row] = make([]int, len(identityColumns))
		for i, column := range identityColumns {
			param, err := datumParamForColumn(
				relation, column, (*predicate)[column.sourceIndex], ChangeUpdate,
			)
			if err != nil {
				return nil, err
			}
			params = append(params, param)
			identityParamPositions[row][i] = len(params)
			fmt.Fprintf(&values, ",$%d", len(params))
		}
		values.WriteByte(')')
	}
	var sql strings.Builder
	targetRows := relation.quoted
	if useExactIdentityMembership(relation, identityColumns) {
		writeSelectiveTargetRowsCTE(
			&sql, relation, identityColumns, setColumns, identityParamPositions,
		)
		targetRows = "pgmigrate_target_rows"
	}
	sql.WriteString("SELECT pgmigrate_batch.ordinal")
	for i, columnIndex := range setColumns {
		writeSelectiveDifference(&sql, relation, columnIndex, i)
	}
	sql.WriteString(" FROM (VALUES ")
	sql.WriteString(values.String())
	sql.WriteString(") AS pgmigrate_batch(ordinal")
	for i := range setColumns {
		fmt.Fprintf(&sql, ",set_%d", i)
	}
	for i := range identityColumns {
		fmt.Fprintf(&sql, ",identity_%d", i)
	}
	sql.WriteByte(')')
	if targetRows == "pgmigrate_target_rows" {
		sql.WriteString(" JOIN ")
		sql.WriteString(targetRows)
		sql.WriteString(" AS pgmigrate_target ON ")
		writeBatchIdentityPredicate(&sql, identityColumns, "identity_", 0)
	} else {
		writeDirectSelectiveTargetJoin(&sql, relation, identityColumns, setColumns)
	}
	return queueSelectiveUpdateInspection(
		replay, relation, setColumns, changes, sql.String(), params,
	)
}

func queueSelectiveUpdateInspection(
	replay *applyPipeline,
	relation *targetRelation,
	setColumns []int,
	changes []Change,
	sql string,
	params []rawParam,
) ([][]int, error) {
	masks := make([][]int, len(changes))
	seen := make([]bool, len(changes))
	replay.queue(sql, params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "inspect selective update " + relation.quoted,
		expectedRows: int64(len(changes)), expectedOrdinals: len(changes),
		consumeRows: func(reader *pgconn.ResultReader) error {
			for reader.NextRow() {
				values := reader.Values()
				if len(values) != len(setColumns)+1 {
					return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
						"selective inspection returned %d columns, expected %d", len(values), len(setColumns)+1,
					))
				}
				ordinal, err := strconv.Atoi(string(values[0]))
				if err != nil || ordinal < 0 || ordinal >= len(changes) || seen[ordinal] {
					return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
						"selective inspection returned invalid ordinal %q", values[0],
					))
				}
				seen[ordinal] = true
				for i, value := range values[1:] {
					switch string(value) {
					case "t":
						masks[ordinal] = append(masks[ordinal], setColumns[i])
					case "f":
					default:
						return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
							"selective inspection returned invalid difference flag %q", value,
						))
					}
				}
			}
			for ordinal, found := range seen {
				if !found {
					return divergenceFor(relation, ChangeUpdate, fmt.Sprintf(
						"selective inspection did not match source row %d", ordinal,
					))
				}
			}
			return nil
		},
	})
	if err := replay.sync(); err != nil {
		return nil, err
	}
	return masks, nil
}

func batchUpdateIdentityColumns(relation *targetRelation) []targetColumn {
	if relation == nil || !relation.capabilities.keyedSetDML || relation.source.ReplicaIdentity == 'f' {
		return nil
	}
	columns := make([]targetColumn, 0, len(predicateTargetColumns(relation)))
	for _, column := range predicateTargetColumns(relation) {
		if !column.key {
			continue
		}
		// Replica identity indexes are expected to be unique and NOT NULL. Keep
		// nullable or drifted targets on the one-row path, whose row-count check
		// is unambiguous without relying on that invariant.
		if !column.notNull {
			return nil
		}
		columns = append(columns, column)
	}
	return columns
}

func batchUpdateIdentityKey(
	relation *targetRelation,
	identityColumns []targetColumn,
	change *Change,
) (string, error) {
	if err := validateTuple(relation, change.New, ChangeUpdate); err != nil {
		return "", err
	}
	predicate := change.Old
	if predicate == nil {
		predicate = change.New
	}
	if err := validateTuple(relation, predicate, ChangeUpdate); err != nil {
		return "", err
	}
	var key strings.Builder
	for _, column := range identityColumns {
		datum := (*predicate)[column.sourceIndex]
		if datum.Kind == DatumUnchangedToast {
			return "", divergenceFor(relation, ChangeUpdate, "replica identity contains unchanged TOAST")
		}
		if _, err := datumParamForColumn(relation, column, datum, ChangeUpdate); err != nil {
			return "", err
		}
		key.WriteByte(byte(datum.Kind))
		key.WriteString(strconv.Itoa(len(datum.Data)))
		key.WriteByte(':')
		key.Write(datum.Data)
	}
	return key.String(), nil
}

func updateSetColumnIndexes(relation *targetRelation, change *Change) []int {
	result := make([]int, 0, len(relation.columns))
	predicate := change.Old
	if predicate == nil {
		predicate = change.New
	}
	for i := range relation.columns {
		column := relation.columns[i]
		datum := (*change.New)[column.sourceIndex]
		if datum.Kind == DatumUnchangedToast {
			continue
		}
		// pgoutput includes unchanged replica-identity values in the new tuple.
		// Assigning them again does needless unique-index work and makes otherwise
		// independent updates conflict inside one set statement.
		if column.key && predicate != nil {
			old := (*predicate)[column.sourceIndex]
			if datum.Kind == old.Kind && bytes.Equal(datum.Data, old.Data) {
				continue
			}
		}
		result = append(result, i)
	}
	return result
}

func updateSetColumnsBatchSafe(relation *targetRelation, setColumns []int) bool {
	for _, columnIndex := range setColumns {
		if relation.columns[columnIndex].conflicting {
			return false
		}
	}
	return true
}

func updateChunkRows(parametersPerRow int) int {
	const (
		maxBindParameters = 65535
		maxSQLRows        = 1000
	)
	if parametersPerRow <= 0 {
		return 1
	}
	rows := maxBindParameters / parametersPerRow
	if rows > maxSQLRows {
		rows = maxSQLRows
	}
	if rows < 1 {
		rows = 1
	}
	return rows
}

// writeBatchIdentityPredicate renders an exact lookup through the replica
// identity's B-tree column order. PostgreSQL can otherwise prefer a smaller
// non-unique prefix index and filter the remaining identity columns, which is
// catastrophic when that prefix matches many rows. The batch path admits only
// NOT NULL replica-identity columns, so equal lower and upper row bounds are
// equivalent to equality while keeping the full ordered key as one index qual.
func writeBatchIdentityPredicate(
	sql *strings.Builder,
	identityColumns []targetColumn,
	batchColumnPrefix string,
	batchColumnOffset int,
) {
	writeTarget := func() {
		sql.WriteString("ROW(")
		for i, column := range identityColumns {
			if i != 0 {
				sql.WriteByte(',')
			}
			sql.WriteString("pgmigrate_target.")
			sql.WriteString(column.quoted)
		}
		sql.WriteByte(')')
	}
	writeBatch := func() {
		sql.WriteString("ROW(")
		for i := range identityColumns {
			if i != 0 {
				sql.WriteByte(',')
			}
			fmt.Fprintf(sql, "pgmigrate_batch.%s%d", batchColumnPrefix, batchColumnOffset+i)
		}
		sql.WriteByte(')')
	}
	if len(identityColumns) == 1 {
		sql.WriteString("pgmigrate_target.")
		sql.WriteString(identityColumns[0].quoted)
		fmt.Fprintf(sql, "=pgmigrate_batch.%s%d", batchColumnPrefix, batchColumnOffset)
		return
	}
	writeTarget()
	sql.WriteString(">=")
	writeBatch()
	sql.WriteString(" AND ")
	writeTarget()
	sql.WriteString("<=")
	writeBatch()
}

// writeCompositeIdentityCTIDPredicate forces PostgreSQL to resolve every batch
// row through the complete composite replica identity before updating. The
// optimization barrier prevents the correlated lookup from being flattened
// into a broad UPDATE ... FROM join, and the outer TidScan touches exactly the
// physical row returned by the primary-key lookup.
func writeCompositeIdentityCTIDPredicate(
	sql *strings.Builder,
	relation *targetRelation,
	identityColumns []targetColumn,
	batchColumnPrefix string,
	batchColumnOffset int,
) {
	writeCompositeIdentityCTIDPredicateMode(
		sql, relation, identityColumns, batchColumnPrefix, batchColumnOffset, false,
	)
}

// writeCompositePrimaryKeyCTIDPredicate adds equal lower and upper bounds in
// the target primary-key order. The scalar equalities remain for exactness,
// while the row bounds stop PostgreSQL from choosing a smaller prefix index
// and filtering the remaining key columns after a potentially huge scan.
func writeCompositePrimaryKeyCTIDPredicate(
	sql *strings.Builder,
	relation *targetRelation,
	identityColumns []targetColumn,
	batchColumnPrefix string,
	batchColumnOffset int,
) {
	writeCompositeIdentityCTIDPredicateMode(
		sql, relation, identityColumns, batchColumnPrefix, batchColumnOffset, true,
	)
}

func writeCompositeIdentityCTIDPredicateMode(
	sql *strings.Builder,
	relation *targetRelation,
	identityColumns []targetColumn,
	batchColumnPrefix string,
	batchColumnOffset int,
	primaryKeyBounds bool,
) {
	sql.WriteString("pgmigrate_target.ctid=(SELECT pgmigrate_lookup.ctid FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_lookup WHERE ")
	for i, column := range identityColumns {
		if i != 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString("pgmigrate_lookup.")
		sql.WriteString(column.quoted)
		fmt.Fprintf(
			sql, "=pgmigrate_batch.%s%d",
			batchColumnPrefix, batchColumnOffset+i,
		)
	}
	if primaryKeyBounds && len(identityColumns) > 1 {
		writeLookupPrimaryKeyBound := func(operator string) {
			sql.WriteString(" AND ROW(")
			for i, column := range identityColumns {
				if i != 0 {
					sql.WriteByte(',')
				}
				sql.WriteString("pgmigrate_lookup.")
				sql.WriteString(column.quoted)
			}
			sql.WriteByte(')')
			sql.WriteString(operator)
			sql.WriteString("ROW(")
			for i := range identityColumns {
				if i != 0 {
					sql.WriteByte(',')
				}
				fmt.Fprintf(
					sql, "pgmigrate_batch.%s%d",
					batchColumnPrefix, batchColumnOffset+i,
				)
			}
			sql.WriteByte(')')
		}
		writeLookupPrimaryKeyBound(">=")
		writeLookupPrimaryKeyBound("<=")
	}
	sql.WriteString(" OFFSET 0)")
}

func applyUpdateChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) error {
	if applied, err := applyUpdateTextStage(
		replay, relation, identityColumns, setColumns, changes,
	); applied || err != nil {
		return err
	}
	if applied, err := applyUpdateArrayChunk(
		replay, relation, identityColumns, setColumns, changes,
	); applied || err != nil {
		return err
	}
	chunkRows := updateChunkRows(len(setColumns) + len(identityColumns))
	for start := 0; start < len(changes); start += chunkRows {
		end := start + chunkRows
		if end > len(changes) {
			end = len(changes)
		}
		if err := applyUpdateValueChunk(
			replay, relation, identityColumns, setColumns, changes[start:end],
		); err != nil {
			return err
		}
	}
	return nil
}

func applyUpdateTextStage(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) (bool, error) {
	if len(changes) < minimumTextCopyStageRows || !relation.capabilities.textCopyStage {
		return false, nil
	}
	// The text stage can only express the composite identity as paired range
	// bounds against stage columns. On a large, cold heap that shape can make
	// PostgreSQL spend minutes scanning candidates before it finds the exact
	// primary-key rows. The array and VALUES paths append the scalar exact-key
	// membership guard used by selective bitmap replay, so retain those paths
	// for precisely the relations where the guard is required.
	if useExactIdentityMembership(relation, identityColumns) {
		return false, nil
	}
	stageColumns := make([]targetColumn, 0, len(setColumns)+len(identityColumns))
	for _, columnIndex := range setColumns {
		stageColumns = append(stageColumns, relation.columns[columnIndex])
	}
	stageColumns = append(stageColumns, identityColumns...)
	if !textCopyStagePreferred(stageColumns) {
		return false, nil
	}
	values := make([]TupleDatum, 0, len(changes)*len(stageColumns))
	for row := range changes {
		for _, columnIndex := range setColumns {
			column := relation.columns[columnIndex]
			values = append(values, (*changes[row].New)[column.sourceIndex])
		}
		predicate := changes[row].Old
		if predicate == nil {
			predicate = changes[row].New
		}
		for _, column := range identityColumns {
			values = append(values, (*predicate)[column.sourceIndex])
		}
	}
	stage, applied, err := replay.loadTextCopyStage(
		relation, ChangeUpdate, stageColumns, values, len(changes),
	)
	if err != nil || !applied {
		return applied, err
	}

	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_target SET ")
	if len(setColumns) == 0 {
		sql.WriteString(relation.columns[0].quoted)
		sql.WriteString("=pgmigrate_target.")
		sql.WriteString(relation.columns[0].quoted)
	} else {
		for i, columnIndex := range setColumns {
			if i != 0 {
				sql.WriteByte(',')
			}
			sql.WriteString(relation.columns[columnIndex].quoted)
			fmt.Fprintf(&sql, "=pgmigrate_batch.column_%d", i)
		}
	}
	sql.WriteString(" FROM ")
	sql.WriteString(stage)
	sql.WriteString(" AS pgmigrate_batch WHERE ")
	if len(identityColumns) > 1 && useSelectiveBitmap(relation) {
		writeCompositeIdentityCTIDPredicate(
			&sql, relation, identityColumns, "column_", len(setColumns),
		)
	} else {
		writeBatchIdentityPredicate(&sql, identityColumns, "column_", len(setColumns))
	}
	sql.WriteString(" RETURNING pgmigrate_batch.ordinal")
	return true, replay.queue(sql.String(), nil, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "staged update " + relation.quoted,
		expectedRows: int64(len(changes)), expectedOrdinals: len(changes),
	})
}

func applyUpdateValueChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) error {
	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_target SET ")
	if len(setColumns) == 0 {
		sql.WriteString(relation.columns[0].quoted)
		sql.WriteString("=pgmigrate_target.")
		sql.WriteString(relation.columns[0].quoted)
	} else {
		for i, columnIndex := range setColumns {
			if i != 0 {
				sql.WriteByte(',')
			}
			sql.WriteString(relation.columns[columnIndex].quoted)
			fmt.Fprintf(&sql, "=pgmigrate_batch.set_%d", i)
		}
	}
	sql.WriteString(" FROM (VALUES ")
	params := make([]rawParam, 0, len(changes)*(len(setColumns)+len(identityColumns)))
	identityParamPositions := make([][]int, len(changes))
	for row := range changes {
		if row != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "(%d", row)
		for _, columnIndex := range setColumns {
			datum := (*changes[row].New)[relation.columns[columnIndex].sourceIndex]
			param, err := datumParam(relation, columnIndex, datum, ChangeUpdate)
			if err != nil {
				return err
			}
			params = append(params, param)
			fmt.Fprintf(&sql, ",$%d", len(params))
		}
		predicate := changes[row].Old
		if predicate == nil {
			predicate = changes[row].New
		}
		identityParamPositions[row] = make([]int, len(identityColumns))
		for i, column := range identityColumns {
			param, err := datumParamForColumn(
				relation, column, (*predicate)[column.sourceIndex], ChangeUpdate,
			)
			if err != nil {
				return err
			}
			params = append(params, param)
			identityParamPositions[row][i] = len(params)
			fmt.Fprintf(&sql, ",$%d", len(params))
		}
		sql.WriteByte(')')
	}
	sql.WriteString(") AS pgmigrate_batch(ordinal")
	for i := range setColumns {
		fmt.Fprintf(&sql, ",set_%d", i)
	}
	for i := range identityColumns {
		fmt.Fprintf(&sql, ",identity_%d", i)
	}
	sql.WriteString(") WHERE ")
	if len(identityColumns) > 1 && useSelectiveBitmap(relation) {
		writeCompositeIdentityCTIDPredicate(
			&sql, relation, identityColumns, "identity_", 0,
		)
	} else {
		writeBatchIdentityPredicate(&sql, identityColumns, "identity_", 0)
	}
	if len(identityColumns) == 1 && useExactIdentityMembership(relation, identityColumns) {
		sql.WriteString(" AND (")
		writeExactIdentityDisjunction(
			&sql, "pgmigrate_target", identityColumns, identityParamPositions,
		)
		sql.WriteByte(')')
	}
	sql.WriteString(" RETURNING pgmigrate_batch.ordinal")
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description: "batch update " + relation.quoted, expectedRows: int64(len(changes)),
		expectedOrdinals: len(changes),
	})
}

func applyUpdateArrayChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	setColumns []int,
	changes []Change,
) (bool, error) {
	params := make([]rawParam, 0, len(setColumns)+len(identityColumns))
	for _, columnIndex := range setColumns {
		column := relation.columns[columnIndex]
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			datums[row] = (*changes[row].New)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeUpdate)
		if err != nil {
			return true, err
		}
		if !supported {
			return false, nil
		}
		params = append(params, param)
	}
	for _, column := range identityColumns {
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			predicate := changes[row].Old
			if predicate == nil {
				predicate = changes[row].New
			}
			datums[row] = (*predicate)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeUpdate)
		if err != nil {
			return true, err
		}
		if !supported {
			return false, nil
		}
		params = append(params, param)
	}
	batchParamCount := len(params)
	var identityParamPositions [][]int
	if useExactIdentityMembership(relation, identityColumns) {
		var err error
		params, identityParamPositions, err = appendSelectiveIdentityScalarParams(
			params, relation, identityColumns, changes,
		)
		if err != nil {
			return true, err
		}
	}

	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_target SET ")
	if len(setColumns) == 0 {
		sql.WriteString(relation.columns[0].quoted)
		sql.WriteString("=pgmigrate_target.")
		sql.WriteString(relation.columns[0].quoted)
	} else {
		for i, columnIndex := range setColumns {
			if i != 0 {
				sql.WriteByte(',')
			}
			sql.WriteString(relation.columns[columnIndex].quoted)
			fmt.Fprintf(&sql, "=pgmigrate_batch.set_%d", i)
		}
	}
	sql.WriteString(" FROM unnest(")
	for i := 0; i < batchParamCount; i++ {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "$%d", i+1)
	}
	sql.WriteString(") WITH ORDINALITY AS pgmigrate_batch(")
	for i := range setColumns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "set_%d", i)
	}
	for i := range identityColumns {
		if len(setColumns) != 0 || i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "identity_%d", i)
	}
	if len(setColumns)+len(identityColumns) != 0 {
		sql.WriteByte(',')
	}
	sql.WriteString("ordinal) WHERE ")
	if len(identityColumns) > 1 && useSelectiveBitmap(relation) {
		writeCompositeIdentityCTIDPredicate(
			&sql, relation, identityColumns, "identity_", 0,
		)
	} else {
		writeBatchIdentityPredicate(&sql, identityColumns, "identity_", 0)
	}
	if len(identityColumns) == 1 && len(identityParamPositions) != 0 {
		sql.WriteString(" AND (")
		writeExactIdentityDisjunction(
			&sql, "pgmigrate_target", identityColumns, identityParamPositions,
		)
		sql.WriteByte(')')
	}
	sql.WriteString(" RETURNING pgmigrate_batch.ordinal - 1")
	return true, replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description:  "array batch update " + relation.quoted,
		expectedRows: int64(len(changes)), expectedOrdinals: len(changes),
	})
}

func applyUpdate(replay *applyPipeline, relation *targetRelation, change *Change) error {
	if err := validateTuple(relation, change.New, ChangeUpdate); err != nil {
		return err
	}
	predicate := change.Old
	if predicate == nil {
		predicate = change.New
	}
	if err := validateTuple(relation, predicate, ChangeUpdate); err != nil {
		return err
	}
	if len(relation.columns) == 0 {
		return applyGeneratedOnlyUpdate(replay, relation, *predicate)
	}

	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" SET ")
	params := make([]rawParam, 0, len(relation.columns)*2)
	setCount := 0
	for i := range relation.columns {
		datum := (*change.New)[relation.columns[i].sourceIndex]
		if datum.Kind == DatumUnchangedToast {
			continue
		}
		if setCount != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(relation.columns[i].quoted)
		param, err := datumParam(relation, i, datum, ChangeUpdate)
		if err != nil {
			return err
		}
		params = append(params, param)
		fmt.Fprintf(&sql, "=$%d", len(params))
		setCount++
	}
	if setCount == 0 {
		sql.WriteString(relation.columns[0].quoted)
		sql.WriteByte('=')
		sql.WriteString(relation.columns[0].quoted)
	}
	sql.WriteString(" WHERE ")
	if err := appendPredicate(&sql, &params, relation, *predicate, ChangeUpdate); err != nil {
		return err
	}
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description: "update " + relation.quoted, expectedRows: 1,
	})
}

func applyGeneratedOnlyUpdate(
	replay *applyPipeline,
	relation *targetRelation,
	predicate Tuple,
) error {
	if !hasReplicaIdentityColumns(relation) {
		// pgoutput excludes generated columns by default. With no published
		// identity and no writable target columns, the source update cannot
		// carry target-writable state, so there is nothing safe to execute.
		return nil
	}
	if len(relation.generatedColumns) == 0 {
		return divergenceFor(relation, ChangeUpdate, "relation has no column available for a generated-only no-op")
	}
	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" SET ")
	sql.WriteString(relation.generatedColumns[0].quoted)
	sql.WriteString("=DEFAULT WHERE ")
	params := make([]rawParam, 0, len(predicateTargetColumns(relation)))
	if err := appendPredicate(&sql, &params, relation, predicate, ChangeUpdate); err != nil {
		return err
	}
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeUpdate,
		description: "generated-only update " + relation.quoted, expectedRows: 1,
	})
}

func hasReplicaIdentityColumns(relation *targetRelation) bool {
	if relation.source.ReplicaIdentity == 'f' {
		return len(predicateTargetColumns(relation)) != 0
	}
	for _, column := range predicateTargetColumns(relation) {
		if column.key {
			return true
		}
	}
	return false
}

func applyDeletes(replay *applyPipeline, relation *targetRelation, changes []Change) error {
	identityColumns := batchUpdateIdentityColumns(relation)
	if primary, safe := primaryKeyDeleteColumns(relation); safe {
		identityColumns = primary
	}
	if len(changes) < 2 || len(identityColumns) == 0 {
		for i := range changes {
			if err := applyDelete(replay, relation, &changes[i]); err != nil {
				return err
			}
		}
		return nil
	}

	chunkRows := applyArrayChunkRows
	if useExactIdentityMembership(relation, identityColumns) {
		chunkRows = applySelectiveProbeChunkRows
	}
	for start := 0; start < len(changes); {
		firstKey, err := batchDeleteIdentityKey(relation, identityColumns, &changes[start])
		if err != nil {
			return err
		}
		seen := map[string]struct{}{firstKey: {}}
		end := start + 1
		for end < len(changes) && end-start < chunkRows {
			key, err := batchDeleteIdentityKey(relation, identityColumns, &changes[end])
			if err != nil {
				return err
			}
			if _, duplicate := seen[key]; duplicate {
				break
			}
			seen[key] = struct{}{}
			end++
		}
		if end-start == 1 {
			if err := applyDelete(replay, relation, &changes[start]); err != nil {
				return err
			}
		} else if err := applyDeleteChunk(
			replay, relation, identityColumns, changes[start:end],
		); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// primaryKeyDeleteColumns returns the target primary key in its catalog index
// order only when pgoutput's old tuple carries every component. Composite row
// bounds are order-sensitive: using table-column or source replica-identity
// order can make PostgreSQL miss the primary-key access path on a large table.
func primaryKeyDeleteColumns(relation *targetRelation) ([]targetColumn, bool) {
	if relation == nil || !relation.capabilities.keyedSetDML {
		return nil, false
	}
	primary := primaryKeyColumns(relation)
	if len(primary) == 0 {
		return nil, false
	}
	for _, column := range primary {
		if !column.key || column.sourceIndex < 0 {
			return nil, false
		}
	}
	return primary, true
}

func deleteUsesTargetPrimaryKey(relation *targetRelation, identityColumns []targetColumn) bool {
	primary, safe := primaryKeyDeleteColumns(relation)
	if !safe || len(primary) != len(identityColumns) {
		return false
	}
	for i := range primary {
		if primary[i].name != identityColumns[i].name {
			return false
		}
	}
	return true
}

func writeDeleteIdentityPredicate(
	sql *strings.Builder,
	relation *targetRelation,
	identityColumns []targetColumn,
	prefix string,
	offset int,
) {
	if deleteUsesTargetPrimaryKey(relation, identityColumns) {
		writeCompositePrimaryKeyCTIDPredicate(sql, relation, identityColumns, prefix, offset)
		return
	}
	writeBatchIdentityPredicate(sql, identityColumns, prefix, offset)
}

func appendPrimaryKeyDeletePredicate(
	sql *strings.Builder,
	params *[]rawParam,
	relation *targetRelation,
	primary []targetColumn,
	tuple Tuple,
) error {
	for i, column := range primary {
		datum := tuple[column.sourceIndex]
		if datum.Kind == DatumNull {
			return divergenceFor(relation, ChangeDelete, "primary key contains NULL")
		}
		if datum.Kind == DatumUnchangedToast {
			return divergenceFor(relation, ChangeDelete, "primary key contains unchanged TOAST")
		}
		param, err := datumParamForColumn(relation, column, datum, ChangeDelete)
		if err != nil {
			return err
		}
		*params = append(*params, param)
		if i != 0 {
			sql.WriteString(" AND ")
		}
		sql.WriteString(column.quoted)
		fmt.Fprintf(sql, " = $%d", len(*params))
	}
	return nil
}

func batchDeleteIdentityKey(
	relation *targetRelation,
	identityColumns []targetColumn,
	change *Change,
) (string, error) {
	if err := validateTuple(relation, change.Old, ChangeDelete); err != nil {
		return "", err
	}
	var key strings.Builder
	for _, column := range identityColumns {
		datum := (*change.Old)[column.sourceIndex]
		if column.primary && datum.Kind == DatumNull {
			return "", divergenceFor(relation, ChangeDelete, "primary key contains NULL")
		}
		if datum.Kind == DatumUnchangedToast {
			return "", divergenceFor(relation, ChangeDelete, "replica identity contains unchanged TOAST")
		}
		if _, err := datumParamForColumn(relation, column, datum, ChangeDelete); err != nil {
			return "", err
		}
		key.WriteByte(byte(datum.Kind))
		key.WriteString(strconv.Itoa(len(datum.Data)))
		key.WriteByte(':')
		key.Write(datum.Data)
	}
	return key.String(), nil
}

func applyDeleteChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) error {
	if applied, err := applyDeleteTextStage(
		replay, relation, identityColumns, changes,
	); applied || err != nil {
		return err
	}
	if applied, err := applyDeleteArrayChunk(
		replay, relation, identityColumns, changes,
	); applied || err != nil {
		return err
	}
	chunkRows := updateChunkRows(len(identityColumns))
	for start := 0; start < len(changes); start += chunkRows {
		end := start + chunkRows
		if end > len(changes) {
			end = len(changes)
		}
		if err := applyDeleteValueChunk(
			replay, relation, identityColumns, changes[start:end],
		); err != nil {
			return err
		}
	}
	return nil
}

func applyDeleteTextStage(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) (bool, error) {
	if len(changes) < minimumTextCopyStageRows ||
		!relation.capabilities.textCopyStage ||
		useExactIdentityMembership(relation, identityColumns) ||
		!textCopyStagePreferred(identityColumns) {
		return false, nil
	}
	values := make([]TupleDatum, 0, len(changes)*len(identityColumns))
	for row := range changes {
		for _, column := range identityColumns {
			values = append(values, (*changes[row].Old)[column.sourceIndex])
		}
	}
	stage, applied, err := replay.loadTextCopyStage(
		relation, ChangeDelete, identityColumns, values, len(changes),
	)
	if err != nil || !applied {
		return applied, err
	}

	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_target USING ")
	sql.WriteString(stage)
	sql.WriteString(" AS pgmigrate_batch WHERE ")
	writeDeleteIdentityPredicate(&sql, relation, identityColumns, "column_", 0)
	sql.WriteString(" RETURNING pgmigrate_batch.ordinal")
	return true, replay.queue(sql.String(), nil, applyExpectation{
		relation: relation, kind: ChangeDelete,
		description:  "staged delete from " + relation.quoted,
		expectedRows: int64(len(changes)), expectedOrdinals: len(changes),
		allowMissingRows: deleteUsesTargetPrimaryKey(relation, identityColumns),
	})
}

func applyDeleteValueChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) error {
	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_target USING (VALUES ")
	params := make([]rawParam, 0, len(changes)*len(identityColumns))
	identityParamPositions := make([][]int, len(changes))
	for row := range changes {
		if row != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "(%d", row)
		identityParamPositions[row] = make([]int, len(identityColumns))
		for i, column := range identityColumns {
			param, err := datumParamForColumn(
				relation, column, (*changes[row].Old)[column.sourceIndex], ChangeDelete,
			)
			if err != nil {
				return err
			}
			params = append(params, param)
			identityParamPositions[row][i] = len(params)
			fmt.Fprintf(&sql, ",$%d", len(params))
		}
		sql.WriteByte(')')
	}
	sql.WriteString(") AS pgmigrate_batch(ordinal")
	for i := range identityColumns {
		fmt.Fprintf(&sql, ",identity_%d", i)
	}
	sql.WriteString(") WHERE ")
	writeDeleteIdentityPredicate(&sql, relation, identityColumns, "identity_", 0)
	if useExactIdentityMembership(relation, identityColumns) &&
		!deleteUsesTargetPrimaryKey(relation, identityColumns) {
		sql.WriteString(" AND (")
		writeExactIdentityDisjunction(
			&sql, "pgmigrate_target", identityColumns, identityParamPositions,
		)
		sql.WriteByte(')')
	}
	sql.WriteString(" RETURNING pgmigrate_batch.ordinal")
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeDelete,
		description: "batch delete from " + relation.quoted, expectedRows: int64(len(changes)),
		expectedOrdinals: len(changes),
		allowMissingRows: deleteUsesTargetPrimaryKey(relation, identityColumns),
	})
}

func applyDeleteArrayChunk(
	replay *applyPipeline,
	relation *targetRelation,
	identityColumns []targetColumn,
	changes []Change,
) (bool, error) {
	params := make([]rawParam, 0, len(identityColumns))
	for _, column := range identityColumns {
		datums := make([]TupleDatum, len(changes))
		for row := range changes {
			datums[row] = (*changes[row].Old)[column.sourceIndex]
		}
		param, supported, err := arrayParamForColumn(relation, column, datums, ChangeDelete)
		if err != nil {
			return true, err
		}
		if !supported {
			return false, nil
		}
		params = append(params, param)
	}
	batchParamCount := len(params)
	var identityParamPositions [][]int
	if useExactIdentityMembership(relation, identityColumns) &&
		!deleteUsesTargetPrimaryKey(relation, identityColumns) {
		var err error
		params, identityParamPositions, err = appendDeleteIdentityScalarParams(
			params, relation, identityColumns, changes,
		)
		if err != nil {
			return true, err
		}
	}

	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" AS pgmigrate_target USING unnest(")
	for i := 0; i < batchParamCount; i++ {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "$%d", i+1)
	}
	sql.WriteString(") WITH ORDINALITY AS pgmigrate_batch(")
	for i := range identityColumns {
		if i != 0 {
			sql.WriteByte(',')
		}
		fmt.Fprintf(&sql, "identity_%d", i)
	}
	sql.WriteString(",ordinal) WHERE ")
	writeDeleteIdentityPredicate(&sql, relation, identityColumns, "identity_", 0)
	if len(identityParamPositions) != 0 {
		sql.WriteString(" AND (")
		writeExactIdentityDisjunction(
			&sql, "pgmigrate_target", identityColumns, identityParamPositions,
		)
		sql.WriteByte(')')
	}
	sql.WriteString(" RETURNING pgmigrate_batch.ordinal - 1")
	return true, replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeDelete,
		description:  "array batch delete from " + relation.quoted,
		expectedRows: int64(len(changes)), expectedOrdinals: len(changes),
		allowMissingRows: deleteUsesTargetPrimaryKey(relation, identityColumns),
	})
}

func applyDelete(replay *applyPipeline, relation *targetRelation, change *Change) error {
	if err := validateTuple(relation, change.Old, ChangeDelete); err != nil {
		return err
	}
	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" WHERE ")
	params := make([]rawParam, 0, len(relation.columns))
	primary, safe := primaryKeyDeleteColumns(relation)
	if safe {
		if err := appendPrimaryKeyDeletePredicate(
			&sql, &params, relation, primary, *change.Old,
		); err != nil {
			return err
		}
	} else {
		if err := appendPredicate(&sql, &params, relation, *change.Old, ChangeDelete); err != nil {
			return err
		}
	}
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeDelete,
		description: "delete from " + relation.quoted, expectedRows: 1,
		allowMissingRows: safe,
	})
}

func applyTruncates(
	replay *applyPipeline,
	relations map[uint32]*targetRelation,
	changes []Change,
) error {
	var sql strings.Builder
	sql.WriteString("TRUNCATE TABLE ")
	for i := range changes {
		relation := relations[changes[i].RelationOID]
		if relation == nil {
			return divergenceFor(nil, ChangeTruncate, "required relation metadata is missing")
		}
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(relation.quoted)
	}
	appendTruncateOptions(&sql, changes[0])
	return replay.queue(sql.String(), nil, applyExpectation{
		relation: relations[changes[0].RelationOID], kind: ChangeTruncate,
		description: "truncate target relations", expectedRows: -1,
	})
}

func sameTruncateOptions(left, right Change) bool {
	return left.TruncateCascade == right.TruncateCascade &&
		left.TruncateRestartIdentity == right.TruncateRestartIdentity
}

func appendTruncateOptions(sql *strings.Builder, change Change) {
	if change.TruncateRestartIdentity {
		sql.WriteString(" RESTART IDENTITY")
	}
	if change.TruncateCascade {
		sql.WriteString(" CASCADE")
	}
}

func appendPredicate(
	sql *strings.Builder,
	params *[]rawParam,
	relation *targetRelation,
	tuple Tuple,
	kind ChangeKind,
) error {
	useAll := relation.source.ReplicaIdentity == 'f'
	count := 0
	for _, column := range predicateTargetColumns(relation) {
		if !useAll && !column.key {
			continue
		}
		datum := tuple[column.sourceIndex]
		if datum.Kind == DatumUnchangedToast {
			return divergenceFor(relation, kind, "replica identity contains unchanged TOAST")
		}
		if count != 0 {
			sql.WriteString(" AND ")
		}
		param, err := datumParamForColumn(relation, column, datum, kind)
		if err != nil {
			return err
		}
		*params = append(*params, param)
		sql.WriteString(column.quoted)
		// IS NOT DISTINCT FROM has no btree strategy, so a predicate built from
		// it cannot use any index and every UPDATE/DELETE degrades to a
		// sequential scan. On a NOT NULL column plain equality is equivalent,
		// because neither form can match a NULL that cannot exist, and it lets
		// the planner reach the replica identity index. Nullable columns only
		// appear here under REPLICA IDENTITY FULL and still need the NULL-safe
		// form.
		if column.notNull {
			fmt.Fprintf(sql, " = $%d", len(*params))
		} else {
			fmt.Fprintf(sql, " IS NOT DISTINCT FROM $%d", len(*params))
		}
		count++
	}
	if count == 0 {
		return divergenceFor(relation, kind, "no replica identity columns are available")
	}
	return nil
}

func predicateTargetColumns(relation *targetRelation) []targetColumn {
	if relation.mappedColumns != nil {
		return relation.mappedColumns
	}
	return relation.columns
}

func datumParam(relation *targetRelation, index int, datum TupleDatum, kind ChangeKind) (rawParam, error) {
	return datumParamForColumn(relation, relation.columns[index], datum, kind)
}

func datumParamForColumn(
	relation *targetRelation,
	column targetColumn,
	datum TupleDatum,
	kind ChangeKind,
) (rawParam, error) {
	param := rawParam{oid: column.oid}
	switch datum.Kind {
	case DatumNull:
		param.isNull = true
		return param, nil
	case DatumText:
		param.data = datum.Data
		return param, nil
	case DatumBinary:
		if sourceOID := relation.source.Columns[column.sourceIndex].Type; sourceOID != param.oid {
			return rawParam{}, divergenceFor(relation, kind, fmt.Sprintf(
				"binary column %q source OID %d differs from target OID %d",
				column.name, sourceOID, param.oid,
			))
		}
		param.data = datum.Data
		param.format = 1
		return param, nil
	case DatumUnchangedToast:
		return rawParam{}, divergenceFor(relation, kind, "unchanged TOAST cannot be used as a parameter")
	default:
		return rawParam{}, divergenceFor(relation, kind, fmt.Sprintf("invalid datum kind %d", datum.Kind))
	}
}

// paramValues renders bind parameters for the extended query protocol, where a
// nil value means SQL NULL and a non-nil zero-length one means a zero-length
// value. Only an explicitly null parameter may be nil.
func paramValues(params []rawParam) [][]byte {
	values := make([][]byte, len(params))
	for i := range params {
		switch {
		case params[i].isNull:
			values[i] = nil
		case params[i].data == nil:
			values[i] = emptyParamValue
		default:
			values[i] = params[i].data
		}
	}
	return values
}

func validateTuple(relation *targetRelation, tuple *Tuple, kind ChangeKind) error {
	if tuple == nil {
		return divergenceFor(relation, kind, "required tuple is absent")
	}
	if len(*tuple) != len(relation.source.Columns) {
		return divergenceFor(relation, kind, fmt.Sprintf(
			"tuple has %d columns, pgoutput relation has %d", len(*tuple), len(relation.source.Columns),
		))
	}
	return nil
}

func divergenceFor(relation *targetRelation, kind ChangeKind, reason string) error {
	name := "<unknown>"
	if relation != nil {
		name = relation.source.Namespace + "." + relation.source.Name
	}
	return &DivergenceError{Relation: name, Change: kind, Reason: reason}
}

func changeKindName(kind ChangeKind) string {
	switch kind {
	case ChangeInsert:
		return "insert"
	case ChangeUpdate:
		return "update"
	case ChangeDelete:
		return "delete"
	case ChangeTruncate:
		return "truncate"
	default:
		return "metadata"
	}
}
