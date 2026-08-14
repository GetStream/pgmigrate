package cdc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	// Workers is the number of target sessions used for replay. Transactions
	// that touch independent relations execute concurrently, while commits and
	// progress remain in source order. Values below one preserve serial replay.
	Workers int
	// BatchSize bounds contiguous dependent source transactions combined into
	// one target transaction. Window bounds source transactions held by the
	// scheduler while it searches for independent table work.
	BatchSize           int
	Window              int
	StreamID            string
	StreamGeneration    string
	FreshSetup          bool
	TargetHasCopiedData bool
	Durable             *DurableWatermark
	PollInterval        time.Duration
	ReconnectDelay      time.Duration
	// EndPosition returns the optional inclusive cutover boundary. Transactions
	// beyond it are never applied.
	EndPosition func(context.Context) (LSN, bool, error)
	// AfterProgress runs after target data and progress commit. Maintenance
	// failures are terminal rather than hidden behind reconnect.
	AfterProgress ProgressCallback
	// Sampler, when set, is told which rows each committed transaction wrote, so
	// that verification can check the replication path rather than only the rows
	// the base copy left in the heap.
	Sampler KeySampler
}

type Applier struct {
	config ApplierConfig

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
	if config.Workers < 1 {
		config.Workers = 1
	}
	if config.BatchSize < 1 {
		config.BatchSize = 1
	}
	if config.Window < 1 {
		config.Window = config.Workers * 4
	} else if config.Window < config.Workers {
		config.Window = config.Workers
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

	progress, progressExists, err := postgres.ReadProgress(ctx, conn, a.config.StreamID)
	if err != nil {
		return fmt.Errorf("cdc: read apply progress: %w", err)
	}
	if err := configureApplySession(ctx, conn); err != nil {
		return err
	}
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
	if a.config.Workers > 1 {
		pool, err := newApplyWorkerPool(ctx, a, conn)
		if err != nil {
			return err
		}
		defer pool.stop()
		return a.runConcurrentConnection(ctx, pool, reader, LSN(progress))
	}
	relationCache := newTargetRelationCache()
	statementCache := newApplyStatementCache(applyStatementCacheCapacity)
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
			ctx, conn, reader, relationCache, statementCache, LSN(progress),
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

func configureApplySession(ctx context.Context, conn *pgx.Conn) error {
	// This connection is dedicated to logical replay. Set replica role once so
	// every source transaction suppresses target triggers and referential
	// actions without paying an extra target round trip per transaction.
	if _, err := conn.Exec(ctx, "SET session_replication_role = replica"); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: disable target replication triggers: %w", err))
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
		newApplyStatementCache(applyStatementCacheCapacity), progress,
	)
}

func (a *Applier) applyFromReader(
	ctx context.Context,
	conn *pgx.Conn,
	reader *Reader,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	progress LSN,
) (bool, LSN, error) {
	for {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return false, progress, nil // nothing staged is left to apply
		}
		if err != nil {
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
		if a.config.EndPosition != nil {
			end, set, err := a.effectiveEndPosition(ctx)
			if err != nil {
				return false, progress, errors.Join(err, transaction.CleanupSpill())
			}
			if set && transaction.EndLSN > end {
				if err := transaction.CleanupSpill(); err != nil {
					return false, progress, fmt.Errorf("cdc: cleanup post-boundary reader spill: %w", err)
				}
				return false, progress, nil
			}
		}
		applyErr := a.applyTransaction(ctx, conn, relationCache, statementCache, &transaction)
		cleanupErr := transaction.CleanupSpill()
		if applyErr != nil {
			return false, progress, errors.Join(applyErr, cleanupErr)
		}
		if cleanupErr != nil {
			return false, progress, fmt.Errorf("cdc: cleanup applied reader spill: %w", cleanupErr)
		}
		return true, transaction.EndLSN, nil
	}
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
	columns          []targetColumn
	mappedColumns    []targetColumn
	generatedColumns []targetColumn
	overrideIdentity bool
}

type targetColumn struct {
	name        string
	quoted      string
	oid         uint32
	key         bool
	identity    string
	sourceIndex int
	generated   bool
	notNull     bool
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
	transaction *Transaction,
) error {
	prepared, err := a.prepareTransaction(ctx, conn, relationCache, statementCache, transaction)
	if err != nil {
		return err
	}
	return a.commitPreparedTransaction(prepared, transaction.EndLSN)
}

type preparedTransaction struct {
	replay     *applyPipeline
	collectors []*sampleCollector
}

func (a *Applier) prepareTransaction(
	ctx context.Context,
	conn *pgx.Conn,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	transaction *Transaction,
) (*preparedTransaction, error) {
	return a.prepareTransactions(
		ctx, conn, relationCache, statementCache, []Transaction{*transaction},
	)
}

func (a *Applier) prepareTransactions(
	ctx context.Context,
	conn *pgx.Conn,
	relationCache *targetRelationCache,
	statementCache *applyStatementCache,
	transactions []Transaction,
) (*preparedTransaction, error) {
	relationSets := make([]map[uint32]*targetRelation, len(transactions))
	for transactionIndex := range transactions {
		transaction := &transactions[transactionIndex]
		relations := make(map[uint32]*targetRelation, len(transaction.Relations))
		for i := range transaction.Relations {
			relation, err := relationCache.resolve(ctx, conn, &transaction.Relations[i], loadTargetRelation)
			if err != nil {
				return nil, err
			}
			relations[relation.source.OID] = relation
		}
		relationSets[transactionIndex] = relations
	}

	replay := newApplyPipeline(ctx, conn.PgConn(), statementCache)
	replay.begin()
	var replayErr error
	collectors := make([]*sampleCollector, 0, len(transactions))
	for i := range transactions {
		transaction := &transactions[i]
		collector := newSampleCollector(a.config.Sampler, transaction)
		collectors = append(collectors, collector)
		if replayErr = a.queueTransaction(replay, relationSets[i], transaction, collector); replayErr != nil {
			break
		}
	}
	if replayErr == nil {
		replayErr = replay.sync()
	}
	if replayErr == nil && replay.conn.TxStatus() != 'T' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after replay is %q, want %q",
			replay.conn.TxStatus(), 'T',
		)
	}
	if replayErr != nil {
		return nil, errors.Join(replayErr, replay.abort())
	}
	return &preparedTransaction{replay: replay, collectors: collectors}, nil
}

func (a *Applier) queueTransaction(
	replay *applyPipeline,
	relations map[uint32]*targetRelation,
	transaction *Transaction,
	collector *sampleCollector,
) error {
	if transaction.Spill != nil {
		return a.applySpilledChanges(replay, relations, transaction.Spill, collector)
	}
	for i := 0; i < len(transaction.Changes); {
		change := &transaction.Changes[i]
		relation := relations[change.RelationOID]
		if relation == nil {
			return divergenceFor(nil, change.Kind, "required relation metadata is missing")
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
				return err
			}
			collector.addAll(transaction.Changes[i:end])
			i = end
		case ChangeUpdate:
			if err := applyUpdate(replay, relation, change); err != nil {
				return err
			}
			collector.add(change)
			i++
		case ChangeDelete:
			if err := applyDelete(replay, relation, change); err != nil {
				return err
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
				return err
			}
			i = end
		default:
			return divergenceFor(relation, change.Kind, "unknown change kind")
		}
	}
	return nil
}

func (a *Applier) commitPreparedTransaction(prepared *preparedTransaction, endLSN LSN) error {
	prepared.replay.queueProgress(a.config.StreamID, a.config.StreamGeneration, endLSN)
	prepared.replay.commit()
	replayErr := prepared.replay.sync()
	if replayErr == nil && prepared.replay.conn.TxStatus() != 'I' {
		replayErr = fmt.Errorf(
			"cdc: target transaction status after commit is %q, want %q",
			prepared.replay.conn.TxStatus(), 'I',
		)
	}
	if replayErr != nil {
		return errors.Join(replayErr, prepared.abort())
	}
	if err := prepared.replay.close(); err != nil {
		return err
	}
	for _, collector := range prepared.collectors {
		collector.flush()
	}
	return nil
}

func (p *preparedTransaction) abort() error {
	if p == nil || p.replay == nil {
		return nil
	}
	return p.replay.abort()
}

func loadTargetRelation(ctx context.Context, db targetRelationQuerier, source *Relation) (*targetRelation, error) {
	rows, err := db.Query(ctx, `
		SELECT a.attname, a.atttypid, a.attidentity::text, a.attgenerated <> '', a.attnotnull
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
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
	}
	for rows.Next() {
		var column targetColumn
		if err := rows.Scan(
			&column.name, &column.oid, &column.identity, &column.generated, &column.notNull,
		); err != nil {
			return nil, err
		}
		if column.identity == "a" {
			result.overrideIdentity = true
		}
		column.quoted = pgx.Identifier{column.name}.Sanitize()
		if column.generated {
			result.generatedColumns = append(result.generatedColumns, column)
		} else {
			result.columns = append(result.columns, column)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
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
	}
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

const applyPipelineWindow = 256

type applyResultKind byte

const (
	applyCommandResult applyResultKind = iota
	applyPrepareResult
	applyDeallocateResult
)

type applyExpectation struct {
	resultKind    applyResultKind
	relation      *targetRelation
	kind          ChangeKind
	description   string
	expectedRows  int64
	expectedTag   string
	progressGuard bool
	statement     string
	paramOIDs     []uint32
}

type applyPipeline struct {
	conn         *pgconn.PgConn
	pipeline     *pgconn.Pipeline
	statements   *applyStatementCache
	expectations []applyExpectation
	commands     int
	closed       bool
}

func newApplyPipeline(
	ctx context.Context,
	conn *pgconn.PgConn,
	statements *applyStatementCache,
) *applyPipeline {
	return &applyPipeline{
		conn:       conn,
		pipeline:   conn.StartPipeline(ctx),
		statements: statements,
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

func (p *applyPipeline) queueProgress(streamID, generation string, remoteLSN LSN) {
	p.queueUnprepared(
		streamProgressSQL,
		streamProgressParams(streamID, generation, remoteLSN),
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
	if p.commands >= applyPipelineWindow {
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
			tag, closeErr := reader.Close()
			if closeErr != nil {
				if firstErr == nil {
					firstErr = expectation.classify(closeErr)
				}
				continue
			}
			if expectation.expectedRows >= 0 && tag.RowsAffected() != expectation.expectedRows && firstErr == nil {
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
	return errors.Join(result, p.close())
}

// emptyParamValue is a non-nil zero-length parameter value, which the extended
// query protocol reads as a zero-length value rather than as NULL.
var emptyParamValue = []byte{}

func applyInserts(replay *applyPipeline, relation *targetRelation, changes []Change) error {
	chunkRows := insertChunkRows(len(relation.columns))
	for start := 0; start < len(changes); start += chunkRows {
		end := start + chunkRows
		if end > len(changes) {
			end = len(changes)
		}
		if err := applyInsertChunk(replay, relation, changes[start:end]); err != nil {
			return err
		}
	}
	return nil
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

func applyDelete(replay *applyPipeline, relation *targetRelation, change *Change) error {
	if err := validateTuple(relation, change.Old, ChangeDelete); err != nil {
		return err
	}
	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(relation.quoted)
	sql.WriteString(" WHERE ")
	params := make([]rawParam, 0, len(relation.columns))
	if err := appendPredicate(&sql, &params, relation, *change.Old, ChangeDelete); err != nil {
		return err
	}
	return replay.queue(sql.String(), params, applyExpectation{
		relation: relation, kind: ChangeDelete,
		description: "delete from " + relation.quoted, expectedRows: 1,
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
