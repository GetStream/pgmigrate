package cdc

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	StreamID             string
	StreamGeneration     string
	FreshSetup           bool
	TargetHasCopiedData  bool
	Durable              *DurableWatermark
	PollInterval         time.Duration
	ReconnectDelay       time.Duration
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
		applied, next, err := a.applyFromReader(ctx, conn, reader, LSN(progress))
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
	return a.applyFromReader(ctx, conn, reader, progress)
}

func (a *Applier) applyFromReader(
	ctx context.Context,
	conn *pgx.Conn,
	reader *Reader,
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
		applyErr := a.applyTransaction(ctx, conn, &transaction)
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

func (a *Applier) applyTransaction(ctx context.Context, conn *pgx.Conn, transaction *Transaction) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cdc: begin target transaction: %w", err)
	}
	defer tx.Rollback(context.Background())
	// Logical replication contains the child-row changes produced by source
	// cascades. Suppress target triggers and referential actions while replaying
	// so those rows are changed exactly once.
	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: disable target replication triggers: %w", err))
	}

	relations := make(map[uint32]*targetRelation, len(transaction.Relations))
	for i := range transaction.Relations {
		relation, err := loadTargetRelation(ctx, tx, &transaction.Relations[i])
		if err != nil {
			return err
		}
		relations[relation.source.OID] = relation
	}

	collector := newSampleCollector(a.config.Sampler, transaction)
	if transaction.Spill != nil {
		if err := a.applySpilledChanges(ctx, tx, relations, transaction.Spill, collector); err != nil {
			return err
		}
	} else {
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
				if err := applyInserts(ctx, tx, relation, transaction.Changes[i:end]); err != nil {
					return err
				}
				collector.addAll(transaction.Changes[i:end])
				i = end
			case ChangeUpdate:
				if err := applyUpdate(ctx, tx, relation, change); err != nil {
					return err
				}
				collector.add(change)
				i++
			case ChangeDelete:
				if err := applyDelete(ctx, tx, relation, change); err != nil {
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
				if err := applyTruncates(ctx, tx, relations, transaction.Changes[i:end]); err != nil {
					return err
				}
				i = end
			default:
				return divergenceFor(relation, change.Kind, "unknown change kind")
			}
		}
	}
	if err := updateStreamProgress(
		ctx, tx, a.config.StreamID, a.config.StreamGeneration, transaction.EndLSN,
	); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: update transactional apply progress: %w", err))
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: commit target transaction: %w", err))
	}
	collector.flush()
	return nil
}

func loadTargetRelation(ctx context.Context, tx pgx.Tx, source *Relation) (*targetRelation, error) {
	rows, err := tx.Query(ctx, `
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
	ctx context.Context,
	tx pgx.Tx,
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
			err = applyInserts(ctx, tx, relation, pending)
			if err == nil {
				collector.addAll(pending)
			}
		case ChangeTruncate:
			err = applyTruncates(ctx, tx, relations, pending)
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
			if err := applyUpdate(ctx, tx, relation, &change); err != nil {
				return err
			}
			collector.add(&change)
			return nil
		case ChangeDelete:
			if err := flush(); err != nil {
				return err
			}
			if err := applyDelete(ctx, tx, relation, &change); err != nil {
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

// emptyParamValue is a non-nil zero-length parameter value, which the extended
// query protocol reads as a zero-length value rather than as NULL.
var emptyParamValue = []byte{}

func applyInserts(ctx context.Context, tx pgx.Tx, relation *targetRelation, changes []Change) error {
	chunkRows := insertChunkRows(len(relation.columns))
	for start := 0; start < len(changes); start += chunkRows {
		end := start + chunkRows
		if end > len(changes) {
			end = len(changes)
		}
		if err := applyInsertChunk(ctx, tx, relation, changes[start:end]); err != nil {
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

func applyInsertChunk(ctx context.Context, tx pgx.Tx, relation *targetRelation, changes []Change) error {
	if len(relation.columns) == 0 {
		sql := "INSERT INTO " + relation.quoted + " DEFAULT VALUES"
		for i := range changes {
			if err := validateTuple(relation, changes[i].New, ChangeInsert); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, sql)
			if err != nil {
				return classifyApplyError(relation, ChangeInsert, fmt.Errorf("insert default row into %s: %w", relation.quoted, err))
			}
			if tag.RowsAffected() != 1 {
				return divergenceFor(relation, ChangeInsert, fmt.Sprintf(
					"default insert affected %d rows, expected exactly one", tag.RowsAffected(),
				))
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
	tag, err := execRaw(ctx, tx, sql.String(), params)
	if err != nil {
		return classifyApplyError(relation, ChangeInsert, fmt.Errorf("insert into %s: %w", relation.quoted, err))
	}
	if tag.RowsAffected() != int64(len(changes)) {
		return divergenceFor(relation, ChangeInsert, fmt.Sprintf(
			"insert affected %d rows, expected %d", tag.RowsAffected(), len(changes),
		))
	}
	return nil
}

func applyUpdate(ctx context.Context, tx pgx.Tx, relation *targetRelation, change *Change) error {
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
		return applyGeneratedOnlyUpdate(ctx, tx, relation, *predicate)
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
	tag, err := execRaw(ctx, tx, sql.String(), params)
	if err != nil {
		return classifyApplyError(relation, ChangeUpdate, fmt.Errorf("update %s: %w", relation.quoted, err))
	}
	return requireOne(relation, ChangeUpdate, tag)
}

func applyGeneratedOnlyUpdate(
	ctx context.Context,
	tx pgx.Tx,
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
	tag, err := execRaw(ctx, tx, sql.String(), params)
	if err != nil {
		return classifyApplyError(relation, ChangeUpdate, fmt.Errorf("generated-only update %s: %w", relation.quoted, err))
	}
	return requireOne(relation, ChangeUpdate, tag)
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

func applyDelete(ctx context.Context, tx pgx.Tx, relation *targetRelation, change *Change) error {
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
	tag, err := execRaw(ctx, tx, sql.String(), params)
	if err != nil {
		return classifyApplyError(relation, ChangeDelete, fmt.Errorf("delete from %s: %w", relation.quoted, err))
	}
	return requireOne(relation, ChangeDelete, tag)
}

func applyTruncates(
	ctx context.Context,
	tx pgx.Tx,
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
	if _, err := tx.Exec(ctx, sql.String()); err != nil {
		return classifyApplyError(relations[changes[0].RelationOID], ChangeTruncate, fmt.Errorf("truncate target relations: %w", err))
	}
	return nil
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

func execRaw(ctx context.Context, tx pgx.Tx, sql string, params []rawParam) (pgconn.CommandTag, error) {
	values := paramValues(params)
	oids := make([]uint32, len(params))
	formats := make([]int16, len(params))
	for i := range params {
		oids[i] = params[i].oid
		formats[i] = params[i].format
	}
	result := tx.Conn().PgConn().ExecParams(ctx, sql, values, oids, formats, nil)
	return result.Close()
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

func requireOne(relation *targetRelation, kind ChangeKind, tag pgconn.CommandTag) error {
	if tag.RowsAffected() != 1 {
		return divergenceFor(relation, kind, fmt.Sprintf(
			"affected %d rows, expected exactly one", tag.RowsAffected(),
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
