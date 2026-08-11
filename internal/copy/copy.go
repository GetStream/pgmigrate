// Package copy inventories, plans, and streams an initial table snapshot.
package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tgross/pgmigrate/internal/state"
	"github.com/tgross/pgmigrate/internal/tuning"
)

// SelectTable is called for every ordinary or partitioned source table.
type SelectTable func(schema, table string) bool

type Column struct {
	Name, TypeName, TypeSchema string
	TypeOID                    uint32
	BuiltIn                    bool
	Generated                  bool
}

type Table struct {
	OID                   uint32
	Schema, Name, RelKind string
	EstimatedRows, Bytes  int64
	// HeapBlocks is the physical main-fork size observed after importing the
	// exported snapshot. RelPages is retained only for source compatibility and
	// is never used for planning.
	HeapBlocks, RelPages int64
	Columns              []Column
	IntegerKey           string
	KeyMin, KeyMax       int64
	HasKeyBounds         bool
	Empty                bool
}

func (t Table) Identifier() string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

type Format string

const (
	Binary Format = "binary"
	Text   Format = "text"
)

// Part is an independently retryable source selection.
type Part struct {
	Table                Table
	ID                   string
	Predicate            string
	Args                 []any
	RangeStart, RangeEnd string
	EndInclusive         bool
	EstimatedBytes       int64
	Unsplit              bool
	Format               Format
}

// QuoteIdentifier uses pgx's PostgreSQL identifier quoting.
func QuoteIdentifier(parts ...string) string { return pgx.Identifier(parts).Sanitize() }

type inventoryQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Inventory reads the current transaction's selected table metadata. Use
// InventorySnapshot when the result will be used to plan snapshot COPY ranges.
func Inventory(ctx context.Context, db *pgx.Conn, selected SelectTable) ([]Table, error) {
	return inventory(ctx, db, selected)
}

// InventorySnapshot imports snapshot before issuing any catalog or data query,
// then reads table metadata, PK bounds, and relpages from that same snapshot.
func InventorySnapshot(ctx context.Context, connect func(context.Context) (*pgx.Conn, error), snapshot string, selected SelectTable) ([]Table, error) {
	if connect == nil || snapshot == "" {
		return nil, errors.New("source connector and snapshot are required")
	}
	conn, err := connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// This must remain the first statement in the transaction.
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(snapshot)); err != nil {
		return nil, fmt.Errorf("import inventory snapshot: %w", err)
	}
	tables, err := inventory(ctx, tx, selected)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit inventory snapshot: %w", err)
	}
	return tables, nil
}

func inventory(ctx context.Context, db inventoryQuerier, selected SelectTable) ([]Table, error) {
	rows, err := db.Query(ctx, `
		SELECT c.oid, n.nspname, c.relname, c.relkind::text,
		       greatest(c.reltuples::bigint, 0), pg_total_relation_size(c.oid),
		       CASE WHEN c.relkind='r' THEN
		         (pg_relation_size(c.oid) + current_setting('block_size')::bigint - 1)
		         / current_setting('block_size')::bigint
		       ELSE 0 END
		FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')
		  AND n.nspname !~ '^pg_toast' AND NOT c.relispartition
		ORDER BY pg_total_relation_size(c.oid) DESC, c.oid`)
	if err != nil {
		return nil, fmt.Errorf("inventory tables: %w", err)
	}
	defer rows.Close()
	var tables []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.OID, &t.Schema, &t.Name, &t.RelKind, &t.EstimatedRows, &t.Bytes, &t.HeapBlocks); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		if selected == nil || selected(t.Schema, t.Name) {
			tables = append(tables, t)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables: %w", err)
	}
	for i := range tables {
		columns, err := columns(ctx, db, tables[i].OID)
		if err != nil {
			return nil, err
		}
		tables[i].Columns = columns
		key, min, max, hasBounds, empty, err := integerPrimaryKey(ctx, db, tables[i])
		if err != nil {
			return nil, err
		}
		if key != "" {
			tables[i].IntegerKey, tables[i].KeyMin, tables[i].KeyMax = key, min, max
			tables[i].HasKeyBounds, tables[i].Empty = hasBounds, empty
		}
	}
	return tables, nil
}

func columns(ctx context.Context, db inventoryQuerier, oid uint32) ([]Column, error) {
	rows, err := db.Query(ctx, `
		SELECT a.attname, a.atttypid, t.typname, n.nspname, n.nspname='pg_catalog',
		       a.attgenerated<>''
		FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
		JOIN pg_namespace n ON n.oid=t.typnamespace
		WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped
		ORDER BY a.attnum`, oid)
	if err != nil {
		return nil, fmt.Errorf("inventory columns for %d: %w", oid, err)
	}
	defer rows.Close()
	var result []Column
	for rows.Next() {
		var c Column
		if err := rows.Scan(&c.Name, &c.TypeOID, &c.TypeName, &c.TypeSchema, &c.BuiltIn, &c.Generated); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

func integerPrimaryKey(ctx context.Context, db inventoryQuerier, t Table) (string, int64, int64, bool, bool, error) {
	var key string
	err := db.QueryRow(ctx, `
		SELECT a.attname FROM pg_index i
		JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0]
		JOIN pg_type ty ON ty.oid=a.atttypid
		WHERE i.indrelid=$1 AND i.indisprimary AND i.indnkeyatts=1
		  AND ty.typname IN ('int2','int4','int8')`, t.OID).Scan(&key)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, 0, false, false, nil
	}
	if err != nil {
		return "", 0, 0, false, false, fmt.Errorf("inspect primary key for %s: %w", t.Identifier(), err)
	}
	var min, max *int64
	q := fmt.Sprintf("SELECT min(%s)::bigint,max(%s)::bigint FROM %s", QuoteIdentifier(key), QuoteIdentifier(key), t.Identifier())
	if err := db.QueryRow(ctx, q).Scan(&min, &max); err != nil {
		return "", 0, 0, false, false, err
	}
	if min == nil {
		return key, 0, 0, false, true, nil
	}
	return key, *min, *max, true, false, nil
}

var binaryBuiltins = map[string]bool{
	"bool": true, "bytea": true, "int2": true, "int4": true, "int8": true,
	"float4": true, "float8": true, "text": true, "varchar": true, "bpchar": true,
	"date": true, "time": true, "timetz": true, "timestamp": true, "timestamptz": true,
	"interval": true, "uuid": true, "json": true, "jsonb": true, "numeric": true,
	"inet": true, "cidr": true, "macaddr": true, "bit": true, "varbit": true,
}

// ConservativeFormat uses binary only for equal majors and allowlisted built-ins.
func ConservativeFormat(t Table, sourceMajor, targetMajor int) Format {
	if sourceMajor != targetMajor {
		return Text
	}
	for _, c := range t.Columns {
		if c.Generated {
			continue
		}
		if !c.BuiltIn || !binaryBuiltins[c.TypeName] {
			return Text
		}
	}
	return Binary
}

// Plan creates at most cap parts. Small tables are unsplit; large tables use an
// integer primary key when available, otherwise physical source block ranges.
func Plan(t Table, desiredBytes int64, cap int, format Format) []Part {
	if cap < 1 {
		cap = 1
	}
	if cap == 1 || desiredBytes <= 0 || t.Bytes <= desiredBytes || t.Empty {
		return []Part{{Table: t, ID: "all", EstimatedBytes: t.Bytes, Unsplit: true, Format: format}}
	}
	count64 := int64(1)
	if t.Bytes > 0 {
		count64 = (t.Bytes-1)/desiredBytes + 1
	}
	if count64 > int64(cap) {
		count64 = int64(cap)
	}
	count := int(count64)
	if count > cap {
		count = cap
	}
	if count < 2 {
		count = 2
	}
	result := make([]Part, 0, count)
	hasBounds := t.HasKeyBounds || (t.IntegerKey != "" && (t.KeyMin != 0 || t.KeyMax != 0))
	if t.IntegerKey != "" && hasBounds && t.KeyMax >= t.KeyMin {
		min := big.NewInt(t.KeyMin)
		max := big.NewInt(t.KeyMax)
		span := new(big.Int).Sub(max, min)
		span.Add(span, big.NewInt(1))
		divisor := big.NewInt(int64(count))
		width := new(big.Int).Add(span, new(big.Int).Sub(divisor, big.NewInt(1)))
		width.Div(width, divisor)
		for i := 0; i < count; i++ {
			startBig := new(big.Int).Add(min, new(big.Int).Mul(width, big.NewInt(int64(i))))
			if startBig.Cmp(max) > 0 {
				break
			}
			start := startBig.Int64()
			nextBig := new(big.Int).Add(startBig, width)
			final := i == count-1 || nextBig.Cmp(max) > 0
			part := Part{Table: t, ID: fmt.Sprintf("pk-%06d", i), RangeStart: strconv.FormatInt(start, 10), EstimatedBytes: t.Bytes / int64(count), Format: format}
			if final {
				part.Predicate = fmt.Sprintf("%s >= %d AND %s <= %d", QuoteIdentifier(t.IntegerKey), start, QuoteIdentifier(t.IntegerKey), t.KeyMax)
				part.RangeEnd = strconv.FormatInt(t.KeyMax, 10)
				part.EndInclusive = true
			} else {
				end := nextBig.Int64()
				part.Predicate = fmt.Sprintf("%s >= %d AND %s < %d", QuoteIdentifier(t.IntegerKey), start, QuoteIdentifier(t.IntegerKey), end)
				part.RangeEnd = strconv.FormatInt(end, 10)
			}
			result = append(result, part)
			if final {
				break
			}
		}
		return result
	}
	blocks := t.HeapBlocks
	if blocks < 2 {
		return []Part{{Table: t, ID: "all", EstimatedBytes: t.Bytes, Unsplit: true, Format: format}}
	}
	width := (blocks + int64(count) - 1) / int64(count)
	for start, i := int64(0), 0; start < blocks && i < count; start, i = start+width, i+1 {
		end := start + width
		if end > blocks {
			end = blocks
		}
		predicate := fmt.Sprintf("ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid", start, end)
		result = append(result, Part{Table: t, ID: fmt.Sprintf("ctid-%06d", i), Predicate: predicate, RangeStart: strconv.FormatInt(start, 10), RangeEnd: strconv.FormatInt(end, 10), EstimatedBytes: t.Bytes / int64(count), Format: format})
	}
	return result
}

// State is the durable subset needed by Runner.
type State interface {
	UpsertTable(context.Context, state.Table) error
	UpsertPart(context.Context, state.Part) error
	PartCompleted(context.Context, uint32, string) (bool, error)
	CompletePart(context.Context, uint32, string, int64, int64, time.Duration) error
	CompleteTable(context.Context, uint32) error
}

type Runner struct {
	Source, Target func(context.Context) (*pgx.Conn, error)
	Snapshot       string
	Workers        int
	State          State
	MaxAttempts    int
	RetryBackoff   func(attempt int) time.Duration
	// TargetSessionGUCs are settings applied to every target session used to
	// write a part. Empty leaves the target's own defaults in place.
	TargetSessionGUCs map[string]string
}

// applyTargetSessionGUCs configures one target session for bulk loading.
func (r Runner) applyTargetSessionGUCs(ctx context.Context, conn *pgx.Conn) error {
	if len(r.TargetSessionGUCs) == 0 {
		return nil
	}
	names := make([]string, 0, len(r.TargetSessionGUCs))
	for name := range r.TargetSessionGUCs {
		names = append(names, name)
	}
	// Sorted so that a refusal is reproducible rather than depending on map order.
	sort.Strings(names)
	for _, name := range names {
		if err := tuning.SetSession(ctx, conn, name, r.TargetSessionGUCs[name]); err != nil {
			return err
		}
	}
	return nil
}

// Run copies largest parts first and records completion only after target commit.
func (r Runner) Run(ctx context.Context, parts []Part) error {
	if r.Source == nil || r.Target == nil || r.State == nil || r.Snapshot == "" {
		return errors.New("source, target, state, and snapshot are required")
	}
	if r.Workers < 1 {
		r.Workers = 1
	}
	if err := r.ensureTargetState(ctx); err != nil {
		return err
	}
	byTable := map[uint32][]Part{}
	for _, p := range parts {
		byTable[p.Table.OID] = append(byTable[p.Table.OID], p)
	}
	for _, ps := range byTable {
		t := ps[0].Table
		if err := r.State.UpsertTable(ctx, state.Table{OID: t.OID, Schema: t.Schema, Name: t.Name, EstimatedRows: t.EstimatedRows, Bytes: t.Bytes, PartsTotal: int64(len(ps))}); err != nil {
			return err
		}
		for _, p := range ps {
			if err := r.State.UpsertPart(ctx, state.Part{TableOID: t.OID, ID: p.ID, RangeStart: p.RangeStart, RangeEnd: p.RangeEnd}); err != nil {
				return err
			}
		}
		if len(ps) > 1 {
			if err := r.prepareSplitTable(ctx, t); err != nil {
				return err
			}
		}
	}
	parts = LargestFirst(parts)
	jobs := make(chan Part)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for range r.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				done, err := r.State.PartCompleted(runCtx, p.Table.OID, p.ID)
				if err == nil && !done {
					err = r.copyPartRetry(runCtx, p)
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					err = fmt.Errorf("copy part %s of %s (%d bytes estimated): %w",
						p.ID, p.Table.Identifier(), p.EstimatedBytes, err)
				}
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
						cancel()
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
send:
	for _, p := range parts {
		select {
		case jobs <- p:
		case <-runCtx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	for oid, ps := range byTable {
		all := true
		for _, p := range ps {
			done, err := r.State.PartCompleted(ctx, oid, p.ID)
			if err != nil {
				return err
			}
			all = all && done
		}
		if all {
			if err := r.State.CompleteTable(ctx, oid); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r Runner) copyPartRetry(ctx context.Context, p Part) error {
	attempts := r.MaxAttempts
	if attempts <= 0 {
		attempts = 5
	}
	if attempts > 5 {
		attempts = 5
	}
	backoff := r.RetryBackoff
	if backoff == nil {
		backoff = func(attempt int) time.Duration {
			return time.Duration(1<<min(attempt-1, 5)) * 100 * time.Millisecond
		}
	}
	return retryConnections(ctx, attempts, backoff, func() error { return r.copyPart(ctx, p) })
}

func retryConnections(ctx context.Context, attempts int, backoff func(int) time.Duration, operation func() error) error {
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = operation()
		if err == nil || !retryableConnectionError(err) || attempt == attempts {
			return err
		}
		timer := time.NewTimer(backoff(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func retryableConnectionError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08")
	}
	return pgconn.SafeToRetry(err)
}

// LargestFirst returns a stable, independently owned worker schedule.
func LargestFirst(parts []Part) []Part {
	result := append([]Part(nil), parts...)
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].EstimatedBytes > result[j].EstimatedBytes
	})
	return result
}

func (r Runner) copyPart(ctx context.Context, p Part) error {
	start := time.Now()
	done, rows, bytes, err := r.targetPartCompleted(ctx, p)
	if err != nil {
		return err
	}
	if done {
		return r.State.CompletePart(ctx, p.Table.OID, p.ID, rows, bytes, 0)
	}
	source, err := r.Source(ctx)
	if err != nil {
		return err
	}
	defer source.Close(context.Background())
	target, err := r.Target(ctx)
	if err != nil {
		return err
	}
	defer target.Close(context.Background())
	// Applied to the session rather than the transaction, and before it begins,
	// so that settings consulted at commit time take effect. This sits outside
	// the format check below because binary COPY needs the tuning just as much
	// and only needs no encoding GUCs pinned.
	if err := r.applyTargetSessionGUCs(ctx, target); err != nil {
		return err
	}
	stx, err := source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer stx.Rollback(ctx)
	// This must remain the first statement in the source transaction.
	if _, err := stx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(r.Snapshot)); err != nil {
		return fmt.Errorf("import snapshot: %w", err)
	}
	if p.Format == Text {
		if err := pinTextCopyGUCs(ctx, stx); err != nil {
			return fmt.Errorf("pin source text COPY settings: %w", err)
		}
	}
	ttx, err := target.Begin(ctx)
	if err != nil {
		return err
	}
	defer ttx.Rollback(ctx)
	if p.Format == Text {
		if err := pinTextCopyGUCs(ctx, ttx); err != nil {
			return fmt.Errorf("pin target text COPY settings: %w", err)
		}
	}
	table := p.Table.Identifier()
	columns := columnList(p.Table.Columns)
	if columns == "" {
		return r.copyGeneratedOnlyPart(ctx, start, p, stx, ttx, table)
	}
	// Every part copies straight into the table it names. A part used to be
	// staged in a session-local temp table and drained with INSERT ... SELECT,
	// which bought nothing -- the rows and the completion marker below share one
	// target transaction, so atomicity comes from the transaction and not from
	// staging -- while writing every row twice and holding the whole part in
	// local buffers, where a large part exhausted temp_buffers and failed the
	// copy with "no empty local buffer available".
	freeze := ""
	if p.Unsplit {
		// The whole table is this part, so truncating makes a repeated attempt
		// idempotent.
		if _, err := ttx.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncate target table: %w", err)
		}
		if p.Table.RelKind == "r" {
			// COPY FREEZE requires a relation created or truncated in the same
			// transaction, and is rejected outright on a partitioned table.
			freeze = ", FREEZE"
		}
	} else if err := clearCopiedRange(ctx, ttx, p, table); err != nil {
		return err
	}
	format := string(p.Format)
	where := ""
	if p.Predicate != "" {
		where = " WHERE " + p.Predicate
	}
	sourceSQL := fmt.Sprintf("COPY (SELECT %s FROM %s%s) TO STDOUT (FORMAT %s)", columns, table, where, format)
	targetSQL := fmt.Sprintf("COPY %s (%s) FROM STDIN (FORMAT %s%s)", table, columns, format, freeze)
	pr, pw := io.Pipe()
	type result struct {
		tagRows int64
		bytes   int64
		err     error
	}
	ch := make(chan result, 1)
	go func() {
		counter := &countingWriter{Writer: pw}
		tag, err := stx.Conn().PgConn().CopyTo(ctx, counter, sourceSQL)
		_ = pw.CloseWithError(err)
		ch <- result{tag.RowsAffected(), counter.n, err}
	}()
	targetTag, targetErr := ttx.Conn().PgConn().CopyFrom(ctx, pr, targetSQL)
	_ = pr.CloseWithError(targetErr)
	src := <-ch
	if src.err != nil || targetErr != nil {
		if src.err != nil {
			src.err = fmt.Errorf("copy out of source: %w", src.err)
		}
		if targetErr != nil {
			targetErr = fmt.Errorf("copy into target: %w", targetErr)
		}
		return errors.Join(src.err, targetErr)
	}
	if _, err := ttx.Exec(ctx, `
		INSERT INTO pgmigrate_internal.copy_parts(table_oid, part_id, rows_copied, bytes_copied)
		VALUES ($1,$2,$3,$4) ON CONFLICT (table_oid,part_id) DO NOTHING`,
		p.Table.OID, p.ID, targetTag.RowsAffected(), src.bytes); err != nil {
		return fmt.Errorf("record target copy completion: %w", err)
	}
	if err := ttx.Commit(ctx); err != nil {
		return fmt.Errorf("commit copied part: %w", err)
	}
	if err := stx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source snapshot: %w", err)
	}
	copiedRows := targetTag.RowsAffected()
	if err := r.State.CompletePart(ctx, p.Table.OID, p.ID, copiedRows, src.bytes, time.Since(start)); err != nil {
		return err
	}
	return nil
}

// clearCopiedRange removes whatever an earlier attempt at this part may have
// left behind, so that copying into the real table stays idempotent. It has to
// run before the copy, not after it, or it would delete the rows the part just
// wrote. Parts split by source block have no key to delete by and rely on the
// target transaction alone, exactly as they did when they were staged.
func clearCopiedRange(ctx context.Context, ttx pgx.Tx, p Part, table string) error {
	if p.Table.IntegerKey == "" || p.RangeStart == "" || p.RangeEnd == "" {
		return nil
	}
	operator := "<"
	if p.EndInclusive {
		operator = "<="
	}
	key := QuoteIdentifier(p.Table.IntegerKey)
	cleanup := fmt.Sprintf("DELETE FROM %s WHERE %s >= %s AND %s %s %s",
		table, key, p.RangeStart, key, operator, p.RangeEnd)
	if _, err := ttx.Exec(ctx, cleanup); err != nil {
		return fmt.Errorf("clean copied range: %w", err)
	}
	return nil
}

func (r Runner) copyGeneratedOnlyPart(
	ctx context.Context,
	start time.Time,
	p Part,
	stx, ttx pgx.Tx,
	table string,
) error {
	where := ""
	if p.Predicate != "" {
		where = " WHERE " + p.Predicate
	}
	var rows int64
	if err := stx.QueryRow(ctx, "SELECT count(*) FROM "+table+where).Scan(&rows); err != nil {
		return fmt.Errorf("count generated-only source rows: %w", err)
	}
	if p.Unsplit {
		if _, err := ttx.Exec(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncate generated-only table: %w", err)
		}
	}
	const batchSize int64 = 256
	for remaining := rows; remaining > 0; {
		count := min(remaining, batchSize)
		batch := &pgx.Batch{}
		for range count {
			batch.Queue("INSERT INTO " + table + " DEFAULT VALUES")
		}
		results := ttx.SendBatch(ctx, batch)
		if err := results.Close(); err != nil {
			return fmt.Errorf("insert generated-only rows: %w", err)
		}
		remaining -= count
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if _, err := ttx.Exec(ctx, `
		INSERT INTO pgmigrate_internal.copy_parts(table_oid, part_id, rows_copied, bytes_copied)
		VALUES ($1,$2,$3,0) ON CONFLICT (table_oid,part_id) DO NOTHING`,
		p.Table.OID, p.ID, rows); err != nil {
		return fmt.Errorf("record generated-only target completion: %w", err)
	}
	if err := ttx.Commit(ctx); err != nil {
		return fmt.Errorf("commit generated-only target rows: %w", err)
	}
	if err := stx.Commit(ctx); err != nil {
		return fmt.Errorf("commit generated-only source snapshot: %w", err)
	}
	return r.State.CompletePart(ctx, p.Table.OID, p.ID, rows, 0, time.Since(start))
}

func pinTextCopyGUCs(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `
		SELECT set_config('DateStyle','ISO, YMD',true),
		       set_config('IntervalStyle','postgres',true),
		       set_config('TimeZone','UTC',true),
		       set_config('extra_float_digits','3',true),
		       set_config('bytea_output','hex',true),
		       set_config('lc_numeric','C',true),
		       set_config('lc_monetary','C',true),
		       set_config('lc_time','C',true),
		       set_config('client_encoding','UTF8',true)`)
	return err
}

func (r Runner) ensureTargetState(ctx context.Context) error {
	conn, err := r.Target(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, `
		CREATE SCHEMA IF NOT EXISTS pgmigrate_internal;
		CREATE TABLE IF NOT EXISTS pgmigrate_internal.copy_tables (
			table_oid oid PRIMARY KEY,
			prepared_at timestamptz NOT NULL DEFAULT clock_timestamp()
		);
		CREATE TABLE IF NOT EXISTS pgmigrate_internal.copy_parts (
			table_oid oid NOT NULL,
			part_id text NOT NULL,
			rows_copied bigint NOT NULL,
			bytes_copied bigint NOT NULL,
			completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			PRIMARY KEY(table_oid,part_id)
		)`)
	if err != nil {
		return fmt.Errorf("ensure target copy state: %w", err)
	}
	return nil
}

func (r Runner) prepareSplitTable(ctx context.Context, table Table) error {
	conn, err := r.Target(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		INSERT INTO pgmigrate_internal.copy_tables(table_oid) VALUES($1)
		ON CONFLICT(table_oid) DO NOTHING`, table.OID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+table.Identifier()); err != nil {
			return fmt.Errorf("truncate %s: %w", table.Identifier(), err)
		}
	}
	return tx.Commit(ctx)
}

func (r Runner) targetPartCompleted(ctx context.Context, p Part) (bool, int64, int64, error) {
	conn, err := r.Target(ctx)
	if err != nil {
		return false, 0, 0, err
	}
	defer conn.Close(context.Background())
	var rows, bytes int64
	err = conn.QueryRow(ctx, `
		SELECT rows_copied,bytes_copied FROM pgmigrate_internal.copy_parts
		WHERE table_oid=$1 AND part_id=$2`, p.Table.OID, p.ID).Scan(&rows, &bytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, 0, nil
	}
	if err != nil {
		return false, 0, 0, err
	}
	return true, rows, bytes, nil
}

type countingWriter struct {
	io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.n += int64(n)
	return n, err
}

func columnList(columns []Column) string {
	values := make([]string, 0, len(columns))
	for _, c := range columns {
		if !c.Generated {
			values = append(values, QuoteIdentifier(c.Name))
		}
	}
	return strings.Join(values, ",")
}
func quoteLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }
