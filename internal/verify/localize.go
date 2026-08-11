package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// RowDiff is one row two sides disagree about, named by its key.
type RowDiff struct {
	Key  []string `json:"key"`
	Kind DiffKind `json:"kind"`
}

// DiffKind says which way a row disagrees, which is what points at the cause: a
// missing row is an apply that did not happen, a differing row is an apply that
// happened wrongly, and an extra row is a delete that did not.
type DiffKind string

const (
	DiffSourceOnly DiffKind = "source_only"
	DiffTargetOnly DiffKind = "target_only"
	DiffDifferent  DiffKind = "different"
)

// rowEntry is one row reduced to its key and a hash of its contents.
type rowEntry struct {
	key  []string
	hash int64
}

// rowSet holds rows by a rendering of their key tuple.
//
// Keys are compared as exact text, never ordered. Ordering text would mean
// choosing a collation, and Go's byte order is not any server's collation order,
// so a comparison that sorted would pair the wrong rows and invent differences.
type rowSet map[string]rowEntry

func identity(key []string) string {
	rendered, err := json.Marshal(key)
	if err != nil {
		return strings.Join(key, "\x1f")
	}
	return string(rendered)
}

// keyProjection is the key columns as text followed by a hash of the whole row,
// which is what both sides return for every row they are asked about.
func keyProjection(table Table) []string {
	projected := make([]string, 0, len(table.Key.Columns)+1)
	for _, column := range table.Key.Columns {
		projected = append(projected, "t."+QuoteIdentifier(column.Name)+"::text")
	}
	return append(projected,
		fmt.Sprintf("pg_catalog.hashtextextended(%s,%d)", renderedRow, hashSeed))
}

// sampleWindow reads the key and row hash of the rows in one page interval.
//
// The hash is kept per row rather than folded into an aggregate. That is what lets
// a disagreement be named straight away: the read that found the row already knows
// which row it was, so nothing has to go back to the heap to work it out.
func sampleWindow(
	ctx context.Context, db querier, table Table, chunk pageChunk,
) (rowSet, int64, error) {
	found := make(rowSet, chunk.Limit)
	if err := scanRows(ctx, db, sampleQuery(table, chunk), table, found, nil); err != nil {
		return nil, 0, fmt.Errorf("sample %s: %w", chunk, err)
	}
	return found, int64(len(found)), nil
}

// sampleQuery is one window's read. It has to plan as a Tid Range Scan under a
// Limit: a ctid predicate the planner did not recognise would be a sequential scan
// of the whole heap per window, which is the entire cost the sample is avoiding and
// would be invisible in a correctness test.
func sampleQuery(table Table, chunk pageChunk) string {
	query := fmt.Sprintf("SELECT %s FROM %s AS t%s",
		strings.Join(keyProjection(table), ","),
		chunk.relation.identifier(), chunk.where())
	if chunk.Limit > 0 {
		// Inlined for the same reason the page bounds are, and with the same
		// justification: it is an int64 this package computed.
		query += fmt.Sprintf(" LIMIT %d", chunk.Limit)
	}
	return query
}

// keyBatch is how many keys one lookup statement may carry.
//
// A batch binds one parameter per key column per key, and the wire protocol allows
// 65535 parameters in a statement, so a wide key has to reduce the batch or the
// statement is rejected outright. Batching at all is only about statement size; it
// is not a bound on correctness.
func keyBatch(requested int64, columns int) int64 {
	if requested <= 0 {
		requested = defaultBatchRows
	}
	if columns <= 0 {
		return requested
	}
	return max(min(requested, 65535/int64(columns)), 1)
}

// readRows reads the current key and row hash for named keys, in batches.
func readRows(
	ctx context.Context, db querier, table Table, keys [][]string, batch int64,
) (rowSet, error) {
	found := make(rowSet, len(keys))
	if !table.Key.present() || len(keys) == 0 {
		return found, nil
	}
	size := int(keyBatch(batch, len(table.Key.Columns)))
	for start := 0; start < len(keys); start += size {
		part, err := readRowBatch(ctx, db, table, keys[start:min(start+size, len(keys))])
		if err != nil {
			return nil, err
		}
		maps.Copy(found, part)
	}
	return found, nil
}

// readRowBatch reads one batch of keys by key equality.
//
// The keys are joined against rather than listed in an IN, so one statement reads
// all of them through the key's own index whatever the key's width.
func readRowBatch(ctx context.Context, db querier, table Table, keys [][]string) (rowSet, error) {
	found := make(rowSet, len(keys))
	columns := table.Key.Columns
	projected := keyProjection(table)
	var (
		tuples []string
		args   []any
		names  []string
	)
	for _, column := range columns {
		names = append(names, QuoteIdentifier(column.Name))
	}
	for row, key := range keys {
		placeholders := make([]string, 0, len(columns))
		for column := range columns {
			args = append(args, key[column])
			// The first tuple carries the casts, which is enough for the whole
			// VALUES list, and casting to the column's own type is what keeps the
			// join an index lookup rather than a comparison of text.
			if row == 0 {
				placeholders = append(placeholders,
					fmt.Sprintf("$%d::%s", len(args), columns[column].Type))
				continue
			}
			placeholders = append(placeholders, fmt.Sprintf("$%d", len(args)))
		}
		tuples = append(tuples, "("+strings.Join(placeholders, ",")+")")
	}
	conditions := make([]string, 0, len(columns))
	for i, name := range names {
		conditions = append(conditions, fmt.Sprintf("t.%s = k.c%d", name, i))
	}
	columnNames := make([]string, 0, len(columns))
	for i := range columns {
		columnNames = append(columnNames, fmt.Sprintf("c%d", i))
	}
	query := fmt.Sprintf("SELECT %s FROM (VALUES %s) AS k(%s) JOIN %s AS t ON %s",
		strings.Join(projected, ","), strings.Join(tuples, ","),
		strings.Join(columnNames, ","), table.Identifier(), strings.Join(conditions, " AND "))
	if err := scanRows(ctx, db, query, table, found, args); err != nil {
		return nil, fmt.Errorf("re-read rows of %s: %w", table.Identifier(), err)
	}
	return found, nil
}

// scanRows reads key columns followed by a row hash into a set.
func scanRows(ctx context.Context, db querier, query string, table Table, into rowSet, args []any) error {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		key := make([]*string, len(table.Key.Columns))
		var hash int64
		dest := make([]any, 0, len(key)+1)
		for i := range key {
			dest = append(dest, &key[i])
		}
		dest = append(dest, &hash)
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		rendered := make([]string, len(key))
		for i, value := range key {
			if value != nil {
				rendered[i] = *value
			}
		}
		into[identity(rendered)] = rowEntry{key: rendered, hash: hash}
	}
	return rows.Err()
}

// compareRows names the rows two sets disagree about.
//
// Sampling cannot produce DiffTargetOnly: the target is only ever asked about keys
// the source supplied, so a row only the target holds is never in either set. That
// is the blind spot the design accepts. A recheck can produce it, because there the
// two sides are asked about the same keys and the source row may have gone.
func compareRows(source, target rowSet) []RowDiff {
	var diffs []RowDiff
	for key, entry := range source {
		other, present := target[key]
		switch {
		case !present:
			diffs = append(diffs, RowDiff{Key: entry.key, Kind: DiffSourceOnly})
		case other.hash != entry.hash:
			diffs = append(diffs, RowDiff{Key: entry.key, Kind: DiffDifferent})
		}
	}
	for key, entry := range target {
		if _, present := source[key]; !present {
			diffs = append(diffs, RowDiff{Key: entry.key, Kind: DiffTargetOnly})
		}
	}
	slices.SortFunc(diffs, func(a, b RowDiff) int {
		if order := slices.Compare(a.Key, b.Key); order != 0 {
			return order
		}
		return strings.Compare(string(a.Kind), string(b.Kind))
	})
	return diffs
}

// compareWindows samples the source window by window, checking each window
// against the target before moving on.
//
// Streaming rather than collecting is what bounds the memory: a million keys and
// their hashes held at once is hundreds of megabytes per table, and nothing needs
// the whole sample at once. A window's rows are compared and thrown away, and only
// the disagreements are kept.
func (w *worker) compareWindows(
	ctx context.Context, table Table, windows []pageChunk, out *TableResult,
) ([]RowDiff, error) {
	name := table.Schema + "." + table.Name
	out.Source.Windows = len(windows)
	out.Source.Relations = countRelations(windows)
	// Pacing runs against the pages the windows cover, not the pages the table
	// has. The two differ by the sampling ratio, and an estimate built on the
	// second would be wrong by that whole factor.
	planned := plannedPages(windows)
	batch := keyBatch(w.cfg.BatchRows, len(table.Key.Columns))
	w.report(Progress{
		Table: name, Side: sideSource, Stage: StageSampling,
		PagesTotal: out.Source.PagesTotal, Estimated: out.Source.Estimated,
	})
	var candidates []RowDiff
	read := int64(0)
	for _, chunk := range windows {
		began := time.Now()
		sourceRows, rows, err := sampleWindow(ctx, w.source, table, chunk)
		if err != nil {
			return nil, err
		}
		pages := chunk.pages()
		w.rate.observe(sideSource, rows, pages, time.Since(began))
		read += pages
		out.Source.Pages += pages
		out.Source.Rows += rows

		keys := make([][]string, 0, len(sourceRows))
		for _, entry := range sourceRows {
			keys = append(keys, entry.key)
		}
		targetRows, err := readRows(ctx, w.target, table, keys, batch)
		if err != nil {
			return nil, err
		}
		out.Target.Batches += int((int64(len(keys)) + batch - 1) / batch)
		out.Target.Keys += int64(len(keys))
		out.Target.Rows += int64(len(targetRows))
		candidates = append(candidates, compareRows(sourceRows, targetRows)...)

		w.report(Progress{
			Table: name, Side: sideSource, Stage: StageSampling,
			Pages: out.Source.Pages, PagesTotal: out.Source.PagesTotal,
			Rows: out.Source.Rows, Estimated: out.Source.Estimated,
			TargetRows: out.Target.Rows,
			Rate:       w.rate.rows(sideSource), ETA: w.rate.eta(sideSource, planned-read),
			Candidates: len(candidates), Coverage: sampledCoverage(out.Source),
		})
		if err := w.throttle(ctx, time.Since(began)); err != nil {
			return nil, err
		}
	}
	return candidates, nil
}

// resolve decides which of the candidate rows really disagree.
//
// A row read from a live source and a target that is still applying is expected
// to differ: the change is in flight. Waiting the difference out is not the answer
// either, because a row written to constantly would never settle. What settles it
// is fixing a position instead of a moment. The source rows are read, a decodable
// marker names a WAL position at or after that read, and the target rows are read
// once apply has passed it. A row that still differs then is not in flight: the
// target has seen everything the source had when it was read.
//
// The window between the read and the marker is milliseconds, so a row written to
// within it can still differ, which is what the retries are for. The budget is a
// deadline rather than a count of attempts because what is being waited out is
// replication latency, which is measured in time: a run of retries against a
// second of lag says nothing, and one retry against a settled target says
// everything. A row still differing when the budget runs out is reported, which is
// the right answer for a row that really is corrupt and the honest one for a row
// too hot to ever pin.
// The count returned alongside is how many rows disagreed on the first look. Its
// difference from the rows still disagreeing at the end is the replication
// latency the loop waited out, which is worth reporting on its own: a stratum
// whose rows all settle is watching a healthy applier, and one whose rows never
// do is watching a broken one.
func (w *worker) resolve(ctx context.Context, table Table, keys [][]string) ([]RowDiff, int, error) {
	batch := keyBatch(w.cfg.BatchRows, len(table.Key.Columns))
	budget := w.cfg.ConvergeTimeout
	if w.cfg.Boundary == nil || w.cfg.WaitApplied == nil {
		// Nothing is applying, so there is nothing to wait for and no position to
		// wait at: whatever differs now differs. One pass, no budget.
		budget = 0
	}
	deadline := time.Now().Add(budget)
	var (
		diffs []RowDiff
		first = -1
	)
	for {
		if len(keys) == 0 {
			return nil, max(first, 0), nil
		}
		source, err := readRows(ctx, w.source, table, keys, batch)
		if err != nil {
			return nil, 0, err
		}
		if w.cfg.Boundary != nil && w.cfg.WaitApplied != nil {
			// The marker is emitted after the read, so every row the read saw was
			// committed at or before the position it reports.
			position, err := w.cfg.Boundary(ctx, w.source)
			if err != nil {
				return nil, 0, fmt.Errorf("mark the source position for a recheck of %s: %w",
					table.Identifier(), err)
			}
			if err := w.cfg.WaitApplied(ctx, position); err != nil {
				return nil, 0, fmt.Errorf("wait for apply to reach %s while rechecking %s: %w",
					position, table.Identifier(), err)
			}
		}
		target, err := readRows(ctx, w.target, table, keys, batch)
		if err != nil {
			return nil, 0, err
		}
		diffs = compareRows(source, target)
		if first < 0 {
			first = len(diffs)
		}
		if len(diffs) == 0 || time.Now().After(deadline) {
			return diffs, first, nil
		}
		keys = keys[:0]
		for _, diff := range diffs {
			keys = append(keys, diff.Key)
		}
	}
}

// candidateKeys bounds how many rows one table reports and rechecks. A table that
// disagrees about millions of rows is answered by the first thousand of them; the
// count is what an operator acts on, and reading a million rows by key to print
// them would cost more than the pass that found them.
func candidateKeys(diffs []RowDiff, limit int64) [][]string {
	if limit > 0 && int64(len(diffs)) > limit {
		diffs = diffs[:limit]
	}
	keys := make([][]string, 0, len(diffs))
	for _, diff := range diffs {
		keys = append(keys, diff.Key)
	}
	return keys
}

// Boundary emits a decodable marker on the source and returns its WAL position.
// It is how a recheck names a position for apply to reach.
type Boundary func(context.Context, *pgx.Conn) (string, error)

// WaitApplied blocks until the target has applied at least the supplied source
// WAL position.
type WaitApplied func(context.Context, string) error
