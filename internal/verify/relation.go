package verify

import (
	"context"
	"fmt"
	"math"
	"strings"
)

// relation is one heap a pass actually reads.
//
// A partitioned table is not one of these: `ctid` is per heap, so a page range
// only means something for a single relation, and a predicate on the parent
// would be pushed down to every leaf and select page N of each of them. Leaves
// are therefore enumerated and read individually. They are also enumerated per
// side rather than paired up, because the two servers agree about nothing here:
// OIDs differ, sizes differ, and a leaf may hold different rows on the two
// sides if the partition bounds were changed. Only the source is enumerated this
// way, because only the source is read by page: the sampled rows are found on the
// target through the key's own index, which reaches a leaf without anyone naming
// it.
type relation struct {
	Schema, Name string
	// Pages is the relation's current size in pages, which is what the sample is
	// planned and paced against. It comes from pg_relation_size rather than
	// relpages, so it is exact and does not depend on the table having been
	// analyzed.
	Pages int64
	// Rows is the planner's row estimate. It sizes the windows and is the
	// denominator of the sampled fraction, and it is only an estimate: on the
	// bloated table this design was measured against it was wrong by region by two
	// orders of magnitude, which is why a window is bounded by a row limit as well
	// as by its pages.
	Rows int64
}

func (r relation) identifier() string { return QuoteIdentifier(r.Schema, r.Name) }

// relationProjection is shared by both enumerations so the two cannot drift.
const relationProjection = `
	SELECT n.nspname, c.relname,
	       (pg_catalog.pg_relation_size(c.oid) /
	        current_setting('block_size')::bigint)::bigint,
	       greatest(c.reltuples, 0)::bigint`

const (
	selfRelationQuery = relationProjection + `
	FROM pg_catalog.pg_class c
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.oid = pg_catalog.to_regclass($1)`

	leafRelationQuery = relationProjection + `
	FROM pg_catalog.pg_partition_tree(pg_catalog.to_regclass($1)) AS tree
	JOIN pg_catalog.pg_class c ON c.oid = tree.relid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE tree.isleaf AND c.relkind = 'r'
	ORDER BY n.nspname, c.relname`
)

// relations reads the heaps one side holds for a table, with their sizes.
func relations(ctx context.Context, db querier, table Table) ([]relation, error) {
	query := selfRelationQuery
	if table.RelKind == "p" {
		query = leafRelationQuery
	}
	rows, err := db.Query(ctx, query, table.Identifier())
	if err != nil {
		return nil, fmt.Errorf("read the heaps of %s: %w", table.Identifier(), err)
	}
	defer rows.Close()
	var out []relation
	for rows.Next() {
		var value relation
		if err := rows.Scan(&value.Schema, &value.Name, &value.Pages, &value.Rows); err != nil {
			return nil, fmt.Errorf("scan a heap of %s: %w", table.Identifier(), err)
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate the heaps of %s: %w", table.Identifier(), err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s has no heap to read: it was dropped or is not a table", table.Identifier())
	}
	return out, nil
}

// pageChunk is one window of the sample: a half-open page interval of one heap.
type pageChunk struct {
	relation relation
	// Start is the first page read. End is one past the last, and zero means
	// "to the end of the relation as it stands when the chunk is read", which is
	// what the final chunk of every relation uses: the relation may have grown
	// since it was measured, and a row in a page beyond the measurement has to
	// fall inside some chunk.
	Start, End int64
	// Limit is the most rows this window may return, and it belongs to the window
	// rather than to the table because a partitioned table's budget is divided
	// among its leaves. One limit computed from the table's budget and applied to
	// every window of every leaf would sample the budget once per leaf.
	Limit int64
}

// where renders the chunk as a ctid range, with any further conditions the
// caller needs anded onto it.
//
// The page numbers are written into the SQL rather than bound as parameters.
// This is the one place in pgmigrate that inlines values, and it is deliberate:
// a Tid Range Scan is only chosen when the planner can recognise the bound at
// planning time, and the whole design rests on getting that node. The values are
// int64 page counters this package computed, never anything read from a user or a
// row, so there is nothing here to inject.
func (c pageChunk) where(extra ...string) string {
	conditions := []string{fmt.Sprintf("t.ctid >= '(%d,0)'::tid", c.Start)}
	if c.End > 0 {
		conditions = append(conditions, fmt.Sprintf("t.ctid < '(%d,0)'::tid", c.End))
	}
	return " WHERE " + strings.Join(append(conditions, extra...), " AND ")
}

// pages is how many pages the chunk covers, for pacing and progress. An open
// ended chunk counts as reaching the measured end of the relation.
func (c pageChunk) pages() int64 {
	if c.End <= 0 {
		return max(c.relation.Pages-c.Start, 1)
	}
	return c.End - c.Start
}

func (c pageChunk) String() string {
	if c.End <= 0 {
		return fmt.Sprintf("%s.%s:%d-", c.relation.Schema, c.relation.Name, c.Start)
	}
	return fmt.Sprintf("%s.%s:%d-%d", c.relation.Schema, c.relation.Name, c.Start, c.End)
}

// planSample picks the page intervals a table's check reads, apportioning the row
// budget across a partitioned table's leaves by their size.
func planSample(relations []relation, budgetRows, windows int64) []pageChunk {
	if windows <= 0 {
		windows = defaultSampleWindows
	}
	total := totalPages(relations)
	var chunks []pageChunk
	for _, rel := range relations {
		share := budgetRows
		if total > 0 {
			share = max(budgetRows*max(rel.Pages, 1)/total, 1)
		}
		chunks = append(chunks, planRelationSample(rel, share, windows)...)
	}
	return chunks
}

// planRelationSample spreads one heap's windows evenly across it.
//
// A relation small enough to read whole is read whole, with an open tail so rows
// appended while the pass runs are still covered. Otherwise the windows are
// placed on an even stride, each sized from the relation's row density with
// headroom, and the last one is moved to the end of the heap: that is where
// appended rows land, and where a replication fault appears first.
//
// Spreading them is the whole point. Row density on a bloated heap varied between
// 0.09 and 17.2 rows per page across regions of the same production table, so a
// sample drawn from one place is not a sample of the table.
func planRelationSample(rel relation, budgetRows, windows int64) []pageChunk {
	whole := []pageChunk{{relation: rel, Start: 0, Limit: budgetRows}}
	if budgetRows <= 0 || rel.Pages <= 0 || rel.Rows <= 0 || rel.Rows <= budgetRows {
		return whole
	}
	limit := rowsPerWindow(budgetRows, windows)
	density := float64(rel.Rows) / float64(rel.Pages)
	pagesPerWindow := max(int64(math.Ceil(sampleHeadroom*float64(limit)/density)), 1)
	stride := rel.Pages / windows
	if stride <= pagesPerWindow {
		return whole
	}
	chunks := make([]pageChunk, 0, windows)
	for window := int64(0); window < windows-1; window++ {
		start := window * stride
		chunks = append(chunks, pageChunk{
			relation: rel, Start: start, End: start + pagesPerWindow, Limit: limit,
		})
	}
	return append(chunks, pageChunk{
		relation: rel, Start: max(rel.Pages-pagesPerWindow, 0), Limit: limit,
	})
}

// rowsPerWindow is the LIMIT one window's read carries, so a dense region cannot
// spend the whole table's budget. A window is bounded twice, by its pages and by
// this: in a dense region the limit binds and the scan stops early, in a sparse
// one the page interval binds and the window yields less. Without it, "stop once
// you have a million rows" would return the first million rows of the heap and
// call it a sample.
func rowsPerWindow(budgetRows, windows int64) int64 {
	if windows <= 0 {
		return budgetRows
	}
	return max(budgetRows/windows, 1)
}

// plannedPages is how many pages the windows cover, which is what pacing and the
// estimate run against. It is not the size of the table: that is totalPages, and
// the difference between the two is the point of sampling.
func plannedPages(chunks []pageChunk) int64 {
	total := int64(0)
	for _, chunk := range chunks {
		total += chunk.pages()
	}
	return total
}

// totalPages is what progress is measured against.
func totalPages(relations []relation) int64 {
	total := int64(0)
	for _, rel := range relations {
		total += max(rel.Pages, 1)
	}
	return total
}

// totalRows is the planner's estimate across a side's heaps.
func totalRows(relations []relation) int64 {
	total := int64(0)
	for _, rel := range relations {
		total += rel.Rows
	}
	return total
}
