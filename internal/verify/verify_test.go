package verify

import (
	"context"
	"strings"
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()
	if got, want := QuoteIdentifier(`odd"schema`, `select`), `"odd""schema"."select"`; got != want {
		t.Fatalf("QuoteIdentifier() = %q, want %q", got, want)
	}
}

// TestPlanSampleSpreadsWindowsAcrossTheHeap is the property the sample rests on.
// The relation is the production table this design was measured against: 131M rows
// in 11.3M pages, whose row density varied between 0.09 and 17.2 rows per page by
// region. A budget spent in one place would be a sample of that place.
func TestPlanSampleSpreadsWindowsAcrossTheHeap(t *testing.T) {
	t.Parallel()
	heap := relation{Schema: "s", Name: "messages", Pages: 11_294_175, Rows: 130_900_000}
	windows := planSample([]relation{heap}, 1_000_000, 128)
	if len(windows) != 128 {
		t.Fatalf("planSample() produced %d windows, want 128", len(windows))
	}
	for i, window := range windows {
		if window.Limit != 7812 {
			t.Errorf("window %d carries limit %d, want the budget divided by the windows", i, window.Limit)
		}
		if i == 0 {
			continue
		}
		if window.Start <= windows[i-1].Start {
			t.Fatalf("window %d starts at %d, not after %d", i, window.Start, windows[i-1].Start)
		}
		if previous := windows[i-1]; previous.End > window.Start {
			t.Fatalf("window %d ends at %d and overlaps %d", i-1, previous.End, window.Start)
		}
	}
	// The read has to stay a small fraction of the heap, which is the entire
	// reason for sampling: 128 windows of roughly a million rows' worth of pages.
	if read, total := plannedPages(windows), totalPages([]relation{heap}); read > total/50 {
		t.Fatalf("the plan reads %d of %d pages, which is not a sample", read, total)
	}
}

// TestPlanSampleAlwaysCoversTheTailOfTheHeap pins the one window that is not on the
// stride. Appended rows land at the end of the heap, so that is where a
// just-replicated row is, and where a replication fault shows up first.
func TestPlanSampleAlwaysCoversTheTailOfTheHeap(t *testing.T) {
	t.Parallel()
	heap := relation{Schema: "s", Name: "events", Pages: 500_000, Rows: 40_000_000}
	windows := planSample([]relation{heap}, 1_000_000, 64)
	last := windows[len(windows)-1]
	if last.End != 0 {
		t.Fatalf("the last window ends at %d, so rows written since the heap was measured fall outside every window", last.End)
	}
	if last.Start >= heap.Pages || last.Start <= windows[len(windows)-2].Start {
		t.Fatalf("the last window starts at %d, which is not the tail of a %d-page heap", last.Start, heap.Pages)
	}
}

// TestPlanSampleReadsASmallRelationWhole keeps the sample from being pointlessly
// clever about a table it could just read. Anything at or under the budget is read
// end to end, with an open tail.
func TestPlanSampleReadsASmallRelationWhole(t *testing.T) {
	t.Parallel()
	for _, heap := range []relation{
		{Schema: "s", Name: "small", Pages: 40, Rows: 900},
		{Schema: "s", Name: "empty", Pages: 0, Rows: 0},
		{Schema: "s", Name: "unanalyzed", Pages: 900, Rows: 0},
	} {
		windows := planSample([]relation{heap}, 1_000_000, 128)
		if len(windows) != 1 || windows[0].Start != 0 || windows[0].End != 0 {
			t.Fatalf("%s: planSample() = %v, want one open window", heap.Name, windows)
		}
	}
}

// TestPlanSampleApportionsTheBudgetAcrossPartitionLeaves covers the case the
// budget can be spent several times over: a partitioned table is many heaps, and
// each leaf gets a share of the budget by size rather than a budget of its own.
func TestPlanSampleApportionsTheBudgetAcrossPartitionLeaves(t *testing.T) {
	t.Parallel()
	leaves := []relation{
		{Schema: "s", Name: "p1", Pages: 1000, Rows: 1_000_000},
		{Schema: "s", Name: "p2", Pages: 2000, Rows: 2_000_000},
		{Schema: "s", Name: "p3", Pages: 7000, Rows: 7_000_000},
	}
	windows := planSample(leaves, 1_000_000, 10)
	budgeted := map[string]int64{}
	for _, window := range windows {
		budgeted[window.relation.Name] += window.Limit
	}
	for name, want := range map[string]int64{"p1": 100_000, "p2": 200_000, "p3": 700_000} {
		if got := budgeted[name]; got != want {
			t.Errorf("leaf %s may return %d rows, want its %d-row share of the budget", name, got, want)
		}
	}
	if total := budgeted["p1"] + budgeted["p2"] + budgeted["p3"]; total > 1_000_000 {
		t.Fatalf("the leaves may return %d rows between them, over the 1,000,000 budget", total)
	}
}

func TestRowsPerWindowDividesTheBudget(t *testing.T) {
	t.Parallel()
	if got := rowsPerWindow(1_000_000, 128); got != 7812 {
		t.Fatalf("rowsPerWindow() = %d, want 7812", got)
	}
	// More windows than rows still has to read something per window, or a window
	// would be a statement that returns nothing.
	if got := rowsPerWindow(10, 128); got != 1 {
		t.Fatalf("rowsPerWindow() = %d, want 1", got)
	}
}

func TestSampledCoverageNeverRoundsUpToTheWholeTable(t *testing.T) {
	t.Parallel()
	if got := sampledCoverage(SourceResult{Rows: 1_000_000, Estimated: 130_900_000}); got > 0.008 {
		t.Fatalf("sampledCoverage() = %v, want the sampled fraction", got)
	}
	// A table read whole reports full coverage, and an estimate lower than what
	// was read is an estimate, not coverage above one.
	if got := sampledCoverage(SourceResult{Rows: 900, Estimated: 900}); got != 1 {
		t.Fatalf("sampledCoverage() = %v, want 1", got)
	}
	if got := sampledCoverage(SourceResult{Rows: 900, Estimated: 100}); got != 1 {
		t.Fatalf("sampledCoverage() = %v, want 1", got)
	}
}

// TestKeyBatchFitsTheBindParameterLimit guards a runtime failure rather than a
// slow query: a lookup binds one parameter per key column per key, and the wire
// protocol allows 65535 in a statement, so a wide key has to shrink the batch.
func TestKeyBatchFitsTheBindParameterLimit(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		requested int64
		columns   int
		want      int64
	}{
		{"a narrow key uses the configured batch", 5000, 2, 5000},
		{"a wide key is clamped to the protocol limit", 5000, 40, 1638},
		{"a very wide key still reads something", 5000, 70000, 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got := keyBatch(testCase.requested, testCase.columns)
			if got != testCase.want {
				t.Fatalf("keyBatch(%d, %d) = %d, want %d",
					testCase.requested, testCase.columns, got, testCase.want)
			}
			if got*int64(testCase.columns) > 65535 && testCase.columns <= 65535 {
				t.Fatalf("a batch of %d keys binds %d parameters", got, got*int64(testCase.columns))
			}
		})
	}
}

func TestPageChunkWhereIsHalfOpenAndInlinesPages(t *testing.T) {
	t.Parallel()
	bounded := pageChunk{Start: 2, End: 4}.where()
	if want := " WHERE t.ctid >= '(2,0)'::tid AND t.ctid < '(4,0)'::tid"; bounded != want {
		t.Fatalf("where() = %q, want %q", bounded, want)
	}
	open := pageChunk{Start: 4}.where("t.id > 0")
	if want := " WHERE t.ctid >= '(4,0)'::tid AND t.id > 0"; open != want {
		t.Fatalf("where() = %q, want %q", open, want)
	}
}

func TestCompareRowsRequiresSourceRowsAndIgnoresExtraTargetRows(t *testing.T) {
	t.Parallel()
	source := rowSet{
		identity([]string{"1"}): {key: []string{"1"}, hash: 10},
		identity([]string{"2"}): {key: []string{"2"}, hash: 20},
		identity([]string{"3"}): {key: []string{"3"}, hash: 30},
	}
	target := rowSet{
		identity([]string{"1"}): {key: []string{"1"}, hash: 10},
		identity([]string{"2"}): {key: []string{"2"}, hash: 99},
		identity([]string{"4"}): {key: []string{"4"}, hash: 40},
	}
	got := compareRows(source, target)
	want := []RowDiff{
		{Key: []string{"2"}, Kind: DiffDifferent},
		{Key: []string{"3"}, Kind: DiffSourceOnly},
	}
	if len(got) != len(want) {
		t.Fatalf("compareRows() = %v", got)
	}
	for i := range want {
		if got[i].Kind != want[i].Kind || got[i].Key[0] != want[i].Key[0] {
			t.Fatalf("compareRows()[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestCompareRowsAcceptsATargetSuperset(t *testing.T) {
	t.Parallel()
	target := rowSet{
		identity([]string{"1"}): {key: []string{"1"}, hash: 10},
		identity([]string{"2"}): {key: []string{"2"}, hash: 20},
	}
	for _, source := range []rowSet{
		nil,
		{},
		{identity([]string{"1"}): {key: []string{"1"}, hash: 10}},
	} {
		if diffs := compareRows(source, target); len(diffs) != 0 {
			t.Fatalf("compareRows(%v, %v) = %v, want no differences", source, target, diffs)
		}
	}
}

func TestCandidateKeysBoundsWhatOneTableReports(t *testing.T) {
	t.Parallel()
	diffs := make([]RowDiff, 10)
	if got := candidateKeys(diffs, 3); len(got) != 3 {
		t.Fatalf("candidateKeys() kept %d", len(got))
	}
	if got := candidateKeys(diffs, 0); len(got) != 10 {
		t.Fatalf("an unset limit kept %d", len(got))
	}
}

func TestResultReportsTheWorstTableNotTheAverage(t *testing.T) {
	t.Parallel()
	result := Result{Tables: []TableResult{
		{Table: Table{Schema: "s", Name: "done"}, Coverage: 1, Complete: true, Converged: true},
		{Table: Table{Schema: "s", Name: "cut"}, Coverage: 0.25, CutShort: "table timeout reached"},
	}}
	if got := result.Coverage(); got != 0.25 {
		t.Fatalf("Coverage() = %v", got)
	}
	if got := result.DivergedTables(); len(got) != 1 || got[0] != "s.cut" {
		t.Fatalf("DivergedTables() = %v", got)
	}
	cut := result.CutShort()
	if len(cut) != 1 || !strings.Contains(cut[0], "s.cut: table timeout reached") {
		t.Fatalf("CutShort() = %v", cut)
	}
}

func TestChooseKeyPrefersThePrimaryKeyThenTheNarrowestNotNullUnique(t *testing.T) {
	t.Parallel()
	key, keyless, err := chooseKey(context.Background(), keyLadder{
		{"items_pkey", true, 1, "id", "bigint", true},
		{"items_app_key", false, 1, "app", "text", true},
	}, 1)
	if err != nil || keyless != "" || len(key.Columns) != 1 || key.Columns[0].Name != "id" || !key.Primary {
		t.Fatalf("chooseKey() = %+v, %q, %v", key, keyless, err)
	}
	key, keyless, err = chooseKey(context.Background(), keyLadder{
		{"items_wide_key", false, 1, "a", "text", true},
		{"items_wide_key", false, 2, "b", "text", true},
		{"items_narrow_key", false, 1, "c", "uuid", true},
	}, 1)
	if err != nil || keyless != "" || len(key.Columns) != 1 || key.Columns[0].Name != "c" {
		t.Fatalf("chooseKey() = %+v, %q, %v", key, keyless, err)
	}
	_, keyless, err = chooseKey(context.Background(), keyLadder{
		{"items_nullable_key", false, 1, "maybe", "text", false},
	}, 1)
	if err != nil || !strings.Contains(keyless, "NULL") {
		t.Fatalf("a nullable unique index should not be a key: %q, %v", keyless, err)
	}
	if _, keyless, err = chooseKey(context.Background(), keyLadder{}, 1); err != nil ||
		!strings.Contains(keyless, "no primary key") {
		t.Fatalf("chooseKey() with no index = %q, %v", keyless, err)
	}
}
