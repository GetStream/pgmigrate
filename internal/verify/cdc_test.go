package verify

import (
	"context"
	"strings"
	"testing"
)

func keyedTable() Table {
	return Table{
		Schema: "s", Name: "messages", RelKind: "r",
		Key: Key{Columns: []KeyColumn{
			{Name: "app_pk", Type: "integer"},
			{Name: "id", Type: "text"},
		}},
	}
}

func TestProjectKeyOrdersColumnsAsTheKeyDoes(t *testing.T) {
	t.Parallel()
	// The recorded key is a map, so the order it was written in is gone. It has
	// to come back out in the key's own order, because that is the order the
	// lookup binds its parameters in.
	key, ok := projectKey(keyedTable(), map[string]string{"id": "abc", "app_pk": "1397531"})
	if !ok {
		t.Fatal("projectKey() refused a key that covers every column")
	}
	if len(key) != 2 || key[0] != "1397531" || key[1] != "abc" {
		t.Fatalf("projectKey() = %v, want [1397531 abc] in key order", key)
	}
}

// TestProjectKeyRefusesAKeyItCannotCover is the case that would otherwise be
// silent. The applier keys a change on the replica identity and this package keys
// a row on the primary key; when those differ, comparing what overlaps would ask
// the target about a key it cannot look up and report every row as missing.
func TestProjectKeyRefusesAKeyItCannotCover(t *testing.T) {
	t.Parallel()
	if _, ok := projectKey(keyedTable(), map[string]string{"id": "abc"}); ok {
		t.Fatal("projectKey() accepted a key missing app_pk")
	}
}

// A recorded key that is absent on the source is not required to be absent on
// the target, even when the CDC check asks both sides about it.
func TestCompareRowsIgnoresAnUnappliedDelete(t *testing.T) {
	t.Parallel()
	target := rowSet{
		identity([]string{"1", "gone"}): {key: []string{"1", "gone"}, hash: 7},
	}
	diffs := compareRows(rowSet{}, target)
	if len(diffs) != 0 {
		t.Fatalf("compareRows() found %v for a row absent on the source, want none", diffs)
	}
}

// TestCompareRowsIsCleanWhenADeleteApplied covers the ordinary case, which must
// not be reported: a correctly applied delete leaves the row absent on both
// sides, and absent on both sides is agreement.
func TestCompareRowsIsCleanWhenADeleteApplied(t *testing.T) {
	t.Parallel()
	if diffs := compareRows(rowSet{}, rowSet{}); len(diffs) != 0 {
		t.Fatalf("compareRows() found %d differences for a row absent on both sides, want none", len(diffs))
	}
}

func TestVerifyCDCSkipsWhenTheRecordedKeyDoesNotCover(t *testing.T) {
	t.Parallel()
	table := keyedTable()
	worker := &worker{cfg: &Config{
		CDCRows: 100,
		CDCKeys: func(context.Context, string, string) (CDCRecorded, error) {
			return CDCRecorded{
				Observed: 10,
				Keys:     []CDCKey{{Key: map[string]string{"id": "abc"}, Kind: "insert"}},
			}, nil
		},
	}}
	var out TableResult
	diffs, err := worker.verifyCDC(context.Background(), table,
		[]relation{{Schema: "s", Name: "messages"}}, &out)
	if err != nil {
		t.Fatalf("verifyCDC() error = %v", err)
	}
	if len(diffs) != 0 {
		t.Fatalf("verifyCDC() invented %d differences from a key it cannot use", len(diffs))
	}
	if out.CDC.Skipped == "" {
		t.Fatal("verifyCDC() skipped the table silently, so the run would look checked")
	}
	if !strings.Contains(out.CDC.Skipped, "app_pk") {
		t.Errorf("verifyCDC() skipped with %q, want it to name the columns involved", out.CDC.Skipped)
	}
}

// TestVerifyCDCReportsObservedWithNothingRetained separates two findings a single
// count would merge: an applier that has written millions of rows here and a
// reservoir that kept none of them, versus a table replication never touched.
func TestVerifyCDCReportsObservedWithNothingRetained(t *testing.T) {
	t.Parallel()
	worker := &worker{cfg: &Config{
		CDCRows: 100,
		CDCKeys: func(context.Context, string, string) (CDCRecorded, error) {
			return CDCRecorded{Observed: 4_200_000}, nil
		},
	}}
	var out TableResult
	if _, err := worker.verifyCDC(context.Background(), keyedTable(),
		[]relation{{Schema: "s", Name: "messages"}}, &out); err != nil {
		t.Fatalf("verifyCDC() error = %v", err)
	}
	if out.CDC.Observed != 4_200_000 {
		t.Errorf("verifyCDC() recorded %d observed changes, want 4200000", out.CDC.Observed)
	}
	if out.CDC.Keys != 0 {
		t.Errorf("verifyCDC() claims to have checked %d keys with none retained", out.CDC.Keys)
	}
}

// TestVerifyCDCWarnsAboutChangesItWasNeverOffered covers the finding that would
// otherwise hide behind an ordinary-looking ratio. Fewer keys than changes is what
// a full reservoir looks like, so a relation whose every identity was unrenderable
// reads exactly like a well-sampled one unless it is said out loud.
func TestVerifyCDCWarnsAboutChangesItWasNeverOffered(t *testing.T) {
	t.Parallel()
	worker := &worker{cfg: &Config{
		CDCRows: 100,
		CDCKeys: func(context.Context, string, string) (CDCRecorded, error) {
			return CDCRecorded{Observed: 900, Dropped: 900}, nil
		},
	}}
	var out TableResult
	if _, err := worker.verifyCDC(context.Background(), keyedTable(),
		[]relation{{Schema: "s", Name: "messages"}}, &out); err != nil {
		t.Fatalf("verifyCDC() error = %v", err)
	}
	if out.CDC.Dropped != 900 {
		t.Errorf("verifyCDC() recorded %d dropped changes, want 900", out.CDC.Dropped)
	}
	if len(out.Warnings) != 1 || !strings.Contains(out.Warnings[0], "900") {
		t.Errorf("verifyCDC() warnings = %v, want one naming the 900 changes it never saw", out.Warnings)
	}
}

func TestVerifyCDCIsInertWithoutARecorder(t *testing.T) {
	t.Parallel()
	worker := &worker{cfg: &Config{}}
	var out TableResult
	diffs, err := worker.verifyCDC(context.Background(), keyedTable(), nil, &out)
	if err != nil || diffs != nil {
		t.Fatalf("verifyCDC() = %v, %v with no recorder, want nothing", diffs, err)
	}
}

// TestCollectRecordedSpendsOneBudgetAcrossEveryLeaf keeps a partitioned table
// from spending the whole budget once per partition, which would make this
// stratum's cost depend on how the table happens to be partitioned.
func TestCollectRecordedSpendsOneBudgetAcrossEveryLeaf(t *testing.T) {
	t.Parallel()
	recorded := make([]CDCKey, 40)
	for i := range recorded {
		recorded[i] = CDCKey{Key: map[string]string{"app_pk": "1", "id": "x"}, Kind: "insert"}
	}
	read := func(context.Context, string, string) (CDCRecorded, error) {
		return CDCRecorded{Observed: 100, Keys: recorded}, nil
	}
	leaves := []relation{
		{Schema: "s", Name: "messages_p1"},
		{Schema: "s", Name: "messages_p2"},
		{Schema: "s", Name: "messages_p3"},
	}
	var out TableResult
	got, err := collectRecorded(context.Background(), read, leaves, 50, &out)
	if err != nil {
		t.Fatalf("collectRecorded() error = %v", err)
	}
	if len(got) != 50 {
		t.Errorf("collectRecorded() gathered %d keys against a budget of 50", len(got))
	}
	if out.CDC.Observed != 300 {
		t.Errorf("collectRecorded() recorded %d observed changes, want 300: the denominator covers every leaf, not only the ones it had budget for",
			out.CDC.Observed)
	}
}
