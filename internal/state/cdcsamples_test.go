package state

import (
	"context"
	"testing"
)

func TestPutCDCSamplesRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())
	samples := []CDCSample{
		{
			Schema: "s", Table: "a", Index: 0, Kind: "insert", LSN: "0/1",
			Key: map[string]string{"app_pk": "1", "id": "x"},
		},
		{
			Schema: "s", Table: "a", Index: 1, Kind: "delete", LSN: "0/2",
			Key: map[string]string{"app_pk": "1", "id": "y"},
		},
		{
			Schema: "s", Table: "b", Index: 0, Kind: "update", LSN: "0/3",
			Key: map[string]string{"id": "z"},
		},
	}
	if err := store.PutCDCSamples(ctx, samples, nil); err != nil {
		t.Fatalf("PutCDCSamples() error = %v", err)
	}

	got, err := store.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("CDCSamples() returned %d samples, want the 2 written for s.a", len(got))
	}
	if got[0].Index != 0 || got[1].Index != 1 {
		t.Errorf("CDCSamples() returned indexes %d,%d, want them ordered 0,1", got[0].Index, got[1].Index)
	}
	if got[0].Key["id"] != "x" || got[0].Key["app_pk"] != "1" {
		t.Errorf("CDCSamples() returned key %v, want the two columns written", got[0].Key)
	}
	if got[1].Kind != "delete" {
		t.Errorf("CDCSamples() returned kind %q, want delete: the kind is what says a row should be absent", got[1].Kind)
	}
}

// TestPutCDCSamplesReplacesByIndex is the reservoir's replacement step. Writing a
// slot again has to overwrite it, because the slot is how the row count per
// relation stays bounded without anything ever counting the rows.
func TestPutCDCSamplesReplacesByIndex(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())
	for _, id := range []string{"first", "second"} {
		if err := store.PutCDCSamples(ctx, []CDCSample{{
			Schema: "s", Table: "a", Index: 7, Kind: "insert",
			Key: map[string]string{"id": id},
		}}, nil); err != nil {
			t.Fatalf("PutCDCSamples(%s) error = %v", id, err)
		}
	}
	got, err := store.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("CDCSamples() returned %d samples, want 1: writing a slot twice must replace it", len(got))
	}
	if got[0].Key["id"] != "second" {
		t.Errorf("CDCSamples() returned %q, want the later write", got[0].Key["id"])
	}
}

// TestCDCSampleStreamsSurviveReopen covers why the counters are persisted at all.
// An applier that restarted with them at zero would allocate slot 0 again and
// replace a sample of the whole stream with a prefix of whatever came after the
// restart.
func TestCDCSampleStreamsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openTestStore(t, dir)
	if err := store.PutCDCSamples(ctx, nil, []CDCSampleStream{
		{Schema: "s", Table: "a", Observed: 4_200_000, Retained: 100_000, Dropped: 3},
	}); err != nil {
		t.Fatalf("PutCDCSamples() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := openTestStore(t, dir)
	streams, err := reopened.LoadCDCSampleStreams(ctx)
	if err != nil {
		t.Fatalf("LoadCDCSampleStreams() error = %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("LoadCDCSampleStreams() returned %d streams, want 1", len(streams))
	}
	if streams[0].Observed != 4_200_000 || streams[0].Retained != 100_000 || streams[0].Dropped != 3 {
		t.Errorf("LoadCDCSampleStreams() = %+v, want the counters as written", streams[0])
	}
	observed, dropped, err := reopened.CDCSampleCounters(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSampleCounters() error = %v", err)
	}
	if observed != 4_200_000 || dropped != 3 {
		t.Errorf("CDCSampleCounters() = %d observed, %d dropped, want 4200000 and 3",
			observed, dropped)
	}
}

// TestCDCSampleCountersAreZeroForAnUnseenRelation separates "the applier saw
// nothing here" from an error. Verification reports the two differently: the
// first means the replication path was not exercised for that table, and it must
// not read as a check that passed.
func TestCDCSampleCountersAreZeroForAnUnseenRelation(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, t.TempDir())
	observed, dropped, err := store.CDCSampleCounters(ctx, "s", "never_written")
	if err != nil {
		t.Fatalf("CDCSampleCounters() error = %v", err)
	}
	if observed != 0 || dropped != 0 {
		t.Errorf("CDCSampleCounters() = %d observed, %d dropped, want zeroes", observed, dropped)
	}
}
