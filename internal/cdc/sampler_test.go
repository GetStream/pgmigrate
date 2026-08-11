package cdc

import (
	"testing"
)

func sampleRelation() *Relation {
	return &Relation{
		OID: 42, Namespace: "s", Name: "messages", ReplicaIdentity: 'd',
		Columns: []Column{
			{Name: "app_pk", Type: 23, Flags: 1},
			{Name: "id", Type: 2950, Flags: 1},
			{Name: "text", Type: 25},
		},
	}
}

func textTuple(values ...string) *Tuple {
	tuple := make(Tuple, 0, len(values))
	for _, value := range values {
		tuple = append(tuple, TupleDatum{Kind: DatumText, Data: []byte(value)})
	}
	return &tuple
}

func TestKeySampleForUsesTheNewTupleAndOnlyKeyColumns(t *testing.T) {
	t.Parallel()
	change := &Change{RelationOID: 42, Kind: ChangeInsert, New: textTuple("1", "abc", "hello")}
	sample, ok := keySampleFor(sampleRelation(), change, 99)
	if !ok {
		t.Fatal("keySampleFor() refused an ordinary insert")
	}
	if len(sample.Columns) != 2 {
		t.Fatalf("keySampleFor() kept %d columns, want the 2 flagged as the replica identity", len(sample.Columns))
	}
	if sample.Columns[0].Name != "app_pk" || sample.Columns[1].Name != "id" {
		t.Errorf("keySampleFor() kept %q,%q, want app_pk,id",
			sample.Columns[0].Name, sample.Columns[1].Name)
	}
	if string(sample.Columns[1].Datum.Data) != "abc" {
		t.Errorf("keySampleFor() kept %q for id, want abc", sample.Columns[1].Datum.Data)
	}
	if sample.Schema != "s" || sample.Table != "messages" || sample.LSN != 99 {
		t.Errorf("keySampleFor() = %s.%s at %d, want s.messages at 99",
			sample.Schema, sample.Table, sample.LSN)
	}
}

// TestKeySampleForUsesTheOldTupleOnDelete matters more than it looks. The delete
// keys are the only thing either stratum has that can find a row the target holds
// and the source does not, and a delete carries no new tuple at all.
func TestKeySampleForUsesTheOldTupleOnDelete(t *testing.T) {
	t.Parallel()
	change := &Change{RelationOID: 42, Kind: ChangeDelete, Old: textTuple("1", "gone", "")}
	sample, ok := keySampleFor(sampleRelation(), change, 1)
	if !ok {
		t.Fatal("keySampleFor() refused a delete, so an unapplied delete would never be checked")
	}
	if string(sample.Columns[1].Datum.Data) != "gone" {
		t.Errorf("keySampleFor() kept %q, want the old tuple's key", sample.Columns[1].Datum.Data)
	}
}

func TestKeySampleForRejectsAnUnchangedToastKey(t *testing.T) {
	t.Parallel()
	tuple := Tuple{
		{Kind: DatumText, Data: []byte("1")},
		{Kind: DatumUnchangedToast},
		{Kind: DatumText, Data: []byte("hello")},
	}
	change := &Change{RelationOID: 42, Kind: ChangeUpdate, New: &tuple}
	if _, ok := keySampleFor(sampleRelation(), change, 1); ok {
		t.Fatal("keySampleFor() accepted a key it cannot name, which would be recorded as a key of the empty string")
	}
}

func TestKeySampleForRejectsARelationWithNoKeyColumns(t *testing.T) {
	t.Parallel()
	relation := &Relation{OID: 7, Namespace: "s", Name: "keyless", Columns: []Column{{Name: "a", Type: 23}}}
	change := &Change{RelationOID: 7, Kind: ChangeInsert, New: textTuple("1")}
	if _, ok := keySampleFor(relation, change, 1); ok {
		t.Fatal("keySampleFor() accepted a relation with no replica identity")
	}
}

// TestSampleCollectorCopiesDatums guards the spilled path. A spilled transaction
// decodes its changes from disk into a buffer it reuses, so a sampler holding the
// original slice would be handed whatever the next change decoded into.
func TestSampleCollectorCopiesDatums(t *testing.T) {
	t.Parallel()
	buffer := []byte("first")
	tuple := Tuple{
		{Kind: DatumText, Data: []byte("1")},
		{Kind: DatumText, Data: buffer},
		{Kind: DatumText, Data: []byte("x")},
	}
	sample, ok := keySampleFor(sampleRelation(), &Change{
		RelationOID: 42, Kind: ChangeInsert, New: &tuple,
	}, 1)
	if !ok {
		t.Fatal("keySampleFor() refused an ordinary insert")
	}
	copy(buffer, "SECON")
	if string(sample.Columns[1].Datum.Data) != "first" {
		t.Fatalf("the sample followed the reused buffer to %q", sample.Columns[1].Datum.Data)
	}
}

func TestSampleCollectorFlushesOnlyWhatWasAdded(t *testing.T) {
	t.Parallel()
	recorder := &recordingSampler{}
	transaction := &Transaction{EndLSN: 5, Relations: []Relation{*sampleRelation()}}
	collector := newSampleCollector(recorder, transaction)
	collector.add(&Change{RelationOID: 42, Kind: ChangeInsert, New: textTuple("1", "a", "")})
	collector.add(&Change{RelationOID: 42, Kind: ChangeTruncate})
	if len(recorder.samples) != 0 {
		t.Fatal("the collector reported before the commit, so a rolled back apply would name rows it never wrote")
	}
	collector.flush()
	if len(recorder.samples) != 1 {
		t.Fatalf("flush() reported %d samples, want 1: a truncate names no row", len(recorder.samples))
	}
}

func TestSampleCollectorIsInertWithoutASampler(t *testing.T) {
	t.Parallel()
	collector := newSampleCollector(nil, &Transaction{})
	collector.add(&Change{RelationOID: 42, Kind: ChangeInsert})
	collector.addAll([]Change{{RelationOID: 42, Kind: ChangeInsert}})
	collector.flush()
}

type recordingSampler struct{ samples []KeySample }

func (r *recordingSampler) Observe(sample KeySample) { r.samples = append(r.samples, sample) }
