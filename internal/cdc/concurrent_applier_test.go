package cdc

import "testing"

func TestApplierDefaultsToSerialForDirectCallers(t *testing.T) {
	t.Parallel()
	applier, err := NewApplier(ApplierConfig{
		ConnString: "postgres://target/db", Directory: t.TempDir(),
		StreamID: "stream", Durable: new(DurableWatermark),
	})
	if err != nil {
		t.Fatal(err)
	}
	if applier.config.Workers != 1 {
		t.Fatalf("workers = %d, want backward-compatible serial default", applier.config.Workers)
	}
}

func TestReplayJobsDependOnThePreviousTransactionForEveryTable(t *testing.T) {
	t.Parallel()
	tails := make(map[uint32]*replayJob)
	job := func(relations ...uint32) *replayJob {
		metadata := make([]Relation, len(relations))
		for i, oid := range relations {
			metadata[i].OID = oid
		}
		result := newReplayJob(Transaction{Relations: metadata}, 0, 1)
		linkReplayJob(result, tails)
		return result
	}

	firstA := job(1)
	bridgeAB := job(1, 2)
	laterB := job(2)
	independentC := job(3)
	joinAC := job(1, 3)

	if firstA.waiting != 0 || independentC.waiting != 0 {
		t.Fatalf("initial independent jobs wait %d/%d, want 0/0", firstA.waiting, independentC.waiting)
	}
	if bridgeAB.waiting != 1 {
		t.Fatalf("multi-table bridge waits for %d jobs, want 1", bridgeAB.waiting)
	}
	if laterB.waiting != 1 {
		t.Fatalf("later B transaction waits for %d jobs, want the bridge", laterB.waiting)
	}
	if joinAC.waiting != 2 {
		t.Fatalf("A/C join waits for %d jobs, want both table tails", joinAC.waiting)
	}
	if len(firstA.dependents) != 1 || firstA.dependents[0] != bridgeAB {
		t.Fatal("first A transaction did not release the A/B bridge")
	}
	if len(bridgeAB.dependents) != 2 {
		t.Fatalf("A/B bridge dependents = %d, want later B and A/C join", len(bridgeAB.dependents))
	}
}

func TestReplayJobDeduplicatesRelationsAndPredecessors(t *testing.T) {
	t.Parallel()
	tails := make(map[uint32]*replayJob)
	previous := newReplayJob(Transaction{Relations: []Relation{{OID: 1}, {OID: 2}}}, 0, 1)
	linkReplayJob(previous, tails)
	next := newReplayJob(Transaction{Relations: []Relation{{OID: 1}, {OID: 1}, {OID: 2}}}, 0, 1)
	linkReplayJob(next, tails)

	if len(next.relations) != 2 {
		t.Fatalf("deduplicated relations = %v, want two OIDs", next.relations)
	}
	if next.waiting != 1 {
		t.Fatalf("shared predecessor counted %d times, want once", next.waiting)
	}
}

func TestReplayJobDoesNotWaitForAnAlreadyCommittedTableTail(t *testing.T) {
	t.Parallel()
	tails := make(map[uint32]*replayJob)
	previous := newReplayJob(Transaction{Relations: []Relation{{OID: 1}}}, 0, 1)
	linkReplayJob(previous, tails)
	previous.committed = true

	next := newReplayJob(Transaction{Relations: []Relation{{OID: 1}}}, 0, 1)
	linkReplayJob(next, tails)
	if next.waiting != 0 {
		t.Fatalf("job waits for %d already committed predecessors, want 0", next.waiting)
	}
}

func TestReplayBatchOnlyCombinesCoveredTableSets(t *testing.T) {
	t.Parallel()
	job := newReplayJob(Transaction{
		EndLSN: 1, Relations: []Relation{{OID: 1}, {OID: 2}},
	}, 100, 4)
	if !job.append(Transaction{EndLSN: 2, Relations: []Relation{{OID: 1}}}, 100, 4) {
		t.Fatal("dependent transaction was not batched")
	}
	if job.append(Transaction{EndLSN: 3, Relations: []Relation{{OID: 3}}}, 100, 4) {
		t.Fatal("independent table was absorbed into a batch")
	}
	if len(job.transactions) != 2 || job.endLSN() != 2 {
		t.Fatalf("batch transactions/end = %d/%d, want 2/2", len(job.transactions), job.endLSN())
	}
}
