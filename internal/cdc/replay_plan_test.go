package cdc

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestReplayKeyDatumSafetyIsFormatAware(t *testing.T) {
	t.Parallel()
	textSafe := []uint32{
		pgtype.BoolOID, pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID,
		pgtype.TextOID, pgtype.VarcharOID, pgtype.UUIDOID,
	}
	binaryOnly := []uint32{
		pgtype.ByteaOID, pgtype.DateOID, pgtype.TimeOID,
		pgtype.TimestampOID, pgtype.TimestamptzOID,
	}
	alwaysUnsafe := []uint32{
		pgtype.NumericOID, pgtype.BPCharOID, pgtype.Float4OID, pgtype.Float8OID,
	}
	for _, oid := range textSafe {
		if !replayKeyDatumSafe(oid, DatumText) || !replayKeyDatumSafe(oid, DatumBinary) {
			t.Errorf("OID %d should be safe in text and binary formats", oid)
		}
	}
	for _, oid := range binaryOnly {
		if replayKeyDatumSafe(oid, DatumText) || !replayKeyDatumSafe(oid, DatumBinary) {
			t.Errorf("OID %d should be binary-only for replay keys", oid)
		}
	}
	for _, oid := range alwaysUnsafe {
		if replayKeyDatumSafe(oid, DatumText) || replayKeyDatumSafe(oid, DatumBinary) {
			t.Errorf("OID %d should be unsafe in both formats", oid)
		}
	}
}

func TestReplayPlanRequiresTextForCustomLanePayload(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(39, "custom_payload")
	relation.source.Columns[1].Type = 99_901
	relation.columns[1].oid = 99_902
	relation.columns[1].arrayOID = 99_903
	relation.columns[1].lanePayloadTextOnly = true
	textTransaction := replayTestTransaction(60, relation, Change{
		RelationOID: relation.source.OID, Kind: ChangeInsert,
		New: replayTuple("text-key", "enum-label"),
	})
	binaryTuple := Tuple{
		{Kind: DatumText, Data: []byte("binary-key")},
		{Kind: DatumBinary, Data: []byte{0, 0, 0, 1}},
	}
	binaryTransaction := replayTestTransaction(62, relation, Change{
		RelationOID: relation.source.OID, Kind: ChangeInsert, New: &binaryTuple,
	})
	resolved := []map[uint32]*targetRelation{
		{relation.source.OID: relation}, {relation.source.OID: relation},
	}
	plan, err := buildReplayPlan(
		"stream", "generation", 10, 8,
		[]Transaction{textTransaction, binaryTransaction}, resolved,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].SerialTransaction >= 0 ||
		plan.Steps[1].SerialTransaction != 1 {
		t.Fatalf("custom payload format did not form a serial barrier: %#v", plan.Steps)
	}
}

func TestReplayPlanNeverSplitsOneMultiRowTransactionAcrossWork(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(40, "items")
	transaction := replayTestTransaction(
		80,
		relation,
		Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("a", "one")},
		Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("b", "two")},
		Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("c", "three")},
	)
	plan, err := buildReplayPlan(
		"stream", "generation", 10, 16,
		[]Transaction{transaction},
		[]map[uint32]*targetRelation{{relation.source.OID: relation}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].SerialTransaction >= 0 ||
		len(plan.Steps[0].Lanes) != 1 || len(plan.Works) != 1 {
		t.Fatalf("one source transaction was split across replay work: %#v", plan)
	}
	lane := plan.Steps[0].Lanes[0]
	if !slices.Equal(lane.TransactionIndexes, []int{0}) || len(lane.Items) != 3 {
		t.Fatalf("one source transaction was not kept intact: %#v", lane)
	}
	for changeIndex, item := range lane.Items {
		if item.transactionIndex != 0 || item.changeIndex != changeIndex {
			t.Fatalf("lane item[%d]=(%d,%d), want (0,%d)", changeIndex, item.transactionIndex, item.changeIndex, changeIndex)
		}
	}
	if work := plan.Works[0]; work.ExpectedTransactions != 1 || work.ExpectedChanges != 3 {
		t.Fatalf("one transaction work totals=(%d,%d), want (1,3)", work.ExpectedTransactions, work.ExpectedChanges)
	}
}

func TestReplayPlanUnionsTransactionsSharingAnyPrimaryKey(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(47, "items")
	transactions := []Transaction{
		replayTestTransaction(
			90,
			relation,
			Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("left", "one")},
			Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("shared", "two")},
		),
		replayTestTransaction(
			92,
			relation,
			Change{RelationOID: relation.source.OID, Kind: ChangeUpdate, Old: replayTuple("shared", "two"), New: replayTuple("shared", "three")},
			Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("right", "four")},
		),
	}
	resolved := []map[uint32]*targetRelation{
		{relation.source.OID: relation},
		{relation.source.OID: relation},
	}
	plan, err := buildReplayPlan("stream", "generation", 20, 32, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || len(plan.Steps[0].Lanes) != 1 || len(plan.Works) != 1 {
		t.Fatalf("overlapping source transactions escaped one component: %#v", plan.Steps)
	}
	lane := plan.Steps[0].Lanes[0]
	if !slices.Equal(lane.TransactionIndexes, []int{0, 1}) {
		t.Fatalf("component transaction order=%v, want [0 1]", lane.TransactionIndexes)
	}
	wantItemTransactions := []int{0, 0, 1, 1}
	gotItemTransactions := make([]int, len(lane.Items))
	for i := range lane.Items {
		gotItemTransactions[i] = lane.Items[i].transactionIndex
	}
	if !slices.Equal(gotItemTransactions, wantItemTransactions) {
		t.Fatalf("component item order=%v, want %v", gotItemTransactions, wantItemTransactions)
	}
}

func TestReplayPlanUnionsTransitivePrimaryKeyOverlap(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(48, "items")
	transactions := []Transaction{
		replayTestTransaction(100, relation,
			Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("a", "zero")},
		),
		replayTestTransaction(102, relation,
			Change{RelationOID: relation.source.OID, Kind: ChangeUpdate, Old: replayTuple("a", "zero"), New: replayTuple("a", "one")},
			Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("b", "one")},
		),
		replayTestTransaction(104, relation,
			Change{RelationOID: relation.source.OID, Kind: ChangeUpdate, Old: replayTuple("b", "one"), New: replayTuple("b", "two")},
			Change{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("c", "two")},
		),
		replayTestTransaction(106, relation,
			Change{RelationOID: relation.source.OID, Kind: ChangeUpdate, Old: replayTuple("c", "two"), New: replayTuple("c", "three")},
		),
	}
	resolved := make([]map[uint32]*targetRelation, len(transactions))
	for i := range resolved {
		resolved[i] = map[uint32]*targetRelation{relation.source.OID: relation}
	}
	plan, err := buildReplayPlan("stream", "generation", 30, 32, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || len(plan.Steps[0].Lanes) != 1 || len(plan.Works) != 1 {
		t.Fatalf("transitively overlapping transactions escaped one component: %#v", plan.Steps)
	}
	if got := plan.Steps[0].Lanes[0].TransactionIndexes; !slices.Equal(got, []int{0, 1, 2, 3}) {
		t.Fatalf("transitive component transaction order=%v, want [0 1 2 3]", got)
	}
	if work := plan.Works[0]; work.ExpectedTransactions != 4 || work.ExpectedChanges != 6 {
		t.Fatalf("transitive component totals=(%d,%d), want (4,6)", work.ExpectedTransactions, work.ExpectedChanges)
	}
}

func TestReplayPlanShardsStablePrimaryKeysDeterministically(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(41, "items")
	transactions := make([]Transaction, 32)
	resolved := make([]map[uint32]*targetRelation, len(transactions))
	for i := range transactions {
		transactions[i] = replayTestTransaction(
			LSN(100+i*2), relation, Change{
				RelationOID: relation.source.OID,
				Kind:        ChangeInsert,
				New:         replayTuple(fmt.Sprint(i), fmt.Sprintf("value-%d", i)),
			},
		)
		resolved[i] = map[uint32]*targetRelation{relation.source.OID: relation}
	}

	first, err := buildReplayPlan("stream", "generation", 10, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildReplayPlan("stream", "generation", 10, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if !first.HasParallel || len(first.Steps) != 1 || len(first.Steps[0].Lanes) < 2 {
		t.Fatalf("plan did not shard primary keys: %#v", first.Steps)
	}
	if first.Claim.Digest != second.Claim.Digest ||
		first.Claim.CatalogDigest != second.Claim.CatalogDigest ||
		!slices.Equal(first.Works, second.Works) {
		t.Fatal("identical replay input produced a different durable plan")
	}
	seen := make([]bool, len(transactions))
	for _, lane := range first.Steps[0].Lanes {
		previous := -1
		for _, item := range lane.Items {
			if item.transactionIndex <= previous {
				t.Fatalf("lane %d lost source order: %d after %d", lane.Lane, item.transactionIndex, previous)
			}
			previous = item.transactionIndex
			seen[item.transactionIndex] = true
		}
	}
	for index, exists := range seen {
		if !exists {
			t.Fatalf("transaction %d is absent from the replay plan", index)
		}
	}
}

func TestReplayTransactionComponentLanesKeepSourceOrderOnHashCollision(t *testing.T) {
	t.Parallel()
	const (
		transactionCount = 7
		laneCount        = 2
	)
	epoch := make([]replayPlanTransaction, transactionCount)
	for i := range epoch {
		var key [sha256.Size]byte
		key[0] = byte(i + 1)
		epoch[i] = replayPlanTransaction{
			index: i,
			keys:  [][sha256.Size]byte{key},
			items: []relationBatchedChange{{transactionIndex: i}},
		}
	}
	lanes, err := replayTransactionComponentLanes(epoch, laneCount)
	if err != nil {
		t.Fatal(err)
	}
	collision := false
	seen := make([]int, transactionCount)
	for _, lane := range lanes {
		if len(lane.TransactionIndexes) > 1 {
			collision = true
		}
		if !slices.IsSorted(lane.TransactionIndexes) {
			t.Fatalf("lane %d reordered colliding components: %v", lane.Lane, lane.TransactionIndexes)
		}
		if len(lane.Items) != len(lane.TransactionIndexes) {
			t.Fatalf("lane %d has %d items for %d transactions", lane.Lane, len(lane.Items), len(lane.TransactionIndexes))
		}
		for i, transactionIndex := range lane.TransactionIndexes {
			if lane.Items[i].transactionIndex != transactionIndex {
				t.Fatalf("lane %d item order[%d]=%d, want %d", lane.Lane, i, lane.Items[i].transactionIndex, transactionIndex)
			}
			seen[transactionIndex]++
		}
	}
	if !collision {
		t.Fatal("test input did not produce a same-lane hash collision")
	}
	for transactionIndex, count := range seen {
		if count != 1 {
			t.Fatalf("transaction %d scheduled %d times, want once", transactionIndex, count)
		}
	}
}

func TestReplayPlanKeepsRepeatedPrimaryKeyOnOneOrderedLane(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(42, "items")
	transactions := make([]Transaction, 6)
	resolved := make([]map[uint32]*targetRelation, len(transactions))
	for i := range transactions {
		oldTuple := replayTuple("same", fmt.Sprintf("old-%d", i))
		newTuple := replayTuple("same", fmt.Sprintf("new-%d", i))
		transactions[i] = replayTestTransaction(
			LSN(200+i*2), relation, Change{
				RelationOID: relation.source.OID, Kind: ChangeUpdate,
				Old: oldTuple, New: newTuple,
			},
		)
		resolved[i] = map[uint32]*targetRelation{relation.source.OID: relation}
	}
	plan, err := buildReplayPlan("stream", "generation", 20, 16, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || len(plan.Steps[0].Lanes) != 1 {
		t.Fatalf("repeated primary key escaped one lane: %#v", plan.Steps)
	}
	for index, item := range plan.Steps[0].Lanes[0].Items {
		if item.transactionIndex != index {
			t.Fatalf("repeated key order[%d]=%d", index, item.transactionIndex)
		}
	}
}

func TestReplayPlanSerializesWholeUnsafeTransactionBetweenEpochs(t *testing.T) {
	t.Parallel()
	safe := replayTestRelation(43, "safe_items")
	unsafe := replayTestRelation(44, "unique_items")
	unsafe.capabilities.crossKeyConflicts = true
	unsafe.capabilities.relationOrderedLane = false

	transactions := []Transaction{
		replayTestTransaction(300, safe, Change{
			RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("a", "before"),
		}),
		{
			CommitLSN: 302, EndLSN: 303, CommitTime: time.Unix(302, 0).UTC(),
			Relations: []Relation{safe.source, unsafe.source},
			Changes: []Change{
				{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("b", "mixed")},
				{RelationOID: unsafe.source.OID, Kind: ChangeInsert, New: replayTuple("c", "unique")},
			},
		},
		replayTestTransaction(304, safe, Change{
			RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("d", "after"),
		}),
	}
	resolved := []map[uint32]*targetRelation{
		{safe.source.OID: safe},
		{safe.source.OID: safe, unsafe.source.OID: unsafe},
		{safe.source.OID: safe},
	}
	plan, err := buildReplayPlan("stream", "generation", 30, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 3 || plan.Steps[0].SerialTransaction >= 0 ||
		plan.Steps[1].SerialTransaction != 1 || plan.Steps[2].SerialTransaction >= 0 {
		t.Fatalf("unsafe transaction did not become a whole barrier: %#v", plan.Steps)
	}
	if len(plan.Steps[0].Lanes) != 1 ||
		!slices.Equal(plan.Steps[0].Lanes[0].TransactionIndexes, []int{0}) ||
		len(plan.Steps[1].Lanes) != 0 ||
		len(plan.Steps[2].Lanes) != 1 ||
		!slices.Equal(plan.Steps[2].Lanes[0].TransactionIndexes, []int{2}) {
		t.Fatalf("unsafe transaction did not fully separate its neighboring epochs: %#v", plan.Steps)
	}
	barrier, exists := replayPlanWork(plan, 1, 0)
	if !exists || barrier.Kind != replayWorkSerial ||
		barrier.ExpectedTransactions != 1 || barrier.ExpectedChanges != 2 {
		t.Fatalf("unsafe barrier work=%#v exists=%t", barrier, exists)
	}
	if got := replayPlanWorkChanges(plan.Works); got != 4 || plan.Claim.Changes != 4 ||
		plan.Claim.Transactions != 3 {
		t.Fatalf("claim counters work=%d changes=%d tx=%d", got, plan.Claim.Changes, plan.Claim.Transactions)
	}
}

func TestReplayPlanKeepsSafeCrossKeyRelationInOneOrderedLane(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(45, "unique_items")
	relation.capabilities.crossKeyConflicts = true
	transactions := []Transaction{
		replayTestTransaction(400, relation, Change{
			RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("a", "one"),
		}),
		replayTestTransaction(402, relation, Change{
			RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("b", "two"),
		}),
	}
	resolved := []map[uint32]*targetRelation{
		{relation.source.OID: relation}, {relation.source.OID: relation},
	}
	plan, err := buildReplayPlan("stream", "generation", 30, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].SerialTransaction >= 0 ||
		len(plan.Steps[0].Lanes) != 1 ||
		!slices.Equal(plan.Steps[0].Lanes[0].TransactionIndexes, []int{0, 1}) {
		t.Fatalf("cross-key relation lost table-local source order: %#v", plan.Steps)
	}
}

func TestReplayPlanRelationLaneUnionsMultiTableTransactions(t *testing.T) {
	t.Parallel()
	withoutPrimary := replayTestRelation(55, "append_log")
	withoutPrimary.capabilities.relationLane = false
	left := replayTestRelation(56, "left_items")
	right := replayTestRelation(57, "right_items")
	transactions := []Transaction{
		{
			CommitLSN: 410, EndLSN: 411, CommitTime: time.Unix(410, 0).UTC(),
			Relations: []Relation{withoutPrimary.source, left.source},
			Changes: []Change{
				{RelationOID: withoutPrimary.source.OID, Kind: ChangeInsert, New: replayTuple("log-a", "one")},
				{RelationOID: left.source.OID, Kind: ChangeInsert, New: replayTuple("left", "one")},
			},
		},
		{
			CommitLSN: 412, EndLSN: 413, CommitTime: time.Unix(412, 0).UTC(),
			Relations: []Relation{withoutPrimary.source, right.source},
			Changes: []Change{
				{RelationOID: withoutPrimary.source.OID, Kind: ChangeInsert, New: replayTuple("log-b", "two")},
				{RelationOID: right.source.OID, Kind: ChangeInsert, New: replayTuple("right", "two")},
			},
		},
	}
	resolved := []map[uint32]*targetRelation{
		{withoutPrimary.source.OID: withoutPrimary, left.source.OID: left},
		{withoutPrimary.source.OID: withoutPrimary, right.source.OID: right},
	}
	plan, err := buildReplayPlan("stream", "generation", 30, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || len(plan.Steps[0].Lanes) != 1 ||
		!slices.Equal(plan.Steps[0].Lanes[0].TransactionIndexes, []int{0, 1}) {
		t.Fatalf("relation lane did not preserve multi-table transaction order: %#v", plan.Steps)
	}
}

func TestReplayPlanUpdateWithUnchangedToastStillShardsByStablePrimaryKey(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(46, "toast_items")
	oldTuple := replayTuple("stable", "old")
	newTuple := Tuple{
		{Kind: DatumText, Data: []byte("stable")},
		{Kind: DatumUnchangedToast},
	}
	transaction := replayTestTransaction(500, relation, Change{
		RelationOID: relation.source.OID, Kind: ChangeUpdate, Old: oldTuple, New: &newTuple,
	})
	resolved := []map[uint32]*targetRelation{{relation.source.OID: relation}}
	plan, err := buildReplayPlan("stream", "generation", 40, 8, []Transaction{transaction}, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].SerialTransaction >= 0 || len(plan.Steps[0].Lanes) != 1 {
		t.Fatalf("non-key unchanged TOAST created a serial barrier: %#v", plan.Steps)
	}

	legacy, err := buildReplayPlanForGenerationVersion(
		"stream", "generation", "generation", 40, 8,
		[]Transaction{transaction}, resolved, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.Steps) != 1 || legacy.Steps[0].SerialTransaction != 0 {
		t.Fatalf("plan v2 no longer reconstructs its legacy TOAST barrier: %#v", legacy.Steps)
	}
}

func TestReplayPlanDoesNotShardChangedPrimaryKeyUpdate(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(51, "changed_key_items")
	transactions := []Transaction{
		replayTestTransaction(510, relation, Change{
			RelationOID: relation.source.OID, Kind: ChangeUpdate,
			Old: replayTuple("before-a", "value"), New: replayTuple("after-a", "value"),
		}),
		replayTestTransaction(512, relation, Change{
			RelationOID: relation.source.OID, Kind: ChangeUpdate,
			Old: replayTuple("before-b", "value"), New: replayTuple("after-b", "value"),
		}),
	}
	plan, err := buildReplayPlan(
		"stream", "generation", 40, 8, transactions,
		[]map[uint32]*targetRelation{
			{relation.source.OID: relation}, {relation.source.OID: relation},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || len(plan.Steps[0].Lanes) != 1 ||
		!slices.Equal(plan.Steps[0].Lanes[0].TransactionIndexes, []int{0, 1}) {
		t.Fatalf("changed primary keys escaped table-local source order: %#v", plan.Steps)
	}
}

func TestReplayUpdateWithoutOldTupleRequiresReplicaIdentityPrimaryKey(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(58, "nil_old_items")
	fingerprint := targetRelationReplayFingerprint(relation)
	left := Change{
		RelationOID: relation.source.OID, Kind: ChangeUpdate,
		New: replayTuple("left", "one"),
	}
	right := Change{
		RelationOID: relation.source.OID, Kind: ChangeUpdate,
		New: replayTuple("right", "two"),
	}
	leftKey, leftSafe, err := replayChangeKey(relation, fingerprint, &left)
	if err != nil {
		t.Fatal(err)
	}
	rightKey, rightSafe, err := replayChangeKey(relation, fingerprint, &right)
	if err != nil {
		t.Fatal(err)
	}
	if !leftSafe || !rightSafe || leftKey == rightKey {
		t.Fatal("replica-identity primary keys did not shard nil-old updates independently")
	}

	alternateIdentity := replayTestRelation(59, "alternate_identity_items")
	alternateIdentity.source.Columns[0].Flags = 0
	alternateIdentity.columns[0].key = false
	alternateFingerprint := targetRelationReplayFingerprint(alternateIdentity)
	left.RelationOID = alternateIdentity.source.OID
	right.RelationOID = alternateIdentity.source.OID
	leftKey, leftSafe, err = replayChangeKey(alternateIdentity, alternateFingerprint, &left)
	if err != nil {
		t.Fatal(err)
	}
	rightKey, rightSafe, err = replayChangeKey(alternateIdentity, alternateFingerprint, &right)
	if err != nil {
		t.Fatal(err)
	}
	if !leftSafe || !rightSafe || leftKey != rightKey {
		t.Fatal("alternate replica identity did not fall back to one table-local lane")
	}
}

func TestReplayPlanV2FingerprintIgnoresV3RelationLaneCapability(t *testing.T) {
	t.Parallel()
	left := replayTestRelation(52, "compat_items")
	right := replayTestRelation(52, "compat_items")
	right.capabilities.relationOrderedLane = false
	right.capabilities.primaryKeyArbiter = false
	if targetRelationReplayFingerprintVersion(left, 2) !=
		targetRelationReplayFingerprintVersion(right, 2) {
		t.Fatal("plan v2 fingerprint included a plan v3 capability")
	}
	if targetRelationReplayFingerprintVersion(left, 3) ==
		targetRelationReplayFingerprintVersion(right, 3) {
		t.Fatal("plan v3 fingerprint omitted relation-ordered lane safety")
	}
}

func TestFreshFragmentedReplayPlanUsesBoundedOrderedFallback(t *testing.T) {
	t.Parallel()
	safe := replayTestRelation(53, "safe_items")
	barrier := replayTestRelation(54, "barrier_items")
	barrier.capabilities.relationLane = false
	barrier.capabilities.relationOrderedLane = false
	transactions := []Transaction{
		replayTestTransaction(520, safe, Change{
			RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("a", "one"),
		}),
		replayTestTransaction(522, barrier, Change{
			RelationOID: barrier.source.OID, Kind: ChangeInsert, New: replayTuple("b", "two"),
		}),
		replayTestTransaction(524, safe, Change{
			RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("c", "three"),
		}),
	}
	resolved := []map[uint32]*targetRelation{
		{safe.source.OID: safe}, {barrier.source.OID: barrier}, {safe.source.OID: safe},
	}
	plan, err := buildReplayPlan("stream", "generation", 40, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 3 || !replayPlanHasSerialWork(plan) {
		t.Fatalf("fixture is not fragmented parallel/serial work: %#v", plan.Steps)
	}
	if shouldUseConcurrentReplayPlan(nil, plan) {
		t.Fatal("fresh fragmented plan would create a multi-commit concurrent claim")
	}
	resume := plan.Claim
	if !shouldUseConcurrentReplayPlan(&resume, plan) {
		t.Fatal("existing exact claim would not resume its durable manifest")
	}
}

func TestReplayPlanWorkTotalsCoverClaimExactly(t *testing.T) {
	t.Parallel()
	safe := replayTestRelation(49, "safe_items")
	unsafe := replayTestRelation(50, "unsafe_items")
	unsafe.capabilities.crossKeyConflicts = true
	transactions := []Transaction{
		replayTestTransaction(600, safe,
			Change{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("a", "one")},
			Change{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("b", "two")},
		),
		replayTestTransaction(602, safe,
			Change{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("c", "three")},
		),
		replayTestTransaction(604, unsafe,
			Change{RelationOID: unsafe.source.OID, Kind: ChangeInsert, New: replayTuple("d", "four")},
			Change{RelationOID: unsafe.source.OID, Kind: ChangeInsert, New: replayTuple("e", "five")},
			Change{RelationOID: unsafe.source.OID, Kind: ChangeInsert, New: replayTuple("f", "six")},
		),
		replayTestTransaction(606, safe,
			Change{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("g", "seven")},
		),
		replayTestTransaction(608, safe,
			Change{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("h", "eight")},
			Change{RelationOID: safe.source.OID, Kind: ChangeInsert, New: replayTuple("i", "nine")},
		),
	}
	resolved := []map[uint32]*targetRelation{
		{safe.source.OID: safe},
		{safe.source.OID: safe},
		{unsafe.source.OID: unsafe},
		{safe.source.OID: safe},
		{safe.source.OID: safe},
	}
	plan, err := buildReplayPlan("stream", "generation", 60, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Claim.Transactions != 5 || plan.Claim.Changes != 9 ||
		plan.Claim.ExpectedWork != len(plan.Works) {
		t.Fatalf("claim totals=(%d,%d,%d), want (5,9,%d)", plan.Claim.Transactions, plan.Claim.Changes, plan.Claim.ExpectedWork, len(plan.Works))
	}
	if got := replayPlanWorkTransactions(plan.Works); got != plan.Claim.Transactions {
		t.Fatalf("work transactions=%d, claim=%d", got, plan.Claim.Transactions)
	}
	if got := replayPlanWorkChanges(plan.Works); got != plan.Claim.Changes {
		t.Fatalf("work changes=%d, claim=%d", got, plan.Claim.Changes)
	}

	covered := make([]int, len(transactions))
	var coveredChanges int64
	coveredWorks := 0
	for _, step := range plan.Steps {
		if step.SerialTransaction >= 0 {
			transactionIndex := step.SerialTransaction
			work, exists := replayPlanWork(plan, step.Index, 0)
			if !exists || work.ExpectedTransactions != 1 ||
				work.ExpectedChanges != int64(transactions[transactionIndex].ChangeCount()) {
				t.Fatalf("serial step %d work=%#v exists=%t", step.Index, work, exists)
			}
			covered[transactionIndex]++
			coveredChanges += int64(transactions[transactionIndex].ChangeCount())
			coveredWorks++
			continue
		}
		for _, lane := range step.Lanes {
			var laneChanges int64
			for _, transactionIndex := range lane.TransactionIndexes {
				covered[transactionIndex]++
				laneChanges += int64(transactions[transactionIndex].ChangeCount())
			}
			if lane.Work.ExpectedTransactions != int64(len(lane.TransactionIndexes)) ||
				lane.Work.ExpectedChanges != laneChanges {
				t.Fatalf("step %d lane %d totals=(%d,%d), want (%d,%d)", step.Index, lane.Lane, lane.Work.ExpectedTransactions, lane.Work.ExpectedChanges, len(lane.TransactionIndexes), laneChanges)
			}
			coveredChanges += laneChanges
			coveredWorks++
		}
	}
	if coveredWorks != len(plan.Works) || coveredChanges != plan.Claim.Changes {
		t.Fatalf("traversed work=(%d,%d), claim=(%d,%d)", coveredWorks, coveredChanges, len(plan.Works), plan.Claim.Changes)
	}
	for transactionIndex, count := range covered {
		if count != 1 {
			t.Fatalf("transaction %d covered %d times, want once", transactionIndex, count)
		}
	}
}

func TestReplayPlanSerializesPrimaryKeyChanges(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(45, "items")
	transaction := replayTestTransaction(400, relation, Change{
		RelationOID: relation.source.OID, Kind: ChangeUpdate,
		Old: replayTuple("old-id", "value"), New: replayTuple("new-id", "value"),
	})
	plan, err := buildReplayPlan(
		"stream", "generation", 40, 8,
		[]Transaction{transaction}, []map[uint32]*targetRelation{{relation.source.OID: relation}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].SerialTransaction >= 0 ||
		len(plan.Steps[0].Lanes) != 1 || len(plan.Works) != 1 ||
		plan.Works[0].Kind != replayWorkParallelLane {
		t.Fatalf("primary key change did not use one relation-ordered lane: %#v", plan)
	}
}

func TestReplayPlanBindsTargetCatalogFingerprint(t *testing.T) {
	t.Parallel()
	relation := replayTestRelation(46, "items")
	transaction := replayTestTransaction(500, relation, Change{
		RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("id", "value"),
	})
	first, err := buildReplayPlan(
		"stream", "generation", 50, 8,
		[]Transaction{transaction}, []map[uint32]*targetRelation{{relation.source.OID: relation}},
	)
	if err != nil {
		t.Fatal(err)
	}
	relation.columns[1].oid = 1043
	second, err := buildReplayPlan(
		"stream", "generation", 50, 8,
		[]Transaction{transaction}, []map[uint32]*targetRelation{{relation.source.OID: relation}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Claim.CatalogDigest == second.Claim.CatalogDigest ||
		first.Claim.Digest == second.Claim.Digest {
		t.Fatal("target catalog change did not change the durable claim identity")
	}
}

func replayTestRelation(oid uint32, name string) *targetRelation {
	source := Relation{
		OID: oid, Namespace: "public", Name: name, ReplicaIdentity: 'd',
		Columns: []Column{
			{Name: "id", Type: 25, Flags: 1},
			{Name: "value", Type: 25},
		},
	}
	return &targetRelation{
		source: source, quoted: `"public"."` + name + `"`,
		capabilities: targetRelationCapabilities{
			relationLane: true, relationOrderedLane: true,
			primaryKeyArbiter: true, keyedSetDML: true,
			binaryCopy: true, textCopyStage: true,
		},
		columns: []targetColumn{
			{
				name: "id", quoted: `"id"`, oid: 25, arrayOID: 1009, key: true,
				primary: true, primaryPos: 1, sourceIndex: 0, notNull: true, replayKeySafe: true,
			},
			{name: "value", quoted: `"value"`, oid: 25, arrayOID: 1009, sourceIndex: 1},
		},
	}
}

func replayTestTransaction(lsn LSN, relation *targetRelation, changes ...Change) Transaction {
	return Transaction{
		CommitLSN: lsn, EndLSN: lsn + 1, CommitTime: time.Unix(int64(lsn), 0).UTC(),
		Relations: []Relation{relation.source}, Changes: changes,
	}
}

func replayTuple(id, value string) *Tuple {
	tuple := Tuple{
		{Kind: DatumText, Data: []byte(id)},
		{Kind: DatumText, Data: []byte(value)},
	}
	return &tuple
}
