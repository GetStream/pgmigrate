package cdc

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"slices"

	"github.com/jackc/pgx/v5/pgtype"
)

type replayPlan struct {
	Claim       replayClaim
	Works       []replayClaimWork
	Steps       []replayPlanStep
	HasParallel bool
}

type replayPlanStep struct {
	Index             int
	Lanes             []replayPlanLane
	SerialTransaction int
}

type replayPlanLane struct {
	Lane               int
	TransactionIndexes []int
	Items              []relationBatchedChange
	Work               replayClaimWork
}

func replayPlanHasSerialWork(plan replayPlan) bool {
	for _, step := range plan.Steps {
		if step.SerialTransaction >= 0 {
			return true
		}
	}
	return false
}

type replayPlanTransaction struct {
	index int
	keys  [][sha256.Size]byte
	items []relationBatchedChange
}

func buildReplayPlan(
	streamID, generation string,
	startLSN LSN,
	laneCount int,
	transactions []Transaction,
	relations []map[uint32]*targetRelation,
) (replayPlan, error) {
	return buildReplayPlanForGeneration(
		streamID, generation, generation, startLSN, laneCount, transactions, relations,
	)
}

func buildReplayPlanForGeneration(
	streamID, generation, startGeneration string,
	startLSN LSN,
	laneCount int,
	transactions []Transaction,
	relations []map[uint32]*targetRelation,
) (replayPlan, error) {
	return buildReplayPlanForGenerationVersion(
		streamID, generation, startGeneration, startLSN, laneCount,
		transactions, relations, replayClaimPlanVersion,
	)
}

func buildReplayPlanForGenerationVersion(
	streamID, generation, startGeneration string,
	startLSN LSN,
	laneCount int,
	transactions []Transaction,
	relations []map[uint32]*targetRelation,
	planVersion int,
) (replayPlan, error) {
	if planVersion < replayClaimMinimumPlanVersion || planVersion > replayClaimPlanVersion {
		return replayPlan{}, fmt.Errorf("cdc: unsupported replay claim plan version %d", planVersion)
	}
	if streamID == "" || generation == "" || startGeneration == "" ||
		laneCount < 1 || len(transactions) == 0 ||
		len(relations) != len(transactions) {
		return replayPlan{}, errors.New("cdc: invalid replay plan input")
	}

	plan := replayPlan{}
	currentEpoch := make([]replayPlanTransaction, 0, len(transactions))
	lastDefinitions := make(map[uint32][sha256.Size]byte)
	relationFingerprints := make(map[*targetRelation][sha256.Size]byte)
	catalogHasher := newReplayClaimHasher("pgmigrate-replay-catalog-v1")
	fingerprintFor := func(relation *targetRelation) [sha256.Size]byte {
		if fingerprint, exists := relationFingerprints[relation]; exists {
			return fingerprint
		}
		fingerprint := targetRelationReplayFingerprintVersion(relation, planVersion)
		relationFingerprints[relation] = fingerprint
		return fingerprint
	}

	flushParallel := func() error {
		if len(currentEpoch) == 0 {
			return nil
		}
		lanes, err := replayTransactionComponentLanes(currentEpoch, laneCount)
		if err != nil {
			return err
		}
		step := replayPlanStep{Index: len(plan.Steps), SerialTransaction: -1}
		for lane := range lanes {
			if len(lanes[lane].TransactionIndexes) == 0 {
				continue
			}
			work, err := parallelReplayWork(
				step.Index, lane, lanes[lane].TransactionIndexes,
				transactions, relations, relationFingerprints,
			)
			if err != nil {
				return err
			}
			lanes[lane].Work = work
			step.Lanes = append(step.Lanes, lanes[lane])
			plan.Works = append(plan.Works, work)
		}
		plan.HasParallel = plan.HasParallel || len(step.Lanes) > 1
		plan.Steps = append(plan.Steps, step)
		currentEpoch = currentEpoch[:0]
		return nil
	}

	for transactionIndex := range transactions {
		transaction := &transactions[transactionIndex]
		resolved := relations[transactionIndex]
		definitionChanged := false
		for relationIndex := range transaction.Relations {
			source := &transaction.Relations[relationIndex]
			target := resolved[source.OID]
			if target == nil {
				return replayPlan{}, divergenceFor(nil, 0, "required relation metadata is missing")
			}
			fingerprint := fingerprintFor(target)
			writeReplayHashInt(catalogHasher, int64(transactionIndex))
			writeReplayHashInt(catalogHasher, int64(relationIndex))
			writeReplayHashBytes(catalogHasher, fingerprint[:])
			if previous, exists := lastDefinitions[source.OID]; exists && previous != fingerprint {
				definitionChanged = true
			}
		}

		planned := replayPlanTransaction{index: transactionIndex}
		parallelSafe := transaction.Spill == nil && !definitionChanged
		if parallelSafe {
			for changeIndex := range transaction.Changes {
				change := &transaction.Changes[changeIndex]
				target := resolved[change.RelationOID]
				key, safe, err := replayChangeKeyForVersion(
					planVersion, target, fingerprintFor(target), change,
				)
				if err != nil {
					return replayPlan{}, err
				}
				if !safe {
					parallelSafe = false
					break
				}
				planned.keys = append(planned.keys, key)
				planned.items = append(planned.items, relationBatchedChange{
					transactionIndex: transactionIndex,
					changeIndex:      changeIndex,
					change:           change,
					relation:         target,
				})
			}
		}

		if parallelSafe {
			// Empty source transactions still advance progress and need an exact
			// durable receipt. Give them a unique synthetic dependency key so they
			// remain indivisible without serializing the rest of the epoch.
			if len(planned.keys) == 0 {
				hasher := newReplayClaimHasher("pgmigrate-replay-empty-transaction-v1")
				writeReplayHashInt(hasher, int64(transaction.EndLSN))
				planned.keys = append(planned.keys, finishReplayHash(hasher))
			}
			currentEpoch = append(currentEpoch, planned)
		} else {
			if err := flushParallel(); err != nil {
				return replayPlan{}, err
			}
			work, err := serialReplayWork(len(plan.Steps), transactionIndex, transaction)
			if err != nil {
				return replayPlan{}, err
			}
			plan.Steps = append(plan.Steps, replayPlanStep{
				Index: len(plan.Steps), SerialTransaction: transactionIndex,
			})
			plan.Works = append(plan.Works, work)
		}

		for relationIndex := range transaction.Relations {
			source := &transaction.Relations[relationIndex]
			lastDefinitions[source.OID] = fingerprintFor(resolved[source.OID])
		}
	}
	if err := flushParallel(); err != nil {
		return replayPlan{}, err
	}

	var transactionsApplied, changesApplied int64
	for i := range transactions {
		transactionsApplied++
		changesApplied += int64(transactions[i].ChangeCount())
	}
	if changesApplied != replayPlanWorkChanges(plan.Works) {
		return replayPlan{}, fmt.Errorf(
			"cdc: replay plan covers %d changes, expected %d",
			replayPlanWorkChanges(plan.Works), changesApplied,
		)
	}
	if transactionsApplied != replayPlanWorkTransactions(plan.Works) {
		return replayPlan{}, fmt.Errorf(
			"cdc: replay plan covers %d transactions, expected %d",
			replayPlanWorkTransactions(plan.Works), transactionsApplied,
		)
	}

	claim := replayClaim{
		StreamID:        streamID,
		Generation:      generation,
		StartGeneration: startGeneration,
		StartLSN:        startLSN,
		EndLSN:          transactions[len(transactions)-1].EndLSN,
		CatalogDigest:   finishReplayHash(catalogHasher),
		PlanVersion:     planVersion,
		LaneCount:       laneCount,
		Transactions:    transactionsApplied,
		Changes:         changesApplied,
		ExpectedWork:    len(plan.Works),
	}
	digest, err := replayPlanDigest(claim, transactions, plan.Works)
	if err != nil {
		return replayPlan{}, err
	}
	claim.Digest = digest
	claim.ID = replayClaimID(claim.Digest)
	claim.FenceGeneration = replayFenceGeneration(claim.Generation, claim.ID)
	plan.Claim = claim
	return plan, nil
}

// replayTransactionComponentLanes schedules complete source transactions.
// Transactions sharing any target primary key are unioned into one connected
// component and therefore one lane. This preserves per-key source order while
// retaining parallelism between genuinely independent components. A source
// transaction is never split across target commits.
func replayTransactionComponentLanes(
	epoch []replayPlanTransaction,
	laneCount int,
) ([]replayPlanLane, error) {
	if len(epoch) == 0 || laneCount < 1 {
		return nil, errors.New("cdc: invalid replay transaction component input")
	}
	parents := make([]int, len(epoch))
	ranks := make([]byte, len(epoch))
	for i := range parents {
		parents[i] = i
	}
	var find func(int) int
	find = func(value int) int {
		if parents[value] != value {
			parents[value] = find(parents[value])
		}
		return parents[value]
	}
	union := func(left, right int) {
		left, right = find(left), find(right)
		if left == right {
			return
		}
		if ranks[left] < ranks[right] {
			left, right = right, left
		}
		parents[right] = left
		if ranks[left] == ranks[right] {
			ranks[left]++
		}
	}
	firstByKey := make(map[[sha256.Size]byte]int)
	for transactionIndex := range epoch {
		if len(epoch[transactionIndex].keys) == 0 {
			return nil, errors.New("cdc: replay transaction has no dependency key")
		}
		for _, key := range epoch[transactionIndex].keys {
			if first, exists := firstByKey[key]; exists {
				union(first, transactionIndex)
			} else {
				firstByKey[key] = transactionIndex
			}
		}
	}

	componentKeys := make(map[int][][sha256.Size]byte)
	for transactionIndex := range epoch {
		root := find(transactionIndex)
		componentKeys[root] = append(componentKeys[root], epoch[transactionIndex].keys...)
	}
	componentLanes := make(map[int]int, len(componentKeys))
	for root, keys := range componentKeys {
		slices.SortFunc(keys, func(left, right [sha256.Size]byte) int {
			return bytes.Compare(left[:], right[:])
		})
		hasher := newReplayClaimHasher("pgmigrate-replay-transaction-component-v1")
		var previous [sha256.Size]byte
		for index, key := range keys {
			if index != 0 && key == previous {
				continue
			}
			writeReplayHashBytes(hasher, key[:])
			previous = key
		}
		digest := finishReplayHash(hasher)
		componentLanes[root] = int(binary.BigEndian.Uint64(digest[:8]) % uint64(laneCount))
	}

	lanes := make([]replayPlanLane, laneCount)
	for lane := range lanes {
		lanes[lane].Lane = lane
	}
	// Iterate transactions, not components, so hash collisions cannot reorder
	// otherwise-independent source transactions assigned to the same lane.
	for transactionIndex := range epoch {
		lane := componentLanes[find(transactionIndex)]
		lanes[lane].TransactionIndexes = append(
			lanes[lane].TransactionIndexes, epoch[transactionIndex].index,
		)
		lanes[lane].Items = append(lanes[lane].Items, epoch[transactionIndex].items...)
	}
	return lanes, nil
}

func replayPlanWorkChanges(works []replayClaimWork) int64 {
	var result int64
	for _, work := range works {
		result += work.ExpectedChanges
	}
	return result
}

func replayPlanWorkTransactions(works []replayClaimWork) int64 {
	var result int64
	for _, work := range works {
		result += work.ExpectedTransactions
	}
	return result
}

func replayChangeKeyForVersion(
	planVersion int,
	relation *targetRelation,
	relationFingerprint [sha256.Size]byte,
	change *Change,
) ([sha256.Size]byte, bool, error) {
	if planVersion == 2 {
		return replayChangeKeyV2(relation, relationFingerprint, change)
	}
	return replayChangeKey(relation, relationFingerprint, change)
}

// replayChangeKeyV2 reconstructs claims written by v58 exactly. Keep this
// frozen until every plan-version-2 claim has been finalized: a rolling restart
// may otherwise reinterpret an in-flight claim and either reject safe resume or
// repeat already committed work.
func replayChangeKeyV2(
	relation *targetRelation,
	relationFingerprint [sha256.Size]byte,
	change *Change,
) ([sha256.Size]byte, bool, error) {
	if relation == nil || change == nil ||
		!relation.capabilities.relationLane || relation.capabilities.crossKeyConflicts {
		return [sha256.Size]byte{}, false, nil
	}
	if !replayLanePayloadSafe(relation, change) {
		return [sha256.Size]byte{}, false, nil
	}
	primary := primaryKeyColumns(relation)
	if len(primary) == 0 {
		return [sha256.Size]byte{}, false, nil
	}
	for _, column := range primary {
		if !column.replayKeySafe {
			return [sha256.Size]byte{}, false, nil
		}
	}

	var tuple *Tuple
	switch change.Kind {
	case ChangeInsert:
		tuple = change.New
		if err := validateTuple(relation, tuple, ChangeInsert); err != nil {
			return [sha256.Size]byte{}, false, err
		}
	case ChangeUpdate:
		if !canPrimaryKeyUpsertV2(relation, change) {
			return [sha256.Size]byte{}, false, nil
		}
		tuple = change.New
	case ChangeDelete:
		deletePrimary, safe := primaryKeyDeleteColumns(relation)
		if !safe || !sameTargetColumns(primary, deletePrimary) {
			return [sha256.Size]byte{}, false, nil
		}
		tuple = change.Old
		if err := validateTuple(relation, tuple, ChangeDelete); err != nil {
			return [sha256.Size]byte{}, false, err
		}
	default:
		return [sha256.Size]byte{}, false, nil
	}
	if tuple == nil {
		return [sha256.Size]byte{}, false, nil
	}

	hasher := newReplayClaimHasher("pgmigrate-replay-lane-v1")
	writeReplayHashBytes(hasher, relationFingerprint[:])
	for _, column := range primary {
		if column.sourceIndex < 0 || column.sourceIndex >= len(*tuple) {
			return [sha256.Size]byte{}, false, nil
		}
		datum := (*tuple)[column.sourceIndex]
		if datum.Kind == DatumNull || datum.Kind == DatumUnchangedToast {
			return [sha256.Size]byte{}, false, nil
		}
		if !replayKeyDatumSafe(column.oid, datum.Kind) {
			return [sha256.Size]byte{}, false, nil
		}
		if _, err := datumParamForColumn(relation, column, datum, change.Kind); err != nil {
			return [sha256.Size]byte{}, false, err
		}
		writeReplayHashInt(hasher, int64(datum.Kind))
		writeReplayHashBytes(hasher, datum.Data)
	}
	return finishReplayHash(hasher), true, nil
}

func replayChangeKey(
	relation *targetRelation,
	relationFingerprint [sha256.Size]byte,
	change *Change,
) ([sha256.Size]byte, bool, error) {
	if relation == nil || change == nil || !relation.capabilities.relationOrderedLane {
		return [sha256.Size]byte{}, false, nil
	}
	if !replayLanePayloadSafe(relation, change) {
		return [sha256.Size]byte{}, false, nil
	}
	var tuple *Tuple
	switch change.Kind {
	case ChangeInsert:
		tuple = change.New
		if err := validateTuple(relation, tuple, ChangeInsert); err != nil {
			return [sha256.Size]byte{}, false, err
		}
	case ChangeUpdate:
		tuple = change.New
		if err := validateTuple(relation, tuple, ChangeUpdate); err != nil {
			return [sha256.Size]byte{}, false, err
		}
	case ChangeDelete:
		tuple = change.Old
		if err := validateTuple(relation, tuple, ChangeDelete); err != nil {
			return [sha256.Size]byte{}, false, err
		}
	default:
		return [sha256.Size]byte{}, false, nil
	}

	// A non-primary UNIQUE/exclusion index can make different primary-key rows
	// conflict, and some otherwise-side-effect-free relations do not expose a
	// canonical primary key at all. Give every such write the same table key.
	// This serializes that table in source order while still allowing unrelated
	// tables to run concurrently. Multi-table source transactions carry every
	// relation key and are unioned atomically by the component planner.
	if !relation.capabilities.relationLane || relation.capabilities.crossKeyConflicts {
		return replayRelationLaneKey(relation, relationFingerprint)
	}

	primary := primaryKeyColumns(relation)
	if len(primary) == 0 {
		return [sha256.Size]byte{}, false, nil
	}
	for _, column := range primary {
		if !column.replayKeySafe {
			return [sha256.Size]byte{}, false, nil
		}
	}

	switch change.Kind {
	case ChangeUpdate:
		// Ordering eligibility depends only on a present, stable primary key.
		// Non-key UnchangedToast values select a different DML shape but cannot
		// make two primary-key rows conflict, so they must not create a global
		// serial barrier.
		if !canShardUpdateByPrimaryKey(relation, change) {
			return replayRelationLaneKey(relation, relationFingerprint)
		}
	case ChangeDelete:
		deletePrimary, safe := primaryKeyDeleteColumns(relation)
		if !safe || !sameTargetColumns(primary, deletePrimary) {
			return replayRelationLaneKey(relation, relationFingerprint)
		}
	}
	if tuple == nil {
		return [sha256.Size]byte{}, false, nil
	}

	hasher := newReplayClaimHasher("pgmigrate-replay-lane-v1")
	writeReplayHashBytes(hasher, relationFingerprint[:])
	for _, column := range primary {
		if column.sourceIndex < 0 || column.sourceIndex >= len(*tuple) {
			return [sha256.Size]byte{}, false, nil
		}
		datum := (*tuple)[column.sourceIndex]
		if datum.Kind == DatumNull || datum.Kind == DatumUnchangedToast {
			return [sha256.Size]byte{}, false, nil
		}
		if !replayKeyDatumSafe(column.oid, datum.Kind) {
			return [sha256.Size]byte{}, false, nil
		}
		if _, err := datumParamForColumn(relation, column, datum, change.Kind); err != nil {
			return [sha256.Size]byte{}, false, err
		}
		writeReplayHashInt(hasher, int64(datum.Kind))
		writeReplayHashBytes(hasher, datum.Data)
	}
	return finishReplayHash(hasher), true, nil
}

func replayRelationLaneKey(
	relation *targetRelation,
	relationFingerprint [sha256.Size]byte,
) ([sha256.Size]byte, bool, error) {
	if relation == nil || !relation.capabilities.relationOrderedLane {
		return [sha256.Size]byte{}, false, nil
	}
	hasher := newReplayClaimHasher("pgmigrate-replay-relation-lane-v1")
	writeReplayHashBytes(hasher, relationFingerprint[:])
	return finishReplayHash(hasher), true, nil
}

func canShardUpdateByPrimaryKey(relation *targetRelation, change *Change) bool {
	if relation == nil || change == nil || change.New == nil ||
		len(*change.New) != len(relation.source.Columns) {
		return false
	}
	primary := primaryKeyColumns(relation)
	if len(primary) == 0 {
		return false
	}
	for _, column := range primary {
		if column.sourceIndex < 0 || column.sourceIndex >= len(*change.New) {
			return false
		}
		newDatum := (*change.New)[column.sourceIndex]
		if newDatum.Kind == DatumNull || newDatum.Kind == DatumUnchangedToast {
			return false
		}
	}
	if change.Old == nil {
		identifiedPrimary, safe := primaryKeyDeleteColumns(relation)
		return safe && sameTargetColumns(primary, identifiedPrimary)
	}
	if len(*change.Old) != len(relation.source.Columns) {
		return false
	}
	for _, column := range primary {
		oldDatum := (*change.Old)[column.sourceIndex]
		newDatum := (*change.New)[column.sourceIndex]
		if oldDatum.Kind == DatumNull || oldDatum.Kind == DatumUnchangedToast ||
			!tupleDatumEqual(oldDatum, newDatum) {
			return false
		}
	}
	return true
}

func replayLanePayloadSafe(relation *targetRelation, change *Change) bool {
	if change.Kind == ChangeDelete {
		return true
	}
	if change.New == nil {
		return false
	}
	for _, column := range relation.columns {
		if !column.lanePayloadTextOnly {
			continue
		}
		if column.sourceIndex < 0 || column.sourceIndex >= len(*change.New) {
			return false
		}
		switch (*change.New)[column.sourceIndex].Kind {
		case DatumText, DatumNull:
		case DatumUnchangedToast:
			if change.Kind != ChangeUpdate {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// replayKeyDatumSafe requires the captured bytes to be a canonical
// representative of PostgreSQL equality. Built-in binary encodings are
// canonical for every type admitted by replayKeyTargetTypeSafe. Text output is
// deliberately narrower because bytea_output, DateStyle, and TimeZone can
// change across capture reconnects without changing the represented key.
func replayKeyDatumSafe(oid uint32, kind DatumKind) bool {
	if !replayKeyTargetTypeSafe(oid) {
		return false
	}
	if kind == DatumBinary {
		return true
	}
	if kind != DatumText {
		return false
	}
	switch oid {
	case pgtype.BoolOID,
		pgtype.Int2OID,
		pgtype.Int4OID,
		pgtype.Int8OID,
		pgtype.TextOID,
		pgtype.VarcharOID,
		pgtype.UUIDOID:
		return true
	default:
		return false
	}
}

func sameTargetColumns(left, right []targetColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].name != right[i].name || left[i].sourceIndex != right[i].sourceIndex {
			return false
		}
	}
	return true
}

func parallelReplayWork(
	step, lane int,
	transactionIndexes []int,
	transactions []Transaction,
	relations []map[uint32]*targetRelation,
	relationFingerprints map[*targetRelation][sha256.Size]byte,
) (replayClaimWork, error) {
	hasher := newReplayClaimHasher("pgmigrate-replay-parallel-work-v1")
	writeReplayHashInt(hasher, int64(step))
	writeReplayHashInt(hasher, int64(lane))
	var changes int64
	previous := -1
	for _, transactionIndex := range transactionIndexes {
		if transactionIndex <= previous || transactionIndex < 0 ||
			transactionIndex >= len(transactions) || transactionIndex >= len(relations) {
			return replayClaimWork{}, errors.New("cdc: parallel replay work has invalid transaction order")
		}
		previous = transactionIndex
		transaction := &transactions[transactionIndex]
		writeReplayHashInt(hasher, int64(transactionIndex))
		writeReplayHashInt(hasher, int64(transaction.EndLSN))
		for relationIndex := range transaction.Relations {
			target := relations[transactionIndex][transaction.Relations[relationIndex].OID]
			fingerprint, exists := relationFingerprints[target]
			if target == nil || !exists {
				return replayClaimWork{}, errors.New("cdc: parallel replay relation fingerprint is missing")
			}
			writeReplayHashBytes(hasher, fingerprint[:])
		}
		size, err := transactionPayloadSize(transaction)
		if err != nil {
			return replayClaimWork{}, err
		}
		writeReplayHashInt(hasher, int64(size))
		if _, err := WriteTransaction(hasher, transaction); err != nil {
			return replayClaimWork{}, err
		}
		changes += int64(transaction.ChangeCount())
	}
	return replayClaimWork{
		Step: step, Work: lane, Kind: replayWorkParallelLane, Lane: lane,
		Digest: finishReplayHash(hasher), ExpectedTransactions: int64(len(transactionIndexes)),
		ExpectedChanges: changes,
	}, nil
}

func serialReplayWork(
	step, transactionIndex int,
	transaction *Transaction,
) (replayClaimWork, error) {
	hasher := newReplayClaimHasher("pgmigrate-replay-serial-work-v1")
	writeReplayHashInt(hasher, int64(step))
	writeReplayHashInt(hasher, int64(transactionIndex))
	size, err := transactionPayloadSize(transaction)
	if err != nil {
		return replayClaimWork{}, err
	}
	writeReplayHashInt(hasher, int64(size))
	if _, err := WriteTransaction(hasher, transaction); err != nil {
		return replayClaimWork{}, err
	}
	return replayClaimWork{
		Step: step, Work: 0, Kind: replayWorkSerial, Lane: -1,
		Digest: finishReplayHash(hasher), ExpectedTransactions: 1,
		ExpectedChanges: int64(transaction.ChangeCount()),
	}, nil
}

func replayPlanDigest(
	claim replayClaim,
	transactions []Transaction,
	works []replayClaimWork,
) ([sha256.Size]byte, error) {
	hasher := newReplayClaimHasher("pgmigrate-replay-claim-v1")
	writeReplayHashBytes(hasher, []byte(claim.StreamID))
	writeReplayHashBytes(hasher, []byte(claim.Generation))
	writeReplayHashBytes(hasher, []byte(claim.StartGeneration))
	writeReplayHashInt(hasher, int64(claim.StartLSN))
	writeReplayHashInt(hasher, int64(claim.EndLSN))
	writeReplayHashInt(hasher, int64(claim.PlanVersion))
	writeReplayHashInt(hasher, int64(claim.LaneCount))
	writeReplayHashInt(hasher, claim.Transactions)
	writeReplayHashInt(hasher, claim.Changes)
	writeReplayHashBytes(hasher, claim.CatalogDigest[:])
	for i := range transactions {
		size, err := transactionPayloadSize(&transactions[i])
		if err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("cdc: hash replay transaction %d size: %w", i, err)
		}
		writeReplayHashInt(hasher, int64(size))
		if _, err := WriteTransaction(hasher, &transactions[i]); err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("cdc: hash replay transaction %d: %w", i, err)
		}
	}
	for _, work := range works {
		writeReplayHashInt(hasher, int64(work.Step))
		writeReplayHashInt(hasher, int64(work.Work))
		writeReplayHashBytes(hasher, []byte(work.Kind))
		writeReplayHashInt(hasher, int64(work.Lane))
		writeReplayHashBytes(hasher, work.Digest[:])
		writeReplayHashInt(hasher, work.ExpectedTransactions)
		writeReplayHashInt(hasher, work.ExpectedChanges)
	}
	return finishReplayHash(hasher), nil
}

func targetRelationReplayFingerprint(relation *targetRelation) [sha256.Size]byte {
	return targetRelationReplayFingerprintVersion(relation, replayClaimPlanVersion)
}

func targetRelationReplayFingerprintVersion(
	relation *targetRelation,
	planVersion int,
) [sha256.Size]byte {
	hasher := newReplayClaimHasher("pgmigrate-target-relation-v1")
	if relation == nil {
		return finishReplayHash(hasher)
	}
	writeReplayHashInt(hasher, int64(relation.source.OID))
	writeReplayHashBytes(hasher, []byte(relation.source.Namespace))
	writeReplayHashBytes(hasher, []byte(relation.source.Name))
	writeReplayHashInt(hasher, int64(relation.source.ReplicaIdentity))
	for _, column := range relation.source.Columns {
		writeReplayHashBytes(hasher, []byte(column.Name))
		writeReplayHashInt(hasher, int64(column.Type))
		writeReplayHashInt(hasher, int64(column.Flags))
	}
	writeReplayHashBool(hasher, relation.overrideIdentity)
	writeReplayHashBool(hasher, relation.capabilities.relationLane)
	if planVersion >= 3 {
		writeReplayHashBool(hasher, relation.capabilities.relationOrderedLane)
		writeReplayHashBool(hasher, relation.capabilities.primaryKeyArbiter)
	}
	writeReplayHashBool(hasher, relation.capabilities.keyedSetDML)
	writeReplayHashBool(hasher, relation.capabilities.binaryCopy)
	writeReplayHashBool(hasher, relation.capabilities.textCopyStage)
	writeReplayHashBool(hasher, relation.capabilities.selectiveUpdates)
	writeReplayHashBool(hasher, relation.capabilities.crossKeyConflicts)
	for _, column := range relation.columns {
		writeTargetColumnFingerprint(hasher, column)
	}
	writeReplayHashBytes(hasher, []byte("generated"))
	for _, column := range relation.generatedColumns {
		writeTargetColumnFingerprint(hasher, column)
	}
	return finishReplayHash(hasher)
}

func writeTargetColumnFingerprint(hasher hash.Hash, column targetColumn) {
	writeReplayHashBytes(hasher, []byte(column.name))
	writeReplayHashInt(hasher, int64(column.oid))
	writeReplayHashInt(hasher, int64(column.arrayOID))
	writeReplayHashBool(hasher, column.key)
	writeReplayHashBool(hasher, column.primary)
	writeReplayHashInt(hasher, int64(column.primaryPos))
	writeReplayHashBool(hasher, column.replayKeySafe)
	writeReplayHashBool(hasher, column.lanePayloadTextOnly)
	writeReplayHashBytes(hasher, []byte(column.identity))
	writeReplayHashInt(hasher, int64(column.sourceIndex))
	writeReplayHashBool(hasher, column.generated)
	writeReplayHashBool(hasher, column.notNull)
	writeReplayHashBool(hasher, column.conflicting)
}

func writeReplayHashBool(hasher hash.Hash, value bool) {
	if value {
		writeReplayHashInt(hasher, 1)
		return
	}
	writeReplayHashInt(hasher, 0)
}

func replayPlanWork(plan replayPlan, step, work int) (replayClaimWork, bool) {
	index := slices.IndexFunc(plan.Works, func(candidate replayClaimWork) bool {
		return candidate.Step == step && candidate.Work == work
	})
	if index < 0 {
		return replayClaimWork{}, false
	}
	return plan.Works[index], true
}
