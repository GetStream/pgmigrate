//go:build integration

package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestPG17ReplayClaimResumesExactLaneReceiptsAndFinalizesOnce(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `
		CREATE TABLE public.claim_items (
			id text PRIMARY KEY,
			value text NOT NULL
		)
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "parallel-claim-resume"
	const generation = "parallel-claim-resume-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation,
		FreshSetup: true, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ensureReplayClaimTables(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}

	relation := replayTestRelation(9_001, "claim_items")
	transactions := make([]Transaction, 96)
	resolved := make([]map[uint32]*targetRelation, len(transactions))
	loaded, err := loadTargetRelation(ctx, control, &relation.source)
	if err != nil {
		t.Fatal(err)
	}
	for i := range transactions {
		transactions[i] = replayTestTransaction(
			LSN(1_000+i*2), relation,
			Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: replayTuple(fmt.Sprintf("id-%03d-a", i), fmt.Sprintf("value-%03d-a", i)),
			},
			Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: replayTuple(fmt.Sprintf("id-%03d-b", i), fmt.Sprintf("value-%03d-b", i)),
			},
		)
		resolved[i] = map[uint32]*targetRelation{relation.source.OID: loaded}
	}
	plan, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Claim.PlanVersion != 2 {
		t.Fatalf("legacy resume fixture plan version=%d, want 2", plan.Claim.PlanVersion)
	}
	if !plan.HasParallel || len(plan.Works) < 2 {
		t.Fatalf("fixture did not produce parallel work: %#v", plan.Steps)
	}
	claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
	if err != nil {
		t.Fatal(err)
	}
	plan.Claim = claim

	var fencedGeneration string
	if err := control.QueryRow(
		ctx, "SELECT stream_generation FROM "+streamIdentityTable+" WHERE stream_id=$1", streamID,
	).Scan(&fencedGeneration); err != nil {
		t.Fatal(err)
	}
	if fencedGeneration != claim.FenceGeneration || fencedGeneration == generation {
		t.Fatalf("active claim generation=%q base=%q fence=%q", fencedGeneration, generation, claim.FenceGeneration)
	}
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation,
		FreshSetup: false, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatalf("claim-aware restart rejected exact fence: %v", err)
	}

	workers, err := openApplyWorkers(
		ctx, control, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	var committed atomic.Int32
	interrupted := errors.New("test: stop after a committed lane")
	applier := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
		afterReplayWork: func(replayClaim, replayClaimWork) error {
			if committed.Add(1) == 1 {
				return interrupted
			}
			return nil
		},
	}}
	err = applier.executeReplayPlan(ctx, workers, plan, transactions, resolved)
	closeApplyWorkers(workers[1:])
	if !errors.Is(err, interrupted) {
		t.Fatalf("first replay interruption = %v, want %v", err, interrupted)
	}
	assertReplayProgress(t, control, streamID, 0, 0, 0)
	storedWorks, err := readReplayClaimWorks(ctx, control, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	committedWorks := 0
	for _, work := range storedWorks {
		if work.CommittedAt != nil {
			committedWorks++
		}
	}
	if committedWorks == 0 {
		t.Fatal("interrupted replay committed no exact lane receipt")
	}
	// A committed lane may expose some complete source transactions while the
	// claim is active, but never half of one source transaction.
	for i := range transactions {
		var pairRows int
		if err := control.QueryRow(ctx, `
			SELECT count(*)
			FROM public.claim_items
			WHERE id IN ($1, $2)
		`, fmt.Sprintf("id-%03d-a", i), fmt.Sprintf("id-%03d-b", i)).Scan(&pairRows); err != nil {
			t.Fatal(err)
		}
		if pairRows != 0 && pairRows != 2 {
			t.Fatalf("source transaction %d committed only %d/2 rows", i, pairRows)
		}
	}
	reconstructed, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replayClaimsEqual(reconstructed.Claim, claim) ||
		!slices.Equal(reconstructed.Works, plan.Works) {
		t.Fatal("new executor did not reconstruct the exact active plan-version-2 claim")
	}
	reconstructed.Claim = claim
	plan = reconstructed

	// A fresh process with fewer physical workers must reconstruct the same
	// logical lane plan, skip exact committed INSERT lanes, finish the rest, and
	// remain restartable after every lane is complete but before public progress.
	secondControl, err := postgres.Connect(ctx, target.URI)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, secondControl); err != nil {
		t.Fatal(err)
	}
	secondWorkers, err := openApplyWorkers(
		ctx, secondControl, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	beforeFinalize := errors.New("test: stop before claim finalization")
	second := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
		beforeReplayFinalize: func(replayClaim) error {
			return beforeFinalize
		},
	}}
	err = second.executeReplayPlan(ctx, secondWorkers, plan, transactions, resolved)
	closeApplyWorkers(secondWorkers[1:])
	secondControl.Close(context.Background())
	if !errors.Is(err, beforeFinalize) {
		t.Fatalf("second replay interruption = %v, want %v", err, beforeFinalize)
	}
	assertReplayProgress(t, control, streamID, 0, 0, 0)
	var rowsBeforeFinalize int
	if err := control.QueryRow(ctx, "SELECT count(*) FROM public.claim_items").Scan(&rowsBeforeFinalize); err != nil {
		t.Fatal(err)
	}
	if rowsBeforeFinalize != len(transactions)*2 {
		t.Fatalf("rows before finalization=%d, want %d", rowsBeforeFinalize, len(transactions)*2)
	}

	thirdControl, err := postgres.Connect(ctx, target.URI)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, thirdControl); err != nil {
		t.Fatal(err)
	}
	thirdWorkers := []*applyWorker{{
		conn: thirdControl, statements: newApplyStatementCache(applyStatementCacheCapacity),
	}}
	third := &Applier{config: ApplierConfig{StreamID: streamID, StreamGeneration: generation}}
	if err := third.executeReplayPlan(ctx, thirdWorkers, plan, transactions, resolved); err != nil {
		t.Fatal(err)
	}
	thirdControl.Close(context.Background())

	assertReplayProgress(
		t, control, streamID, plan.Claim.EndLSN,
		int64(len(transactions)), int64(len(transactions)*2),
	)
	if _, exists, err := readReplayClaim(ctx, control, streamID); err != nil || exists {
		t.Fatalf("finalized claim exists=%t err=%v", exists, err)
	}
	var baseGeneration string
	if err := control.QueryRow(ctx, `
		SELECT base_generation, stream_generation
		FROM `+streamIdentityTable+`
		WHERE stream_id=$1
	`, streamID).Scan(&baseGeneration, &fencedGeneration); err != nil {
		t.Fatal(err)
	}
	if baseGeneration != generation || fencedGeneration != claim.FenceGeneration ||
		fencedGeneration == generation {
		t.Fatalf(
			"final stream base/effective=%q/%q, want %q/%q",
			baseGeneration, fencedGeneration, generation, claim.FenceGeneration,
		)
	}
	for i := range transactions {
		for _, suffix := range []string{"a", "b"} {
			var value string
			if err := control.QueryRow(
				ctx, "SELECT value FROM public.claim_items WHERE id=$1",
				fmt.Sprintf("id-%03d-%s", i, suffix),
			).Scan(&value); err != nil {
				t.Fatalf("read row %d/%s: %v", i, suffix, err)
			}
			if want := fmt.Sprintf("value-%03d-%s", i, suffix); value != want {
				t.Fatalf("row %d/%s value=%q, want %q", i, suffix, value, want)
			}
		}
	}
}

func TestPG17ReplayClaimResumesCoalescedBinaryStageWithoutRepeatingDML(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `
		CREATE TABLE public.coalesced_binary_items (
			id bigint PRIMARY KEY,
			value bigint NOT NULL
		);
		INSERT INTO public.coalesced_binary_items
		SELECT id, 0 FROM generate_series(1, 128) AS id
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "coalesced-binary-resume"
	const generation = "coalesced-binary-resume-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation,
		FreshSetup: true, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatal(err)
	}
	source := Relation{
		OID: 9_301, Namespace: "public", Name: "coalesced_binary_items", ReplicaIdentity: 'd',
		Columns: []Column{
			{Name: "id", Type: pgtype.Int8OID, Flags: 1},
			{Name: "value", Type: pgtype.Int8OID},
		},
	}
	loaded, err := loadTargetRelation(ctx, control, &source)
	if err != nil {
		t.Fatal(err)
	}
	binaryInt8 := func(value int64) TupleDatum {
		return TupleDatum{Kind: DatumBinary, Data: binary.BigEndian.AppendUint64(nil, uint64(value))}
	}
	transactions := make([]Transaction, 0, 256)
	resolved := make([]map[uint32]*targetRelation, 0, 256)
	for round := int64(0); round < 2; round++ {
		for id := int64(1); id <= 128; id++ {
			oldTuple := Tuple{binaryInt8(id), binaryInt8(round)}
			newTuple := Tuple{binaryInt8(id), binaryInt8(round + 1)}
			endLSN := LSN(len(transactions) + 1)
			transactions = append(transactions, Transaction{
				CommitLSN: endLSN, EndLSN: endLSN,
				Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: &oldTuple, New: &newTuple,
				}},
			})
			resolved = append(resolved, map[uint32]*targetRelation{source.OID: loaded})
		}
	}
	plan, err := buildReplayPlan(streamID, generation, 0, 1, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
	if err != nil {
		t.Fatal(err)
	}
	plan.Claim = claim
	interrupted := errors.New("test: stop after coalesced binary lane commit")
	applier := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
		afterReplayWork: func(replayClaim, replayClaimWork) error { return interrupted },
	}}
	cache := newApplyStatementCache(applyStatementCacheCapacity)
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}
	workers := []*applyWorker{{conn: control, statements: cache}}
	if err := applier.executeReplayPlan(ctx, workers, plan, transactions, resolved); !errors.Is(err, interrupted) {
		t.Fatalf("interrupted replay error = %v, want %v", err, interrupted)
	}
	assertReplayProgress(t, control, streamID, 0, 0, 0)
	var finalRows int
	if err := control.QueryRow(ctx, `
		SELECT count(*) FROM public.coalesced_binary_items WHERE value = 2
	`).Scan(&finalRows); err != nil {
		t.Fatal(err)
	}
	if finalRows != 128 {
		t.Fatalf("coalesced binary DML produced %d final rows, want 128", finalRows)
	}

	applier.config.afterReplayWork = nil
	if err := applier.executeReplayPlan(ctx, workers, plan, transactions, resolved); err != nil {
		t.Fatal(err)
	}
	assertReplayProgress(t, control, streamID, claim.EndLSN, 256, 256)
	if err := control.QueryRow(ctx, `
		SELECT count(*) FROM public.coalesced_binary_items WHERE value = 2
	`).Scan(&finalRows); err != nil {
		t.Fatal(err)
	}
	if finalRows != 128 {
		t.Fatalf("resumed replay repeated binary DML; final rows = %d", finalRows)
	}
}

func TestPG17ReplayClaimV3ReconstructsAfterV4CatalogTightening(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `
		CREATE FUNCTION pg_catalog.claim_v3_catalog_predicate(value text)
			RETURNS boolean LANGUAGE sql IMMUTABLE
			RETURN value <> '';
		CREATE TABLE public.claim_v3_items (
			id text PRIMARY KEY,
			value text NOT NULL
		);
		CREATE INDEX claim_v3_items_partial
			ON public.claim_v3_items (value)
			WHERE pg_catalog.claim_v3_catalog_predicate(value)
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "plan-v3-catalog-resume"
	const generation = "plan-v3-catalog-resume-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation,
		FreshSetup: true, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ensureReplayClaimTables(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}

	relation := replayTestRelation(9_005, "claim_v3_items")
	loaded, err := loadTargetRelation(ctx, control, &relation.source)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.capabilities.relationLane || loaded.capabilities.relationOrderedLane ||
		!loaded.capabilities.relationOrderedLaneV3 {
		t.Fatalf("v3 compatibility fixture capabilities=%+v", loaded.capabilities)
	}

	const transactionCount = 64
	transactions := make([]Transaction, transactionCount)
	resolved := make([]map[uint32]*targetRelation, transactionCount)
	for index := range transactions {
		transactions[index] = replayTestTransaction(
			LSN(4_000+index*2), relation,
			Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: replayTuple(fmt.Sprintf("id-%03d-a", index), fmt.Sprintf("value-%03d-a", index)),
			},
			Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: replayTuple(fmt.Sprintf("id-%03d-b", index), fmt.Sprintf("value-%03d-b", index)),
			},
		)
		resolved[index] = map[uint32]*targetRelation{relation.source.OID: loaded}
	}
	plan, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 3,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Claim.PlanVersion != 3 || !plan.HasParallel || replayPlanHasSerialWork(plan) {
		t.Fatalf("v3 compatibility fixture plan=%#v claim=%+v", plan.Steps, plan.Claim)
	}
	freshV4, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replayPlanHasSerialWork(freshV4) {
		t.Fatal("v4 did not tighten the custom pg_catalog index dependency")
	}

	claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
	if err != nil {
		t.Fatal(err)
	}
	plan.Claim = claim
	workers, err := openApplyWorkers(
		ctx, control, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 4,
	)
	if err != nil {
		t.Fatal(err)
	}

	interrupted := errors.New("test: interrupt plan-v3 claim after one lane")
	var committed atomic.Int32
	first := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
		afterReplayWork: func(replayClaim, replayClaimWork) error {
			if committed.Add(1) == 1 {
				return interrupted
			}
			return nil
		},
	}}
	if err := first.executeReplayPlan(
		ctx, workers, plan, transactions, resolved,
	); !errors.Is(err, interrupted) {
		t.Fatalf("plan-v3 interruption=%v, want %v", err, interrupted)
	}
	closeApplyWorkers(workers[1:])
	assertReplayProgress(t, control, streamID, 0, 0, 0)
	for index := range transactions {
		var pairRows int
		if err := control.QueryRow(ctx, `
			SELECT count(*) FROM public.claim_v3_items WHERE id IN ($1, $2)
		`, fmt.Sprintf("id-%03d-a", index), fmt.Sprintf("id-%03d-b", index)).Scan(&pairRows); err != nil {
			t.Fatal(err)
		}
		if pairRows != 0 && pairRows != 2 {
			t.Fatalf("plan-v3 source transaction %d exposed %d/2 rows", index, pairRows)
		}
	}

	reconstructed, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 3,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replayClaimsEqual(reconstructed.Claim, claim) ||
		!slices.Equal(reconstructed.Works, plan.Works) {
		t.Fatal("v4 binary did not reconstruct the exact active plan-version-3 claim")
	}
	reconstructed.Claim = claim
	resumeControl, err := postgres.Connect(ctx, target.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer resumeControl.Close(context.Background())
	if err := configureApplySession(ctx, resumeControl); err != nil {
		t.Fatal(err)
	}
	resumeWorkers, err := openApplyWorkers(
		ctx, resumeControl, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 3,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeApplyWorkers(resumeWorkers[1:])
	resumed := &Applier{config: ApplierConfig{StreamID: streamID, StreamGeneration: generation}}
	if err := resumed.executeReplayPlan(
		ctx, resumeWorkers, reconstructed, transactions, resolved,
	); err != nil {
		t.Fatal(err)
	}
	assertReplayProgress(
		t, control, streamID, claim.EndLSN, transactionCount, transactionCount*2,
	)
	var rows int
	if err := control.QueryRow(ctx, "SELECT count(*) FROM public.claim_v3_items").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != transactionCount*2 {
		t.Fatalf("plan-v3 resumed rows=%d, want %d", rows, transactionCount*2)
	}
	if _, exists, err := readReplayClaim(ctx, control, streamID); err != nil || exists {
		t.Fatalf("finalized plan-v3 claim exists=%t err=%v", exists, err)
	}
}

func TestPG17ReplayClaimV4ReconstructsAfterV5UnaccentAdmission(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `CREATE SCHEMA claim_v5_unaccent`); err != nil {
		t.Fatalf("create unaccent fixture schema: %v", err)
	}
	if _, err := control.Exec(ctx,
		`CREATE EXTENSION unaccent WITH SCHEMA claim_v5_unaccent`,
	); err != nil {
		t.Skipf("server lacks relocatable unaccent support: %v", err)
	}
	if _, err := control.Exec(ctx, `
		CREATE TEXT SEARCH CONFIGURATION public.claim_v5_unaccent_config
			(COPY = pg_catalog.simple);
		ALTER TEXT SEARCH CONFIGURATION public.claim_v5_unaccent_config
			ALTER MAPPING FOR word, hword, hword_part
			WITH claim_v5_unaccent.unaccent, pg_catalog.simple;
		CREATE TABLE public.claim_v4_items (
			id text PRIMARY KEY,
			value text NOT NULL
		);
		CREATE INDEX claim_v4_items_search
			ON public.claim_v4_items USING gin
			(to_tsvector('public.claim_v5_unaccent_config'::regconfig, value));
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "plan-v4-unaccent-resume"
	const generation = "plan-v4-unaccent-resume-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation,
		FreshSetup: true, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ensureReplayClaimTables(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}

	relation := replayTestRelation(9_006, "claim_v4_items")
	loaded, err := loadTargetRelation(ctx, control, &relation.source)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.capabilities.relationLane ||
		!loaded.capabilities.relationOrderedLane ||
		loaded.capabilities.relationOrderedLaneV4 {
		t.Fatalf("v4 compatibility fixture capabilities=%+v", loaded.capabilities)
	}

	const transactionCount = 32
	transactions := make([]Transaction, transactionCount)
	resolved := make([]map[uint32]*targetRelation, transactionCount)
	for index := range transactions {
		transactions[index] = replayTestTransaction(
			LSN(5_000+index*2), relation,
			Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: replayTuple(fmt.Sprintf("id-%03d", index), fmt.Sprintf("value-%03d", index)),
			},
		)
		resolved[index] = map[uint32]*targetRelation{relation.source.OID: loaded}
	}
	plan, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Claim.PlanVersion != 4 || !replayPlanHasSerialWork(plan) {
		t.Fatalf("v4 compatibility fixture plan=%#v claim=%+v", plan.Steps, plan.Claim)
	}
	freshV5, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 5,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !freshV5.HasParallel || replayPlanHasSerialWork(freshV5) {
		t.Fatalf("v5 did not admit trusted unaccent lanes: %#v", freshV5.Steps)
	}

	claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
	if err != nil {
		t.Fatal(err)
	}
	plan.Claim = claim
	workers, err := openApplyWorkers(
		ctx, control, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	interrupted := errors.New("test: interrupt plan-v4 claim after durable serial group")
	first := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
		afterReplayWork: func(replayClaim, replayClaimWork) error {
			return interrupted
		},
	}}
	if err := first.executeReplayPlan(
		ctx, workers, plan, transactions, resolved,
	); !errors.Is(err, interrupted) {
		t.Fatalf("plan-v4 interruption=%v, want %v", err, interrupted)
	}
	closeApplyWorkers(workers[1:])
	assertReplayProgress(t, control, streamID, 0, 0, 0)

	reconstructed, err := buildReplayPlanForGenerationVersion(
		streamID, generation, generation, 0, 8, transactions, resolved, 4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !replayClaimsEqual(reconstructed.Claim, claim) ||
		!slices.Equal(reconstructed.Works, plan.Works) {
		t.Fatal("v5 binary did not reconstruct the exact active plan-version-4 claim")
	}
	reconstructed.Claim = claim
	resumeControl, err := postgres.Connect(ctx, target.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer resumeControl.Close(context.Background())
	if err := configureApplySession(ctx, resumeControl); err != nil {
		t.Fatal(err)
	}
	resumeWorkers, err := openApplyWorkers(
		ctx, resumeControl, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 3,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeApplyWorkers(resumeWorkers[1:])
	resumed := &Applier{config: ApplierConfig{StreamID: streamID, StreamGeneration: generation}}
	if err := resumed.executeReplayPlan(
		ctx, resumeWorkers, reconstructed, transactions, resolved,
	); err != nil {
		t.Fatal(err)
	}
	assertReplayProgress(t, control, streamID, claim.EndLSN, transactionCount, transactionCount)
	var rows int
	if err := control.QueryRow(ctx, "SELECT count(*) FROM public.claim_v4_items").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != transactionCount {
		t.Fatalf("plan-v4 resumed rows=%d, want %d", rows, transactionCount)
	}
	if _, exists, err := readReplayClaim(ctx, control, streamID); err != nil || exists {
		t.Fatalf("finalized plan-v4 claim exists=%t err=%v", exists, err)
	}
}

func TestPG17ReplayClaimCommitsContiguousSerialWorkAndReceiptsAtomically(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `
		CREATE FUNCTION public.serial_claim_check(value text)
			RETURNS boolean LANGUAGE sql IMMUTABLE
			RETURN value <> '';
		CREATE TABLE public.serial_claim_items (
			id text PRIMARY KEY,
			value text NOT NULL CHECK (public.serial_claim_check(value))
		)
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "serial-claim-group-resume"
	const generation = "serial-claim-group-resume-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation,
		FreshSetup: true, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := ensureReplayClaimTables(ctx, control); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}

	relation := replayTestRelation(9_004, "serial_claim_items")
	loaded, err := loadTargetRelation(ctx, control, &relation.source)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.capabilities.relationLane || loaded.capabilities.relationOrderedLane {
		t.Fatal("custom CHECK-constrained fixture unexpectedly admitted parallel replay")
	}
	const transactionCount = 128
	transactions := make([]Transaction, transactionCount)
	resolved := make([]map[uint32]*targetRelation, transactionCount)
	for index := range transactions {
		transactions[index] = replayTestTransaction(
			LSN(2_000+index*2), relation,
			Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: replayTuple(fmt.Sprintf("id-%03d", index), fmt.Sprintf("value-%03d", index)),
			},
		)
		resolved[index] = map[uint32]*targetRelation{relation.source.OID: loaded}
	}
	plan, err := buildReplayPlan(streamID, generation, 0, 8, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != transactionCount || len(plan.Works) != transactionCount ||
		!replayPlanHasSerialWork(plan) {
		t.Fatalf("fixture plan is not one contiguous serial run: steps=%d works=%d", len(plan.Steps), len(plan.Works))
	}
	claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
	if err != nil {
		t.Fatal(err)
	}
	plan.Claim = claim
	lastWork := plan.Works[len(plan.Works)-1]
	if _, err := control.Exec(ctx, `
		UPDATE `+replayClaimWorkTable+`
		SET committed_at = clock_timestamp()
		WHERE claim_id = $1 AND step_index = $2 AND work_index = $3
	`, claim.ID, lastWork.Step, lastWork.Work); err != nil {
		t.Fatal(err)
	}
	if _, err := beginSerialReplayWorkGroup(ctx, control, claim, plan.Works); err == nil {
		t.Fatal("serial replay accepted a committed suffix after an uncommitted gap")
	}
	if _, err := control.Exec(ctx, `
		UPDATE `+replayClaimWorkTable+`
		SET committed_at = NULL
		WHERE claim_id = $1 AND step_index = $2 AND work_index = $3
	`, claim.ID, lastWork.Step, lastWork.Work); err != nil {
		t.Fatal(err)
	}

	// Model a v58 interruption after its first per-transaction receipt. The new
	// grouped executor must retain that exact prefix and atomically commit only
	// the uncommitted suffix.
	firstWork := plan.Works[0]
	firstTransaction := plan.Steps[0].SerialTransaction
	predecessor := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
	}}
	firstWorker := &applyWorker{
		conn: control, statements: newApplyStatementCache(applyStatementCacheCapacity),
	}
	if err := predecessor.executeReplayWork(
		ctx, firstWorker, claim, firstWork,
		func(replay *applyPipeline) error {
			return predecessor.queueTransactionChanges(
				replay, resolved[firstTransaction], &transactions[firstTransaction], nil,
			)
		},
	); err != nil {
		t.Fatal(err)
	}

	interrupted := errors.New("test: stop after atomically committed serial group")
	var callbacks atomic.Int32
	applier := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
		afterReplayWork: func(replayClaim, replayClaimWork) error {
			if callbacks.Add(1) == 1 {
				return interrupted
			}
			return nil
		},
	}}
	workers := []*applyWorker{firstWorker}
	if err := applier.executeReplayPlan(ctx, workers, plan, transactions, resolved); !errors.Is(err, interrupted) {
		t.Fatalf("serial replay interruption=%v, want %v", err, interrupted)
	}
	assertReplayProgress(t, control, streamID, 0, 0, 0)
	storedWorks, err := readReplayClaimWorks(ctx, control, claim.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, work := range storedWorks {
		if work.CommittedAt == nil {
			t.Fatalf("serial work %d/%d was not committed with its group", work.Step, work.Work)
		}
	}
	var visibleRows int
	if err := control.QueryRow(ctx, "SELECT count(*) FROM public.serial_claim_items").Scan(&visibleRows); err != nil {
		t.Fatal(err)
	}
	if visibleRows != transactionCount {
		t.Fatalf("serial group exposed %d rows, want %d", visibleRows, transactionCount)
	}

	// A fresh executor skips every exact receipt and only publishes the durable
	// claim totals. No DML is repeated after the post-commit interruption.
	resumed := &Applier{config: ApplierConfig{StreamID: streamID, StreamGeneration: generation}}
	if err := resumed.executeReplayPlan(ctx, workers, plan, transactions, resolved); err != nil {
		t.Fatal(err)
	}
	assertReplayProgress(t, control, streamID, plan.Claim.EndLSN, transactionCount, transactionCount)
	if err := control.QueryRow(ctx, "SELECT count(*) FROM public.serial_claim_items").Scan(&visibleRows); err != nil {
		t.Fatal(err)
	}
	if visibleRows != transactionCount {
		t.Fatalf("resumed serial group exposed %d rows, want %d", visibleRows, transactionCount)
	}
}

func TestPG17ReplayClaimAllowsCustomNonKeyPayloads(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `
		CREATE TYPE public.claim_mood AS ENUM ('calm', 'fast');
		CREATE DOMAIN public.claim_guarded_text AS text CHECK (VALUE <> 'blocked');
		CREATE TABLE public.claim_custom_payload (
			id bigint PRIMARY KEY,
			mood public.claim_mood NOT NULL,
			moods public.claim_mood[] NOT NULL,
			note text NOT NULL
		);
		CREATE TABLE public.claim_custom_domain (
			id bigint PRIMARY KEY,
			value public.claim_guarded_text NOT NULL
		);
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "parallel-custom-non-key"
	const generation = "parallel-custom-non-key-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation, FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}

	// The enum OID intentionally models a source catalog OID that differs from
	// the target. Text pgoutput values are bound with the target column OID; only
	// the canonical built-in bigint primary key participates in lane hashing.
	source := Relation{
		OID: 9_201, Namespace: "public", Name: "claim_custom_payload", ReplicaIdentity: 'd',
		Columns: []Column{
			{Name: "id", Type: pgtype.Int8OID, Flags: 1},
			{Name: "mood", Type: 99_902},
			{Name: "moods", Type: 99_905},
			{Name: "note", Type: pgtype.TextOID},
		},
	}
	loaded, err := loadTargetRelation(ctx, control, &source)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.capabilities.relationLane || loaded.capabilities.binaryCopy ||
		!loaded.capabilities.textCopyStage {
		t.Fatalf("custom non-key capabilities=%+v", loaded.capabilities)
	}
	if !loaded.columns[0].replayKeySafe || loaded.columns[1].replayKeySafe ||
		loaded.columns[2].replayKeySafe {
		t.Fatalf("custom non-key replay key columns=%+v", loaded.columns)
	}
	domainSource := Relation{
		OID: 9_202, Namespace: "public", Name: "claim_custom_domain", ReplicaIdentity: 'd',
		Columns: []Column{
			{Name: "id", Type: pgtype.Int8OID, Flags: 1},
			{Name: "value", Type: 99_904},
		},
	}
	domain, err := loadTargetRelation(ctx, control, &domainSource)
	if err != nil {
		t.Fatal(err)
	}
	if domain.capabilities.relationLane {
		t.Fatalf("custom domain was admitted to a replay lane: %+v", domain.capabilities)
	}

	// With two logical lanes, 256 rows put at least 64 enum values in each lane
	// and exercise the target-typed COPY stage concurrently.
	transactions := make([]Transaction, 256)
	resolved := make([]map[uint32]*targetRelation, len(transactions))
	for i := range transactions {
		mood := "calm"
		if i%2 != 0 {
			mood = "fast"
		}
		value := Tuple{
			{Kind: DatumText, Data: []byte(fmt.Sprintf("%d", i+1))},
			{Kind: DatumText, Data: []byte(mood)},
			{Kind: DatumText, Data: []byte("{calm,fast}")},
			{Kind: DatumText, Data: []byte(fmt.Sprintf("note-%03d", i))},
		}
		transactions[i] = Transaction{
			CommitLSN: LSN(2_000 + i*2), EndLSN: LSN(2_001 + i*2),
			Relations: []Relation{source},
			Changes: []Change{{
				RelationOID: source.OID, Kind: ChangeInsert, New: &value,
			}},
		}
		resolved[i] = map[uint32]*targetRelation{source.OID: loaded}
	}
	plan, err := buildReplayPlan(streamID, generation, 0, 2, transactions, resolved)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.HasParallel || replayPlanHasSerialWork(plan) {
		t.Fatalf("custom non-key plan was not parallel: %#v", plan.Steps)
	}
	if len(plan.Steps) != 1 || len(plan.Steps[0].Lanes) != 2 ||
		plan.Steps[0].Lanes[0].Work.ExpectedChanges < 64 ||
		plan.Steps[0].Lanes[1].Work.ExpectedChanges < 64 {
		t.Fatalf("custom non-key fixture did not exercise two COPY-stage lanes: %#v", plan.Steps)
	}
	claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
	if err != nil {
		t.Fatal(err)
	}
	plan.Claim = claim
	workers, err := openApplyWorkers(
		ctx, control, newApplyStatementCache(applyStatementCacheCapacity), target.URI, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeApplyWorkers(workers[1:])
	applier := &Applier{config: ApplierConfig{
		StreamID: streamID, StreamGeneration: generation,
	}}
	if err := applier.executeReplayPlan(ctx, workers, plan, transactions, resolved); err != nil {
		t.Fatal(err)
	}

	var rows, fast, arrays int
	if err := control.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE mood = 'fast'),
		       count(*) FILTER (
		         WHERE moods = ARRAY['calm', 'fast']::public.claim_mood[]
		       )
		FROM public.claim_custom_payload
	`).Scan(&rows, &fast, &arrays); err != nil {
		t.Fatal(err)
	}
	if rows != len(transactions) || fast != len(transactions)/2 || arrays != len(transactions) {
		t.Fatalf("custom payload rows=%d fast=%d arrays=%d, want %d/%d/%d",
			rows, fast, arrays, len(transactions), len(transactions)/2, len(transactions))
	}
	assertReplayProgress(
		t, control, streamID, plan.Claim.EndLSN,
		int64(len(transactions)), int64(len(transactions)),
	)
}

func TestPG17ReplayClaimGenerationFenceIsMonotonic(t *testing.T) {
	target := pgtest.Start(t, 17)
	control := target.Connect(t)
	ctx := context.Background()
	if _, err := control.Exec(ctx, `
		CREATE TABLE public.fence_items (
			id text PRIMARY KEY,
			value text NOT NULL
		)
	`); err != nil {
		t.Fatal(err)
	}

	const streamID = "monotonic-replay-fence"
	const generation = "monotonic-replay-fence-v1"
	if err := EnsureStreamProgressIdentity(ctx, control, StreamIdentityConfig{
		StreamID: streamID, Generation: generation, FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, control); err != nil {
		t.Fatal(err)
	}
	relation := replayTestRelation(9_101, "fence_items")
	loaded, err := loadTargetRelation(ctx, control, &relation.source)
	if err != nil {
		t.Fatal(err)
	}
	controlStatements := newApplyStatementCache(applyStatementCacheCapacity)

	applyClaim := func(start LSN, startGeneration, prefix string) replayClaim {
		t.Helper()
		transactions := make([]Transaction, 64)
		resolved := make([]map[uint32]*targetRelation, len(transactions))
		for i := range transactions {
			transactions[i] = replayTestTransaction(
				start+LSN(i*2)+1, relation, Change{
					RelationOID: relation.source.OID, Kind: ChangeInsert,
					New: replayTuple(
						fmt.Sprintf("%s-%03d", prefix, i),
						fmt.Sprintf("value-%s-%03d", prefix, i),
					),
				},
			)
			resolved[i] = map[uint32]*targetRelation{relation.source.OID: loaded}
		}
		plan, err := buildReplayPlanForGeneration(
			streamID, generation, startGeneration, start, 8, transactions, resolved,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !plan.HasParallel {
			t.Fatal("monotonic fence fixture did not produce parallel work")
		}
		claim, err := ensureReplayClaim(ctx, control, plan.Claim, plan.Works)
		if err != nil {
			t.Fatal(err)
		}
		plan.Claim = claim
		workers, err := openApplyWorkers(
			ctx, control, controlStatements, target.URI, 4,
		)
		if err != nil {
			t.Fatal(err)
		}
		applier := &Applier{config: ApplierConfig{
			StreamID: streamID, StreamGeneration: generation,
		}}
		if err := applier.executeReplayPlan(ctx, workers, plan, transactions, resolved); err != nil {
			closeApplyWorkers(workers[1:])
			t.Fatal(err)
		}
		closeApplyWorkers(workers[1:])
		return claim
	}

	// This transaction starts under the configured generation before the first
	// claim creates its fence, and deliberately avoids touching claim rows.
	legacyConn, err := postgres.Connect(ctx, target.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer legacyConn.Close(context.Background())
	legacyTx, err := legacyConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacyTx.Exec(ctx, `
		INSERT INTO public.fence_items (id, value) VALUES ('stale-base', 'must-roll-back')
	`); err != nil {
		t.Fatal(err)
	}
	first := applyClaim(0, generation, "first")
	if err := updateStreamProgress(
		ctx, legacyTx, streamID, generation, 0, first.EndLSN+100, 1, 1,
	); !errors.Is(err, ErrStreamGenerationMismatch) {
		_ = legacyTx.Rollback(ctx)
		t.Fatalf("stale configured-generation progress error=%v", err)
	}
	_ = legacyTx.Rollback(ctx)

	// A transaction started with the first claim's completed token must also be
	// permanently fenced by the second claim; no effective token is ever reused.
	previousConn, err := postgres.Connect(ctx, target.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer previousConn.Close(context.Background())
	previousTx, err := previousConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := previousTx.Exec(ctx, `
		INSERT INTO public.fence_items (id, value) VALUES ('stale-previous', 'must-roll-back')
	`); err != nil {
		t.Fatal(err)
	}
	second := applyClaim(first.EndLSN, first.FenceGeneration, "second")
	if second.FenceGeneration == first.FenceGeneration || second.FenceGeneration == generation {
		t.Fatalf("replay fence was reused: base=%q first=%q second=%q",
			generation, first.FenceGeneration, second.FenceGeneration)
	}
	if err := updateStreamProgress(
		ctx, previousTx, streamID, first.FenceGeneration, first.EndLSN, second.EndLSN+100, 1, 1,
	); !errors.Is(err, ErrStreamGenerationMismatch) {
		_ = previousTx.Rollback(ctx)
		t.Fatalf("stale previous-generation progress error=%v", err)
	}
	_ = previousTx.Rollback(ctx)

	var staleRows int
	if err := control.QueryRow(ctx, `
		SELECT count(*) FROM public.fence_items
		WHERE id IN ('stale-base', 'stale-previous')
	`).Scan(&staleRows); err != nil {
		t.Fatal(err)
	}
	if staleRows != 0 {
		t.Fatalf("%d stale pre-fence rows committed", staleRows)
	}
	assertReplayProgress(t, control, streamID, second.EndLSN, 128, 128)
	var base, effective, progressGeneration string
	if err := control.QueryRow(ctx, `
		SELECT identity.base_generation, identity.stream_generation,
		       progress.stream_generation
		FROM `+streamIdentityTable+` AS identity
		JOIN `+cdcProgressTable+` AS progress USING (stream_id)
		WHERE identity.stream_id = $1
	`, streamID).Scan(&base, &effective, &progressGeneration); err != nil {
		t.Fatal(err)
	}
	if base != generation || effective != second.FenceGeneration ||
		progressGeneration != second.FenceGeneration {
		t.Fatalf("final generations base/effective/progress=%q/%q/%q, want %q/%q/%q",
			base, effective, progressGeneration,
			generation, second.FenceGeneration, second.FenceGeneration)
	}
}

func assertReplayProgress(
	t *testing.T,
	conn *pgx.Conn,
	streamID string,
	wantLSN LSN,
	wantTransactions, wantRows int64,
) {
	t.Helper()
	progress, exists, err := postgres.ReadReplicationProgress(context.Background(), conn, streamID)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || LSN(progress.RemoteLSN) != wantLSN ||
		progress.Transactions != wantTransactions || progress.Rows != wantRows {
		t.Fatalf(
			"progress exists=%t lsn=%s tx=%d rows=%d, want true/%s/%d/%d",
			exists, progress.RemoteLSN, progress.Transactions, progress.Rows,
			pglogrepl.LSN(wantLSN), wantTransactions, wantRows,
		)
	}
}
