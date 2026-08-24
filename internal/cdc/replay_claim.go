package cdc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	replayClaimPlanVersion = 2
	replayClaimTable       = "pgmigrate_internal.cdc_replay_claims"
	replayClaimWorkTable   = "pgmigrate_internal.cdc_replay_claim_work"
)

type replayWorkKind string

const (
	replayWorkParallelLane replayWorkKind = "parallel_lane"
	replayWorkSerial       replayWorkKind = "serial_transaction"
)

// replayClaim is one immutable, complete EndLSN range. Target progress remains
// at StartLSN while its work rows commit independently. Finalization advances
// progress only after every exact work row is complete.
type replayClaim struct {
	ID              string
	StreamID        string
	Generation      string // immutable configured/base generation
	StartGeneration string // effective generation before this claim fenced it
	FenceGeneration string
	StartLSN        LSN
	EndLSN          LSN
	Digest          [sha256.Size]byte
	CatalogDigest   [sha256.Size]byte
	PlanVersion     int
	LaneCount       int
	Transactions    int64
	Changes         int64
	ExpectedWork    int
	CreatedAt       time.Time
	WorkManifest    []replayClaimWork
}

// replayClaimWork is both the immutable expected-work manifest and its receipt.
// committed_at is updated in the same target transaction as the work's DML.
type replayClaimWork struct {
	Step                 int
	Work                 int
	Kind                 replayWorkKind
	Lane                 int
	Digest               [sha256.Size]byte
	ExpectedTransactions int64
	ExpectedChanges      int64
	CommittedAt          *time.Time
}

func replayFenceGeneration(generation, claimID string) string {
	return generation + "\npgmigrate-replay-claim-v1:" + claimID
}

func ensureReplayClaimTables(ctx context.Context, db streamIdentityDB) error {
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+replayClaimTable+` (
			claim_id text PRIMARY KEY,
			stream_id text NOT NULL UNIQUE
				REFERENCES `+streamIdentityTable+` (stream_id) ON DELETE CASCADE,
			stream_generation text NOT NULL,
			start_generation text NOT NULL,
			fence_generation text NOT NULL,
			start_lsn pg_lsn NOT NULL,
			end_lsn pg_lsn NOT NULL,
			claim_digest bytea NOT NULL CHECK (octet_length(claim_digest) = 32),
			catalog_digest bytea NOT NULL CHECK (octet_length(catalog_digest) = 32),
			plan_version integer NOT NULL CHECK (plan_version > 0),
			lane_count integer NOT NULL CHECK (lane_count > 0),
			transactions bigint NOT NULL CHECK (transactions >= 0),
			changes bigint NOT NULL CHECK (changes >= 0),
			expected_work integer NOT NULL CHECK (expected_work >= 0),
			created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			CHECK (end_lsn > start_lsn)
		)
	`); err != nil {
		return fmt.Errorf("cdc: create replay claim table: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+replayClaimTable+" ADD COLUMN IF NOT EXISTS start_generation text",
	); err != nil {
		return fmt.Errorf("cdc: add replay claim start generation: %w", err)
	}
	if _, err := db.Exec(ctx, `
		UPDATE `+replayClaimTable+` AS claim
		SET start_generation = coalesce(
			(
				SELECT progress.stream_generation
				FROM `+cdcProgressTable+` AS progress
				WHERE progress.stream_id = claim.stream_id
			),
			claim.stream_generation
		)
		WHERE claim.start_generation IS NULL
	`); err != nil {
		return fmt.Errorf("cdc: backfill replay claim start generation: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+replayClaimTable+" ALTER COLUMN start_generation SET NOT NULL",
	); err != nil {
		return fmt.Errorf("cdc: require replay claim start generation: %w", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+replayClaimWorkTable+` (
			claim_id text NOT NULL
				REFERENCES `+replayClaimTable+` (claim_id) ON DELETE CASCADE,
			step_index integer NOT NULL CHECK (step_index >= 0),
			work_index integer NOT NULL CHECK (work_index >= 0),
			work_kind text NOT NULL CHECK (work_kind IN ('parallel_lane','serial_transaction')),
			lane_index integer NOT NULL CHECK (lane_index >= -1),
			work_digest bytea NOT NULL CHECK (octet_length(work_digest) = 32),
			expected_transactions bigint NOT NULL CHECK (expected_transactions >= 0),
			expected_changes bigint NOT NULL CHECK (expected_changes >= 0),
			committed_at timestamptz,
			PRIMARY KEY (claim_id, step_index, work_index)
		)
	`); err != nil {
		return fmt.Errorf("cdc: create replay claim work table: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+replayClaimWorkTable+" ADD COLUMN IF NOT EXISTS expected_transactions bigint",
	); err != nil {
		return fmt.Errorf("cdc: add replay work expected transactions: %w", err)
	}
	if _, err := db.Exec(ctx, `
		UPDATE `+replayClaimWorkTable+`
		SET expected_transactions = CASE
			WHEN work_kind = 'serial_transaction' THEN 1
			ELSE 0
		END
		WHERE expected_transactions IS NULL
	`); err != nil {
		return fmt.Errorf("cdc: backfill replay work expected transactions: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+replayClaimWorkTable+" ALTER COLUMN expected_transactions SET NOT NULL",
	); err != nil {
		return fmt.Errorf("cdc: require replay work expected transactions: %w", err)
	}
	return nil
}

func readReplayClaim(ctx context.Context, db streamIdentityDB, streamID string) (replayClaim, bool, error) {
	var relation *string
	if err := db.QueryRow(ctx, "SELECT to_regclass($1)::text", replayClaimTable).Scan(&relation); err != nil {
		return replayClaim{}, false, fmt.Errorf("cdc: inspect active replay claim table: %w", err)
	}
	if relation == nil {
		return replayClaim{}, false, nil
	}
	var claim replayClaim
	var start, end string
	var digest, catalogDigest []byte
	err := db.QueryRow(ctx, `
		SELECT claim_id, stream_id, stream_generation, start_generation, fence_generation,
		       start_lsn::text, end_lsn::text, claim_digest, catalog_digest,
		       plan_version, lane_count, transactions, changes, expected_work, created_at
		FROM `+replayClaimTable+`
		WHERE stream_id = $1
	`, streamID).Scan(
		&claim.ID, &claim.StreamID, &claim.Generation, &claim.StartGeneration,
		&claim.FenceGeneration,
		&start, &end, &digest, &catalogDigest, &claim.PlanVersion, &claim.LaneCount,
		&claim.Transactions, &claim.Changes, &claim.ExpectedWork, &claim.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return replayClaim{}, false, nil
	}
	if err != nil {
		return replayClaim{}, false, fmt.Errorf("cdc: read active replay claim: %w", err)
	}
	if err := decodeReplayClaim(&claim, start, end, digest, catalogDigest); err != nil {
		return replayClaim{}, false, err
	}
	return claim, true, nil
}

func decodeReplayClaim(
	claim *replayClaim,
	start, end string,
	digest, catalogDigest []byte,
) error {
	startLSN, err := pglogrepl.ParseLSN(start)
	if err != nil {
		return fmt.Errorf("cdc: parse replay claim start LSN %q: %w", start, err)
	}
	endLSN, err := pglogrepl.ParseLSN(end)
	if err != nil {
		return fmt.Errorf("cdc: parse replay claim end LSN %q: %w", end, err)
	}
	if len(digest) != sha256.Size || len(catalogDigest) != sha256.Size {
		return errors.New("cdc: replay claim has an invalid digest length")
	}
	claim.StartLSN = LSN(startLSN)
	claim.EndLSN = LSN(endLSN)
	copy(claim.Digest[:], digest)
	copy(claim.CatalogDigest[:], catalogDigest)
	if claim.PlanVersion != replayClaimPlanVersion {
		return fmt.Errorf("cdc: unsupported replay claim plan version %d", claim.PlanVersion)
	}
	if claim.StartGeneration == "" || claim.LaneCount < 1 || claim.ExpectedWork < 0 ||
		claim.StartLSN >= claim.EndLSN {
		return errors.New("cdc: replay claim has invalid bounds or counts")
	}
	if claim.FenceGeneration != replayFenceGeneration(claim.Generation, claim.ID) {
		return errors.New("cdc: replay claim fence does not match its immutable identity")
	}
	if claim.FenceGeneration == claim.StartGeneration {
		return errors.New("cdc: replay claim reused its starting effective generation")
	}
	return nil
}

func readReplayClaimWorks(
	ctx context.Context,
	db streamIdentityDB,
	claimID string,
) ([]replayClaimWork, error) {
	return readReplayClaimWorksMode(ctx, db, claimID, false)
}

func readReplayClaimWorksForUpdate(
	ctx context.Context,
	db streamIdentityDB,
	claimID string,
) ([]replayClaimWork, error) {
	return readReplayClaimWorksMode(ctx, db, claimID, true)
}

func readReplayClaimWorksMode(
	ctx context.Context,
	db streamIdentityDB,
	claimID string,
	forUpdate bool,
) ([]replayClaimWork, error) {
	rows, err := queryReplayClaimWorks(ctx, db, claimID, forUpdate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var works []replayClaimWork
	for rows.Next() {
		var work replayClaimWork
		var kind string
		var digest []byte
		if err := rows.Scan(
			&work.Step, &work.Work, &kind, &work.Lane, &digest,
			&work.ExpectedTransactions, &work.ExpectedChanges, &work.CommittedAt,
		); err != nil {
			return nil, fmt.Errorf("cdc: scan replay claim work: %w", err)
		}
		work.Kind = replayWorkKind(kind)
		if len(digest) != sha256.Size {
			return nil, errors.New("cdc: replay work has an invalid digest length")
		}
		copy(work.Digest[:], digest)
		works = append(works, work)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cdc: read replay claim work: %w", err)
	}
	return works, nil
}

type replayClaimRows interface {
	Close()
	Next() bool
	Scan(...any) error
	Err() error
}

type replayClaimQueryDB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func queryReplayClaimWorks(
	ctx context.Context,
	db streamIdentityDB,
	claimID string,
	forUpdate bool,
) (pgx.Rows, error) {
	querier, ok := db.(replayClaimQueryDB)
	if !ok {
		return nil, errors.New("cdc: replay claim database cannot query work rows")
	}
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE"
	}
	rows, err := querier.Query(ctx, `
		SELECT step_index, work_index, work_kind, lane_index, work_digest,
		       expected_transactions, expected_changes, committed_at
		FROM `+replayClaimWorkTable+`
		WHERE claim_id = $1
		ORDER BY step_index, work_index
	`+lockClause, claimID)
	if err != nil {
		return nil, fmt.Errorf("cdc: query replay claim work: %w", err)
	}
	return rows, nil
}

func ensureReplayClaim(
	ctx context.Context,
	conn *pgx.Conn,
	desired replayClaim,
	works []replayClaimWork,
) (replayClaim, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return replayClaim{}, fmt.Errorf("cdc: begin replay claim: %w", err)
	}
	defer tx.Rollback(context.Background())

	var baseGeneration, currentGeneration string
	if err := tx.QueryRow(ctx, `
		SELECT base_generation, stream_generation
		FROM `+streamIdentityTable+`
		WHERE stream_id = $1
		FOR UPDATE
	`, desired.StreamID).Scan(&baseGeneration, &currentGeneration); err != nil {
		return replayClaim{}, fmt.Errorf("cdc: lock replay stream identity: %w", err)
	}
	if baseGeneration != desired.Generation {
		return replayClaim{}, fmt.Errorf(
			"%w: stream %q has base %q, replay claim expects %q",
			ErrStreamGenerationMismatch, desired.StreamID, baseGeneration, desired.Generation,
		)
	}

	existing, exists, err := readReplayClaim(ctx, tx, desired.StreamID)
	if err != nil {
		return replayClaim{}, err
	}
	if exists {
		if currentGeneration != existing.FenceGeneration {
			return replayClaim{}, fmt.Errorf(
				"%w: stream %q has effective generation %q, active replay claim expects %q",
				ErrStreamGenerationMismatch, desired.StreamID,
				currentGeneration, existing.FenceGeneration,
			)
		}
		desired.WorkManifest = cloneReplayWorkManifest(works)
		storedWorks, err := readReplayClaimWorks(ctx, tx, existing.ID)
		if err != nil {
			return replayClaim{}, err
		}
		if err := validateReplayClaim(existing, storedWorks, desired, works); err != nil {
			return replayClaim{}, err
		}
		existing.WorkManifest = cloneReplayWorkManifest(works)
		if err := tx.Commit(ctx); err != nil {
			return replayClaim{}, fmt.Errorf("cdc: finish existing replay claim check: %w", err)
		}
		return existing, nil
	}

	if desired.ID == "" {
		return replayClaim{}, errors.New("cdc: proposed replay claim identity is invalid")
	}
	expectedFence := replayFenceGeneration(desired.Generation, desired.ID)
	if desired.FenceGeneration == "" {
		desired.FenceGeneration = expectedFence
	}
	if desired.FenceGeneration != expectedFence || desired.FenceGeneration == currentGeneration ||
		desired.StartGeneration != currentGeneration {
		return replayClaim{}, errors.New("cdc: proposed replay claim fence is invalid or already effective")
	}
	desired.WorkManifest = cloneReplayWorkManifest(works)
	if err := validateReplayClaimManifest(desired, works); err != nil {
		return replayClaim{}, err
	}

	var progressLSN string
	var progressGeneration *string
	err = tx.QueryRow(ctx, `
		SELECT remote_lsn::text, stream_generation
		FROM `+cdcProgressTable+`
		WHERE stream_id = $1
		FOR UPDATE
	`, desired.StreamID).Scan(&progressLSN, &progressGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		if desired.StartLSN != 0 {
			return replayClaim{}, fmt.Errorf(
				"cdc: replay claim starts at %s but target progress is missing",
				pglogrepl.LSN(desired.StartLSN),
			)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+cdcProgressTable+` (
				stream_id, remote_lsn, stream_generation,
				transactions_applied, rows_applied
			) VALUES ($1, '0/0'::pg_lsn, $2, 0, 0)
		`, desired.StreamID, desired.StartGeneration); err != nil {
			return replayClaim{}, fmt.Errorf("cdc: initialize replay claim progress: %w", err)
		}
		progressLSN = "0/0"
		progressGeneration = &desired.StartGeneration
	} else if err != nil {
		return replayClaim{}, fmt.Errorf("cdc: lock replay claim progress: %w", err)
	}
	parsedProgress, err := pglogrepl.ParseLSN(progressLSN)
	if err != nil {
		return replayClaim{}, fmt.Errorf("cdc: parse replay claim progress %q: %w", progressLSN, err)
	}
	if LSN(parsedProgress) != desired.StartLSN || progressGeneration == nil ||
		*progressGeneration != desired.StartGeneration {
		return replayClaim{}, fmt.Errorf(
			"cdc: replay claim start mismatch: target=%s generation=%v claim=%s generation=%q",
			parsedProgress, progressGeneration,
			pglogrepl.LSN(desired.StartLSN), desired.StartGeneration,
		)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO `+replayClaimTable+` (
			claim_id, stream_id, stream_generation, start_generation, fence_generation,
			start_lsn, end_lsn, claim_digest, catalog_digest,
			plan_version, lane_count, transactions, changes, expected_work
		) VALUES ($1,$2,$3,$4,$5,$6::pg_lsn,$7::pg_lsn,$8,$9,$10,$11,$12,$13,$14)
	`,
		desired.ID, desired.StreamID, desired.Generation,
		desired.StartGeneration, desired.FenceGeneration,
		pglogrepl.LSN(desired.StartLSN).String(), pglogrepl.LSN(desired.EndLSN).String(),
		desired.Digest[:], desired.CatalogDigest[:], desired.PlanVersion, desired.LaneCount,
		desired.Transactions, desired.Changes, desired.ExpectedWork,
	); err != nil {
		return replayClaim{}, fmt.Errorf("cdc: insert replay claim: %w", err)
	}
	for _, work := range works {
		if _, err := tx.Exec(ctx, `
			INSERT INTO `+replayClaimWorkTable+` (
				claim_id, step_index, work_index, work_kind, lane_index,
				work_digest, expected_transactions, expected_changes
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		`, desired.ID, work.Step, work.Work, string(work.Kind), work.Lane,
			work.Digest[:], work.ExpectedTransactions, work.ExpectedChanges); err != nil {
			return replayClaim{}, fmt.Errorf(
				"cdc: insert replay claim work step=%d work=%d: %w",
				work.Step, work.Work, err,
			)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE `+streamIdentityTable+`
		SET stream_generation = $2, progress_started = true
		WHERE stream_id = $1
		  AND base_generation = $3
		  AND stream_generation = $4
	`, desired.StreamID, desired.FenceGeneration, desired.Generation, desired.StartGeneration)
	if err != nil {
		return replayClaim{}, fmt.Errorf("cdc: fence replay claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return replayClaim{}, ErrStreamGenerationMismatch
	}
	if err := tx.Commit(ctx); err != nil {
		return replayClaim{}, fmt.Errorf("cdc: commit replay claim: %w", err)
	}
	return desired, nil
}

func validateReplayClaim(
	stored replayClaim,
	storedWorks []replayClaimWork,
	desired replayClaim,
	desiredWorks []replayClaimWork,
) error {
	if stored.ID != desired.ID || stored.StreamID != desired.StreamID ||
		stored.Generation != desired.Generation || stored.StartGeneration != desired.StartGeneration ||
		stored.FenceGeneration != desired.FenceGeneration ||
		stored.StartLSN != desired.StartLSN || stored.EndLSN != desired.EndLSN ||
		stored.Digest != desired.Digest || stored.CatalogDigest != desired.CatalogDigest ||
		stored.PlanVersion != desired.PlanVersion || stored.LaneCount != desired.LaneCount ||
		stored.Transactions != desired.Transactions || stored.Changes != desired.Changes ||
		stored.ExpectedWork != desired.ExpectedWork {
		return errors.New("cdc: active replay claim does not match the reconstructed durable range")
	}
	if err := validateReplayClaimManifest(stored, storedWorks); err != nil {
		return fmt.Errorf("cdc: active replay claim manifest is invalid: %w", err)
	}
	if err := validateReplayClaimManifest(desired, desiredWorks); err != nil {
		return fmt.Errorf("cdc: reconstructed replay claim manifest is invalid: %w", err)
	}
	if len(storedWorks) != len(desiredWorks) {
		return errors.New("cdc: active replay claim work count does not match its reconstructed plan")
	}
	for i := range storedWorks {
		left, right := storedWorks[i], desiredWorks[i]
		if left.Step != right.Step || left.Work != right.Work || left.Kind != right.Kind ||
			left.Lane != right.Lane || left.Digest != right.Digest ||
			left.ExpectedTransactions != right.ExpectedTransactions ||
			left.ExpectedChanges != right.ExpectedChanges {
			return fmt.Errorf(
				"cdc: active replay work %d/%d does not match its reconstructed plan",
				right.Step, right.Work,
			)
		}
	}
	return nil
}

func cloneReplayWorkManifest(works []replayClaimWork) []replayClaimWork {
	result := append([]replayClaimWork(nil), works...)
	for i := range result {
		result[i].CommittedAt = nil
	}
	return result
}

func validateReplayClaimManifest(claim replayClaim, works []replayClaimWork) error {
	if claim.ID == "" || claim.StreamID == "" || claim.Generation == "" ||
		claim.StartGeneration == "" ||
		claim.FenceGeneration != replayFenceGeneration(claim.Generation, claim.ID) ||
		claim.FenceGeneration == claim.StartGeneration {
		return errors.New("cdc: replay claim generations do not form a unique fence")
	}
	if len(works) != claim.ExpectedWork {
		return fmt.Errorf(
			"cdc: replay claim expects %d work rows, manifest has %d",
			claim.ExpectedWork, len(works),
		)
	}
	var transactions, changes int64
	previousStep, previousWork := -1, -1
	for index, work := range works {
		if work.Step < 0 || work.Work < 0 || work.ExpectedTransactions <= 0 ||
			work.ExpectedChanges < 0 {
			return fmt.Errorf("cdc: replay work manifest row %d has invalid counters or indexes", index)
		}
		if index != 0 && (work.Step < previousStep ||
			(work.Step == previousStep && work.Work <= previousWork)) {
			return errors.New("cdc: replay work manifest is not in unique step/work order")
		}
		switch work.Kind {
		case replayWorkParallelLane:
			if work.Lane < 0 || work.Work != work.Lane {
				return fmt.Errorf("cdc: parallel replay work %d/%d has an invalid lane", work.Step, work.Work)
			}
		case replayWorkSerial:
			if work.Lane != -1 || work.Work != 0 || work.ExpectedTransactions != 1 {
				return fmt.Errorf("cdc: serial replay work %d/%d has an invalid manifest", work.Step, work.Work)
			}
		default:
			return fmt.Errorf("cdc: replay work %d/%d has unknown kind %q", work.Step, work.Work, work.Kind)
		}
		transactions += work.ExpectedTransactions
		changes += work.ExpectedChanges
		previousStep, previousWork = work.Step, work.Work
	}
	if transactions != claim.Transactions || changes != claim.Changes {
		return fmt.Errorf(
			"cdc: replay work manifest covers transactions=%d/%d changes=%d/%d",
			transactions, claim.Transactions, changes, claim.Changes,
		)
	}
	return nil
}

func beginReplayClaimWork(
	ctx context.Context,
	conn *pgx.Conn,
	claim replayClaim,
	work replayClaimWork,
) (bool, error) {
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return false, classifyApplyError(nil, 0, fmt.Errorf("cdc: begin replay work: %w", err))
	}
	rollback := func() {
		_, _ = conn.Exec(context.Background(), "ROLLBACK")
	}
	var stored replayClaimWork
	var kind string
	var digest []byte
	err := conn.QueryRow(ctx, `
		SELECT work.step_index, work.work_index, work.work_kind, work.lane_index,
		       work.work_digest, work.expected_transactions,
		       work.expected_changes, work.committed_at
		FROM `+replayClaimWorkTable+` AS work
		JOIN `+replayClaimTable+` AS claim USING (claim_id)
		JOIN `+streamIdentityTable+` AS identity USING (stream_id)
		WHERE work.claim_id = $1 AND work.step_index = $2 AND work.work_index = $3
		  AND claim.claim_digest = $4
		  AND claim.stream_generation = $5
		  AND identity.base_generation = claim.stream_generation
		  AND identity.stream_generation = claim.fence_generation
		  AND NOT EXISTS (
		    SELECT 1
		    FROM `+replayClaimWorkTable+` AS prior
		    WHERE prior.claim_id = work.claim_id
		      AND prior.step_index < work.step_index
		      AND prior.committed_at IS NULL
		  )
		FOR UPDATE OF work
	`, claim.ID, work.Step, work.Work, claim.Digest[:], claim.Generation).Scan(
		&stored.Step, &stored.Work, &kind, &stored.Lane, &digest,
		&stored.ExpectedTransactions, &stored.ExpectedChanges, &stored.CommittedAt,
	)
	if err != nil {
		rollback()
		return false, fmt.Errorf("cdc: lock replay work %d/%d: %w", work.Step, work.Work, err)
	}
	stored.Kind = replayWorkKind(kind)
	if len(digest) != sha256.Size {
		rollback()
		return false, errors.New("cdc: locked replay work has an invalid digest")
	}
	copy(stored.Digest[:], digest)
	if stored.Step != work.Step || stored.Work != work.Work || stored.Kind != work.Kind ||
		stored.Lane != work.Lane || stored.Digest != work.Digest ||
		stored.ExpectedTransactions != work.ExpectedTransactions ||
		stored.ExpectedChanges != work.ExpectedChanges {
		rollback()
		return false, errors.New("cdc: locked replay work does not match its reconstructed manifest")
	}
	if stored.CommittedAt != nil {
		rollback()
		return true, nil
	}
	return false, nil
}

const replayWorkCompletionSQL = `
	UPDATE ` + replayClaimWorkTable + ` AS work
	SET committed_at = clock_timestamp()
	FROM ` + replayClaimTable + ` AS claim,
	     ` + streamIdentityTable + ` AS identity
	WHERE work.claim_id = $1
	  AND work.step_index = $2
	  AND work.work_index = $3
	  AND work.work_digest = $4
	  AND work.expected_transactions = $5
	  AND work.expected_changes = $6
	  AND work.committed_at IS NULL
	  AND claim.claim_id = work.claim_id
	  AND claim.claim_digest = $7
	  AND claim.stream_generation = $8
	  AND identity.stream_id = claim.stream_id
	  AND identity.base_generation = claim.stream_generation
	  AND identity.stream_generation = claim.fence_generation
	RETURNING 1
`

func replayWorkCompletionParams(claim replayClaim, work replayClaimWork) []rawParam {
	return []rawParam{
		{data: []byte(claim.ID), oid: pgtype.TextOID},
		{data: []byte(fmt.Sprint(work.Step)), oid: pgtype.Int4OID},
		{data: []byte(fmt.Sprint(work.Work)), oid: pgtype.Int4OID},
		{data: work.Digest[:], oid: pgtype.ByteaOID, format: 1},
		{data: []byte(fmt.Sprint(work.ExpectedTransactions)), oid: pgtype.Int8OID},
		{data: []byte(fmt.Sprint(work.ExpectedChanges)), oid: pgtype.Int8OID},
		{data: claim.Digest[:], oid: pgtype.ByteaOID, format: 1},
		{data: []byte(claim.Generation), oid: pgtype.TextOID},
	}
}

func finalizeReplayClaim(ctx context.Context, conn *pgx.Conn, claim replayClaim) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cdc: begin replay claim finalization: %w", err)
	}
	defer tx.Rollback(context.Background())

	var baseGeneration, currentGeneration string
	if err := tx.QueryRow(ctx, `
		SELECT base_generation, stream_generation
		FROM `+streamIdentityTable+`
		WHERE stream_id = $1
		FOR UPDATE
	`, claim.StreamID).Scan(&baseGeneration, &currentGeneration); err != nil {
		return fmt.Errorf("cdc: lock final replay identity: %w", err)
	}
	if baseGeneration != claim.Generation || currentGeneration != claim.FenceGeneration {
		return fmt.Errorf("%w: replay fence changed before finalization", ErrStreamGenerationMismatch)
	}

	stored, exists, err := readReplayClaim(ctx, tx, claim.StreamID)
	if err != nil {
		return err
	}
	if !exists || stored.ID != claim.ID || stored.Digest != claim.Digest {
		return errors.New("cdc: replay claim changed before finalization")
	}
	storedWorks, err := readReplayClaimWorksForUpdate(ctx, tx, claim.ID)
	if err != nil {
		return err
	}
	if err := validateReplayClaim(stored, storedWorks, claim, claim.WorkManifest); err != nil {
		return fmt.Errorf("cdc: validate exact replay claim before finalization: %w", err)
	}
	for _, work := range storedWorks {
		if work.CommittedAt == nil {
			return fmt.Errorf(
				"cdc: replay claim work %d/%d is not committed",
				work.Step, work.Work,
			)
		}
	}

	var progressLSN string
	var progressGeneration *string
	if err := tx.QueryRow(ctx, `
		SELECT remote_lsn::text, stream_generation
		FROM `+cdcProgressTable+`
		WHERE stream_id = $1
		FOR UPDATE
	`, claim.StreamID).Scan(&progressLSN, &progressGeneration); err != nil {
		return fmt.Errorf("cdc: lock replay progress for finalization: %w", err)
	}
	parsedProgress, err := pglogrepl.ParseLSN(progressLSN)
	if err != nil {
		return fmt.Errorf("cdc: parse replay finalization progress %q: %w", progressLSN, err)
	}
	if LSN(parsedProgress) != claim.StartLSN || progressGeneration == nil ||
		*progressGeneration != claim.StartGeneration {
		return errors.New("cdc: target progress changed while replay claim was active")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE `+cdcProgressTable+`
		SET remote_lsn = $4::pg_lsn,
		    stream_generation = $3,
		    transactions_applied = transactions_applied + $5::bigint,
		    rows_applied = rows_applied + $6::bigint,
		    updated_at = clock_timestamp()
		WHERE stream_id = $1
		  AND stream_generation = $2
		  AND remote_lsn = $7::pg_lsn
	`, claim.StreamID, claim.StartGeneration, claim.FenceGeneration,
		pglogrepl.LSN(claim.EndLSN).String(), claim.Transactions, claim.Changes,
		pglogrepl.LSN(claim.StartLSN).String())
	if err != nil {
		return fmt.Errorf("cdc: finalize replay progress: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("cdc: replay progress changed before monotonic finalization")
	}
	tag, err = tx.Exec(ctx, "DELETE FROM "+replayClaimTable+" WHERE claim_id = $1", claim.ID)
	if err != nil {
		return fmt.Errorf("cdc: delete finalized replay claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("cdc: finalized replay claim disappeared before deletion")
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyApplyError(nil, 0, fmt.Errorf("cdc: commit replay claim finalization: %w", err))
	}
	return nil
}

func newReplayClaimHasher(label string) hash.Hash {
	hasher := sha256.New()
	writeReplayHashBytes(hasher, []byte(label))
	return hasher
}

func writeReplayHashBytes(hasher hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = hasher.Write(size[:])
	_, _ = hasher.Write(value)
}

func writeReplayHashInt(hasher hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = hasher.Write(encoded[:])
}

func finishReplayHash(hasher hash.Hash) [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func replayClaimID(digest [sha256.Size]byte) string {
	return hex.EncodeToString(digest[:])
}

func replayClaimsEqual(left, right replayClaim) bool {
	return left.ID == right.ID && left.StreamID == right.StreamID &&
		left.Generation == right.Generation && left.StartGeneration == right.StartGeneration &&
		left.FenceGeneration == right.FenceGeneration &&
		left.StartLSN == right.StartLSN && left.EndLSN == right.EndLSN &&
		bytes.Equal(left.Digest[:], right.Digest[:])
}
