package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	cdcProgressTable    = "pgmigrate_internal.replication_progress"
	streamIdentityTable = "pgmigrate_internal.cdc_stream_identity"
)

var (
	ErrMissingTargetProgress    = errors.New("cdc: target progress is missing for an initialized stream")
	ErrStreamGenerationMismatch = errors.New("cdc: target stream generation does not match migration")
)

const streamProgressSQL = `
	WITH valid_identity AS MATERIALIZED (
		SELECT stream_id
		FROM ` + streamIdentityTable + `
		WHERE stream_id = $1 AND stream_generation = $2
		FOR UPDATE
	),
	mark_started AS (
		UPDATE ` + streamIdentityTable + ` AS identity
		SET progress_started = true
		FROM valid_identity
		WHERE identity.stream_id = valid_identity.stream_id
		  AND NOT identity.progress_started
		RETURNING identity.stream_id
	),
	progress_source AS (
		SELECT valid_identity.stream_id
		FROM valid_identity
		LEFT JOIN mark_started USING (stream_id)
		WHERE (
			$3::pg_lsn = '0/0'::pg_lsn
			AND NOT EXISTS (
				SELECT 1 FROM ` + cdcProgressTable + ` AS current
				WHERE current.stream_id = $1
			)
		) OR EXISTS (
			SELECT 1 FROM ` + cdcProgressTable + ` AS current
			WHERE current.stream_id = $1
			  AND current.stream_generation = $2
			  AND current.remote_lsn = $3::pg_lsn
		)
	),
	progress AS (
		INSERT INTO ` + cdcProgressTable + ` AS existing (
			stream_id, remote_lsn, stream_generation,
			transactions_applied, rows_applied
		)
		SELECT stream_id, $4::pg_lsn, $2, $5::bigint, $6::bigint
		FROM progress_source
		ON CONFLICT (stream_id) DO UPDATE
		SET remote_lsn = EXCLUDED.remote_lsn,
		    stream_generation = EXCLUDED.stream_generation,
		    transactions_applied = existing.transactions_applied + EXCLUDED.transactions_applied,
		    rows_applied = existing.rows_applied + EXCLUDED.rows_applied,
		    updated_at = clock_timestamp()
		WHERE existing.stream_generation = EXCLUDED.stream_generation
		  AND existing.remote_lsn = $3::pg_lsn
		  AND EXCLUDED.remote_lsn > existing.remote_lsn
		RETURNING 1
	)
	SELECT 1 / count(*)::integer
	FROM progress
`

type StreamIdentityConfig struct {
	StreamID            string
	Generation          string
	FreshSetup          bool
	TargetHasCopiedData bool
}

type streamIdentityDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// EnsureStreamProgressIdentity validates a durable generation marker separate
// from the mutable progress row. base_generation is the immutable configured
// generation; stream_generation remains the current monotonic replay fence so
// binaries that predate base_generation are fenced by the column they already
// validate. Once
// progress has started, deleting only the progress row is detected and replay
// from zero is refused.
func EnsureStreamProgressIdentity(
	ctx context.Context,
	db streamIdentityDB,
	config StreamIdentityConfig,
) error {
	if config.StreamID == "" || config.Generation == "" {
		return errors.New("cdc: stream ID and generation are required")
	}
	if err := postgres.EnsureProgressTable(ctx, db); err != nil {
		return fmt.Errorf("cdc: ensure progress table for stream identity: %w", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+streamIdentityTable+` (
			stream_id text PRIMARY KEY,
			stream_generation text NOT NULL,
			base_generation text NOT NULL,
			progress_started boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)
	`); err != nil {
		return fmt.Errorf("cdc: create stream identity table: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+streamIdentityTable+" ADD COLUMN IF NOT EXISTS base_generation text",
	); err != nil {
		return fmt.Errorf("cdc: add base stream generation: %w", err)
	}
	if err := backfillBaseStreamGeneration(ctx, db); err != nil {
		return err
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+streamIdentityTable+" ALTER COLUMN base_generation SET NOT NULL",
	); err != nil {
		return fmt.Errorf("cdc: require base stream generation: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+cdcProgressTable+" ADD COLUMN IF NOT EXISTS stream_generation text",
	); err != nil {
		return fmt.Errorf("cdc: add progress stream generation: %w", err)
	}
	// Ensure upgrades of an already-created replay journal happen before its
	// active claim is used to validate the temporary progress/fence mismatch.
	if err := ensureReplayClaimTables(ctx, db); err != nil {
		return err
	}

	var baseGeneration, effectiveGeneration string
	var progressStarted bool
	identityExists := true
	err := db.QueryRow(
		ctx,
		"SELECT base_generation, stream_generation, progress_started FROM "+streamIdentityTable+" WHERE stream_id = $1",
		config.StreamID,
	).Scan(&baseGeneration, &effectiveGeneration, &progressStarted)
	if errors.Is(err, pgx.ErrNoRows) {
		identityExists = false
	} else if err != nil {
		return fmt.Errorf("cdc: read stream identity: %w", err)
	}
	if identityExists && baseGeneration != config.Generation {
		return fmt.Errorf("%w: stream %q has base %q, migration has %q",
			ErrStreamGenerationMismatch, config.StreamID, baseGeneration, config.Generation)
	}

	var progressGeneration *string
	progressExists := true
	err = db.QueryRow(
		ctx,
		"SELECT stream_generation FROM "+cdcProgressTable+" WHERE stream_id = $1",
		config.StreamID,
	).Scan(&progressGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		progressExists = false
	} else if err != nil {
		return fmt.Errorf("cdc: read progress generation: %w", err)
	}

	if !identityExists {
		if progressExists && progressGeneration != nil && *progressGeneration != "" &&
			*progressGeneration != config.Generation {
			return fmt.Errorf(
				"%w: progress for stream %q has %q, migration has %q",
				ErrStreamGenerationMismatch, config.StreamID,
				*progressGeneration, config.Generation,
			)
		}
		if config.TargetHasCopiedData && !config.FreshSetup {
			return fmt.Errorf("%w: stream %q has copied target data but no identity",
				ErrMissingTargetProgress, config.StreamID)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO `+streamIdentityTable+` (
				stream_id, stream_generation, base_generation, progress_started
			) VALUES ($1, $2, $2, $3)
		`, config.StreamID, config.Generation, progressExists); err != nil {
			return fmt.Errorf("cdc: create stream identity: %w", err)
		}
		baseGeneration = config.Generation
		effectiveGeneration = config.Generation
		progressStarted = progressExists
	}

	activeClaim, activeReplayFence, err := readReplayClaim(ctx, db, config.StreamID)
	if err != nil {
		return err
	}
	if activeReplayFence && (activeClaim.Generation != baseGeneration ||
		activeClaim.FenceGeneration != effectiveGeneration) {
		return fmt.Errorf(
			"%w: stream %q active claim does not match base/effective identity",
			ErrStreamGenerationMismatch, config.StreamID,
		)
	}
	expectedProgressGeneration := effectiveGeneration
	if activeReplayFence {
		expectedProgressGeneration = activeClaim.StartGeneration
	}

	if progressExists {
		if progressGeneration != nil && *progressGeneration != "" &&
			*progressGeneration != expectedProgressGeneration {
			return fmt.Errorf("%w: progress for stream %q has %q, identity expects %q",
				ErrStreamGenerationMismatch, config.StreamID,
				*progressGeneration, expectedProgressGeneration)
		}
		if _, err := db.Exec(ctx, `
			UPDATE `+cdcProgressTable+`
			SET stream_generation = $2
			WHERE stream_id = $1 AND (stream_generation IS NULL OR stream_generation = '')
		`, config.StreamID, expectedProgressGeneration); err != nil {
			return fmt.Errorf("cdc: adopt progress generation: %w", err)
		}
		if !progressStarted {
			if _, err := db.Exec(ctx, `
				UPDATE `+streamIdentityTable+` SET progress_started = true WHERE stream_id = $1
			`, config.StreamID); err != nil {
				return fmt.Errorf("cdc: mark stream progress started: %w", err)
			}
		}
		return nil
	}
	if activeReplayFence {
		return fmt.Errorf(
			"%w: stream %q has an active replay claim but no target progress",
			ErrMissingTargetProgress, config.StreamID,
		)
	}

	if (progressStarted || config.TargetHasCopiedData) && !config.FreshSetup {
		return fmt.Errorf("%w: stream %q generation %q",
			ErrMissingTargetProgress, config.StreamID, config.Generation)
	}
	return nil
}

// resolveStreamEffectiveGeneration maps an immutable configured generation to
// the current target-side replay fence. Callers must use the returned token for
// ordinary progress writes, while continuing to use configuredGeneration when
// constructing durable replay plans.
func resolveStreamEffectiveGeneration(
	ctx context.Context,
	db streamIdentityDB,
	streamID, configuredGeneration string,
) (string, error) {
	if streamID == "" || configuredGeneration == "" {
		return "", errors.New("cdc: stream ID and configured generation are required")
	}
	var baseGeneration, effectiveGeneration string
	err := db.QueryRow(ctx, `
		SELECT base_generation, stream_generation
		FROM `+streamIdentityTable+`
		WHERE stream_id = $1
	`, streamID).Scan(&baseGeneration, &effectiveGeneration)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: stream %q has no durable identity",
			ErrStreamGenerationMismatch, streamID)
	}
	if err != nil {
		return "", fmt.Errorf("cdc: read effective stream generation: %w", err)
	}
	if baseGeneration != configuredGeneration || effectiveGeneration == "" {
		return "", fmt.Errorf(
			"%w: stream %q has base/effective %q/%q, migration has base %q",
			ErrStreamGenerationMismatch, streamID,
			baseGeneration, effectiveGeneration, configuredGeneration,
		)
	}
	return effectiveGeneration, nil
}

func backfillBaseStreamGeneration(ctx context.Context, db streamIdentityDB) error {
	var relation *string
	if err := db.QueryRow(ctx, "SELECT to_regclass($1)::text", replayClaimTable).Scan(&relation); err != nil {
		return fmt.Errorf("cdc: inspect replay claims while upgrading stream identity: %w", err)
	}
	if relation != nil {
		// A replay claim stores the immutable configured generation separately
		// from its fence. Preserve the fence in the legacy stream_generation
		// column and recover the newly introduced base from the active claim.
		if _, err := db.Exec(ctx, `
			UPDATE `+streamIdentityTable+` AS identity
			SET base_generation = claim.stream_generation
			FROM `+replayClaimTable+` AS claim
			WHERE identity.stream_id = claim.stream_id
			  AND identity.base_generation IS NULL
			  AND identity.stream_generation = claim.fence_generation
		`); err != nil {
			return fmt.Errorf("cdc: recover replay-fenced stream identity: %w", err)
		}
	}
	if _, err := db.Exec(ctx, `
		UPDATE `+streamIdentityTable+`
		SET base_generation = stream_generation
		WHERE base_generation IS NULL
	`); err != nil {
		return fmt.Errorf("cdc: backfill base stream generation: %w", err)
	}
	return nil
}

func updateStreamProgress(
	ctx context.Context,
	tx pgx.Tx,
	streamID string,
	generation string,
	expectedLSN LSN,
	remoteLSN LSN,
	transactions int64,
	rows int64,
) error {
	tag, err := tx.Exec(
		ctx, streamProgressSQL, streamID, generation,
		pglogrepl.LSN(expectedLSN).String(), pglogrepl.LSN(remoteLSN).String(),
		transactions, rows,
	)
	if isProgressGuardError(err) {
		return ErrStreamGenerationMismatch
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStreamGenerationMismatch
	}
	return nil
}

func streamProgressParams(
	streamID, generation string,
	expectedLSN LSN,
	remoteLSN LSN,
	transactions, rows int64,
) []rawParam {
	return []rawParam{
		{data: []byte(streamID), oid: pgtype.TextOID},
		{data: []byte(generation), oid: pgtype.TextOID},
		{data: []byte(pglogrepl.LSN(expectedLSN).String()), oid: pgtype.TextOID},
		{data: []byte(pglogrepl.LSN(remoteLSN).String()), oid: pgtype.TextOID},
		{data: []byte(fmt.Sprint(transactions)), oid: pgtype.Int8OID},
		{data: []byte(fmt.Sprint(rows)), oid: pgtype.Int8OID},
	}
}

func isProgressGuardError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22012"
}
