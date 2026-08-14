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
	cdcProgressTable         = "pgmigrate_internal.replication_progress"
	streamIdentityTable      = "pgmigrate_internal.cdc_stream_identity"
	streamReplayReceiptTable = "pgmigrate_internal.cdc_replay_receipts"
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
	),
	progress AS (
		INSERT INTO ` + cdcProgressTable + ` (stream_id, remote_lsn, stream_generation)
		SELECT stream_id, $3::pg_lsn, $2
		FROM progress_source
		ON CONFLICT (stream_id) DO UPDATE
		SET remote_lsn = EXCLUDED.remote_lsn,
		    stream_generation = EXCLUDED.stream_generation,
		    updated_at = clock_timestamp()
		WHERE ` + cdcProgressTable + `.stream_generation IS NULL
		   OR ` + cdcProgressTable + `.stream_generation = EXCLUDED.stream_generation
		RETURNING 1
	)
	SELECT 1 / count(*)::integer
	FROM progress
`

const streamReplayReceiptSQL = `
	WITH valid_identity AS MATERIALIZED (
		SELECT stream_id
		FROM ` + streamIdentityTable + `
		WHERE stream_id = $1 AND stream_generation = $2
	),
	receipts AS (
		INSERT INTO ` + streamReplayReceiptTable + ` (
			stream_id, stream_generation, first_lsn, last_lsn
		)
		SELECT valid_identity.stream_id, $2, $3::pg_lsn, $4::pg_lsn
		FROM valid_identity
		ON CONFLICT DO NOTHING
		RETURNING 1
	)
	SELECT 1 / count(*)::integer
	FROM receipts
`

const checkpointStreamProgressSQL = `
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
	),
	progress AS (
		INSERT INTO ` + cdcProgressTable + ` (stream_id, remote_lsn, stream_generation)
		SELECT stream_id, $3::pg_lsn, $2
		FROM progress_source
		ON CONFLICT (stream_id) DO UPDATE
		SET remote_lsn = EXCLUDED.remote_lsn,
		    stream_generation = EXCLUDED.stream_generation,
		    updated_at = clock_timestamp()
		WHERE ` + cdcProgressTable + `.stream_generation IS NULL
		   OR ` + cdcProgressTable + `.stream_generation = EXCLUDED.stream_generation
		RETURNING 1
	),
	cleanup AS (
		DELETE FROM ` + streamReplayReceiptTable + ` AS receipt
		USING progress
		WHERE receipt.stream_id = $1
		  AND receipt.stream_generation = $2
		  AND receipt.last_lsn <= $3::pg_lsn
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
// from the mutable progress row. Once progress has started, deleting only the
// progress row is detected and replay from zero is refused.
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
			progress_started boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)
	`); err != nil {
		return fmt.Errorf("cdc: create stream identity table: %w", err)
	}
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+streamReplayReceiptTable+` (
			stream_id text NOT NULL,
			stream_generation text NOT NULL,
			first_lsn pg_lsn NOT NULL,
			last_lsn pg_lsn NOT NULL,
			created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			PRIMARY KEY (stream_id, stream_generation, last_lsn),
			CHECK (first_lsn <= last_lsn)
		)
	`); err != nil {
		return fmt.Errorf("cdc: create replay receipt table: %w", err)
	}
	if _, err := db.Exec(
		ctx,
		"ALTER TABLE "+cdcProgressTable+" ADD COLUMN IF NOT EXISTS stream_generation text",
	); err != nil {
		return fmt.Errorf("cdc: add progress stream generation: %w", err)
	}

	var storedGeneration string
	var progressStarted bool
	identityExists := true
	err := db.QueryRow(
		ctx,
		"SELECT stream_generation, progress_started FROM "+streamIdentityTable+" WHERE stream_id = $1",
		config.StreamID,
	).Scan(&storedGeneration, &progressStarted)
	if errors.Is(err, pgx.ErrNoRows) {
		identityExists = false
	} else if err != nil {
		return fmt.Errorf("cdc: read stream identity: %w", err)
	}
	if identityExists && storedGeneration != config.Generation {
		return fmt.Errorf("%w: stream %q has %q, migration has %q",
			ErrStreamGenerationMismatch, config.StreamID, storedGeneration, config.Generation)
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
		if config.TargetHasCopiedData && !config.FreshSetup {
			return fmt.Errorf("%w: stream %q has copied target data but no identity",
				ErrMissingTargetProgress, config.StreamID)
		}
		if _, err := db.Exec(ctx, `
			INSERT INTO `+streamIdentityTable+` (stream_id, stream_generation, progress_started)
			VALUES ($1, $2, $3)
		`, config.StreamID, config.Generation, progressExists); err != nil {
			return fmt.Errorf("cdc: create stream identity: %w", err)
		}
		progressStarted = progressExists
	}

	if progressExists {
		if progressGeneration != nil && *progressGeneration != "" && *progressGeneration != config.Generation {
			return fmt.Errorf("%w: progress for stream %q has %q, migration has %q",
				ErrStreamGenerationMismatch, config.StreamID, *progressGeneration, config.Generation)
		}
		if _, err := db.Exec(ctx, `
			UPDATE `+cdcProgressTable+` SET stream_generation = $2 WHERE stream_id = $1
		`, config.StreamID, config.Generation); err != nil {
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

	var receiptExists bool
	if err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM `+streamReplayReceiptTable+`
			WHERE stream_id = $1 AND stream_generation = $2
		)
	`, config.StreamID, config.Generation).Scan(&receiptExists); err != nil {
		return fmt.Errorf("cdc: read replay receipts: %w", err)
	}
	// A crash can durably commit the first DML receipts before the first
	// canonical progress checkpoint. Those receipts are sufficient to resume
	// safely. Once progress_started is set, a missing progress row still means
	// target state was tampered with and must never be guessed.
	if (progressStarted || config.TargetHasCopiedData && !receiptExists) && !config.FreshSetup {
		return fmt.Errorf("%w: stream %q generation %q",
			ErrMissingTargetProgress, config.StreamID, config.Generation)
	}
	return nil
}

func updateStreamProgress(
	ctx context.Context,
	tx pgx.Tx,
	streamID string,
	generation string,
	remoteLSN LSN,
) error {
	tag, err := tx.Exec(
		ctx, streamProgressSQL, streamID, generation, pglogrepl.LSN(remoteLSN).String(),
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

func streamProgressParams(streamID, generation string, remoteLSN LSN) []rawParam {
	return []rawParam{
		{data: []byte(streamID), oid: pgtype.TextOID},
		{data: []byte(generation), oid: pgtype.TextOID},
		{data: []byte(pglogrepl.LSN(remoteLSN).String()), oid: pgtype.TextOID},
	}
}

func streamReplayReceiptParams(streamID, generation string, transactions []Transaction) []rawParam {
	return []rawParam{
		{data: []byte(streamID), oid: pgtype.TextOID},
		{data: []byte(generation), oid: pgtype.TextOID},
		{data: []byte(pglogrepl.LSN(transactions[0].EndLSN).String()), oid: pgtype.TextOID},
		{data: []byte(pglogrepl.LSN(transactions[len(transactions)-1].EndLSN).String()), oid: pgtype.TextOID},
	}
}

type streamReplayReceipt struct {
	first LSN
	last  LSN
}

func loadStreamReplayReceipts(
	ctx context.Context,
	conn *pgx.Conn,
	streamID string,
	generation string,
	progress LSN,
) ([]streamReplayReceipt, error) {
	rows, err := conn.Query(ctx, `
		SELECT first_lsn::text, last_lsn::text
		FROM `+streamReplayReceiptTable+`
		WHERE stream_id = $1 AND stream_generation = $2 AND last_lsn > $3::pg_lsn
		ORDER BY first_lsn
	`, streamID, generation, pglogrepl.LSN(progress).String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var receipts []streamReplayReceipt
	for rows.Next() {
		var firstValue, lastValue string
		if err := rows.Scan(&firstValue, &lastValue); err != nil {
			return nil, err
		}
		first, err := pglogrepl.ParseLSN(firstValue)
		if err != nil {
			return nil, fmt.Errorf("cdc: parse replay receipt first LSN %q: %w", firstValue, err)
		}
		last, err := pglogrepl.ParseLSN(lastValue)
		if err != nil {
			return nil, fmt.Errorf("cdc: parse replay receipt last LSN %q: %w", lastValue, err)
		}
		receipts = append(receipts, streamReplayReceipt{first: LSN(first), last: LSN(last)})
	}
	return receipts, rows.Err()
}

func checkpointStreamProgress(
	ctx context.Context,
	conn *pgx.Conn,
	streamID string,
	generation string,
	remoteLSN LSN,
) error {
	tag, err := conn.Exec(
		ctx, checkpointStreamProgressSQL,
		streamID, generation, pglogrepl.LSN(remoteLSN).String(),
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

func isProgressGuardError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22012"
}
