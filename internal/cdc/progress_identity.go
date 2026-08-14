package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	cdcProgressTable    = "pgmigrate_internal.replication_progress"
	streamIdentityTable = "pgmigrate_internal.cdc_stream_identity"
)

var (
	ErrMissingTargetProgress    = errors.New("cdc: target progress is missing for an initialized stream")
	ErrStreamGenerationMismatch = errors.New("cdc: target stream generation does not match migration")
)

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

	if (progressStarted || config.TargetHasCopiedData) && !config.FreshSetup {
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
	tag, err := tx.Exec(ctx, `
		WITH valid_identity AS MATERIALIZED (
			SELECT stream_id
			FROM `+streamIdentityTable+`
			WHERE stream_id = $1 AND stream_generation = $2
			FOR UPDATE
		),
		mark_started AS (
			UPDATE `+streamIdentityTable+` AS identity
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
		)
		INSERT INTO `+cdcProgressTable+` (stream_id, remote_lsn, stream_generation)
		SELECT stream_id, $3::pg_lsn, $2
		FROM progress_source
		ON CONFLICT (stream_id) DO UPDATE
		SET remote_lsn = EXCLUDED.remote_lsn,
		    stream_generation = EXCLUDED.stream_generation,
		    updated_at = clock_timestamp()
		WHERE `+cdcProgressTable+`.stream_generation IS NULL
		   OR `+cdcProgressTable+`.stream_generation = EXCLUDED.stream_generation
	`, streamID, generation, pglogrepl.LSN(remoteLSN).String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStreamGenerationMismatch
	}
	return nil
}
