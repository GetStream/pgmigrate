package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	progressSchema = "pgmigrate_internal"
	progressTable  = progressSchema + ".replication_progress"
)

// ProgressExecer is implemented by *pgx.Conn and pgx.Tx.
type ProgressExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// ProgressQuerier is implemented by *pgx.Conn and pgx.Tx.
type ProgressQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// EnsureProgressTable creates the target-local durable progress store.
func EnsureProgressTable(ctx context.Context, db ProgressExecer) error {
	if _, err := db.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+progressSchema); err != nil {
		return err
	}
	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+progressTable+` (
			stream_id text PRIMARY KEY,
			remote_lsn pg_lsn NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)
	`)
	return err
}

// ReadProgress returns the authoritative target-local progress for streamID.
// The boolean is false when no progress has been recorded.
func ReadProgress(
	ctx context.Context,
	db ProgressQuerier,
	streamID string,
) (pglogrepl.LSN, bool, error) {
	var value string
	err := db.QueryRow(
		ctx,
		"SELECT remote_lsn::text FROM "+progressTable+" WHERE stream_id = $1",
		streamID,
	).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return 0, false, err
	}
	return lsn, true, nil
}

// UpdateProgress records remoteLSN. Pass the same pgx.Tx used for applied DML
// so data and resume progress commit or roll back atomically.
func UpdateProgress(
	ctx context.Context,
	tx ProgressExecer,
	streamID string,
	remoteLSN pglogrepl.LSN,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO `+progressTable+` (stream_id, remote_lsn)
		VALUES ($1, $2::pg_lsn)
		ON CONFLICT (stream_id) DO UPDATE
		SET remote_lsn = EXCLUDED.remote_lsn,
		    updated_at = clock_timestamp()
	`, streamID, remoteLSN.String())
	return err
}
