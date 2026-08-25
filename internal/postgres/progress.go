package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	progressSchema = "pgmigrate_internal"
	progressTable  = progressSchema + ".replication_progress"
)

// ReplicationProgress is the target-local, transactionally committed replay
// position and work count for one migration stream. The counters advance in
// the same target transaction as the replicated DML and remote LSN, so a crash
// cannot report changes that were rolled back or count a committed batch twice.
type ReplicationProgress struct {
	RemoteLSN    pglogrepl.LSN
	Transactions int64
	Rows         int64
	UpdatedAt    time.Time
}

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
			transactions_applied bigint NOT NULL DEFAULT 0,
			rows_applied bigint NOT NULL DEFAULT 0,
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)
	`)
	if err != nil {
		return err
	}
	_, err = db.Exec(ctx, `
		ALTER TABLE `+progressTable+`
			ADD COLUMN IF NOT EXISTS transactions_applied bigint NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS rows_applied bigint NOT NULL DEFAULT 0
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
	progress, exists, err := ReadReplicationProgress(ctx, db, streamID)
	return progress.RemoteLSN, exists, err
}

// ReadReplicationProgress returns the authoritative target-local replay
// position and exact committed work counters for streamID.
func ReadReplicationProgress(
	ctx context.Context,
	db ProgressQuerier,
	streamID string,
) (ReplicationProgress, bool, error) {
	var progress ReplicationProgress
	var value string
	err := db.QueryRow(
		ctx,
		`SELECT remote_lsn::text, transactions_applied, rows_applied, updated_at
		 FROM `+progressTable+` WHERE stream_id = $1`,
		streamID,
	).Scan(&value, &progress.Transactions, &progress.Rows, &progress.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReplicationProgress{}, false, nil
	}
	if err != nil {
		return ReplicationProgress{}, false, err
	}

	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return ReplicationProgress{}, false, err
	}
	progress.RemoteLSN = lsn
	return progress, true, nil
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
