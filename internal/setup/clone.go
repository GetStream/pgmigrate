package setup

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// cloneSnapshot binds an untouched physical clone to a slot created BEFORE the
// clone. An exported snapshot ID is local to a server: the clone exports its own
// snapshot, and the provider's recovery LSN supplies the logical stream boundary.
// The random publication comment is copied by physical backup and proves this
// clone was taken after this particular setup attempt, not a previous migration.
func cloneSnapshot(ctx context.Context, cfg *Config, source *pgx.Conn, snapshot *Snapshot) (_ *pgx.Conn, err error) {
	marker := "pgmigrate-copy:" + rand.Text()
	if _, err := source.Exec(ctx, "COMMENT ON PUBLICATION "+quoteIdentifier(snapshot.Publication)+" IS '"+marker+"'"); err != nil {
		return nil, fmt.Errorf("mark source for cloning: %w", err)
	}
	if cfg.CopySourceReady != nil {
		cfg.CopySourceReady()
	}
	if cfg.CopySourceFile != "" {
		cfg.CopySourceDSN, err = waitCopySourceFile(ctx, cfg.CopySourceFile)
		if err != nil {
			return nil, err
		}
	}
	clone, err := waitClone(ctx, cfg.CopySourceDSN)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = clone.Close(context.Background())
		}
	}()

	var sourceVersion, cloneVersion int
	var sourceDB, cloneDB string
	var recovery bool
	if err := source.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000, current_database()").Scan(&sourceVersion, &sourceDB); err != nil {
		return nil, err
	}
	if err := clone.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000, current_database(), pg_is_in_recovery()").Scan(&cloneVersion, &cloneDB, &recovery); err != nil {
		return nil, err
	}
	if sourceVersion != cloneVersion || sourceDB != cloneDB || recovery {
		return nil, errors.New("copy-source must be a restored/promoted physical clone of the same database and PostgreSQL major, not a streaming standby")
	}
	var comment string
	if err := clone.QueryRow(ctx, `SELECT coalesce(obj_description(oid, 'pg_publication'), '')
		FROM pg_catalog.pg_publication WHERE pubname=$1`, snapshot.Publication).Scan(&comment); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("read clone provenance: %w", err)
	}
	if comment != marker {
		return nil, errors.New("copy-source is stale or unrelated: create a fresh physical clone AFTER this run reports source slot ready")
	}

	var subscribed bool
	if err := clone.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_catalog.pg_subscription
		WHERE subenabled AND subdbid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database()))`).Scan(&subscribed); err != nil {
		return nil, fmt.Errorf("inspect clone subscriptions: %w", err)
	}
	if subscribed {
		return nil, errors.New("copy-source has enabled subscriptions; the restored clone must remain unchanged")
	}
	seed, err := cloneSeedLSN(ctx, source, clone)
	if err != nil {
		return nil, err
	}
	var current string
	if err := source.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&current); err != nil {
		return nil, err
	}
	if err := validateCloneLSN(snapshot.ConsistentPoint, seed, current); err != nil {
		return nil, err
	}
	// Clone creation may take hours. Do not accept a slot whose retained WAL was
	// lost in the meantime, or one consumed by another client.
	var usable bool
	if err := source.QueryRow(ctx, `SELECT NOT active AND restart_lsn IS NOT NULL
		AND wal_status <> 'lost' AND confirmed_flush_lsn <= $2::pg_lsn
		FROM pg_catalog.pg_replication_slots WHERE slot_name=$1`, snapshot.Slot, seed).Scan(&usable); err != nil {
		return nil, fmt.Errorf("validate retained clone WAL: %w", err)
	}
	if !usable {
		return nil, errors.New("source slot no longer retains an exclusive usable stream for the clone")
	}
	// Explicitly disable PG17+'s transaction timeout as well as the ordinary
	// session timeouts: this transaction lasts until all copy workers finish.
	if cloneVersion >= 17 {
		if _, err := clone.Exec(ctx, "SET transaction_timeout=0"); err != nil {
			return nil, err
		}
	}
	if _, err := clone.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return nil, err
	}
	if err := clone.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot.Name); err != nil {
		return nil, fmt.Errorf("export clone snapshot: %w", err)
	}
	snapshot.CopySource = true
	snapshot.SlotConsistentPoint = snapshot.ConsistentPoint
	snapshot.ConsistentPoint = seed
	snapshot.BackendPID = clone.PgConn().PID()
	return clone, nil
}

func waitClone(ctx context.Context, dsn string) (*pgx.Conn, error) {
	for {
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := postgres.Connect(attempt, dsn)
		if err == nil {
			var recovery bool
			err = conn.QueryRow(attempt, "SELECT pg_is_in_recovery()").Scan(&recovery)
			if err == nil && !recovery {
				cancel()
				return conn, nil
			}
			_ = conn.Close(context.Background())
		}
		cancel()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil && !retryCloneConnection(err) {
			return nil, fmt.Errorf("connect copy-source: %w", err)
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Only endpoint availability errors are transient. Retrying TLS or client
// configuration errors forever hides the cause and retains source WAL forever.
func retryCloneConnection(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "57P03" // server is still starting
	}
	var networkError net.Error
	return errors.As(err, &networkError) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func cloneSeedLSN(ctx context.Context, source, clone *pgx.Conn) (string, error) {
	var sourceAurora, cloneAurora, rds bool
	if err := source.QueryRow(ctx, "SELECT to_regprocedure('pg_catalog.aurora_volume_logical_start_lsn()') IS NOT NULL").Scan(&sourceAurora); err != nil {
		return "", err
	}
	if err := clone.QueryRow(ctx, `SELECT to_regprocedure('pg_catalog.aurora_volume_logical_start_lsn()') IS NOT NULL,
		EXISTS (SELECT FROM pg_catalog.pg_available_extensions WHERE name='rds_tools')`).Scan(&cloneAurora, &rds); err != nil {
		return "", err
	}
	if sourceAurora != cloneAurora {
		return "", errors.New("copy-source cannot cross RDS/Aurora storage engines: their seed LSNs are not interchangeable")
	}
	query := "SELECT pg_last_wal_replay_lsn()::text"
	switch {
	case cloneAurora:
		query = "SELECT pg_catalog.aurora_volume_logical_start_lsn()::text"
	case rds:
		if _, err := clone.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS rds_tools"); err != nil {
			return "", fmt.Errorf("enable rds_tools on copy-source: %w", err)
		}
		query = "SELECT rds_tools.logical_seed_lsn()::text"
	}
	// On upstream PostgreSQL, a promoted physical clone retains the last
	// replayed record's END position. Never substitute pg_current_wal_lsn():
	// it is on the clone's new timeline and can silently skip source changes.
	var seed *string
	if err := clone.QueryRow(ctx, query).Scan(&seed); err != nil {
		return "", fmt.Errorf("read copy-source seed LSN: %w", err)
	}
	if seed == nil {
		return "", errors.New("copy-source has no physical recovery seed LSN")
	}
	return *seed, nil
}

func validateCloneLSN(slot, seed, current string) error {
	var positions [3]pglogrepl.LSN
	for i, value := range []string{slot, seed, current} {
		lsn, err := pglogrepl.ParseLSN(value)
		if err != nil || lsn == 0 {
			return fmt.Errorf("invalid clone synchronization LSN %q", value)
		}
		positions[i] = lsn
	}
	if positions[1] < positions[0] || positions[1] > positions[2] {
		return fmt.Errorf("copy-source seed LSN %s is outside retained source interval [%s, %s]; create the clone after slot preparation", seed, slot, current)
	}
	return nil
}

// Read the handoff once: changing the file during COPY must not switch servers
// underneath the exported snapshot. Missing and empty files mean "not ready".
func waitCopySourceFile(ctx context.Context, path string) (string, error) {
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		data, err := os.ReadFile(path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("read copy-source file: %w", err)
		}
		if dsn := strings.TrimSpace(string(data)); dsn != "" {
			if _, err := pgx.ParseConfig(dsn); err != nil {
				// pgx parse errors can include the complete DSN and password.
				return "", errors.New("invalid DSN in copy-source file")
			}
			return dsn, nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
}
