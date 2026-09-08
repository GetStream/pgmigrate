package setup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
)

// ResumeClone exports a new snapshot on the same unchanged physical clone.
// The operator must keep the clone free of writes, DDL and table rewrites for
// the entire migration, including downtime. Provenance alone cannot prove that
// a privileged client has not changed the restored data.
func ResumeClone(ctx context.Context, cfg Config, snapshot Snapshot, durable uint64) (_ *Holder, err error) {
	if !snapshot.CopySource {
		return nil, errors.New("COPY resume requires an unchanged physical clone")
	}
	if cfg.CopySourceFile != "" {
		data, err := os.ReadFile(cfg.CopySourceFile)
		if err != nil {
			return nil, fmt.Errorf("read clone DSN for resume: %w", err)
		}
		cfg.CopySourceDSN = strings.TrimSpace(string(data))
	}
	if strings.TrimSpace(cfg.CopySourceDSN) == "" {
		return nil, errors.New("COPY resume requires the original clone DSN; progress was preserved")
	}
	source, err := postgres.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, err
	}
	defer source.Close(context.Background())
	var marker, confirmed string
	if err := source.QueryRow(ctx, `SELECT coalesce(obj_description(oid,'pg_publication'),'') FROM pg_publication WHERE pubname=$1`, snapshot.Publication).Scan(&marker); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(marker, "pgmigrate-copy:") {
		return nil, errors.New("source clone provenance marker is missing")
	}
	if err := source.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1 AND NOT active AND wal_status <> 'lost' AND restart_lsn IS NOT NULL`, snapshot.Slot).Scan(&confirmed); err != nil {
		return nil, fmt.Errorf("validate exclusive retained source slot: %w", err)
	}
	seed, err := pglogrepl.ParseLSN(snapshot.ConsistentPoint)
	if err != nil {
		return nil, err
	}
	acknowledged, err := pglogrepl.ParseLSN(confirmed)
	if err != nil {
		return nil, err
	}
	if uint64(acknowledged) > max(uint64(seed), durable) {
		return nil, errors.New("source slot advanced beyond locally recovered CDC; refusing COPY resume")
	}
	clone, err := postgres.Connect(ctx, cfg.CopySourceDSN)
	if err != nil {
		return nil, errors.New("connect original clone for COPY resume failed")
	}
	defer func() {
		if err != nil {
			clone.Close(context.Background())
		}
	}()
	var cloneMarker, database, sourceDatabase string
	var recovery, subscribed bool
	var major, sourceMajor int
	if err := source.QueryRow(ctx, `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&sourceDatabase, &sourceMajor); err != nil {
		return nil, err
	}
	if err := clone.QueryRow(ctx, `SELECT current_database(),current_setting('server_version_num')::int/10000,pg_is_in_recovery(),EXISTS(SELECT FROM pg_subscription WHERE subenabled)`).Scan(&database, &major, &recovery, &subscribed); err != nil {
		return nil, err
	}
	if database != sourceDatabase || major != sourceMajor || recovery || subscribed {
		return nil, errors.New("COPY resume requires the original promoted clone without subscriptions")
	}
	if err := clone.QueryRow(ctx, `SELECT coalesce(obj_description(oid,'pg_publication'),'') FROM pg_publication WHERE pubname=$1`, snapshot.Publication).Scan(&cloneMarker); err != nil {
		return nil, err
	}
	if cloneMarker != marker {
		return nil, errors.New("clone provenance differs from the original source stream")
	}
	cloneSeed, err := cloneSeedLSN(ctx, source, clone)
	if err != nil {
		return nil, err
	}
	if cloneSeed != snapshot.ConsistentPoint {
		return nil, errors.New("clone recovery point differs from the saved COPY snapshot")
	}
	if major >= 17 {
		if _, err := clone.Exec(ctx, "SET transaction_timeout=0"); err != nil {
			return nil, err
		}
	}
	if _, err := clone.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return nil, err
	}
	if err := clone.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot.Name); err != nil {
		return nil, err
	}
	snapshot.BackendPID = clone.PgConn().PID()
	monitor, err := postgres.Connect(ctx, cfg.CopySourceDSN)
	if err != nil {
		return nil, err
	}
	return &Holder{Snapshot: snapshot, SnapshotDSN: cfg.CopySourceDSN, copyConn: clone, monitor: monitor}, nil
}
