// Package setup creates and owns the source snapshot lifecycle used by copy.
package setup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SnapshotState is implemented by state.Store.
type SnapshotState interface {
	SetSnapshot(context.Context, string, string, string) error
}

var ErrSnapshotHolderLost = errors.New("snapshot holder backend is no longer alive")

// Table identifies a publication member.
type Table struct {
	Schema string
	Name   string
}

// Config controls source and target setup.
type Config struct {
	SourceDSN      string
	TargetDSN      string
	Dir            string
	MigrationID    string
	Tables         []Table
	EnableFailover bool
}

// Snapshot is the durable description written to snapshot.json.
type Snapshot struct {
	SourceFingerprint string    `json:"source_fingerprint"`
	Publication       string    `json:"publication"`
	Slot              string    `json:"slot"`
	Name              string    `json:"snapshot"`
	ConsistentPoint   string    `json:"consistent_point"`
	BackendPID        uint32    `json:"backend_pid"`
	Failover          bool      `json:"failover"`
	CreatedAt         time.Time `json:"created_at"`
}

// Holder owns the command-idle replication connection. The orchestrator must
// retain it until every copy transaction has imported Snapshot.Name. No method
// sends traffic on that connection.
type Holder struct {
	Snapshot Snapshot
	repl     *pgconn.PgConn
	monitor  *pgx.Conn
	mu       sync.Mutex
	closed   bool
}

// Alive checks the holder backend through the separate monitoring connection.
// It deliberately never pings the snapshot-exporting connection.
func (h *Holder) Alive(ctx context.Context) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, nil
	}
	var alive bool
	err := h.monitor.QueryRow(
		ctx,
		"SELECT EXISTS(SELECT FROM pg_catalog.pg_stat_activity WHERE pid=$1)",
		int32(h.Snapshot.BackendPID),
	).Scan(&alive)
	return alive, err
}

// Watchdog polls the holder backend using only the monitor connection. The
// returned channel receives exactly one terminal value: ctx.Err(), a monitor
// query error, or ErrSnapshotHolderLost. It never commands or pings the exporter.
func (h *Holder) Watchdog(ctx context.Context, interval time.Duration) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer close(result)
		if interval <= 0 {
			result <- errors.New("watchdog interval must be positive")
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			alive, err := h.Alive(ctx)
			if err != nil {
				result <- err
				return
			}
			if !alive {
				result <- ErrSnapshotHolderLost
				return
			}
			select {
			case <-ctx.Done():
				result <- ctx.Err()
				return
			case <-ticker.C:
			}
		}
	}()
	return result
}

// Close releases the exported snapshot by closing its command-idle connection.
// The logical slot and publication remain for CDC.
func (h *Holder) Close(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	var result error
	if h.repl != nil {
		result = errors.Join(result, h.repl.Close(ctx))
	}
	if h.monitor != nil {
		result = errors.Join(result, h.monitor.Close(ctx))
	}
	return result
}

// Run creates publication, logical slot and exported snapshot, target progress
// storage, durable JSON, and SQLite snapshot state. On failure it removes every
// source object created by this attempt.
func Run(ctx context.Context, cfg Config, state SnapshotState) (_ *Holder, err error) {
	if state == nil {
		return nil, errors.New("snapshot state is required")
	}
	if strings.TrimSpace(cfg.SourceDSN) == "" || strings.TrimSpace(cfg.TargetDSN) == "" ||
		strings.TrimSpace(cfg.Dir) == "" || len(cfg.Tables) == 0 {
		return nil, errors.New("source DSN, target DSN, directory, and selected tables are required")
	}

	source, err := postgres.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("connect source setup: %w", err)
	}
	defer source.Close(context.Background())
	target, err := postgres.Connect(ctx, cfg.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("connect target setup: %w", err)
	}
	defer target.Close(context.Background())

	repl, err := replicationConnect(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, err
	}
	holderOwnsRepl := false
	defer func() {
		if !holderOwnsRepl {
			_ = repl.Close(context.Background())
		}
	}()

	system, err := pglogrepl.IdentifySystem(ctx, repl)
	if err != nil {
		return nil, fmt.Errorf("identify source system: %w", err)
	}
	fingerprint := SourceFingerprint(system.SystemID, system.DBName)
	publication, slot := Names(fingerprint, cfg.MigrationID)

	createdPublication := false
	createdSlot := false
	defer func() {
		if err == nil {
			return
		}
		_ = repl.Close(context.Background())
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if createdSlot {
			_, _ = source.Exec(cleanupCtx,
				"SELECT pg_catalog.pg_drop_replication_slot($1) WHERE EXISTS (SELECT FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)",
				slot)
		}
		if createdPublication {
			_, _ = source.Exec(cleanupCtx, "DROP PUBLICATION IF EXISTS "+quoteIdentifier(publication))
		}
		_ = os.Remove(filepath.Join(cfg.Dir, "snapshot.json.tmp"))
	}()

	if err := ensureNoPublicationCollision(ctx, source, publication); err != nil {
		return nil, err
	}
	if err := ensureNoSlotCollision(ctx, source, slot); err != nil {
		return nil, err
	}
	if err := createPublication(ctx, source, publication, cfg.Tables); err != nil {
		return nil, err
	}
	createdPublication = true

	var major int
	if err := source.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int / 10000").Scan(&major); err != nil {
		return nil, fmt.Errorf("read source major: %w", err)
	}
	caps, err := postgres.CapabilitiesForMajor(major)
	if err != nil {
		return nil, err
	}
	if cfg.EnableFailover && !caps.ReplicationSlotFailover {
		return nil, fmt.Errorf("failover logical slots require PostgreSQL 17 or newer")
	}

	slotResult, err := createSlot(ctx, repl, slot, cfg.EnableFailover)
	if err != nil {
		return nil, fmt.Errorf("create logical slot: %w", err)
	}
	createdSlot = true

	if err := postgres.EnsureProgressTable(ctx, target); err != nil {
		return nil, fmt.Errorf("create target progress table: %w", err)
	}
	monitor, err := postgres.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("connect snapshot monitor: %w", err)
	}
	defer func() {
		if !holderOwnsRepl {
			_ = monitor.Close(context.Background())
		}
	}()

	snapshot := Snapshot{
		SourceFingerprint: fingerprint,
		Publication:       publication,
		Slot:              slotResult.SlotName,
		Name:              slotResult.SnapshotName,
		ConsistentPoint:   slotResult.ConsistentPoint,
		BackendPID:        repl.PID(),
		Failover:          cfg.EnableFailover,
		CreatedAt:         time.Now().UTC(),
	}
	if snapshot.Name == "" {
		return nil, errors.New("logical slot did not export a snapshot")
	}
	if err := writeSnapshotAtomic(cfg.Dir, snapshot); err != nil {
		return nil, err
	}
	if err := state.SetSnapshot(ctx, snapshot.Slot, snapshot.Name, snapshot.ConsistentPoint); err != nil {
		_ = os.Remove(filepath.Join(cfg.Dir, "snapshot.json"))
		return nil, fmt.Errorf("persist snapshot state: %w", err)
	}

	holderOwnsRepl = true
	return &Holder{Snapshot: snapshot, repl: repl, monitor: monitor}, nil
}

func replicationConnect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	config, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse source replication DSN: %w", err)
	}
	config.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect source replication protocol: %w", err)
	}
	return conn, nil
}

func createSlot(ctx context.Context, conn *pgconn.PgConn, name string, failover bool) (pglogrepl.CreateReplicationSlotResult, error) {
	if !failover {
		return pglogrepl.CreateReplicationSlot(ctx, conn, name, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{
				Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT",
			})
	}
	command := fmt.Sprintf(
		"CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER true)", name,
	)
	return pglogrepl.ParseCreateReplicationSlot(conn.Exec(ctx, command))
}

func ensureNoPublicationCollision(ctx context.Context, conn *pgx.Conn, name string) error {
	var owner string
	err := conn.QueryRow(ctx, `
		SELECT pg_catalog.pg_get_userbyid(pubowner)
		FROM pg_catalog.pg_publication WHERE pubname=$1`, name).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect publication collision: %w", err)
	}
	var current string
	if err := conn.QueryRow(ctx, "SELECT current_user").Scan(&current); err != nil {
		return err
	}
	return fmt.Errorf("publication %q already exists (owner %q, current user %q); refusing to adopt it", name, owner, current)
}

func ensureNoSlotCollision(ctx context.Context, conn *pgx.Conn, name string) error {
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT FROM pg_catalog.pg_replication_slots WHERE slot_name=$1)", name).Scan(&exists); err != nil {
		return fmt.Errorf("inspect slot collision: %w", err)
	}
	if exists {
		return fmt.Errorf("replication slot %q already exists; refusing to adopt it", name)
	}
	return nil
}

// ResumeConfirmation is an explicit safety assertion made after reading
// durable migration state. Recovery is disabled unless NoSnapshot is true.
type ResumeConfirmation struct {
	NoSnapshot bool
}

// RecoverStale removes artifacts left by a process killed before SetSnapshot.
// It validates all existing deterministic objects before dropping any of them.
// Publications must be owned by the current user and contain exactly the
// selected tables. Slots must be inactive pgoutput logical slots for the
// current database with the expected failover setting.
func RecoverStale(ctx context.Context, cfg Config, confirmation ResumeConfirmation) error {
	if !confirmation.NoSnapshot {
		return errors.New("stale setup recovery requires durable confirmation that no snapshot is recorded")
	}
	if strings.TrimSpace(cfg.SourceDSN) == "" || strings.TrimSpace(cfg.Dir) == "" || len(cfg.Tables) == 0 {
		return errors.New("source DSN, migration directory, and selected tables are required for stale setup recovery")
	}
	repl, err := replicationConnect(ctx, cfg.SourceDSN)
	if err != nil {
		return err
	}
	system, err := pglogrepl.IdentifySystem(ctx, repl)
	_ = repl.Close(context.Background())
	if err != nil {
		return fmt.Errorf("identify source system for recovery: %w", err)
	}
	publication, slot := Names(SourceFingerprint(system.SystemID, system.DBName), cfg.MigrationID)

	conn, err := postgres.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return fmt.Errorf("connect source recovery: %w", err)
	}
	defer conn.Close(context.Background())
	pubExists, err := validateRecoverablePublication(ctx, conn, publication, cfg.Tables)
	if err != nil {
		return err
	}
	slotExists, err := validateRecoverableSlot(ctx, conn, slot, cfg.EnableFailover)
	if err != nil {
		return err
	}
	// Validation above is intentionally complete before the first mutation.
	if slotExists {
		if _, err := conn.Exec(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", slot); err != nil {
			return fmt.Errorf("drop stale replication slot: %w", err)
		}
	}
	if pubExists {
		if _, err := conn.Exec(ctx, "DROP PUBLICATION "+quoteIdentifier(publication)); err != nil {
			return fmt.Errorf("drop stale publication: %w", err)
		}
	}
	var result error
	for _, name := range []string{"snapshot.json", "snapshot.json.tmp"} {
		if err := os.Remove(filepath.Join(cfg.Dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove stale %s: %w", name, err))
		}
	}
	return result
}

func validateRecoverablePublication(
	ctx context.Context,
	conn *pgx.Conn,
	name string,
	tables []Table,
) (bool, error) {
	var owner, current string
	var allTables, insert, update, deleteRows, truncate bool
	err := conn.QueryRow(ctx, `
		SELECT pg_catalog.pg_get_userbyid(pubowner), current_user,
		       puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
		FROM pg_catalog.pg_publication WHERE pubname=$1
	`, name).Scan(&owner, &current, &allTables, &insert, &update, &deleteRows, &truncate)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect stale publication: %w", err)
	}
	if owner != current {
		return false, fmt.Errorf("publication %q is owned by %q, not current user %q; refusing recovery", name, owner, current)
	}
	if allTables || !insert || !update || !deleteRows || !truncate {
		return false, fmt.Errorf("publication %q settings do not match a pgmigrate publication; refusing recovery", name)
	}
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_catalog.pg_publication_rel pr
		JOIN pg_catalog.pg_publication p ON p.oid=pr.prpubid
		JOIN pg_catalog.pg_class c ON c.oid=pr.prrelid
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE p.pubname=$1 ORDER BY n.nspname, c.relname
	`, name)
	if err != nil {
		return false, fmt.Errorf("inspect stale publication tables: %w", err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return false, err
		}
		actual = append(actual, schema+"\x00"+table)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	expected := make([]string, 0, len(tables))
	for _, table := range tables {
		expected = append(expected, table.Schema+"\x00"+table.Name)
	}
	slices.Sort(actual)
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		return false, fmt.Errorf("publication %q table membership does not match expected migration tables; refusing recovery", name)
	}
	return true, nil
}

func validateRecoverableSlot(
	ctx context.Context,
	conn *pgx.Conn,
	name string,
	expectFailover bool,
) (bool, error) {
	var major int
	if err := conn.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int / 10000").Scan(&major); err != nil {
		return false, err
	}
	var plugin, database string
	var active, failover, temporary, twoPhase bool
	query := `
		SELECT COALESCE(plugin,''), COALESCE(database,''), active, false, temporary, two_phase
		FROM pg_catalog.pg_replication_slots WHERE slot_name=$1`
	if major >= 17 {
		query = `
			SELECT COALESCE(plugin,''), COALESCE(database,''), active, failover, temporary, two_phase
			FROM pg_catalog.pg_replication_slots WHERE slot_name=$1`
	}
	err := conn.QueryRow(ctx, query, name).Scan(
		&plugin, &database, &active, &failover, &temporary, &twoPhase,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect stale replication slot: %w", err)
	}
	var currentDatabase string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&currentDatabase); err != nil {
		return false, err
	}
	if plugin != "pgoutput" || database != currentDatabase || active ||
		failover != expectFailover || temporary || twoPhase {
		return false, fmt.Errorf(
			"replication slot %q does not match expected inactive pgoutput slot for database %q (plugin=%q database=%q active=%v failover=%v temporary=%v two_phase=%v); refusing recovery",
			name, currentDatabase, plugin, database, active, failover, temporary, twoPhase,
		)
	}
	return true, nil
}

func createPublication(ctx context.Context, conn *pgx.Conn, name string, tables []Table) error {
	qualified := make([]string, 0, len(tables))
	for _, table := range tables {
		if table.Schema == "" || table.Name == "" {
			return errors.New("publication table schema and name are required")
		}
		qualified = append(qualified, quoteIdentifier(table.Schema)+"."+quoteIdentifier(table.Name))
	}
	statement := "CREATE PUBLICATION " + quoteIdentifier(name) + " FOR TABLE " + strings.Join(qualified, ",")
	if _, err := conn.Exec(ctx, statement); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

// CleanupOwned validates deterministic source objects completely before
// removing either one. It refuses altered, active, or foreign-owned objects.
func CleanupOwned(
	ctx context.Context,
	sourceDSN, publication, slot string,
	tables []Table,
	expectFailover bool,
) error {
	conn, err := postgres.Connect(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect validated source cleanup: %w", err)
	}
	defer conn.Close(context.Background())
	pubExists, err := validateRecoverablePublication(ctx, conn, publication, tables)
	if err != nil {
		return err
	}
	slotExists, err := validateRecoverableSlot(ctx, conn, slot, expectFailover)
	if err != nil {
		return err
	}
	if slotExists {
		if _, err := conn.Exec(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", slot); err != nil {
			return fmt.Errorf("drop validated replication slot: %w", err)
		}
	}
	if pubExists {
		if _, err := conn.Exec(ctx, "DROP PUBLICATION "+quoteIdentifier(publication)); err != nil {
			return fmt.Errorf("drop validated publication: %w", err)
		}
	}
	return nil
}

// SourceFingerprint binds state to a PostgreSQL cluster and database.
func SourceFingerprint(systemID, database string) string {
	sum := sha256.Sum256([]byte(systemID + "\x00" + database))
	return hex.EncodeToString(sum[:])
}

var safeNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Names returns deterministic identifiers within PostgreSQL's 63-byte limit.
func Names(sourceFingerprint, migrationID string) (publication, slot string) {
	sum := sha256.Sum256([]byte(sourceFingerprint + "\x00" + migrationID))
	suffix := hex.EncodeToString(sum[:8])
	return "pgmigrate_pub_" + suffix, "pgmigrate_slot_" + suffix
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func writeSnapshotAtomic(dir string, snapshot Snapshot) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create migration directory: %w", err)
	}
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot metadata: %w", err)
	}
	data = append(data, '\n')
	tmp := filepath.Join(dir, "snapshot.json.tmp")
	final := filepath.Join(dir, "snapshot.json")
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create snapshot metadata: %w", err)
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write snapshot metadata: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync snapshot metadata: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close snapshot metadata: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("publish snapshot metadata: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		_ = os.Remove(final)
		return fmt.Errorf("open migration directory for sync: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		_ = os.Remove(final)
		return fmt.Errorf("sync migration directory: %w", err)
	}
	ok = true
	return nil
}
