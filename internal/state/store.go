package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS migration (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	source_fingerprint TEXT NOT NULL,
	filter_fingerprint TEXT NOT NULL,
	slot_name TEXT NOT NULL DEFAULT '',
	snapshot_name TEXT NOT NULL DEFAULT '',
	consistent_point TEXT NOT NULL DEFAULT '',
	phase TEXT NOT NULL,
	end_position TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tables (
	oid INTEGER PRIMARY KEY,
	schema_name TEXT NOT NULL,
	table_name TEXT NOT NULL,
	estimated_rows INTEGER NOT NULL DEFAULT 0,
	bytes INTEGER NOT NULL DEFAULT 0,
	parts_total INTEGER NOT NULL DEFAULT 0,
	completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
	completed_at INTEGER NOT NULL DEFAULT 0,
	UNIQUE (schema_name, table_name)
);
CREATE TABLE IF NOT EXISTS parts (
	table_oid INTEGER NOT NULL REFERENCES tables(oid) ON DELETE CASCADE,
	part_id TEXT NOT NULL,
	range_start TEXT NOT NULL DEFAULT '',
	range_end TEXT NOT NULL DEFAULT '',
	rows_copied INTEGER NOT NULL DEFAULT 0,
	bytes_copied INTEGER NOT NULL DEFAULT 0,
	duration_ns INTEGER NOT NULL DEFAULT 0,
	completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
	completed_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (table_oid, part_id)
);
CREATE TABLE IF NOT EXISTS indexes (
	oid INTEGER PRIMARY KEY,
	table_oid INTEGER NOT NULL REFERENCES tables(oid) ON DELETE CASCADE,
	name TEXT NOT NULL,
	definition TEXT NOT NULL DEFAULT '',
	target_definition TEXT NOT NULL DEFAULT '',
	bytes INTEGER NOT NULL DEFAULT 0,
	completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
	completed_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS constraints (
	oid INTEGER PRIMARY KEY,
	table_oid INTEGER NOT NULL REFERENCES tables(oid) ON DELETE CASCADE,
	name TEXT NOT NULL,
	kind TEXT NOT NULL DEFAULT '',
	definition TEXT NOT NULL DEFAULT '',
	target_definition TEXT NOT NULL DEFAULT '',
	completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
	completed_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS apply_progress (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	staged_lsn TEXT NOT NULL DEFAULT '',
	applied_lsn TEXT NOT NULL DEFAULT '',
	txns INTEGER NOT NULL DEFAULT 0,
	rows_applied INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL
);
-- One row per table, so a separate pgmigrate status can report what verification
-- is doing, how much of each table it looked at, and how the table came out.
-- sampled_rows against estimated_rows is the load-bearing pair: a check reads a
-- fraction of a large table, and a reader has to be told which fraction because it
-- is no longer all of it.
CREATE TABLE IF NOT EXISTS verify_tables (
	table_oid INTEGER PRIMARY KEY REFERENCES tables(oid) ON DELETE CASCADE,
	stage TEXT NOT NULL DEFAULT '',
	source_pages INTEGER NOT NULL DEFAULT 0,
	source_pages_total INTEGER NOT NULL DEFAULT 0,
	sampled_rows INTEGER NOT NULL DEFAULT 0,
	estimated_rows INTEGER NOT NULL DEFAULT 0,
	target_rows INTEGER NOT NULL DEFAULT 0,
	rows_per_second REAL NOT NULL DEFAULT 0,
	eta_ns INTEGER NOT NULL DEFAULT 0,
	coverage REAL NOT NULL DEFAULT 0,
	candidate_rows INTEGER NOT NULL DEFAULT 0,
	-- The CDC stratum's own figures, kept apart from sampled_rows because they
	-- count different rows found a different way. Added together they would read
	-- as one coverage number, and the cheap half would stand for the expensive
	-- one.
	cdc_keys INTEGER NOT NULL DEFAULT 0,
	cdc_observed INTEGER NOT NULL DEFAULT 0,
	unresolved_rows INTEGER NOT NULL DEFAULT 0,
	converged INTEGER NOT NULL DEFAULT 0 CHECK (converged IN (0, 1)),
	complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
	updated_at INTEGER NOT NULL
);
-- Keys the applier recorded while applying them, which is the only way to check
-- the replication path. A physical sample of the source heap cannot reach it: on
-- a bloated heap, position says nothing about write time, so the rows CDC wrote
-- are scattered wherever the free space map put them.
--
-- WITHOUT ROWID, and no secondary index, because the primary key is the only
-- access path there is: writes replace a known (relation, index), and the one
-- read is a prefix scan of one relation.
CREATE TABLE IF NOT EXISTS cdc_samples (
	schema_name TEXT NOT NULL,
	table_name TEXT NOT NULL,
	sample_index INTEGER NOT NULL,
	key TEXT NOT NULL,
	kind TEXT NOT NULL,
	lsn TEXT NOT NULL DEFAULT '',
	observed_at INTEGER NOT NULL,
	PRIMARY KEY (schema_name, table_name, sample_index)
) WITHOUT ROWID;
-- The reservoir's counters. observed is the denominator for CDC coverage, and
-- both counters have to survive a restart: an applier that began again from zero
-- would refill the reservoir from index 0 and replace a sample of the whole
-- stream with a short prefix of its tail.
CREATE TABLE IF NOT EXISTS cdc_sample_streams (
	schema_name TEXT NOT NULL,
	table_name TEXT NOT NULL,
	observed INTEGER NOT NULL DEFAULT 0,
	retained INTEGER NOT NULL DEFAULT 0,
	dropped INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL,
	PRIMARY KEY (schema_name, table_name)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS findings (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	severity TEXT NOT NULL,
	message TEXT NOT NULL,
	resolved INTEGER NOT NULL DEFAULT 0 CHECK (resolved IN (0, 1)),
	observed_at INTEGER NOT NULL,
	resolved_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS steps (
	name TEXT PRIMARY KEY,
	detail TEXT NOT NULL DEFAULT '',
	completed INTEGER NOT NULL DEFAULT 0 CHECK (completed IN (0, 1)),
	completed_at INTEGER NOT NULL DEFAULT 0
);
-- How the previous run died. Deliberately outside ResetBaseCopy's reach: its
-- only purpose is to be readable by the attempt that follows a reset.
CREATE TABLE IF NOT EXISTS failed_attempt (
	id INTEGER PRIMARY KEY CHECK (id = 1),
	phase TEXT NOT NULL DEFAULT '',
	signature TEXT NOT NULL DEFAULT '',
	detail TEXT NOT NULL DEFAULT '',
	consecutive INTEGER NOT NULL DEFAULT 0,
	observed_at INTEGER NOT NULL DEFAULT 0
);
`

// schemaVersion is the state schema this binary reads and writes. It must be
// raised whenever schema changes, together with a migration below.
//
// The CREATE TABLE statements above describe the current schema and only ever
// run for a table that does not exist yet, so they do nothing for a directory
// created by an older binary. Migrations are what carry such a directory
// forward. This matters more here than in most schemas: the whole value of the
// state directory is resuming a copy that took hours, so a directory that
// cannot be migrated means starting over.
const schemaVersion = 6

// migrations upgrade an existing state directory to schemaVersion. Index i moves
// version i to version i+1. Each has to tolerate the change already being
// present, because a newly created directory gets the current schema from the
// CREATE TABLE statements above and then runs every migration over it.
var migrations = []func(context.Context, *sql.Tx) error{
	// 1 -> 2: record the target's own rendering of an index or constraint, so a
	// resume compares two renderings from the same server rather than comparing
	// the source's text to the target's. An empty recording means "not yet
	// recorded", which is what every row of an upgraded directory holds and what
	// matchIndex and matchConstraint already treat as absent.
	1: func(ctx context.Context, tx *sql.Tx) error {
		for _, table := range []string{"indexes", "constraints"} {
			if err := addColumn(ctx, tx, table, "target_definition", "TEXT NOT NULL DEFAULT ''"); err != nil {
				return err
			}
		}
		return nil
	},
	// 2 -> 3: the verification fence and its progress reporting. Both are gone
	// again at version 4, so this migration only has to leave a version-2
	// directory in a shape version 4 can then upgrade, which it already is.
	2: func(context.Context, *sql.Tx) error { return nil },
	// 3 -> 4: verification compares page ranges and bucket vectors instead of key
	// ranges, so its records change shape and its fence disappears. There is
	// nothing to carry forward: a verification result describes a comparison that
	// has been superseded, and the fence columns describe a mechanism that no
	// longer exists. verify_tables is created by the statements above.
	3: func(ctx context.Context, tx *sql.Tx) error {
		for _, table := range []string{"verify_ranges", "verify_checks"} {
			if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
				return fmt.Errorf("drop %s: %w", table, err)
			}
		}
		for _, column := range []string{"fence_position", "fence_expires_at", "fence_reached"} {
			if err := dropColumn(ctx, tx, "migration", column); err != nil {
				return err
			}
		}
		return nil
	},
	// 4 -> 5: verification samples the source and looks the sampled rows up on the
	// target, so the target no longer reads pages and there are no buckets to
	// disagree about. What replaces them is how much of the table was looked at,
	// which a reader now has to be told explicitly because it is no longer all of
	// it.
	4: func(ctx context.Context, tx *sql.Tx) error {
		for _, column := range []string{
			"target_pages", "target_pages_total", "divergent_buckets", "rows_done",
		} {
			if err := dropColumn(ctx, tx, "verify_tables", column); err != nil {
				return err
			}
		}
		for _, column := range []string{
			"sampled_rows", "estimated_rows", "target_rows", "candidate_rows",
		} {
			if err := addColumn(ctx, tx, "verify_tables", column, "INTEGER NOT NULL DEFAULT 0"); err != nil {
				return err
			}
		}
		return nil
	},
	// 5 -> 6: the CDC stratum. Its two tables are created by the statements
	// above, and an upgraded directory correctly starts with an empty reservoir:
	// a sample describes changes an applier observed, and no applier before this
	// version observed any. Verification's per-table row gains the stratum's
	// figures, which start at zero for the same reason.
	5: func(ctx context.Context, tx *sql.Tx) error {
		for _, column := range []string{"cdc_keys", "cdc_observed"} {
			if err := addColumn(ctx, tx, "verify_tables", column, "INTEGER NOT NULL DEFAULT 0"); err != nil {
				return err
			}
		}
		return nil
	},
}

// Store owns one serialized SQLite connection pool. Stores opened with Open
// also own the exclusive migration-directory lock; read-only stores do not.
// A Store is safe for concurrent use.
type Store struct {
	db       *sql.DB
	lockFile *os.File
	readOnly bool
	mu       sync.Mutex
	closed   bool
}

// Open initializes a migration directory and opens its durable state. Existing
// state is accepted only when both fingerprints match.
func Open(ctx context.Context, dir string, fingerprints Fingerprints) (_ *Store, err error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("migration directory is required")
	}
	if strings.TrimSpace(fingerprints.Source) == "" {
		return nil, errors.New("source fingerprint is required")
	}
	if strings.TrimSpace(fingerprints.Filter) == "" {
		return nil, errors.New("filter fingerprint is required")
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create migration directory: %w", err)
	}
	for _, subdir := range []string{"dump", "cdc", "log"} {
		if err := os.MkdirAll(filepath.Join(dir, subdir), 0o750); err != nil {
			return nil, fmt.Errorf("create migration subdirectory %q: %w", subdir, err)
		}
	}

	lockFile, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open migration lock: %w", err)
	}
	defer func() {
		if err != nil {
			_ = lockFile.Close()
		}
	}()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("lock migration directory: %w", err)
	}
	defer func() {
		if err != nil {
			_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		}
	}()

	db, err := sql.Open("sqlite", filepath.Join(dir, "state.db"))
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	if err := configure(ctx, db); err != nil {
		return nil, err
	}
	if err := initialize(ctx, db, fingerprints); err != nil {
		return nil, err
	}
	return &Store{db: db, lockFile: lockFile}, nil
}

// OpenReadOnly opens existing migration state for status and inventory reads.
// It neither creates files nor acquires the migration-directory lock, so it can
// run concurrently with the migration writer.
func OpenReadOnly(ctx context.Context, dir string) (*Store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("migration directory is required")
	}
	filename := filepath.Join(dir, "state.db")
	info, err := os.Stat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrStateNotFound, filename)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect state database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrStateNotFound, filename)
	}

	absolute, err := filepath.Abs(filename)
	if err != nil {
		return nil, fmt.Errorf("resolve state database path: %w", err)
	}
	dsn := (&url.URL{Scheme: "file", Path: absolute, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open state database read-only: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = db.Close()
		}
	}()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("connect state database read-only: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA query_only = ON"); err != nil {
		return nil, fmt.Errorf("configure read-only state database: %w", err)
	}
	var exists int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM migration WHERE id=1").Scan(&exists)
	if err != nil || exists != 1 {
		if err != nil {
			return nil, fmt.Errorf("%w: invalid state database: %v", ErrStateNotFound, err)
		}
		return nil, fmt.Errorf("%w: migration record is absent", ErrStateNotFound)
	}
	// Checked after the record above so that an absent or unreadable database is
	// still reported as missing state rather than as a version mismatch.
	if err := requireCurrentSchema(ctx, db); err != nil {
		return nil, err
	}

	closeOnError = false
	return &Store{db: db, readOnly: true}, nil
}

// OpenControl opens the state database for a separate cutover controller. It
// does not acquire LOCK (which remains owned by run), but serializes controllers
// with a distinct nonblocking CONTROL lock. SQLite WAL coordinates its writes
// with the main process.
func OpenControl(ctx context.Context, dir string) (_ *Store, err error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("migration directory is required")
	}
	filename := filepath.Join(dir, "state.db")
	if _, err := os.Stat(filename); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrStateNotFound, filename)
		}
		return nil, fmt.Errorf("inspect state database: %w", err)
	}
	lockFile, err := os.OpenFile(filepath.Join(dir, "CONTROL"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open control lock: %w", err)
	}
	defer func() {
		if err != nil {
			_ = lockFile.Close()
		}
	}()
	if err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: cutover controller: %s", ErrLocked, dir)
		}
		return nil, fmt.Errorf("lock cutover controller: %w", err)
	}
	db, err := sql.Open("sqlite", filename)
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		return nil, fmt.Errorf("open control state database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err = configure(ctx, db); err != nil {
		_ = db.Close()
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		return nil, err
	}
	// The controller shares a directory owned by a running migration, so it must
	// not upgrade the schema underneath it. Refuse a mismatch instead.
	if err = requireCurrentSchema(ctx, db); err != nil {
		_ = db.Close()
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		return nil, err
	}
	return &Store{db: db, lockFile: lockFile}, nil
}

func configure(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect state database: %w", err)
	}
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
		return fmt.Errorf("enable SQLite WAL mode: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("enable SQLite WAL mode: SQLite selected %q", mode)
	}
	for _, pragma := range []string{
		"PRAGMA synchronous = FULL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configure SQLite (%s): %w", pragma, err)
		}
	}
	return nil
}

func initialize(ctx context.Context, db *sql.DB, fingerprints Fingerprints) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin state initialization: %w", err)
	}
	defer tx.Rollback()
	if err := migrate(ctx, tx); err != nil {
		return err
	}

	var source, filter string
	err = tx.QueryRowContext(
		ctx,
		"SELECT source_fingerprint, filter_fingerprint FROM migration WHERE id = 1",
	).Scan(&source, &filter)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		now := time.Now().UTC().UnixNano()
		if _, err := tx.ExecContext(
			ctx, `
			INSERT INTO migration
				(id, source_fingerprint, filter_fingerprint, phase, created_at, updated_at)
			VALUES (1, ?, ?, ?, ?, ?)`,
			fingerprints.Source, fingerprints.Filter, PhasePreflight, now, now,
		); err != nil {
			return fmt.Errorf("create migration state: %w", err)
		}
		if _, err := tx.ExecContext(
			ctx,
			"INSERT OR IGNORE INTO apply_progress (id, updated_at) VALUES (1, ?)", now,
		); err != nil {
			return fmt.Errorf("create apply progress: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read migration fingerprints: %w", err)
	case source != fingerprints.Source:
		return &FingerprintMismatchError{Field: "source", Have: source, Want: fingerprints.Source}
	case filter != fingerprints.Filter:
		return &FingerprintMismatchError{Field: "filter", Have: filter, Want: fingerprints.Filter}
	}
	if _, err := tx.ExecContext(
		ctx,
		"INSERT OR IGNORE INTO apply_progress (id, updated_at) VALUES (1, ?)",
		time.Now().UTC().UnixNano(),
	); err != nil {
		return fmt.Errorf("ensure apply progress: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state initialization: %w", err)
	}
	return nil
}

// migrate brings the schema of an existing state directory up to schemaVersion,
// or creates it, in the caller's transaction. The version is raised in the same
// transaction as the statements it describes, so an interrupted upgrade leaves
// the directory at its old version and is simply retried.
func migrate(ctx context.Context, tx *sql.Tx) error {
	version, err := schemaVersionOf(ctx, tx)
	if err != nil {
		return err
	}
	if version > schemaVersion {
		return &SchemaVersionError{Have: version, Want: schemaVersion}
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize state schema: %w", err)
	}
	for from := version; from < schemaVersion; from++ {
		if from >= len(migrations) || migrations[from] == nil {
			continue
		}
		if err := migrations[from](ctx, tx); err != nil {
			return fmt.Errorf("upgrade state schema from version %d to %d: %w", from, from+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return fmt.Errorf("record state schema version: %w", err)
	}
	return nil
}

type sqlQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func schemaVersionOf(ctx context.Context, db sqlQuerier) (int, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return 0, fmt.Errorf("read state schema version: %w", err)
	}
	return version, nil
}

// requireCurrentSchema refuses a state directory that this binary cannot read,
// for the two openers that cannot upgrade one.
func requireCurrentSchema(ctx context.Context, db sqlQuerier) error {
	version, err := schemaVersionOf(ctx, db)
	if err != nil {
		return err
	}
	if version != schemaVersion {
		return &SchemaVersionError{Have: version, Want: schemaVersion}
	}
	return nil
}

func addColumn(ctx context.Context, tx *sql.Tx, table, column, definition string) error {
	var present int
	err := tx.QueryRowContext(
		ctx,
		"SELECT count(*) FROM pragma_table_info(?) WHERE name = ?", table, column,
	).Scan(&present)
	if err != nil {
		return fmt.Errorf("inspect %s.%s: %w", table, column, err)
	}
	if present != 0 {
		return nil
	}
	if _, err := tx.ExecContext(
		ctx,
		"ALTER TABLE "+table+" ADD COLUMN "+column+" "+definition,
	); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

// dropColumn removes a column that a later schema version stopped using, and does
// nothing when it is already absent, which is the state a freshly created directory
// is in before any migration runs over it.
func dropColumn(ctx context.Context, tx *sql.Tx, table, column string) error {
	var present int
	err := tx.QueryRowContext(
		ctx,
		"SELECT count(*) FROM pragma_table_info(?) WHERE name = ?", table, column,
	).Scan(&present)
	if err != nil {
		return fmt.Errorf("inspect %s.%s: %w", table, column, err)
	}
	if present == 0 {
		return nil
	}
	if _, err := tx.ExecContext(
		ctx,
		"ALTER TABLE "+table+" DROP COLUMN "+column,
	); err != nil {
		return fmt.Errorf("drop %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *Store) write(ctx context.Context, fn func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.readOnly {
		return ErrReadOnly
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin state transaction: %w", err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state transaction: %w", err)
	}
	return nil
}

// Close closes SQLite and, for a writer, checkpoints and releases the lock.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	var result error
	if !s.readOnly {
		if _, err := s.db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			result = errors.Join(result, fmt.Errorf("checkpoint state database: %w", err))
		}
	}
	if err := s.db.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("close state database: %w", err))
	}
	if s.lockFile != nil {
		if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN); err != nil {
			result = errors.Join(result, fmt.Errorf("unlock migration directory: %w", err))
		}
		if err := s.lockFile.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("close migration lock: %w", err))
		}
	}
	return result
}

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func fromUnixNano(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}
