//go:build integration

package copy

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/pgtest"
	"github.com/tgross/pgmigrate/internal/state"
)

type crashCompletionState struct {
	*state.Store
	mu   sync.Mutex
	fail bool
}

func (s *crashCompletionState) CompletePart(ctx context.Context, oid uint32, id string, rows, bytes int64, duration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		s.fail = false
		return errors.New("simulated crash before local completion marker")
	}
	return s.Store.CompletePart(ctx, oid, id, rows, bytes, duration)
}

func TestPG17SnapshotSplitTextAndBinary(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	src := source.Connect(t)
	dst := target.Connect(t)
	_, err := src.Exec(ctx, `
		CREATE TYPE mood AS ENUM ('ok','great');
		CREATE TABLE binary_rows (id bigint PRIMARY KEY, value text);
		INSERT INTO binary_rows SELECT i, repeat(i::text, 20) FROM generate_series(1,100) i;
		CREATE TABLE text_rows (id bigint PRIMARY KEY, value mood);
		INSERT INTO text_rows VALUES (1,'ok'),(2,'great');
		CREATE TABLE generated_rows (
			id bigint PRIMARY KEY,
			doubled bigint GENERATED ALWAYS AS (id * 2) STORED
		);
		INSERT INTO generated_rows VALUES (3);
		CREATE TABLE ctid_rows (value text);
		INSERT INTO ctid_rows SELECT repeat(i::text, 100) FROM generate_series(1,1000) i;
		ANALYZE ctid_rows;
		UPDATE pg_class SET relpages=1 WHERE oid='ctid_rows'::regclass;
		CREATE TABLE text_gucs (
			id bigint PRIMARY KEY,
			marker mood,
			ts timestamptz,
			iv interval,
			f double precision,
			b bytea
		);
		INSERT INTO text_gucs VALUES (
			1, 'ok', '2024-03-31 01:59:59.123456+05:30',
			'1 year 2 mons 3 days 04:05:06.789',
			1.2345678901234567, decode('00ff5c','hex')
		)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dst.Exec(ctx, `
		CREATE TYPE mood AS ENUM ('ok','great');
		CREATE TABLE binary_rows (id bigint PRIMARY KEY, value text);
		CREATE TABLE text_rows (id bigint PRIMARY KEY, value mood);
		CREATE TABLE generated_rows (
			id bigint PRIMARY KEY,
			doubled bigint GENERATED ALWAYS AS (id * 2) STORED
		);
		CREATE TABLE ctid_rows (value text);
		CREATE TABLE text_gucs (
			id bigint PRIMARY KEY,
			marker mood,
			ts timestamptz,
			iv interval,
			f double precision,
			b bytea
		)`)
	if err != nil {
		t.Fatal(err)
	}
	exporter, err := sourceConnect(ctx, source.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Close(ctx)
	tx, err := exporter.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var snapshot string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec(ctx, "DELETE FROM binary_rows WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	sourceDefaults := map[string]string{
		"DateStyle": "SQL, DMY", "IntervalStyle": "sql_standard",
		"TimeZone": "America/New_York", "extra_float_digits": "0", "bytea_output": "escape",
	}
	targetDefaults := map[string]string{
		"DateStyle": "German, DMY", "IntervalStyle": "postgres_verbose",
		"TimeZone": "Asia/Tokyo", "extra_float_digits": "-3", "bytea_output": "hex",
	}
	tables, err := InventorySnapshot(ctx, func(ctx context.Context) (*pgx.Conn, error) {
		return connectWithDefaults(ctx, source.URI, sourceDefaults)
	}, snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	var parts []Part
	for _, table := range tables {
		format := ConservativeFormat(table, 17, 17)
		if table.Name == "binary_rows" {
			if table.KeyMin != 1 {
				t.Fatalf("snapshot boundary min=%d, want 1", table.KeyMin)
			}
			parts = append(parts, Plan(table, 1, 4, format)...)
		} else {
			if table.Name == "text_rows" && format != Text {
				t.Fatalf("enum table selected %s", format)
			}
			if table.Name == "generated_rows" {
				if len(table.Columns) != 2 || !table.Columns[1].Generated {
					t.Fatalf("generated metadata=%#v", table.Columns)
				}
			}
			if table.Name == "ctid_rows" {
				var stalePages int64
				if err := src.QueryRow(ctx, "SELECT relpages FROM pg_class WHERE oid='ctid_rows'::regclass").Scan(&stalePages); err != nil {
					t.Fatal(err)
				}
				if stalePages != 1 || table.HeapBlocks <= stalePages {
					t.Fatalf("stale relpages=%d snapshot heap blocks=%d", stalePages, table.HeapBlocks)
				}
				planned := Plan(table, 1, 4, format)
				if len(planned) < 2 || !strings.HasPrefix(planned[0].ID, "ctid-") {
					t.Fatalf("ctid plan=%#v", planned)
				}
				parts = append(parts, planned...)
			} else {
				parts = append(parts, Plan(table, 0, 1, format)...)
			}
		}
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runner := Runner{
		Source: func(ctx context.Context) (*pgx.Conn, error) {
			return connectWithDefaults(ctx, source.URI, sourceDefaults)
		},
		Target: func(ctx context.Context) (*pgx.Conn, error) {
			return connectWithDefaults(ctx, target.URI, targetDefaults)
		},
		Snapshot: snapshot,
		Workers:  3,
		State:    store,
	}
	if err := runner.Run(ctx, parts); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM binary_rows").Scan(&count); err != nil || count != 100 {
		t.Fatalf("binary row count=%d err=%v", count, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM text_rows").Scan(&count); err != nil || count != 2 {
		t.Fatalf("text row count=%d err=%v", count, err)
	}
	var doubled int
	if err := dst.QueryRow(ctx, "SELECT doubled FROM generated_rows WHERE id=3").Scan(&doubled); err != nil || doubled != 6 {
		t.Fatalf("generated value=%d err=%v", doubled, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM ctid_rows").Scan(&count); err != nil || count != 1000 {
		t.Fatalf("ctid row count=%d err=%v", count, err)
	}
	var textValuesEqual bool
	if err := dst.QueryRow(ctx, `
		SELECT ts='2024-03-31 01:59:59.123456+05:30'::timestamptz
		   AND iv='1 year 2 mons 3 days 04:05:06.789'::interval
		   AND f=1.2345678901234567::double precision
		   AND b=decode('00ff5c','hex')
		FROM text_gucs WHERE id=1`).Scan(&textValuesEqual); err != nil || !textValuesEqual {
		t.Fatalf("cross-default text values equal=%v err=%v", textValuesEqual, err)
	}
}

func sourceConnect(ctx context.Context, uri string) (*pgx.Conn, error) {
	return pgx.Connect(ctx, uri)
}

func TestPG17GeneratedOnlyRowsResumeFromTargetMarker(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	src := source.Connect(t)
	dst := target.Connect(t)
	if _, err := src.Exec(ctx, `
		CREATE TABLE generated_nonempty (
			value integer GENERATED ALWAYS AS (42) STORED
		);
		CREATE TABLE generated_empty (
			value integer GENERATED ALWAYS AS (7) STORED
		);
		DO $$
		BEGIN
			FOR i IN 1..600 LOOP
				INSERT INTO generated_nonempty DEFAULT VALUES;
			END LOOP;
		END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Exec(ctx, `
		CREATE TABLE generated_nonempty (
			value integer GENERATED ALWAYS AS (42) STORED
		);
		CREATE TABLE generated_empty (
			value integer GENERATED ALWAYS AS (7) STORED
		)`); err != nil {
		t.Fatal(err)
	}
	exporter, err := pgx.Connect(ctx, source.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Close(ctx)
	tx, err := exporter.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var snapshot string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	tables, err := InventorySnapshot(ctx, func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, source.URI)
	}, snapshot, func(_, table string) bool { return strings.HasPrefix(table, "generated_") })
	if err != nil {
		t.Fatal(err)
	}
	var parts []Part
	for _, table := range tables {
		if got := columnList(table.Columns); got != "" {
			t.Fatalf("%s writable columns=%q", table.Name, got)
		}
		parts = append(parts, Plan(table, 0, 1, ConservativeFormat(table, 17, 17))...)
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "generated-only"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	crashState := &crashCompletionState{Store: store, fail: true}
	runner := Runner{
		Source:   func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, source.URI) },
		Target:   func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, target.URI) },
		Snapshot: snapshot,
		Workers:  1,
		State:    crashState,
	}
	if err := runner.Run(ctx, parts); err == nil {
		t.Fatal("first run unexpectedly survived simulated crash")
	}
	var count int
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM generated_nonempty").Scan(&count); err != nil || count != 600 {
		t.Fatalf("post-crash rows=%d err=%v", count, err)
	}
	if err := runner.Run(ctx, parts); err != nil {
		t.Fatal(err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM generated_nonempty WHERE value=42").Scan(&count); err != nil || count != 600 {
		t.Fatalf("resumed rows=%d err=%v", count, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM generated_empty").Scan(&count); err != nil || count != 0 {
		t.Fatalf("empty generated rows=%d err=%v", count, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM pgmigrate_internal.copy_parts").Scan(&count); err != nil || count != 2 {
		t.Fatalf("target markers=%d err=%v", count, err)
	}
}

// TestPG17PartsCopyIntoTheirOwnTable pins that every part shape writes straight
// into the table it names. Splitting used to stage a part in a session-local
// temp table and drain it with INSERT ... SELECT, which pinned more local
// buffers than temp_buffers holds once the rows had to be detoasted back out of
// the temp table's TOAST relation, and which discarded an unsplit partitioned
// table's rows outright because only split parts were ever drained.
func TestPG17PartsCopyIntoTheirOwnTable(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	src := source.Connect(t)
	dst := target.Connect(t)
	// STORAGE EXTERNAL pushes every value out of line uncompressed, so each row
	// is read back through the TOAST relation rather than the main fork.
	const schema = `
		CREATE TABLE wide_toast (id bigint PRIMARY KEY, a text, b text, c text, d text);
		ALTER TABLE wide_toast ALTER COLUMN a SET STORAGE EXTERNAL,
		                       ALTER COLUMN b SET STORAGE EXTERNAL,
		                       ALTER COLUMN c SET STORAGE EXTERNAL,
		                       ALTER COLUMN d SET STORAGE EXTERNAL;
		CREATE TABLE events (id bigint, at date, note text) PARTITION BY RANGE (at);
		CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')`
	if _, err := src.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec(ctx, `
		INSERT INTO wide_toast
		SELECT i, repeat(md5(i::text), 100), repeat(md5(i::text), 100),
		          repeat(md5(i::text), 100), repeat(md5(i::text), 100)
		FROM generate_series(1,400) i;
		INSERT INTO events
		SELECT i, ('2024-01-01'::date + (i * 100)), 'note ' || i
		FROM generate_series(1,6) i`); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	exporter, err := pgx.Connect(ctx, source.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Close(ctx)
	tx, err := exporter.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var snapshot string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	tables, err := InventorySnapshot(ctx, func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, source.URI)
	}, snapshot, nil)
	if err != nil {
		t.Fatal(err)
	}
	var parts []Part
	for _, table := range tables {
		format := ConservativeFormat(table, 17, 17)
		switch table.Name {
		case "wide_toast":
			planned := Plan(table, 1, 4, format)
			if len(planned) < 2 || !strings.HasPrefix(planned[0].ID, "pk-") {
				t.Fatalf("wide_toast plan=%#v", planned)
			}
			parts = append(parts, planned...)
		case "events":
			planned := Plan(table, 0, 1, format)
			if len(planned) != 1 || !planned[0].Unsplit || planned[0].Table.RelKind != "p" {
				t.Fatalf("events plan=%#v", planned)
			}
			parts = append(parts, planned...)
		default:
			t.Fatalf("unexpected table %s", table.Name)
		}
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "staging"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// The PostgreSQL minimum, well under the size of the staged part. Draining a
	// staged part fails here exactly as it did against a target left at the 8 MB
	// default with production-sized parts.
	minimalLocalBuffers := map[string]string{"temp_buffers": "100"}
	runner := Runner{
		Source: func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, source.URI) },
		Target: func(ctx context.Context) (*pgx.Conn, error) {
			return connectWithDefaults(ctx, target.URI, minimalLocalBuffers)
		},
		Snapshot: snapshot,
		Workers:  2,
		State:    store,
	}
	if err := runner.Run(ctx, parts); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := dst.QueryRow(ctx, `
		SELECT count(*) FROM wide_toast
		WHERE a=repeat(md5(id::text),100) AND d=repeat(md5(id::text),100)`).Scan(&count); err != nil || count != 400 {
		t.Fatalf("detoasted split rows=%d err=%v", count, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil || count != 6 {
		t.Fatalf("unsplit partitioned rows=%d err=%v", count, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM events_2025").Scan(&count); err != nil || count == 0 {
		t.Fatalf("routed partition rows=%d err=%v", count, err)
	}
	// A temp schema is created on first use and outlives the session that made
	// it, so its absence proves no part staged anything.
	if err := dst.QueryRow(ctx, `SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'pg\_temp%'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("target temp schemas=%d err=%v", count, err)
	}
	// Copying into the real table is only safe if a part that runs a second time
	// replaces its own rows rather than adding them, which for a split part is
	// the range delete and for an unsplit part the truncate. Forgetting both
	// completion markers is the strongest form of that: every part runs again
	// against a target that already holds its rows.
	if _, err := dst.Exec(ctx, "DELETE FROM pgmigrate_internal.copy_parts"); err != nil {
		t.Fatal(err)
	}
	replay, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "staging"})
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Close()
	runner.State = replay
	if err := runner.Run(ctx, parts); err != nil {
		t.Fatal(err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM wide_toast").Scan(&count); err != nil || count != 400 {
		t.Fatalf("replayed split rows=%d err=%v", count, err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil || count != 6 {
		t.Fatalf("replayed partitioned rows=%d err=%v", count, err)
	}
}

func connectWithDefaults(ctx context.Context, uri string, defaults map[string]string) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(uri)
	if err != nil {
		return nil, err
	}
	for name, value := range defaults {
		config.RuntimeParams[name] = value
	}
	return pgx.ConnectConfig(ctx, config)
}
