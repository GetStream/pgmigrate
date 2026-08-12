//go:build integration

package indexbuild

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pgx/v5"
)

func TestPG17IndexesAndForeignKeys(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	src := source.Connect(t)
	dst := target.Connect(t)
	_, err := src.Exec(ctx, `
		CREATE TABLE parent (
			id bigint PRIMARY KEY,
			code text,
			CONSTRAINT parent_code_key UNIQUE(code) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TABLE child (
			id bigint PRIMARY KEY,
			parent_id bigint REFERENCES parent(id)
		);
		CREATE INDEX child_parent_idx ON child(parent_id);
		CREATE TABLE booking (
			id bigint,
			during int8range,
			CONSTRAINT booking_no_overlap EXCLUDE USING gist (during WITH &&)
		);
		CREATE TABLE heap_only (value text)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Exec(ctx, `
		CREATE TABLE parent (id bigint, code text);
		CREATE TABLE child (id bigint, parent_id bigint);
		CREATE TABLE booking (id bigint, during int8range);
		CREATE TABLE heap_only (value text)`); err != nil {
		t.Fatal(err)
	}
	indexes, constraints, err := Inventory(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, index := range indexes {
		if index.Name == "child_parent_idx" || index.Name == "parent_pkey" {
			if _, err := dst.Exec(ctx, index.Definition); err != nil {
				t.Fatalf("simulate crash-after-index-create: %v", err)
			}
		}
	}
	seen := map[uint32]bool{}
	for _, constraint := range constraints {
		if !seen[constraint.TableOID] {
			if err := store.UpsertTable(ctx, state.Table{OID: constraint.TableOID, Schema: constraint.Schema, Name: constraint.Table}); err != nil {
				t.Fatal(err)
			}
			seen[constraint.TableOID] = true
		}
	}
	for _, index := range indexes {
		if !seen[index.TableOID] {
			if err := store.UpsertTable(ctx, state.Table{OID: index.TableOID, Schema: index.Schema, Name: index.Table}); err != nil {
				t.Fatal(err)
			}
			seen[index.TableOID] = true
		}
	}
	hookCalled := false
	runner := Runner{
		Target:                        func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, target.URI) },
		Workers:                       2,
		State:                         store,
		MaintenanceWorkMem:            "16MB",
		MaxParallelMaintenanceWorkers: 1,
		AfterManaged:                  func(context.Context) error { hookCalled = true; return nil },
	}
	if err := runner.Run(ctx, indexes, constraints); err != nil {
		t.Fatal(err)
	}
	var primary, unique, foreign, exclusion, validated, deferrable, deferred bool
	if err := dst.QueryRow(
		ctx, `
		SELECT bool_or(contype='p'), bool_or(contype='u'), bool_or(contype='f'),
		       bool_or(contype='x'),
		       bool_and(CASE WHEN contype='f' THEN convalidated ELSE true END),
		       bool_or(conname='parent_code_key' AND condeferrable),
		       bool_or(conname='parent_code_key' AND condeferred)
		FROM pg_constraint
		WHERE conrelid IN ('parent'::regclass,'child'::regclass,'booking'::regclass)`,
	).Scan(&primary, &unique, &foreign, &exclusion, &validated, &deferrable, &deferred); err != nil {
		t.Fatal(err)
	}
	if !primary || !unique || !foreign || !exclusion || !validated || !deferrable || !deferred {
		t.Fatalf("constraints primary=%v unique=%v foreign=%v exclusion=%v validated=%v deferrable=%v deferred=%v",
			primary, unique, foreign, exclusion, validated, deferrable, deferred)
	}
	if !hookCalled {
		t.Fatal("deferred restore hook was not called")
	}
	// Statistics are the orchestrator's job now, through VACUUM (ANALYZE) after
	// this phase. That is not a tidy-up: the verifier needs the visibility map
	// that only VACUUM populates, so analyzing here would leave the target
	// needing a second pass over every table.
	var analyzed bool
	if err := dst.QueryRow(ctx, `
		SELECT bool_or(last_analyze IS NOT NULL) FROM pg_stat_all_tables
		WHERE schemaname='public'`).Scan(&analyzed); err != nil {
		t.Fatal(err)
	}
	if analyzed {
		t.Fatal("index build gathered statistics; the vacuum phase is meant to")
	}
}

// seedInventory records the tables, indexes and constraints that Runner.Run
// registers before it builds anything, so that tests calling ensureIndex or
// ensureConstraint directly start from the same state. Entries on a partition
// child point at the root of their partition tree, which is registered from the
// root's own entries.
func seedInventory(ctx context.Context, t *testing.T, store *state.Store, indexes []Index, constraints []Constraint) {
	t.Helper()
	seen := map[uint32]bool{}
	upsert := func(oid uint32, schema, name string) {
		if seen[oid] {
			return
		}
		if err := store.UpsertTable(ctx, state.Table{OID: oid, Schema: schema, Name: name}); err != nil {
			t.Fatal(err)
		}
		seen[oid] = true
	}
	for _, x := range indexes {
		if x.SelectedOID == x.TableOID {
			upsert(x.SelectedOID, x.Schema, x.Table)
		}
	}
	for _, c := range constraints {
		if c.SelectedOID == c.TableOID {
			upsert(c.SelectedOID, c.Schema, c.Table)
		}
	}
	for _, x := range indexes {
		if err := store.UpsertIndex(ctx, state.Index{
			OID: x.OID, TableOID: x.SelectedOID,
			Name: x.Name, Definition: x.Definition, Bytes: x.Bytes,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range constraints {
		if err := store.UpsertConstraint(ctx, state.Constraint{
			OID: c.OID, TableOID: c.SelectedOID,
			Name: c.Name, Kind: c.Kind, Definition: c.Definition,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func openStore(ctx context.Context, t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestPG17IndexSurvivesSearchPathRendering covers a false index collision that
// no resume could clear. pg_get_indexdef renders a reference to an object bare
// when its schema is on the reading session's search_path and qualified when it
// is not, so a source that keeps its objects on the path hands out a definition
// the target cannot even execute, and the two sides never agree on the text.
// The production case was a GIN index over a text search configuration in the
// schema both databases keep their objects in.
func TestPG17IndexSurvivesSearchPathRendering(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	const fixture = `
		CREATE SCHEMA shard;
		CREATE TEXT SEARCH CONFIGURATION shard.unaccent_simple (COPY = pg_catalog.simple);
		CREATE TABLE shard.users (id bigint, deleted_at timestamptz)`
	for _, instance := range []*pgtest.Instance{source, target} {
		if _, err := instance.Connect(t).Exec(ctx, fixture); err != nil {
			t.Fatal(err)
		}
	}
	// Only the source carries the schema on its path, so only the source renders
	// the configuration bare. The target cannot resolve a bare reference.
	src := source.Connect(t)
	if _, err := src.Exec(ctx, `
		ALTER DATABASE pgmigrate SET search_path = shard, public;
		CREATE INDEX gin_autocomplete_id_idx ON shard.users USING gin
			(to_tsvector('shard.unaccent_simple'::regconfig, (id)::text))
			WHERE deleted_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	unpinned := source.Connect(t)
	var rendered string
	if err := unpinned.QueryRow(ctx,
		"SELECT pg_get_indexdef('shard.gin_autocomplete_id_idx'::regclass)").Scan(&rendered); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(rendered, "shard.unaccent_simple") {
		t.Fatalf("fixture does not reproduce the rendering difference: %s", rendered)
	}

	indexes, constraints, err := Inventory(ctx, source.Connect(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 {
		t.Fatalf("indexes = %#v", indexes)
	}
	if !strings.Contains(indexes[0].Definition, "shard.unaccent_simple") {
		t.Fatalf("inventory definition is not schema-qualified: %s", indexes[0].Definition)
	}
	if !indexes[0].Expression {
		t.Fatalf("index over an expression not reported as one: %#v", indexes[0])
	}
	store := openStore(ctx, t)
	seedInventory(ctx, t, store, indexes, constraints)
	runner := Runner{
		Target:  func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, target.URI) },
		Workers: 1, State: store,
	}
	if err := runner.Run(ctx, indexes, nil); err != nil {
		t.Fatalf("build index whose expression names another schema: %v", err)
	}
	// The recorded expectation is the target's own rendering, so a resume that
	// re-reads the index recognises it.
	recorded, err := store.IndexTargetDefinition(ctx, indexes[0].OID)
	if err != nil {
		t.Fatal(err)
	}
	if recorded == "" {
		t.Fatal("target rendering was not recorded")
	}
	if err := runner.ensureIndex(ctx, indexes[0]); err != nil {
		t.Fatalf("resume over an index pgmigrate built: %v", err)
	}
	// A state store with no recording reaches the first-sight path that a crash
	// between creating an index and recording it leaves behind.
	fresh := openStore(ctx, t)
	seedInventory(ctx, t, fresh, indexes, constraints)
	firstSight := Runner{Target: runner.Target, Workers: 1, State: fresh}
	if err := firstSight.ensureIndex(ctx, indexes[0]); err != nil {
		t.Fatalf("adopt an index pgmigrate built but did not record: %v", err)
	}
}

// TestPG17ConstraintSurvivesDeparseRoundTrip covers a false constraint collision
// with no operator workaround. PostgreSQL pushes a cast on an array down onto
// its elements, so a constraint whose stored parse tree predates that
// normalization deparses differently from the same constraint parsed today, and
// no DDL can make a current server reproduce the older text.
//
// A source of a supported version normalizes on the way in, so the older
// rendering is supplied directly; that text is exactly what the affected
// production source reported.
func TestPG17ConstraintSurvivesDeparseRoundTrip(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	const fixture = `
		CREATE TABLE sync_queue (
			id bigint,
			direction character varying,
			CONSTRAINT sync_queue_direction_check CHECK (direction::text = ANY
				(ARRAY['promote'::character varying, 'demote'::character varying]::text[]))
		)`
	for _, instance := range []*pgtest.Instance{source, target} {
		if _, err := instance.Connect(t).Exec(ctx, fixture); err != nil {
			t.Fatal(err)
		}
	}
	indexes, constraints, err := Inventory(ctx, source.Connect(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(constraints) != 1 {
		t.Fatalf("constraints = %#v", constraints)
	}
	store := openStore(ctx, t)
	seedInventory(ctx, t, store, indexes, constraints)
	runner := Runner{
		Target:  func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, target.URI) },
		Workers: 1, State: store,
	}
	carried := constraints[0]
	carried.Definition = "CHECK (direction::text = ANY " +
		"(ARRAY['promote'::character varying, 'demote'::character varying]::text[]))"
	if err := runner.ensureConstraint(ctx, carried); err != nil {
		t.Fatalf("constraint carried forward from an older parse tree: %v", err)
	}
	recorded, err := store.ConstraintTargetDefinition(ctx, carried.OID)
	if err != nil {
		t.Fatal(err)
	}
	// The target distributes the cast onto each element, which is exactly the
	// difference that used to be reported as a collision.
	if strings.Contains(recorded, "]::text[]") || !strings.Contains(recorded, "character varying::text") {
		t.Fatalf("recorded expectation is not the target's rendering: %q", recorded)
	}

	// The true positive: a predicate that really differs is still refused, which
	// is what separates a rendering artifact from drift. A fresh state store puts
	// the comparison back on the first-sight path.
	fresh := openStore(ctx, t)
	seedInventory(ctx, t, fresh, indexes, constraints)
	diverged := carried
	diverged.Definition = "CHECK (direction::text = ANY " +
		"(ARRAY['promote'::character varying]::text[]))"
	err = Runner{Target: runner.Target, Workers: 1, State: fresh}.ensureConstraint(ctx, diverged)
	var collision *CollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("error=%v, want CollisionError for a different predicate", err)
	}
}

// TestPG17PartitionedTableIndexesAndConstraints covers a partitioned table's
// whole index and constraint story, none of which worked.
//
// A primary key on a partitioned parent cannot be built as a bare index and
// promoted, because PostgreSQL rejects ADD CONSTRAINT ... USING INDEX there, so
// the phase failed outright. A parent's index is rendered ON ONLY and stays
// invalid until every partition's index is attached to it, and nothing attached
// them, so on a target where the partitions did exist the planner ignored every
// parent index. A foreign key on a partitioned parent cannot be added NOT VALID
// before PostgreSQL 18, which the deferred validation path assumed. And a
// foreign key that references a partitioned table is held as one clone per
// partition, none of which VALIDATE CONSTRAINT reaches, so adding it NOT VALID
// left the target permanently unlike the source.
func TestPG17PartitionedTableIndexesAndConstraints(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	src := source.Connect(t)
	if _, err := src.Exec(ctx, `
		CREATE SCHEMA shard;
		CREATE TABLE shard.tenants (id bigint PRIMARY KEY);
		CREATE TABLE shard.results (
			app_pk bigint NOT NULL,
			id bigint NOT NULL,
			created_at date NOT NULL,
			tenant_id bigint,
			label text,
			CONSTRAINT results_label_check CHECK (label <> ''),
			CONSTRAINT results_pkey PRIMARY KEY (app_pk, id, created_at),
			CONSTRAINT results_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES shard.tenants(id)
		) PARTITION BY RANGE (created_at);
		CREATE TABLE shard.results_2024 PARTITION OF shard.results
			FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		CREATE TABLE shard.results_default PARTITION OF shard.results DEFAULT;
		CREATE INDEX results_label_idx ON shard.results (label);
		CREATE TABLE shard.result_notes (
			id bigint PRIMARY KEY,
			app_pk bigint NOT NULL,
			result_id bigint NOT NULL,
			created_at date NOT NULL,
			CONSTRAINT result_notes_result_fkey
				FOREIGN KEY (app_pk, result_id, created_at)
				REFERENCES shard.results (app_pk, id, created_at)
		);
		INSERT INTO shard.tenants VALUES (1);
		INSERT INTO shard.results VALUES (1,1,'2024-06-06',1,'a'),(1,2,'2030-06-06',1,'b');
		INSERT INTO shard.result_notes VALUES (1,1,1,'2024-06-06')`); err != nil {
		t.Fatal(err)
	}
	// The target is left in the shape the schema restore produces: partitions
	// created as standalone tables and then attached, CHECK constraints inline,
	// no index or deferred constraint, and the rows already copied through the
	// partitioned parent.
	dst := target.Connect(t)
	if _, err := dst.Exec(ctx, `
		CREATE SCHEMA shard;
		CREATE TABLE shard.tenants (id bigint NOT NULL);
		CREATE TABLE shard.results (
			app_pk bigint NOT NULL, id bigint NOT NULL, created_at date NOT NULL,
			tenant_id bigint, label text,
			CONSTRAINT results_label_check CHECK (label <> '')
		) PARTITION BY RANGE (created_at);
		CREATE TABLE shard.results_2024 (
			app_pk bigint NOT NULL, id bigint NOT NULL, created_at date NOT NULL,
			tenant_id bigint, label text,
			CONSTRAINT results_label_check CHECK (label <> ''));
		CREATE TABLE shard.results_default (
			app_pk bigint NOT NULL, id bigint NOT NULL, created_at date NOT NULL,
			tenant_id bigint, label text,
			CONSTRAINT results_label_check CHECK (label <> ''));
		ALTER TABLE ONLY shard.results ATTACH PARTITION shard.results_2024
			FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		ALTER TABLE ONLY shard.results ATTACH PARTITION shard.results_default DEFAULT;
		CREATE TABLE shard.result_notes (
			id bigint NOT NULL, app_pk bigint NOT NULL,
			result_id bigint NOT NULL, created_at date NOT NULL);
		INSERT INTO shard.tenants VALUES (1);
		INSERT INTO shard.results VALUES (1,1,'2024-06-06',1,'a'),(1,2,'2030-06-06',1,'b');
		INSERT INTO shard.result_notes VALUES (1,1,1,'2024-06-06')`); err != nil {
		t.Fatal(err)
	}
	// Only the copied tables are selected, as the copy inventory reports them:
	// the partitioned root and the ordinary table, never the partitions.
	rows, err := src.Query(ctx, `
		SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname='shard' AND c.relkind IN ('r','p') AND NOT c.relispartition`)
	if err != nil {
		t.Fatal(err)
	}
	selected := map[uint32]bool{}
	for rows.Next() {
		var oid uint32
		if err := rows.Scan(&oid); err != nil {
			t.Fatal(err)
		}
		selected[oid] = true
	}
	rows.Close()
	if len(selected) != 3 {
		t.Fatalf("selected %d root tables, want 3", len(selected))
	}
	indexes, constraints, err := Inventory(ctx, source.Connect(t), func(oid uint32) bool { return selected[oid] })
	if err != nil {
		t.Fatal(err)
	}
	// A partition's indexes belong to a selected table through the root of its
	// partition tree, so they are inventoried even though the partition itself is
	// never selected.
	found := map[string]Index{}
	for _, x := range indexes {
		found[x.Name] = x
	}
	for _, name := range []string{
		"results_pkey", "results_label_idx",
		"results_2024_pkey", "results_2024_label_idx",
		"results_default_pkey", "results_default_label_idx",
	} {
		if _, ok := found[name]; !ok {
			t.Fatalf("index %q is missing from the inventory: %#v", name, found)
		}
	}
	if !found["results_pkey"].Partitioned || found["results_pkey"].ConstraintName != "results_pkey" {
		t.Fatalf("parent primary key index = %#v", found["results_pkey"])
	}
	if found["results_2024_pkey"].ParentIndexName != "results_pkey" {
		t.Fatalf("partition primary key index = %#v", found["results_2024_pkey"])
	}
	// A partition's inherited copy of a parent constraint is not inventoried:
	// adding the constraint to the parent creates it.
	var referencing *Constraint
	for i, c := range constraints {
		if c.Table == "results_2024" || c.Table == "results_default" {
			t.Fatalf("inherited constraint %s on %s should not be inventoried", c.Name, c.Table)
		}
		if c.Name == "result_notes_result_fkey" {
			referencing = &constraints[i]
		}
	}
	if referencing == nil {
		t.Fatal("the foreign key referencing the partitioned table is missing from the inventory")
	}
	if referencing.Partitioned || !referencing.ReferencesPartitioned {
		t.Fatalf("foreign key referencing a partitioned table = %#v", *referencing)
	}

	store := openStore(ctx, t)
	seedInventory(ctx, t, store, indexes, constraints)
	runner := Runner{
		Target:  func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, target.URI) },
		Workers: 2, State: store,
	}
	if err := runner.Run(ctx, indexes, constraints); err != nil {
		t.Fatalf("build partitioned table indexes: %v", err)
	}

	// Every index on a partitioned parent is valid, which only holds once all of
	// its partitions' indexes are attached to it.
	var invalid []string
	rows, err = dst.Query(ctx, `
		SELECT ic.relname FROM pg_index i
		JOIN pg_class ic ON ic.oid=i.indexrelid
		JOIN pg_class t ON t.oid=i.indrelid
		JOIN pg_namespace n ON n.oid=t.relnamespace
		WHERE n.nspname='shard' AND NOT i.indisvalid`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		invalid = append(invalid, name)
	}
	rows.Close()
	if len(invalid) != 0 {
		t.Fatalf("invalid indexes on the target: %v", invalid)
	}
	var attached int
	if err := dst.QueryRow(ctx, `
		SELECT count(*) FROM pg_inherits h
		JOIN pg_class child ON child.oid=h.inhrelid AND child.relkind='i'`).Scan(&attached); err != nil {
		t.Fatal(err)
	}
	if attached != 4 {
		t.Fatalf("attached partition indexes = %d, want 4", attached)
	}
	var validated bool
	if err := dst.QueryRow(ctx, `
		SELECT convalidated FROM pg_constraint
		WHERE conrelid='shard.results'::regclass AND conname='results_tenant_fkey'`).Scan(&validated); err != nil {
		t.Fatalf("foreign key on a partitioned parent: %v", err)
	}
	if !validated {
		t.Fatal("foreign key on a partitioned parent was left unvalidated")
	}
	// A foreign key that references a partitioned table is held as one clone per
	// partition. VALIDATE CONSTRAINT marks only the constraint it is given, so a
	// key added NOT VALID and validated afterwards leaves every clone unvalidated
	// for good, which no later statement can repair.
	unvalidated := func(db *pgx.Conn) string {
		var names string
		if err := db.QueryRow(ctx, `
			SELECT coalesce(string_agg(c.conname, ',' ORDER BY c.conname), '')
			FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace
			WHERE n.nspname='shard' AND c.contype IN ('c','f') AND NOT c.convalidated`).
			Scan(&names); err != nil {
			t.Fatal(err)
		}
		return names
	}
	if got, want := unvalidated(dst), unvalidated(src); got != want {
		t.Fatalf("unvalidated constraints on the target = %q, source = %q", got, want)
	}
	if _, err := dst.Exec(ctx,
		"INSERT INTO shard.result_notes VALUES (2,9,9,'2024-06-06')"); err == nil {
		t.Fatal("row accepted against a missing partitioned parent row")
	}
	// The primary key is enforced through the parent, and rows still route.
	if _, err := dst.Exec(ctx,
		"INSERT INTO shard.results VALUES (1,1,'2024-06-06',1,'duplicate')"); err == nil {
		t.Fatal("duplicate row accepted through the partitioned primary key")
	}
	if _, err := dst.Exec(ctx,
		"INSERT INTO shard.results VALUES (2,1,'2024-07-07',1,'routed')"); err != nil {
		t.Fatalf("insert into the migrated partitioned table: %v", err)
	}
	var counts string
	if err := dst.QueryRow(ctx, `
		SELECT string_agg(partition||'='||total, ',' ORDER BY partition) FROM (
			SELECT tableoid::regclass::text AS partition, count(*) AS total
			FROM shard.results GROUP BY 1) totals`).Scan(&counts); err != nil {
		t.Fatal(err)
	}
	if counts != "shard.results_2024=2,shard.results_default=1" {
		t.Fatalf("rows per partition = %s", counts)
	}
	// Resuming re-inspects every object and re-runs the attachments.
	if err := runner.Run(ctx, indexes, constraints); err != nil {
		t.Fatalf("resume over a built partitioned table: %v", err)
	}
}

func TestPG17ForeignKeyCollision(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	src := source.Connect(t)
	dst := target.Connect(t)
	if _, err := src.Exec(ctx, `
		CREATE TABLE parent (id bigint PRIMARY KEY);
		CREATE TABLE child (id bigint PRIMARY KEY, parent_id bigint,
			CONSTRAINT child_parent_fkey FOREIGN KEY(parent_id) REFERENCES parent(id));
		CREATE INDEX collision_idx ON child(parent_id)`); err != nil {
		t.Fatal(err)
	}
	if _, err := dst.Exec(ctx, `
		CREATE TABLE parent (id bigint PRIMARY KEY);
		CREATE TABLE child (id bigint PRIMARY KEY, parent_id bigint,
			CONSTRAINT child_parent_fkey FOREIGN KEY(parent_id) REFERENCES parent(id) ON DELETE CASCADE);
		CREATE INDEX collision_idx ON child(id)`); err != nil {
		t.Fatal(err)
	}
	indexes, constraints, err := Inventory(ctx, src, nil)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, c := range constraints {
		if err := store.UpsertTable(ctx, state.Table{OID: c.TableOID, Schema: c.Schema, Name: c.Table}); err != nil {
			t.Fatal(err)
		}
	}
	runner := Runner{Target: func(ctx context.Context) (*pgx.Conn, error) {
		return pgx.Connect(ctx, target.URI)
	}, Workers: 2, State: store}
	for _, index := range indexes {
		if index.Name == "collision_idx" {
			err = runner.ensureIndex(ctx, index)
			var collision *CollisionError
			if !errors.As(err, &collision) {
				t.Fatalf("index error=%v, want CollisionError", err)
			}
		}
	}
	err = runner.Run(ctx, nil, constraints)
	var collision *CollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("error=%v, want CollisionError", err)
	}
}
