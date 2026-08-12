//go:build integration

package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/cdc"
	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/copy"
	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/schema"
	"github.com/GetStream/pgmigrate/internal/setup"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pgx/v5"
)

func TestPG17TargetIdentityRejectsWrongEndpoints(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	wrongSource := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	wrongTarget := pgtest.Start(t, 17)
	fingerprint, err := sourceFingerprint(ctx, source.URI)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: fingerprint, Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetSnapshot(ctx, "pgmigrate_stream", "snapshot", "0/1"); err != nil {
		t.Fatal(err)
	}
	if err := recordTargetIdentity(ctx, target.URI, fingerprint, "filter", "pgmigrate_stream",
		streamGeneration(fingerprint, "filter")); err != nil {
		t.Fatal(err)
	}
	generation := streamGeneration(fingerprint, "filter")
	if err := initializeTargetProgress(ctx, target.URI, "pgmigrate_stream", generation); err != nil {
		t.Fatal(err)
	}
	if err := validateTargetProgress(ctx, target.URI, "pgmigrate_stream", generation); err != nil {
		t.Fatalf("matching target progress: %v", err)
	}
	if err := validateTargetIdentity(ctx, config.Config{Source: source.URI, Target: target.URI}, store); err != nil {
		t.Fatalf("matching identity: %v", err)
	}
	if err := validateTargetIdentity(ctx, config.Config{Source: wrongSource.URI, Target: target.URI}, store); err == nil {
		t.Fatal("wrong source was accepted")
	}
	if err := validateTargetIdentity(ctx, config.Config{Source: source.URI, Target: wrongTarget.URI}, store); err == nil {
		t.Fatal("wrong target was accepted")
	}
	targetConn := target.Connect(t)
	if _, err := targetConn.Exec(ctx,
		"DELETE FROM pgmigrate_internal.replication_progress WHERE stream_id='pgmigrate_stream'"); err != nil {
		t.Fatal(err)
	}
	if err := validateTargetProgress(ctx, target.URI, "pgmigrate_stream", generation); !errors.Is(err, cdc.ErrMissingTargetProgress) {
		t.Fatalf("missing target progress error = %v", err)
	}
}

func TestPG17BaseRestartRecoversSetupObjectsBeforeSnapshotMetadata(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	sourceConn := source.Connect(t)
	if _, err := sourceConn.Exec(ctx, "CREATE TABLE public.stale_setup (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	var oid uint32
	if err := sourceConn.QueryRow(ctx, "SELECT 'public.stale_setup'::regclass::oid").Scan(&oid); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	fingerprint, err := sourceFingerprint(ctx, source.URI)
	if err != nil {
		t.Fatal(err)
	}
	publication, slot := setup.Names(fingerprint, migrationID(dir))
	if _, err := sourceConn.Exec(ctx, "CREATE PUBLICATION "+pgx.Identifier{publication}.Sanitize()+" FOR TABLE public.stale_setup"); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceConn.Exec(ctx, "SELECT * FROM pg_catalog.pg_create_logical_replication_slot($1,'pgoutput')", slot); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(ctx, dir, state.Fingerprints{Source: fingerprint, Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.TransitionPhase(ctx, state.PhaseSetup); err != nil {
		t.Fatal(err)
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := resetInterruptedBaseCopy(ctx, config.Config{
		Source: source.URI, Target: target.URI, Dir: dir,
	}, store, []copy.Table{{OID: oid, Schema: "public", Name: "stale_setup"}}, migration); err != nil {
		t.Fatal(err)
	}
	var artifacts int
	if err := sourceConn.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM pg_catalog.pg_publication WHERE pubname=$1) +
		       (SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name=$2)`,
		publication, slot).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if artifacts != 0 {
		t.Fatalf("stale setup artifacts remaining = %d", artifacts)
	}
}

func TestPG17DumpSelectionCatalogsSequenceAndExtensionDependencies(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	conn := source.Connect(t)
	var available bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name='citext')").Scan(&available); err != nil {
		t.Fatal(err)
	}
	if !available {
		t.Skip("citext unavailable")
	}
	if _, err := conn.Exec(ctx, `
		CREATE EXTENSION citext;
		CREATE TABLE public.selected_dump (
			id serial PRIMARY KEY,
			value citext NOT NULL
		);
		CREATE TABLE public.excluded_dump(id integer PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tables, err := copy.Inventory(ctx, conn, func(schema, table string) bool {
		return schema == "public" && table == "selected_dump"
	})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := dumpSelection(ctx, source.URI, "", tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Tables) != 1 || selection.Tables[0].Name != "selected_dump" {
		t.Fatalf("tables = %#v", selection.Tables)
	}
	if len(selection.DependentRelations) != 1 || selection.DependentRelations[0].Name != "selected_dump_id_seq" {
		t.Fatalf("dependent relations = %#v", selection.DependentRelations)
	}
	if len(selection.Extensions) != 1 || selection.Extensions[0] != "citext" {
		t.Fatalf("extensions = %#v", selection.Extensions)
	}
}

// TestPG17SelectionCarriesPartitionsIntoTheArchive covers a silent data-loss
// defect. The copy inventory represents a partitioned table by its root, so its
// partitions never reached selection, and every archive entry naming one — the
// partition's own CREATE TABLE, its TABLE ATTACH, its indexes and constraints —
// was dropped from the restore list, not commented out but never emitted. The
// target was left holding a partitioned table with no partitions, which reports
// a successful migration and then rejects every row written to it.
func TestPG17SelectionCarriesPartitionsIntoTheArchive(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	conn := source.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE SCHEMA shard;
		CREATE TABLE shard.results (
			app_pk bigint NOT NULL,
			id bigint NOT NULL,
			created_at date NOT NULL,
			label text,
			PRIMARY KEY (app_pk, id, created_at)
		) PARTITION BY RANGE (created_at);
		CREATE TABLE shard.results_2024 PARTITION OF shard.results
			FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
		CREATE TABLE shard.results_default PARTITION OF shard.results DEFAULT;
		CREATE INDEX results_label_idx ON shard.results (label);
		CREATE TABLE shard.ordinary (id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tables, err := copy.Inventory(ctx, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	copied := map[string]bool{}
	for _, table := range tables {
		copied[table.Name] = true
	}
	if len(copied) != 2 || !copied["results"] || !copied["ordinary"] {
		t.Fatalf("copied tables = %#v, want only the partitioned root and the ordinary table", copied)
	}
	selection, err := dumpSelection(ctx, source.URI, "", tables)
	if err != nil {
		t.Fatal(err)
	}
	partitions := map[string]bool{}
	for _, partition := range selection.Partitions {
		partitions[partition.Schema+"."+partition.Name] = true
	}
	if len(partitions) != 2 || !partitions["shard.results_2024"] || !partitions["shard.results_default"] {
		t.Fatalf("partitions = %#v", selection.Partitions)
	}

	// The server's own client tools are used because a host pg_dump may be older
	// than the container and would refuse the archive.
	containerURI := "postgresql://pgmigrate:pgmigrate@127.0.0.1:5432/pgmigrate?sslmode=disable"
	for _, command := range [][]string{
		{
			"pg_dump", "--dbname", containerURI, "--format=custom", "--schema-only",
			"--no-owner", "--no-privileges", "--file", "/tmp/partitioned.dump",
		},
		{"sh", "-c", "pg_restore --list /tmp/partitioned.dump > /tmp/partitioned.list"},
	} {
		code, output, err := source.Container.Exec(ctx, command)
		if err != nil || code != 0 {
			body, _ := io.ReadAll(output)
			t.Fatalf("%v: code=%d err=%v output=%s", command[0], code, err, body)
		}
	}
	listing, err := source.Container.CopyFileFromContainer(ctx, "/tmp/partitioned.list")
	if err != nil {
		t.Fatal(err)
	}
	defer listing.Close()
	data, err := io.ReadAll(listing)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := schema.ParseTOC(data)
	if err != nil {
		t.Fatal(err)
	}
	kept := map[string]bool{}
	preData := map[string]bool{}
	for _, entry := range exactArchiveEntries(entries, selection) {
		key := entry.Description + " " + entry.Namespace + " " + entry.Tag
		kept[key] = true
		if schema.Classify(entry) == schema.PreData {
			preData[key] = true
		}
	}
	// Both partitions and their attachments must be restored with the schema.
	for _, key := range []string{
		"TABLE shard results",
		"TABLE shard results_2024",
		"TABLE shard results_default",
		"TABLE ATTACH shard results_2024",
		"TABLE ATTACH shard results_default",
	} {
		if !preData[key] {
			t.Errorf("%q is not restored with the pre-data schema", key)
		}
	}
	// Their indexes and constraints are retained too, but built by indexbuild.
	for _, key := range []string{
		"CONSTRAINT shard results_2024 results_2024_pkey",
		"INDEX shard results_2024_label_idx",
	} {
		if !kept[key] {
			t.Errorf("%q was dropped from the archive selection", key)
		}
		if preData[key] {
			t.Errorf("%q is restored with the pre-data schema, but indexbuild owns it", key)
		}
	}
}

func TestPG17DeferredInspectorMatchesExactTriggerDefinition(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	for _, database := range []string{source.URI, target.URI} {
		conn, err := pgx.Connect(ctx, database)
		if err != nil {
			t.Fatal(err)
		}
		_, err = conn.Exec(ctx, `
			CREATE TABLE public.deferred_item(id integer PRIMARY KEY);
			CREATE FUNCTION public.deferred_touch() RETURNS trigger
			LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
			CREATE TRIGGER deferred_trigger BEFORE INSERT ON public.deferred_item
			FOR EACH ROW EXECUTE FUNCTION public.deferred_touch()`)
		conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	sourceConn := source.Connect(t)
	var oid uint32
	if err := sourceConn.QueryRow(ctx, `
		SELECT t.oid FROM pg_trigger t
		WHERE t.tgrelid='public.deferred_item'::regclass AND t.tgname='deferred_trigger'`).Scan(&oid); err != nil {
		t.Fatal(err)
	}
	targetConn := target.Connect(t)
	inspect := inspectDeferred(t.TempDir(), source.URI)
	status, err := inspect(ctx, targetConn, schema.TOCEntry{ObjectOID: int64(oid), Description: "TRIGGER"})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Exists || status.Diverged {
		t.Fatalf("matching trigger status = %#v", status)
	}
	if _, err := targetConn.Exec(ctx, "DROP TRIGGER deferred_trigger ON public.deferred_item"); err != nil {
		t.Fatal(err)
	}
	status, err = inspect(ctx, targetConn, schema.TOCEntry{ObjectOID: int64(oid), Description: "TRIGGER"})
	if err != nil {
		t.Fatal(err)
	}
	if status.Exists {
		t.Fatalf("missing trigger status = %#v", status)
	}
}

// commentFixture is applied to both databases so that a matching target proves
// each comment was resolved to the right object rather than merely not found.
const commentFixture = `
	CREATE SCHEMA app;
	CREATE EXTENSION citext WITH SCHEMA app;
	CREATE TYPE app.order_state AS ENUM ('new','done');
	CREATE FUNCTION app.order_label(app.order_state) RETURNS text
		LANGUAGE sql IMMUTABLE AS $$ SELECT $1::text $$;
	CREATE TABLE app.orders (id bigint PRIMARY KEY, handle app.citext, note text);
	CREATE MATERIALIZED VIEW app.order_summary AS SELECT count(*) AS total FROM app.orders;
	COMMENT ON SCHEMA app IS 'application schema';
	COMMENT ON TYPE app.order_state IS 'order lifecycle';
	COMMENT ON FUNCTION app.order_label(app.order_state) IS 'label for a state';
	COMMENT ON TABLE app.orders IS 'customer orders';
	COMMENT ON COLUMN app.orders.note IS 'free-form note';
	COMMENT ON MATERIALIZED VIEW app.order_summary IS 'order rollup'`

// TestPG17CommentEntriesResolveTheirOwnObject covers the deferred COMMENT path
// against real pg_dump output, which nothing exercised before.
//
// pg_dump writes every COMMENT entry with a nil catalog identity and names the
// commented object in the tag without its schema, carrying the schema in a
// separate column. Reading the source comment by object OID therefore looked up
// 0/0 and reported every comment as missing, and matching the bare tag against
// schema-qualified selections dropped table and column comments from the restore
// list entirely.
func TestPG17CommentEntriesResolveTheirOwnObject(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	sourceConn, targetConn := source.Connect(t), target.Connect(t)
	for _, conn := range []*pgx.Conn{sourceConn, targetConn} {
		if _, err := conn.Exec(ctx, commentFixture); err != nil {
			t.Fatal(err)
		}
	}
	tables, err := copy.Inventory(ctx, sourceConn, func(namespace, table string) bool {
		return namespace == "app" && table == "orders"
	})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := dumpSelection(ctx, source.URI, "", tables)
	if err != nil {
		t.Fatal(err)
	}

	// The server's own client tools are used because a host pg_dump may be
	// older than the container and would refuse the archive.
	containerURI := "postgresql://pgmigrate:pgmigrate@127.0.0.1:5432/pgmigrate?sslmode=disable"
	for _, command := range [][]string{
		{
			"pg_dump", "--dbname", containerURI, "--format=custom", "--schema-only",
			"--no-owner", "--no-privileges", "--file", "/tmp/schema.dump",
		},
		{"sh", "-c", "pg_restore --list /tmp/schema.dump > /tmp/schema.list"},
	} {
		code, output, err := source.Container.Exec(ctx, command)
		if err != nil || code != 0 {
			body, _ := io.ReadAll(output)
			t.Fatalf("%v: code=%d err=%v output=%s", command[0], code, err, body)
		}
	}
	listing, err := source.Container.CopyFileFromContainer(ctx, "/tmp/schema.list")
	if err != nil {
		t.Fatal(err)
	}
	defer listing.Close()
	data, err := io.ReadAll(listing)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := schema.ParseTOC(data)
	if err != nil {
		t.Fatal(err)
	}

	// Extension comments are deliberately skipped: CREATE EXTENSION sets them
	// on the target from its own control file.
	deferred := map[string]bool{}
	for _, entry := range entries {
		if entry.Description != "COMMENT" {
			continue
		}
		if strings.HasPrefix(entry.Tag, "EXTENSION ") {
			if schema.Classify(entry) != schema.Skipped {
				t.Errorf("comment on %q is not skipped", entry.Tag)
			}
			continue
		}
		if !selectedTOCEntry(entry, selection) {
			continue
		}
		deferred[entry.Tag] = true
		status, err := inspectComment(ctx, sourceConn, targetConn, entry)
		if err != nil {
			t.Errorf("inspect comment on %q in schema %q: %v", entry.Tag, entry.Namespace, err)
			continue
		}
		if !status.Exists || status.Diverged {
			t.Errorf("comment on %q in schema %q resolved to %#v", entry.Tag, entry.Namespace, status)
		}
	}
	for _, tag := range []string{
		"TABLE orders", "COLUMN orders.note", "SCHEMA app",
		"TYPE order_state", "MATERIALIZED VIEW order_summary",
	} {
		if !deferred[tag] {
			t.Errorf("comment on %q was dropped from the restore list", tag)
		}
	}

	// A target comment on a different object of the same name must not satisfy
	// the entry: that is what an unqualified lookup would accept.
	if _, err := targetConn.Exec(ctx, `
		CREATE SCHEMA other;
		CREATE TABLE other.orders (id bigint PRIMARY KEY);
		COMMENT ON TABLE other.orders IS 'unrelated orders';
		COMMENT ON TABLE app.orders IS 'diverged'`); err != nil {
		t.Fatal(err)
	}
	status, err := inspectComment(ctx, sourceConn, targetConn,
		schema.TOCEntry{Description: "COMMENT", Namespace: "app", Tag: "TABLE orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !status.Exists || !status.Diverged || status.Definition != "diverged" {
		t.Fatalf("diverged comment status = %#v", status)
	}
}

func TestPG17CrossSchemaSelectionUsesExactDependencyClosure(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	conn := source.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE SCHEMA app;
		CREATE SCHEMA shared;
		CREATE TYPE shared.order_state AS ENUM ('new','done');
		CREATE DOMAIN shared.order_code AS text CHECK (VALUE <> '');
		CREATE TYPE shared.order_payload AS (note text);
		CREATE FUNCTION shared.default_code() RETURNS shared.order_code
			LANGUAGE sql IMMUTABLE AS $$ SELECT 'generated'::shared.order_code $$;
		CREATE TABLE app.orders(
			id bigint PRIMARY KEY,
			state shared.order_state NOT NULL,
			code shared.order_code NOT NULL DEFAULT shared.default_code(),
			payload shared.order_payload
		);
		CREATE TABLE shared.excluded(id bigint PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tables, err := copy.Inventory(ctx, conn, func(schema, table string) bool {
		return schema == "app" && table == "orders"
	})
	if err != nil {
		t.Fatal(err)
	}
	selection, err := dumpSelection(ctx, source.URI, "", tables)
	if err != nil {
		t.Fatal(err)
	}
	objects := map[schema.CatalogObject]bool{}
	for _, object := range selection.Objects {
		objects[object] = true
	}
	type catalogIdentity struct {
		name    string
		catalog int64
		object  int64
		want    bool
	}
	var identities []catalogIdentity
	for _, query := range []struct {
		name, sql string
		want      bool
	}{
		{"enum", `SELECT 'pg_type'::regclass::oid::bigint,'shared.order_state'::regtype::oid::bigint`, true},
		{"domain", `SELECT 'pg_type'::regclass::oid::bigint,'shared.order_code'::regtype::oid::bigint`, true},
		{"composite", `SELECT 'pg_type'::regclass::oid::bigint,'shared.order_payload'::regtype::oid::bigint`, true},
		{"function", `SELECT 'pg_proc'::regclass::oid::bigint,'shared.default_code()'::regprocedure::oid::bigint`, true},
		{"shared namespace", `SELECT 'pg_namespace'::regclass::oid::bigint,'shared'::regnamespace::oid::bigint`, true},
		{"excluded table", `SELECT 'pg_class'::regclass::oid::bigint,'shared.excluded'::regclass::oid::bigint`, false},
	} {
		item := catalogIdentity{name: query.name, want: query.want}
		if err := conn.QueryRow(ctx, query.sql).Scan(&item.catalog, &item.object); err != nil {
			t.Fatal(err)
		}
		identities = append(identities, item)
	}
	var entries []schema.TOCEntry
	for i, identity := range identities {
		if objects[schema.CatalogObject{CatalogOID: identity.catalog, ObjectOID: identity.object}] != identity.want {
			t.Errorf("%s closure membership = %v, want %v", identity.name,
				objects[schema.CatalogObject{CatalogOID: identity.catalog, ObjectOID: identity.object}], identity.want)
		}
		entries = append(entries, schema.TOCEntry{
			DumpID: int64(i + 1), CatalogOID: identity.catalog, ObjectOID: identity.object,
			Description: "TYPE", Namespace: "shared", Tag: identity.name,
		})
	}
	filtered := exactArchiveEntries(entries, selection)
	for _, entry := range filtered {
		if entry.Tag == "excluded table" {
			t.Fatal("excluded cross-schema table survived exact TOC filter")
		}
	}
	if len(filtered) != len(identities)-1 {
		t.Fatalf("filtered dependency entries = %#v", filtered)
	}
}

func TestPG17WrongTargetCannotRunPostCompletionCleanup(t *testing.T) {
	ctx := context.Background()
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	wrong := pgtest.Start(t, 17)
	fingerprint, err := sourceFingerprint(ctx, source.URI)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(ctx, t.TempDir(), state.Fingerprints{Source: fingerprint, Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetSnapshot(ctx, "cleanup_stream", "snapshot", "0/1"); err != nil {
		t.Fatal(err)
	}
	if err := recordTargetIdentity(ctx, target.URI, fingerprint, "filter", "cleanup_stream",
		streamGeneration(fingerprint, "filter")); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTargetCleanupRequested(ctx, true); err != nil {
		t.Fatal(err)
	}
	wrongConn := wrong.Connect(t)
	if _, err := wrongConn.Exec(ctx, `
		CREATE SCHEMA pgmigrate_internal;
		CREATE TABLE pgmigrate_internal.do_not_drop(id integer)`); err != nil {
		t.Fatal(err)
	}
	if err := finalizeTargetCleanup(ctx, wrong.URI, store); err == nil {
		t.Fatal("wrong target cleanup succeeded")
	}
	var remains bool
	if err := wrongConn.QueryRow(ctx,
		"SELECT to_regclass('pgmigrate_internal.do_not_drop') IS NOT NULL").Scan(&remains); err != nil {
		t.Fatal(err)
	}
	if !remains {
		t.Fatal("wrong target was mutated during cleanup validation")
	}
}
