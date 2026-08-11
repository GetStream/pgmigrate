//go:build integration

package schema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/pgtest"
)

// TestArchiveTOCParsesExoticObjects dumps object classes whose descriptions are
// multi-word or word-prefixed by a shorter description, then parses the real
// archive listing. A user-defined text-search configuration aborted the schema
// phase of the first managed-cloud migration, and operator classes parsed
// silently wrong. Synthetic TOC text cannot catch a spelling that pg_dump
// changes between majors, so this runs the actual client tools.
func TestArchiveTOCParsesExoticObjects(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			source := pgtest.Start(t, major)
			ctx := context.Background()
			conn := source.Connect(t)
			if _, err := conn.Exec(ctx, `
				CREATE TEXT SEARCH CONFIGURATION order_search (COPY = simple);
				CREATE TEXT SEARCH DICTIONARY order_dictionary (TEMPLATE = pg_catalog.simple);
				CREATE OPERATOR CLASS text_ops_copy FOR TYPE text USING btree AS
					OPERATOR 1 < (text, text),
					OPERATOR 2 <= (text, text),
					OPERATOR 3 = (text, text),
					OPERATOR 4 >= (text, text),
					OPERATOR 5 > (text, text),
					FUNCTION 1 bttextcmp(text, text);
				CREATE TABLE orders (id bigint PRIMARY KEY, note text);
				CREATE INDEX orders_note_search ON orders
					USING gin (to_tsvector('order_search'::regconfig, coalesce(note, '')));
				CREATE STATISTICS orders_stats ON id, note FROM orders;
				CREATE TABLE secured (id bigint PRIMARY KEY);
				ALTER TABLE secured ENABLE ROW LEVEL SECURITY`); err != nil {
				t.Fatal(err)
			}

			// The server's own client tools are used because a host pg_dump may
			// be older than the container and would refuse the archive.
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
			entries, err := ParseTOC(data)
			if err != nil {
				t.Fatalf("parse PostgreSQL %d archive listing: %v", major, err)
			}

			want := map[string]string{
				"TEXT SEARCH CONFIGURATION": "order_search",
				"TEXT SEARCH DICTIONARY":    "order_dictionary",
				"OPERATOR CLASS":            "text_ops_copy",
				"OPERATOR FAMILY":           "text_ops_copy",
				"STATISTICS":                "orders_stats",
				"ROW SECURITY":              "secured",
			}
			found := make(map[string]TOCEntry, len(want))
			for _, entry := range entries {
				if _, ok := want[entry.Description]; ok {
					found[entry.Description] = entry
				}
			}
			for description, tag := range want {
				entry, ok := found[description]
				if !ok {
					t.Errorf("archive listing has no %s entry", description)
					continue
				}
				if entry.Tag != tag || entry.Namespace != "public" {
					t.Errorf("%s parsed as namespace=%q tag=%q, want namespace=%q tag=%q",
						description, entry.Namespace, entry.Tag, "public", tag)
				}
			}
		})
	}
}

// exec runs a command inside an instance and fails the test on a nonzero exit.
func execIn(t *testing.T, instance *pgtest.Instance, command ...string) {
	t.Helper()
	code, output, err := instance.Container.Exec(context.Background(), command)
	body, _ := io.ReadAll(output)
	if err != nil || code != 0 {
		t.Fatalf("%v: code=%d err=%v output=%s", command, code, err, body)
	}
}

// fetch copies a file out of an instance and returns its contents.
func fetch(t *testing.T, instance *pgtest.Instance, path string) []byte {
	t.Helper()
	reader, err := instance.Container.CopyFileFromContainer(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// place writes data to the host and copies it into an instance.
func place(t *testing.T, instance *pgtest.Instance, data []byte, path string) {
	t.Helper()
	host := filepath.Join(t.TempDir(), filepath.Base(path))
	if err := os.WriteFile(host, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := instance.Container.CopyFileToContainer(context.Background(), host, path, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestPreDataRestoresInArchiveOrder is the evidence for not reordering the
// restore list at all. pgmigrate used to hoist every EXTENSION entry to the
// front, which lifted CREATE EXTENSION ... WITH SCHEMA shard_schema above the
// CREATE SCHEMA shard_schema it requires and aborted the schema phase of the
// first managed-cloud migration. The fixture reproduces that shape: extensions,
// a text-search configuration built on one of them, and a table using a type
// from another all live in a schema the dump has to create.
//
// pg_dump's own order is asserted to be dependency-correct and then restored
// verbatim, so a future major that emits a different order fails here rather
// than in production.
func TestPreDataRestoresInArchiveOrder(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			source := pgtest.Start(t, major)
			target := pgtest.Start(t, major)
			ctx := context.Background()
			src := source.Connect(t)
			var available bool
			if err := src.QueryRow(ctx, `SELECT count(*)=2 FROM pg_available_extensions
				WHERE name IN ('citext','unaccent')`).Scan(&available); err != nil {
				t.Fatal(err)
			}
			if !available {
				t.Skip("citext and unaccent are unavailable in this PostgreSQL image")
			}
			if _, err := src.Exec(ctx, `
				CREATE SCHEMA shard_schema;
				CREATE EXTENSION citext WITH SCHEMA shard_schema;
				CREATE EXTENSION unaccent WITH SCHEMA shard_schema;
				CREATE TEXT SEARCH CONFIGURATION shard_schema.unaccent_simple (COPY = simple);
				ALTER TEXT SEARCH CONFIGURATION shard_schema.unaccent_simple
					ALTER MAPPING FOR hword, hword_part, word
					WITH shard_schema.unaccent, simple;
				CREATE TABLE shard_schema.message_history (
					id bigint PRIMARY KEY,
					handle shard_schema.citext NOT NULL,
					body text NOT NULL
				);
				CREATE INDEX message_history_search ON shard_schema.message_history
					USING gin (to_tsvector('shard_schema.unaccent_simple'::regconfig, body));
				CREATE TABLE shard_schema.excluded (id bigint PRIMARY KEY, secret text)`); err != nil {
				t.Fatal(err)
			}

			// The server's own client tools are used because a host pg_dump may
			// be older than the container and would refuse the archive.
			containerURI := "postgresql://pgmigrate:pgmigrate@127.0.0.1:5432/pgmigrate?sslmode=disable"
			execIn(t, source, "pg_dump", "--dbname", containerURI, "--format=custom", "--schema-only",
				"--no-owner", "--no-privileges", "--file", "/tmp/schema.dump")
			execIn(t, source, "sh", "-c", "pg_restore --list /tmp/schema.dump > /tmp/schema.list")
			entries, err := ParseTOC(fetch(t, source, "/tmp/schema.list"))
			if err != nil {
				t.Fatalf("parse PostgreSQL %d archive listing: %v", major, err)
			}

			// Deselecting one table also proves the use-list suppresses it:
			// commented entries must not reach the target at all.
			dumpIDs := make(map[int64]bool, len(entries))
			for _, entry := range entries {
				if !strings.HasPrefix(entry.Tag, "excluded") {
					dumpIDs[entry.DumpID] = true
				}
			}
			entries = FilterTOC(entries, nil, dumpIDs)

			position := func(description, tag string) int {
				for i, entry := range entries {
					if entry.Description == description && entry.Tag == tag {
						return i
					}
				}
				t.Fatalf("archive listing has no %s %q", description, tag)
				return -1
			}
			schemaAt := position("SCHEMA", "shard_schema")
			for _, dependent := range []struct{ description, tag string }{
				{"EXTENSION", "citext"},
				{"EXTENSION", "unaccent"},
				{"TEXT SEARCH CONFIGURATION", "unaccent_simple"},
				{"TABLE", "message_history"},
			} {
				if at := position(dependent.description, dependent.tag); at < schemaAt {
					t.Errorf("pg_dump lists %s %q at %d, before its schema at %d",
						dependent.description, dependent.tag, at, schemaAt)
				}
			}
			if position("EXTENSION", "unaccent") > position("TEXT SEARCH CONFIGURATION", "unaccent_simple") {
				t.Error("pg_dump lists the unaccent extension after the configuration that uses it")
			}

			// RestorePreData builds exactly this list; it cannot be called here
			// because it would run the host pg_restore against a newer archive.
			place(t, target, UseList(entries, func(e TOCEntry) bool { return Classify(e) == PreData }),
				"/tmp/predata.list")
			place(t, target, fetch(t, source, "/tmp/schema.dump"), "/tmp/schema.dump")
			execIn(t, target, "pg_restore", "--dbname", containerURI, "--no-owner", "--no-privileges",
				"--exit-on-error", "--use-list", "/tmp/predata.list", "/tmp/schema.dump")

			var extensions int
			var configuration, handleType, excluded bool
			if err := target.Connect(t).QueryRow(
				ctx, `
				SELECT (SELECT count(*) FROM pg_extension e
				        JOIN pg_namespace n ON n.oid=e.extnamespace
				        WHERE n.nspname='shard_schema' AND e.extname IN ('citext','unaccent')),
				       EXISTS(SELECT 1 FROM pg_ts_config c
				              JOIN pg_namespace n ON n.oid=c.cfgnamespace
				              WHERE n.nspname='shard_schema' AND c.cfgname='unaccent_simple'),
				       (SELECT a.atttypid=pg_catalog.to_regtype('shard_schema.citext')
				        FROM pg_attribute a
				        WHERE a.attrelid=pg_catalog.to_regclass('shard_schema.message_history')
				          AND a.attname='handle'),
				       pg_catalog.to_regclass('shard_schema.excluded') IS NOT NULL`,
			).Scan(&extensions, &configuration, &handleType, &excluded); err != nil {
				t.Fatal(err)
			}
			if extensions != 2 || !configuration || !handleType || excluded {
				t.Fatalf("extensions=%d configuration=%v handleType=%v excluded=%v",
					extensions, configuration, handleType, excluded)
			}
		})
	}
}

type deferredTestMarkers struct{ done map[string]bool }

func (m *deferredTestMarkers) StepCompleted(_ context.Context, name string) (bool, error) {
	return m.done[name], nil
}

func (m *deferredTestMarkers) CompleteStep(_ context.Context, name, _ string) error {
	m.done[name] = true
	return nil
}

func TestPG17DeferredObjectsResumeIndividually(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	entries := []TOCEntry{
		{DumpID: 11, Description: "TRIGGER", Namespace: "public", Tag: "items trigger", Raw: "11; trigger"},
		{DumpID: 12, Description: "COMMENT", Namespace: "public", Tag: "items comment", Raw: "12; comment"},
		{DumpID: 13, Description: "FK CONSTRAINT", Namespace: "public", Tag: "items fk", Raw: "13; fk"},
	}
	markers := &deferredTestMarkers{done: map[string]bool{}}
	checks := map[int64]int{}
	service := Service{
		Tools:           Tools{Restore: "/usr/bin/true"},
		DeferredMarkers: markers,
		InspectDeferred: func(_ context.Context, _ *pgx.Conn, entry TOCEntry) (DeferredStatus, error) {
			checks[entry.DumpID]++
			return DeferredStatus{Exists: checks[entry.DumpID] > 1, Definition: entry.Tag}, nil
		},
	}
	if err := service.RestoreDeferred(ctx, target.URI, "unused.dump", t.TempDir()+"/deferred.list", entries); err != nil {
		t.Fatal(err)
	}
	if len(markers.done) != 2 || markers.done["schema:deferred:13"] {
		t.Fatalf("markers=%v", markers.done)
	}
	if err := service.RestoreDeferred(ctx, target.URI, "unused.dump", t.TempDir()+"/deferred.list", entries); err != nil {
		t.Fatal(err)
	}
	service.InspectDeferred = func(_ context.Context, _ *pgx.Conn, entry TOCEntry) (DeferredStatus, error) {
		return DeferredStatus{Exists: true, Diverged: true, Definition: "foreign definition"}, nil
	}
	err := service.RestoreDeferred(ctx, target.URI, "unused.dump", t.TempDir()+"/deferred.list",
		[]TOCEntry{{DumpID: 14, Description: "TRIGGER", Namespace: "public", Tag: "collision", Raw: "14; trigger"}})
	var collision *DeferredCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("error=%v, want DeferredCollisionError", err)
	}
}

// TestPG17DeferredRestoreKeepsObjectItRestoredWithoutComparingRenderings proves
// that a post-data object whose target rendering differs from the source's is
// accepted rather than refused. Deparsed SQL is not comparable across two
// databases, and refusing on that difference left migrations unable to resume
// past an object pgmigrate had itself restored.
func TestPG17DeferredRestoreKeepsObjectItRestoredWithoutComparingRenderings(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	entries := []TOCEntry{
		{DumpID: 21, Description: "TRIGGER", Namespace: "public", Tag: "items trigger", Raw: "21; trigger"},
	}
	markers := &deferredTestMarkers{done: map[string]bool{}}
	// The inspector reports a rendering that never equals the source's, as a
	// differing search_path or a normalized parse tree would.
	service := Service{
		Tools:           Tools{Restore: "/usr/bin/true"},
		DeferredMarkers: markers,
		InspectDeferred: func(_ context.Context, _ *pgx.Conn, _ TOCEntry) (DeferredStatus, error) {
			return DeferredStatus{Exists: true, Definition: "target rendering"}, nil
		},
	}
	list := t.TempDir() + "/deferred.list"
	if err := service.RestoreDeferred(ctx, target.URI, "unused.dump", list, entries); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if !markers.done["schema:deferred:21"] {
		t.Fatalf("markers=%v, want the trigger adopted", markers.done)
	}
	if err := service.RestoreDeferred(ctx, target.URI, "unused.dump", list, entries); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// An object that disappears after pgmigrate restored it is still refused.
	service.InspectDeferred = func(_ context.Context, _ *pgx.Conn, _ TOCEntry) (DeferredStatus, error) {
		return DeferredStatus{}, nil
	}
	err := service.RestoreDeferred(ctx, target.URI, "unused.dump", list, entries)
	var collision *DeferredCollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("error=%v, want DeferredCollisionError for a vanished object", err)
	}
}
