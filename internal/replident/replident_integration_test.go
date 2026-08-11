//go:build integration

package replident

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tgross/pgmigrate/internal/pgtest"
)

// memoryRecorder stands in for the state store, and enforces the same rule: an
// original is written once and never overwritten, so a resume cannot record FULL
// as the value to revert to.
type memoryRecorder struct {
	records map[uint32]Record
	writes  int
}

func newMemoryRecorder() *memoryRecorder {
	return &memoryRecorder{records: map[uint32]Record{}}
}

func (r *memoryRecorder) Recorded(_ context.Context, oid uint32) (bool, error) {
	_, ok := r.records[oid]
	return ok, nil
}

func (r *memoryRecorder) Record(_ context.Context, record Record) error {
	if existing, ok := r.records[record.OID]; ok {
		return fmt.Errorf("original for %s recorded twice: %+v then %+v",
			record.Identifier(), existing, record)
	}
	r.records[record.OID] = record
	r.writes++
	return nil
}

// fixtures is every schema shape whose replica identity has to be judged
// correctly, laid out so the assertions read as a table.
const fixtures = `
	CREATE SCHEMA app;

	-- The overwhelmingly common case: identifiable, must be left alone.
	CREATE TABLE app.keyed (id bigint PRIMARY KEY, payload text);

	-- No primary key at all, so DEFAULT publishes nothing.
	CREATE TABLE app.unkeyed (id bigint, payload text);

	-- A primary key exists, but NOTHING overrides it.
	CREATE TABLE app.nothing_keyed (id bigint PRIMARY KEY, payload text);
	ALTER TABLE app.nothing_keyed REPLICA IDENTITY NOTHING;

	-- Already the fallback; altering again would be a pointless write and, worse,
	-- would record FULL as the value to restore.
	CREATE TABLE app.already_full (id bigint, payload text);
	ALTER TABLE app.already_full REPLICA IDENTITY FULL;

	-- A designated unique index is a usable identity even with no primary key.
	CREATE TABLE app.designated (id bigint NOT NULL, payload text);
	CREATE UNIQUE INDEX designated_key ON app.designated (id);
	ALTER TABLE app.designated REPLICA IDENTITY USING INDEX designated_key;

	-- A unique index that nobody designated is NOT an identity, however much it
	-- looks like one.
	CREATE TABLE app.undesignated (id bigint NOT NULL, payload text);
	CREATE UNIQUE INDEX undesignated_key ON app.undesignated (id);

	-- A partitioned parent with a primary key: every leaf inherits the key, so
	-- nothing needs changing, and the parent itself holds no rows.
	CREATE TABLE app.part_keyed (id bigint, region text, PRIMARY KEY (id, region))
	  PARTITION BY LIST (region);
	CREATE TABLE app.part_keyed_eu PARTITION OF app.part_keyed FOR VALUES IN ('eu');
	CREATE TABLE app.part_keyed_us PARTITION OF app.part_keyed FOR VALUES IN ('us');

	-- A partitioned parent without a primary key: the leaves are what appear in
	-- the replication stream, and each needs the fallback individually because
	-- ALTER TABLE on the parent does not cascade.
	CREATE TABLE app.part_unkeyed (id bigint, region text) PARTITION BY LIST (region);
	CREATE TABLE app.part_unkeyed_eu PARTITION OF app.part_unkeyed FOR VALUES IN ('eu');
	CREATE TABLE app.part_unkeyed_us PARTITION OF app.part_unkeyed FOR VALUES IN ('us');

	-- Sub-partitioning: only the storage-bearing relations at the bottom matter,
	-- whatever the depth.
	CREATE TABLE app.deep (id bigint, region text, created date)
	  PARTITION BY LIST (region);
	CREATE TABLE app.deep_eu PARTITION OF app.deep FOR VALUES IN ('eu')
	  PARTITION BY RANGE (created);
	CREATE TABLE app.deep_eu_2025 PARTITION OF app.deep_eu
	  FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
	CREATE TABLE app.deep_eu_2026 PARTITION OF app.deep_eu
	  FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
`

func loadFixtures(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), fixtures); err != nil {
		t.Fatalf("load fixtures: %v", err)
	}
}

// selectAll returns the tables pgmigrate's inventory would select: ordinary
// tables and partitioned parents, never a partition.
func selectAll(t *testing.T, conn *pgx.Conn) []Table {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT c.oid, n.nspname, c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND c.relkind IN ('r','p') AND NOT c.relispartition
		ORDER BY c.relname`)
	if err != nil {
		t.Fatalf("select tables: %v", err)
	}
	defer rows.Close()
	var tables []Table
	for rows.Next() {
		var table Table
		if err := rows.Scan(&table.OID, &table.Schema, &table.Name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("select tables: %v", err)
	}
	return tables
}

func names(relations []Relation) []string {
	list := make([]string, 0, len(relations))
	for _, relation := range relations {
		list = append(list, relation.Name)
	}
	sort.Strings(list)
	return list
}

func identities(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT c.relname, c.relreplident::text ||
		       coalesce((SELECT ':' || x.relname
		                 FROM pg_index i JOIN pg_class x ON x.oid = i.indexrelid
		                 WHERE i.indrelid = c.oid AND i.indisreplident), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'app' AND c.relkind = 'r'`)
	if err != nil {
		t.Fatalf("read identities: %v", err)
	}
	defer rows.Close()
	found := map[string]string{}
	for rows.Next() {
		var name, identity string
		if err := rows.Scan(&name, &identity); err != nil {
			t.Fatalf("scan identity: %v", err)
		}
		found[name] = identity
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read identities: %v", err)
	}
	return found
}

// TestInspectExpandsPartitionsAndJudgesEveryShape is the assertion the 110-table
// rehearsal regression needed: the chosen set is exactly the relations that
// cannot identify their rows, with leaf partitions standing in for the parents
// that were selected.
func TestInspectExpandsPartitionsAndJudgesEveryShape(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			conn := pgtest.Start(t, major).Connect(t)
			loadFixtures(t, conn)

			relations, err := Inspect(ctx, conn, selectAll(t, conn))
			if err != nil {
				t.Fatal(err)
			}

			// Only storage-bearing relations: the parents contribute their leaves
			// and never themselves, because a parent's relreplident is never
			// consulted.
			wantInspected := []string{
				"already_full", "deep_eu_2025", "deep_eu_2026", "designated", "keyed",
				"nothing_keyed", "part_keyed_eu", "part_keyed_us",
				"part_unkeyed_eu", "part_unkeyed_us", "undesignated", "unkeyed",
			}
			if got := names(relations); !equal(got, wantInspected) {
				t.Fatalf("inspected %v, want %v", got, wantInspected)
			}

			wantNeedy := []string{
				"deep_eu_2025", "deep_eu_2026", "nothing_keyed",
				"part_unkeyed_eu", "part_unkeyed_us", "undesignated", "unkeyed",
			}
			if got := names(NeedFallback(relations)); !equal(got, wantNeedy) {
				t.Fatalf("needy %v, want %v", got, wantNeedy)
			}

			byName := map[string]Relation{}
			for _, relation := range relations {
				byName[relation.Name] = relation
			}
			// Partition marks a relation nobody named, which is what lets a
			// message explain why a leaf is being altered.
			for _, name := range []string{"part_keyed_eu", "part_unkeyed_us", "deep_eu_2025"} {
				if !byName[name].Partition {
					t.Errorf("%s was not reported as a partition", name)
				}
			}
			for _, name := range []string{"keyed", "unkeyed", "designated"} {
				if byName[name].Partition {
					t.Errorf("%s was reported as a partition", name)
				}
			}
			if got := byName["designated"]; got.IdentityIndex != "designated_key" ||
				!got.HasValidIdentityIndex {
				t.Errorf("designated identity index = %q valid=%v",
					got.IdentityIndex, got.HasValidIdentityIndex)
			}
			// An undesignated unique index must not register as an identity index,
			// or detection would skip a relation the source will refuse to update.
			if got := byName["undesignated"]; got.HasValidIdentityIndex ||
				got.HasValidPrimaryKey {
				t.Errorf("undesignated looked identifiable: %+v", got)
			}
			for _, relation := range relations {
				if !relation.Owned {
					t.Errorf("%s reports the creating role as not an owner", relation.Name)
				}
			}
		})
	}
}

// TestInspectCatchesAnInvalidatedPrimaryKey covers a primary key that exists in
// the catalog but whose index is invalid, which cannot serve as a replica
// identity. The previous check omitted indisvalid and would have called this
// relation identifiable.
func TestInspectCatchesAnInvalidatedPrimaryKey(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			conn := pgtest.Start(t, major).Connect(t)
			if _, err := conn.Exec(ctx, `
				CREATE SCHEMA app;
				CREATE TABLE app.broken_key (id bigint PRIMARY KEY, payload text);
			`); err != nil {
				t.Fatal(err)
			}
			// A failed CREATE INDEX CONCURRENTLY or REINDEX leaves indisvalid
			// false; forcing the flag reproduces that state without the race.
			if _, err := conn.Exec(ctx, `
				UPDATE pg_index SET indisvalid = false
				WHERE indrelid = 'app.broken_key'::regclass AND indisprimary`); err != nil {
				t.Fatal(err)
			}

			relations, err := Inspect(ctx, conn, selectAll(t, conn))
			if err != nil {
				t.Fatal(err)
			}
			if len(relations) != 1 {
				t.Fatalf("inspected %v", names(relations))
			}
			if relations[0].HasValidPrimaryKey {
				t.Error("an invalid primary key index was reported as valid")
			}
			if !NeedsFallback(relations[0]) {
				t.Error("a relation whose only primary key index is invalid was skipped")
			}
		})
	}
}

// TestInspectReportsRelationsTheRoleDoesNotOwn is the detect-early case. Only an
// owner may change a replica identity, so a relation owned by another role is a
// gap pgmigrate cannot close, and saying so before touching anything beats a bare
// permission error partway through setup.
func TestInspectReportsRelationsTheRoleDoesNotOwn(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			instance := pgtest.Start(t, major)
			admin := instance.Connect(t)
			if _, err := admin.Exec(ctx, `
				CREATE SCHEMA app;
				CREATE TABLE app.unkeyed (id bigint, payload text);
				CREATE ROLE migrator LOGIN PASSWORD 'migrator';
				GRANT USAGE ON SCHEMA app TO migrator;
				GRANT SELECT ON ALL TABLES IN SCHEMA app TO migrator;
			`); err != nil {
				t.Fatal(err)
			}

			migrator := connectAs(t, instance.URI, "migrator", "migrator")
			relations, err := Inspect(ctx, migrator, selectAll(t, admin))
			if err != nil {
				t.Fatal(err)
			}
			if len(relations) != 1 {
				t.Fatalf("inspected %v", names(relations))
			}
			if relations[0].Owned {
				t.Fatal("a relation owned by another role was reported as owned")
			}
			if got := Unowned(NeedFallback(relations)); len(got) != 1 {
				t.Fatalf("unowned needy relations = %v", names(got))
			}

			// Apply refuses rather than surfacing a raw permission failure, and
			// records nothing, so no phantom revert is left behind.
			recorder := newMemoryRecorder()
			applied, err := Apply(ctx, migrator, relations, recorder)
			if err == nil {
				t.Fatal("altering a relation owned by another role succeeded")
			}
			if !strings.Contains(err.Error(), "does not own it") {
				t.Errorf("error does not explain ownership: %v", err)
			}
			if len(applied) != 0 || recorder.writes != 0 {
				t.Errorf("applied %d and recorded %d for an unowned relation",
					len(applied), recorder.writes)
			}
		})
	}
}

// TestApplyAndRevertRestoreEveryOriginalMode is the round trip. Reverting has to
// restore the mode and, for USING INDEX, the designation, or the source is left
// subtly different from how it was found.
func TestApplyAndRevertRestoreEveryOriginalMode(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			conn := pgtest.Start(t, major).Connect(t)
			loadFixtures(t, conn)
			// Designate an index and then drop it, so USING INDEX is among the
			// originals that a revert has to restore.
			if _, err := conn.Exec(ctx, `
				CREATE TABLE app.stale (id bigint NOT NULL, payload text);
				CREATE UNIQUE INDEX stale_key ON app.stale (id);
				ALTER TABLE app.stale REPLICA IDENTITY USING INDEX stale_key;
				UPDATE pg_index SET indisvalid = false
				WHERE indrelid = 'app.stale'::regclass AND indisreplident;
			`); err != nil {
				t.Fatal(err)
			}

			before := identities(t, conn)
			relations, err := Inspect(ctx, conn, selectAll(t, conn))
			if err != nil {
				t.Fatal(err)
			}
			needy := NeedFallback(relations)
			// The stale designation is a gap: the index it names is no longer valid.
			if !containsName(needy, "stale") {
				t.Fatalf("a stale USING INDEX designation was not detected: %v", names(needy))
			}

			recorder := newMemoryRecorder()
			applied, err := Apply(ctx, conn, needy, recorder)
			if err != nil {
				t.Fatal(err)
			}
			if len(applied) != len(needy) {
				t.Fatalf("applied %d of %d needy relations", len(applied), len(needy))
			}

			during := identities(t, conn)
			for _, relation := range relations {
				want := before[relation.Name]
				if NeedsFallback(relation) {
					want = IdentityFull
				}
				if during[relation.Name] != want {
					t.Errorf("%s is %q during the migration, want %q",
						relation.Name, during[relation.Name], want)
				}
			}

			if err := Revert(ctx, conn, applied); err != nil {
				t.Fatal(err)
			}
			after := identities(t, conn)
			for name, want := range before {
				if after[name] != want {
					t.Errorf("%s is %q after the revert, want %q", name, after[name], want)
				}
			}
		})
	}
}

// TestApplyIsIdempotentAcrossAResume covers the crash-safety contract: a second
// pass over relations already at FULL must not record FULL as the original, or the
// revert would leave production permanently inflated.
func TestApplyIsIdempotentAcrossAResume(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			conn := pgtest.Start(t, major).Connect(t)
			loadFixtures(t, conn)
			before := identities(t, conn)
			recorder := newMemoryRecorder()

			for attempt := range 3 {
				// A resume re-inspects, so the second pass sees FULL where the
				// first one wrote it and would judge nothing needy. Applying the
				// originally needy set instead is the harsher test.
				relations, err := Inspect(ctx, conn, selectAll(t, conn))
				if err != nil {
					t.Fatal(err)
				}
				needy := NeedFallback(relations)
				if attempt == 0 {
					if len(needy) == 0 {
						t.Fatal("no relation needed the fallback")
					}
				} else if len(needy) != 0 {
					t.Fatalf("attempt %d still found %v needy", attempt, names(needy))
				}
				for _, record := range recorder.records {
					needy = append(needy, Relation{
						OID: record.OID, Schema: record.Schema, Name: record.Table,
						Identity: IdentityFull, Owned: true,
					})
				}
				if _, err := Apply(ctx, conn, needy, recorder); err != nil {
					t.Fatalf("attempt %d: %v", attempt, err)
				}
			}

			for _, record := range recorder.records {
				if record.Mode == IdentityFull && before[record.Table] != IdentityFull {
					t.Errorf("%s recorded FULL as its original", record.Table)
				}
				if record.Mode != before[record.Table] &&
					!strings.HasPrefix(before[record.Table], record.Mode+":") {
					t.Errorf("%s recorded %q, want %q", record.Table, record.Mode,
						before[record.Table])
				}
			}

			var records []Record
			for _, record := range recorder.records {
				records = append(records, record)
			}
			if err := Revert(ctx, conn, records); err != nil {
				t.Fatal(err)
			}
			after := identities(t, conn)
			for name, want := range before {
				if after[name] != want {
					t.Errorf("%s is %q after the revert, want %q", name, after[name], want)
				}
			}
		})
	}
}

// TestRevertAttemptsEveryRecordDespiteAFailure matters because a revert runs at
// cutover, when nobody is watching closely: one relation that cannot be moved must
// not leave the others inflated.
func TestRevertAttemptsEveryRecordDespiteAFailure(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			conn := pgtest.Start(t, major).Connect(t)
			if _, err := conn.Exec(ctx, `
				CREATE SCHEMA app;
				CREATE TABLE app.first (id bigint);
				CREATE TABLE app.second (id bigint);
				ALTER TABLE app.first REPLICA IDENTITY FULL;
				ALTER TABLE app.second REPLICA IDENTITY FULL;
			`); err != nil {
				t.Fatal(err)
			}

			err := Revert(ctx, conn, []Record{
				{Schema: "app", Table: "first", Mode: IdentityDefault},
				{Schema: "app", Table: "gone", Mode: IdentityDefault},
				{Schema: "app", Table: "second", Mode: IdentityDefault},
			})
			if err == nil {
				t.Fatal("reverting a missing relation reported success")
			}
			after := identities(t, conn)
			for _, name := range []string{"first", "second"} {
				if after[name] != IdentityDefault {
					t.Errorf("%s is %q, so a failure earlier in the list stopped the revert",
						name, after[name])
				}
			}
		})
	}
}

func TestInspectIgnoresAnEmptySelection(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			relations, err := Inspect(context.Background(),
				pgtest.Start(t, major).Connect(t), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(relations) != 0 {
				t.Fatalf("inspected %v for an empty selection", names(relations))
			}
		})
	}
}

func connectAs(t *testing.T, uri, user, password string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(uri)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	config.User, config.Password = user, password
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func containsName(relations []Relation, name string) bool {
	for _, relation := range relations {
		if relation.Name == name {
			return true
		}
	}
	return false
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
