//go:build integration

package app

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tgross/pgmigrate/internal/config"
	pgcopy "github.com/tgross/pgmigrate/internal/copy"
	"github.com/tgross/pgmigrate/internal/pgtest"
	"github.com/tgross/pgmigrate/internal/setup"
	"github.com/tgross/pgmigrate/internal/state"
)

// replidentFixtures mixes relations that must be left alone with every shape that
// needs the fallback, including a partitioned parent whose leaves are what
// actually appear in the replication stream.
const replidentFixtures = `
	CREATE TABLE public.keyed (id bigint PRIMARY KEY, payload text);
	CREATE TABLE public.unkeyed (id bigint, payload text);
	CREATE TABLE public.nothing_keyed (id bigint PRIMARY KEY, payload text);
	ALTER TABLE public.nothing_keyed REPLICA IDENTITY NOTHING;
	CREATE TABLE public.part_unkeyed (id bigint, region text) PARTITION BY LIST (region);
	CREATE TABLE public.part_unkeyed_eu PARTITION OF public.part_unkeyed FOR VALUES IN ('eu');
	CREATE TABLE public.part_unkeyed_us PARTITION OF public.part_unkeyed FOR VALUES IN ('us');
	CREATE TABLE public.part_keyed (id bigint, region text, PRIMARY KEY (id, region))
	  PARTITION BY LIST (region);
	CREATE TABLE public.part_keyed_eu PARTITION OF public.part_keyed FOR VALUES IN ('eu');
`

// selectedTables returns what pgmigrate's inventory would select: ordinary tables
// and partitioned parents, never a partition.
func selectedTables(t *testing.T, conn *pgx.Conn) []pgcopy.Table {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT c.oid, n.nspname, c.relname
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind IN ('r','p') AND NOT c.relispartition
		ORDER BY c.relname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []pgcopy.Table
	for rows.Next() {
		var table pgcopy.Table
		if err := rows.Scan(&table.OID, &table.Schema, &table.Name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return tables
}

// fullIdentities returns the relations currently at REPLICA IDENTITY FULL.
func fullIdentities(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relreplident = 'f'
		ORDER BY c.relname`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var full []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		full = append(full, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return full
}

func identityModes(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT c.relname, c.relreplident::text
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND c.relkind = 'r'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	modes := map[string]string{}
	for rows.Next() {
		var name, mode string
		if err := rows.Scan(&name, &mode); err != nil {
			t.Fatal(err)
		}
		modes[name] = mode
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return modes
}

func replidentStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(context.Background(), t.TempDir(),
		state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestPG17FallbackTouchesOnlyTheRelationsThatNeedIt is the narrowness assertion.
// The rehearsal this change comes from set 110 relations to FULL because one
// needed it, so the interesting claim is not that the needy ones were altered but
// that the identifiable ones were not.
func TestPG17FallbackTouchesOnlyTheRelationsThatNeedIt(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, replidentFixtures); err != nil {
		t.Fatal(err)
	}
	store := replidentStore(t)
	cfg := config.Config{Source: instance.URI, Dir: t.TempDir()}
	var out bytes.Buffer
	app := App{Out: &out}

	if err := app.applyReplicaIdentityFallback(ctx, cfg, store, selectedTables(t, conn)); err != nil {
		t.Fatal(err)
	}

	want := []string{"nothing_keyed", "part_unkeyed_eu", "part_unkeyed_us", "unkeyed"}
	if got := fullIdentities(t, conn); !equalStrings(got, want) {
		t.Fatalf("relations at FULL = %v, want %v", got, want)
	}

	// The operator has to be told, since this changed a production database.
	printed := out.String()
	for _, want := range []string{
		"REPLICA IDENTITY FULL", "public.unkeyed",
		"public.part_unkeyed_us", "a partition of public.part_unkeyed",
	} {
		if !strings.Contains(printed, want) {
			t.Errorf("output omits %q:\n%s", want, printed)
		}
	}
	if strings.Contains(printed, "public.keyed") {
		t.Errorf("output names a relation that was not changed:\n%s", printed)
	}

	finding := openFindings(t, store)[replidentFinding]
	if finding.Severity != "warning" {
		t.Errorf("finding = %+v, want an open warning", finding)
	}
	for _, want := range []string{"public.unkeyed", "was DEFAULT", "was NOTHING"} {
		if !strings.Contains(finding.Message, want) {
			t.Errorf("finding message omits %q: %q", want, finding.Message)
		}
	}
}

// TestPG17FallbackIsIdempotentAcrossResumes covers the crash-safety property that
// matters most here: a resume must not record FULL as the identity to restore, or
// production stays inflated forever.
func TestPG17FallbackIsIdempotentAcrossResumes(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, replidentFixtures); err != nil {
		t.Fatal(err)
	}
	store := replidentStore(t)
	cfg := config.Config{Source: instance.URI, Dir: t.TempDir()}
	app := App{Out: &bytes.Buffer{}}
	before := identityModes(t, conn)
	tables := selectedTables(t, conn)

	for attempt := range 3 {
		if err := app.applyReplicaIdentityFallback(ctx, cfg, store, tables); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}

	records, err := recordedReplicaIdentities(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("recorded %d originals, want 4", len(records))
	}
	for _, record := range records {
		if record.Mode != before[record.Table] {
			t.Errorf("%s recorded %q as its original, want %q",
				record.Table, record.Mode, before[record.Table])
		}
	}

	if err := restoreReplicaIdentities(ctx, cfg.Source, store); err != nil {
		t.Fatal(err)
	}
	if got := identityModes(t, conn); !sameModes(got, before) {
		t.Errorf("identities after revert = %v, want %v", got, before)
	}
	if _, open := openFindings(t, store)[replidentFinding]; open {
		t.Error("the finding is still open after the identities were restored")
	}
}

// TestPG17CutoverCleanupDropsThePublicationBeforeRestoring is the ordering fix.
// Restoring an identity while the relation is still published makes every
// production UPDATE and DELETE on it fail, so the publication has to go first.
// The assertion is the one that matters to the source: writes work afterwards.
func TestPG17CutoverCleanupDropsThePublicationBeforeRestoring(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE public.unkeyed (id bigint, payload text);
		INSERT INTO public.unkeyed VALUES (1, 'a');
	`); err != nil {
		t.Fatal(err)
	}
	store := replidentStore(t)
	cfg := config.Config{
		Source: instance.URI, Dir: t.TempDir(),
		Target: emptyTargetURI(t, instance.URI, conn, "cleanup_target"),
	}
	app := App{Out: &bytes.Buffer{}}
	tables := selectedTables(t, conn)
	if err := app.applyReplicaIdentityFallback(ctx, cfg, store, tables); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx,
		"CREATE PUBLICATION pgmigrate_test_pub FOR TABLE public.unkeyed"); err != nil {
		t.Fatal(err)
	}

	snapshot := setup.Snapshot{Publication: "pgmigrate_test_pub"}
	if err := cleanupAfterCutover(ctx, cfg, store, snapshot,
		[]setup.Table{{Schema: "public", Name: "unkeyed"}}); err != nil {
		t.Fatal(err)
	}

	if got := identityModes(t, conn)["unkeyed"]; got != "d" {
		t.Fatalf("identity after cleanup = %q, want d", got)
	}
	var exists bool
	if err := conn.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT FROM pg_publication WHERE pubname='pgmigrate_test_pub')",
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("the publication survived cleanup")
	}
	// The point of the ordering: the source can be written again. With the
	// publication still in place this UPDATE would fail outright.
	if _, err := conn.Exec(ctx, "UPDATE public.unkeyed SET payload='b' WHERE id=1"); err != nil {
		t.Fatalf("the source cannot be written after cleanup: %v", err)
	}
}

// TestPG17NoCleanupKeepsFullRatherThanBreakingWrites covers the case the old
// ordering got exactly backwards. Under --no-cleanup the publication is retained
// deliberately, so restoring the original identity would leave the relation
// published and unwritable — permanently, since nothing else runs afterwards.
func TestPG17NoCleanupKeepsFullRatherThanBreakingWrites(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE public.unkeyed (id bigint, payload text);
		INSERT INTO public.unkeyed VALUES (1, 'a');
	`); err != nil {
		t.Fatal(err)
	}
	store := replidentStore(t)
	cfg := config.Config{
		Source: instance.URI, Dir: t.TempDir(), NoCleanup: true,
		Target: emptyTargetURI(t, instance.URI, conn, "nocleanup_target"),
	}
	app := App{Out: &bytes.Buffer{}}
	if err := app.applyReplicaIdentityFallback(ctx, cfg, store, selectedTables(t, conn)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx,
		"CREATE PUBLICATION pgmigrate_test_pub FOR TABLE public.unkeyed"); err != nil {
		t.Fatal(err)
	}

	if err := cleanupAfterCutover(ctx, cfg, store,
		setup.Snapshot{Publication: "pgmigrate_test_pub"}, nil); err != nil {
		t.Fatal(err)
	}

	if got := identityModes(t, conn)["unkeyed"]; got != "f" {
		t.Fatalf("identity = %q, want f: reverting under --no-cleanup breaks source writes", got)
	}
	// FULL is what keeps this working while the publication stands.
	if _, err := conn.Exec(ctx, "UPDATE public.unkeyed SET payload='b' WHERE id=1"); err != nil {
		t.Fatalf("the source cannot be written: %v", err)
	}
	finding, open := openFindings(t, store)[replidentFinding]
	if !open {
		t.Fatal("nothing records that the source is still at FULL")
	}
	for _, want := range []string{"public.unkeyed", "--no-cleanup", "Drop the publication"} {
		if !strings.Contains(finding.Message, want) {
			t.Errorf("finding message omits %q: %q", want, finding.Message)
		}
	}
}

// TestPG17TargetDoesNotInheritFullFromTheAlteredSource covers a cost that would
// otherwise outlive the migration entirely. pg_dump reads the source after the
// fallback has been applied and emits the replica identity inside the CREATE TABLE
// table-of-contents entry, so the target arrives at FULL and cannot be filtered out
// of it. Cleanup has to put the target back too, or the new production database
// pays for a temporary workaround forever.
func TestPG17TargetDoesNotInheritFullFromTheAlteredSource(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE public.unkeyed (id bigint, payload text);
		CREATE TABLE public.designated (id bigint NOT NULL, payload text);
		CREATE UNIQUE INDEX designated_key ON public.designated (id);
		ALTER TABLE public.designated REPLICA IDENTITY USING INDEX designated_key;
		UPDATE pg_index SET indisvalid = false
		WHERE indrelid = 'public.designated'::regclass AND indisreplident;
	`); err != nil {
		t.Fatal(err)
	}
	store := replidentStore(t)
	targetURI := emptyTargetURI(t, instance.URI, conn, "identity_target")
	cfg := config.Config{Source: instance.URI, Target: targetURI, Dir: t.TempDir()}
	app := App{Out: &bytes.Buffer{}}
	if err := app.applyReplicaIdentityFallback(ctx, cfg, store, selectedTables(t, conn)); err != nil {
		t.Fatal(err)
	}

	// Stand in for the schema restore: the target arrives with FULL because the
	// dump was taken from the altered source, and with the index present because
	// post-data has run by the time cleanup happens.
	target, err := pgx.Connect(ctx, targetURI)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close(context.Background())
	if _, err := target.Exec(ctx, `
		CREATE TABLE public.unkeyed (id bigint, payload text);
		ALTER TABLE public.unkeyed REPLICA IDENTITY FULL;
		CREATE TABLE public.designated (id bigint NOT NULL, payload text);
		CREATE UNIQUE INDEX designated_key ON public.designated (id);
		ALTER TABLE public.designated REPLICA IDENTITY FULL;
	`); err != nil {
		t.Fatal(err)
	}

	if err := restoreTargetReplicaIdentities(ctx, targetURI, store); err != nil {
		t.Fatal(err)
	}
	modes := identityModes(t, target)
	if modes["unkeyed"] != "d" {
		t.Errorf("target unkeyed identity = %q, want d", modes["unkeyed"])
	}
	// The designation is restored, not merely the mode, so the target keeps the
	// identity the source had rather than a weaker approximation of it.
	if modes["designated"] != "i" {
		t.Errorf("target designated identity = %q, want i", modes["designated"])
	}
	// The source is untouched: it is still published at this point in a real run.
	if got := identityModes(t, conn); got["unkeyed"] != "f" || got["designated"] != "f" {
		t.Errorf("source identities changed by the target restore: %v", got)
	}
}

// TestPG17TargetRestoreIgnoresRelationsThatWereNeverCreated covers cleanup after a
// run that stopped before its schema was restored: there is nothing to put back,
// which is not a failure.
func TestPG17TargetRestoreIgnoresRelationsThatWereNeverCreated(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, "CREATE TABLE public.unkeyed (id bigint)"); err != nil {
		t.Fatal(err)
	}
	store := replidentStore(t)
	targetURI := emptyTargetURI(t, instance.URI, conn, "bare_target")
	cfg := config.Config{Source: instance.URI, Target: targetURI, Dir: t.TempDir()}
	app := App{Out: &bytes.Buffer{}}
	if err := app.applyReplicaIdentityFallback(ctx, cfg, store, selectedTables(t, conn)); err != nil {
		t.Fatal(err)
	}
	if err := restoreTargetReplicaIdentities(ctx, targetURI, store); err != nil {
		t.Fatalf("cleanup failed on a target whose schema was never restored: %v", err)
	}
}

// TestPG17FallbackStopsWhenTheRoleDoesNotOwnARelation is the detect-early case at
// the app level: nothing is altered and nothing is recorded, so there is no
// half-applied state to reason about.
func TestPG17FallbackStopsWhenTheRoleDoesNotOwnARelation(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	admin := instance.Connect(t)
	if _, err := admin.Exec(ctx, `
		CREATE TABLE public.unkeyed (id bigint, payload text);
		CREATE ROLE migrator LOGIN PASSWORD 'migrator';
		GRANT USAGE ON SCHEMA public TO migrator;
		GRANT SELECT ON ALL TABLES IN SCHEMA public TO migrator;
	`); err != nil {
		t.Fatal(err)
	}
	migratorURI := replaceCredentials(t, instance.URI, "migrator", "migrator")
	store := replidentStore(t)
	cfg := config.Config{Source: migratorURI, Dir: t.TempDir()}
	app := App{Out: &bytes.Buffer{}}

	err := app.applyReplicaIdentityFallback(ctx, cfg, store, selectedTables(t, admin))
	if err == nil {
		t.Fatal("the fallback succeeded against a relation the role does not own")
	}
	if !strings.Contains(err.Error(), "does not own it") {
		t.Errorf("error does not explain ownership: %v", err)
	}
	if got := identityModes(t, admin)["unkeyed"]; got != "d" {
		t.Errorf("identity = %q, want d: nothing should have been altered", got)
	}
	records, err := recordedReplicaIdentities(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Errorf("recorded %+v for a relation that was never altered", records)
	}
}

// emptyTargetURI returns a DSN for a fresh database on the same server, so a test
// exercising cleanup has a target that is genuinely distinct from the source
// without paying for a second container.
func emptyTargetURI(t *testing.T, uri string, conn *pgx.Conn, name string) string {
	t.Helper()
	if _, err := conn.Exec(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	parsed, err := pgx.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	return "postgres://" + parsed.User + ":" + parsed.Password + "@" + parsed.Host + ":" +
		strconv.Itoa(int(parsed.Port)) + "/" + name + "?sslmode=disable"
}

// replaceCredentials rewrites a DSN to connect as another role, because the code
// under test takes a DSN rather than a connection.
func replaceCredentials(t *testing.T, uri, user, password string) string {
	t.Helper()
	parsed, err := pgx.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	return "postgres://" + user + ":" + password + "@" + parsed.Host + ":" +
		strconv.Itoa(int(parsed.Port)) + "/" + parsed.Database + "?sslmode=disable"
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	sorted := append([]string(nil), got...)
	sort.Strings(sorted)
	for i := range sorted {
		if sorted[i] != want[i] {
			return false
		}
	}
	return true
}

func sameModes(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for name, mode := range want {
		if got[name] != mode {
			return false
		}
	}
	return true
}
