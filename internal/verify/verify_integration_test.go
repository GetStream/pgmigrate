//go:build integration

package verify

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/jackc/pgx/v5"
)

func connector(uri string) Connector {
	return func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, uri) }
}

func exec(t *testing.T, conn *pgx.Conn, statements ...string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := conn.Exec(context.Background(), statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

// inventoryOf reads the source inventory for one table, which is what a run is
// given.
func inventoryOf(t *testing.T, source *pgx.Conn, schema, name string) []Table {
	t.Helper()
	tables, err := Inventory(context.Background(), source, func(s, n string) bool {
		return s == schema && n == name
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("inventory of %s.%s = %#v", schema, name, tables)
	}
	return tables
}

// TestPostgres17BloatedSourceIsSampledByPageAndNamesTheRowThatDiffers covers the
// headline claim and the price paid for it. A source holding the same rows in far
// more pages than the target is read in its own physical order and the rows found
// there are looked up on the target by key, so a changed row is named; and when the
// budget is smaller than the table, only a fraction of the heap is read.
func TestPostgres17BloatedSourceIsSampledByPageAndNamesTheRowThatDiffers(t *testing.T) {
	sourceInstance := pgtest.Start(t, 17)
	targetInstance := pgtest.Start(t, 17)
	ctx := context.Background()
	source := sourceInstance.Connect(t)
	target := targetInstance.Connect(t)

	ddl := `
		CREATE SCHEMA "odd""schema";
		CREATE TABLE "odd""schema"."select" (
			"odd""id" bigint PRIMARY KEY,
			payload text,
			stamp timestamptz,
			amount numeric
		)`
	insert := `
		INSERT INTO "odd""schema"."select"
		SELECT i, 'value-'||i, timestamptz '2026-08-06 12:00:00+02' + i*interval '1 second', i/3.0
		FROM generate_series(1,20000) i`
	exec(t, source, ddl, insert)
	exec(t, target, ddl, insert)
	// Bloat the source the way a long-lived OLTP table is bloated: rewrite every
	// row several times and never let autovacuum reclaim the dead versions. The
	// rows are identical on both sides afterwards; only the page count differs.
	exec(t, source,
		`ALTER TABLE "odd""schema"."select" SET (autovacuum_enabled=false)`,
		`UPDATE "odd""schema"."select" SET amount=amount`,
		`UPDATE "odd""schema"."select" SET amount=amount`,
		`UPDATE "odd""schema"."select" SET amount=amount`,
		`ANALYZE "odd""schema"."select"`)

	tables := inventoryOf(t, source, `odd"schema`, "select")
	if len(tables[0].Key.Columns) != 1 ||
		tables[0].Key.Columns[0].Name != `odd"id` || !tables[0].Key.Primary {
		t.Fatalf("chosen key = %#v", tables[0].Key)
	}
	// A budget larger than the table reads it whole, which is what a small table
	// gets and what makes the divergence assertions below deterministic.
	whole := Config{
		Source: connector(sourceInstance.URI), Target: connector(targetInstance.URI),
		Tables: tables, Workers: 2, SampleRows: 1_000_000, BatchRows: 512,
	}
	equal, err := Run(ctx, whole)
	if err != nil {
		t.Fatal(err)
	}
	if !equal.Converged || !equal.Complete {
		t.Fatalf("equal databases did not converge: %#v", equal)
	}
	got := equal.Tables[0]
	if got.Source.Rows != 20000 || got.Target.Keys != 20000 || got.Target.Rows != 20000 {
		t.Fatalf("every live row should be read and looked up: %#v", got)
	}
	if got.Target.Batches < 20000/512 {
		t.Fatalf("target lookups = %d batches of %d keys", got.Target.Batches, got.Target.Keys)
	}
	if got.Coverage != 1 {
		t.Fatalf("coverage = %v, want 1 for a table read whole", got.Coverage)
	}

	exec(t, target, `UPDATE "odd""schema"."select" SET payload='different' WHERE "odd""id"=4711`)
	exec(t, target, `DELETE FROM "odd""schema"."select" WHERE "odd""id"=9`)
	// A row only the target holds is invisible to a check that walks the source:
	// nothing ever asks about a key the source did not supply. It must not be
	// reported, and the run must not claim to have looked.
	exec(t, target, `INSERT INTO "odd""schema"."select" VALUES (999999,'extra',NULL,NULL)`)
	diverged, err := Run(ctx, whole)
	if err != nil {
		t.Fatal(err)
	}
	if diverged.Converged {
		t.Fatalf("two changed rows converged: %#v", diverged)
	}
	want := map[string]DiffKind{"4711": DiffDifferent, "9": DiffSourceOnly}
	for _, diff := range diverged.Tables[0].Unresolved {
		if len(diff.Key) != 1 {
			t.Fatalf("key = %#v, want one column", diff.Key)
		}
		kind, expected := want[diff.Key[0]]
		if !expected {
			t.Fatalf("unexpected divergent row %#v", diff)
		}
		if diff.Kind != kind {
			t.Errorf("row %s reported %q, want %q", diff.Key[0], diff.Kind, kind)
		}
		delete(want, diff.Key[0])
	}
	if len(want) != 0 {
		t.Fatalf("rows not reported: %#v", want)
	}

	// The same table under a budget smaller than itself. This is the case every
	// large table gets, and what it must not do is read the whole heap or claim
	// full coverage for having read part of it.
	sampled := whole
	sampled.SampleRows, sampled.SampleWindows = 2000, 8
	partial, err := Run(ctx, sampled)
	if err != nil {
		t.Fatal(err)
	}
	side := partial.Tables[0]
	if side.Source.Windows != 8 {
		t.Fatalf("windows = %d, want 8: %#v", side.Source.Windows, side)
	}
	if side.Source.Pages >= side.Source.PagesTotal {
		t.Fatalf("the sample read %d of %d pages, which is the whole heap",
			side.Source.Pages, side.Source.PagesTotal)
	}
	if side.Source.Rows == 0 || side.Source.Rows > 2000 {
		t.Fatalf("sampled rows = %d, want between 1 and the 2000-row budget", side.Source.Rows)
	}
	if side.Target.Keys != side.Source.Rows {
		t.Fatalf("looked up %d keys for %d sampled rows", side.Target.Keys, side.Source.Rows)
	}
	if side.Coverage >= 0.5 {
		t.Fatalf("coverage = %v for a 2000-row sample of a 20000-row table", side.Coverage)
	}
}

// TestPostgres17SampleWindowIsAnsweredByATidRangeScanUnderItsLimit pins the plan
// the whole design depends on. A ctid range the planner did not recognise would
// fall back to a sequential scan of the entire heap per window, which is worse than
// the exhaustive comparison this replaced and invisible in a correctness test. The
// limit has to reach the plan too, or a dense window would spend the whole budget.
func TestPostgres17SampleWindowIsAnsweredByATidRangeScanUnderItsLimit(t *testing.T) {
	instance := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := instance.Connect(t)
	exec(t, conn,
		`CREATE TABLE public.wide(id bigint PRIMARY KEY, payload text)`,
		`INSERT INTO public.wide SELECT i, repeat('x',400) FROM generate_series(1,20000) i`,
		`ANALYZE public.wide`)
	tables := inventoryOf(t, conn, "public", "wide")
	heaps, err := relations(ctx, conn, tables[0])
	if err != nil {
		t.Fatal(err)
	}
	if heaps[0].Pages < 100 || heaps[0].Rows == 0 {
		t.Fatalf("the test table is too small to sample: %#v", heaps[0])
	}
	windows := planSample(heaps, 2000, 8)
	if len(windows) != 8 {
		t.Fatalf("sample plan = %#v, want eight windows", windows)
	}
	// The middle window is the interesting one: it is bounded at both ends, which
	// is the shape every window but the last has.
	window := windows[len(windows)/2]
	if window.End == 0 || window.Limit == 0 {
		t.Fatalf("window %s is not bounded at both ends: %#v", window, window)
	}
	rows, err := conn.Query(ctx, "EXPLAIN (COSTS OFF) "+sampleQuery(tables[0], window))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "Tid Range Scan") {
		t.Fatalf("window %s is not read as a page range:\n%s", window, plan.String())
	}
	if !strings.Contains(plan.String(), "Limit") {
		t.Fatalf("window %s is not bounded by its row limit:\n%s", window, plan.String())
	}
}

// TestPostgres17PartitionLeavesAreSampledWhileTheTargetIsReachedThroughTheParent
// covers an asymmetry the sample introduces. Page numbers are per heap, so the
// source's leaves are enumerated and given a share of the budget each; the target
// is never asked about a page, so its lookups go through the parent and its own
// partitioning is nobody's business.
func TestPostgres17PartitionLeavesAreSampledWhileTheTargetIsReachedThroughTheParent(t *testing.T) {
	sourceInstance := pgtest.Start(t, 17)
	targetInstance := pgtest.Start(t, 17)
	ctx := context.Background()
	source := sourceInstance.Connect(t)
	target := targetInstance.Connect(t)

	exec(t, source,
		`CREATE TABLE public.events(id bigint PRIMARY KEY, payload text) PARTITION BY RANGE (id)`,
		`CREATE TABLE public.events_low PARTITION OF public.events FOR VALUES FROM (0) TO (5000)`,
		`CREATE TABLE public.events_high PARTITION OF public.events FOR VALUES FROM (5000) TO (10000)`)
	// The target splits the same range differently. Nothing pairs the leaves up, so
	// this has to compare equal.
	exec(t, target,
		`CREATE TABLE public.events(id bigint PRIMARY KEY, payload text) PARTITION BY RANGE (id)`,
		`CREATE TABLE public.events_a PARTITION OF public.events FOR VALUES FROM (0) TO (2500)`,
		`CREATE TABLE public.events_b PARTITION OF public.events FOR VALUES FROM (2500) TO (7500)`,
		`CREATE TABLE public.events_c PARTITION OF public.events FOR VALUES FROM (7500) TO (10000)`)
	insert := `INSERT INTO public.events SELECT i, 'payload-'||i FROM generate_series(1,9999) i`
	exec(t, source, insert, `ANALYZE public.events`)
	exec(t, target, insert, `ANALYZE public.events`)

	tables := inventoryOf(t, source, "public", "events")
	if tables[0].RelKind != "p" || !tables[0].Key.Primary {
		t.Fatalf("inventory = %#v, want a partitioned table with a primary key", tables[0])
	}
	cfg := Config{
		Source: connector(sourceInstance.URI), Target: connector(targetInstance.URI),
		Tables: tables, SampleRows: 1_000_000,
	}
	result, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Converged {
		t.Fatalf("differently partitioned copies of the same rows diverged: %#v", result)
	}
	got := result.Tables[0]
	if got.Source.Relations != 2 {
		t.Fatalf("source leaves read = %d, want 2", got.Source.Relations)
	}
	if got.Source.Rows != 9999 || got.Target.Rows != 9999 {
		t.Fatalf("rows read = source %d, target %d", got.Source.Rows, got.Target.Rows)
	}

	// A budget smaller than the table has to be divided among the leaves rather
	// than spent once per leaf, or a hundred-partition table would read a hundred
	// budgets.
	sampled := cfg
	sampled.SampleRows, sampled.SampleWindows = 2000, 4
	partial, err := Run(ctx, sampled)
	if err != nil {
		t.Fatal(err)
	}
	if rows := partial.Tables[0].Source.Rows; rows == 0 || rows > 2000 {
		t.Fatalf("the leaves returned %d rows between them, want at most the 2000-row budget", rows)
	}

	exec(t, target, `UPDATE public.events SET payload='changed' WHERE id=8000`)
	diverged, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if diverged.Converged || diverged.UnresolvedRows() != 1 {
		t.Fatalf("a changed row in a leaf was not reported: %#v", diverged.Tables[0])
	}
	if row := diverged.Tables[0].Unresolved[0]; row.Key[0] != "8000" || row.Kind != DiffDifferent {
		t.Fatalf("unresolved row = %#v", row)
	}
}

// TestPostgres17KeylessTableIsSkippedLoudlyRatherThanBlockingCutover covers the one
// table this design cannot check at all. A sampled row is found on the target by
// key, so a table without one is not compared, and saying so is the whole answer.
// It is reported complete and converged deliberately: an unverifiable table is not
// evidence of a broken copy, and failing the run over it would block a cutover for
// good rather than telling anyone anything.
func TestPostgres17KeylessTableIsSkippedLoudlyRatherThanBlockingCutover(t *testing.T) {
	sourceInstance := pgtest.Start(t, 17)
	targetInstance := pgtest.Start(t, 17)
	ctx := context.Background()
	source := sourceInstance.Connect(t)
	target := targetInstance.Connect(t)

	ddl := `CREATE TABLE public.duplicate_rows(value text)`
	exec(t, source, ddl, `INSERT INTO public.duplicate_rows VALUES ('A'),('A'),('C')`)
	exec(t, target, ddl, `INSERT INTO public.duplicate_rows VALUES ('B'),('B'),('C')`)

	tables := inventoryOf(t, source, "public", "duplicate_rows")
	if tables[0].Key.present() || tables[0].Keyless == "" {
		t.Fatalf("keyless inventory = %#v", tables[0])
	}
	result, err := Run(ctx, Config{
		Source: connector(sourceInstance.URI), Target: connector(targetInstance.URI),
		Tables: tables,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Converged || !result.Complete {
		t.Fatalf("a skipped table must not fail the run: %#v", result)
	}
	got := result.Tables[0]
	if got.Coverage != 0 {
		t.Fatalf("coverage = %v, want 0 for a table nothing was read from", got.Coverage)
	}
	if got.Source.Rows != 0 || got.Target.Keys != 0 {
		t.Fatalf("a skipped table should read nothing: %#v", got)
	}
	if len(got.Warnings) == 0 || !strings.Contains(got.Warnings[0], "not compared") {
		t.Fatalf("the skip has to be explained: %#v", got.Warnings)
	}
	if result.Coverage() != 0 {
		t.Fatalf("the run's coverage = %v, want the worst table's", result.Coverage())
	}
}

// TestPostgres17RecheckAttributesAnInFlightRowToReplicationLatency covers the rule
// that makes verifying a live source possible: a row that differs is re-read
// against a fixed WAL position, and only reported once the target is known to have
// seen everything the source held.
func TestPostgres17RecheckAttributesAnInFlightRowToReplicationLatency(t *testing.T) {
	sourceInstance := pgtest.Start(t, 17)
	targetInstance := pgtest.Start(t, 17)
	ctx := context.Background()
	source := sourceInstance.Connect(t)
	target := targetInstance.Connect(t)

	ddl := `CREATE TABLE public.items(id bigint PRIMARY KEY, payload text)`
	insert := `INSERT INTO public.items SELECT i,'payload-'||i FROM generate_series(1,2000) i`
	exec(t, source, ddl, insert)
	exec(t, target, ddl, insert)
	// The source is ahead by one row, exactly as it is while apply is behind.
	exec(t, source, `UPDATE public.items SET payload='newer' WHERE id=7`)

	tables := inventoryOf(t, source, "public", "items")
	var boundaries, waits atomic.Int32
	cfg := Config{
		Source: connector(sourceInstance.URI), Target: connector(targetInstance.URI),
		Tables: tables, SampleRows: 1_000_000, ConvergeTimeout: 500 * time.Millisecond,
		Boundary: func(ctx context.Context, conn *pgx.Conn) (string, error) {
			boundaries.Add(1)
			var position string
			err := conn.QueryRow(
				ctx,
				`SELECT pg_catalog.pg_logical_emit_message(false,'pgmigrate_verify','recheck')::text`,
			).Scan(&position)
			return position, err
		},
		// Standing in for the applier: reaching the marked position is exactly what
		// makes the pending change visible on the target.
		WaitApplied: func(ctx context.Context, position string) error {
			waits.Add(1)
			if position == "" {
				return fmt.Errorf("the boundary did not report a position")
			}
			_, err := target.Exec(ctx, `UPDATE public.items SET payload='newer' WHERE id=7`)
			return err
		},
	}
	result, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Converged {
		t.Fatalf("a change in flight was reported as divergence: %#v", result.Tables[0])
	}
	got := result.Tables[0]
	if got.Candidates != 1 || got.InFlight != 1 {
		t.Fatalf("in-flight rows = %d of %d candidates, want 1 of 1", got.InFlight, got.Candidates)
	}
	if boundaries.Load() == 0 || waits.Load() == 0 {
		t.Fatalf("the recheck did not mark a position: %d boundaries, %d waits",
			boundaries.Load(), waits.Load())
	}

	// A row the target will never receive is reported once the convergence budget
	// runs out, and the run still reaches a complete answer.
	exec(t, source, `UPDATE public.items SET payload='never-applied' WHERE id=11`)
	cfg.WaitApplied = func(context.Context, string) error { waits.Add(1); return nil }
	began := time.Now()
	diverged, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if diverged.Converged || !diverged.Complete {
		t.Fatalf("a row that never applied should be complete and divergent: %#v", diverged)
	}
	if rows := diverged.Tables[0].Unresolved; len(rows) != 1 || rows[0].Key[0] != "11" ||
		rows[0].Kind != DiffDifferent {
		t.Fatalf("unresolved rows = %#v", rows)
	}
	if elapsed := time.Since(began); elapsed < cfg.ConvergeTimeout {
		t.Fatalf("the row was given up on after %s, before its %s budget", elapsed, cfg.ConvergeTimeout)
	}
}

// TestCDCStratumFindsTheDeleteTheHeapSampleCannotSee is the whole reason this
// stratum exists.
//
// The heap sample walks the source, so it only ever asks the target about keys
// the source still has. A row the source deleted and the target kept is therefore
// invisible to it by construction — the one direction an unapplied delete shows
// up in. Asking both sides about a key the applier recorded is what makes it
// visible, and this test asserts both halves: the sample stays clean and the
// stratum reports the row.
func TestCDCStratumFindsTheDeleteTheHeapSampleCannotSee(t *testing.T) {
	sourceInstance := pgtest.Start(t, 17)
	targetInstance := pgtest.Start(t, 17)
	ctx := context.Background()
	source := sourceInstance.Connect(t)
	target := targetInstance.Connect(t)

	ddl := `CREATE TABLE public.notes (app_pk int, id text, body text, PRIMARY KEY (app_pk, id))`
	insert := `INSERT INTO public.notes
		SELECT 1, 'n'||i, 'body-'||i FROM generate_series(1,5000) i`
	exec(t, source, ddl, insert, "ANALYZE public.notes")
	exec(t, target, ddl, insert, "ANALYZE public.notes")

	// The source drops a row and the target does not, which is what an applier
	// that failed to replay a delete leaves behind.
	exec(t, source, `DELETE FROM public.notes WHERE app_pk=1 AND id='n42'`)

	tables := inventoryOf(t, source, "public", "notes")
	base := Config{
		Source: connector(sourceInstance.URI), Target: connector(targetInstance.URI),
		Tables: tables, Workers: 1, SampleRows: 1_000_000, BatchRows: 512,
	}

	blind, err := Run(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if !blind.Converged {
		t.Fatalf("the heap sample reported a row it cannot see: %#v", blind.Tables[0].Unresolved)
	}
	if blind.CDCObserved() != 0 {
		t.Fatalf("CDCObserved() = %d with no recorder configured", blind.CDCObserved())
	}

	// Now the applier reports what it applied, including the delete.
	withCDC := base
	withCDC.CDCRows = 100
	withCDC.CDCKeys = func(context.Context, string, string) (CDCRecorded, error) {
		return CDCRecorded{
			Observed: 3,
			Keys: []CDCKey{
				{Key: map[string]string{"app_pk": "1", "id": "n42"}, Kind: "delete"},
				{Key: map[string]string{"app_pk": "1", "id": "n7"}, Kind: "insert"},
				{Key: map[string]string{"app_pk": "1", "id": "n99"}, Kind: "update"},
			},
		}, nil
	}
	seen, err := Run(ctx, withCDC)
	if err != nil {
		t.Fatal(err)
	}
	if seen.Converged {
		t.Fatal("the CDC stratum missed a delete the target never applied")
	}
	unresolved := seen.Tables[0].Unresolved
	if len(unresolved) != 1 {
		t.Fatalf("unresolved = %#v, want only the unapplied delete", unresolved)
	}
	if unresolved[0].Kind != DiffTargetOnly {
		t.Errorf("unapplied delete reported as %q, want %q", unresolved[0].Kind, DiffTargetOnly)
	}
	if got := unresolved[0].Key; len(got) != 2 || got[0] != "1" || got[1] != "n42" {
		t.Errorf("unapplied delete named %v, want [1 n42]", got)
	}
	if cdc := seen.Tables[0].CDC; cdc.Keys != 3 || cdc.Observed != 3 || cdc.Deletes != 1 {
		t.Errorf("CDC result = %+v, want 3 keys of 3 observed with 1 delete", cdc)
	}
	if seen.CDCKeys() != 3 || seen.CDCObserved() != 3 {
		t.Errorf("run totals = %d of %d, want 3 of 3", seen.CDCKeys(), seen.CDCObserved())
	}

	// A delete both sides applied is absent on both, and absent on both is
	// agreement. It must not be reported.
	exec(t, target, `DELETE FROM public.notes WHERE app_pk=1 AND id='n42'`)
	clean, err := Run(ctx, withCDC)
	if err != nil {
		t.Fatal(err)
	}
	if !clean.Converged {
		t.Fatalf("a correctly applied delete was reported: %#v", clean.Tables[0].Unresolved)
	}
}
