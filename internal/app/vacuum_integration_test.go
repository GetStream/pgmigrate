//go:build integration

package app

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tgross/pgmigrate/internal/config"
	"github.com/tgross/pgmigrate/internal/pgtest"
	"github.com/tgross/pgmigrate/internal/state"
)

// TestPG17VacuumTargetTidiesTheHeapGathersStatisticsAndResumes covers what the
// phase exists for. A bulk load leaves the target with no statistics and a
// visibility map at zero, and the ANALYZE this replaced addressed only the first.
func TestPG17VacuumTargetTidiesTheHeapGathersStatisticsAndResumes(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	store := tuningStore(t)
	cfg := config.Config{Target: instance.URI, Dir: t.TempDir(), Workers: 2}
	conn := instance.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE loaded (id bigint PRIMARY KEY, payload text);
		INSERT INTO loaded SELECT n, repeat('x', 200) FROM generate_series(1,200000) AS n;
		CREATE TABLE tiny (id bigint PRIMARY KEY);
		INSERT INTO tiny VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"loaded", "tiny"} {
		var oid uint32
		if err := conn.QueryRow(ctx,
			"SELECT to_regclass($1)::oid", "public."+name).Scan(&oid); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertTable(ctx, state.Table{OID: oid, Schema: "public", Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	if pages, visible := heapVisibility(t, conn, "public.loaded"); visible != 0 {
		t.Fatalf("a freshly loaded table already reports %d of %d pages all-visible", visible, pages)
	}

	if err := vacuumTarget(ctx, cfg, store, map[string]string{"maintenance_work_mem": "16MB"}); err != nil {
		t.Fatal(err)
	}
	pages, visible := heapVisibility(t, conn, "public.loaded")
	if pages == 0 || float64(visible)/float64(pages) < 0.9 {
		t.Fatalf("%d of %d pages all-visible, want at least 90%%", visible, pages)
	}
	var analyzed bool
	if err := conn.QueryRow(ctx, `
		SELECT last_analyze IS NOT NULL FROM pg_stat_all_tables
		WHERE schemaname='public' AND relname='loaded'`).Scan(&analyzed); err != nil || !analyzed {
		t.Fatalf("statistics were not gathered: analyzed=%v err=%v", analyzed, err)
	}

	// Each table is a durable step, so a resumed run skips the tables it already
	// vacuumed rather than vacuuming the whole target again.
	steps, err := store.ListSteps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recorded := map[string]bool{}
	for _, step := range steps {
		if step.Completed && strings.HasPrefix(step.Name, vacuumStepPrefix) {
			recorded[strings.TrimPrefix(step.Name, vacuumStepPrefix)] = true
		}
	}
	if !recorded["public.loaded"] || !recorded["public.tiny"] {
		t.Fatalf("recorded vacuum steps = %v, want one per table", recorded)
	}
	before := vacuumCount(t, conn, "public.loaded")
	if err := vacuumTarget(ctx, cfg, store, nil); err != nil {
		t.Fatal(err)
	}
	if after := vacuumCount(t, conn, "public.loaded"); after != before {
		t.Errorf("a resumed run vacuumed the table again (%d then %d)", before, after)
	}
}

func heapVisibility(t *testing.T, conn *pgx.Conn, table string) (int64, int64) {
	t.Helper()
	var pages, visible int64
	if err := conn.QueryRow(context.Background(), `
		SELECT relpages, relallvisible FROM pg_catalog.pg_class
		WHERE oid = pg_catalog.to_regclass($1)`, table).Scan(&pages, &visible); err != nil {
		t.Fatalf("read visibility of %s: %v", table, err)
	}
	return pages, visible
}

func vacuumCount(t *testing.T, conn *pgx.Conn, table string) int64 {
	t.Helper()
	var count int64
	if err := conn.QueryRow(context.Background(),
		"SELECT vacuum_count FROM pg_stat_all_tables WHERE relid = to_regclass($1)",
		table).Scan(&count); err != nil {
		t.Fatalf("read vacuum count of %s: %v", table, err)
	}
	return count
}
