//go:build integration

package app

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tgross/pgmigrate/internal/config"
	"github.com/tgross/pgmigrate/internal/pgtest"
	"github.com/tgross/pgmigrate/internal/setup"
	"github.com/tgross/pgmigrate/internal/state"
	"github.com/tgross/pgmigrate/internal/tuning"
)

func tuningStore(t *testing.T) *state.Store {
	t.Helper()
	store, err := state.Open(context.Background(), t.TempDir(),
		state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func currentSetting(t *testing.T, conn *pgx.Conn, name string) string {
	t.Helper()
	var value string
	if err := conn.QueryRow(context.Background(), "SELECT current_setting($1)", name).Scan(&value); err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return value
}

// autoConfSettings reports what postgresql.auto.conf currently pins, which is
// where ALTER SYSTEM writes. Checking the running value is not enough: a change
// written without a reload is inert but still there, and would take effect at
// whatever unrelated reload or restart came next.
func autoConfSettings(t *testing.T, conn *pgx.Conn) map[string]string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT name, setting FROM pg_file_settings
		WHERE sourcefile LIKE '%postgresql.auto.conf'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	pinned := map[string]string{}
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			t.Fatal(err)
		}
		pinned[name] = setting
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return pinned
}

func openFindings(t *testing.T, store *state.Store) map[string]state.Finding {
	t.Helper()
	findings, err := store.ListFindings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	open := map[string]state.Finding{}
	for _, finding := range findings {
		if !finding.Resolved {
			open[finding.ID] = finding
		}
	}
	return open
}

func TestPG17TuneTargetAppliesAndCutoverCleanupRestores(t *testing.T) {
	// The whole point of recording originals before changing anything: whatever
	// the load needed, the target ends up as it started, and it does so through
	// the same cleanup path a real cutover takes.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	store := tuningStore(t)
	cfg := config.Config{Target: instance.URI, Dir: t.TempDir(), Workers: 4}

	before := map[string]string{}
	for _, name := range tuning.Managed {
		before[name] = currentSetting(t, instance.Connect(t), name)
	}

	sessionGUCs, err := tuneTarget(ctx, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessionGUCs) == 0 {
		t.Fatal("no session settings were produced for the copy and index workers")
	}
	if _, ok := sessionGUCs[tuning.SynchronousCommit]; !ok {
		t.Error("synchronous_commit is missing from the session settings")
	}

	tuned := instance.Connect(t)
	if got := currentSetting(t, tuned, tuning.MaxWALSize); got == before[tuning.MaxWALSize] {
		t.Fatalf("max_wal_size is still %q, so nothing was tuned", got)
	}
	if finding, ok := openFindings(t, store)[tuningFinding]; !ok {
		t.Error("no open finding records that the target is configured for a bulk load")
	} else if !strings.Contains(finding.Message, tuning.MaxWALSize) {
		t.Errorf("finding does not name what changed: %q", finding.Message)
	}

	// --no-cleanup retains replication objects but must not leave the target in
	// its bulk-load configuration, because it is about to serve production.
	if err := cleanupAfterCutover(ctx, config.Config{
		Source: instance.URI, Target: instance.URI, Dir: cfg.Dir, NoCleanup: true,
	}, store, setup.Snapshot{}, nil); err != nil {
		t.Fatal(err)
	}
	restored := instance.Connect(t)
	for name, want := range before {
		if got := currentSetting(t, restored, name); got != want {
			t.Errorf("%s = %q after cleanup, want the original %q", name, got, want)
		}
	}
	if _, ok := openFindings(t, store)[tuningFinding]; ok {
		t.Error("the bulk-load finding is still open after the settings were restored")
	}
}

func TestPG17TuneTargetIsIdempotentAcrossAResume(t *testing.T) {
	// A resumed run calls this again. It must not record the bulk-load value as
	// the value to revert to, which would make the target permanently tuned.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	store := tuningStore(t)
	cfg := config.Config{Target: instance.URI, Dir: t.TempDir(), Workers: 4}

	original := currentSetting(t, instance.Connect(t), tuning.MaxWALSize)
	for range 3 {
		if _, err := tuneTarget(ctx, cfg, store); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := recordedTuning(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, change := range changes {
		if change.Name != tuning.MaxWALSize {
			continue
		}
		found = true
		if change.From != original {
			t.Errorf("recorded original = %q after three attempts, want %q", change.From, original)
		}
	}
	if !found {
		t.Fatal("max_wal_size was never recorded")
	}
	if err := revertTargetTuning(ctx, instance.URI, store); err != nil {
		t.Fatal(err)
	}
	if got := currentSetting(t, instance.Connect(t), tuning.MaxWALSize); got != original {
		t.Fatalf("max_wal_size = %q after revert, want %q", got, original)
	}
}

func TestPG17TuneTargetSkippedLeavesTheTargetAlone(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	store := tuningStore(t)
	before := currentSetting(t, instance.Connect(t), tuning.MaxWALSize)

	gucs, err := tuneTarget(ctx, config.Config{
		Target: instance.URI, Dir: t.TempDir(), Workers: 4, SkipTargetTuning: true,
	}, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(gucs) != 0 {
		t.Errorf("produced session settings despite --skip-target-tuning: %v", gucs)
	}
	if got := currentSetting(t, instance.Connect(t), tuning.MaxWALSize); got != before {
		t.Errorf("max_wal_size = %q despite --skip-target-tuning, want %q", got, before)
	}
	changes, err := recordedTuning(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Errorf("recorded tuning despite --skip-target-tuning: %v", changes)
	}
}

func TestPG17TuneTargetStopsByDefaultWhenASettingIsRefused(t *testing.T) {
	// An override the target will reject stands in for any refused change. By
	// default the run stops, and stopping has to leave the target as it was found
	// rather than half tuned with nothing left to revert it.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	before := currentSetting(t, instance.Connect(t), tuning.CheckpointTimeout)

	refused := config.Config{
		Target: instance.URI, Dir: t.TempDir(), Workers: 4,
		// Parses as a duration, is longer than the current value so it is really
		// planned, and is past checkpoint_timeout's one-day maximum so PostgreSQL
		// refuses it. A merely small value would instead be dropped as a
		// downgrade and never attempted.
		CheckpointTimeout: "2d",
	}
	strictStore := tuningStore(t)
	_, err := tuneTarget(ctx, refused, strictStore)
	if err == nil {
		t.Fatal("a refused setting did not stop the run")
	}
	if !strings.Contains(err.Error(), "--warn-on-tuning-errors") {
		t.Errorf("error does not point at the best-effort flag: %v", err)
	}
	if got := currentSetting(t, instance.Connect(t), tuning.CheckpointTimeout); got != before {
		t.Errorf("checkpoint_timeout = %q after a refused tuning stopped the run, want the original %q", got, before)
	}
	// The settings applied before the refusal must be gone from
	// postgresql.auto.conf, not merely inactive. Left there they are a change the
	// operator never asked for, waiting for the next reload.
	for name, value := range autoConfSettings(t, instance.Connect(t)) {
		t.Errorf("a refused tuning left %s=%s pinned in postgresql.auto.conf", name, value)
	}

	// With the flag, the same refusal is recorded and the run proceeds with the
	// settings that did apply.
	warnStore := tuningStore(t)
	refused.WarnOnTuningErrors = true
	gucs, err := tuneTarget(ctx, refused, warnStore)
	if err != nil {
		t.Fatalf("--warn-on-tuning-errors did not tolerate a refused setting: %v", err)
	}
	if len(gucs) == 0 {
		t.Error("best effort produced no session settings at all")
	}
	if _, ok := openFindings(t, warnStore)["target-tuning-incomplete"]; !ok {
		t.Error("no finding records that tuning was incomplete")
	}
}

func TestPG17TuneTargetDegradesToSessionSettingsWithoutAlterSystem(t *testing.T) {
	// The managed-service shape: ALTER SYSTEM is unavailable, so the system
	// settings are reported rather than attempted, and the run still gets the
	// session tuning that matters most for index builds.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	admin := instance.Connect(t)
	if _, err := admin.Exec(ctx, "CREATE ROLE limited LOGIN PASSWORD 'limited'"); err != nil {
		t.Fatal(err)
	}
	limitedURI := strings.Replace(instance.URI, "pgmigrate:pgmigrate@", "limited:limited@", 1)
	if limitedURI == instance.URI {
		t.Fatalf("could not derive a limited DSN from %q", instance.URI)
	}
	store := tuningStore(t)

	gucs, err := tuneTarget(ctx, config.Config{
		Target: limitedURI, Dir: t.TempDir(), Workers: 4,
	}, store)
	if err != nil {
		t.Fatalf("a target that refuses ALTER SYSTEM stopped the run: %v", err)
	}
	if len(gucs) == 0 {
		t.Error("no session tuning applied, so the load would be entirely untuned")
	}
	changes, err := recordedTuning(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Errorf("recorded system changes that were never made: %v", changes)
	}
	finding, ok := openFindings(t, store)["target-tuning-blocked"]
	if !ok {
		t.Fatal("no finding records that the target settings were left alone")
	}
	if !strings.Contains(finding.Message, tuning.MaxWALSize) {
		t.Errorf("finding does not name what was left alone: %q", finding.Message)
	}
}
