//go:build integration

package tuning

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tgross/pgmigrate/internal/pgtest"
)

// memoryRecorder is the crash-safety contract in miniature: an original is
// written once and never overwritten, so a second apply cannot record a
// bulk-load value as the value to revert to.
type memoryRecorder struct {
	records map[string]Change
	writes  int
}

func newMemoryRecorder() *memoryRecorder {
	return &memoryRecorder{records: map[string]Change{}}
}

func (r *memoryRecorder) Recorded(_ context.Context, name string) (bool, error) {
	_, ok := r.records[name]
	return ok, nil
}

func (r *memoryRecorder) Record(_ context.Context, change Change) error {
	if _, ok := r.records[change.Name]; ok {
		return fmt.Errorf("original for %s recorded twice", change.Name)
	}
	r.records[change.Name] = change
	r.writes++
	return nil
}

func settingValue(t *testing.T, conn *pgx.Conn, name string) string {
	t.Helper()
	var value string
	if err := conn.QueryRow(context.Background(),
		"SELECT current_setting($1)", name).Scan(&value); err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return value
}

func TestObserveReadsEveryManagedSetting(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			conn := pgtest.Start(t, major).Connect(t)
			target, err := Observe(ctx, conn)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range Managed {
				setting, ok := target.Settings[name]
				if !ok {
					t.Errorf("%s is absent from the observed settings", name)
					continue
				}
				if setting.Context == "" {
					t.Errorf("%s has no context, so its scope cannot be decided", name)
				}
				if !setting.AlterSystem {
					t.Errorf("%s reports ALTER SYSTEM unavailable to a superuser", name)
				}
			}
			if target.SharedBuffersBytes <= 0 {
				t.Errorf("shared_buffers read as %d bytes", target.SharedBuffersBytes)
			}
			if target.MaxWorkerProcesses <= 0 {
				t.Errorf("max_worker_processes read as %d", target.MaxWorkerProcesses)
			}
		})
	}
}

func TestApplyThenRevertRestoresTheOriginalSettings(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			ctx := context.Background()
			instance := pgtest.Start(t, major)
			conn := instance.Connect(t)

			before := map[string]string{}
			for _, name := range Managed {
				before[name] = settingValue(t, conn, name)
			}

			target, err := Observe(ctx, conn)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := Derive(target, Overrides{}, 4)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.SystemChanges()) == 0 {
				t.Fatal("a stock container planned no system changes; the test proves nothing")
			}
			recorder := newMemoryRecorder()
			applied, err := Apply(ctx, conn, plan, recorder)
			if err != nil {
				t.Fatal(err)
			}

			// A reload does not change the running value of a sighup setting
			// instantly from this session's view in every case, so assert on
			// pg_settings after reconnecting.
			fresh := instance.Connect(t)
			changed := false
			for _, change := range applied {
				if settingValue(t, fresh, change.Name) != before[change.Name] {
					changed = true
				}
			}
			if !changed {
				t.Fatal("no applied system setting took effect on the target")
			}

			if err := Revert(ctx, conn, applied); err != nil {
				t.Fatal(err)
			}
			after := instance.Connect(t)
			for _, change := range applied {
				if got := settingValue(t, after, change.Name); got != before[change.Name] {
					t.Errorf("%s = %q after revert, want the original %q",
						change.Name, got, before[change.Name])
				}
			}
		})
	}
}

func TestApplyIsIdempotentAndNeverRerecordsAnOriginal(t *testing.T) {
	// The property that makes an interrupted tuning safe: applying twice leaves
	// the target in the same place and leaves the recorded original alone.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)

	original := settingValue(t, conn, MaxWALSize)
	recorder := newMemoryRecorder()
	for attempt := range 2 {
		// Each attempt observes on a new connection, as a resumed run would. A
		// backend that is already open picks up a reloaded sighup setting only at
		// a command boundary, so reusing one connection would read a stale value
		// and test something other than a resume.
		attemptConn := instance.Connect(t)
		target, err := Observe(ctx, attemptConn)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := Derive(target, Overrides{}, 4)
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 1 && len(plan.SystemChanges()) != 0 {
			t.Errorf("second attempt replanned settled system changes: %v", plan.SystemChanges())
		}
		if _, err := Apply(ctx, attemptConn, plan, recorder); err != nil {
			t.Fatal(err)
		}
	}
	recorded, ok := recorder.records[MaxWALSize]
	if !ok {
		t.Fatal("max_wal_size original was never recorded")
	}
	// The recorded original is canonical, so it reads the same way current_setting
	// renders it and can be handed straight back to ALTER SYSTEM.
	if recorded.From != original {
		t.Fatalf("recorded original = %q, want %q", recorded.From, original)
	}
	if err := Revert(ctx, conn, []Change{recorded}); err != nil {
		t.Fatal(err)
	}
	if got := settingValue(t, instance.Connect(t), MaxWALSize); got != original {
		t.Fatalf("max_wal_size = %q after revert, want %q", got, original)
	}
}

func TestRevertResetsASettingItWasFirstToPin(t *testing.T) {
	// Reverting by RESET rather than by writing the old value back is what keeps
	// postgresql.auto.conf as it was found, so a value inherited from
	// postgresql.conf keeps being inherited afterwards.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)

	target, err := Observe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if target.Settings[MaxWALSize].FromAutoConf {
		t.Fatal("max_wal_size is already pinned in postgresql.auto.conf on a fresh container")
	}
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	change := Change{}
	for _, candidate := range plan.SystemChanges() {
		if candidate.Name == MaxWALSize {
			change = candidate
		}
	}
	if !change.ResetOnRevert {
		t.Fatal("max_wal_size was not marked for revert by RESET")
	}
	if _, err := Apply(ctx, conn, plan, newMemoryRecorder()); err != nil {
		t.Fatal(err)
	}
	if err := Revert(ctx, conn, plan.SystemChanges()); err != nil {
		t.Fatal(err)
	}
	// After a RESET revert the setting is inherited again rather than pinned,
	// which is exactly the state a fresh container was in.
	after, err := Observe(ctx, instance.Connect(t))
	if err != nil {
		t.Fatal(err)
	}
	if after.Settings[MaxWALSize].FromAutoConf {
		t.Error("revert left max_wal_size pinned in postgresql.auto.conf")
	}
}

func TestObserveReportsAlterSystemRefusedWithoutPrivilege(t *testing.T) {
	// The managed-service case. RDS and Cloud SQL refuse ALTER SYSTEM whatever
	// the role, and a plain role is the closest a container gets to that, so this
	// is what proves the plan degrades to session settings rather than failing.
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	admin := instance.Connect(t)
	if _, err := admin.Exec(ctx,
		"CREATE ROLE unprivileged LOGIN PASSWORD 'unprivileged'"); err != nil {
		t.Fatal(err)
	}
	limitedURI := strings.Replace(instance.URI, "pgmigrate:pgmigrate@", "unprivileged:unprivileged@", 1)
	if limitedURI == instance.URI {
		t.Fatalf("could not derive an unprivileged DSN from %q", instance.URI)
	}
	limited, err := pgx.Connect(ctx, limitedURI)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close(context.Background())

	target, err := Observe(ctx, limited)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{MaxWALSize, CheckpointTimeout} {
		if target.Settings[name].AlterSystem {
			t.Errorf("%s reports ALTER SYSTEM available to an unprivileged role", name)
		}
	}
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.SystemChanges()) != 0 {
		t.Errorf("planned system changes for an unprivileged role: %v", plan.SystemChanges())
	}
	if len(plan.SessionGUCs()) == 0 {
		t.Error("planned no session tuning, so the load would be entirely untuned")
	}
	for _, name := range []string{MaxWALSize, CheckpointTimeout} {
		if _, ok := plan.Blocked[name]; !ok {
			t.Errorf("%s is neither planned nor reported as blocked", name)
		}
	}
	// Applying such a plan must be a no-op rather than an error, because there
	// is nothing wrong: the target simply cannot be changed that way.
	applied, err := Apply(ctx, limited, plan, newMemoryRecorder())
	if err != nil {
		t.Fatalf("applying a session-only plan failed: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("applied %d system changes without the privilege to", len(applied))
	}
}

func TestApplySessionTakesEffectOnTheSession(t *testing.T) {
	ctx := context.Background()
	instance := pgtest.Start(t, 17)
	conn := instance.Connect(t)

	target, err := Observe(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Derive(target, Overrides{MaintenanceWorkMem: "1GB"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	gucs, err := ApplySession(ctx, conn, plan)
	if err != nil {
		t.Fatal(err)
	}
	if got := settingValue(t, conn, MaintenanceWorkMem); got != "1GB" {
		t.Errorf("maintenance_work_mem = %q on the tuned session, want 1GB", got)
	}
	if got := settingValue(t, conn, SynchronousCommit); got != "off" {
		t.Errorf("synchronous_commit = %q on the tuned session, want off", got)
	}
	if _, ok := gucs[MaintenanceWorkMem]; !ok {
		t.Error("maintenance_work_mem is missing from the applied session settings")
	}

	// A separate session must be untouched, which is what makes these safe for
	// the copy and index workers but not for the CDC applier.
	other := instance.Connect(t)
	if got := settingValue(t, other, SynchronousCommit); got == "off" {
		t.Error("synchronous_commit leaked to another session")
	}
}

func TestApplySessionReportsARefusedSettingWithoutLosingTheRest(t *testing.T) {
	// Best-effort mode depends on this: one setting the target will not accept
	// must not discard the ones it will.
	ctx := context.Background()
	conn := pgtest.Start(t, 17).Connect(t)
	plan := Plan{Changes: []Change{
		{Name: SynchronousCommit, To: "off", Scope: ScopeSession},
		{Name: MaintenanceWorkMem, To: "not-a-size", Scope: ScopeSession},
	}}
	applied, err := ApplySession(ctx, conn, plan)
	if err == nil {
		t.Fatal("an invalid session setting was accepted")
	}
	if !strings.Contains(err.Error(), MaintenanceWorkMem) {
		t.Errorf("error does not name the setting that failed: %v", err)
	}
	if _, ok := applied[SynchronousCommit]; !ok {
		t.Error("a valid session setting was dropped because another failed")
	}
	if _, ok := applied[MaintenanceWorkMem]; ok {
		t.Error("a refused setting was reported as applied")
	}
}
