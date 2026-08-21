package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/state"
)

func TestNewRequiresTokenBeyondLoopback(t *testing.T) {
	_, err := New(Options{
		Config:  config.Config{Dir: t.TempDir()},
		Address: "0.0.0.0:9188",
		Actions: noOpActions(),
	})
	if err == nil || !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("New() error = %v, want token requirement", err)
	}
}

func TestStatusBeforePreflight(t *testing.T) {
	server := newTestServer(t, config.Config{Dir: t.TempDir()}, "", noOpActions())
	recorder := request(t, server, http.MethodGet, "/api/status", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response statusResponse
	decode(t, recorder, &response)
	if response.Snapshot != nil {
		t.Fatalf("snapshot = %#v, want nil before preflight", response.Snapshot)
	}
	if response.Operations["migration"].State != "idle" || response.Operations["verification"].State != "idle" {
		t.Fatalf("operations = %#v, want idle", response.Operations)
	}
}

func TestControllerStartupLeavesMigrationDirectoryUntouched(t *testing.T) {
	migrationDir := filepath.Join(t.TempDir(), "not-created")
	var actionCalled atomic.Bool
	action := func(context.Context, config.Config, io.Writer) error {
		actionCalled.Store(true)
		return nil
	}
	server, err := New(Options{
		Config:  config.Config{Dir: migrationDir},
		Actions: Actions{Preflight: action, Run: action, Verify: action},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.cancel)
	for _, target := range []string{"/", "/api/config", "/api/status"} {
		if got := request(t, server, http.MethodGet, target, "", ""); got.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, body = %s", target, got.Code, got.Body.String())
		}
	}
	if actionCalled.Load() {
		t.Fatal("controller startup invoked a database action")
	}
	if _, err := os.Stat(migrationDir); !os.IsNotExist(err) {
		t.Fatalf("migration directory was touched on startup: stat error = %v", err)
	}
}

func TestStatusReportsDurableProgress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := state.Open(ctx, dir, state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []state.Phase{state.PhaseSetup, state.PhaseSchema, state.PhaseCopy} {
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertTable(ctx, state.Table{OID: 1, Schema: "public", Name: "done", PartsTotal: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTable(ctx, state.Table{OID: 2, Schema: "public", Name: "pending"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTable(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPart(ctx, state.Part{TableOID: 1, ID: "all"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompletePart(ctx, 1, "all", 1234, 5678, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	server := newTestServer(t, config.Config{Dir: dir}, "", noOpActions())
	recorder := request(t, server, http.MethodGet, "/api/status", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response statusResponse
	decode(t, recorder, &response)
	if response.Snapshot == nil || response.Snapshot.Phase != state.PhaseCopy {
		t.Fatalf("snapshot = %#v, want copy phase", response.Snapshot)
	}
	tables := response.Snapshot.Objects["tables"]
	if tables.Done != 1 || tables.Total != 2 {
		t.Fatalf("table progress = %#v, want 1/2", tables)
	}
	if response.Copy.Rows != 1234 || response.Copy.Bytes != 5678 || response.Copy.Duration != 2*time.Second {
		t.Fatalf("copy progress = %#v", response.Copy)
	}
}

func TestRunAndVerifyCanBeControlledConcurrently(t *testing.T) {
	dir := t.TempDir()
	initializeStateAt(t, dir, state.PhaseFollow)
	runStarted := make(chan struct{})
	verifyStarted := make(chan struct{})
	blocking := func(started chan<- struct{}) Action {
		return func(ctx context.Context, _ config.Config, _ io.Writer) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
	}
	server := newTestServer(t, config.Config{Dir: dir}, "", Actions{
		Preflight: func(context.Context, config.Config, io.Writer) error { return nil },
		Run:       blocking(runStarted),
		Verify:    blocking(verifyStarted),
	})

	if got := requestAction(t, server, "run", server.configurationViewSnapshot().Revision, ""); got.Code != http.StatusAccepted {
		t.Fatalf("run status = %d, body = %s", got.Code, got.Body.String())
	}
	waitChannel(t, runStarted)
	if got := requestAction(t, server, "verify", server.configurationViewSnapshot().Revision, ""); got.Code != http.StatusAccepted {
		t.Fatalf("verify status = %d, body = %s", got.Code, got.Body.String())
	}
	waitChannel(t, verifyStarted)
	if got := requestAction(t, server, "preflight", server.configurationViewSnapshot().Revision, ""); got.Code != http.StatusConflict {
		t.Fatalf("preflight status = %d, want conflict", got.Code)
	}
	if got := request(t, server, http.MethodPost, "/api/actions/stop-verification", "stop-verification", ""); got.Code != http.StatusAccepted {
		t.Fatalf("stop verification status = %d, body = %s", got.Code, got.Body.String())
	}
	if got := request(t, server, http.MethodPost, "/api/actions/stop-migration", "stop-migration", ""); got.Code != http.StatusAccepted {
		t.Fatalf("stop migration status = %d, body = %s", got.Code, got.Body.String())
	}
	waitForState(t, server, "verification", "stopped")
	waitForState(t, server, "migration", "stopped")
}

func TestLifecycleGuardsControllerActions(t *testing.T) {
	t.Run("verification before follow", func(t *testing.T) {
		server := newTestServer(t, config.Config{Dir: t.TempDir()}, "", noOpActions())
		got := requestAction(t, server, "verify", server.configurationViewSnapshot().Revision, "")
		if got.Code != http.StatusConflict || !strings.Contains(got.Body.String(), "requires a migration in follow phase") {
			t.Fatalf("verify status = %d, body = %s", got.Code, got.Body.String())
		}
	})

	t.Run("completed migration", func(t *testing.T) {
		dir := t.TempDir()
		initializeStateAt(t, dir, state.PhaseComplete)
		server := newTestServer(t, config.Config{Dir: dir}, "", noOpActions())
		for _, action := range []string{"preflight", "run", "verify"} {
			got := requestAction(t, server, action, server.configurationViewSnapshot().Revision, "")
			if got.Code != http.StatusConflict {
				t.Errorf("%s status = %d, body = %s", action, got.Code, got.Body.String())
			}
		}
	})
}

func TestTokenAndConfirmationAreRequired(t *testing.T) {
	server := newTestServer(t, config.Config{Dir: t.TempDir()}, "secret", noOpActions())
	if got := request(t, server, http.MethodGet, "/api/status", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("status without token = %d, want unauthorized", got.Code)
	}
	if got := request(t, server, http.MethodGet, "/api/status", "", "secret"); got.Code != http.StatusOK {
		t.Fatalf("status with token = %d, body = %s", got.Code, got.Body.String())
	}
	if got := request(t, server, http.MethodPost, "/api/actions/preflight", "", "secret"); got.Code != http.StatusPreconditionFailed {
		t.Fatalf("action without confirmation = %d, want precondition failed", got.Code)
	}
	if got := request(t, server, http.MethodPost, "/api/actions/preflight", "preflight", "secret"); got.Code != http.StatusPreconditionFailed {
		t.Fatalf("action without configuration revision = %d, want precondition failed", got.Code)
	}
	if got := requestAction(t, server, "preflight", "not-a-revision", "secret"); got.Code != http.StatusPreconditionFailed {
		t.Fatalf("action with invalid configuration revision = %d, want precondition failed", got.Code)
	}
}

func TestConfigurationRequiresAuthenticationAndRedactsCredentials(t *testing.T) {
	cfg := validControllerConfig(t)
	cfg.Source = "postgres://source-user:source-password@source/database"
	cfg.Target = "postgres://target-user:target-password@target/database"
	server := newTestServer(t, cfg, "secret", noOpActions())

	if got := request(t, server, http.MethodGet, "/api/config", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("config without token = %d, want unauthorized", got.Code)
	}
	if got := requestJSON(t, server, http.MethodPut, "/api/config", `{"workers":2}`, ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("config update without token = %d, want unauthorized", got.Code)
	}
	for _, target := range []string{"/api/config", "/api/status"} {
		got := request(t, server, http.MethodGet, target, "", "secret")
		if got.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, body = %s", target, got.Code, got.Body.String())
		}
		for _, secret := range []string{cfg.Source, cfg.Target, "source-password", "target-password"} {
			if strings.Contains(got.Body.String(), secret) {
				t.Errorf("GET %s exposed database credential %q", target, secret)
			}
		}
	}

	got := request(t, server, http.MethodGet, "/api/config", "", "secret")
	var view configurationView
	decode(t, got, &view)
	if !view.SourceConfigured || !view.TargetConfigured {
		t.Fatalf("connection flags = source:%v target:%v, want true", view.SourceConfigured, view.TargetConfigured)
	}
	if view.Revision == "" {
		t.Fatal("configuration revision is missing")
	}
}

func TestConfigurationUpdateParsesValuesAndPreservesDefaults(t *testing.T) {
	cfg := validControllerConfig(t)
	cfg.Source = ""
	cfg.Target = ""
	cfg.NoCleanup = true
	cfg.SequenceOffset = 1234
	cfg.EndPosition = "0/123"
	server := newTestServer(t, cfg, "secret", noOpActions())

	body := `{
		"source":"postgres://new-source/database",
		"target":"postgres://new-target/database",
		"table_filter":"",
		"workers":7,
		"split_threshold":2048,
		"restore_jobs":3,
		"ack_warnings":true,
		"allow_collation_change":true,
		"pg_dump_path":"/usr/local/bin/pg_dump",
		"pg_restore_path":"/usr/local/bin/pg_restore",
		"metrics":":9190",
		"wal_sample_duration":"45s",
		"segment_prune_interval":"2m",
		"retry_base_copy":true,
		"skip_target_tuning":true,
		"warn_on_tuning_errors":true,
		"target_memory":"64GB",
		"maintenance_work_mem":"1GB",
		"max_parallel_maintenance_workers":2,
		"max_wal_size":"16GB",
		"checkpoint_timeout":"20min",
		"verify_workers":2,
		"verify_sample_rows":500,
		"verify_sample_windows":10,
		"verify_batch_rows":50,
		"verify_duty_cycle":0.5,
		"verify_table_timeout":"1h30m",
		"verify_converge_timeout":"90s",
		"verify_cdc_rows":120,
		"cdc_sample_rows":300
	}`
	got := requestJSON(t, server, http.MethodPut, "/api/config", body, "secret")
	if got.Code != http.StatusOK {
		t.Fatalf("PUT config status = %d, body = %s", got.Code, got.Body.String())
	}
	if strings.Contains(got.Body.String(), "postgres://") {
		t.Fatalf("PUT config exposed credentials: %s", got.Body.String())
	}
	var view configurationView
	decode(t, got, &view)
	if view.Workers != 7 || view.SplitThreshold != 2048 || view.RestoreJobs != 3 ||
		view.WALSampleDuration != "45s" || view.SegmentPruneInterval != "2m0s" ||
		view.VerifyWorkers != 2 || view.VerifyTableTimeout != "1h30m0s" || view.VerifyConvergeTimeout != "1m30s" {
		t.Fatalf("updated view = %#v", view)
	}
	expected := cfg
	expected.Source = "postgres://new-source/database"
	expected.Target = "postgres://new-target/database"
	expected.AckWarnings = true
	expected.AllowCollationChange = true
	expected.Workers = 7
	expected.SplitThreshold = 2048
	expected.RestoreJobs = 3
	expected.PGDumpPath = "/usr/local/bin/pg_dump"
	expected.PGRestorePath = "/usr/local/bin/pg_restore"
	expected.Metrics = ":9190"
	expected.WALSampleDuration = 45 * time.Second
	expected.SegmentPruneInterval = 2 * time.Minute
	expected.RetryBaseCopy = true
	expected.SkipTargetTuning = true
	expected.WarnOnTuningErrors = true
	expected.TargetMemory = "64GB"
	expected.MaintenanceWorkMem = "1GB"
	expected.MaxParallelMaintenance = 2
	expected.MaxWALSize = "16GB"
	expected.CheckpointTimeout = "20min"
	expected.VerifyWorkers = 2
	expected.VerifySampleRows = 500
	expected.VerifySampleWindows = 10
	expected.VerifyBatchRows = 50
	expected.VerifyDutyCycle = 0.5
	expected.VerifyTableTimeout = 90 * time.Minute
	expected.VerifyConvergeTimeout = 90 * time.Second
	expected.VerifyCDCRows = 120
	expected.CDCSampleRows = 300
	if updated := server.configurationSnapshot(); updated != expected {
		t.Fatalf("complete update did not round trip\nwant: %#v\ngot:  %#v", expected, updated)
	}

	// Blank credentials are write-only no-ops, and omitted settings retain the
	// current values instead of resetting process defaults.
	got = requestJSON(t, server, http.MethodPut, "/api/config", `{"source":" ","target":"","ack_warnings":false}`, "secret")
	if got.Code != http.StatusOK {
		t.Fatalf("second PUT config status = %d, body = %s", got.Code, got.Body.String())
	}
	updated := server.configurationSnapshot()
	if updated.Source != "postgres://new-source/database" || updated.Target != "postgres://new-target/database" {
		t.Fatalf("credentials changed after blank update: source=%q target=%q", updated.Source, updated.Target)
	}
	if updated.Workers != 7 || updated.AckWarnings {
		t.Fatalf("partial update lost values: workers=%d ack=%v", updated.Workers, updated.AckWarnings)
	}
	if updated.Dir != cfg.Dir || !updated.NoCleanup || updated.SequenceOffset != 1234 || updated.EndPosition != "0/123" {
		t.Fatalf("startup/CLI-only configuration changed: %#v", updated)
	}
}

func TestInvalidConfigurationDoesNotReplaceCurrentConfiguration(t *testing.T) {
	server := newTestServer(t, validControllerConfig(t), "", noOpActions())
	before := server.configurationSnapshot()
	for _, body := range []string{
		`{"workers":0}`,
		`{"wal_sample_duration":"tomorrow"}`,
		`{"verify_duty_cycle":2}`,
		`{"unknown_setting":true}`,
	} {
		got := requestJSON(t, server, http.MethodPut, "/api/config", body, "")
		if got.Code != http.StatusBadRequest {
			t.Errorf("PUT %s status = %d, body = %s", body, got.Code, got.Body.String())
		}
		if after := server.configurationSnapshot(); after != before {
			t.Fatalf("invalid update %s changed config\nbefore: %#v\nafter:  %#v", body, before, after)
		}
	}
}

func TestConfigurationUpdateIsLockedWhileOperationsAreActive(t *testing.T) {
	for _, test := range []struct {
		name   string
		action string
		slot   string
		phase  state.Phase
	}{
		{name: "migration", action: "preflight", slot: "migration"},
		{name: "verification", action: "verify", slot: "verification", phase: state.PhaseFollow},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := validControllerConfig(t)
			if test.phase != "" {
				initializeStateAt(t, cfg.Dir, test.phase)
			}
			started := make(chan struct{})
			action := func(ctx context.Context, _ config.Config, _ io.Writer) error {
				close(started)
				<-ctx.Done()
				return ctx.Err()
			}
			actions := noOpActions()
			if test.action == "preflight" {
				actions.Preflight = action
			} else {
				actions.Verify = action
			}
			server := newTestServer(t, cfg, "", actions)
			if got := requestAction(t, server, test.action, server.configurationViewSnapshot().Revision, ""); got.Code != http.StatusAccepted {
				t.Fatalf("start status = %d, body = %s", got.Code, got.Body.String())
			}
			waitChannel(t, started)
			got := requestJSON(t, server, http.MethodPut, "/api/config", `{"workers":2}`, "")
			if got.Code != http.StatusConflict || !strings.Contains(got.Body.String(), test.slot+" is active") {
				t.Fatalf("PUT config status = %d, body = %s", got.Code, got.Body.String())
			}
			if got := request(t, server, http.MethodPost, "/api/actions/stop-"+test.slot, "stop-"+test.slot, ""); got.Code != http.StatusAccepted {
				t.Fatalf("stop status = %d, body = %s", got.Code, got.Body.String())
			}
			waitForState(t, server, test.slot, "stopped")
		})
	}
}

func TestActionUsesConfigurationSnapshot(t *testing.T) {
	cfg := validControllerConfig(t)
	cfg.Source = "original-source"
	release := make(chan struct{})
	received := make(chan config.Config, 1)
	actions := noOpActions()
	actions.Preflight = func(_ context.Context, cfg config.Config, _ io.Writer) error {
		<-release
		received <- cfg
		return nil
	}
	server := newTestServer(t, cfg, "", actions)
	if got := requestAction(t, server, "preflight", server.configurationViewSnapshot().Revision, ""); got.Code != http.StatusAccepted {
		t.Fatalf("preflight status = %d, body = %s", got.Code, got.Body.String())
	}
	server.mu.Lock()
	server.cfg.Source = "later-source"
	server.mu.Unlock()
	close(release)
	select {
	case got := <-received:
		if got.Source != "original-source" {
			t.Fatalf("action source = %q, want snapshot", got.Source)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for action configuration")
	}
}

func TestActionRejectsStaleConfigurationRevision(t *testing.T) {
	var called atomic.Bool
	actions := noOpActions()
	actions.Preflight = func(context.Context, config.Config, io.Writer) error {
		called.Store(true)
		return nil
	}
	server := newTestServer(t, validControllerConfig(t), "", actions)
	reviewed := request(t, server, http.MethodGet, "/api/config", "", "")
	if reviewed.Code != http.StatusOK {
		t.Fatalf("GET config status = %d, body = %s", reviewed.Code, reviewed.Body.String())
	}
	var reviewedConfiguration configurationView
	decode(t, reviewed, &reviewedConfiguration)

	updated := requestJSON(t, server, http.MethodPut, "/api/config", `{"workers":2}`, "")
	if updated.Code != http.StatusOK {
		t.Fatalf("PUT config status = %d, body = %s", updated.Code, updated.Body.String())
	}
	var current configurationView
	decode(t, updated, &current)
	if current.Revision == reviewedConfiguration.Revision {
		t.Fatalf("updated revision = %q, want a new token", current.Revision)
	}

	stale := requestAction(t, server, "preflight", reviewedConfiguration.Revision, "")
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "configuration revision") {
		t.Fatalf("stale action status = %d, body = %s", stale.Code, stale.Body.String())
	}
	if called.Load() {
		t.Fatal("stale action invoked preflight")
	}
	if operation := server.operationSnapshots()["migration"]; operation.State != "idle" {
		t.Fatalf("migration operation = %#v, want idle", operation)
	}

	accepted := requestAction(t, server, "preflight", current.Revision, "")
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("current action status = %d, body = %s", accepted.Code, accepted.Body.String())
	}
	waitForState(t, server, "migration", "succeeded")
	if !called.Load() {
		t.Fatal("current action did not invoke preflight")
	}
}

func TestActionRejectsConfigurationRevisionFromPreviousController(t *testing.T) {
	var called atomic.Bool
	actions := noOpActions()
	actions.Preflight = func(context.Context, config.Config, io.Writer) error {
		called.Store(true)
		return nil
	}
	serverA := newTestServer(t, validControllerConfig(t), "", noOpActions())
	serverB := newTestServer(t, validControllerConfig(t), "", actions)

	updatedA := requestJSON(t, serverA, http.MethodPut, "/api/config", `{"workers":2}`, "")
	updatedB := requestJSON(t, serverB, http.MethodPut, "/api/config", `{"workers":2}`, "")
	if updatedA.Code != http.StatusOK || updatedB.Code != http.StatusOK {
		t.Fatalf("matching configuration saves returned %d and %d", updatedA.Code, updatedB.Code)
	}
	var viewA, viewB configurationView
	decode(t, updatedA, &viewA)
	decode(t, updatedB, &viewB)
	generationA, sequenceA, okA := strings.Cut(viewA.Revision, ":")
	generationB, sequenceB, okB := strings.Cut(viewB.Revision, ":")
	if !okA || !okB || sequenceA != sequenceB {
		t.Fatalf("matching save counts produced revisions %q and %q", viewA.Revision, viewB.Revision)
	}
	if generationA == generationB {
		t.Fatalf("separate controllers reused configuration generation %q", generationA)
	}

	stale := requestAction(t, serverB, "preflight", viewA.Revision, "")
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "configuration revision") {
		t.Fatalf("previous-controller action status = %d, body = %s", stale.Code, stale.Body.String())
	}
	if called.Load() {
		t.Fatal("previous-controller revision invoked preflight")
	}
	if operation := serverB.operationSnapshots()["migration"]; operation.State != "idle" {
		t.Fatalf("migration operation = %#v, want idle", operation)
	}

	accepted := requestAction(t, serverB, "preflight", viewB.Revision, "")
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("current-controller action status = %d, body = %s", accepted.Code, accepted.Body.String())
	}
	waitForState(t, serverB, "migration", "succeeded")
	if !called.Load() {
		t.Fatal("current-controller revision did not invoke preflight")
	}
}

func TestIndexContainsControllerProgressUI(t *testing.T) {
	server := newTestServer(t, config.Config{Dir: t.TempDir()}, "", noOpActions())
	recorder := request(t, server, http.MethodGet, "/", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("index status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"pgmigrate controller", "Object completion", "lifecycleBar", "Stop migration",
		"confirmDialog", "data-action=\"run\" disabled", "no rows compared",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("index does not contain %q", want)
		}
	}
	if recorder.Header().Get("Content-Security-Policy") == "" {
		t.Error("Content-Security-Policy header is missing")
	}
}

func TestIndexContainsCompleteWriteOnlyConfigurationUI(t *testing.T) {
	server := newTestServer(t, config.Config{Dir: t.TempDir()}, "", noOpActions())
	recorder := request(t, server, http.MethodGet, "/", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("index status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"Migration configuration", "Database connections", "Migration", "Copy",
		"Target tuning", "Verification", "Advanced copy and runtime settings",
		"Advanced tuning overrides", "Advanced verification settings", "saveConfiguration",
		`data-secret-config="source" type="password"`,
		`data-secret-config="target" type="password"`,
		"sourceDsn.value='';targetDsn.value=''",
		"configurationRevision=data.revision",
		"X-PGMigrate-Config-Revision",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("configuration UI does not contain %q", want)
		}
	}
	for _, field := range []string{
		"table_filter", "ack_warnings", "allow_collation_change", "workers",
		"split_threshold", "restore_jobs", "pg_dump_path", "pg_restore_path",
		"metrics", "wal_sample_duration", "segment_prune_interval", "retry_base_copy",
		"skip_target_tuning", "warn_on_tuning_errors", "target_memory",
		"maintenance_work_mem", "max_parallel_maintenance_workers", "max_wal_size",
		"checkpoint_timeout", "verify_workers", "verify_sample_rows",
		"verify_sample_windows", "verify_batch_rows", "verify_duty_cycle",
		"verify_table_timeout", "verify_converge_timeout", "verify_cdc_rows",
		"cdc_sample_rows",
	} {
		if !strings.Contains(body, `data-config="`+field+`"`) {
			t.Errorf("configuration UI is missing %q", field)
		}
	}
	for _, forbidden := range []string{
		`data-config="dir"`, `data-config="listen"`, `data-config="token"`,
		`data-config="no_cleanup"`, `data-config="end_position"`,
		`data-config="sequence_offset"`, "localStorage", "pgmigrate-source", "pgmigrate-target",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("configuration UI unexpectedly contains %q", forbidden)
		}
	}
}

func newTestServer(t *testing.T, cfg config.Config, token string, actions Actions) *Server {
	t.Helper()
	server, err := New(Options{Config: cfg, Address: DefaultAddress, Token: token, Actions: actions})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.cancel)
	return server
}

func noOpActions() Actions {
	action := func(context.Context, config.Config, io.Writer) error { return nil }
	return Actions{Preflight: action, Run: action, Verify: action}
}

func request(t *testing.T, server *Server, method, target, confirmation, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	if confirmation != "" {
		req.Header.Set("X-PGMigrate-Confirm", confirmation)
	}
	if token != "" {
		req.Header.Set("X-PGMigrate-Token", token)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func requestJSON(t *testing.T, server *Server, method, target, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-PGMigrate-Token", token)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func requestAction(t *testing.T, server *Server, action, revision, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/actions/"+action, nil)
	req.Header.Set("X-PGMigrate-Confirm", action)
	req.Header.Set("X-PGMigrate-Config-Revision", revision)
	if token != "" {
		req.Header.Set("X-PGMigrate-Token", token)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func validControllerConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.FromEnvironment()
	cfg.Source = "source"
	cfg.Target = "target"
	cfg.Dir = t.TempDir()
	return cfg
}

func decode(t *testing.T, recorder *httptest.ResponseRecorder, value any) {
	t.Helper()
	if err := json.NewDecoder(recorder.Body).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func waitChannel(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for operation to start")
	}
}

func waitForState(t *testing.T, server *Server, slot, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if server.operationSnapshots()[slot].State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s operation state = %s, want %s", slot, server.operationSnapshots()[slot].State, want)
}

func initializeStateAt(t *testing.T, dir string, wanted state.Phase) {
	t.Helper()
	store, err := state.Open(context.Background(), dir, state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, phase := range []state.Phase{
		state.PhaseSetup, state.PhaseSchema, state.PhaseCopy, state.PhaseIndexes,
		state.PhaseCatchup, state.PhaseFollow, state.PhaseDrained, state.PhaseCutover,
		state.PhaseComplete,
	} {
		if wanted == state.PhasePreflight {
			return
		}
		if err := store.TransitionPhase(context.Background(), phase); err != nil {
			t.Fatal(err)
		}
		if phase == wanted {
			return
		}
	}
	t.Fatalf("unsupported test phase %q", wanted)
}
