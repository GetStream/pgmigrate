package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	if err := store.UpsertTable(ctx, state.Table{OID: 1, Schema: "public", Name: "done"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTable(ctx, state.Table{OID: 2, Schema: "public", Name: "pending"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTable(ctx, 1); err != nil {
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
}

func TestRunAndVerifyCanBeControlledConcurrently(t *testing.T) {
	runStarted := make(chan struct{})
	verifyStarted := make(chan struct{})
	blocking := func(started chan<- struct{}) Action {
		return func(ctx context.Context, _ config.Config, _ io.Writer) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
	}
	server := newTestServer(t, config.Config{Dir: t.TempDir()}, "", Actions{
		Preflight: func(context.Context, config.Config, io.Writer) error { return nil },
		Run:       blocking(runStarted),
		Verify:    blocking(verifyStarted),
	})

	if got := request(t, server, http.MethodPost, "/api/actions/run", "run", ""); got.Code != http.StatusAccepted {
		t.Fatalf("run status = %d, body = %s", got.Code, got.Body.String())
	}
	waitChannel(t, runStarted)
	if got := request(t, server, http.MethodPost, "/api/actions/verify", "verify", ""); got.Code != http.StatusAccepted {
		t.Fatalf("verify status = %d, body = %s", got.Code, got.Body.String())
	}
	waitChannel(t, verifyStarted)
	if got := request(t, server, http.MethodPost, "/api/actions/preflight", "preflight", ""); got.Code != http.StatusConflict {
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
}

func TestIndexContainsControllerProgressUI(t *testing.T) {
	server := newTestServer(t, config.Config{Dir: t.TempDir()}, "", noOpActions())
	recorder := request(t, server, http.MethodGet, "/", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("index status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, want := range []string{"pgmigrate controller", "Object completion", "lifecycleBar", "Stop migration"} {
		if !strings.Contains(body, want) {
			t.Errorf("index does not contain %q", want)
		}
	}
	if recorder.Header().Get("Content-Security-Policy") == "" {
		t.Error("Content-Security-Policy header is missing")
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
