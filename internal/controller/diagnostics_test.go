package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/config"
)

func TestWALComparison(t *testing.T) {
	for _, test := range []struct {
		name, source, staged, applied string
		uncaptured, replay, total     string
		failed                        bool
	}{
		{name: "split backlog", source: "1/30", staged: "1/20", applied: "1/10", uncaptured: "16", replay: "16", total: "32"},
		{name: "staged drained but capture behind", source: "1/30", staged: "1/10", applied: "1/10", uncaptured: "32", replay: "0", total: "32"},
		{name: "sampled equality", source: "1/30", staged: "1/30", applied: "1/30", uncaptured: "0", replay: "0", total: "0"},
		{name: "above JS safe integer", source: "FFFFFFFF/FFFFFFFF", staged: "FFFFFFFF/FFFFFFFE", applied: "0/1", uncaptured: "1", replay: "18446744073709551613", total: "18446744073709551614"},
		{name: "word rollover", source: "2/0", staged: "1/FFFFFFFF", applied: "1/FFFFFFFE", uncaptured: "1", replay: "1", total: "2"},
		{name: "source unavailable", staged: "1/20", applied: "1/10", replay: "16"},
		{name: "copy before apply", source: "1/30", staged: "1/20", applied: "0/0", uncaptured: "16"},
		{name: "no state", source: "1/30"},
		{name: "uninitialized", source: "1/30", staged: "0/0", applied: "0/0"},
		{name: "applied ahead", source: "1/30", staged: "1/10", applied: "1/20", failed: true},
		{name: "source behind", source: "1/10", staged: "1/30", applied: "1/20", replay: "16", failed: true},
		{name: "invalid trailing text", source: "1/30junk", staged: "1/20", applied: "1/10", replay: "16"},
		{name: "invalid checkpoint", source: "1/30", staged: "1/+20", applied: "1/10"},
	} {
		t.Run(test.name, func(t *testing.T) {
			view := walView{SourceLSN: test.source, StagedLSN: test.staged, AppliedLSN: test.applied}
			view.compare()
			for _, check := range []struct {
				got  *string
				want string
			}{{view.UncapturedBytes, test.uncaptured}, {view.ReplayBytes, test.replay}, {view.TotalBytes, test.total}} {
				if check.want == "" {
					if check.got != nil {
						t.Fatalf("unknown gap = %q, want null", *check.got)
					}
				} else if check.got == nil || *check.got != check.want {
					t.Fatalf("gap = %v, want %s", check.got, check.want)
				}
			}
			if (view.Error != "") != test.failed {
				t.Fatalf("error = %q", view.Error)
			}
			encoded, err := json.Marshal(view)
			if err != nil {
				t.Fatal(err)
			}
			if test.total != "" && !strings.Contains(string(encoded), `"total_bytes":"`+test.total+`"`) {
				t.Fatalf("JSON lost decimal string: %s", encoded)
			}
		})
	}
	view := walView{SourceLSN: "1/30", StagedLSN: "1/20", AppliedLSN: "1/10", Error: "source identity mismatch"}
	view.compare()
	if view.TotalBytes != nil || view.UncapturedBytes != nil || view.ReplayBytes == nil {
		t.Fatalf("identity failure did not suppress source comparison: %+v", view)
	}
}

func TestMaintenanceProgressIsPhaseSpecific(t *testing.T) {
	for _, test := range []struct {
		name, kind, data, unit string
		done, total            int64
		measurable             bool
	}{
		{"index blocks", "index", `{"phase":"building index: scanning table","blocks_done":20,"blocks_total":100}`, "blocks", 20, 100, true},
		{"index tuples", "index", `{"phase":"building index: loading tuples in tree","blocks_done":100,"blocks_total":100,"tuples_done":2,"tuples_total":8}`, "tuples", 2, 8, true},
		{"build sort stale blocks", "index", `{"phase":"building index: sorting live tuples","blocks_done":100,"blocks_total":100}`, "", 0, 0, false},
		{"waiting", "index", `{"phase":"waiting for old snapshots","lockers_done":1,"lockers_total":4,"current_locker_pid":123}`, "lockers", 1, 4, true},
		{"sort stale blocks", "index", `{"phase":"index validation: sorting tuples","blocks_done":100,"blocks_total":100}`, "", 0, 0, false},
		{"scan", "vacuum", `{"phase":"scanning heap","heap_blks_scanned":7,"heap_blks_total":10}`, "heap blocks scanned", 7, 10, true},
		{"vacuum heap", "vacuum", `{"phase":"vacuuming heap","heap_blks_scanned":10,"heap_blks_vacuumed":3,"heap_blks_total":10}`, "heap blocks vacuumed", 3, 10, true},
		{"PG17 indexes", "vacuum", `{"phase":"vacuuming indexes","heap_blks_scanned":10,"heap_blks_total":10,"index_vacuum_count":1}`, "indexes", 0, 0, false},
		{"PG18 indexes", "vacuum", `{"phase":"cleaning up indexes","indexes_processed":1,"indexes_total":3}`, "indexes", 1, 3, true},
		{"cleanup not done", "vacuum", `{"phase":"performing final cleanup","heap_blks_scanned":10,"heap_blks_total":10}`, "", 0, 0, false},
		{"vacuum full scan", "vacuum_full", `{"command":"VACUUM FULL","phase":"seq scanning heap","heap_blks_scanned":3,"heap_blks_total":10}`, "heap blocks scanned", 3, 10, true},
		{"vacuum full rebuild", "vacuum_full", `{"command":"VACUUM FULL","phase":"rebuilding index","heap_blks_scanned":10,"heap_blks_total":10}`, "", 0, 0, false},
		{"unknown total", "index", `{"phase":"building index","blocks_done":50}`, "tuples", 0, 0, false},
		{"inconsistent counters", "index", `{"phase":"building index","blocks_done":101,"blocks_total":100}`, "blocks", 101, 100, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var p maintenanceCounters
			if err := json.Unmarshal([]byte(test.data), &p); err != nil {
				t.Fatal(err)
			}
			job := maintenanceJob{Kind: test.kind}
			job.setProgress(p)
			if job.Done != test.done || job.Total != test.total || job.Unit != test.unit || (job.Percent != nil) != test.measurable {
				t.Fatalf("progress = %+v", job)
			}
			if job.Percent != nil && *job.Percent != 100*float64(test.done)/float64(test.total) {
				t.Fatalf("percent = %v", *job.Percent)
			}
		})
	}
}

func TestDiagnosticsAuthAndUnavailableStateAreReadOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-created")
	server := newTestServer(t, config.Config{Dir: dir, Source: "postgres://secret:credential%zz@source/db", Target: "postgres://secret:credential%zz@target/db"}, "token", noOpActions())
	if got := request(t, server, http.MethodGet, "/api/diagnostics", "", ""); got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", got.Code)
	}
	got := request(t, server, http.MethodGet, "/api/diagnostics", "", "token")
	if got.Code != http.StatusOK {
		t.Fatalf("status = %d, %s", got.Code, got.Body.String())
	}
	var view diagnosticsView
	decode(t, got, &view)
	if view.WAL.TotalBytes != nil || view.WAL.Error == "" || view.Source.Error == "" || view.Target.Error == "" {
		t.Fatalf("unavailable diagnostics = %+v", view)
	}
	for _, secret := range []string{"credential", "postgres://", "secret", "%zz"} {
		if strings.Contains(got.Body.String(), secret) {
			t.Fatalf("diagnostics exposed %q", secret)
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("diagnostics created state directory: %v", err)
	}
	if status := request(t, server, http.MethodGet, "/api/status", "", "token"); status.Code != http.StatusOK {
		t.Fatal("diagnostics failure affected durable status")
	}
	if script := request(t, server, http.MethodGet, "/progress.js", "", ""); script.Code != http.StatusOK || !strings.HasPrefix(script.Header().Get("Content-Type"), "text/javascript") {
		t.Fatal("progress script is not served")
	}
}

func TestDiagnosticsCacheSharesSamplesAndInvalidatesConfiguration(t *testing.T) {
	var cache diagnosticsCache
	var calls atomic.Int64
	collect := func() diagnosticsView { calls.Add(1); return diagnosticsView{SampledAt: time.Now().UTC()} }
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cache.sample(context.Background(), "one", collect); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("20 browser requests collected %d samples", calls.Load())
	}
	if _, err := cache.sample(context.Background(), "two", collect); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("configuration change did not invalidate cache")
	}
	cache.mu.Lock()
	cache.value.SampledAt = time.Now().Add(-2 * diagnosticsInterval)
	cache.mu.Unlock()
	if _, err := cache.sample(context.Background(), "two", collect); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("expired sample was reused")
	}
}

func TestDiagnosticsWaitingRequestCanCancel(t *testing.T) {
	var cache diagnosticsCache
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := cache.sample(ctx, "one", func() diagnosticsView { close(entered); <-release; return diagnosticsView{SampledAt: time.Now()} })
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("cancel error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request cancellation blocked on diagnostics")
	}
}
