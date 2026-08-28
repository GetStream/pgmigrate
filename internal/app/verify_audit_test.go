package app

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/verify"
)

func TestVerificationAuditAppendsEveryConcurrentObservation(t *testing.T) {
	dir := t.TempDir()
	var ids []string
	for run := 0; run < 2; run++ {
		audit, err := newVerificationAudit(dir, "7", "42")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, audit.runID)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				events := []verify.AuditEvent{
					{Time: time.Now().UTC(), Table: "public.items", Key: []string{"quoted\"key\n"}, Stratum: "cdc", Outcome: "deferred", Kind: verify.DiffDifferent, Source: &verify.RowSnapshot{Present: true, Hash: "9223372036854775807", Version: "123"}, Target: &verify.RowSnapshot{Present: true, Hash: "0"}},
					{Time: time.Now().UTC(), Table: "public.items", Key: []string{"quoted\"key\n"}, Stratum: "cdc", Outcome: "converged"},
				}
				if err := audit.write(events); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if err := audit.finish(true, true, nil); err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] == ids[1] || ids[0] == "" {
		t.Fatalf("run IDs not unique: %v", ids)
	}
	path := filepath.Join(dir, "log", "verify-audit.jsonl")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("audit exposes keys with permissions %v", info.Mode())
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	counts := make(map[string]map[string]int)
	for scanner.Scan() {
		var event struct {
			RunID string `json:"run_id"`
			verify.AuditEvent
		}
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatalf("interleaved/invalid JSON: %v", err)
		}
		if event.Time.IsZero() {
			t.Fatal("missing timestamp")
		}
		if event.Outcome == "run_started" && !slices.Equal(event.IgnoredApps, []string{"7", "42"}) {
			t.Fatalf("run scope missing from audit: %+v", event)
		}
		if counts[event.RunID] == nil {
			counts[event.RunID] = make(map[string]int)
		}
		counts[event.RunID][event.Outcome]++
		if event.Outcome == "deferred" && (event.Source.Hash != "9223372036854775807" || event.Source.Version != "123" || event.Key[0] != "quoted\"key\n") {
			t.Fatalf("metadata corrupted: %+v", event)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		c := counts[id]
		if c["run_started"] != 1 || c["run_converged"] != 1 || c["deferred"] != 8 || c["converged"] != 8 {
			t.Fatalf("lost observations for %s: %v", id, c)
		}
	}
}

func TestVerificationAuditFinalOutcomesAndWriteFailures(t *testing.T) {
	for _, tc := range []struct {
		name                string
		complete, converged bool
		err                 error
		want                string
	}{
		{name: "converged", complete: true, converged: true, want: "run_converged"},
		{name: "diverged", complete: true, want: "run_diverged"},
		{name: "incomplete", want: "run_incomplete"},
		{name: "error", complete: true, converged: true, err: errors.New("stopped"), want: "run_incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			audit, err := newVerificationAudit(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := audit.finish(tc.complete, tc.converged, tc.err); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(filepath.Join(dir, "log", "verify-audit.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			dec := json.NewDecoder(f)
			var e verify.AuditEvent
			if err := dec.Decode(&e); err != nil {
				t.Fatal(err)
			}
			if err := dec.Decode(&e); err != nil {
				t.Fatal(err)
			}
			if e.Outcome != tc.want {
				t.Fatalf("footer=%s want %s", e.Outcome, tc.want)
			}
			if err := audit.write([]verify.AuditEvent{{Outcome: "deferred"}}); err == nil {
				t.Fatal("closed audit write succeeded")
			}
		})
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "log"), []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := newVerificationAudit(dir); err == nil {
		t.Fatal("invalid audit path silently ignored")
	}
	audit, err := newVerificationAudit(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := audit.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := audit.finish(true, true, nil); err == nil {
		t.Fatal("footer failure ignored")
	}
}
