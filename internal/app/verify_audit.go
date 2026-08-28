package app

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/verify"
)

// verificationAudit is independent of the controller's truncated output buffer.
// Each batch is synced before verification continues; an audit failure is fatal.
type verificationAudit struct {
	mu    sync.Mutex
	file  *os.File
	runID string
}

func newVerificationAudit(dir string, ignoredApps ...string) (*verificationAudit, error) {
	path := filepath.Join(dir, "log", "verify-audit.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open verification audit: %w", err)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		file.Close()
		return nil, err
	}
	out := &verificationAudit{file: file, runID: hex.EncodeToString(id[:])}
	if err := out.write([]verify.AuditEvent{{Time: time.Now().UTC(), Outcome: "run_started", IgnoredApps: ignoredApps}}); err != nil {
		file.Close()
		return nil, err
	}
	return out, nil
}

func (a *verificationAudit) write(events []verify.AuditEvent) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	enc := json.NewEncoder(a.file)
	for _, event := range events {
		record := struct {
			RunID string `json:"run_id"`
			verify.AuditEvent
		}{a.runID, event}
		if err := enc.Encode(record); err != nil {
			return err
		}
	}
	return a.file.Sync()
}

func (a *verificationAudit) finish(complete, converged bool, runErr error) error {
	outcome := "run_incomplete"
	if runErr == nil && complete {
		outcome = "run_diverged"
		if converged {
			outcome = "run_converged"
		}
	}
	err := a.write([]verify.AuditEvent{{Time: time.Now().UTC(), Outcome: outcome}})
	return errors.Join(err, a.file.Close())
}
