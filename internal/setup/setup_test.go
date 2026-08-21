package setup

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNamesAreDeterministicAndSafe(t *testing.T) {
	pubA, slotA := Names("source", "migration")
	pubB, slotB := Names("source", "migration")
	if pubA != pubB || slotA != slotB {
		t.Fatal("Names is not deterministic")
	}
	for _, name := range []string{pubA, slotA} {
		if len(name) > 63 || !safeNamePattern.MatchString(name) {
			t.Errorf("unsafe PostgreSQL name %q", name)
		}
	}
	pubC, slotC := Names("source", "other")
	if pubA == pubC || slotA == slotC {
		t.Error("migration identity did not affect names")
	}
}

func TestSourceFingerprintSeparatesClusterAndDatabase(t *testing.T) {
	if SourceFingerprint("system-a", "db") == SourceFingerprint("system-b", "db") {
		t.Error("system identifier did not affect fingerprint")
	}
	if SourceFingerprint("system", "db-a") == SourceFingerprint("system", "db-b") {
		t.Error("database did not affect fingerprint")
	}
}

func TestWriteSnapshotAtomic(t *testing.T) {
	dir := t.TempDir()
	want := Snapshot{
		SourceFingerprint: "fingerprint",
		Publication:       "pgmigrate_pub_a",
		Slot:              "pgmigrate_slot_a",
		Name:              "snapshot",
		ConsistentPoint:   "0/10",
		CreatedAt:         time.Now().UTC().Truncate(time.Nanosecond),
	}
	if err := writeSnapshotAtomic(dir, want); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got Snapshot
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot.json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary snapshot remains: %v", err)
	}
	if !strings.HasSuffix(string(data), "\n") {
		t.Error("snapshot JSON lacks final newline")
	}
}

func TestWatchdogRejectsInvalidIntervalWithoutTouchingExporter(t *testing.T) {
	holder := &Holder{}
	err := <-holder.Watchdog(context.Background(), 0)
	if err == nil || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("Watchdog() error = %v", err)
	}
}

func TestSnapshotHolderConnectionDisablesWALSenderTimeout(t *testing.T) {
	config, err := snapshotHolderConfig("postgres://user:pass@localhost:5432/chat?connect_timeout=7")
	if err != nil {
		t.Fatal(err)
	}
	if got := config.RuntimeParams["wal_sender_timeout"]; got != "0" {
		t.Fatalf("wal_sender_timeout = %q, want 0", got)
	}
	if got := config.RuntimeParams["application_name"]; got != "pgmigrate_snapshot_holder" {
		t.Fatalf("application_name = %q", got)
	}
	if config.ConnectTimeout != 7*time.Second {
		t.Fatalf("connect timeout = %s", config.ConnectTimeout)
	}

	// DialFunc must still produce the normal network error for an unreachable
	// local endpoint. This exercises the explicit keepalive dialer without
	// depending on its unexported function identity.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	conn, dialErr := config.DialFunc(ctx, "tcp", "127.0.0.1:1")
	if conn != nil {
		_ = conn.Close()
	}
	var opErr *net.OpError
	if dialErr == nil || !errors.As(dialErr, &opErr) {
		t.Fatalf("DialFunc error = %v, want network error", dialErr)
	}
}

func TestRecoverStaleRequiresExplicitNoSnapshotConfirmation(t *testing.T) {
	err := RecoverStale(context.Background(), Config{}, ResumeConfirmation{})
	if err == nil || !strings.Contains(err.Error(), "no snapshot") {
		t.Fatalf("RecoverStale() error = %v", err)
	}
}
