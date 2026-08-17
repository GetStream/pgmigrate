package cdcbench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsUnstableUpdateGeometry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Rows++
	if err := cfg.validate(); err == nil {
		t.Fatal("rows not divisible by batch were accepted")
	}
	cfg = DefaultConfig()
	cfg.BacklogUpdates++
	if err := cfg.validate(); err == nil {
		t.Fatal("backlog not divisible by batch was accepted")
	}
}

func TestRate(t *testing.T) {
	if got := rate(25_000, 2*time.Second); got != 12_500 {
		t.Fatalf("rate = %v, want 12500", got)
	}
}

func TestRunRejectsReusedWorkDirectoryBeforeStartingContainers(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "old.seg"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.WorkDir = dir
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("reused work directory was accepted")
	}
}
