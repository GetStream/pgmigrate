package config_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/config"
)

func TestIgnoredVerificationApps(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  []string
	}{
		{"", nil}, {" \t", nil}, {"7", []string{"7"}},
		{" 7,42,007 ", []string{"42", "7"}},
	} {
		cfg := config.FromEnvironment()
		cfg.VerifyIgnoreApps = tc.input
		got, err := cfg.IgnoredVerificationApps()
		if err != nil || !slices.Equal(got, tc.want) {
			t.Errorf("IgnoredVerificationApps(%q) = %v, %v; want %v", tc.input, got, err, tc.want)
		}
		if err := cfg.ValidateVerify(); err != nil {
			t.Errorf("ValidateVerify(%q): %v", tc.input, err)
		}
	}
	for _, input := range []string{"0", "-1", "1,", ",1", "1,,2", "foo", "1.5", "9223372036854775808", "1); DROP TABLE items"} {
		cfg := config.FromEnvironment()
		cfg.VerifyIgnoreApps = input
		if err := cfg.ValidateVerify(); err == nil || !strings.Contains(err.Error(), "verify-ignore-apps") {
			t.Errorf("ValidateVerify(%q) = %v, want app ID validation error", input, err)
		}
	}
}

func TestFromEnvironment(t *testing.T) {
	t.Setenv(config.SourceEnv, "postgres://source/db")
	t.Setenv(config.TargetEnv, "postgres://target/db")

	got := config.FromEnvironment()
	if got.Source != "postgres://source/db" {
		t.Fatalf("Source = %q", got.Source)
	}
	if got.Target != "postgres://target/db" {
		t.Fatalf("Target = %q", got.Target)
	}
	if got.ReplayWorkers != 8 {
		t.Fatalf("ReplayWorkers = %d, want 8", got.ReplayWorkers)
	}
	if got.ReplayBatchBytes != 8<<20 || got.ReplayBatchChanges != 32_768 {
		t.Fatalf(
			"Replay batch = %d bytes / %d changes, want %d / %d",
			got.ReplayBatchBytes, got.ReplayBatchChanges, 8<<20, 32_768,
		)
	}
}

func TestValidateConnections(t *testing.T) {
	valid := config.Config{
		Source: "postgres://source/db",
		Target: "postgres://target/db",
		Dir:    t.TempDir(),
	}
	if err := valid.ValidateConnections(); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}

	err := (config.Config{}).ValidateConnections()
	if err == nil {
		t.Fatal("empty configuration accepted")
	}
	for _, want := range []string{config.SourceEnv, config.TargetEnv, "--dir"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestValidateDir(t *testing.T) {
	if err := (config.Config{Dir: "migration"}).ValidateDir(); err != nil {
		t.Fatalf("valid directory rejected: %v", err)
	}
	if err := (config.Config{Dir: " \t"}).ValidateDir(); err == nil {
		t.Fatal("blank directory accepted")
	}
}

func TestValidateReplayWorkers(t *testing.T) {
	for _, workers := range []int{1, config.ReplayWorkersMax} {
		if err := config.ValidateReplayWorkers(workers); err != nil {
			t.Errorf("ValidateReplayWorkers(%d) error = %v", workers, err)
		}
	}
	for _, workers := range []int{0, -1, config.ReplayWorkersMax + 1} {
		if err := config.ValidateReplayWorkers(workers); err == nil {
			t.Errorf("ValidateReplayWorkers(%d) accepted an invalid value", workers)
		}
	}
}
