package config_test

import (
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/config"
)

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
