// Package testutil contains helpers shared by unit and integration tests.
//
// Helpers accept testing.TB as their first argument, call Helper, and fail the
// current test rather than returning setup errors. Package-specific helpers
// should remain beside the tests that use them.
package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// WriteFile writes content beneath a temporary directory and returns its path.
func WriteFile(t testing.TB, name, content string) string {
	t.Helper()

	root := t.TempDir()
	filename := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatalf("create test directory: %v", err)
	}
	if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	return filename
}
