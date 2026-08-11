package config_test

import (
	"strings"
	"testing"

	"github.com/tgross/pgmigrate/internal/config"
	"github.com/tgross/pgmigrate/internal/testutil"
)

func TestParseFilter(t *testing.T) {
	input := `
# application tables
public.*
!public.audit_*

sales.orders
`
	filter, err := config.ParseFilter(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseFilter() error: %v", err)
	}

	tests := []struct {
		schema string
		table  string
		want   bool
	}{
		{schema: "public", table: "users", want: true},
		{schema: "public", table: "audit_log", want: false},
		{schema: "sales", table: "orders", want: true},
		{schema: "sales", table: "customers", want: false},
	}
	for _, test := range tests {
		if got := filter.Match(test.schema, test.table); got != test.want {
			t.Errorf("Match(%q, %q) = %t, want %t", test.schema, test.table, got, test.want)
		}
	}
}

func TestFilterExclusionsOnlyDefaultToIncluded(t *testing.T) {
	filter, err := config.ParseFilter(strings.NewReader("!private.*\n"))
	if err != nil {
		t.Fatalf("ParseFilter() error: %v", err)
	}
	if !filter.Match("public", "users") {
		t.Error("unmatched table excluded")
	}
	if filter.Match("private", "users") {
		t.Error("excluded table included")
	}
}

func TestFilterLastMatchWins(t *testing.T) {
	filter, err := config.ParseFilter(strings.NewReader("public.*\n!public.audit_*\npublic.audit_keep\n"))
	if err != nil {
		t.Fatalf("ParseFilter() error: %v", err)
	}
	if !filter.Match("public", "audit_keep") {
		t.Error("later include did not override exclusion")
	}
}

func TestFilterNormalizationAndFingerprint(t *testing.T) {
	left, err := config.ParseFilter(strings.NewReader(" # comment\n public.* \n\n !public.secret_* \n"))
	if err != nil {
		t.Fatalf("ParseFilter(left) error: %v", err)
	}
	right, err := config.ParseFilter(strings.NewReader("public.*\n!public.secret_*\n"))
	if err != nil {
		t.Fatalf("ParseFilter(right) error: %v", err)
	}

	const normalized = "public.*\n!public.secret_*"
	if got := left.Normalized(); got != normalized {
		t.Errorf("Normalized() = %q, want %q", got, normalized)
	}
	if left.Fingerprint() != right.Fingerprint() {
		t.Errorf("equivalent filters have different fingerprints: %s != %s", left.Fingerprint(), right.Fingerprint())
	}
	if len(left.Fingerprint()) != 64 {
		t.Errorf("fingerprint length = %d, want 64", len(left.Fingerprint()))
	}
}

func TestLoadFilter(t *testing.T) {
	filename := testutil.WriteFile(t, "filters/tables.txt", "public.*\n")
	filter, err := config.LoadFilter(filename)
	if err != nil {
		t.Fatalf("LoadFilter() error: %v", err)
	}
	if !filter.Match("public", "users") {
		t.Error("loaded filter did not match table")
	}
}

func TestParseFilterRejectsInvalidPatterns(t *testing.T) {
	for _, input := range []string{"users\n", "public.[\n", "!\n", "a.b.c\n"} {
		t.Run(strings.TrimSpace(input), func(t *testing.T) {
			if _, err := config.ParseFilter(strings.NewReader(input)); err == nil {
				t.Fatalf("ParseFilter(%q) succeeded", input)
			}
		})
	}
}
