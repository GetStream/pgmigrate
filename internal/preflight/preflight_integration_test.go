//go:build integration

package preflight_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/preflight"
)

// TestPG17ReplicationPrivilegeProbe checks that replication capability is
// decided by opening a replication connection rather than by inspecting role
// attributes. A non-superuser role holding only REPLICATION must pass, which is
// the capability managed services grant without setting pg_roles.rolreplication.
func TestPG17ReplicationPrivilegeProbe(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn := source.Connect(t)
	var oid uint32
	for _, statement := range []string{
		"CREATE TABLE selected (id bigint PRIMARY KEY, value text)",
		"CREATE ROLE plain LOGIN PASSWORD 'probe'",
		"CREATE ROLE streamer LOGIN REPLICATION PASSWORD 'probe'",
		"GRANT CREATE ON DATABASE pgmigrate TO plain, streamer",
		"GRANT SELECT ON selected TO plain, streamer",
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := conn.QueryRow(ctx, "SELECT 'selected'::regclass::oid").Scan(&oid); err != nil {
		t.Fatal(err)
	}

	tool := filepath.Join(t.TempDir(), "pg-tool")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\necho 'pg_dump (PostgreSQL) 17.1'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	probe := func(dsn string) (preflight.Finding, bool) {
		result, err := preflight.Run(ctx, preflight.Config{
			SourceDSN: dsn, TargetDSN: target.URI,
			Tables:            []preflight.Table{{OID: oid, Schema: "public", Name: "selected"}},
			PGDumpPath:        tool,
			PGRestorePath:     tool,
			WALSampleDuration: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("preflight: %v", err)
		}
		for _, finding := range result.Findings {
			if finding.ID == "source-replication-privilege" {
				return finding, true
			}
		}
		return preflight.Finding{}, false
	}

	finding, found := probe(roleDSN(t, source.URI, "plain", "probe"))
	if !found || finding.Severity != preflight.SeverityError {
		t.Fatalf("role without replication rights finding = %+v found=%v", finding, found)
	}
	for _, remediation := range []string{"ALTER ROLE", "rds_replication", "WITH REPLICATION"} {
		if !strings.Contains(finding.Message, remediation) {
			t.Errorf("finding message omits %q remediation: %s", remediation, finding.Message)
		}
	}

	if finding, found := probe(roleDSN(t, source.URI, "streamer", "probe")); found {
		t.Fatalf("non-superuser REPLICATION role was rejected: %+v", finding)
	}
}

func roleDSN(t testing.TB, uri, user, password string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parse instance URI: %v", err)
	}
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}

func TestPG17MandatoryPreflight(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	sourceConn := source.Connect(t)
	if _, err := sourceConn.Exec(ctx, "CREATE TABLE selected (id bigint PRIMARY KEY, value text)"); err != nil {
		t.Fatal(err)
	}
	var oid uint32
	if err := sourceConn.QueryRow(ctx, "SELECT 'selected'::regclass::oid").Scan(&oid); err != nil {
		t.Fatal(err)
	}

	tool := filepath.Join(t.TempDir(), "pg-tool")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\necho 'pg_dump (PostgreSQL) 17.1'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	result, err := preflight.Run(ctx, preflight.Config{
		SourceDSN:           source.URI,
		TargetDSN:           target.URI,
		Tables:              []preflight.Table{{OID: oid, Schema: "public", Name: "selected"}},
		PGDumpPath:          tool,
		PGRestorePath:       tool,
		WALSampleDuration:   10 * time.Millisecond,
		AcknowledgeWarnings: true,
	})
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	// A clean pair produces the known unbounded-retention warning plus the
	// informational tuning plan, and nothing else. Listing both by ID keeps this
	// strict: a new finding on a clean pair still has to be accounted for here.
	clean := map[string]preflight.Finding{}
	for _, finding := range result.Findings {
		clean[finding.ID] = finding
	}
	if !result.Allowed || len(result.Findings) != 2 ||
		clean["wal-retention-unbounded"].Severity != preflight.SeverityWarning ||
		clean["target-tuning"].Severity != preflight.SeverityInfo {
		t.Fatalf("preflight allowed=%v findings=%+v", result.Allowed, result.Findings)
	}
	// A container at stock settings has something to tune, so the plan has to name
	// the checkpoint settings that dominate a large load.
	for _, want := range []string{"max_wal_size", "checkpoint_timeout"} {
		if !strings.Contains(clean["target-tuning"].Message, want) {
			t.Errorf("tuning plan does not mention %s: %q", want, clean["target-tuning"].Message)
		}
	}

	for _, statement := range []string{
		"SELECT pg_catalog.lo_create(0)",
		"CREATE UNLOGGED TABLE live_unlogged (id integer)",
		"CREATE TABLE generated_source (id integer, doubled integer GENERATED ALWAYS AS (id*2) STORED, UNIQUE (id) DEFERRABLE)",
		"CREATE TABLE exclusion_source (period int4range, EXCLUDE USING gist (period WITH &&))",
		"CREATE MATERIALIZED VIEW materialized_source AS SELECT 1 AS id",
	} {
		if _, err := sourceConn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	result, err = preflight.Run(ctx, preflight.Config{
		SourceDSN:           source.URI,
		TargetDSN:           target.URI,
		Tables:              []preflight.Table{{OID: oid, Schema: "public", Name: "selected"}},
		PGDumpPath:          tool,
		PGRestorePath:       tool,
		WALSampleDuration:   10 * time.Millisecond,
		AcknowledgeWarnings: true,
	})
	if err != nil {
		t.Fatalf("object preflight: %v", err)
	}
	byID := make(map[string]preflight.Finding)
	for _, finding := range result.Findings {
		byID[finding.ID] = finding
	}
	if result.Allowed || byID["large-objects"].Severity != preflight.SeverityError {
		t.Fatalf("large-object result allowed=%v finding=%+v", result.Allowed, byID["large-objects"])
	}
	for _, id := range []string{
		"unlogged-tables", "generated-columns", "exclusion-constraints",
		"deferrable-unique-constraints", "materialized-views",
	} {
		if byID[id].Severity != preflight.SeverityWarning {
			t.Errorf("%s finding = %+v", id, byID[id])
		}
	}
	if !strings.Contains(byID["unlogged-tables"].Message, "live writes") {
		t.Errorf("unlogged warning is not explicit: %q", byID["unlogged-tables"].Message)
	}
	var unloggedOID uint32
	if err := sourceConn.QueryRow(ctx, "SELECT 'live_unlogged'::regclass::oid").Scan(&unloggedOID); err != nil {
		t.Fatal(err)
	}
	result, err = preflight.Run(ctx, preflight.Config{
		SourceDSN: source.URI, TargetDSN: target.URI,
		Tables:     []preflight.Table{{OID: unloggedOID, Schema: "public", Name: "live_unlogged"}},
		PGDumpPath: tool, PGRestorePath: tool, WALSampleDuration: 10 * time.Millisecond,
		AcknowledgeWarnings: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Allowed {
		t.Fatal("selected unlogged table was acknowledgeable")
	}
	found := false
	for _, finding := range result.Findings {
		if finding.ID == "table-"+fmt.Sprint(unloggedOID)+"-unlogged" &&
			finding.Severity == preflight.SeverityError {
			found = true
		}
	}
	if !found {
		t.Fatalf("selected unlogged error missing: %+v", result.Findings)
	}
}
