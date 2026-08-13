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

func TestPG17SessionTimeouts(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sourceConn := source.Connect(t)
	if _, err := sourceConn.Exec(ctx, "CREATE TABLE selected (id bigint PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceConn.Exec(ctx, "ALTER DATABASE pgmigrate SET statement_timeout = '60s'"); err != nil {
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
		SourceDSN: source.URI, TargetDSN: target.URI,
		Tables:     []preflight.Table{{OID: oid, Schema: "public", Name: "selected"}},
		PGDumpPath: tool, PGRestorePath: tool, WALSampleDuration: 10 * time.Millisecond,
		AcknowledgeWarnings: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	var found preflight.Finding
	for _, finding := range result.Findings {
		if finding.ID == "source-statement-timeout" {
			found = finding
		}
	}
	if found.Severity != preflight.SeverityWarning || !strings.Contains(found.Message, "60s") {
		t.Fatalf("source-statement-timeout = %+v findings=%+v", found, result.Findings)
	}
	if !result.Allowed {
		t.Fatalf("acknowledgeable timeout blocked: %+v", result.Findings)
	}
}

// TestPG17SequenceHeadroom checks the headroom report against real sequences: a
// sequence with less room left than --sequence-offset stops the migration because
// the cutover's setval would be rejected, one merely running low is
// acknowledgeable, and widening it with the ALTER the finding recommends clears
// it. Sequences no selected table owns or defaults from are none of pgmigrate's
// business and stay unreported.
func TestPG17SequenceHeadroom(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn := source.Connect(t)
	for _, statement := range []string{
		"CREATE SEQUENCE low MAXVALUE 6000000",
		"CREATE SEQUENCE tight MAXVALUE 2147483647",
		"CREATE SEQUENCE unselected MAXVALUE 10",
		`CREATE TABLE selected (
			id bigserial PRIMARY KEY,
			tag integer DEFAULT nextval('low'),
			note integer DEFAULT nextval('tight'))`,
		"SELECT setval('low',2000000)",
		"SELECT setval('tight',2147000000)",
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	oids := make(map[string]uint32, 4)
	for _, name := range []string{"selected", "selected_id_seq", "low", "tight", "unselected"} {
		var oid uint32
		if err := conn.QueryRow(ctx, "SELECT $1::regclass::oid", name).Scan(&oid); err != nil {
			t.Fatalf("resolve %s: %v", name, err)
		}
		oids[name] = oid
	}

	tool := filepath.Join(t.TempDir(), "pg-tool")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\necho 'pg_dump (PostgreSQL) 17.1'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := func() (preflight.Result, map[string]preflight.Finding) {
		result, err := preflight.Run(ctx, preflight.Config{
			SourceDSN: source.URI, TargetDSN: target.URI,
			Tables:     []preflight.Table{{OID: oids["selected"], Schema: "public", Name: "selected"}},
			PGDumpPath: tool, PGRestorePath: tool, WALSampleDuration: 10 * time.Millisecond,
			SequenceOffset: 1_000_000, AcknowledgeWarnings: true,
		})
		if err != nil {
			t.Fatalf("preflight: %v", err)
		}
		byID := make(map[string]preflight.Finding, len(result.Findings))
		for _, finding := range result.Findings {
			byID[finding.ID] = finding
		}
		return result, byID
	}
	headroom := func(name string) string {
		return fmt.Sprintf("sequence-%d-headroom", oids[name])
	}

	result, byID := run()
	if result.Allowed {
		t.Error("a sequence with less room than the offset was acknowledgeable")
	}
	if finding := byID[headroom("tight")]; finding.Severity != preflight.SeverityError ||
		!strings.Contains(finding.Message, "public.tight") {
		t.Errorf("tight finding = %+v, want an error naming the sequence", finding)
	}
	if finding := byID[headroom("low")]; finding.Severity != preflight.SeverityWarning ||
		!strings.Contains(finding.Message, "4000000 values left") {
		t.Errorf("low finding = %+v, want a warning counting what is left", finding)
	}
	for _, name := range []string{"selected_id_seq", "unselected"} {
		if finding, reported := byID[headroom(name)]; reported {
			t.Errorf("%s was reported: %+v", name, finding)
		}
	}

	if _, err := conn.Exec(ctx, "ALTER SEQUENCE tight MAXVALUE 9223372036854775807"); err != nil {
		t.Fatal(err)
	}
	result, byID = run()
	if _, reported := byID[headroom("tight")]; reported || !result.Allowed {
		t.Errorf("after the recommended ALTER: allowed=%v findings=%+v", result.Allowed, result.Findings)
	}
	if _, reported := byID[headroom("low")]; !reported {
		t.Errorf("the acknowledgeable warning disappeared: %+v", result.Findings)
	}
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
