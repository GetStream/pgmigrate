//go:build integration

package preflight

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/collation"
	"github.com/tgross/pgmigrate/internal/pgtest"
)

// TestCollationRiskDetection validates the ordering- and equality-sensitive
// schema probes against every supported major. These decide whether a collation
// difference is an acknowledgeable ordering change or a structural hazard.
func TestCollationRiskDetection(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			instance := pgtest.Start(t, major)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			conn := instance.Connect(t)

			risks, err := collationRisks(ctx, conn)
			if err != nil {
				t.Fatalf("probe clean schema: %v", err)
			}
			if len(risks) != 0 {
				t.Fatalf("clean schema reported risks %q", risks)
			}

			if _, err := conn.Exec(
				ctx,
				"CREATE COLLATION nondeterministic (provider=icu, locale='und-u-ks-level2', deterministic=false)",
			); err != nil {
				t.Skipf("server lacks nondeterministic ICU collations: %v", err)
			}
			if _, err := conn.Exec(
				ctx,
				"CREATE TABLE case_insensitive (value text COLLATE nondeterministic PRIMARY KEY)",
			); err != nil {
				t.Fatal(err)
			}
			risks, err = collationRisks(ctx, conn)
			if err != nil {
				t.Fatalf("probe nondeterministic unique index: %v", err)
			}
			if len(risks) != 1 || !strings.Contains(risks[0], "nondeterministic collation") {
				t.Fatalf("nondeterministic unique index risks = %q", risks)
			}

			if _, err := conn.Exec(
				ctx,
				"CREATE TABLE ranged (name text) PARTITION BY RANGE (name)",
			); err != nil {
				t.Fatal(err)
			}
			risks, err = collationRisks(ctx, conn)
			if err != nil {
				t.Fatalf("probe text range partition: %v", err)
			}
			if len(risks) != 2 || !strings.Contains(risks[1], "range-partitioned") {
				t.Fatalf("combined risks = %q", risks)
			}
		})
	}
}

// TestCollationReadAndCompareAgainstRealDatabases checks the decision against
// locales a server produced rather than strings a test wrote, on every supported
// major. The reason to insist on that: Read goes through to_jsonb because the
// column holding the provider locale was renamed in PostgreSQL 17, and only a
// real catalog can prove that reading it works on both sides of the rename.
//
// Which locale names a server accepts is a property of its C library, and the
// test images are musl-based, so each case creates its database and skips when the
// server will not have it. The en_US.utf8 against en_US.UTF-8 case is the one that
// motivated the whole feature, and it is available everywhere.
func TestCollationReadAndCompareAgainstRealDatabases(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			instance := pgtest.Start(t, major)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			conn := instance.Connect(t)

			source, err := collation.Read(ctx, conn)
			if err != nil {
				t.Fatalf("read the connected database: %v", err)
			}
			if source.Collate == "" || source.Provider == "" {
				t.Fatalf("read reported an empty collation identity: %+v", source)
			}

			tests := []struct {
				name           string
				collate, ctype string
				compatible     bool
			}{
				// A spelling of the locale this server already has. glibc and musl both
				// resolve it to the same locale, which is exactly the RDS against
				// Cloud SQL case, and refusing it would be the false alarm that teaches
				// operators to pass --allow-collation-change without reading.
				{
					name: "codeset spelling", collate: alternateSpelling(source.Collate),
					ctype: alternateSpelling(source.CType), compatible: true,
				},
				{name: "different locale", collate: "de_DE.UTF-8", ctype: "de_DE.UTF-8"},
				// LC_CTYPE alone: character classification differs while ordering does
				// not, and upper(), lower() and regex classes follow it.
				{name: "ctype only", collate: source.Collate, ctype: "C"},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					uri := databaseWithLocale(t, ctx, conn, instance.URI,
						"locale_"+strings.ReplaceAll(test.name, " ", "_"), test.collate, test.ctype)
					other, err := pgx.Connect(ctx, uri)
					if err != nil {
						t.Fatalf("connect to the created database: %v", err)
					}
					defer other.Close(context.Background())
					target, err := collation.Read(ctx, other)
					if err != nil {
						t.Fatalf("read the created database: %v", err)
					}

					differences := collation.Compare(source, target)
					if test.compatible {
						if len(differences) != 0 {
							t.Fatalf("%+v and %+v were called incompatible: %+v",
								source, target, differences)
						}
						return
					}
					if len(differences) == 0 {
						t.Fatalf("%+v and %+v were called compatible", source, target)
					}
					findings, err := checkCollation(ctx, conn, other, false)
					if err != nil {
						t.Fatalf("check: %v", err)
					}
					if len(findings) == 0 || findings[0].Severity != SeverityError {
						t.Fatalf("findings = %+v, want a blocking error", findings)
					}
					if allowed, err := checkCollation(ctx, conn, other, true); err != nil {
						t.Fatal(err)
					} else if len(allowed) == 0 || allowed[0].Severity != SeverityInfo {
						t.Fatalf("findings with --allow-collation-change = %+v", allowed)
					}
				})
			}
		})
	}
}

// TestCollationStopsPreflightBeforeTheWALSample proves the reordering is worth
// something: the check that sleeps for the WAL sample must not run once collation
// has already decided the migration cannot start.
func TestCollationStopsPreflightBeforeTheWALSample(t *testing.T) {
	instance := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn := instance.Connect(t)
	uri := databaseWithLocale(t, ctx, conn, instance.URI, "wrong_locale", "de_DE.UTF-8", "de_DE.UTF-8")

	const sample = 30 * time.Second
	started := time.Now()
	result, err := Run(ctx, Config{
		SourceDSN: instance.URI, TargetDSN: uri,
		WALSampleDuration: sample, AcknowledgeWarnings: true,
	})
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= sample {
		t.Errorf("preflight took %s, so it sampled the WAL rate before reporting collation", elapsed)
	}
	if result.Allowed || !result.Incomplete {
		t.Fatalf("result allowed=%v incomplete=%v", result.Allowed, result.Incomplete)
	}
	for _, finding := range result.Findings {
		if finding.Kind != "collation" {
			t.Errorf("a later check still ran: %+v", finding)
		}
	}

	// With the change allowed, the collation finding no longer blocks and every
	// remaining check runs, including the sample that was skipped above.
	result, err = Run(ctx, Config{
		SourceDSN: instance.URI, TargetDSN: uri,
		WALSampleDuration: 10 * time.Millisecond, AcknowledgeWarnings: true,
		AllowCollationChange: true,
	})
	if err != nil {
		t.Fatalf("allowed preflight: %v", err)
	}
	if result.Incomplete {
		t.Error("preflight stopped early despite --allow-collation-change")
	}
	if !hasFinding(result.Findings, "wal-retention-unbounded") {
		t.Errorf("the checks after collation did not run: %+v", result.Findings)
	}
}

func hasFinding(findings []Finding, id string) bool {
	for _, finding := range findings {
		if finding.ID == id {
			return true
		}
	}
	return false
}

// alternateSpelling renames a locale's codeset without renaming the locale, so a
// test can ask for the locale a server already has under a name it does not
// currently use.
func alternateSpelling(locale string) string {
	if strings.Contains(locale, ".utf8") {
		return strings.Replace(locale, ".utf8", ".UTF-8", 1)
	}
	if strings.Contains(locale, ".UTF-8") {
		return strings.Replace(locale, ".UTF-8", ".utf8", 1)
	}
	return locale
}

// databaseWithLocale creates a database with the requested locale and returns a
// DSN for it, skipping the test when the server's C library does not have that
// locale.
func databaseWithLocale(
	t *testing.T,
	ctx context.Context,
	conn *pgx.Conn,
	uri, name, collate, ctype string,
) string {
	t.Helper()
	// template0 is required: a locale may only differ from the template's.
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"CREATE DATABASE %s TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE %s LC_CTYPE %s",
		pgx.Identifier{name}.Sanitize(), quoteLiteral(collate), quoteLiteral(ctype),
	)); err != nil {
		t.Skipf("server cannot host LC_COLLATE %q LC_CTYPE %q: %v", collate, ctype, err)
	}
	parsed, err := pgx.ParseConfig(uri)
	if err != nil {
		t.Fatal(err)
	}
	return "postgres://" + parsed.User + ":" + parsed.Password + "@" + parsed.Host + ":" +
		strconv.Itoa(int(parsed.Port)) + "/" + name + "?sslmode=disable"
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
