package preflight

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/collation"
	"github.com/GetStream/pgmigrate/internal/replident"
	"github.com/GetStream/pgmigrate/internal/tuning"
)

func TestParseToolMajor(t *testing.T) {
	tests := map[string]int{
		"pg_dump (PostgreSQL) 16.9":       16,
		"pg_restore (PostgreSQL) 17.2":    17,
		"pg_dump (PostgreSQL) 18beta1":    18,
		"pg_restore (PostgreSQL) 18devel": 18,
	}
	for output, want := range tests {
		got, err := ParseToolMajor(output)
		if err != nil {
			t.Errorf("ParseToolMajor(%q): %v", output, err)
		} else if got != want {
			t.Errorf("ParseToolMajor(%q) = %d, want %d", output, got, want)
		}
	}
	if _, err := ParseToolMajor("not a version"); err == nil {
		t.Error("ParseToolMajor accepted invalid output")
	}
}

func TestWALRetentionHeadroomEstimate(t *testing.T) {
	if finding := walRetentionFinding(
		1024, 100, time.Second, 20*time.Second,
	); finding == nil || finding.ID != "wal-retention-headroom" {
		t.Fatalf("insufficient headroom finding = %+v", finding)
	}
	if finding := walRetentionFinding(
		4096, 100, time.Second, 20*time.Second,
	); finding != nil {
		t.Fatalf("sufficient headroom finding = %+v", finding)
	}
}

func TestExtensionVersionCompatibility(t *testing.T) {
	if finding := extensionVersionFinding(
		"hstore", "1.8", "1.8", "", false, true,
	); finding != nil {
		t.Fatalf("compatible extension finding = %+v", finding)
	}
	if finding := extensionVersionFinding(
		"hstore", "1.8", "1.9", "", false, false,
	); finding == nil || finding.ID != "extension-hstore-version" {
		t.Fatalf("unavailable source version finding = %+v", finding)
	}
	if finding := extensionVersionFinding(
		"hstore", "1.8", "1.8", "1.7", true, true,
	); finding == nil || finding.ID != "extension-hstore-installed-version" {
		t.Fatalf("incompatible installed version finding = %+v", finding)
	}
}

// TestCollationFindingStopsALocaleChangeUntilItIsAllowed pins the policy: a
// locale or provider difference stops the run, and only its own flag accepts it.
func TestCollationFindingStopsALocaleChangeUntilItIsAllowed(t *testing.T) {
	difference := collation.Difference{
		Kind: collation.KindLocale, Source: "en_US.UTF-8 [libc]", Target: "de_DE.UTF8 [libc]",
	}

	finding := collationFinding(difference, nil, false)
	if finding.ID != "collation-locale" || finding.Severity != SeverityError {
		t.Fatalf("finding = %+v, want a collation-locale error", finding)
	}
	// The values an operator configured have to appear, because "incompatible
	// collations" without them says nothing about which side to change.
	for _, want := range []string{"en_US.UTF-8", "de_DE.UTF8", "--allow-collation-change"} {
		if !strings.Contains(finding.Message, want) {
			t.Errorf("message omits %q:\n%s", want, finding.Message)
		}
	}
	if strings.Contains(finding.Message, "--ack-warnings") {
		t.Errorf("message offers the wrong flag:\n%s", finding.Message)
	}

	allowed := collationFinding(difference, nil, true)
	if allowed.Severity != SeverityInfo {
		t.Errorf("finding with --allow-collation-change = %+v, want info", allowed)
	}
	if !strings.Contains(allowed.Message, "--allow-collation-change") {
		t.Errorf("allowed message does not say what allowed it:\n%s", allowed.Message)
	}

	provider := collationFinding(collation.Difference{
		Kind: collation.KindProvider, Source: "libc", Target: "icu",
	}, nil, false)
	if provider.ID != "collation-provider" || provider.Severity != SeverityError {
		t.Fatalf("finding = %+v, want a collation-provider error", provider)
	}
}

// TestCollationVersionRemainsAcknowledgeable keeps the one difference that
// managed services produce routinely from demanding the escape hatch: the same
// locale under two glibc versions, as observed between RDS and Cloud SQL.
func TestCollationVersionRemainsAcknowledgeable(t *testing.T) {
	difference := collation.Difference{
		Kind: collation.KindVersion, Source: "2.26-59.amzn2", Target: "2.19",
	}
	finding := collationFinding(difference, nil, false)
	if finding.ID != "collation-version" || finding.Severity != SeverityWarning {
		t.Fatalf("finding = %+v, want a collation-version warning", finding)
	}
	if !strings.Contains(finding.Message, "--ack-warnings") {
		t.Errorf("message does not say how to acknowledge it:\n%s", finding.Message)
	}
	if got := collationFinding(difference, nil, true); got.Severity != SeverityInfo {
		t.Errorf("--allow-collation-change did not cover the weaker case: %+v", got)
	}
}

// TestCollationFindingRefusesStructuralRisksOutright covers the case the flag
// must not unblock, where collation decides which rows are equal and which
// partition a row belongs to rather than only the order rows come back in.
func TestCollationFindingRefusesStructuralRisksOutright(t *testing.T) {
	risks := []string{"2 unique index(es) use a nondeterministic collation"}
	for _, allowChange := range []bool{false, true} {
		for _, kind := range []collation.Kind{
			collation.KindLocale, collation.KindProvider, collation.KindVersion,
		} {
			finding := collationFinding(collation.Difference{
				Kind: kind, Source: "a", Target: "b",
			}, risks, allowChange)
			if finding.Severity != SeverityError {
				t.Errorf("%s difference with a structural risk (allowChange=%v) = %q, want error",
					kind, allowChange, finding.Severity)
			}
			if !strings.Contains(finding.Message, risks[0]) {
				t.Errorf("message omits the risk:\n%s", finding.Message)
			}
			// Naming a flag that cannot help sends the operator to try it.
			if strings.Contains(finding.Message, "rerun with --allow-collation-change") {
				t.Errorf("message offers a flag that will not unblock it:\n%s", finding.Message)
			}
		}
	}
}

// TestBlocksOnCollationStopsOnlyForTheCheckItself makes sure the short circuit
// that skips the rest of preflight is not triggered by a failed catalog read or
// by a difference that was accepted.
func TestBlocksOnCollationStopsOnlyForTheCheckItself(t *testing.T) {
	if !blocksOnCollation(Finding{ID: "collation-locale", Kind: "collation", Severity: SeverityError}) {
		t.Error("a blocking collation error did not stop preflight")
	}
	for _, finding := range []Finding{
		{ID: "collation-version", Kind: "collation", Severity: SeverityWarning},
		{ID: "collation-locale", Kind: "collation", Severity: SeverityInfo},
		{ID: "collation", Kind: "probe", Severity: SeverityError},
		{ID: "wal-level", Kind: "replication", Severity: SeverityError},
	} {
		if blocksOnCollation(finding) {
			t.Errorf("%+v stopped preflight early", finding)
		}
	}
}

func TestTuningFindingsDescribeThePlanAndWhatIsBlocked(t *testing.T) {
	findings := tuningFindings(tuning.Plan{
		MemoryBytes: 64 << 30, MemoryEstimated: true,
		Changes: []tuning.Change{
			{Name: "maintenance_work_mem", From: "64MB", To: "4GB", Scope: tuning.ScopeSession},
			{Name: "max_wal_size", From: "1GB", To: "64GB", Scope: tuning.ScopeSystem},
		},
		Blocked: map[string]string{
			"checkpoint_timeout": "changing it needs ALTER SYSTEM, which this target does not permit",
		},
	})
	byID := map[string]Finding{}
	for _, finding := range findings {
		byID[finding.ID] = finding
	}

	plan, ok := byID["target-tuning"]
	if !ok {
		t.Fatal("no finding describes the plan")
	}
	if plan.Severity != SeverityInfo {
		t.Errorf("plan severity = %q, want info so it never blocks", plan.Severity)
	}
	// The message has to carry the values, the memory it was sized for, and the
	// fact that memory was a guess, because that is what an operator needs to
	// decide whether to pass --target-memory.
	for _, want := range []string{
		"maintenance_work_mem", "64MB", "4GB", "max_wal_size", "64GB",
		"estimated from shared_buffers", "restored at cutover",
	} {
		if !strings.Contains(plan.Message, want) {
			t.Errorf("plan message omits %q: %q", want, plan.Message)
		}
	}

	// A blocked setting is its own warning, so acknowledging it is a deliberate
	// choice rather than something buried in the plan.
	blocked, ok := byID["target-tuning-checkpoint-timeout"]
	if !ok {
		t.Fatalf("no finding warns about the blocked setting: %+v", findings)
	}
	if blocked.Severity != SeverityWarning {
		t.Errorf("blocked severity = %q, want warning", blocked.Severity)
	}
	for _, want := range []string{"ALTER SYSTEM", "parameter group", "--skip-target-tuning"} {
		if !strings.Contains(blocked.Message, want) {
			t.Errorf("blocked message omits %q: %q", want, blocked.Message)
		}
	}
}

func TestTuningFindingsReportAnAlreadyTunedTarget(t *testing.T) {
	findings := tuningFindings(tuning.Plan{Blocked: map[string]string{}})
	if len(findings) != 1 {
		t.Fatalf("findings = %+v, want exactly one", findings)
	}
	if findings[0].Severity != SeverityInfo {
		t.Errorf("severity = %q, want info", findings[0].Severity)
	}
	if !strings.Contains(findings[0].Message, "already configured") {
		t.Errorf("message does not say the target needs nothing: %q", findings[0].Message)
	}
}

// TestReplicaIdentityFindingsWarnWhereFixableAndFailWhereNot pins the severity
// split, which is what decides whether a run can proceed at all. A gap pgmigrate
// can close is a warning the operator acknowledges after reading the cost; a gap
// it cannot close has to fail here rather than as a permission error partway
// through setup.
func TestReplicaIdentityFindingsWarnWhereFixableAndFailWhereNot(t *testing.T) {
	findings := replicaIdentityFindings([]replident.Relation{
		// Identifiable relations must produce nothing at all: a finding per
		// selected table is how 110 relations came to be forced to FULL.
		{
			OID: 1, Schema: "app", Name: "keyed",
			Identity: replident.IdentityDefault, HasValidPrimaryKey: true, Owned: true,
		},
		{
			OID: 2, Schema: "app", Name: "designated", Identity: replident.IdentityIndex,
			IdentityIndex: "designated_key", HasValidIdentityIndex: true, Owned: true,
		},
		{
			OID: 3, Schema: "app", Name: "already_full",
			Identity: replident.IdentityFull, Owned: true,
		},

		{
			OID: 4, Schema: "app", Name: "unkeyed", Identity: replident.IdentityDefault,
			Owned: true, SizeBytes: 4096, RowWrites: 17,
		},
		{
			OID: 5, Schema: "app", Name: "part_us", Identity: replident.IdentityDefault,
			Owned: true, Partition: true, Parent: "app.part",
		},
		{
			OID: 6, Schema: "app", Name: "nothing_keyed", Identity: replident.IdentityNothing,
			HasValidPrimaryKey: true, Owned: true,
		},
		{OID: 7, Schema: "app", Name: "foreign_owned", Identity: replident.IdentityDefault},
	})

	byID := map[string]Finding{}
	for _, finding := range findings {
		byID[finding.ID] = finding
	}
	if len(findings) != 4 {
		t.Fatalf("findings = %+v, want one per unidentifiable relation only", findings)
	}

	for _, oid := range []uint32{4, 5, 6} {
		finding := byID[fmt.Sprintf("table-%d-replica-identity", oid)]
		if finding.Severity != SeverityWarning {
			t.Errorf("oid %d severity = %q, want warning so --ack-warnings can consent",
				oid, finding.Severity)
		}
		if finding.Kind != "replica-identity" {
			t.Errorf("oid %d kind = %q", oid, finding.Kind)
		}
	}

	// The operator has to be able to tell what will happen and what it costs.
	unkeyed := byID["table-4-replica-identity"].Message
	for _, want := range []string{
		"app.unkeyed", "4096 bytes", "17 recorded UPDATE/DELETE rows",
		"no valid primary key", "REPLICA IDENTITY FULL", "restore the original at cleanup",
		"all old column values to WAL",
	} {
		if !strings.Contains(unkeyed, want) {
			t.Errorf("message omits %q: %q", want, unkeyed)
		}
	}
	// A leaf partition is nowhere in the table selection, so the message says
	// which selected table it came from.
	if partition := byID["table-5-replica-identity"].Message; !strings.Contains(
		partition, "a partition of app.part",
	) {
		t.Errorf("partition message does not name its parent: %q", partition)
	}
	// NOTHING with a primary key is the case most likely to be misread as a
	// missing key, so the reason has to say the setting overrides the key.
	if nothing := byID["table-6-replica-identity"].Message; !strings.Contains(
		nothing, "REPLICA IDENTITY NOTHING",
	) {
		t.Errorf("message does not explain NOTHING: %q", nothing)
	}

	unowned := byID["table-7-replica-identity"]
	if unowned.Severity != SeverityError {
		t.Fatalf("unowned severity = %q, want error: acknowledging cannot make the ALTER work",
			unowned.Severity)
	}
	for _, want := range []string{"does not own it", "grant ownership", "deselect the table"} {
		if !strings.Contains(unowned.Message, want) {
			t.Errorf("unowned message omits %q: %q", want, unowned.Message)
		}
	}
}

// TestReplicaIdentityFindingsNoLongerOfferTheRemovedFlag guards the remediation
// text. The flag is gone, so naming it would send an operator to a command cobra
// now rejects.
func TestReplicaIdentityFindingsNoLongerOfferTheRemovedFlag(t *testing.T) {
	findings := replicaIdentityFindings([]replident.Relation{
		{
			OID: 1, Schema: "app", Name: "unkeyed",
			Identity: replident.IdentityDefault, Owned: true,
		},
		{OID: 2, Schema: "app", Name: "foreign", Identity: replident.IdentityDefault},
	})
	if len(findings) != 2 {
		t.Fatalf("findings = %+v", findings)
	}
	for _, finding := range findings {
		if strings.Contains(finding.Message, "force-replica-identity-full") {
			t.Errorf("%s still points at the removed flag: %q", finding.ID, finding.Message)
		}
	}
}

func TestGateAggregatesSeverityAndAcknowledgements(t *testing.T) {
	findings := []Finding{
		{ID: "info", Severity: SeverityInfo},
		{ID: "warning-b", Severity: SeverityWarning},
		{ID: "warning-a", Severity: SeverityWarning},
	}
	err := Gate(findings, []string{"warning-a"}, false)
	if err == nil || !strings.Contains(err.Error(), "warning-b") || strings.Contains(err.Error(), "warning-a") {
		t.Fatalf("Gate() error = %v", err)
	}
	if err := Gate(findings, nil, true); err != nil {
		t.Fatalf("Gate() with warning acknowledgement = %v", err)
	}

	findings = append(findings, Finding{ID: "hard-error", Severity: SeverityError})
	if err := Gate(findings, []string{"hard-error"}, true); err == nil ||
		!strings.Contains(err.Error(), "hard-error") {
		t.Fatalf("Gate() allowed acknowledged error: %v", err)
	}
}
