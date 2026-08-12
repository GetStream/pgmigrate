// Package preflight validates that a PostgreSQL migration can be started safely.
package preflight

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/GetStream/pgmigrate/internal/collation"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/replident"
	"github.com/GetStream/pgmigrate/internal/tuning"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Severity controls whether a finding blocks setup.
type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

// Table identifies a selected source table.
type Table struct {
	OID    uint32
	Schema string
	Name   string
}

// Finding is one stable, acknowledgeable preflight observation.
type Finding struct {
	ID       string
	Kind     string
	Severity Severity
	Message  string
}

// Result is the complete preflight decision.
type Result struct {
	SourceMajor int
	TargetMajor int
	Findings    []Finding
	Allowed     bool
	// Incomplete reports that preflight stopped before running every check, which
	// it does only when a finding cannot be resolved by anything the remaining
	// checks could report. Findings are then a prefix of the full set rather than
	// the whole picture, and saying so matters because an absent finding otherwise
	// reads as a passing check.
	Incomplete bool
}

// Config contains inputs that affect preflight.
type Config struct {
	SourceDSN           string
	TargetDSN           string
	Tables              []Table
	RequiredExtensions  []string
	Acknowledged        []string
	AcknowledgeWarnings bool
	// AllowCollationChange accepts a source and target that collate text
	// differently. It is deliberately not covered by AcknowledgeWarnings, because
	// a changed collation changes how the application's own queries order and
	// compare strings after cutover.
	AllowCollationChange bool
	PGDumpPath           string
	PGRestorePath        string
	// WALSampleDuration controls the WAL-rate sample. Zero uses one minute.
	WALSampleDuration time.Duration
	// WALRetentionDuration is the period of generated WAL that must fit within
	// max_slot_wal_keep_size. Zero uses one hour.
	WALRetentionDuration time.Duration
	// SkipTargetTuning reports that the run will leave target settings alone, in
	// which case there is nothing to report beyond saying so.
	SkipTargetTuning bool
	// TuningOverrides and TuningWorkers reproduce what the run will derive, so
	// preflight reports the plan the run would actually apply.
	TuningOverrides tuning.Overrides
	TuningWorkers   int
}

// Run connects to both databases and runs every mandatory preflight check.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if strings.TrimSpace(cfg.SourceDSN) == "" || strings.TrimSpace(cfg.TargetDSN) == "" {
		return Result{}, errors.New("source and target DSNs are required")
	}
	source, err := pgx.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return Result{}, fmt.Errorf("connect source: %w", err)
	}
	defer source.Close(context.Background())
	target, err := pgx.Connect(ctx, cfg.TargetDSN)
	if err != nil {
		return Result{}, fmt.Errorf("connect target: %w", err)
	}
	defer target.Close(context.Background())
	return RunConnections(ctx, source, target, cfg)
}

// RunConnections runs preflight against already-open connections.
func RunConnections(ctx context.Context, source, target *pgx.Conn, cfg Config) (Result, error) {
	var result Result
	sourceMajor, _, err := serverVersion(ctx, source)
	if err != nil {
		return result, fmt.Errorf("read source version: %w", err)
	}
	targetMajor, _, err := serverVersion(ctx, target)
	if err != nil {
		return result, fmt.Errorf("read target version: %w", err)
	}
	result.SourceMajor, result.TargetMajor = sourceMajor, targetMajor
	add := func(f Finding) { result.Findings = append(result.Findings, f) }

	if _, err := postgres.CapabilitiesForMajor(sourceMajor); err != nil {
		add(errorFinding("source-version", "version", err.Error()))
	}
	if _, err := postgres.CapabilitiesForMajor(targetMajor); err != nil {
		add(errorFinding("target-version", "version", err.Error()))
	}
	if targetMajor < sourceMajor {
		add(errorFinding("version-order", "version",
			fmt.Sprintf("target PostgreSQL %d is older than source PostgreSQL %d", targetMajor, sourceMajor)))
	}

	// Collation comes first, and stops preflight when it blocks. It is cheap, it is
	// the one finding an operator answers by rebuilding the target rather than by
	// changing a setting, and the check immediately after it samples the WAL rate
	// for a minute. Reporting a wrong target collation a minute late, every time
	// they retry, is a minute spent proving something already known.
	runCheck(add, "collation", func() ([]Finding, error) {
		return checkCollation(ctx, source, target, cfg.AllowCollationChange)
	})
	if slices.ContainsFunc(result.Findings, blocksOnCollation) {
		result.Incomplete = true
		return decide(result, cfg), nil
	}

	runCheck(add, "source-replication", func() ([]Finding, error) {
		return checkSourceReplication(ctx, source, cfg.WALSampleDuration, cfg.WALRetentionDuration)
	})
	runCheck(add, "replica-identity", func() ([]Finding, error) {
		return checkReplicaIdentity(ctx, source, cfg.Tables)
	})
	runCheck(add, "target-empty", func() ([]Finding, error) {
		return checkTargetEmpty(ctx, target)
	})
	runCheck(add, "source-objects", func() ([]Finding, error) {
		return checkSourceObjects(ctx, source)
	})
	runCheck(add, "extensions", func() ([]Finding, error) {
		return checkExtensions(ctx, source, target, cfg.RequiredExtensions)
	})
	runCheck(add, "source-privileges", func() ([]Finding, error) {
		return checkSourcePrivileges(ctx, source, cfg.Tables)
	})
	runCheck(add, "source-replication-connection", func() ([]Finding, error) {
		return checkReplicationConnection(ctx, cfg.SourceDSN), nil
	})
	runCheck(add, "target-privileges", func() ([]Finding, error) {
		return checkTargetPrivileges(ctx, target)
	})
	runCheck(add, "target-tuning", func() ([]Finding, error) {
		return checkTargetTuning(ctx, target, cfg)
	})

	dumpPath := cfg.PGDumpPath
	if dumpPath == "" {
		dumpPath = "pg_dump"
	}
	restorePath := cfg.PGRestorePath
	if restorePath == "" {
		restorePath = "pg_restore"
	}
	for _, tool := range []struct{ id, path string }{{"pg-dump-version", dumpPath}, {"pg-restore-version", restorePath}} {
		major, err := ToolMajor(ctx, tool.path)
		if err != nil {
			add(errorFinding(tool.id, "tool", err.Error()))
		} else if major < sourceMajor {
			add(errorFinding(tool.id, "tool",
				fmt.Sprintf("%s major %d is older than source PostgreSQL %d", tool.path, major, sourceMajor)))
		}
	}

	return decide(result, cfg), nil
}

func decide(result Result, cfg Config) Result {
	slices.SortFunc(result.Findings, func(a, b Finding) int { return strings.Compare(a.ID, b.ID) })
	result.Allowed = Gate(result.Findings, cfg.Acknowledged, cfg.AcknowledgeWarnings) == nil
	return result
}

// blocksOnCollation reports a collation difference that no later check can
// inform and that neither acknowledgement nor --allow-collation-change accepted.
// A probe failure is not one: it carries kind "probe", and hiding the rest of
// preflight because one catalog read failed would help nobody.
func blocksOnCollation(finding Finding) bool {
	return finding.Kind == "collation" && finding.Severity == SeverityError
}

func runCheck(add func(Finding), id string, check func() ([]Finding, error)) {
	findings, err := check()
	if err != nil {
		add(errorFinding(id, "probe", err.Error()))
		return
	}
	for _, finding := range findings {
		add(finding)
	}
}

// Gate rejects errors and unacknowledged warnings.
func Gate(findings []Finding, acknowledged []string, acknowledgeWarnings bool) error {
	acks := make(map[string]bool, len(acknowledged))
	for _, id := range acknowledged {
		acks[id] = true
	}
	var blocked []string
	for _, finding := range findings {
		if finding.Severity == SeverityError ||
			(finding.Severity == SeverityWarning && !acknowledgeWarnings && !acks[finding.ID]) {
			blocked = append(blocked, finding.ID)
		}
	}
	if len(blocked) != 0 {
		slices.Sort(blocked)
		return fmt.Errorf("preflight blocked by: %s", strings.Join(blocked, ", "))
	}
	return nil
}

func serverVersion(ctx context.Context, conn *pgx.Conn) (int, int, error) {
	var number int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&number); err != nil {
		return 0, 0, err
	}
	return number / 10000, number, nil
}

func checkSourceReplication(
	ctx context.Context,
	conn *pgx.Conn,
	sampleDuration, retentionDuration time.Duration,
) ([]Finding, error) {
	var walLevel string
	var freeSlots, freeSenders int
	err := conn.QueryRow(ctx, `
		SELECT current_setting('wal_level'),
		       current_setting('max_replication_slots')::int -
		         (SELECT count(*) FROM pg_catalog.pg_replication_slots),
		       current_setting('max_wal_senders')::int -
		         (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE backend_type='walsender')
	`).Scan(&walLevel, &freeSlots, &freeSenders)
	if err != nil {
		return nil, err
	}
	var findings []Finding
	if walLevel != "logical" {
		findings = append(findings, errorFinding("wal-level", "replication", "source wal_level must be logical"))
	}
	if freeSlots < 1 {
		findings = append(findings, errorFinding("replication-slot-capacity", "replication", "source has no free replication slot"))
	}
	if freeSenders < 1 {
		findings = append(findings, errorFinding("wal-sender-capacity", "replication", "source has no free WAL sender"))
	}
	retentionFinding, err := checkWALRetention(ctx, conn, sampleDuration, retentionDuration)
	if err != nil {
		return nil, err
	}
	if retentionFinding != nil {
		findings = append(findings, *retentionFinding)
	}
	return findings, nil
}

func checkWALRetention(
	ctx context.Context,
	conn *pgx.Conn,
	sampleDuration, retentionDuration time.Duration,
) (*Finding, error) {
	if sampleDuration == 0 {
		sampleDuration = time.Minute
	}
	if sampleDuration < 0 {
		return nil, errors.New("WAL sample duration cannot be negative")
	}
	if retentionDuration == 0 {
		retentionDuration = time.Hour
	}
	if retentionDuration < 0 {
		return nil, errors.New("WAL retention duration cannot be negative")
	}

	var setting string
	var start string
	if err := conn.QueryRow(ctx, `
		SELECT current_setting('max_slot_wal_keep_size'), pg_catalog.pg_current_wal_lsn()::text
	`).Scan(&setting, &start); err != nil {
		return nil, err
	}
	timer := time.NewTimer(sampleDuration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	var generated float64
	if err := conn.QueryRow(
		ctx,
		"SELECT pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), $1::pg_lsn)",
		start,
	).Scan(&generated); err != nil {
		return nil, err
	}
	if setting == "-1" {
		return &Finding{
			ID:       "wal-retention-unbounded",
			Kind:     "replication",
			Severity: SeverityWarning,
			Message: "max_slot_wal_keep_size is unbounded, so PostgreSQL will never invalidate the migration " +
				"slot and a stalled migration can retain WAL until the source WAL volume is full; run monitors " +
				"WAL directory growth against max_wal_size and records follow-wal-headroom if the slot dominates it",
		}, nil
	}
	if generated <= 0 || sampleDuration <= 0 {
		return &Finding{
			ID:       "wal-retention-estimate",
			Kind:     "replication",
			Severity: SeverityWarning,
			Message:  "WAL generation was not observed during sampling; retention headroom cannot be estimated",
		}, nil
	}
	var limitBytes int64
	if err := conn.QueryRow(ctx, "SELECT pg_catalog.pg_size_bytes($1)", setting).Scan(&limitBytes); err != nil {
		return &Finding{
			ID:       "wal-retention-estimate",
			Kind:     "replication",
			Severity: SeverityWarning,
			Message:  fmt.Sprintf("max_slot_wal_keep_size %q cannot be converted to bytes; retention headroom is unknown", setting),
		}, nil
	}
	return walRetentionFinding(limitBytes, generated, sampleDuration, retentionDuration), nil
}

func walRetentionFinding(
	limitBytes int64,
	generated float64,
	sampleDuration, retentionDuration time.Duration,
) *Finding {
	required := int64(generated / sampleDuration.Seconds() * retentionDuration.Seconds())
	if required > limitBytes {
		return &Finding{
			ID:       "wal-retention-headroom",
			Kind:     "replication",
			Severity: SeverityWarning,
			Message: fmt.Sprintf(
				"estimated WAL retention need is %d bytes over %s, exceeding max_slot_wal_keep_size (%d bytes)",
				required, retentionDuration, limitBytes,
			),
		}
	}
	return nil
}

func checkReplicaIdentity(ctx context.Context, conn *pgx.Conn, tables []Table) ([]Finding, error) {
	var findings []Finding
	for _, table := range tables {
		var persistence string
		err := conn.QueryRow(ctx,
			"SELECT c.relpersistence::text FROM pg_catalog.pg_class c WHERE c.oid=$1",
			table.OID).Scan(&persistence)
		if errors.Is(err, pgx.ErrNoRows) {
			findings = append(findings, errorFinding(fmt.Sprintf("table-%d-missing", table.OID), "replica-identity",
				fmt.Sprintf("selected table %s.%s does not exist", table.Schema, table.Name)))
			continue
		}
		if err != nil {
			return nil, err
		}
		if persistence == "u" {
			findings = append(findings, errorFinding(fmt.Sprintf("table-%d-unlogged", table.OID), "replication",
				fmt.Sprintf("selected table %s.%s is unlogged and cannot be added to logical replication", table.Schema, table.Name)))
		}
	}
	relations, err := replident.Inspect(ctx, conn, replidentTables(tables))
	if err != nil {
		return nil, err
	}
	return append(findings, replicaIdentityFindings(relations)...), nil
}

func replidentTables(tables []Table) []replident.Table {
	converted := make([]replident.Table, 0, len(tables))
	for _, table := range tables {
		converted = append(converted, replident.Table{
			OID: table.OID, Schema: table.Schema, Name: table.Name,
		})
	}
	return converted
}

// replicaIdentityFindings reports every relation logical replication cannot
// identify rows in.
//
// The severity split is the whole point of checking here. A relation pgmigrate
// owns is a warning: the run will set REPLICA IDENTITY FULL on it and restore the
// original at cleanup, and the operator consents through --ack-warnings after
// reading what it will cost. A relation owned by another role is an error, because
// no amount of acknowledging makes the ALTER succeed, and finding out now beats
// finding out from a bare permission failure partway through setup.
func replicaIdentityFindings(relations []replident.Relation) []Finding {
	var findings []Finding
	for _, relation := range replident.NeedFallback(relations) {
		id := fmt.Sprintf("table-%d-replica-identity", relation.OID)
		if !relation.Owned {
			findings = append(findings, errorFinding(id, "replica-identity", fmt.Sprintf(
				"%s cannot identify UPDATE/DELETE rows: it %s, and the migration role does not own "+
					"it, so pgmigrate cannot set REPLICA IDENTITY FULL. Add a primary key, designate a "+
					"unique non-partial index over NOT NULL columns with REPLICA IDENTITY USING INDEX, "+
					"grant ownership to the migration role, or deselect the table",
				relation.Describe(), relation.Reason(),
			)))
			continue
		}
		findings = append(findings, Finding{
			ID: id, Kind: "replica-identity", Severity: SeverityWarning,
			Message: fmt.Sprintf(
				"%s cannot identify UPDATE/DELETE rows: it %s, so pgmigrate will set REPLICA IDENTITY "+
					"FULL on it for the duration of the migration and restore the original at cleanup. "+
					"Until then every source UPDATE/DELETE on it writes all old column values to WAL and "+
					"apply matches target rows on all columns. Adding a primary key, or designating a "+
					"unique non-partial index over NOT NULL columns with REPLICA IDENTITY USING INDEX, "+
					"avoids that",
				relation.Describe(), relation.Reason(),
			),
		})
	}
	return findings
}

func checkTargetEmpty(ctx context.Context, conn *pgx.Conn) ([]Finding, error) {
	var count int
	err := conn.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind IN ('r','p','v','m','S','f')
		  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
		  AND n.nspname <> 'information_schema'
		  AND n.nspname <> 'pgmigrate_internal'
	`).Scan(&count)
	if err != nil {
		return nil, err
	}
	if count != 0 {
		return []Finding{errorFinding("target-not-empty", "target", fmt.Sprintf("target contains %d user relation(s)", count))}, nil
	}
	return nil, nil
}

// orderingOnlyRisk describes the residual exposure when collations differ but
// no schema object depends on text ordering or equality for its structure.
//
// Verification is genuinely unaffected: it reads both sides in physical page order
// and groups rows by a hash of their key, so it never orders or compares text and
// cannot care that the two servers sort it differently.
const orderingOnlyRisk = "pgmigrate rebuilds every target index using the target collation, " +
	"so every index remains valid, and verification never orders text, so it is unaffected. " +
	"What changes for the application is text ordering for ORDER BY and range queries " +
	"after cutover"

// collationRisks reports source schema structures whose correctness depends on
// text ordering or text equality, and which therefore turn a collation
// difference from an ordering change into a structural hazard.
func collationRisks(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	probes := []struct {
		query, message string
	}{
		{
			`
			SELECT count(*) FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class c ON c.oid=i.indrelid
			JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
			CROSS JOIN LATERAL unnest(i.indcollation) AS ic(coll)
			JOIN pg_catalog.pg_collation col ON col.oid=ic.coll
			WHERE (i.indisunique OR i.indisprimary) AND NOT col.collisdeterministic
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\' AND n.nspname <> 'information_schema'`,
			"%d unique index(es) use a nondeterministic collation, so changing collation can " +
				"change which text values compare equal and make a rebuilt unique index reject or merge rows",
		},
		{
			`
			SELECT count(*) FROM pg_catalog.pg_partitioned_table p
			JOIN pg_catalog.pg_class c ON c.oid=p.partrelid
			JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
			CROSS JOIN LATERAL unnest(p.partcollation) AS pc(coll)
			WHERE p.partstrat='r' AND pc.coll <> 0
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\' AND n.nspname <> 'information_schema'`,
			"%d range-partitioned table(s) partition on a collatable column, so different text " +
				"ordering can route rows to a different partition or to none",
		},
	}
	var risks []string
	for _, probe := range probes {
		var count int
		if err := conn.QueryRow(ctx, probe.query).Scan(&count); err != nil {
			return nil, err
		}
		if count > 0 {
			risks = append(risks, fmt.Sprintf(probe.message, count))
		}
	}
	return risks, nil
}

func checkCollation(ctx context.Context, source, target *pgx.Conn, allowChange bool) ([]Finding, error) {
	sourceSettings, err := collation.Read(ctx, source)
	if err != nil {
		return nil, err
	}
	targetSettings, err := collation.Read(ctx, target)
	if err != nil {
		return nil, err
	}
	differences := collation.Compare(sourceSettings, targetSettings)
	if len(differences) == 0 {
		return nil, nil
	}
	risks, err := collationRisks(ctx, source)
	if err != nil {
		return nil, err
	}
	findings := make([]Finding, 0, len(differences))
	for _, difference := range differences {
		findings = append(findings, collationFinding(difference, risks, allowChange))
	}
	return findings, nil
}

// collationFinding renders one difference and decides whether it stops the
// migration.
//
// A locale or provider difference is an error, because the target will order and
// classify text differently from the database the application was written
// against, and that is a decision for an operator rather than something to note
// in passing. --allow-collation-change is how they make it, and it is a separate
// flag from --ack-warnings so that acknowledging a crowded preflight cannot
// silently accept a changed collation as well.
//
// Two things do not follow that rule. A provider-version difference stays a
// warning: the locale is the same, and real migrations between managed services
// routinely cross glibc versions, so requiring the flag every time would make it
// meaningless. A structural risk, where the source schema depends on text
// ordering or text equality for its shape rather than for its output, stays an
// error the flag cannot waive, and its message says so instead of pointing at a
// flag that will not help.
func collationFinding(difference collation.Difference, risks []string, allowChange bool) Finding {
	severity, closing := collationDisposition(difference.Kind, risks, allowChange)
	message := fmt.Sprintf("%s\n\n  source:      %s\n  target:      %s\n\n%s\n\n%s",
		collationHeadline(difference.Kind), difference.Source, difference.Target,
		collationConsequence(difference.Kind), closing)
	return Finding{
		ID:       "collation-" + string(difference.Kind),
		Kind:     "collation",
		Severity: severity,
		Message:  message,
	}
}

func collationHeadline(kind collation.Kind) string {
	switch kind {
	case collation.KindProvider:
		return "source and target use different collation providers"
	case collation.KindVersion:
		return "source and target report different versions of the same collation"
	default:
		return "source and target use incompatible collations"
	}
}

func collationConsequence(kind collation.Kind) string {
	switch kind {
	case collation.KindProvider:
		return "A different collation library orders and classifies text differently " +
			"even under the same locale name."
	case collation.KindVersion:
		return "The locale is the same, but the provider's definition of it is not, " +
			"and definitions reorder strings between versions."
	default:
		return "These collations are neither identical nor known aliases of the same " +
			"collation. Changing collation can change string ordering and comparison behavior."
	}
}

func collationDisposition(kind collation.Kind, risks []string, allowChange bool) (Severity, string) {
	if len(risks) != 0 {
		return SeverityError, "This cannot be allowed through, because the source schema " +
			"depends on text ordering or text equality for its structure rather than only for " +
			"its output: " + strings.Join(risks, "; ") + "."
	}
	if allowChange {
		return SeverityInfo, "Allowed by --allow-collation-change: " + orderingOnlyRisk + "."
	}
	if kind == collation.KindVersion {
		return SeverityWarning, orderingOnlyRisk +
			". Acknowledge it with --ack-warnings once that is understood."
	}
	return SeverityError, orderingOnlyRisk +
		". If you have verified that these collations are compatible for your data, " +
		"rerun with --allow-collation-change."
}

func checkSourceObjects(ctx context.Context, conn *pgx.Conn) ([]Finding, error) {
	queries := []struct {
		id, message string
		severity    Severity
		query       string
	}{
		{"large-objects", "source contains large objects; pgmigrate cannot migrate them", SeverityError, "SELECT count(*) FROM pg_catalog.pg_largeobject_metadata"},
		{"foreign-tables", "source contains foreign tables; their data is not copied normally", SeverityWarning, "SELECT count(*) FROM pg_catalog.pg_class WHERE relkind='f'"},
		{"unlogged-tables", "source contains unlogged tables; live writes to them cannot migrate because logical replication does not capture them", SeverityWarning, "SELECT count(*) FROM pg_catalog.pg_class WHERE relpersistence='u' AND relkind IN ('r','p')"},
		{"materialized-views", "source contains materialized views; refresh them after migration", SeverityWarning, "SELECT count(*) FROM pg_catalog.pg_class WHERE relkind='m'"},
		{"generated-columns", "source contains generated columns; verify generated expressions and copy behavior", SeverityWarning, `
			SELECT count(*) FROM pg_catalog.pg_attribute a
			JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
			JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
			WHERE a.attgenerated <> '' AND NOT a.attisdropped
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\' AND n.nspname <> 'information_schema'`},
		{"exclusion-constraints", "source contains exclusion constraints; recreate and validate them after copy", SeverityWarning, `
			SELECT count(*) FROM pg_catalog.pg_constraint c
			JOIN pg_catalog.pg_namespace n ON n.oid=c.connamespace
			WHERE c.contype='x'
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\' AND n.nspname <> 'information_schema'`},
		{"deferrable-unique-constraints", "source contains deferrable unique constraints; preserve deferred enforcement semantics", SeverityWarning, `
			SELECT count(*) FROM pg_catalog.pg_constraint c
			JOIN pg_catalog.pg_namespace n ON n.oid=c.connamespace
			WHERE c.contype='u' AND c.condeferrable
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\' AND n.nspname <> 'information_schema'`},
	}
	var findings []Finding
	for _, item := range queries {
		var count int
		if err := conn.QueryRow(ctx, item.query).Scan(&count); err != nil {
			return nil, err
		}
		if count > 0 {
			findings = append(findings, Finding{
				ID: item.id, Kind: "object", Severity: item.severity,
				Message: fmt.Sprintf("%s (%d found)", item.message, count),
			})
		}
	}
	return findings, nil
}

func checkExtensions(ctx context.Context, source, target *pgx.Conn, required []string) ([]Finding, error) {
	sourceVersions := make(map[string]string)
	rows, err := source.Query(ctx, "SELECT extname, extversion FROM pg_catalog.pg_extension WHERE extname <> 'plpgsql'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name, version string
		if err := rows.Scan(&name, &version); err != nil {
			return nil, err
		}
		sourceVersions[name] = version
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	names := append([]string(nil), required...)
	for name := range sourceVersions {
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	var findings []Finding
	for _, name := range names {
		var defaultVersion string
		var installedVersion string
		var installed bool
		err := target.QueryRow(ctx, `
			SELECT a.default_version, COALESCE(e.extversion,''), e.extversion IS NOT NULL
			FROM pg_catalog.pg_available_extensions a
			LEFT JOIN pg_catalog.pg_extension e ON e.extname=a.name
			WHERE a.name=$1
		`, name).Scan(&defaultVersion, &installedVersion, &installed)
		if errors.Is(err, pgx.ErrNoRows) {
			findings = append(findings, errorFinding("extension-"+name, "extension",
				fmt.Sprintf("extension %q is not available on target", name)))
			continue
		}
		if err != nil {
			return nil, err
		}
		sourceVersion, installedOnSource := sourceVersions[name]
		if !installedOnSource {
			continue
		}
		var exactAvailable bool
		if err := target.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT FROM pg_catalog.pg_available_extension_versions
				WHERE name=$1 AND version=$2
			)
		`, name, sourceVersion).Scan(&exactAvailable); err != nil {
			return nil, err
		}
		if finding := extensionVersionFinding(
			name, sourceVersion, defaultVersion, installedVersion, installed, exactAvailable,
		); finding != nil {
			findings = append(findings, *finding)
		}
	}
	return findings, nil
}

func extensionVersionFinding(
	name, sourceVersion, defaultVersion, installedVersion string,
	installed, exactAvailable bool,
) *Finding {
	if installed && installedVersion != sourceVersion {
		finding := errorFinding("extension-"+name+"-installed-version", "extension",
			fmt.Sprintf("extension %q is version %s on source but target has incompatible installed version %s",
				name, sourceVersion, installedVersion))
		return &finding
	}
	if !exactAvailable {
		finding := errorFinding("extension-"+name+"-version", "extension",
			fmt.Sprintf("extension %q source version %s is unavailable on target (target default is %s)",
				name, sourceVersion, defaultVersion))
		return &finding
	}
	return nil
}

// replicationRemediation lists the grant that supplies replication rights on
// each supported deployment. Managed services do not set pg_roles.rolreplication,
// so the attribute cannot be inspected to predict this capability.
const replicationRemediation = "grant replication rights: self-managed PostgreSQL " +
	`"ALTER ROLE <role> REPLICATION"; Amazon RDS or Aurora "GRANT rds_replication TO <role>"; ` +
	`Cloud SQL "ALTER USER <role> WITH REPLICATION"`

// checkReplicationConnection proves the source role can start a WAL sender by
// opening the same replication connection the migration itself uses. Role
// attributes cannot answer this: RDS grants replication through rds_replication
// membership while leaving rolreplication false, and PostgreSQL never inherits
// role attributes through membership.
func checkReplicationConnection(ctx context.Context, dsn string) []Finding {
	if strings.TrimSpace(dsn) == "" {
		return []Finding{{
			ID: "source-replication-privilege", Kind: "privilege", Severity: SeverityWarning,
			Message: "source DSN is unavailable, so the replication-connection probe was skipped",
		}}
	}
	if err := probeReplicationConnection(ctx, dsn); err != nil {
		return []Finding{errorFinding("source-replication-privilege", "privilege",
			fmt.Sprintf("source replication connection failed: %v; %s", err, replicationRemediation))}
	}
	return nil
}

func probeReplicationConnection(ctx context.Context, dsn string) error {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse source DSN: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string, 1)
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = pglogrepl.IdentifySystem(ctx, conn)
	return err
}

func checkSourcePrivileges(ctx context.Context, conn *pgx.Conn, tables []Table) ([]Finding, error) {
	var createDB bool
	if err := conn.QueryRow(ctx, `
		SELECT has_database_privilege(current_user,current_database(),'CREATE')
	`).Scan(&createDB); err != nil {
		return nil, err
	}
	var findings []Finding
	if !createDB {
		findings = append(findings, errorFinding("source-publication-privilege", "privilege",
			"source user needs CREATE on the database to create a publication"))
		return findings, nil
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	tableList := make([]string, 0, len(tables))
	for _, table := range tables {
		tableList = append(tableList, quoteIdentifier(table.Schema)+"."+quoteIdentifier(table.Name))
	}
	statement := fmt.Sprintf("CREATE PUBLICATION pgmigrate_preflight_probe_%d", conn.PgConn().PID())
	if len(tableList) != 0 {
		statement += " FOR TABLE " + strings.Join(tableList, ",")
	}
	if _, err := tx.Exec(ctx, statement); err != nil {
		findings = append(findings, errorFinding("source-publication-probe", "privilege",
			"source publication probe failed: "+err.Error()))
	}
	return findings, nil
}

func checkTargetPrivileges(ctx context.Context, conn *pgx.Conn) ([]Finding, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "CREATE SCHEMA pgmigrate_preflight_probe"); err != nil {
		return []Finding{errorFinding("target-ddl-privilege", "privilege", "target cannot create migration schema: "+err.Error())}, nil
	}
	if _, err := tx.Exec(ctx, "CREATE TABLE pgmigrate_preflight_probe.t(id integer)"); err != nil {
		return []Finding{errorFinding("target-ddl-privilege", "privilege", "target cannot create migration table: "+err.Error())}, nil
	}
	var findings []Finding
	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		findings = append(findings, Finding{
			ID: "target-session-replication-role", Kind: "privilege",
			Severity: SeverityError, Message: "target user cannot set session_replication_role: " + err.Error(),
		})
		return findings, nil // transaction is aborted
	}
	if err := tx.Rollback(ctx); err != nil {
		return nil, err
	}

	originTx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer originTx.Rollback(context.Background())
	probeOrigin := fmt.Sprintf("pgmigrate_preflight_probe_%d", conn.PgConn().PID())
	_, probeErr := originTx.Exec(ctx, "SELECT pg_catalog.pg_replication_origin_create($1)", probeOrigin)
	if probeErr == nil {
		if _, probeErr = originTx.Exec(ctx, "SELECT pg_catalog.pg_replication_origin_session_setup($1)", probeOrigin); probeErr == nil {
			_, probeErr = originTx.Exec(ctx, "SELECT pg_catalog.pg_replication_origin_session_reset()")
		}
	}
	if probeErr != nil {
		// Replication origins are optional because custom transactional progress
		// is the resume authority.
		findings = append(findings, Finding{
			ID: "target-replication-origin", Kind: "privilege",
			Severity: SeverityWarning, Message: "target replication-origin functions are unavailable; custom progress remains authoritative: " + probeErr.Error(),
		})
	}
	return findings, nil
}

var toolVersionPattern = regexp.MustCompile(`(?m)(?:PostgreSQL\)?\s+)?(\d+)(?:\.\d+)?`)

// ParseToolMajor extracts a PostgreSQL client-tool major from --version output.
func ParseToolMajor(output string) (int, error) {
	match := toolVersionPattern.FindStringSubmatch(output)
	if len(match) != 2 {
		return 0, fmt.Errorf("cannot parse PostgreSQL tool version %q", strings.TrimSpace(output))
	}
	major, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("parse PostgreSQL tool version: %w", err)
	}
	return major, nil
}

// ToolMajor executes a PostgreSQL client tool's --version command.
func ToolMajor(ctx context.Context, path string) (int, error) {
	resolved, err := exec.LookPath(path)
	if err != nil {
		return 0, fmt.Errorf("locate %s: %w", path, err)
	}
	output, err := exec.CommandContext(ctx, resolved, "--version").CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("%s --version: %w", path, err)
	}
	return ParseToolMajor(string(output))
}

// checkTargetTuning reports the settings the run would change on the target for
// the bulk load, and warns about the ones it cannot.
//
// The point of doing this in preflight is timing. A target that refuses
// ALTER SYSTEM is the normal case on a managed service, and finding out means the
// difference between an operator deciding up front to raise max_wal_size through
// a parameter group, and discovering after a multi-hour copy that checkpoints
// were firing continuously throughout it.
func checkTargetTuning(ctx context.Context, target *pgx.Conn, cfg Config) ([]Finding, error) {
	if cfg.SkipTargetTuning {
		return []Finding{{
			ID: "target-tuning-skipped", Kind: "tuning", Severity: SeverityInfo,
			Message: "target settings will be left as they are; the bulk load will use whatever the target defaults to",
		}}, nil
	}
	workers := cfg.TuningWorkers
	if workers < 1 {
		workers = 1
	}
	observed, err := tuning.Observe(ctx, target)
	if err != nil {
		return nil, err
	}
	plan, err := tuning.Derive(observed, cfg.TuningOverrides, workers)
	if err != nil {
		return nil, err
	}
	return tuningFindings(plan), nil
}

// tuningFindings describes a plan, separated from reading the target so that what
// an operator is told can be tested without a server.
func tuningFindings(plan tuning.Plan) []Finding {
	var findings []Finding
	if plan.Empty() {
		findings = append(findings, Finding{
			ID: "target-tuning", Kind: "tuning", Severity: SeverityInfo,
			Message: "target is already configured at least as well as this migration would set it",
		})
	} else {
		described := make([]string, 0, len(plan.Changes))
		for _, change := range plan.Changes {
			described = append(described,
				fmt.Sprintf("%s %s to %s (%s)", change.Name, change.From, change.To, change.Scope))
		}
		memory := "operator-supplied"
		if plan.MemoryEstimated {
			memory = "estimated from shared_buffers"
		}
		findings = append(findings, Finding{
			ID: "target-tuning", Kind: "tuning", Severity: SeverityInfo,
			Message: fmt.Sprintf("target will be tuned for the bulk load and restored at cutover, sized for %s of memory (%s): %s",
				tuning.FormatBytes(plan.MemoryBytes), memory, strings.Join(described, ", ")),
		})
	}

	blocked := make([]string, 0, len(plan.Blocked))
	for name := range plan.Blocked {
		blocked = append(blocked, name)
	}
	slices.Sort(blocked)
	for _, name := range blocked {
		findings = append(findings, Finding{
			ID: "target-tuning-" + strings.ReplaceAll(name, "_", "-"), Kind: "tuning",
			Severity: SeverityWarning,
			Message: fmt.Sprintf("%s cannot be tuned for the bulk load, which will be slower: %s. %s",
				name, plan.Blocked[name], tuningRemediation),
		})
	}
	return findings
}

// tuningRemediation names the two ways an operator can get the settings applied
// when the tool cannot, which is the common case on a managed service.
const tuningRemediation = "Set it on the target before starting (an RDS parameter group, " +
	"a Cloud SQL database flag, or postgresql.conf), or pass --skip-target-tuning to stop trying. " +
	"This affects how long the migration takes, not whether it is correct."

func errorFinding(id, kind, message string) Finding {
	return Finding{ID: id, Kind: kind, Severity: SeverityError, Message: message}
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
