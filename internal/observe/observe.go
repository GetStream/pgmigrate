// Package observe renders migration state and exposes isolated Prometheus
// collectors without using the process-global registry.
package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
)

// Provider is implemented by state.Store.
type Provider interface {
	Snapshot(context.Context) (state.Status, error)
}

// ObjectCounts is a JSON-friendly completion summary.
type ObjectCounts struct {
	Done  int64 `json:"done"`
	Total int64 `json:"total"`
}

// Apply is the rendered replication apply state.
type Apply struct {
	StagedLSN                   string        `json:"staged_lsn"`
	AppliedLSN                  string        `json:"applied_lsn"`
	Txns                        int64         `json:"transactions"`
	Rows                        int64         `json:"rows"`
	DMLStatements               int64         `json:"dml_statements"`
	TargetCommits               int64         `json:"target_commits"`
	RowsPerSecond               float64       `json:"rows_per_second"`
	RowsPerDMLStatement         float64       `json:"rows_per_dml_statement"`
	TransactionsPerTargetCommit float64       `json:"source_transactions_per_target_commit"`
	Tables                      []ApplyTable  `json:"tables,omitempty"`
	UpdatedAt                   time.Time     `json:"updated_at"`
	LagBytes                    uint64        `json:"lag_bytes"`
	StaleFor                    time.Duration `json:"stale_for"`
}

// ApplyTable is one relation's replay throughput and row-statement compression.
type ApplyTable struct {
	Table               string  `json:"table"`
	Rows                int64   `json:"rows"`
	DMLStatements       int64   `json:"dml_statements"`
	RowsPerSecond       float64 `json:"rows_per_second"`
	RowsPerDMLStatement float64 `json:"rows_per_dml_statement"`
}

// Recovery is the rendered startup validation of local CDC segments.
type Recovery struct {
	TotalBytes         int64         `json:"total_bytes"`
	TrustedBytes       int64         `json:"trusted_bytes"`
	ScannedBytes       int64         `json:"scanned_bytes"`
	TotalSegments      int64         `json:"total_segments"`
	TrustedSegments    int64         `json:"trusted_segments"`
	ScannedSegments    int64         `json:"scanned_segments"`
	Elapsed            time.Duration `json:"elapsed"`
	ScanBytesPerSecond float64       `json:"scan_bytes_per_second"`
	FallbackReason     string        `json:"fallback_reason,omitempty"`
	ManifestRebuilt    bool          `json:"manifest_rebuilt"`
}

// Verification is one table's check progress and outcome.
//
// Pages are the source's alone, because only the source is read by page; the
// target is reached by key and counted in rows. Sampled against Estimated is
// reported because it is the size of the claim: a converged table means the rows
// that were compared agreed, not that the tables match. Candidates are reported
// separately from unresolved rows because on a live source the first without the
// second is a change in flight rather than a defect.
type Verification struct {
	Table            string        `json:"table"`
	Stage            string        `json:"stage,omitempty"`
	SourcePages      int64         `json:"source_pages"`
	SourcePagesTotal int64         `json:"source_pages_total"`
	Sampled          int64         `json:"sampled_rows"`
	Estimated        int64         `json:"estimated_rows"`
	TargetRows       int64         `json:"target_rows"`
	Rate             float64       `json:"rows_per_second"`
	ETA              time.Duration `json:"eta"`
	Coverage         float64       `json:"coverage"`
	Candidates       int64         `json:"candidate_rows,omitempty"`
	// CDCKeys and CDCObserved are the separate check of the rows the applier
	// wrote. They stay out of Sampled: a heap sample cannot find a row because it
	// was replicated, so the two cover different rows and adding them would let
	// the cheaper one stand for both.
	CDCKeys     int64     `json:"cdc_keys,omitempty"`
	CDCObserved int64     `json:"cdc_observed,omitempty"`
	Unresolved  int64     `json:"unresolved_rows,omitempty"`
	Converged   bool      `json:"converged"`
	Complete    bool      `json:"complete"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Snapshot is a rendering-neutral status value.
type Snapshot struct {
	CapturedAt     time.Time               `json:"captured_at"`
	Phase          state.Phase             `json:"phase"`
	EndPosition    string                  `json:"end_position,omitempty"`
	Objects        map[string]ObjectCounts `json:"objects"`
	Verification   []Verification          `json:"verification,omitempty"`
	OpenFindings   int64                   `json:"open_findings"`
	CompletedSteps int64                   `json:"completed_steps"`
	Apply          Apply                   `json:"apply"`
	Recovery       Recovery                `json:"recovery"`
}

// Capture obtains one transactionally consistent state snapshot.
func Capture(ctx context.Context, provider Provider, now time.Time) (Snapshot, error) {
	if provider == nil {
		return Snapshot{}, fmt.Errorf("status provider is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	status, err := provider.Snapshot(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	lag, err := lagBytes(status.Apply.StagedLSN, status.Apply.AppliedLSN)
	if err != nil {
		return Snapshot{}, err
	}
	stale := time.Duration(0)
	if !status.Apply.UpdatedAt.IsZero() && now.After(status.Apply.UpdatedAt) {
		stale = now.Sub(status.Apply.UpdatedAt)
	}
	return Snapshot{
		CapturedAt:  now.UTC(),
		Phase:       status.Migration.Phase,
		EndPosition: status.Migration.EndPosition,
		Objects: map[string]ObjectCounts{
			"tables":      counts(status.Tables),
			"parts":       counts(status.Parts),
			"indexes":     counts(status.Indexes),
			"constraints": counts(status.Constraints),
			"verify":      counts(status.VerifyTables),
		},
		Verification:   verification(status.Verification),
		OpenFindings:   status.OpenFindings,
		CompletedSteps: status.CompletedSteps,
		Apply: Apply{
			StagedLSN: status.Apply.StagedLSN, AppliedLSN: status.Apply.AppliedLSN,
			Txns: status.Apply.Txns, Rows: status.Apply.Rows,
			DMLStatements: status.Apply.DMLStatements, TargetCommits: status.Apply.TargetCommits,
			RowsPerSecond:               status.Apply.RowsPerSecond,
			RowsPerDMLStatement:         ratio(status.Apply.Rows, status.Apply.DMLStatements),
			TransactionsPerTargetCommit: ratio(status.Apply.Txns, status.Apply.TargetCommits),
			Tables:                      applyTables(status.ApplyTables),
			UpdatedAt:                   status.Apply.UpdatedAt, LagBytes: lag, StaleFor: stale,
		},
		Recovery: Recovery{
			TotalBytes:         status.Recovery.TotalBytes,
			TrustedBytes:       status.Recovery.TrustedBytes,
			ScannedBytes:       status.Recovery.ScannedBytes,
			TotalSegments:      status.Recovery.TotalSegments,
			TrustedSegments:    status.Recovery.TrustedSegments,
			ScannedSegments:    status.Recovery.ScannedSegments,
			Elapsed:            status.Recovery.Elapsed,
			ScanBytesPerSecond: status.Recovery.ScanBytesPerSecond,
			FallbackReason:     status.Recovery.FallbackReason,
			ManifestRebuilt:    status.Recovery.ManifestRebuilt,
		},
	}, nil
}

func applyTables(tables []state.ApplyTableProgress) []ApplyTable {
	if len(tables) == 0 {
		return nil
	}
	out := make([]ApplyTable, 0, len(tables))
	for _, table := range tables {
		out = append(out, ApplyTable{
			Table:               table.Schema + "." + table.Table,
			Rows:                table.Rows,
			DMLStatements:       table.DMLStatements,
			RowsPerSecond:       table.RowsPerSecond,
			RowsPerDMLStatement: ratio(table.Rows, table.DMLStatements),
		})
	}
	return out
}

func ratio(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func counts(value state.Counts) ObjectCounts {
	return ObjectCounts{Done: value.Done, Total: value.Total}
}

func verification(tables []state.VerifyTable) []Verification {
	if len(tables) == 0 {
		return nil
	}
	out := make([]Verification, 0, len(tables))
	for _, table := range tables {
		name := table.Schema + "." + table.Name
		if table.Schema == "" && table.Name == "" {
			name = fmt.Sprintf("oid %d", table.TableOID)
		}
		out = append(out, Verification{
			Table: name, Stage: table.Stage,
			SourcePages: table.SourcePages, SourcePagesTotal: table.SourcePagesTotal,
			Sampled: table.Sampled, Estimated: table.Estimated, TargetRows: table.TargetRows,
			Rate: table.Rate, ETA: table.ETA, Coverage: table.Coverage,
			Candidates: table.Candidates, Unresolved: table.Unresolved,
			CDCKeys: table.CDCKeys, CDCObserved: table.CDCObserved,
			Converged: table.Converged, Complete: table.Complete, UpdatedAt: table.UpdatedAt,
		})
	}
	return out
}

func lagBytes(staged, applied string) (uint64, error) {
	if staged == "" || applied == "" {
		return 0, nil
	}
	stagedLSN, err := pglogrepl.ParseLSN(staged)
	if err != nil {
		return 0, fmt.Errorf("parse staged LSN %q: %w", staged, err)
	}
	appliedLSN, err := pglogrepl.ParseLSN(applied)
	if err != nil {
		return 0, fmt.Errorf("parse applied LSN %q: %w", applied, err)
	}
	if appliedLSN >= stagedLSN {
		return 0, nil
	}
	return uint64(stagedLSN - appliedLSN), nil
}

// RenderJSON writes an indented, newline-terminated snapshot.
func RenderJSON(w io.Writer, snapshot Snapshot) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(snapshot); err != nil {
		return fmt.Errorf("render JSON status: %w", err)
	}
	return nil
}

// RenderText writes a compact, stable human-readable snapshot.
func RenderText(w io.Writer, snapshot Snapshot) error {
	var out strings.Builder
	fmt.Fprintf(&out, "phase: %s\n", snapshot.Phase)
	if snapshot.EndPosition != "" {
		fmt.Fprintf(&out, "end position: %s\n", snapshot.EndPosition)
	}
	for _, name := range []string{"tables", "parts", "indexes", "constraints", "verify"} {
		value := snapshot.Objects[name]
		fmt.Fprintf(&out, "%s: %d/%d\n", name, value.Done, value.Total)
	}
	for _, table := range snapshot.Verification {
		fmt.Fprintf(&out, "verify %s: %s %d/%d rows sampled (%.2f%%), %d/%d source pages, %d target rows%s%s%s\n",
			table.Table, table.Stage,
			table.Sampled, table.Estimated, 100*table.Coverage,
			table.SourcePages, table.SourcePagesTotal,
			table.TargetRows, cdcSuffix(table), divergenceSuffix(table), etaSuffix(table))
	}
	fmt.Fprintf(
		&out,
		"recovery: %d/%d bytes trusted, %d scanned; %d/%d segments trusted, %d scanned; %s elapsed, %.0f scan bytes/s; manifest rebuilt: %t\n",
		snapshot.Recovery.TrustedBytes,
		snapshot.Recovery.TotalBytes,
		snapshot.Recovery.ScannedBytes,
		snapshot.Recovery.TrustedSegments,
		snapshot.Recovery.TotalSegments,
		snapshot.Recovery.ScannedSegments,
		snapshot.Recovery.Elapsed.Round(time.Millisecond),
		snapshot.Recovery.ScanBytesPerSecond,
		snapshot.Recovery.ManifestRebuilt,
	)
	if snapshot.Recovery.FallbackReason != "" {
		fmt.Fprintf(&out, "recovery fallback: %s\n", snapshot.Recovery.FallbackReason)
	}
	fmt.Fprintf(&out, "findings: %d open\n", snapshot.OpenFindings)
	fmt.Fprintf(&out, "steps: %d complete\n", snapshot.CompletedSteps)
	fmt.Fprintf(&out, "apply: %s staged, %s applied, %d source txns, %d rows, %d DML statements, %d target commits, %.0f rows/s, %.2f rows/statement, %.2f txns/commit\n",
		emptyDash(snapshot.Apply.StagedLSN), emptyDash(snapshot.Apply.AppliedLSN),
		snapshot.Apply.Txns, snapshot.Apply.Rows, snapshot.Apply.DMLStatements,
		snapshot.Apply.TargetCommits, snapshot.Apply.RowsPerSecond,
		snapshot.Apply.RowsPerDMLStatement, snapshot.Apply.TransactionsPerTargetCommit)
	for _, table := range snapshot.Apply.Tables {
		fmt.Fprintf(
			&out,
			"apply %s: %d rows, %d DML statements, %.0f rows/s, %.2f rows/statement\n",
			table.Table, table.Rows, table.DMLStatements,
			table.RowsPerSecond, table.RowsPerDMLStatement,
		)
	}
	fmt.Fprintf(&out, "lag: %d bytes, %s stale\n", snapshot.Apply.LagBytes, snapshot.Apply.StaleFor.Round(time.Millisecond))
	if _, err := io.WriteString(w, out.String()); err != nil {
		return fmt.Errorf("render text status: %w", err)
	}
	return nil
}

// cdcSuffix reports the replication check next to the heap sample, and says
// nothing at all when the applier recorded nothing for the table. A zero printed
// there would read as a check that passed rather than one that never ran.
func cdcSuffix(table Verification) string {
	if table.CDCObserved <= 0 {
		return ""
	}
	return fmt.Sprintf(", %d/%d applied rows checked", table.CDCKeys, table.CDCObserved)
}

// etaSuffix appends an estimate only when one has been measured, so a status
// line never shows a made-up zero.
func etaSuffix(table Verification) string {
	if table.Complete || table.ETA <= 0 {
		return ""
	}
	return fmt.Sprintf(", eta %s", table.ETA.Round(time.Second))
}

// divergenceSuffix names what a table disagreed about, distinguishing rows that
// merely differed when first read from rows that were still differing after being
// re-read against a fixed WAL position.
func divergenceSuffix(table Verification) string {
	switch {
	case table.Unresolved > 0:
		return fmt.Sprintf(", %d rows diverged", table.Unresolved)
	case table.Candidates > 0:
		return fmt.Sprintf(", %d rows settled while rechecking", table.Candidates)
	default:
		return ""
	}
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

// Collector reads a fresh state snapshot for each Prometheus collection.
type Collector struct {
	Provider Provider
	Timeout  time.Duration
	Now      func() time.Time

	phase, objects, apply, applyRate, applyCompression, applyTable, applyTableRate,
	applyTableCompression, lag, findings, steps *prometheus.Desc
	verifyPages, verifyRows, verifyCoverage *prometheus.Desc
	verifyDivergent, verifyRate, verifyETA  *prometheus.Desc
	recoveryBytes, recoverySegments         *prometheus.Desc
	recoveryElapsed, recoveryScanRate       *prometheus.Desc
}

// verifyLabels are the labels every verification metric carries.
var verifyLabels = []string{"table"}

// NewCollector constructs an unregistered collector.
func NewCollector(provider Provider) *Collector {
	namespace := "pgmigrate"
	return &Collector{
		Provider: provider,
		Timeout:  5 * time.Second,
		Now:      func() time.Time { return time.Now().UTC() },
		phase:    prometheus.NewDesc(namespace+"_phase", "Current migration phase (one labeled value is 1).", []string{"phase"}, nil),
		objects:  prometheus.NewDesc(namespace+"_objects", "Migration object completion counts.", []string{"object", "state"}, nil),
		apply: prometheus.NewDesc(namespace+"_apply_total",
			"Committed source transactions and rows, target INSERT/UPDATE/DELETE statements, and target commits.",
			[]string{"unit"}, nil),
		applyRate: prometheus.NewDesc(namespace+"_apply_rows_per_second",
			"Committed source row changes applied per second over the latest reporting interval.", nil, nil),
		applyCompression: prometheus.NewDesc(namespace+"_apply_compression_ratio",
			"Cumulative source work represented by one target operation.", []string{"kind"}, nil),
		applyTable: prometheus.NewDesc(namespace+"_apply_table_total",
			"Cumulative committed row and INSERT/UPDATE/DELETE statement counters for one table.",
			[]string{"table", "unit"}, nil),
		applyTableRate: prometheus.NewDesc(namespace+"_apply_table_rows_per_second",
			"Committed source row changes applied per second for one table over the latest reporting interval.",
			[]string{"table"}, nil),
		applyTableCompression: prometheus.NewDesc(namespace+"_apply_table_compression_ratio",
			"Cumulative source row changes represented by one target DML statement for one table.",
			[]string{"table"}, nil),
		lag:      prometheus.NewDesc(namespace+"_apply_lag", "Replication apply lag.", []string{"unit"}, nil),
		findings: prometheus.NewDesc(namespace+"_open_findings", "Current unresolved findings.", nil, nil),
		steps:    prometheus.NewDesc(namespace+"_completed_steps", "Current completed cutover/orchestration steps.", nil, nil),
		verifyPages: prometheus.NewDesc(namespace+"_verify_pages",
			"Source heap pages read while sampling one table, against the pages the table has.",
			append(slices.Clone(verifyLabels), "state"), nil),
		verifyRows: prometheus.NewDesc(namespace+"_verify_rows",
			"Rows sampled from the source, estimated in the table, and found on the target.",
			append(slices.Clone(verifyLabels), "kind"), nil),
		verifyCoverage: prometheus.NewDesc(namespace+"_verify_coverage",
			"Fraction of a table's rows that were sampled.", verifyLabels, nil),
		verifyDivergent: prometheus.NewDesc(namespace+"_verify_divergent",
			"Sampled rows that disagreed when first read, and rows still differing after being re-read.",
			append(slices.Clone(verifyLabels), "kind"), nil),
		verifyRate: prometheus.NewDesc(namespace+"_verify_rows_per_second",
			"Measured sampling rate for one table.", verifyLabels, nil),
		verifyETA: prometheus.NewDesc(namespace+"_verify_eta_seconds",
			"Estimated seconds remaining for one table, zero when unmeasured or done.", verifyLabels, nil),
		recoveryBytes: prometheus.NewDesc(namespace+"_recovery_bytes",
			"CDC segment bytes encountered, trusted from durable metadata, or scanned and checksummed.",
			[]string{"kind"}, nil),
		recoverySegments: prometheus.NewDesc(namespace+"_recovery_segments",
			"CDC segments encountered, trusted from durable metadata, or scanned and checksummed.",
			[]string{"kind"}, nil),
		recoveryElapsed: prometheus.NewDesc(namespace+"_recovery_elapsed_seconds",
			"Elapsed CDC startup recovery time in seconds.", nil, nil),
		recoveryScanRate: prometheus.NewDesc(namespace+"_recovery_scan_bytes_per_second",
			"CDC bytes scanned and checksummed per second during startup recovery.", nil, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		c.phase, c.objects, c.apply, c.applyRate, c.applyCompression,
		c.applyTable, c.applyTableRate, c.applyTableCompression,
		c.lag, c.findings, c.steps,
		c.verifyPages, c.verifyRows, c.verifyCoverage, c.verifyDivergent,
		c.verifyRate, c.verifyETA,
		c.recoveryBytes, c.recoverySegments, c.recoveryElapsed, c.recoveryScanRate,
	} {
		ch <- desc
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	if c.Provider == nil {
		ch <- prometheus.NewInvalidMetric(c.phase, fmt.Errorf("status provider is required"))
		return
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	now := time.Now().UTC()
	if c.Now != nil {
		now = c.Now()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	snapshot, err := Capture(ctx, c.Provider, now)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.phase, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.phase, prometheus.GaugeValue, 1, string(snapshot.Phase))
	for _, name := range []string{"tables", "parts", "indexes", "constraints", "verify"} {
		value := snapshot.Objects[name]
		ch <- prometheus.MustNewConstMetric(c.objects, prometheus.GaugeValue, float64(value.Done), name, "done")
		ch <- prometheus.MustNewConstMetric(c.objects, prometheus.GaugeValue, float64(value.Total), name, "total")
	}
	ch <- prometheus.MustNewConstMetric(c.apply, prometheus.CounterValue, float64(snapshot.Apply.Txns), "transactions")
	ch <- prometheus.MustNewConstMetric(c.apply, prometheus.CounterValue, float64(snapshot.Apply.Rows), "rows")
	ch <- prometheus.MustNewConstMetric(c.apply, prometheus.CounterValue,
		float64(snapshot.Apply.DMLStatements), "dml_statements")
	ch <- prometheus.MustNewConstMetric(c.apply, prometheus.CounterValue,
		float64(snapshot.Apply.TargetCommits), "target_commits")
	ch <- prometheus.MustNewConstMetric(c.applyRate, prometheus.GaugeValue, snapshot.Apply.RowsPerSecond)
	ch <- prometheus.MustNewConstMetric(c.applyCompression, prometheus.GaugeValue,
		snapshot.Apply.RowsPerDMLStatement, "rows_per_dml_statement")
	ch <- prometheus.MustNewConstMetric(c.applyCompression, prometheus.GaugeValue,
		snapshot.Apply.TransactionsPerTargetCommit, "source_transactions_per_target_commit")
	for _, table := range snapshot.Apply.Tables {
		ch <- prometheus.MustNewConstMetric(c.applyTable, prometheus.CounterValue,
			float64(table.Rows), table.Table, "rows")
		ch <- prometheus.MustNewConstMetric(c.applyTable, prometheus.CounterValue,
			float64(table.DMLStatements), table.Table, "dml_statements")
		ch <- prometheus.MustNewConstMetric(c.applyTableRate, prometheus.GaugeValue,
			table.RowsPerSecond, table.Table)
		ch <- prometheus.MustNewConstMetric(c.applyTableCompression, prometheus.GaugeValue,
			table.RowsPerDMLStatement, table.Table)
	}
	ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, float64(snapshot.Apply.LagBytes), "bytes")
	ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, snapshot.Apply.StaleFor.Seconds(), "seconds")
	ch <- prometheus.MustNewConstMetric(c.findings, prometheus.GaugeValue, float64(snapshot.OpenFindings))
	ch <- prometheus.MustNewConstMetric(c.steps, prometheus.GaugeValue, float64(snapshot.CompletedSteps))
	for _, value := range []struct {
		kind     string
		bytes    int64
		segments int64
	}{
		{"total", snapshot.Recovery.TotalBytes, snapshot.Recovery.TotalSegments},
		{"trusted", snapshot.Recovery.TrustedBytes, snapshot.Recovery.TrustedSegments},
		{"scanned", snapshot.Recovery.ScannedBytes, snapshot.Recovery.ScannedSegments},
	} {
		ch <- prometheus.MustNewConstMetric(
			c.recoveryBytes, prometheus.GaugeValue, float64(value.bytes), value.kind,
		)
		ch <- prometheus.MustNewConstMetric(
			c.recoverySegments, prometheus.GaugeValue, float64(value.segments), value.kind,
		)
	}
	ch <- prometheus.MustNewConstMetric(
		c.recoveryElapsed, prometheus.GaugeValue, snapshot.Recovery.Elapsed.Seconds(),
	)
	ch <- prometheus.MustNewConstMetric(
		c.recoveryScanRate, prometheus.GaugeValue, snapshot.Recovery.ScanBytesPerSecond,
	)
	for _, table := range snapshot.Verification {
		labels := []string{table.Table}
		for _, page := range []struct {
			state string
			value int64
		}{{"done", table.SourcePages}, {"total", table.SourcePagesTotal}} {
			ch <- prometheus.MustNewConstMetric(c.verifyPages, prometheus.GaugeValue,
				float64(page.value), append(slices.Clone(labels), page.state)...)
		}
		for _, kind := range []struct {
			name  string
			value int64
		}{{"sampled", table.Sampled}, {"estimated", table.Estimated}, {"target", table.TargetRows}} {
			ch <- prometheus.MustNewConstMetric(c.verifyRows, prometheus.GaugeValue,
				float64(kind.value), append(slices.Clone(labels), kind.name)...)
		}
		ch <- prometheus.MustNewConstMetric(c.verifyCoverage, prometheus.GaugeValue, table.Coverage, labels...)
		ch <- prometheus.MustNewConstMetric(c.verifyDivergent, prometheus.GaugeValue,
			float64(table.Candidates), append(slices.Clone(labels), "candidates")...)
		ch <- prometheus.MustNewConstMetric(c.verifyDivergent, prometheus.GaugeValue,
			float64(table.Unresolved), append(slices.Clone(labels), "rows")...)
		ch <- prometheus.MustNewConstMetric(c.verifyRate, prometheus.GaugeValue, table.Rate, labels...)
		ch <- prometheus.MustNewConstMetric(c.verifyETA, prometheus.GaugeValue, table.ETA.Seconds(), labels...)
	}
}

// NewRegistry returns a private registry containing only this migration's
// collector. It never mutates prometheus.DefaultRegisterer.
func NewRegistry(provider Provider) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(provider))
	return registry
}
