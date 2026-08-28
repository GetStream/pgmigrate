// Package verify samples the source and checks the sampled rows against the
// target, using deterministic server-side row hashes.
//
// One table's check reads about a million rows from a spread of page intervals of
// the source heap, each answered by a Tid Range Scan, and looks those exact rows
// up on the target through the key's own index. Both sides return a hash of the
// whole row, so a missing row and a wrongly applied column value are the same
// finding, and a disagreement is named by its key from the read that found it.
//
// The cost of this does not scale with the size of the table, which is the point.
// The exhaustive comparison it replaced read both heaps end to end: 163.7 GB and
// 330M row hashes for a 66-minute answer on one production shard, 57 minutes of it
// a single table. See docs/design-verify-sampled.md.
//
// Verification is one-way: source rows must exist on the target with matching
// contents. Extra target rows are ignored, including during CDC checks and
// rechecks. A sample cannot prove that every source row matches; a clean result
// means only that the source rows compared agreed.
package verify

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// defaultSampleRows is how many rows one table's check reads from the source.
	defaultSampleRows int64 = 1_000_000
	// defaultSampleWindows is how many separate places in the heap it reads them
	// from. Row density varies by two orders of magnitude between regions of a
	// bloated heap, so a sample taken from one place is not a sample of the table.
	defaultSampleWindows int64 = 128
	// sampleHeadroom oversizes a window's page interval against the density
	// estimate, so the row limit is normally what bounds the read rather than
	// reltuples being right.
	sampleHeadroom = 2.0
	// defaultBatchRows is how many keys one target lookup carries.
	defaultBatchRows int64 = 5000
	// defaultConvergeTimeout bounds the loop that waits for rows in flight to
	// settle. Reaching it means the rows are reported as divergence.
	defaultConvergeTimeout = time.Minute
	defaultCDCRecheckDelay = time.Minute
	// defaultRowThreshold bounds how many differing rows one table names.
	defaultRowThreshold int64 = 1000
)

// SelectTable is called for every ordinary or partitioned source table.
type SelectTable func(schema, table string) bool

// Connector opens a new PostgreSQL connection owned by the caller.
type Connector func(context.Context) (*pgx.Conn, error)

// Table is the verifier's source inventory for one selected table.
type Table struct {
	OID                   uint32 `json:"oid"`
	Schema, Name, RelKind string
	Key                   Key `json:"key"`
	// Keyless explains why the table has no key, and is empty when it has one.
	// Such a table cannot be compared at all: the check finds rows on the target by
	// key, and there is nothing to look them up by.
	Keyless string `json:"keyless,omitempty"`
}

func (t Table) Identifier() string { return QuoteIdentifier(t.Schema, t.Name) }

// QuoteIdentifier safely quotes a PostgreSQL identifier or qualified name.
func QuoteIdentifier(parts ...string) string { return pgx.Identifier(parts).Sanitize() }

type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Inventory reads selected ordinary and partitioned tables and chooses each one's
// key. Child partitions are represented by their selected partitioned root,
// matching copy.Inventory; the source's leaves are enumerated when the sample is
// planned, because a page interval only means something within one heap.
func Inventory(ctx context.Context, db *pgx.Conn, selected SelectTable) ([]Table, error) {
	return inventory(ctx, db, selected)
}

func inventory(ctx context.Context, db querier, selected SelectTable) ([]Table, error) {
	rows, err := db.Query(ctx, `
		SELECT c.oid, n.nspname, c.relname, c.relkind::text
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relkind IN ('r','p')
		  AND n.nspname NOT IN ('pg_catalog','information_schema')
		  AND n.nspname !~ '^pg_toast' AND NOT c.relispartition
		ORDER BY c.oid`)
	if err != nil {
		return nil, fmt.Errorf("inventory verification tables: %w", err)
	}
	defer rows.Close()
	var tables []Table
	for rows.Next() {
		var table Table
		if err := rows.Scan(&table.OID, &table.Schema, &table.Name, &table.RelKind); err != nil {
			return nil, fmt.Errorf("scan verification table: %w", err)
		}
		if selected == nil || selected(table.Schema, table.Name) {
			tables = append(tables, table)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate verification tables: %w", err)
	}
	for i := range tables {
		key, keyless, err := chooseKey(ctx, db, tables[i].OID)
		if err != nil {
			return nil, err
		}
		tables[i].Key, tables[i].Keyless = key, keyless
	}
	return tables, nil
}

// SourceResult is what the sampling pass read.
//
// Pages and PagesTotal are deliberately not the same order of magnitude: the
// first is what the windows covered, the second is the whole heap. A reader who
// sees them equal should be reading a table small enough to have been read whole.
type SourceResult struct {
	Pages      int64 `json:"pages"`
	PagesTotal int64 `json:"pages_total"`
	Relations  int   `json:"relations"`
	Windows    int   `json:"windows"`
	Rows       int64 `json:"rows"`
	// Estimated is the planner's row count for the whole table, which is what
	// Rows is a sample of.
	Estimated int64 `json:"estimated_rows"`
}

// TargetResult is what the lookup pass read. It counts keys rather than pages,
// because the target is reached through an index and never scanned.
type TargetResult struct {
	Batches int   `json:"batches"`
	Keys    int64 `json:"keys"`
	Rows    int64 `json:"rows"`
}

// TableResult is the result for one selected table.
type TableResult struct {
	Table  Table        `json:"table"`
	Source SourceResult `json:"source"`
	Target TargetResult `json:"target"`
	// CDC is the separate check of the rows the applier reported writing. The
	// heap sample cannot reach them, so a table can be clean in one and not the
	// other, and the two are never added together.
	CDC CDCResult `json:"cdc"`
	// Candidates is how many sampled rows disagreed before convergence. It can be
	// non-zero on a converged table: a change in flight during the check
	// disagrees, and is then resolved by re-reading the rows.
	Candidates int `json:"candidate_rows,omitempty"`
	// InFlight counts rows that differed when first read and agreed once re-read
	// against a fixed WAL position, which is replication latency rather than a
	// defect.
	InFlight int `json:"in_flight_rows,omitempty"`
	// Unresolved names the rows that still differed when the convergence budget
	// ran out. These are the divergence.
	Unresolved []RowDiff `json:"unresolved,omitempty"`
	Warnings   []string  `json:"warnings,omitempty"`
	// Coverage is the fraction of the table that was looked at, and for anything
	// large it is a small number. A converged table means "the rows I sampled
	// matched", never "the two tables match".
	Coverage float64       `json:"coverage"`
	Duration time.Duration `json:"duration"`
	// Complete is false when the check stopped for any reason other than
	// finishing. Such a table cannot converge, whatever it did read.
	Complete  bool   `json:"complete"`
	CutShort  string `json:"cut_short,omitempty"`
	Converged bool   `json:"converged"`
}

// Result summarizes an all-table verification run.
type Result struct {
	Tables    []TableResult `json:"tables"`
	Warnings  []string      `json:"warnings,omitempty"`
	Complete  bool          `json:"complete"`
	Converged bool          `json:"converged"`
}

// Config controls one verification run.
type Config struct {
	Source, Target Connector
	Tables         []Table
	// Workers verify whole tables in parallel. One is the default because two
	// concurrent scans saturated a two-vCPU production source. A separate worker
	// handles deferred CDC key lookups using at most one additional connection pair.
	Workers int
	// SampleRows is how many rows per table the source pass reads.
	SampleRows int64
	// SampleWindows is how many page intervals it reads them from.
	SampleWindows int64
	// BatchRows is how many keys one target lookup carries.
	BatchRows int64
	// DutyCycle is the fraction of the time verification may spend querying,
	// between 0 and 1. It sleeps between windows to stay under it.
	DutyCycle float64
	// TableTimeout bounds active work on one table, including the deferred CDC
	// recheck, excluding its delay and queue time. Zero disables it.
	TableTimeout time.Duration
	// ConvergeTimeout bounds heap-sample rechecks against a WAL boundary.
	ConvergeTimeout time.Duration
	// CDCRecheckDelay is the minimum time between a CDC mismatch snapshot and its
	// deferred check. Zero selects one minute.
	CDCRecheckDelay time.Duration
	// RowThreshold bounds heap candidate rechecks. CDC candidates and the audit
	// of initial mismatches are not truncated by this threshold.
	RowThreshold int64
	// CDCKeys supplies the rows the applier recorded writing, which is the only
	// way to check the replication path: a physical sample of the source finds
	// the rows the base copy wrote and misses those almost entirely. Nil disables
	// the stratum.
	CDCKeys CDCKeys
	// CDCRows bounds how many recorded keys one table's check looks at.
	CDCRows int64

	// Boundary and WaitApplied let a heap-sample row that differs be attributed to
	// replication latency rather than to corruption. Both nil means nothing is
	// applying, which is the case at cutover, and a row that differs is reported
	// as it stands.
	Boundary    Boundary
	WaitApplied WaitApplied

	Progress ProgressSink
	// Audit receives all mismatches and deferred outcomes and must support
	// concurrent calls. Failure aborts the run rather than silently losing the audit trail.
	Audit func([]AuditEvent) error
}

func (c *Config) applyDefaults() {
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.SampleRows <= 0 {
		c.SampleRows = defaultSampleRows
	}
	if c.SampleWindows <= 0 {
		c.SampleWindows = defaultSampleWindows
	}
	if c.BatchRows <= 0 {
		c.BatchRows = defaultBatchRows
	}
	if c.ConvergeTimeout <= 0 {
		c.ConvergeTimeout = defaultConvergeTimeout
	}
	if c.RowThreshold <= 0 {
		c.RowThreshold = defaultRowThreshold
	}
	if c.CDCRecheckDelay <= 0 {
		c.CDCRecheckDelay = defaultCDCRecheckDelay
	}
	if c.CDCRows <= 0 {
		c.CDCRows = defaultCDCKeys
	}
	if c.DutyCycle <= 0 || c.DutyCycle > 1 {
		c.DutyCycle = 1
	}
	if c.TableTimeout < 0 {
		c.TableTimeout = 0
	}
}

// Run compares all tables and returns a complete result. A data mismatch is
// represented by Result.Converged=false, not an execution error.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Source == nil || cfg.Target == nil {
		return Result{}, errors.New("source and target connectors are required")
	}
	cfg.applyDefaults()
	type item struct {
		index  int
		result TableResult
		err    error
	}
	jobs := make(chan int)
	results := make(chan item, len(cfg.Tables))
	deferred := make(chan item, len(cfg.Tables))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	shared := &rate{}
	// A single rechecker handles due CDC candidates while the initial table
	// workers keep scanning. It never scans a heap or blocks their job queue.
	var rechecks sync.WaitGroup
	rechecks.Add(1)
	go func() {
		defer rechecks.Done()
		worker := &worker{cfg: &cfg, rate: shared}
		defer worker.close()
		for job := range deferred {
			job.result, job.err = worker.recheckCDC(runCtx, job.result)
			results <- job
			if job.err != nil {
				cancel()
			}
		}
	}()
	var wg sync.WaitGroup
	for range min(cfg.Workers, max(1, len(cfg.Tables))) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := &worker{cfg: &cfg, rate: shared}
			defer worker.close()
			for index := range jobs {
				result, err := worker.verifyTable(runCtx, cfg.Tables[index])
				job := item{index: index, result: result, err: err}
				if err == nil && result.CDC.Pending > 0 {
					deferred <- job
				} else {
					results <- job
				}
				if err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for i := range cfg.Tables {
			select {
			case jobs <- i:
			case <-runCtx.Done():
				return
			}
		}
	}()
	wg.Wait()
	close(deferred)
	rechecks.Wait()
	close(results)
	out := Result{
		Tables:    make([]TableResult, len(cfg.Tables)),
		Converged: true, Complete: true,
	}
	var first error
	for item := range results {
		if item.err != nil && first == nil {
			first = item.err
		}
		out.Tables[item.index] = item.result
	}
	if first != nil {
		return Result{}, first
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	for i := range out.Tables {
		out.Converged = out.Converged && out.Tables[i].Converged
		out.Complete = out.Complete && out.Tables[i].Complete
		out.Warnings = append(out.Warnings, out.Tables[i].Warnings...)
	}

	slices.Sort(out.Warnings)
	out.Warnings = slices.Compact(out.Warnings)
	return out, nil
}

// worker owns one source and one target connection for the tables it is given, so
// a run opens a bounded number of connections however many chunks it reads.
type worker struct {
	cfg            *Config
	rate           *rate
	source, target *pgx.Conn
}

func (w *worker) close() {
	if w.source != nil {
		w.source.Close(context.Background())
		w.source = nil
	}
	if w.target != nil {
		w.target.Close(context.Background())
		w.target = nil
	}
}

func (w *worker) connect(ctx context.Context) error {
	if w.source == nil {
		source, err := w.cfg.Source(ctx)
		if err != nil {
			return fmt.Errorf("connect source verifier: %w", err)
		}
		w.source = source
		if err := pinSerialization(ctx, source); err != nil {
			return err
		}
	}
	if w.target == nil {
		target, err := w.cfg.Target(ctx)
		if err != nil {
			return fmt.Errorf("connect target verifier: %w", err)
		}
		w.target = target
		if err := pinSerialization(ctx, target); err != nil {
			return err
		}
	}
	return nil
}

// verifyTable samples the source, looks the sampled rows up on the target, and
// names whatever still disagrees.
func (w *worker) verifyTable(ctx context.Context, table Table) (TableResult, error) {
	if err := w.connect(ctx); err != nil {
		return TableResult{}, err
	}
	started := time.Now()
	out := TableResult{Table: table, Complete: true, Converged: true, Coverage: 1}
	tableCtx := ctx
	if w.cfg.TableTimeout > 0 {
		var cancel context.CancelFunc
		tableCtx, cancel = context.WithTimeout(ctx, w.cfg.TableTimeout)
		defer cancel()
	}
	if !table.Key.present() {
		// A sampled row is found on the target by key, so a table without one
		// cannot be checked this way at all. It is reported as checked and clean
		// rather than as a failure, because it is neither: nothing was compared.
		// The warning is the whole answer, and Coverage says zero.
		reason := table.Keyless
		if reason == "" {
			reason = "it has no primary key and no usable unique index"
		}
		out.Coverage = 0
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%s was not compared, because %s, and a sampled row can only be found on the target by key",
			table.Identifier(), reason,
		))
		return w.finish(out, started, ""), nil
	}

	source, err := relations(tableCtx, w.source, table)
	if err != nil {
		return TableResult{}, err
	}
	out.Source.PagesTotal = totalPages(source)
	out.Source.Estimated = totalRows(source)
	windows := planSample(source, w.cfg.SampleRows, w.cfg.SampleWindows)
	candidates, err := w.compareWindows(tableCtx, table, windows, &out)
	if err != nil {
		if cut := cutShort(ctx, tableCtx, err); cut != "" {
			return w.finish(out, started, cut), nil
		}
		return TableResult{}, err
	}
	out.Coverage = sampledCoverage(out.Source)
	out.Candidates = len(candidates)
	var unresolved []RowDiff
	if len(candidates) > 0 {
		w.report(Progress{
			Table: table.Schema + "." + table.Name, Stage: StageRechecking,
			Pages: out.Source.Pages, PagesTotal: out.Source.PagesTotal,
			Rows: out.Source.Rows, Estimated: out.Source.Estimated,
			Candidates: len(candidates), Coverage: out.Coverage,
		})
		resolved, _, err := w.resolve(tableCtx, table, candidateKeys(candidates, w.cfg.RowThreshold))
		if err != nil {
			if reason := cutShort(ctx, tableCtx, err); reason != "" {
				out.Converged = false
				return w.finish(out, started, reason), nil
			}
			return TableResult{}, err
		}
		out.InFlight = len(candidates) - len(resolved)
		unresolved = resolved
	}

	// The CDC stratum runs whatever the heap sample found, because it is checking
	// different rows. A clean heap sample is not a reason to skip the only check
	// that looks at the replication path.
	cdcDiffs, err := w.verifyCDC(tableCtx, table, source, &out)
	if err != nil {
		if reason := cutShort(ctx, tableCtx, err); reason != "" {
			out.Converged = false
			return w.finish(out, started, reason), nil
		}
		return TableResult{}, err
	}
	out.Unresolved = unresolved
	out.CDC.pending = cdcDiffs
	out.CDC.Pending = len(cdcDiffs)
	out.Converged = len(unresolved) == 0 && len(cdcDiffs) == 0
	return w.finish(out, started, ""), nil
}

func countRelations(chunks []pageChunk) int {
	seen := make(map[string]bool, len(chunks))
	for _, chunk := range chunks {
		seen[chunk.relation.identifier()] = true
	}
	return len(seen)
}

// sampledCoverage is the fraction of the table the check actually looked at.
//
// It must never round up to one for a table that was sampled. Reporting full
// coverage for a read of a hundredth of the heap is the most misleading thing this
// package could do, and a caller that gates on coverage would be gating on a lie.
func sampledCoverage(side SourceResult) float64 {
	if side.Estimated <= 0 {
		return 1
	}
	return min(float64(side.Rows)/float64(side.Estimated), 1)
}

// finish records the outcome and reports it, so a status in another process sees
// the same figures as the caller.
func (w *worker) finish(out TableResult, started time.Time, cut string) TableResult {
	stage := StageDone
	if out.CDC.Pending > 0 && cut == "" {
		stage = StageCDCDeferred
		out.Complete, out.Converged = false, false
	}
	if cut != "" {
		out.Complete, out.CutShort, out.Converged = false, cut, false
	}
	out.Duration = time.Since(started)
	w.report(Progress{
		Table: out.Table.Schema + "." + out.Table.Name, Side: sideSource, Stage: stage,
		Pages: out.Source.Pages, PagesTotal: out.Source.PagesTotal,
		Rows: out.Source.Rows, Estimated: out.Source.Estimated,
		TargetRows: out.Target.Rows, Coverage: out.Coverage,
		CDCKeys: out.CDC.Keys, CDCObserved: out.CDC.Observed,
		CDCPending: out.CDC.Pending,
		Candidates: out.Candidates, Unresolved: len(out.Unresolved),
		Converged: out.Converged, Complete: out.Complete,
	})
	return out
}

func (w *worker) report(update Progress) {
	if w.cfg.Progress != nil {
		w.cfg.Progress.Update(update)
	}
}

// throttle sleeps so querying stays under the configured duty cycle.
func (w *worker) throttle(ctx context.Context, elapsed time.Duration) error {
	if w.cfg.DutyCycle >= 1 || elapsed <= 0 {
		return nil
	}
	idle := time.Duration(float64(elapsed) * (1/w.cfg.DutyCycle - 1))
	timer := time.NewTimer(idle)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// cutShort names the reason a comparison stopped early, and returns empty for an
// error that is a real failure rather than a guard firing.
func cutShort(parent, table context.Context, err error) string {
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return ""
	}
	if parent.Err() != nil {
		return "" // the whole run is going away; report the error
	}
	if errors.Is(table.Err(), context.DeadlineExceeded) {
		return "table timeout reached"
	}
	return ""
}

// pinSerialization fixes every setting the rendering of a row to text depends on.
//
// The comparison hashes rendered rows, so a difference in DateStyle or TimeZone
// between the two servers would read as a difference in the data. These are set on
// the session rather than per transaction because a pass is a sequence of
// independent statements.
func pinSerialization(ctx context.Context, conn *pgx.Conn) error {
	for _, statement := range []string{
		"SET DateStyle = 'ISO, YMD'",
		"SET IntervalStyle = 'postgres'",
		"SET TimeZone = 'UTC'",
		"SET extra_float_digits = 3",
		"SET bytea_output = 'hex'",
		"SET lc_numeric = 'C'",
	} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("pin verification serialization (%s): %w", statement, err)
		}
	}
	return nil
}

// DivergedTables returns stable qualified names for mismatching tables.
func (r Result) DivergedTables() []string {
	var names []string
	for _, table := range r.Tables {
		if !table.Converged {
			names = append(names, table.Table.Schema+"."+table.Table.Name)
		}
	}
	slices.Sort(names)
	return names
}

// CutShort names the tables whose comparison stopped before finishing, each with
// its reason.
//
// Such a table reports Converged=false whatever it did read, so a caller that only
// knew about divergence would blame the data for what is really an unfinished
// comparison.
func (r Result) CutShort() []string {
	var reasons []string
	for _, table := range r.Tables {
		if table.Complete {
			continue
		}
		reason := table.CutShort
		if reason == "" {
			reason = "stopped early"
		}
		reasons = append(reasons, fmt.Sprintf("%s.%s: %s",
			table.Table.Schema, table.Table.Name, reason))
	}
	slices.Sort(reasons)
	return reasons
}

// Coverage is the smallest per-table coverage any check reached, which is the
// weakest claim in the run rather than the average.
func (r Result) Coverage() float64 {
	coverage := 1.0
	for _, table := range r.Tables {
		coverage = min(coverage, table.Coverage)
	}
	return coverage
}

// CDCKeys and CDCObserved are how many applier-recorded rows the run checked and
// how many changes the applier saw. They are reported next to the sampled figures
// and never merged with them: one says the base copy landed, the other says
// replication is applying correctly, and a run can be clean in one and not the
// other.
func (r Result) CDCKeys() int64 {
	total := int64(0)
	for _, table := range r.Tables {
		total += table.CDC.Keys
	}
	return total
}

func (r Result) CDCObserved() int64 {
	total := int64(0)
	for _, table := range r.Tables {
		total += table.CDC.Observed
	}
	return total
}

// CDCDeletes is how many of the checked keys were rows the applier deleted, which
// is worth reporting on its own. A deletion is the one outcome no read of either
// heap can find: the row is gone from both, and only a key recorded as it was
// removed proves the removal happened on both sides.
func (r Result) CDCDeletes() int64 {
	total := int64(0)
	for _, table := range r.Tables {
		total += table.CDC.Deletes
	}
	return total
}

// UnresolvedRows is how many individual rows the run named as divergence.
func (r Result) UnresolvedRows() int {
	total := 0
	for _, table := range r.Tables {
		total += len(table.Unresolved)
	}
	return total
}
