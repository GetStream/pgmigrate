package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GetStream/pgmigrate/internal/cdc"
	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/GetStream/pgmigrate/internal/verify"
)

// marker writes the KEEP markers a recheck waits on.
//
// A row that differs between a live source and a target that is still applying is
// expected to differ, so verification has to be able to ask "has apply seen
// everything the source had when I read it?". That needs a source WAL position the
// applier will certainly reach, and on an otherwise idle source no such position
// exists unless something writes one: hence a marker.
//
// The marker is nontransactional, so decoding does not wait for a transaction to
// commit. What is easy to miss is that a nontransactional message is written to WAL
// but nothing commits it, and the walsender only reads up to the flush position. An
// unflushed marker stays invisible until some unrelated transaction happens to
// flush the WAL past it, which makes the recheck wait on the source's write traffic
// rather than on apply: seconds against a lightly loaded source, and forever
// against an idle one.
//
// PostgreSQL 17 added a flush argument that settles it in one statement. On 16
// there is no way to flush WAL from SQL, so the marker is followed by a
// transactional message on a second connection: that one gets an XID, so its commit
// record flushes everything before it, the marker included.
type marker struct {
	dsn     string
	payload string
	flush   bool

	mu    sync.Mutex
	nudge *pgx.Conn
}

func newMarker(cfg config.Config, kind string, capabilities postgres.Capabilities) *marker {
	return &marker{
		dsn:     cfg.Source,
		payload: kind + migrationID(cfg.Dir),
		flush:   capabilities.LogicalMessageFlush,
	}
}

// emit writes a marker on the supplied source connection and returns its position.
func (m *marker) emit(ctx context.Context, conn *pgx.Conn) (string, error) {
	statement := "SELECT pg_catalog.pg_logical_emit_message(false,$1,$2)::text"
	if m.flush {
		statement = "SELECT pg_catalog.pg_logical_emit_message(false,$1,$2,flush=>true)::text"
	}
	var lsn string
	if err := conn.QueryRow(ctx, statement, cdc.CutoverMessagePrefix, m.payload).Scan(&lsn); err != nil {
		return "", err
	}
	if m.flush {
		return lsn, nil
	}
	return lsn, m.flushWAL(ctx)
}

// flushWAL commits a transactional message, whose commit record is what carries the
// preceding marker to disk. It is only reached on PostgreSQL 16.
func (m *marker) flushWAL(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nudge == nil {
		conn, err := postgres.Connect(ctx, m.dsn)
		if err != nil {
			return fmt.Errorf("connect to flush the verification marker: %w", err)
		}
		// An asynchronous commit would return before writing anything, which is
		// the one thing this connection exists to do.
		if _, err := conn.Exec(ctx, "SET synchronous_commit = on"); err != nil {
			conn.Close(context.Background())
			return err
		}
		m.nudge = conn
	}
	_, err := m.nudge.Exec(ctx,
		"SELECT pg_catalog.pg_logical_emit_message(true,$1,$2)",
		cdc.CutoverMessagePrefix, m.payload+":flush")
	if err != nil {
		// The connection may be the casualty rather than the statement, so it is
		// dropped and reopened on the next marker instead of failing every one.
		m.nudge.Close(context.Background())
		m.nudge = nil
		return fmt.Errorf("flush the verification marker: %w", err)
	}
	return nil
}

func (m *marker) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nudge != nil {
		m.nudge.Close(context.Background())
		m.nudge = nil
	}
}

// sourceCapabilities reads what the source's release supports.
func sourceCapabilities(ctx context.Context, dsn string) (postgres.Capabilities, error) {
	conn, err := postgres.Connect(ctx, dsn)
	if err != nil {
		return postgres.Capabilities{}, err
	}
	defer conn.Close(context.Background())
	var major int
	if err := conn.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int / 10000").Scan(&major); err != nil {
		return postgres.Capabilities{}, fmt.Errorf("read source PostgreSQL major: %w", err)
	}
	return postgres.CapabilitiesForMajor(major)
}

// recheckHooks returns what verification needs to tell replication latency from
// corruption: a way to mark a source position, and a way to wait for apply to reach
// it.
//
// Both are nil unless changes are actually being applied. During catch-up or after
// the drain nothing advances apply, so waiting for a marker would wait forever, and
// a target nothing is writing to cannot have a row in flight in the first place.
func recheckHooks(
	cfg config.Config, boundary *marker, slot string, phase state.Phase,
) (verify.Boundary, verify.WaitApplied) {
	if phase != state.PhaseFollow {
		return nil, nil
	}
	wait := func(ctx context.Context, position string) error {
		waitCtx, cancel := context.WithTimeout(ctx, recheckWaitTimeout)
		defer cancel()
		if err := waitTargetProgress(waitCtx, cfg.Target, slot, position); err != nil {
			return fmt.Errorf(
				"%w: the pgmigrate run process may not be applying changes. Check its log, and whether it is still running",
				err,
			)
		}
		return nil
	}
	return boundary.emit, wait
}

// recheckWaitTimeout bounds how long a recheck waits for apply. Reaching it means
// the run process is not applying, which is a fault to report rather than to wait
// out.
const recheckWaitTimeout = 2 * time.Minute

// verifyProgress reports what verification is doing three ways: a live line for
// whoever is watching, structured events in the migration log, and durable rows a
// separate pgmigrate status can read.
type verifyProgress struct {
	dir   string
	store *state.Store
	out   io.Writer
	// interactive selects the single rewritten line. A file or pipe gets whole
	// lines at a slower cadence instead, because a log full of carriage returns is
	// unreadable.
	interactive bool
	oids        map[string]uint32

	mu       sync.Mutex
	reported map[string]time.Time
	stages   map[string]verify.Stage
	// merged holds each table's latest figures. A table's stages report different
	// subsets of them — only the sampling pass counts pages — so the durable row is
	// assembled here rather than by whichever stage reported last.
	merged map[string]*state.VerifyTable
	dirty  bool
}

// progressInterval rate-limits reporting. Windows can complete many times a second,
// and neither a terminal nor a SQLite write per window is worth that.
const progressInterval = 500 * time.Millisecond

func newVerifyProgress(cfg config.Config, store *state.Store, out io.Writer, tables []state.Table) *verifyProgress {
	oids := make(map[string]uint32, len(tables))
	for _, table := range tables {
		oids[table.Schema+"."+table.Name] = table.OID
	}
	interactive := false
	if file, ok := out.(*os.File); ok {
		if info, err := file.Stat(); err == nil {
			interactive = info.Mode()&os.ModeCharDevice != 0
		}
	}
	return &verifyProgress{
		dir: cfg.Dir, store: store, out: out, interactive: interactive, oids: oids,
		reported: make(map[string]time.Time),
		stages:   make(map[string]verify.Stage),
		merged:   make(map[string]*state.VerifyTable),
	}
}

func (p *verifyProgress) Update(update verify.Progress) {
	key := update.Table + " " + string(update.Side)
	p.mu.Lock()
	changed := p.stages[key] != update.Stage
	due := changed || update.Stage == verify.StageDone ||
		time.Since(p.reported[key]) >= progressInterval
	if due {
		p.stages[key] = update.Stage
		p.reported[key] = time.Now()
		p.dirty = true
	}
	record := p.record(update)
	p.mu.Unlock()
	if !due {
		return
	}
	p.render(update)
	logEvent(p.dir, "verify_progress", map[string]any{
		"table": update.Table, "side": update.Side, "stage": update.Stage,
		"pages": update.Pages, "pages_total": update.PagesTotal, "rows": update.Rows,
		"estimated_rows": update.Estimated, "target_rows": update.TargetRows,
		"rows_per_second": update.Rate, "eta": update.ETA.String(),
		"coverage": update.Coverage, "candidate_rows": update.Candidates,
		"cdc_keys": update.CDCKeys, "cdc_observed": update.CDCObserved,
		"cdc_pending_rows": update.CDCPending,
		"unresolved":       update.Unresolved,
	})
	if p.store == nil || record == nil {
		return
	}
	_ = p.store.UpsertVerifyTable(context.WithoutCancel(context.Background()), *record)
}

// record merges one observation into the table's durable row and returns a copy of
// it. The caller must hold the lock.
func (p *verifyProgress) record(update verify.Progress) *state.VerifyTable {
	oid, known := p.oids[update.Table]
	if !known {
		return nil
	}
	into, seen := p.merged[update.Table]
	if !seen {
		into = &state.VerifyTable{TableOID: oid}
		p.merged[update.Table] = into
	}
	into.Stage = string(update.Stage)
	// Only an observation of the source carries page counts. The rechecking and
	// done stages report no side, and must not zero what the sampling stage
	// recorded.
	if update.Side == verify.Side("source") {
		into.SourcePages, into.SourcePagesTotal = update.Pages, update.PagesTotal
	}
	if update.Rows > 0 {
		into.Sampled = update.Rows
	}
	if update.Estimated > 0 {
		into.Estimated = update.Estimated
	}
	if update.TargetRows > 0 {
		into.TargetRows = update.TargetRows
	}
	if update.Rate > 0 {
		into.Rate, into.ETA = update.Rate, update.ETA
	}
	// The CDC stage reports its own two figures and nothing else, deliberately,
	// because its keys are not sampled rows. So an observation without coverage
	// leaves the sample's coverage alone rather than reading, for as long as that
	// stage runs, as a table nothing was read from.
	if update.Coverage > 0 {
		into.Coverage = update.Coverage
	}
	if update.Candidates > 0 {
		into.Candidates = int64(update.Candidates)
	}
	if update.CDCKeys > 0 {
		into.CDCKeys = update.CDCKeys
	}
	if update.CDCObserved > 0 {
		into.CDCObserved = update.CDCObserved
	}
	into.Unresolved = int64(update.Unresolved)
	into.Converged, into.Complete = update.Converged, update.Complete
	copied := *into
	return &copied
}

func (p *verifyProgress) render(update verify.Progress) {
	if p.out == nil {
		return
	}
	if update.Stage == verify.StageCDCDeferred || update.Stage == verify.StageCDCRechecking {
		p.line(fmt.Sprintf("verify %s: %s, %d CDC rows pending (not final divergence)",
			update.Table, update.Stage, update.CDCPending))
		return
	}
	// The CDC stratum checks rows the heap sample cannot reach, so it reports its
	// own two numbers. Falling through to the line below would repeat the sample's
	// counts and read as progress through a sample that has already finished.
	if update.Stage == verify.StageCDC {
		p.line(fmt.Sprintf("verify %s: %s, %s of %s applied rows",
			update.Table, update.Stage,
			compactCount(update.CDCKeys), compactCount(update.CDCObserved)))
		return
	}
	// The sampled figure is shown against the whole table rather than against the
	// pages the windows cover. Read against the windows it would always reach 100%
	// and would read as a claim about the table that the check never made.
	line := fmt.Sprintf("verify %s: %s %s of %s rows (%.1f%%), %s pages read, %s",
		update.Table, update.Stage,
		compactCount(update.Rows), compactCount(max(update.Estimated, update.Rows)),
		100*update.Coverage, compactCount(update.Pages),
		renderRate(update.Rate, update.ETA))
	if update.Candidates > 0 {
		line += fmt.Sprintf(", %d rows to recheck", update.Candidates)
	}
	p.line(line)
}

// line writes one progress line, over the previous one when a terminal is
// watching and on its own line when the output is a log.
func (p *verifyProgress) line(text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.interactive {
		fmt.Fprintf(p.out, "\r\033[2K%s", text)
		return
	}
	fmt.Fprintln(p.out, text)
}

// finish closes the live line so whatever prints next starts cleanly.
func (p *verifyProgress) finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.interactive && p.dirty && p.out != nil {
		fmt.Fprintln(p.out)
	}
}

func renderRate(rate float64, eta time.Duration) string {
	if rate <= 0 {
		return "measuring rate"
	}
	if eta <= 0 {
		return compactCount(int64(rate)) + " rows/s"
	}
	return fmt.Sprintf("%s rows/s, eta %s", compactCount(int64(rate)), eta.Round(time.Second))
}

// compactCount renders large counts readably, because the numbers verify deals in
// run to nine digits.
func compactCount(value int64) string {
	switch {
	case value >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(value)/1e9)
	case value >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(value)/1e6)
	case value >= 10_000:
		return fmt.Sprintf("%.0fk", float64(value)/1e3)
	default:
		return fmt.Sprint(value)
	}
}

// verificationWarnings renders what a run could not promise, so a keyless table or
// an unlocalized divergence is not buried in the JSON result.
func verificationWarnings(result verify.Result) string {
	if len(result.Warnings) == 0 {
		return ""
	}
	var builder strings.Builder
	for _, warning := range result.Warnings {
		builder.WriteString("warning: " + warning + "\n")
	}
	return builder.String()
}

// verificationSummary says what a clean run established, which is less than "the
// two databases match": a number of rows out of a larger number. The least covered
// table is named because it is the weakest claim in the run, and the average would
// hide it.
func verificationSummary(result verify.Result) string {
	var (
		compared, skipped, ignored int
		sampled, estimated         int64
		leastName                  string
		least                      = 1.0
	)
	for _, table := range result.Tables {
		if len(table.Table.Key.Columns) == 0 {
			skipped++
			continue
		}
		compared++
		ignored += table.IgnoredRows
		sampled += table.Source.Rows
		// An unanalyzed table estimates zero rows, and a stale estimate can be
		// under what was read. Neither is a denominator.
		estimated += max(table.Source.Estimated, table.Source.Rows)
		if table.Coverage < least {
			least, leastName = table.Coverage, table.Table.Schema+"."+table.Table.Name
		}
	}
	if compared == 0 {
		return fmt.Sprintf("verified nothing: none of the %d tables has a key to check by\n", skipped)
	}
	line := fmt.Sprintf("verified %d tables: %s of %s rows sampled, %s, %d divergent",
		compared, compactCount(sampled), compactCount(estimated),
		cdcSummary(result), len(result.DivergedTables()))
	if leastName != "" {
		line += fmt.Sprintf(" (least covered %s, %.2f%%)", leastName, 100*least)
	}
	if skipped > 0 {
		line += fmt.Sprintf(", %d not compared for want of a key", skipped)
	}
	if ignored > 0 {
		line += fmt.Sprintf("; %d mismatches ignored by application scope (audited)", ignored)
	}
	return line + "\n"
}

// cdcSummary reports the CDC stratum separately from the heap sample, because
// they check different rows and only one of them says anything about
// replication.
//
// An empty reservoir says so in words. Rendering it as "0 CDC keys checked" would
// read like a clean result for the replication path, when it means the path was
// not looked at: no applier has run since the reservoir was added, or it was
// turned off.
func cdcSummary(result verify.Result) string {
	keys, observed := result.CDCKeys(), result.CDCObserved()
	if observed == 0 {
		return "no applied rows recorded, so the replication path was not checked"
	}
	line := fmt.Sprintf("%s of %s applied rows checked",
		compactCount(keys), compactCount(observed))
	if deletes := result.CDCDeletes(); deletes > 0 {
		line += fmt.Sprintf(" (%s recorded delete keys; target-only rows ignored)", compactCount(deletes))
	}
	var changed, pending, advanced int
	for _, table := range result.Tables {
		changed += table.CDC.SourceChanged
		pending += table.CDC.Pending
		advanced += table.CDC.Advanced
	}
	if changed > 0 {
		line += fmt.Sprintf("; %d source-changed CDC rows checked for convergence or target advancement", changed)
	}
	if advanced > 0 {
		line += fmt.Sprintf("; %d CDC target rows advanced without matching; accepted as progressing", advanced)
	}
	if pending > 0 {
		line += fmt.Sprintf("; %d CDC rows still pending", pending)
	}
	return line
}

// verificationDivergence explains each divergence in the terms an operator has to
// act on: how many rows differ, out of how many were looked at, and which way they
// differ. A count alone needed hand arithmetic to interpret.
func verificationDivergence(result verify.Result) string {
	var parts []string
	for _, table := range result.Tables {
		if table.Converged {
			continue
		}
		name := table.Table.Schema + "." + table.Table.Name
		if len(table.Unresolved) == 0 {
			reason := table.CutShort
			if reason == "" {
				reason = "did not converge"
			}
			parts = append(parts, name+": "+reason)
			continue
		}
		detail := fmt.Sprintf("%s: %d of %s sampled rows still differ (%s)",
			name, len(table.Unresolved), compactCount(table.Source.Rows),
			diffKinds(table.Unresolved))
		if table.InFlight > 0 {
			detail += fmt.Sprintf("; %d rows settled while rechecking", table.InFlight)
		}
		parts = append(parts, detail)
	}
	return strings.Join(parts, "; ")
}

// diffKinds counts the directions a table's rows differ in, because which
// direction it is points at the cause: a missing row is an apply that did not
// happen, a differing one an apply that happened wrongly.
func diffKinds(rows []verify.RowDiff) string {
	counts := make(map[verify.DiffKind]int, 2)
	for _, row := range rows {
		counts[row.Kind]++
	}
	var parts []string
	for _, kind := range []verify.DiffKind{
		verify.DiffSourceOnly, verify.DiffDifferent, verify.DiffTargetStalled,
	} {
		if counts[kind] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[kind], kind))
		}
	}
	return strings.Join(parts, ", ")
}

// verificationRows renders the rows a divergence was localized to, which is what an
// operator acts on: a name they can go and look at, rather than a table-level "does
// not match".
func verificationRows(result verify.Result) string {
	var builder strings.Builder
	for _, table := range result.Tables {
		for _, row := range table.Unresolved {
			fmt.Fprintf(&builder, "%s.%s %s: %s\n",
				table.Table.Schema, table.Table.Name, row.Kind, strings.Join(row.Key, ", "))
		}
	}
	return builder.String()
}

// errVerificationIncomplete reports a verification that stopped before comparing
// everything it was asked to.
var errVerificationIncomplete = errors.New("verification did not compare every table in full")
