package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/setup"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/singleflight"
)

const diagnosticsInterval = 5 * time.Second

type diagnosticsView struct {
	Revision  string          `json:"revision"`
	SampledAt time.Time       `json:"sampled_at"`
	SampleAge int64           `json:"sample_age_ms"`
	WAL       walView         `json:"wal"`
	Source    maintenanceView `json:"source"`
	Target    maintenanceView `json:"target"`
}

type walView struct {
	SourceLSN      string     `json:"source_lsn,omitempty"`
	StagedLSN      string     `json:"staged_lsn,omitempty"`
	AppliedLSN     string     `json:"applied_lsn,omitempty"`
	SourceAt       *time.Time `json:"source_sampled_at,omitempty"`
	CheckpointAt   *time.Time `json:"checkpoint_sampled_at,omitempty"`
	ApplyUpdatedAt *time.Time `json:"apply_updated_at,omitempty"`
	// Decimal strings preserve the full pg_lsn range in JavaScript and JSON.
	UncapturedBytes *string `json:"uncaptured_bytes"`
	ReplayBytes     *string `json:"replay_bytes"`
	TotalBytes      *string `json:"total_bytes"`
	Error           string  `json:"error,omitempty"`
}

type maintenanceView struct {
	Jobs       []maintenanceJob `json:"jobs"`
	Error      string           `json:"error,omitempty"`
	Restricted bool             `json:"restricted"`
	Truncated  bool             `json:"truncated"`
}

type maintenanceJob struct {
	PID            int      `json:"pid"`
	Kind           string   `json:"kind"`
	Command        string   `json:"command"`
	Phase          string   `json:"phase"`
	Table          string   `json:"table"`
	Index          string   `json:"index,omitempty"`
	ElapsedSeconds float64  `json:"elapsed_seconds"`
	WaitEvent      string   `json:"wait_event,omitempty"`
	LockerPID      int64    `json:"locker_pid,omitempty"`
	IndexCycles    int64    `json:"index_cycles,omitempty"`
	Done           int64    `json:"done,string"`
	Total          int64    `json:"total,string"`
	Unit           string   `json:"unit,omitempty"`
	Percent        *float64 `json:"percent,omitempty"`
}

// Diagnostics never participate in replay, acknowledgement or durable state.
// One in-flight sample and one five-second cache are shared by all browser tabs.
type diagnosticsCache struct {
	mu     sync.Mutex
	value  diagnosticsView
	flight singleflight.Group
}

func (c *diagnosticsCache) sample(ctx context.Context, revision string, collect func() diagnosticsView) (diagnosticsView, error) {
	result := c.flight.DoChan(revision, func() (any, error) {
		c.mu.Lock()
		cached := c.value
		c.mu.Unlock()
		if cached.Revision == revision && time.Since(cached.SampledAt) < diagnosticsInterval {
			return cached, nil
		}
		value := collect()
		value.Revision = revision
		c.mu.Lock()
		c.value = value
		c.mu.Unlock()
		return value, nil
	})
	select {
	case <-ctx.Done():
		return diagnosticsView{}, ctx.Err()
	case result := <-result:
		return result.Val.(diagnosticsView), result.Err
	}
}

func (s *Server) serveDiagnostics(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "controller token is missing or invalid")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.mu.Lock()
	cfg, revision := s.cfg, s.configurationRevisionLocked()
	s.mu.Unlock()
	view, err := s.diagnostics.sample(r.Context(), revision, func() diagnosticsView {
		ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
		defer cancel()
		return collectDiagnostics(ctx, cfg)
	})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "diagnostics request canceled")
		return
	}
	view.SampleAge = max(0, time.Since(view.SampledAt).Milliseconds())
	writeJSON(w, http.StatusOK, view)
}

func collectDiagnostics(ctx context.Context, cfg config.Config) diagnosticsView {
	view := diagnosticsView{SampledAt: time.Now().UTC()}
	var fingerprint string
	store, err := state.OpenReadOnly(ctx, cfg.Dir)
	if err == nil {
		snapshot, readErr := store.Snapshot(ctx)
		store.Close()
		if readErr == nil {
			at := time.Now().UTC()
			view.WAL.StagedLSN, view.WAL.AppliedLSN = snapshot.Apply.StagedLSN, snapshot.Apply.AppliedLSN
			view.WAL.CheckpointAt = &at
			if !snapshot.Apply.UpdatedAt.IsZero() {
				view.WAL.ApplyUpdatedAt = &snapshot.Apply.UpdatedAt
			}
			fingerprint = snapshot.Migration.SourceFingerprint
		} else {
			view.WAL.Error = "Durable checkpoints are unavailable"
		}
	} else if !errors.Is(err, state.ErrStateNotFound) {
		view.WAL.Error = "Durable checkpoints are unavailable"
	}

	// Each database gets its own bounded read-only connection; a slow source
	// must not hide target maintenance or delay the normal status endpoint.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		conn, err := diagnosticsConnect(ctx, cfg.Source)
		if err != nil {
			view.Source.Error = "Source diagnostics unavailable (connection or permission)"
			view.WAL.Error = view.Source.Error
			return
		}
		defer conn.Close(ctx)
		var position *string
		var systemID, database string
		var at time.Time
		err = conn.QueryRow(ctx, `SELECT
			CASE WHEN pg_catalog.pg_is_in_recovery() THEN NULL ELSE pg_catalog.pg_current_wal_lsn()::text END,
			pg_catalog.clock_timestamp(), system_identifier::text, pg_catalog.current_database()
			FROM pg_catalog.pg_control_system()`).Scan(&position, &at, &systemID, &database)
		switch {
		case err != nil:
			view.WAL.Error = "Source WAL unavailable (connection or permission)"
		case position == nil:
			view.WAL.Error = "Configured source is in recovery; primary WAL is unavailable"
		case fingerprint != "" && setup.SourceFingerprint(systemID, database) != fingerprint:
			view.WAL.Error = "Configured source does not match the migration's source identity"
		default:
			view.WAL.SourceLSN, view.WAL.SourceAt = *position, &at
		}
		view.Source = readMaintenance(ctx, conn)
	}()
	go func() {
		defer wg.Done()
		conn, err := diagnosticsConnect(ctx, cfg.Target)
		if err != nil {
			view.Target.Error = "Target diagnostics unavailable (connection or permission)"
			return
		}
		defer conn.Close(ctx)
		view.Target = readMaintenance(ctx, conn)
	}()
	wg.Wait()
	view.WAL.compare()
	return view
}

func diagnosticsConnect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("database is not configured")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnectTimeout = 2 * time.Second
	cfg.RuntimeParams["application_name"] = "pgmigrate-diagnostics"
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.RuntimeParams["statement_timeout"] = "1500"
	cfg.RuntimeParams["lock_timeout"] = "500"
	return pgx.ConnectConfig(ctx, cfg)
}

func (w *walView) compare() {
	w.UncapturedBytes, w.ReplayBytes, w.TotalBytes = nil, nil, nil
	staged, stagedErr := diagnosticLSN(w.StagedLSN)
	applied, appliedErr := diagnosticLSN(w.AppliedLSN)
	if stagedErr != nil || staged == 0 {
		return // Not initialized is unknown, not zero backlog.
	}
	applyKnown := appliedErr == nil && applied != 0
	if applyKnown && applied > staged {
		w.Error = "Checkpoints are out of order; lag is unavailable"
		return
	}
	decimal := func(value pglogrepl.LSN) *string { s := strconv.FormatUint(uint64(value), 10); return &s }
	if applyKnown {
		w.ReplayBytes = decimal(staged - applied)
	}
	source, err := diagnosticLSN(w.SourceLSN)
	if err != nil || source == 0 || w.Error != "" {
		return
	}
	if source < staged {
		w.Error = "Source WAL is behind the recorded capture checkpoint; comparison is unavailable"
		return
	}
	w.UncapturedBytes = decimal(source - staged)
	if applyKnown {
		w.TotalBytes = decimal(source - applied)
	}
}

func diagnosticLSN(value string) (pglogrepl.LSN, error) {
	high, low, ok := strings.Cut(value, "/")
	if !ok || len(high) < 1 || len(high) > 8 || len(low) < 1 || len(low) > 8 {
		return 0, errors.New("invalid LSN")
	}
	hi, hiErr := strconv.ParseUint(high, 16, 32)
	lo, loErr := strconv.ParseUint(low, 16, 32)
	if hiErr != nil || loErr != nil || strings.ContainsAny(high+low, "+-") {
		return 0, errors.New("invalid LSN")
	}
	return pglogrepl.LSN(hi<<32 | lo), nil
}

// JSON extraction allows PG16/17 to omit the PG18-only vacuum index counters.
// OIDs are resolved only in the connected database, never against another DB's
// catalog, and neither query text nor user data is returned.
const maintenanceSQL = `WITH jobs AS (
	SELECT pid, relid, index_relid, 'index' AS kind, pg_catalog.to_jsonb(p) AS progress
	FROM pg_catalog.pg_stat_progress_create_index p WHERE datname = pg_catalog.current_database()
	UNION ALL
	SELECT pid, relid, 0::oid, 'vacuum', pg_catalog.to_jsonb(p)
	FROM pg_catalog.pg_stat_progress_vacuum p WHERE datname = pg_catalog.current_database()
	UNION ALL
	SELECT pid, relid, 0::oid, 'vacuum_full', pg_catalog.to_jsonb(p)
	FROM pg_catalog.pg_stat_progress_cluster p
	WHERE datname = pg_catalog.current_database() AND command = 'VACUUM FULL'
)
SELECT j.pid, j.kind, j.progress,
	coalesce(pg_catalog.quote_ident(n.nspname)||'.'||pg_catalog.quote_ident(t.relname), ''),
	coalesce(pg_catalog.quote_ident(ni.nspname)||'.'||pg_catalog.quote_ident(i.relname), ''),
	coalesce(extract(epoch FROM pg_catalog.clock_timestamp()-a.query_start), 0)::float8,
	coalesce(a.wait_event_type||': '||a.wait_event, '')
FROM jobs j
LEFT JOIN pg_catalog.pg_class t ON t.oid=j.relid
LEFT JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace
LEFT JOIN pg_catalog.pg_class i ON i.oid=j.index_relid
LEFT JOIN pg_catalog.pg_namespace ni ON ni.oid=i.relnamespace
LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid=j.pid
ORDER BY j.pid LIMIT 101`

type maintenanceCounters struct {
	Command          string  `json:"command"`
	Phase            *string `json:"phase"`
	BlocksDone       int64   `json:"blocks_done"`
	BlocksTotal      int64   `json:"blocks_total"`
	TuplesDone       int64   `json:"tuples_done"`
	TuplesTotal      int64   `json:"tuples_total"`
	LockersDone      int64   `json:"lockers_done"`
	LockersTotal     int64   `json:"lockers_total"`
	CurrentLockerPID int64   `json:"current_locker_pid"`
	HeapScanned      int64   `json:"heap_blks_scanned"`
	HeapVacuumed     int64   `json:"heap_blks_vacuumed"`
	HeapTotal        int64   `json:"heap_blks_total"`
	IndexCycles      int64   `json:"index_vacuum_count"`
	IndexesDone      int64   `json:"indexes_processed"`
	IndexesTotal     int64   `json:"indexes_total"`
}

func readMaintenance(ctx context.Context, conn *pgx.Conn) maintenanceView {
	view := maintenanceView{Jobs: []maintenanceJob{}}
	rows, err := conn.Query(ctx, maintenanceSQL)
	if err != nil {
		view.Error = "Maintenance progress unavailable (connection or permission)"
		return view
	}
	defer rows.Close()
	for count := 0; rows.Next(); count++ {
		if count == 100 {
			view.Truncated = true
			break
		}
		var job maintenanceJob
		var raw []byte
		if err = rows.Scan(&job.PID, &job.Kind, &raw, &job.Table, &job.Index, &job.ElapsedSeconds, &job.WaitEvent); err != nil {
			break
		}
		var counters maintenanceCounters
		if err = json.Unmarshal(raw, &counters); err != nil {
			break
		}
		if counters.Phase == nil {
			view.Restricted = true
			continue
		}
		job.setProgress(counters)
		view.Jobs = append(view.Jobs, job)
	}
	if err != nil || rows.Err() != nil {
		view.Jobs = nil
		view.Error = "Maintenance progress unavailable (connection or permission)"
	}
	return view
}

func (j *maintenanceJob) setProgress(p maintenanceCounters) {
	j.Phase, j.Command, j.LockerPID, j.IndexCycles = *p.Phase, p.Command, p.CurrentLockerPID, p.IndexCycles
	switch j.Kind {
	case "index":
		switch {
		case strings.HasPrefix(j.Phase, "waiting for"):
			j.Done, j.Total, j.Unit = p.LockersDone, p.LockersTotal, "lockers"
		case j.Phase == "building index: loading tuples in tree":
			j.Done, j.Total, j.Unit = p.TuplesDone, p.TuplesTotal, "tuples"
		case j.Phase == "building index", j.Phase == "building index: scanning table", j.Phase == "index validation: scanning index", j.Phase == "index validation: scanning table":
			j.Done, j.Total, j.Unit = p.BlocksDone, p.BlocksTotal, "blocks"
			if j.Total == 0 && j.Phase == "building index" {
				j.Done, j.Total, j.Unit = p.TuplesDone, p.TuplesTotal, "tuples"
			}
		}
	case "vacuum":
		j.Command = "VACUUM / autovacuum"
		switch j.Phase {
		case "scanning heap":
			j.Done, j.Total, j.Unit = p.HeapScanned, p.HeapTotal, "heap blocks scanned"
		case "vacuuming heap":
			j.Done, j.Total, j.Unit = p.HeapVacuumed, p.HeapTotal, "heap blocks vacuumed"
		case "vacuuming indexes", "cleaning up indexes":
			j.Done, j.Total, j.Unit = p.IndexesDone, p.IndexesTotal, "indexes"
		}
	case "vacuum_full":
		if j.Phase == "seq scanning heap" {
			j.Done, j.Total, j.Unit = p.HeapScanned, p.HeapTotal, "heap blocks scanned"
		}
	}
	if j.Total > 0 && j.Done >= 0 && j.Done <= j.Total {
		percent := 100 * float64(j.Done) / float64(j.Total)
		j.Percent = &percent
	}
}

func (s *Server) progressScript(w http.ResponseWriter, _ *http.Request) {
	data, err := assets.ReadFile("progress.js")
	if err != nil {
		http.Error(w, "progress UI unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}
