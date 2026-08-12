// Package cutover provides a rerunnable, durably stepped migration cutover.
package cutover

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pgx/v5"
)

const (
	stepEndPosition = "cutover.end_position"
	stepDrain       = "cutover.drain"
	stepSequences   = "cutover.sequences"
	stepCleanup     = "cutover.cleanup"
	stepReport      = "cutover.report"
)

// Connector opens a PostgreSQL connection owned by the caller.
type Connector func(context.Context) (*pgx.Conn, error)

// State is the durable API used by Service.
type State interface {
	StepCompleted(context.Context, string) (bool, error)
	CompleteStep(context.Context, string, string) error
	SetEndPosition(context.Context, string) error
	Migration(context.Context) (state.Migration, error)
	ListSteps(context.Context) ([]state.Step, error)
}

type phaseState interface {
	TransitionPhase(context.Context, state.Phase) error
}

// SequenceResult records one absolute target sequence synchronization.
type SequenceResult struct {
	Schema      string `json:"schema"`
	Name        string `json:"name"`
	SourceValue int64  `json:"source_value"`
	TargetValue int64  `json:"target_value"`
	IsCalled    bool   `json:"is_called"`
}

// Sequence identifies one selected sequence eligible for synchronization.
type Sequence struct {
	Schema string
	Name   string
}

// Report is the atomic cutover audit artifact.
type Report struct {
	Version       int                 `json:"version"`
	ToolVersion   string              `json:"tool_version"`
	Configuration ReportConfiguration `json:"configuration"`
	CompletedAt   time.Time           `json:"completed_at"`
	EndPosition   string              `json:"end_position"`
	Sequences     []SequenceResult    `json:"sequences,omitempty"`
	Steps         []state.Step        `json:"steps"`
}

// ReportConfiguration captures the settings that shaped this cutover.
type ReportConfiguration struct {
	SequenceOffset int64             `json:"sequence_offset"`
	Values         map[string]string `json:"values,omitempty"`
}

// Config controls the ordered, rerunnable cutover steps.
//
// Cutover moves data and metadata; it does not judge either. It neither samples
// the source for writes nor verifies the copy, so freezing application writes and
// deciding that the data is good enough to serve are the operator's, done before
// this runs. Nothing here will stop a cutover that loses writes made after the
// end position.
type Config struct {
	Source, Target Connector
	State          State
	Dir            string
	SequenceOffset int64
	Sequences      []Sequence
	WaitDrain      func(context.Context, string) error
	Cleanup        func(context.Context) error
	Now            func() time.Time
	ToolVersion    string
	AuditConfig    map[string]string

	// EmitBoundary is a test seam, and the hook production uses to mark the end
	// position in the stream itself rather than inferring it. Left nil it falls
	// back to the source's current flush position.
	EmitBoundary func(context.Context) (string, error)
}

// Run executes incomplete steps in order. A marker is written only after its
// action succeeds, so process restarts safely resume from the first missing
// marker.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if cfg.Source == nil || cfg.Target == nil || cfg.State == nil ||
		cfg.WaitDrain == nil || cfg.Cleanup == nil || strings.TrimSpace(cfg.Dir) == "" {
		return Report{}, errors.New("source, target, state, directory, drain, and cleanup are required")
	}
	if cfg.SequenceOffset == 0 {
		cfg.SequenceOffset = 1000
	}
	if cfg.SequenceOffset < 0 {
		return Report{}, errors.New("sequence offset must not be negative")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.EmitBoundary == nil {
		cfg.EmitBoundary = func(ctx context.Context) (string, error) {
			conn, err := cfg.Source(ctx)
			if err != nil {
				return "", err
			}
			defer conn.Close(context.Background())
			var lsn string
			err = conn.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_flush_lsn()::text").Scan(&lsn)
			return lsn, err
		}
	}
	done, err := cfg.State.StepCompleted(ctx, stepReport)
	if err != nil {
		return Report{}, err
	}
	if done {
		// A crash between publishing the report and recording the phase leaves a
		// finished cutover looking unfinished. Finish it and hand back what it wrote.
		if err := transitionOptional(ctx, cfg.State, state.PhaseComplete); err != nil {
			return Report{}, err
		}
		return readReport(cfg.Dir)
	}
	report := Report{
		Version:     1,
		ToolVersion: cfg.ToolVersion,
		Configuration: ReportConfiguration{
			SequenceOffset: cfg.SequenceOffset,
			Values:         cloneStrings(cfg.AuditConfig),
		},
	}

	migration, err := cfg.State.Migration(ctx)
	if err != nil {
		return Report{}, err
	}
	// An end position recorded by an earlier attempt is reused as it stands. It is
	// the point the target was already drained to, and moving it would silently
	// redefine what this cutover migrated.
	report.EndPosition = migration.EndPosition
	if report.EndPosition == "" {
		report.EndPosition, err = cfg.EmitBoundary(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("emit cutover boundary: %w", err)
		}
		if err := cfg.State.SetEndPosition(ctx, report.EndPosition); err != nil {
			return Report{}, err
		}
	}
	if err := runStep(ctx, cfg.State, stepEndPosition, func() (string, error) {
		return report.EndPosition, nil
	}); err != nil {
		return Report{}, err
	}

	if err := runStep(ctx, cfg.State, stepDrain, func() (string, error) {
		return report.EndPosition, cfg.WaitDrain(ctx, report.EndPosition)
	}); err != nil {
		return Report{}, err
	}
	if err := transitionOptional(ctx, cfg.State, state.PhaseDrained); err != nil {
		return Report{}, err
	}

	if err := runStep(ctx, cfg.State, stepSequences, func() (string, error) {
		sequences, err := synchronizeSequences(ctx, cfg.Source, cfg.Target, cfg.SequenceOffset, cfg.Sequences)
		report.Sequences = sequences
		data, _ := json.Marshal(sequences)
		return string(data), err
	}); err != nil {
		return Report{}, err
	}
	// Advancing the target's sequences is where a cutover stops being reversible,
	// so that is where the phase says one is under way.
	if err := transitionOptional(ctx, cfg.State, state.PhaseCutover); err != nil {
		return Report{}, err
	}

	if err := runStep(ctx, cfg.State, stepCleanup, func() (string, error) {
		return "source replication objects removed", cfg.Cleanup(ctx)
	}); err != nil {
		return Report{}, err
	}

	done, err = cfg.State.StepCompleted(ctx, stepReport)
	if err != nil {
		return Report{}, err
	}
	if done {
		if err := transitionOptional(ctx, cfg.State, state.PhaseComplete); err != nil {
			return Report{}, err
		}
		return readReport(cfg.Dir)
	}
	// A crash can occur after the atomic rename but before the SQLite marker.
	// Adopt only a valid report and finish that one missing marker.
	if existing, readErr := readReport(cfg.Dir); readErr == nil {
		if err := cfg.State.CompleteStep(ctx, stepReport, "cutover-report.json"); err != nil {
			return Report{}, err
		}
		if err := transitionOptional(ctx, cfg.State, state.PhaseComplete); err != nil {
			return Report{}, err
		}
		return existing, nil
	}
	report.CompletedAt = cfg.Now()
	report.Steps, err = cfg.State.ListSteps(ctx)
	if err != nil {
		return Report{}, err
	}
	hydrateReportDetails(&report)
	// Include the report action in the artifact even though its durable marker
	// can only be committed after the atomically published file.
	report.Steps = append(report.Steps, state.Step{Name: stepReport, Detail: "cutover-report.json", Completed: true, CompletedAt: report.CompletedAt})
	if err := writeReportAtomic(cfg.Dir, report); err != nil {
		return Report{}, err
	}
	if err := cfg.State.CompleteStep(ctx, stepReport, "cutover-report.json"); err != nil {
		return Report{}, err
	}
	if err := transitionOptional(ctx, cfg.State, state.PhaseComplete); err != nil {
		return Report{}, err
	}
	return report, nil
}

func transitionOptional(ctx context.Context, store State, phase state.Phase) error {
	phases, ok := store.(phaseState)
	if !ok {
		return nil
	}
	if err := phases.TransitionPhase(ctx, phase); err != nil {
		return fmt.Errorf("transition cutover phase to %s: %w", phase, err)
	}
	return nil
}

func cloneStrings(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func hydrateReportDetails(report *Report) {
	for _, step := range report.Steps {
		if !step.Completed || step.Detail == "" {
			continue
		}
		if step.Name == stepSequences && report.Sequences == nil {
			_ = json.Unmarshal([]byte(step.Detail), &report.Sequences)
		}
	}
}

func runStep(ctx context.Context, store State, name string, action func() (string, error)) error {
	done, err := store.StepCompleted(ctx, name)
	if err != nil {
		return err
	}
	if done {
		return nil
	}
	detail, err := action()
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if err := store.CompleteStep(ctx, name, detail); err != nil {
		return fmt.Errorf("complete %s: %w", name, err)
	}
	return nil
}

func synchronizeSequences(
	ctx context.Context,
	sourceConnect, targetConnect Connector,
	offset int64,
	selected []Sequence,
) ([]SequenceResult, error) {
	source, err := sourceConnect(ctx)
	if err != nil {
		return nil, err
	}
	defer source.Close(context.Background())
	target, err := targetConnect(ctx)
	if err != nil {
		return nil, err
	}
	defer target.Close(context.Background())
	rows, err := source.Query(ctx, `
		SELECT n.nspname,c.relname,s.seqincrement
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		JOIN pg_catalog.pg_sequence s ON s.seqrelid=c.oid
		WHERE c.relkind='S' AND n.nspname NOT IN ('pg_catalog','information_schema')
		  AND n.nspname !~ '^pg_toast'
		ORDER BY n.nspname,c.relname`)
	if err != nil {
		return nil, fmt.Errorf("inventory source sequences: %w", err)
	}
	type sequenceName struct {
		schema, name string
		increment    int64
	}
	allowed := make(map[string]bool, len(selected))
	for _, sequence := range selected {
		allowed[sequence.Schema+"\x00"+sequence.Name] = true
	}
	var names []sequenceName
	for rows.Next() {
		var name sequenceName
		if err := rows.Scan(&name.schema, &name.name, &name.increment); err != nil {
			rows.Close()
			return nil, err
		}
		if !allowed[name.schema+"\x00"+name.name] {
			continue
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	results := make([]SequenceResult, 0, len(names))
	for _, name := range names {
		var last int64
		var called bool
		qualified := QuoteIdentifier(name.schema, name.name)
		if err := source.QueryRow(ctx, "SELECT last_value::bigint,is_called FROM "+qualified).Scan(&last, &called); err != nil {
			return nil, fmt.Errorf("read source sequence %s: %w", qualified, err)
		}
		delta := offset
		if name.increment < 0 {
			delta = -offset
		}
		if (delta > 0 && last > math.MaxInt64-delta) || (delta < 0 && last < math.MinInt64-delta) {
			return nil, fmt.Errorf("sequence %s offset overflows bigint", qualified)
		}
		value := last + delta
		// regclass input receives a fully quoted name, preserving unusual names.
		if _, err := target.Exec(ctx, "SELECT pg_catalog.setval($1::regclass,$2,$3)", qualified, value, called); err != nil {
			return nil, fmt.Errorf("synchronize target sequence %s: %w", qualified, err)
		}
		results = append(results, SequenceResult{Schema: name.schema, Name: name.name, SourceValue: last, TargetValue: value, IsCalled: called})
	}
	return results, nil
}

// QuoteIdentifier safely quotes a PostgreSQL identifier or qualified name.
func QuoteIdentifier(parts ...string) string { return pgx.Identifier(parts).Sanitize() }

func reportPath(dir string) string { return filepath.Join(dir, "cutover-report.json") }

func readReport(dir string) (Report, error) {
	data, err := os.ReadFile(reportPath(dir))
	if err != nil {
		return Report{}, fmt.Errorf("read completed cutover report: %w", err)
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return Report{}, fmt.Errorf("decode completed cutover report: %w", err)
	}
	return report, nil
}

// ReadReport validates and returns the durable completed cutover artifact.
func ReadReport(dir string) (Report, error) {
	return readReport(dir)
}

func writeReportAtomic(dir string, report Report) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create cutover report directory: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode cutover report: %w", err)
	}
	data = append(data, '\n')
	tmp := reportPath(dir) + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create cutover report: %w", err)
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write cutover report: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync cutover report: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close cutover report: %w", err)
	}
	if err := os.Rename(tmp, reportPath(dir)); err != nil {
		return fmt.Errorf("publish cutover report: %w", err)
	}
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open cutover report directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync cutover report directory: %w", err)
	}
	ok = true
	return nil
}
