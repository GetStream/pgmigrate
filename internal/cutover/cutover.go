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

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/state"
	"github.com/tgross/pgmigrate/internal/verify"
)

const (
	stepWriteCheck  = "cutover.write_check"
	stepEndPosition = "cutover.end_position"
	stepDrain       = "cutover.drain"
	stepVerify      = "cutover.verify"
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

// ActivitySample records source writes observed during a quiet-period sample.
type ActivitySample struct {
	StartedAt  time.Time     `json:"started_at"`
	EndedAt    time.Time     `json:"ended_at"`
	Interval   time.Duration `json:"interval"`
	Before     int64         `json:"before"`
	After      int64         `json:"after"`
	Writes     int64         `json:"writes"`
	Overridden bool          `json:"overridden"`
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
	Activity      *ActivitySample     `json:"activity,omitempty"`
	Verification  *verify.Result      `json:"verification,omitempty"`
	// VerifiedFraction is the share of the rows in the checked tables that
	// verification actually compared. It is recorded on its own because it is the
	// size of what the gate below established, and verification samples: reading
	// this as a promise that the copy is sound would be reading it as more than it
	// is.
	VerifiedFraction float64          `json:"verified_fraction"`
	Sequences        []SequenceResult `json:"sequences,omitempty"`
	Steps            []state.Step     `json:"steps"`
}

// ReportConfiguration captures the safety-relevant cutover settings.
type ReportConfiguration struct {
	SampleInterval time.Duration     `json:"sample_interval"`
	AllowWrites    bool              `json:"allow_writes"`
	SequenceOffset int64             `json:"sequence_offset"`
	Values         map[string]string `json:"values,omitempty"`
}

// Config controls the ordered, rerunnable cutover steps.
type Config struct {
	Source, Target Connector
	State          State
	Dir            string
	SampleInterval time.Duration
	AllowWrites    bool
	SequenceOffset int64
	Sequences      []Sequence
	WaitDrain      func(context.Context, string) error
	Verify         func(context.Context) (verify.Result, error)
	Cleanup        func(context.Context) error
	Now            func() time.Time
	Sleep          func(context.Context, time.Duration) error
	ToolVersion    string
	AuditConfig    map[string]string

	// SampleActivity and ReadFlushLSN are test seams. Production callers should
	// leave them nil to use source PostgreSQL.
	SampleActivity func(context.Context) (ActivitySample, error)
	ReadFlushLSN   func(context.Context) (string, error)
	EmitBoundary   func(context.Context) (string, error)
}

// ErrWritesObserved prevents unsafe cutover while source writes continue.
var ErrWritesObserved = errors.New("source writes observed during cutover sample")

// ErrVerificationFailed is the hard gate before sequence changes.
//
// It is not an authority on whether the copy is sound, and must not be read as
// one: verification samples each table, so passing this gate means the rows it
// compared agreed. What it does catch is a named divergence, which is worth
// refusing a cutover over.
var ErrVerificationFailed = errors.New("verification failed")

// ErrVerificationExecution means verification could not produce a verdict at all.
// The application must recover follow before source writes resume.
var ErrVerificationExecution = errors.New("verification execution failed")

// Run executes incomplete steps in safety order. A marker is written only after
// its action succeeds, so process restarts safely resume from the first missing
// marker.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if cfg.Source == nil || cfg.Target == nil || cfg.State == nil ||
		cfg.WaitDrain == nil || cfg.Verify == nil || cfg.Cleanup == nil ||
		strings.TrimSpace(cfg.Dir) == "" {
		return Report{}, errors.New("source, target, state, directory, drain, verify, and cleanup are required")
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 5 * time.Second
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
	if cfg.Sleep == nil {
		cfg.Sleep = sleep
	}
	if cfg.SampleActivity == nil {
		cfg.SampleActivity = func(ctx context.Context) (ActivitySample, error) {
			return sampleWrites(ctx, cfg)
		}
	}
	if cfg.ReadFlushLSN == nil {
		cfg.ReadFlushLSN = func(ctx context.Context) (string, error) {
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
		if _, tracksPhases := cfg.State.(phaseState); tracksPhases {
			migration, err := cfg.State.Migration(ctx)
			if err != nil {
				return Report{}, err
			}
			if migration.Phase != state.PhaseComplete {
				if _, err := cfg.SampleActivity(ctx); err != nil {
					return Report{}, fmt.Errorf("%s: %w", stepWriteCheck, err)
				}
				currentLSN, err := cfg.ReadFlushLSN(ctx)
				if err != nil {
					return Report{}, fmt.Errorf("read source flush position: %w", err)
				}
				if advanced, err := lsnAfter(currentLSN, migration.EndPosition); err != nil {
					return Report{}, err
				} else if advanced {
					return Report{}, fmt.Errorf("%w: source WAL advanced from cutover end %s to %s",
						ErrWritesObserved, migration.EndPosition, currentLSN)
				}
				if err := transitionOptional(ctx, cfg.State, state.PhaseComplete); err != nil {
					return Report{}, err
				}
			}
		}
		return readReport(cfg.Dir)
	}
	report := Report{
		Version:     1,
		ToolVersion: cfg.ToolVersion,
		Configuration: ReportConfiguration{
			SampleInterval: cfg.SampleInterval,
			AllowWrites:    cfg.AllowWrites,
			SequenceOffset: cfg.SequenceOffset,
			Values:         cloneStrings(cfg.AuditConfig),
		},
	}

	// Unlike ordinary steps, write-freeze validation is intentionally repeated
	// on every incomplete command invocation. Its marker is audit-only.
	sample, err := cfg.SampleActivity(ctx)
	report.Activity = &sample
	if err != nil {
		return Report{}, fmt.Errorf("%s: %w", stepWriteCheck, err)
	}
	if err := runStep(ctx, cfg.State, stepWriteCheck, func() (string, error) {
		data, marshalErr := json.Marshal(sample)
		return string(data), marshalErr
	}); err != nil {
		return Report{}, err
	}

	// Sample again directly before observing the flush position. This closes the
	// resume window between the invocation-level check and end-position reuse.
	sample, err = cfg.SampleActivity(ctx)
	report.Activity = &sample
	if err != nil {
		return Report{}, fmt.Errorf("%s: %w", stepEndPosition, err)
	}
	migration, err := cfg.State.Migration(ctx)
	if err != nil {
		return Report{}, err
	}
	report.EndPosition = migration.EndPosition
	if report.EndPosition == "" {
		emit := cfg.ReadFlushLSN
		if cfg.EmitBoundary != nil {
			emit = cfg.EmitBoundary
		}
		report.EndPosition, err = emit(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("emit cutover boundary: %w", err)
		}
		if err := cfg.State.SetEndPosition(ctx, report.EndPosition); err != nil {
			return Report{}, err
		}
	} else {
		currentLSN, err := cfg.ReadFlushLSN(ctx)
		if err != nil {
			return Report{}, fmt.Errorf("read source flush position: %w", err)
		}
		if advanced, err := lsnAfter(currentLSN, report.EndPosition); err != nil {
			return Report{}, err
		} else if advanced {
			return Report{}, fmt.Errorf("%w: source WAL advanced from cutover end %s to %s",
				ErrWritesObserved, report.EndPosition, currentLSN)
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

	if err := runStep(ctx, cfg.State, stepVerify, func() (string, error) {
		result, err := cfg.Verify(ctx)
		report.Verification = &result
		report.VerifiedFraction = result.SampledFraction()
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrVerificationExecution, err)
		}
		// A check that stopped early is a gate that never closed, not data that
		// disagreed, and cutover must refuse it for a different stated reason.
		if cut := result.CutShort(); len(cut) > 0 {
			return "", fmt.Errorf("%w: verification stopped before checking every table: %s",
				ErrVerificationFailed, strings.Join(cut, "; "))
		}
		if !result.Converged {
			return "", fmt.Errorf("%w: %s", ErrVerificationFailed, strings.Join(result.DivergedTables(), ", "))
		}
		data, _ := json.Marshal(result)
		return string(data), nil
	}); err != nil {
		return Report{}, err
	}
	if err := transitionOptional(ctx, cfg.State, state.PhaseCutover); err != nil {
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

func lsnAfter(current, end string) (bool, error) {
	currentLSN, err := pglogrepl.ParseLSN(current)
	if err != nil {
		return false, fmt.Errorf("parse current source LSN %q: %w", current, err)
	}
	endLSN, err := pglogrepl.ParseLSN(end)
	if err != nil {
		return false, fmt.Errorf("parse cutover end LSN %q: %w", end, err)
	}
	return currentLSN > endLSN, nil
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
		switch step.Name {
		case stepWriteCheck:
			if report.Activity == nil {
				var value ActivitySample
				if json.Unmarshal([]byte(step.Detail), &value) == nil {
					report.Activity = &value
				}
			}
		case stepVerify:
			if report.Verification == nil {
				var value verify.Result
				if json.Unmarshal([]byte(step.Detail), &value) == nil {
					report.Verification = &value
					report.VerifiedFraction = value.SampledFraction()
				}
			}
		case stepSequences:
			if report.Sequences == nil {
				_ = json.Unmarshal([]byte(step.Detail), &report.Sequences)
			}
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

func sampleWrites(ctx context.Context, cfg Config) (ActivitySample, error) {
	conn, err := cfg.Source(ctx)
	if err != nil {
		return ActivitySample{}, err
	}
	defer conn.Close(context.Background())
	sample := ActivitySample{StartedAt: cfg.Now(), Interval: cfg.SampleInterval, Overridden: cfg.AllowWrites}
	if _, err := conn.Exec(ctx, "SELECT pg_catalog.pg_stat_clear_snapshot()"); err != nil {
		return sample, fmt.Errorf("clear initial source statistics snapshot: %w", err)
	}
	if err := conn.QueryRow(ctx, writeCounterSQL).Scan(&sample.Before); err != nil {
		return sample, fmt.Errorf("read initial source write counter: %w", err)
	}
	if err := cfg.Sleep(ctx, cfg.SampleInterval); err != nil {
		return sample, err
	}
	if _, err := conn.Exec(ctx, "SELECT pg_catalog.pg_stat_clear_snapshot()"); err != nil {
		return sample, fmt.Errorf("clear final source statistics snapshot: %w", err)
	}
	if err := conn.QueryRow(ctx, writeCounterSQL).Scan(&sample.After); err != nil {
		return sample, fmt.Errorf("read final source write counter: %w", err)
	}
	sample.EndedAt = cfg.Now()
	sample.Writes = sample.After - sample.Before
	if sample.Writes < 0 {
		// Statistics were reset during the sample; treat that as unsafe activity.
		sample.Writes = 1
	}
	if sample.Writes > 0 && !cfg.AllowWrites {
		return sample, fmt.Errorf("%w: %d tuple changes in %s", ErrWritesObserved, sample.Writes, cfg.SampleInterval)
	}
	return sample, nil
}

const writeCounterSQL = `
	SELECT (tup_inserted+tup_updated+tup_deleted)::bigint
	FROM pg_catalog.pg_stat_database WHERE datname=current_database()`

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

func sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

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
