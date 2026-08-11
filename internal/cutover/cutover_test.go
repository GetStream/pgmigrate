package cutover

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/state"
	"github.com/tgross/pgmigrate/internal/verify"
)

type resumeState struct {
	mu         sync.Mutex
	steps      map[string]bool
	failReport bool
}

func (s *resumeState) StepCompleted(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.steps[name], nil
}

func (s *resumeState) CompleteStep(_ context.Context, name, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if name == stepReport && s.failReport {
		s.failReport = false
		return errors.New("injected marker failure")
	}
	s.steps[name] = true
	return nil
}

func (*resumeState) SetEndPosition(context.Context, string) error { return nil }
func (*resumeState) Migration(context.Context) (state.Migration, error) {
	return state.Migration{EndPosition: "0/123"}, nil
}

func (s *resumeState) ListSteps(context.Context) ([]state.Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []state.Step
	for name, done := range s.steps {
		result = append(result, state.Step{Name: name, Completed: done})
	}
	return result, nil
}

func TestReportResumesAfterMarkerFailure(t *testing.T) {
	t.Parallel()
	steps := map[string]bool{
		stepWriteCheck: true, stepEndPosition: true, stepDrain: true,
		stepVerify: true, stepSequences: true, stepCleanup: true,
	}
	store := &resumeState{steps: steps, failReport: true}
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	connector := func(context.Context) (*pgx.Conn, error) {
		t.Fatal("completed step unexpectedly opened PostgreSQL")
		return nil, nil
	}
	cfg := Config{
		Source: connector, Target: connector, State: store, Dir: t.TempDir(),
		SampleActivity: func(context.Context) (ActivitySample, error) {
			return ActivitySample{}, nil
		},
		ReadFlushLSN: func(context.Context) (string, error) { return "0/123", nil },
		WaitDrain: func(context.Context, string) error {
			t.Fatal("completed drain unexpectedly reran")
			return nil
		},
		Verify: func(context.Context) (verify.Result, error) {
			t.Fatal("completed verification unexpectedly reran")
			return verify.Result{}, nil
		},
		Cleanup: func(context.Context) error {
			t.Fatal("completed cleanup unexpectedly reran")
			return nil
		},
		Now:         func() time.Time { return now },
		ToolVersion: "v1.2.3",
		AuditConfig: map[string]string{"migration": "test"},
	}
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("expected injected report marker failure")
	}
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resume cutover: %v", err)
	}
	if !report.CompletedAt.Equal(now) || report.EndPosition != "0/123" {
		t.Fatalf("resumed report = %#v", report)
	}
	if report.ToolVersion != "v1.2.3" || report.Configuration.Values["migration"] != "test" {
		t.Fatalf("report metadata = %#v", report)
	}
	if done, _ := store.StepCompleted(context.Background(), stepReport); !done {
		t.Fatal("report marker was not completed on resume")
	}
}

func TestResumeRerunsWriteFreezeAfterMarker(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{}}
	calls := 0
	resumed := false
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(),
		SampleActivity: func(context.Context) (ActivitySample, error) {
			calls++
			if resumed {
				return ActivitySample{Writes: 1}, ErrWritesObserved
			}
			if calls == 2 {
				return ActivitySample{}, errors.New("injected crash")
			}
			return ActivitySample{}, nil
		},
		ReadFlushLSN: func(context.Context) (string, error) { return "0/123", nil },
		WaitDrain:    func(context.Context, string) error { return nil },
		Verify:       func(context.Context) (verify.Result, error) { return verify.Result{Converged: true}, nil },
		Cleanup:      func(context.Context) error { return nil },
	}
	if _, err := Run(context.Background(), cfg); err == nil || !store.steps[stepWriteCheck] {
		t.Fatalf("first run error = %v, steps = %#v", err, store.steps)
	}
	resumed = true
	if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrWritesObserved) {
		t.Fatalf("resume error = %v, want ErrWritesObserved", err)
	}
}

func TestExistingEndPositionRejectsLaterSourceWAL(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{
		stepWriteCheck: true, stepEndPosition: true,
	}}
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(),
		SampleActivity: func(context.Context) (ActivitySample, error) {
			return ActivitySample{}, nil
		},
		ReadFlushLSN: func(context.Context) (string, error) { return "0/124", nil },
		WaitDrain:    func(context.Context, string) error { return nil },
		Verify:       func(context.Context) (verify.Result, error) { return verify.Result{Converged: true}, nil },
		Cleanup:      func(context.Context) error { return nil },
	}
	if _, err := Run(context.Background(), cfg); !errors.Is(err, ErrWritesObserved) {
		t.Fatalf("Run error = %v, want ErrWritesObserved", err)
	}
}

func TestVerifierExecutionErrorIsClassifiedForFollowRecovery(t *testing.T) {
	t.Parallel()
	store := &resumeState{steps: map[string]bool{
		stepWriteCheck: true, stepEndPosition: true, stepDrain: true,
	}}
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unused") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unused") },
		State:  store, Dir: t.TempDir(),
		SampleActivity: func(context.Context) (ActivitySample, error) {
			return ActivitySample{}, nil
		},
		ReadFlushLSN: func(context.Context) (string, error) { return "0/123", nil },
		WaitDrain:    func(context.Context, string) error { return nil },
		Verify:       func(context.Context) (verify.Result, error) { return verify.Result{}, errors.New("query timeout") },
		Cleanup:      func(context.Context) error { return nil },
	}
	_, err := Run(context.Background(), cfg)
	if !errors.Is(err, ErrVerificationExecution) {
		t.Fatalf("error = %v, want ErrVerificationExecution", err)
	}
}

type phaseStore struct {
	resumeState
	phase       state.Phase
	transitions []state.Phase
}

func (s *phaseStore) Migration(context.Context) (state.Migration, error) {
	return state.Migration{EndPosition: "0/123", Phase: s.phase}, nil
}

func (s *phaseStore) TransitionPhase(_ context.Context, phase state.Phase) error {
	s.phase = phase
	s.transitions = append(s.transitions, phase)
	return nil
}

func TestLifecycleTransitionsAndReportConfiguration(t *testing.T) {
	t.Parallel()
	store := &phaseStore{
		resumeState: resumeState{steps: map[string]bool{
			stepWriteCheck: true, stepEndPosition: true,
			stepSequences: true, stepCleanup: true,
		}},
		phase: state.PhaseFollow,
	}
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(), ToolVersion: "test-version",
		AuditConfig: map[string]string{"profile": "safe"},
		SampleActivity: func(context.Context) (ActivitySample, error) {
			return ActivitySample{}, nil
		},
		ReadFlushLSN: func(context.Context) (string, error) { return "0/123", nil },
		WaitDrain:    func(context.Context, string) error { return nil },
		Verify:       func(context.Context) (verify.Result, error) { return verify.Result{Converged: true}, nil },
		Cleanup:      func(context.Context) error { return nil },
	}
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []state.Phase{state.PhaseDrained, state.PhaseCutover, state.PhaseComplete}
	if len(store.transitions) != len(want) {
		t.Fatalf("transitions = %v", store.transitions)
	}
	for i := range want {
		if store.transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", store.transitions, want)
		}
	}
	if report.ToolVersion != "test-version" || report.Configuration.Values["profile"] != "safe" {
		t.Fatalf("report configuration = %#v", report)
	}
}
