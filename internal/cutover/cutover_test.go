package cutover

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/state"
)

type resumeState struct {
	mu          sync.Mutex
	steps       map[string]bool
	endPosition string
	failReport  bool
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

func (s *resumeState) SetEndPosition(_ context.Context, position string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endPosition = position
	return nil
}

func (s *resumeState) Migration(context.Context) (state.Migration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return state.Migration{EndPosition: s.endPosition}, nil
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
		stepEndPosition: true, stepDrain: true,
		stepSequences: true, stepCleanup: true,
	}
	store := &resumeState{steps: steps, endPosition: "0/123", failReport: true}
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	connector := func(context.Context) (*pgx.Conn, error) {
		t.Fatal("completed step unexpectedly opened PostgreSQL")
		return nil, nil
	}
	cfg := Config{
		Source: connector, Target: connector, State: store, Dir: t.TempDir(),
		EmitBoundary: func(context.Context) (string, error) {
			t.Fatal("a recorded end position was unexpectedly emitted again")
			return "", nil
		},
		WaitDrain: func(context.Context, string) error {
			t.Fatal("completed drain unexpectedly reran")
			return nil
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

// The boundary is emitted once and then reused for the life of the migration.
// A second attempt that emitted a fresh one would move the line between what
// this cutover migrated and what it abandoned, after the target had already been
// drained to the first.
func TestResumeReusesTheBoundaryTheFirstAttemptEmitted(t *testing.T) {
	t.Parallel()
	// Sequences and cleanup start marked done because they need PostgreSQL and this
	// test is about the boundary. Nothing before them is marked, so both attempts
	// run the end-position and drain steps for real.
	store := &resumeState{steps: map[string]bool{stepSequences: true, stepCleanup: true}}
	emitted := 0
	drained := false
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(),
		EmitBoundary: func(context.Context) (string, error) {
			emitted++
			return "0/500", nil
		},
		WaitDrain: func(context.Context, string) error {
			if !drained {
				return errors.New("apply has not reached the end position")
			}
			return nil
		},
		Cleanup: func(context.Context) error { return nil },
	}
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("expected the first attempt to fail draining")
	}
	drained = true
	report, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("resume cutover: %v", err)
	}
	if emitted != 1 {
		t.Errorf("the boundary was emitted %d times, want once", emitted)
	}
	if report.EndPosition != "0/500" {
		t.Errorf("end position = %q, want the emitted 0/500", report.EndPosition)
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
		resumeState: resumeState{
			steps:       map[string]bool{stepEndPosition: true, stepSequences: true, stepCleanup: true},
			endPosition: "0/123",
		},
		phase: state.PhaseFollow,
	}
	cfg := Config{
		Source: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected source connection") },
		Target: func(context.Context) (*pgx.Conn, error) { return nil, errors.New("unexpected target connection") },
		State:  store, Dir: t.TempDir(), ToolVersion: "test-version",
		AuditConfig: map[string]string{"profile": "safe"},
		WaitDrain:   func(context.Context, string) error { return nil },
		Cleanup:     func(context.Context) error { return nil },
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
