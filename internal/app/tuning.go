package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/preflight"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/GetStream/pgmigrate/internal/tuning"
)

// tuningStepPrefix names the steps that hold what target tuning has to undo.
// ALTER SYSTEM outlives the process, so these records are the only thing that
// can put a target back after a crash, and they are written before the change.
const tuningStepPrefix = "target_tuning."

// tuningFinding marks a target that is configured for a bulk load rather than
// for serving. It stays open until the settings are reverted, so status shows it.
const tuningFinding = "target-tuned-for-load"

// applyTuningPreflight fills in the tuning inputs of a preflight config, so that
// preflight reports the same plan the run would apply rather than a guess at it.
func applyTuningPreflight(cfg config.Config, into *preflight.Config) error {
	overrides, err := cfg.TuningOverrides()
	if err != nil {
		return err
	}
	into.SkipTargetTuning = cfg.SkipTargetTuning
	into.TuningOverrides = overrides
	into.TuningWorkers = cfg.Workers
	return nil
}

// tuningRecorder stores originals in the steps table, which already survives
// everything except an explicit base-copy reset.
type tuningRecorder struct{ store *state.Store }

func (r tuningRecorder) Recorded(ctx context.Context, name string) (bool, error) {
	return r.store.StepCompleted(ctx, tuningStepPrefix+name)
}

func (r tuningRecorder) Record(ctx context.Context, change tuning.Change) error {
	detail, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("encode tuning record for %s: %w", change.Name, err)
	}
	return r.store.CompleteStep(ctx, tuningStepPrefix+change.Name, string(detail))
}

// tuneTarget configures the target for the bulk load and returns the settings
// that the copy and index-build sessions should apply to their own connections.
//
// It is safe to call on a resume. Derive compares each setting against what the
// target currently reports, so a setting already moved plans nothing and its
// recorded original is never overwritten with a bulk-load value.
func tuneTarget(ctx context.Context, cfg config.Config, store *state.Store) (map[string]string, error) {
	if cfg.SkipTargetTuning {
		logEvent(cfg.Dir, "target_tuning", map[string]any{"skipped": true})
		return nil, nil
	}
	overrides, err := cfg.TuningOverrides()
	if err != nil {
		return nil, err
	}
	conn, err := postgres.Connect(ctx, cfg.Target)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())

	target, err := tuning.Observe(ctx, conn)
	if err != nil {
		return nil, tuningFailure(ctx, cfg, store, err)
	}
	plan, err := tuning.Derive(target, overrides, cfg.Workers)
	if err != nil {
		return nil, err
	}
	logEvent(cfg.Dir, "target_tuning_plan", map[string]any{
		"memory_bytes":     plan.MemoryBytes,
		"memory_estimated": plan.MemoryEstimated,
		"changes":          len(plan.Changes),
		"blocked":          plan.Blocked,
	})

	// Session settings are probed here so a refusal surfaces now rather than
	// inside an index-build worker later.
	sessionGUCs, sessionErr := tuning.ApplySession(ctx, conn, plan)
	if sessionErr != nil {
		if err := tuningFailure(ctx, cfg, store, sessionErr); err != nil {
			return nil, err
		}
	}
	applied, applyErr := tuning.Apply(ctx, conn, plan, tuningRecorder{store})
	if applyErr != nil {
		if err := tuningFailure(ctx, cfg, store, applyErr); err != nil {
			// Stopping has to leave the target as it was found. Half-tuned is the
			// worst outcome: the run is not proceeding, so nothing will revert
			// these later, and an operator who walks away leaves a server with
			// bulk-load checkpoint settings and no record of why.
			if revertErr := revertTargetTuning(ctx, cfg.Target, store); revertErr != nil {
				return nil, errors.Join(err, fmt.Errorf("undo partial tuning: %w", revertErr))
			}
			return nil, err
		}
	}
	for _, change := range applied {
		logEvent(cfg.Dir, "target_tuning", map[string]any{
			"setting": change.Name, "from": change.From, "to": change.To, "scope": change.Scope,
		})
	}
	for name, value := range sessionGUCs {
		logEvent(cfg.Dir, "target_tuning", map[string]any{
			"setting": name, "to": value, "scope": tuning.ScopeSession,
		})
	}
	if len(applied) != 0 {
		// Only the system changes outlive the process, so only they warrant a
		// finding warning that the target is not in its serving configuration.
		if err := store.UpsertFinding(ctx, state.Finding{
			ID: tuningFinding, Kind: "target-tuning", Severity: "warning",
			Message: tuningMessage(applied),
		}); err != nil {
			return nil, err
		}
	}
	if len(plan.Blocked) != 0 {
		// Preflight reports these too, but --ack-warnings acknowledges every
		// warning at once, so without a finding here an operator who acked a
		// crowded preflight has nothing left telling them the load is running
		// with the settings that matter most left at their defaults.
		if err := store.UpsertFinding(ctx, state.Finding{
			ID: "target-tuning-blocked", Kind: "target-tuning", Severity: "warning",
			Message: blockedMessage(plan.Blocked),
		}); err != nil {
			return nil, err
		}
	}
	return sessionGUCs, nil
}

func tuningMessage(applied []tuning.Change) string {
	parts := make([]string, 0, len(applied))
	for _, change := range applied {
		parts = append(parts, fmt.Sprintf("%s=%s (was %s)", change.Name, change.To, change.From))
	}
	return "target server settings were temporarily changed for the bulk load: " +
		strings.Join(parts, ", ")
}

func blockedMessage(blocked map[string]string) string {
	names := make([]string, 0, len(blocked))
	for name := range blocked {
		names = append(names, name)
	}
	slices.Sort(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+" ("+blocked[name]+")")
	}
	return "the bulk load is running with these target settings left as they are, which makes it slower: " +
		strings.Join(parts, "; ")
}

// tuningFailure applies the failure policy. By default a target that cannot be
// tuned stops the run, because the alternative is a migration that silently takes
// far longer than the operator planned for. With --warn-on-tuning-errors the
// failure is recorded and the run proceeds with whatever did apply.
func tuningFailure(ctx context.Context, cfg config.Config, store *state.Store, cause error) error {
	if !cfg.WarnOnTuningErrors {
		return fmt.Errorf("tune target for the bulk load: %w "+
			"(pass --warn-on-tuning-errors to continue untuned, or --skip-target-tuning to not try)", cause)
	}
	logEvent(cfg.Dir, "target_tuning_error", map[string]any{"error": cause.Error()})
	if err := store.UpsertFinding(ctx, state.Finding{
		ID: "target-tuning-incomplete", Kind: "target-tuning", Severity: "warning",
		Message: "some target settings could not be tuned for the bulk load, which will be slower: " + cause.Error(),
	}); err != nil {
		return err
	}
	return nil
}

// revertTargetTuning puts back everything tuneTarget changed at the server level,
// reading what to restore from the steps written before each change.
//
// This runs even under --no-cleanup. A target left with a bulk-load max_wal_size
// and a 30 minute checkpoint_timeout is about to start serving production traffic
// with a much longer crash recovery than its operator expects, which is not
// metadata to be retained but a misconfiguration to be undone.
func revertTargetTuning(ctx context.Context, targetDSN string, store *state.Store) error {
	changes, err := recordedTuning(ctx, store)
	if err != nil {
		return err
	}
	if len(changes) == 0 {
		return nil
	}
	conn, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if err := tuning.Revert(ctx, conn, changes); err != nil {
		return err
	}
	// Resolving is best effort: the finding is absent when only session settings
	// were applied, and a missing finding must not fail a completed revert.
	_ = store.ResolveFinding(ctx, tuningFinding)
	return nil
}

func recordedTuning(ctx context.Context, store *state.Store) ([]tuning.Change, error) {
	steps, err := store.ListSteps(ctx)
	if err != nil {
		return nil, err
	}
	var changes []tuning.Change
	for _, step := range steps {
		if !step.Completed || !strings.HasPrefix(step.Name, tuningStepPrefix) {
			continue
		}
		var change tuning.Change
		if err := json.Unmarshal([]byte(step.Detail), &change); err != nil {
			return nil, fmt.Errorf("decode %s: %w", step.Name, err)
		}
		changes = append(changes, change)
	}
	return changes, nil
}
