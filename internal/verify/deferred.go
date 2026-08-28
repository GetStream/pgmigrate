package verify

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// unchangedSource checks the row version as well as its contents: a no-op update
// or an update followed by a revert is still a touched row, not a stable sample.
func unchangedSource(original rowEntry, current rowSet) bool {
	row, ok := current[identity(original.key)]
	return ok && row.version == original.version && row.hash == original.hash
}

// advancedTarget means a present target row received a write since the previous
// observation. It is evidence of activity, not proof of convergence or direction.
// Losing a previously present target row is not progress toward a present source.
func advancedTarget(key string, previous, current rowSet) bool {
	row, present := current[key]
	old, wasPresent := previous[key]
	return present && (!wasPresent || row.version != old.version || row.hash != old.hash)
}

// recheckCDC makes one final decision after the defer interval. Source reads
// bracket the target read; a stable match takes precedence over advancement.
// Rows that still differ require target progress, even if the source changed.
// Progress is accepted separately from equality. There are no further retries.
func (w *worker) recheckCDC(ctx context.Context, out TableResult) (result TableResult, err error) {
	defer func() {
		if err != nil || result.CutShort != "" {
			auditErr := w.auditDiffs(out.Table, "cdc", "incomplete", out.CDC.pending, nil, nil, out.CDC.baseline, out.CDC.replayBoundary)
			err = errors.Join(err, auditErr)
		}
	}()
	timer := time.NewTimer(max(time.Until(out.CDC.recheckAt), 0))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return TableResult{}, ctx.Err()
	case <-timer.C:
	}
	started := time.Now()
	previous := out.Duration
	tableCtx := ctx
	if w.cfg.TableTimeout > 0 {
		remaining := w.cfg.TableTimeout - previous
		if remaining <= 0 {
			return w.finish(out, started.Add(-previous), "table timeout reached"), nil
		}
		var cancel context.CancelFunc
		tableCtx, cancel = context.WithTimeout(ctx, remaining)
		defer cancel()
	}
	w.report(Progress{Table: out.Table.Schema + "." + out.Table.Name, Stage: StageCDCRechecking, CDCKeys: out.CDC.Keys, CDCObserved: out.CDC.Observed, CDCPending: out.CDC.Pending, Unresolved: len(out.Unresolved)})
	finishError := func(err error) (TableResult, error) {
		w.close()
		if reason := cutShort(ctx, tableCtx, err); reason != "" {
			return w.finish(out, started.Add(-previous), reason), nil
		}
		return TableResult{}, err
	}
	if err := w.connect(tableCtx); err != nil {
		return finishError(err)
	}
	keys := candidateKeys(out.CDC.pending, 0)
	before, err := readRowsVersioned(tableCtx, w.source, out.Table, keys, w.cfg.BatchRows, true)
	if err != nil {
		return finishError(err)
	}
	if w.cfg.Boundary != nil && w.cfg.WaitApplied != nil {
		// A minute is a minimum defer, not proof that replay has seen this fresh
		// source snapshot. Fence that snapshot before the single target read.
		// The confirmation stays bounded even with table timeouts disabled.
		confirmCtx, cancel := context.WithTimeout(tableCtx, w.cfg.ConvergeTimeout)
		position, confirmErr := w.cfg.Boundary(confirmCtx, w.source)
		if confirmErr == nil {
			out.CDC.replayBoundary = position
			confirmErr = w.cfg.WaitApplied(confirmCtx, position)
		}
		timedOut := errors.Is(confirmCtx.Err(), context.DeadlineExceeded) && tableCtx.Err() == nil
		cancel()
		if confirmErr != nil {
			if timedOut {
				w.close()
				return w.finish(out, started.Add(-previous), "CDC replay confirmation timeout"), nil
			}
			return finishError(fmt.Errorf("confirm replay for CDC recheck of %s: %w", out.Table.Identifier(), confirmErr))
		}
	}
	target, err := readRowsVersioned(tableCtx, w.target, out.Table, keys, w.cfg.BatchRows, true)
	if err != nil {
		return finishError(err)
	}
	after, err := readRowsVersioned(tableCtx, w.source, out.Table, keys, w.cfg.BatchRows, true)
	if err != nil {
		return finishError(err)
	}
	events := make([]AuditEvent, 0, len(keys))
	var unresolved []RowDiff
	changed := 0
	for _, key := range keys {
		id := identity(key)
		original := out.CDC.baseline[id]
		other, present := target[id]
		e := AuditEvent{Time: time.Now().UTC(), Table: out.Table.Schema + "." + out.Table.Name, Key: key, Stratum: "cdc", OriginalSource: snapshot(out.CDC.baseline, id), PreviousTarget: snapshot(out.CDC.targetBaseline, id), Source: snapshot(before, id), SourceAfter: snapshot(after, id), Target: snapshot(target, id)}
		e.ReplayBoundary = out.CDC.replayBoundary
		e.SourceChanged = !unchangedSource(original, before) || !unchangedSource(original, after)
		if e.SourceChanged {
			changed++
		}
		advanced := advancedTarget(id, out.CDC.targetBaseline, target)
		currentSource, sourcePresent := after[id]
		_, targetWasPresent := out.CDC.targetBaseline[id]
		// A target deletion is progress only when the source disappeared too.
		if e.SourceChanged && !sourcePresent && !present && targetWasPresent {
			advanced = true
		}
		matched := sourcePresent && present && unchangedSource(currentSource, before) && other.hash == currentSource.hash
		switch {
		case !matched && w.cfg.ignoredApps[rowApp(out.Table, currentSource)]:
			e.Outcome, e.AppID = "ignored_app", rowApp(out.Table, currentSource)
			e.Kind = DiffDifferent
			if !present {
				e.Kind = DiffSourceOnly
			}
			out.IgnoredRows++
		case matched:
			// The target may already have caught up between the initial source
			// and target reads. Equal rows need no further target write.
			e.Outcome = "converged"
			out.CDC.InFlight++
		case e.SourceChanged && !advanced:
			e.Outcome, e.Kind = "unresolved", DiffTargetStalled
			unresolved = append(unresolved, RowDiff{Key: key, Kind: e.Kind})
		case advanced:
			e.Outcome = "advanced"
			out.CDC.Advanced++
		default:
			e.Outcome = "unresolved"
			e.Kind = DiffDifferent
			if !present {
				e.Kind = DiffSourceOnly
			}
			unresolved = append(unresolved, RowDiff{Key: key, Kind: e.Kind})
		}
		events = append(events, e)
	}
	if err := w.audit(events); err != nil {
		return TableResult{}, err
	}
	out.CDC.SourceChanged += changed

	if out.CDC.Advanced > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf("%s: %d CDC target row(s) advanced without matching the source; accepted as progressing, not verified equal", out.Table.Identifier(), out.CDC.Advanced))
	}
	out.CDC.pending, out.CDC.baseline, out.CDC.targetBaseline, out.CDC.Pending = nil, nil, nil, 0
	out.Unresolved = append(out.Unresolved, unresolved...)
	out.Complete = true
	out.Converged = len(out.Unresolved) == 0
	return w.finish(out, started.Add(-previous), ""), nil
}
