package verify

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// defaultCDCKeys is how many recorded keys one table's CDC check looks at.
const defaultCDCKeys int64 = 100_000

// CDCKeys returns the keys an applier recorded while writing them, for one
// relation. It is nil when nothing has been applying, which is the case at
// cutover and for a directory whose applier predates the reservoir.
type CDCKeys func(ctx context.Context, schema, table string) (CDCRecorded, error)

// CDCRecorded is one relation's reservoir: the keys it retained, and how many
// changes they were drawn from. Observed is what makes the retained keys
// interpretable — a hundred thousand keys out of a hundred thousand changes is a
// complete check of the CDC path, and out of ten million it is a one percent one.
type CDCRecorded struct {
	Keys     []CDCKey
	Observed int64
	// Dropped is how many of those changes could not be recorded, because nothing
	// could render their identity to a value a lookup can bind. It is what tells a
	// relation that contributed nothing apart from one whose reservoir is simply
	// full, since both show fewer keys than changes.
	Dropped int64
}

// CDCKey is one recorded change, named by column so that a key the applier chose
// on the replica identity can be checked against the key verification chose on
// the primary key, or refused when they do not line up.
type CDCKey struct {
	Key  map[string]string
	Kind string
}

// CDCResult is what the CDC stratum checked for one table.
//
// It is kept apart from SourceResult and never folded into it. The two strata
// answer different questions — one asks whether the base copy landed, the other
// whether replication is applying correctly — and a reader who saw one number
// would take the cheap answer for the expensive one.
type CDCResult struct {
	// Keys is how many recorded changes were checked, and Observed how many the
	// applier saw. The ratio is this stratum's coverage.
	Keys     int64 `json:"keys"`
	Observed int64 `json:"observed"`
	// Deletes is how many of the checked keys were recorded as deletes. These
	// still check a source row if the key was later reinserted, but do not require
	// the target to remove rows that are absent from the source.
	Deletes int64 `json:"deletes"`
	// Dropped is how many changes the applier could not name, and so never
	// offered. Without it, a relation whose every key is unrenderable looks the
	// same as one whose reservoir is merely full.
	Dropped    int64  `json:"dropped,omitempty"`
	Candidates int    `json:"candidate_rows,omitempty"`
	InFlight   int    `json:"in_flight_rows,omitempty"`
	Skipped    string `json:"skipped,omitempty"`
	// Pending counts initial differences awaiting the deferred check. They are not
	// divergence until rechecked; a canceled or incomplete check cannot clear them.
	Pending        int `json:"pending_rows,omitempty"`
	pending        []RowDiff
	baseline       rowSet
	recheckAt      time.Time
	targetBaseline rowSet
	// Advanced counts rows accepted because the target changed during the delay.
	// These are progressing, not verified equal to the source.
	Advanced int `json:"advanced_rows,omitempty"`
	// SourceChanged counts touched source rows. Each requires target advancement,
	// and contributes to either matched/advanced or unresolved; none is skipped.
	SourceChanged int `json:"source_changed_rows,omitempty"`
}

// verifyCDC checks the rows the applier reported writing.
//
// The first pass snapshots each differing source row, including xmin. A separate
// worker rechecks it after one minute. Rows touched on the source in that interval
// require target advancement; a source change alone cannot clear a mismatch.
// A target that advanced is accepted as progressing even if it does not yet
// match. A stalled target fails. There is exactly one deferred check, no retries.
//
// Like the heap sample, this is a source-to-target check. A recorded key absent
// from the source at the initial read produces no difference, whether the target still
// holds it or which operation was recorded. A key reinserted on the source is
// checked against its current contents, even if it was recorded as a delete.
func (w *worker) verifyCDC(
	ctx context.Context, table Table, leaves []relation, out *TableResult,
) ([]RowDiff, error) {
	if w.cfg.CDCKeys == nil {
		return nil, nil
	}
	recorded, err := collectRecorded(ctx, w.cfg.CDCKeys, leaves, w.cfg.CDCRows, out)
	if err != nil {
		return nil, err
	}
	if out.CDC.Dropped > 0 {
		// Said out loud because it is otherwise indistinguishable from an ordinary
		// sample: a full reservoir also holds fewer keys than the applier saw.
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d change(s) applied to %s could not be recorded, because nothing could render their identity, so that much of its replication path was not checked",
			out.CDC.Dropped, table.Identifier(),
		))
	}
	if len(recorded) == 0 {
		return nil, nil
	}

	keys := make([][]string, 0, len(recorded))
	for _, entry := range recorded {
		key, ok := projectKey(table, entry.Key)
		if !ok {
			// The applier's replica identity does not carry every column
			// verification keys this table on. Comparing what is left would ask
			// the target about a key it cannot look up, so the stratum reports
			// that it checked nothing instead of inventing a result.
			out.CDC.Skipped = fmt.Sprintf(
				"the applier recorded these changes by %s, which does not cover the %s this check keys rows on",
				renderColumns(entry.Key), renderKeyColumns(table),
			)
			return nil, nil
		}
		keys = append(keys, key)
		if entry.Kind == "delete" {
			out.CDC.Deletes++
		}
	}
	out.CDC.Keys = int64(len(keys))
	w.report(Progress{
		Table: table.Schema + "." + table.Name, Stage: StageCDC,
		CDCKeys: out.CDC.Keys, CDCObserved: out.CDC.Observed,
	})
	source, err := readRowsVersioned(ctx, w.source, table, keys, w.cfg.BatchRows, true)
	if err != nil {
		return nil, err
	}
	target, err := readRowsVersioned(ctx, w.target, table, keys, w.cfg.BatchRows, true)
	if err != nil {
		return nil, err
	}
	candidates := compareRows(source, target)
	out.CDC.Candidates = len(candidates)
	out.CDC.recheckAt = time.Now().Add(w.cfg.CDCRecheckDelay)
	out.CDC.baseline = make(rowSet, len(candidates))
	out.CDC.targetBaseline = make(rowSet, len(candidates))
	for _, diff := range candidates {
		id := identity(diff.Key)
		out.CDC.baseline[id] = source[id]
		if row, ok := target[id]; ok {
			out.CDC.targetBaseline[id] = row
		}
	}
	if err := w.auditDiffs(table, "cdc", "deferred", candidates, source, target, nil); err != nil {
		return nil, err
	}
	return candidates, nil
}

// collectRecorded reads every leaf's reservoir up to the budget.
//
// The budget bounds the table, not the leaf. A partitioned table checked leaf by
// leaf against the same figure would spend the whole budget once per partition,
// and the cost of this stratum would then depend on how the table happens to be
// partitioned rather than on what was asked for.
//
// Observed is accumulated for every leaf even after the budget is full, because
// it is the denominator: stopping early would report a higher coverage for having
// looked at less.
//
// The budget is always positive, because applyDefaults substitutes defaultCDCKeys
// for anything else. There is no unbounded mode, and one would buy nothing: the
// reservoir is capped per relation as it is written.
func collectRecorded(
	ctx context.Context, read CDCKeys, leaves []relation, budget int64, out *TableResult,
) ([]CDCKey, error) {
	var recorded []CDCKey
	for _, leaf := range leaves {
		found, err := read(ctx, leaf.Schema, leaf.Name)
		if err != nil {
			return nil, fmt.Errorf("read recorded changes for %s: %w", leaf.identifier(), err)
		}
		out.CDC.Observed += found.Observed
		out.CDC.Dropped += found.Dropped
		if int64(len(recorded)) >= budget {
			continue
		}
		recorded = append(recorded, found.Keys...)
		if int64(len(recorded)) > budget {
			recorded = recorded[:budget]
		}
	}
	return recorded, nil
}

// projectKey pulls verification's key columns, in order, out of a recorded key.
func projectKey(table Table, recorded map[string]string) ([]string, bool) {
	key := make([]string, 0, len(table.Key.Columns))
	for _, column := range table.Key.Columns {
		value, present := recorded[column.Name]
		if !present {
			return nil, false
		}
		key = append(key, value)
	}
	return key, true
}

// renderColumns names the columns a recorded key carries, sorted so the message
// is the same on every run.
func renderColumns(recorded map[string]string) string {
	names := slices.Sorted(maps.Keys(recorded))
	return "(" + strings.Join(names, ", ") + ")"
}

func renderKeyColumns(table Table) string {
	names := make([]string, 0, len(table.Key.Columns))
	for _, column := range table.Key.Columns {
		names = append(names, column.Name)
	}
	return "(" + strings.Join(names, ", ") + ")"
}
