package verify

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
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
	// Deletes is how many of the checked keys were deletes. They are the only
	// check either stratum makes that can catch a row the target holds and the
	// source does not.
	Deletes int64 `json:"deletes"`
	// Dropped is how many changes the applier could not name, and so never
	// offered. Without it, a relation whose every key is unrenderable looks the
	// same as one whose reservoir is merely full.
	Dropped    int64  `json:"dropped,omitempty"`
	Candidates int    `json:"candidate_rows,omitempty"`
	InFlight   int    `json:"in_flight_rows,omitempty"`
	Skipped    string `json:"skipped,omitempty"`
}

// verifyCDC checks the rows the applier reported writing.
//
// The whole check is one call to resolve. That is not a shortcut: resolve already
// reads both sides by key, fixes a WAL position between the two reads, waits for
// apply to pass it, and re-reads whatever still differs until the convergence
// budget runs out. Comparing the two sides once beforehand would only re-find the
// rows that are legitimately in flight.
//
// A correctly applied delete is absent on both sides and produces nothing. An
// unapplied one is absent on the source and present on the target, which
// compareRows reports as DiffTargetOnly — the direction the heap sample cannot
// see at all, because it only ever asks about keys the source still has.
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
	unresolved, candidates, err := w.resolve(ctx, table, keys)
	if err != nil {
		return nil, err
	}
	out.CDC.Candidates = candidates
	out.CDC.InFlight = candidates - len(unresolved)
	return unresolved, nil
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
