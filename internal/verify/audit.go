package verify

import (
	"fmt"
	"strconv"
	"time"
)

// RowSnapshot contains only comparison metadata, never full row contents. Version
// is the row xmin, compared only within the same database, never across sides.
type RowSnapshot struct {
	Present bool   `json:"present"`
	Hash    string `json:"hash,omitempty"`
	Version string `json:"version,omitempty"`
}

// AuditEvent records each observed mismatch and every later disposition. Keys
// and hashes allow investigation even when a candidate settles or is skipped.
type AuditEvent struct {
	Time           time.Time    `json:"time"`
	Table          string       `json:"table,omitempty"`
	Key            []string     `json:"key,omitempty"`
	Stratum        string       `json:"stratum,omitempty"`
	Outcome        string       `json:"outcome"`
	SourceChanged  bool         `json:"source_changed,omitempty"`
	Kind           DiffKind     `json:"kind,omitempty"`
	Source         *RowSnapshot `json:"source,omitempty"`
	Target         *RowSnapshot `json:"target,omitempty"`
	OriginalSource *RowSnapshot `json:"original_source,omitempty"`
	PreviousTarget *RowSnapshot `json:"previous_target,omitempty"`
	SourceAfter    *RowSnapshot `json:"source_after,omitempty"`
}

func snapshot(rows rowSet, key string) *RowSnapshot {
	row, ok := rows[key]
	out := &RowSnapshot{Present: ok}
	if ok {
		out.Hash = strconv.FormatInt(row.hash, 10)
		out.Version = row.version
	}
	return out
}

func (w *worker) audit(events []AuditEvent) error {
	if w.cfg.Audit == nil || len(events) == 0 {
		return nil
	}
	if err := w.cfg.Audit(events); err != nil {
		return fmt.Errorf("write verification audit: %w", err)
	}
	return nil
}

func (w *worker) auditDiffs(table Table, stratum, outcome string, diffs []RowDiff, source, target, original rowSet) error {
	if w.cfg.Audit == nil {
		return nil
	}
	events := make([]AuditEvent, 0, len(diffs))
	for _, diff := range diffs {
		id := identity(diff.Key)
		e := AuditEvent{Time: time.Now().UTC(), Table: table.Schema + "." + table.Name, Key: diff.Key, Stratum: stratum, Outcome: outcome, Kind: diff.Kind}
		if source != nil {
			e.Source = snapshot(source, id)
		}
		if target != nil {
			e.Target = snapshot(target, id)
		}
		if original != nil {
			e.OriginalSource = snapshot(original, id)
		}
		events = append(events, e)
	}
	return w.audit(events)
}

func (w *worker) auditRecheck(table Table, keys [][]string, source, target rowSet) error {
	if w.cfg.Audit == nil {
		return nil
	}
	events := make([]AuditEvent, 0, len(keys))
	for _, key := range keys {
		id := identity(key)
		s, sp := source[id]
		t, tp := target[id]
		e := AuditEvent{Time: time.Now().UTC(), Table: table.Schema + "." + table.Name, Key: key, Stratum: "heap", Outcome: "converged", Source: snapshot(source, id), Target: snapshot(target, id)}
		switch {
		case !sp:
			e.Outcome = "source_absent"
		case !tp:
			e.Outcome = "mismatch"
			e.Kind = DiffSourceOnly
		case s.hash != t.hash:
			e.Outcome = "mismatch"
			e.Kind = DiffDifferent
		}
		events = append(events, e)
	}
	return w.audit(events)
}
