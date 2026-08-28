package verify

import "time"

func appKeyIndex(table Table) int {
	for i, column := range table.Key.Columns {
		if column.Name == "app_pk" {
			return i
		}
	}
	return -1
}

func rowApp(table Table, row rowEntry) string {
	if i := appKeyIndex(table); i >= 0 && i < len(row.key) {
		return row.key[i]
	}
	return row.appID
}

// excludeDiffs changes only the verification verdict. The row has already been
// compared; every ignored mismatch remains in the audit with its snapshots.
func (w *worker) excludeDiffs(table Table, stratum string, diffs []RowDiff, source, target rowSet) ([]RowDiff, int, error) {
	if len(w.cfg.ignoredApps) == 0 || len(diffs) == 0 {
		return diffs, 0, nil
	}
	kept := make([]RowDiff, 0, len(diffs))
	var events []AuditEvent
	for _, diff := range diffs {
		id := identity(diff.Key)
		app := rowApp(table, source[id])
		if !w.cfg.ignoredApps[app] {
			kept = append(kept, diff)
			continue
		}
		events = append(events, AuditEvent{
			Time: time.Now().UTC(), Table: table.Schema + "." + table.Name,
			Key: diff.Key, Stratum: stratum, Outcome: "ignored_app", Kind: diff.Kind,
			AppID: app, Source: snapshot(source, id), Target: snapshot(target, id),
		})
	}
	if err := w.audit(events); err != nil {
		return nil, 0, err
	}
	return kept, len(events), nil
}
