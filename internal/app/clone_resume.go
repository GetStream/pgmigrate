package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/setup"

	pgcopy "github.com/GetStream/pgmigrate/internal/copy"
	"github.com/GetStream/pgmigrate/internal/state"
)

var errCopyPlanMissing = errors.New("saved COPY plan is missing; progress was preserved")

func loadCopyPlan(ctx context.Context, store *state.Store) ([]pgcopy.Part, error) {
	steps, err := store.ListSteps(ctx)
	if err != nil {
		return nil, err
	}
	for _, step := range steps {
		if step.Name != "copy.plan" || !step.Completed {
			continue
		}
		var parts []pgcopy.Part
		if err := json.Unmarshal([]byte(step.Detail), &parts); err != nil {
			return nil, fmt.Errorf("read saved COPY plan: %w", err)
		}
		if len(parts) == 0 {
			return nil, errors.New("saved COPY plan is empty; progress was preserved")
		}
		return parts, nil
	}
	return nil, errCopyPlanMissing
}

// validateCopyPlan checks structural and physical range assumptions again on
// the clone. It is not a substitute for keeping the clone free of writes.
func validateCopyPlan(ctx context.Context, holder *setup.Holder, parts []pgcopy.Part) error {
	selected := map[string]bool{}
	for _, p := range parts {
		selected[p.Table.Schema+"\x00"+p.Table.Name] = true
	}
	tables, err := pgcopy.InventorySnapshot(ctx, connector(holder.SnapshotDSN), holder.Snapshot.Name, func(schema, table string) bool { return selected[schema+"\x00"+table] })
	if err != nil {
		return err
	}
	byOID := map[uint32]pgcopy.Table{}
	for _, table := range tables {
		byOID[table.OID] = table
	}
	if len(byOID) != len(selected) {
		return errors.New("clone COPY tables changed; progress was preserved")
	}
	for _, part := range parts {
		actual, ok := byOID[part.Table.OID]
		saved := part.Table
		if !ok || actual.Schema != saved.Schema || actual.Name != saved.Name || actual.RelKind != saved.RelKind ||
			actual.IntegerKey != saved.IntegerKey || actual.KeyMin != saved.KeyMin || actual.KeyMax != saved.KeyMax ||
			actual.HasKeyBounds != saved.HasKeyBounds || actual.HeapBlocks != saved.HeapBlocks || !reflect.DeepEqual(actual.Columns, saved.Columns) {
			return fmt.Errorf("clone table %s changed since COPY planning; progress was preserved", saved.Identifier())
		}
	}
	return nil
}

// Older releases saved range bounds but not the complete plan. Reconstruct it
// only when every selected table and every original range can be matched.
func recoverCopyPlan(ctx context.Context, cfg config.Config, store *state.Store, holder *setup.Holder) ([]pgcopy.Part, error) {
	filter, err := loadFilter(cfg.TableFilter)
	if err != nil {
		return nil, err
	}
	tables, err := pgcopy.InventorySnapshot(ctx, connector(holder.SnapshotDSN), holder.Snapshot.Name, filter.Match)
	if err != nil {
		return nil, err
	}
	savedTables, err := store.ListTables(ctx)
	if err != nil {
		return nil, err
	}
	savedParts, err := store.ListParts(ctx)
	if err != nil {
		return nil, err
	}
	if len(tables) != len(savedTables) {
		return nil, errors.New("cannot recover incomplete legacy COPY plan; progress was preserved")
	}
	byOID := map[uint32][]state.Part{}
	for _, p := range savedParts {
		byOID[p.TableOID] = append(byOID[p.TableOID], p)
	}
	for _, t := range savedTables {
		if int64(len(byOID[t.OID])) != t.PartsTotal {
			return nil, errors.New("legacy COPY ranges are incomplete; progress was preserved")
		}
	}
	source, err := postgres.Connect(ctx, holder.SnapshotDSN)
	if err != nil {
		return nil, err
	}
	defer source.Close(context.Background())
	target, err := postgres.Connect(ctx, cfg.Target)
	if err != nil {
		return nil, err
	}
	defer target.Close(context.Background())
	var sourceMajor, targetMajor int
	if err := source.QueryRow(ctx, "SELECT current_setting('server_version_num')::int/10000").Scan(&sourceMajor); err != nil {
		return nil, err
	}
	if err := target.QueryRow(ctx, "SELECT current_setting('server_version_num')::int/10000").Scan(&targetMajor); err != nil {
		return nil, err
	}
	var parts []pgcopy.Part
	for _, table := range tables {
		saved := byOID[table.OID]
		if len(saved) == 0 {
			return nil, errors.New("legacy COPY table has no saved ranges")
		}
		planned := pgcopy.Plan(table, 1, len(saved), pgcopy.ConservativeFormat(table, sourceMajor, targetMajor))
		if len(planned) != len(saved) {
			return nil, errors.New("legacy COPY range count changed")
		}
		bounds := map[string]state.Part{}
		for _, p := range saved {
			bounds[p.ID] = p
		}
		for _, p := range planned {
			old, ok := bounds[p.ID]
			if !ok || old.RangeStart != p.RangeStart || old.RangeEnd != p.RangeEnd {
				return nil, errors.New("legacy COPY range bounds changed; progress was preserved")
			}
		}
		parts = append(parts, planned...)
	}
	data, err := json.Marshal(parts)
	if err != nil {
		return nil, err
	}
	if err := store.CompleteStep(ctx, "copy.plan", string(data)); err != nil {
		return nil, err
	}
	return parts, nil
}
