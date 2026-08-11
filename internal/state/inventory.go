package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ListTables returns the durable table inventory.
func (s *Store) ListTables(ctx context.Context) ([]Table, error) {
	return s.listTables(ctx, false)
}

// PendingTables returns tables without a durable completion marker.
func (s *Store) PendingTables(ctx context.Context) ([]Table, error) {
	return s.listTables(ctx, true)
}

func (s *Store) listTables(ctx context.Context, pending bool) ([]Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	query := `
		SELECT oid, schema_name, table_name, estimated_rows, bytes, parts_total,
			completed, completed_at
		FROM tables`
	if pending {
		query += " WHERE completed=0"
	}
	query += " ORDER BY bytes DESC, oid"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	var result []Table
	for rows.Next() {
		var item Table
		var completedAt int64
		if err := rows.Scan(
			&item.OID, &item.Schema, &item.Name, &item.EstimatedRows, &item.Bytes,
			&item.PartsTotal, &item.Completed, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan table inventory: %w", err)
		}
		item.CompletedAt = fromUnixNano(completedAt)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table inventory: %w", err)
	}
	return result, nil
}

// ListParts returns the durable copy-part inventory.
func (s *Store) ListParts(ctx context.Context) ([]Part, error) {
	return s.listParts(ctx, false)
}

// PendingParts returns copy parts without a durable completion marker.
func (s *Store) PendingParts(ctx context.Context) ([]Part, error) {
	return s.listParts(ctx, true)
}

func (s *Store) listParts(ctx context.Context, pending bool) ([]Part, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	query := `
		SELECT table_oid, part_id, range_start, range_end, rows_copied,
			bytes_copied, duration_ns, completed, completed_at
		FROM parts`
	if pending {
		query += " WHERE completed=0"
	}
	query += " ORDER BY table_oid, part_id"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var result []Part
	for rows.Next() {
		var item Part
		var duration, completedAt int64
		if err := rows.Scan(
			&item.TableOID, &item.ID, &item.RangeStart, &item.RangeEnd,
			&item.Rows, &item.Bytes, &duration, &item.Completed, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan part inventory: %w", err)
		}
		item.Duration = time.Duration(duration)
		item.CompletedAt = fromUnixNano(completedAt)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate part inventory: %w", err)
	}
	return result, nil
}

// ListIndexes returns the durable index inventory.
func (s *Store) ListIndexes(ctx context.Context) ([]Index, error) {
	return s.listIndexes(ctx, false)
}

// PendingIndexes returns indexes without a durable completion marker.
func (s *Store) PendingIndexes(ctx context.Context) ([]Index, error) {
	return s.listIndexes(ctx, true)
}

func (s *Store) listIndexes(ctx context.Context, pending bool) ([]Index, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	query := `
		SELECT oid, table_oid, name, definition, bytes, completed, completed_at
		FROM indexes`
	if pending {
		query += " WHERE completed=0"
	}
	query += " ORDER BY bytes DESC, oid"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list indexes: %w", err)
	}
	defer rows.Close()

	var result []Index
	for rows.Next() {
		var item Index
		var completedAt int64
		if err := rows.Scan(
			&item.OID, &item.TableOID, &item.Name, &item.Definition, &item.Bytes,
			&item.Completed, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan index inventory: %w", err)
		}
		item.CompletedAt = fromUnixNano(completedAt)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate index inventory: %w", err)
	}
	return result, nil
}

// ListConstraints returns the durable constraint inventory.
func (s *Store) ListConstraints(ctx context.Context) ([]Constraint, error) {
	return s.listConstraints(ctx, false)
}

// PendingConstraints returns constraints without a durable completion marker.
func (s *Store) PendingConstraints(ctx context.Context) ([]Constraint, error) {
	return s.listConstraints(ctx, true)
}

func (s *Store) listConstraints(ctx context.Context, pending bool) ([]Constraint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	query := `
		SELECT oid, table_oid, name, kind, definition, completed, completed_at
		FROM constraints`
	if pending {
		query += " WHERE completed=0"
	}
	query += " ORDER BY table_oid, oid"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list constraints: %w", err)
	}
	defer rows.Close()

	var result []Constraint
	for rows.Next() {
		var item Constraint
		var completedAt int64
		if err := rows.Scan(
			&item.OID, &item.TableOID, &item.Name, &item.Kind, &item.Definition,
			&item.Completed, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan constraint inventory: %w", err)
		}
		item.CompletedAt = fromUnixNano(completedAt)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate constraint inventory: %w", err)
	}
	return result, nil
}

// ListFindings returns every durable finding.
func (s *Store) ListFindings(ctx context.Context) ([]Finding, error) {
	return s.listFindings(ctx, false)
}

// PendingFindings returns unresolved findings.
func (s *Store) PendingFindings(ctx context.Context) ([]Finding, error) {
	return s.listFindings(ctx, true)
}

func (s *Store) listFindings(ctx context.Context, pending bool) ([]Finding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	query := `
		SELECT id, kind, severity, message, resolved, observed_at, resolved_at
		FROM findings`
	if pending {
		query += " WHERE resolved=0"
	}
	query += " ORDER BY observed_at, id"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list findings: %w", err)
	}
	defer rows.Close()

	var result []Finding
	for rows.Next() {
		var item Finding
		var observedAt, resolvedAt int64
		if err := rows.Scan(
			&item.ID, &item.Kind, &item.Severity, &item.Message, &item.Resolved,
			&observedAt, &resolvedAt,
		); err != nil {
			return nil, fmt.Errorf("scan finding inventory: %w", err)
		}
		item.ObservedAt = fromUnixNano(observedAt)
		item.ResolvedAt = fromUnixNano(resolvedAt)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate finding inventory: %w", err)
	}
	return result, nil
}

// UpsertStep adds or refreshes a pending orchestrator action without clearing a
// completion marker.
func (s *Store) UpsertStep(ctx context.Context, step Step) error {
	if step.Name == "" {
		return errors.New("step name is required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO steps (name, detail) VALUES (?, ?)
			ON CONFLICT(name) DO UPDATE SET
				detail=CASE WHEN steps.completed=0 THEN excluded.detail ELSE steps.detail END`,
			step.Name, step.Detail,
		)
		if err != nil {
			return fmt.Errorf("upsert step %s: %w", step.Name, err)
		}
		return nil
	})
}

// ListSteps returns every durable orchestrator step.
func (s *Store) ListSteps(ctx context.Context) ([]Step, error) {
	return s.listSteps(ctx, false)
}

// PendingSteps returns steps without a durable completion marker.
func (s *Store) PendingSteps(ctx context.Context) ([]Step, error) {
	return s.listSteps(ctx, true)
}

func (s *Store) listSteps(ctx context.Context, pending bool) ([]Step, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	query := "SELECT name, detail, completed, completed_at FROM steps"
	if pending {
		query += " WHERE completed=0"
	}
	query += " ORDER BY name"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list steps: %w", err)
	}
	defer rows.Close()

	var result []Step
	for rows.Next() {
		var item Step
		var completedAt int64
		if err := rows.Scan(&item.Name, &item.Detail, &item.Completed, &completedAt); err != nil {
			return nil, fmt.Errorf("scan step inventory: %w", err)
		}
		item.CompletedAt = fromUnixNano(completedAt)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate step inventory: %w", err)
	}
	return result, nil
}
