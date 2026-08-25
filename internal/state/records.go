package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ResetBaseCopy forgets all state tied to an exported snapshot. It is the only
// supported recovery for setup/schema/copy after the exporting process dies;
// callers must first remove the old source and target objects.
func (s *Store) ResetBaseCopy(ctx context.Context) error {
	return s.resetSnapshotState(ctx, false, nil, func(phase Phase) error {
		if phaseOrder[phase] > phaseOrder[PhaseCopy] {
			return fmt.Errorf("base-copy reset is only allowed through copy phase (current %s)", phase)
		}
		return nil
	})
}

// ResetForFreshSnapshot forgets all state derived from a completed base-copy
// snapshot after the caller has independently proved that its logical stream
// is unrecoverable and removed the migration-owned source and target objects.
// It deliberately excludes drained, cutover, and complete migrations.
func (s *Store) ResetForFreshSnapshot(ctx context.Context, resolvedFindingIDs ...string) error {
	return s.resetSnapshotState(ctx, true, resolvedFindingIDs, func(phase Phase) error {
		switch phase {
		case PhaseIndexes, PhaseCatchup, PhaseFollow:
			return nil
		default:
			return fmt.Errorf("fresh-snapshot reset is unavailable in %s phase", phase)
		}
	})
}

func (s *Store) resetSnapshotState(
	ctx context.Context,
	clearFailure bool,
	resolvedFindingIDs []string,
	validate func(Phase) error,
) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		var phase Phase
		if err := tx.QueryRowContext(ctx, "SELECT phase FROM migration WHERE id=1").Scan(&phase); err != nil {
			return fmt.Errorf("read reset phase: %w", err)
		}
		if err := validate(phase); err != nil {
			return err
		}
		for _, statement := range []string{
			"DELETE FROM verify_tables", "DELETE FROM constraints", "DELETE FROM indexes",
			"DELETE FROM parts", "DELETE FROM tables",
			"DELETE FROM steps WHERE name NOT LIKE 'preflight.%'",
			"UPDATE apply_progress SET staged_lsn='', applied_lsn='', txns=0, rows_applied=0, updated_at=0 WHERE id=1",
			"UPDATE migration SET slot_name='', snapshot_name='', consistent_point='', end_position='', phase='preflight', updated_at=0 WHERE id=1",
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("reset base-copy state: %w", err)
			}
		}
		if clearFailure {
			if _, err := tx.ExecContext(ctx, "DELETE FROM failed_attempt"); err != nil {
				return fmt.Errorf("clear superseded failed attempt: %w", err)
			}
		}
		for _, id := range resolvedFindingIDs {
			if id == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE findings SET resolved=1, resolved_at=?
				WHERE id=? AND resolved=0`, time.Now().UTC().UnixNano(), id); err != nil {
				return fmt.Errorf("resolve superseded finding %s: %w", id, err)
			}
		}
		return nil
	})
}

// RecordFailedAttempt remembers how a run died. The counter advances only while
// consecutive runs fail in the same phase for the same reason, so a supervisor
// can tell a transient failure from one that will repeat forever.
func (s *Store) RecordFailedAttempt(ctx context.Context, phase Phase, signature, detail string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO failed_attempt(id,phase,signature,detail,consecutive,observed_at)
			VALUES(1,?,?,?,1,?)
			ON CONFLICT(id) DO UPDATE SET
				consecutive=CASE
					WHEN failed_attempt.phase=excluded.phase AND failed_attempt.signature=excluded.signature
					THEN failed_attempt.consecutive+1 ELSE 1 END,
				phase=excluded.phase, signature=excluded.signature,
				detail=excluded.detail, observed_at=excluded.observed_at`,
			phase, signature, detail, time.Now().UTC().UnixNano())
		if err != nil {
			return fmt.Errorf("record failed attempt: %w", err)
		}
		return nil
	})
}

// FailedAttempt reports how the previous run died, or a zero value when no run
// has failed since the last recorded progress.
func (s *Store) FailedAttempt(ctx context.Context) (FailedAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return FailedAttempt{}, ErrClosed
	}
	var attempt FailedAttempt
	var observed int64
	err := s.db.QueryRowContext(ctx, `
		SELECT phase,signature,detail,consecutive,observed_at FROM failed_attempt WHERE id=1`).Scan(
		&attempt.Phase, &attempt.Signature, &attempt.Detail, &attempt.Consecutive, &observed,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return FailedAttempt{}, nil
	}
	if err != nil {
		return FailedAttempt{}, fmt.Errorf("read failed attempt: %w", err)
	}
	if observed != 0 {
		attempt.ObservedAt = time.Unix(0, observed).UTC()
	}
	return attempt, nil
}

// ClearFailedAttempt forgets the previous failure, which callers do once a run
// makes progress that a restart would keep.
func (s *Store) ClearFailedAttempt(ctx context.Context) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "DELETE FROM failed_attempt"); err != nil {
			return fmt.Errorf("clear failed attempt: %w", err)
		}
		return nil
	})
}

// ResolveFailedAttempt clears attempt only if it is still the failure the
// caller observed, and resolves findingID in the same transaction. A later
// failure must remain visible even if an older run subsequently reports
// progress from another goroutine or process.
func (s *Store) ResolveFailedAttempt(
	ctx context.Context,
	attempt FailedAttempt,
	findingID string,
) (bool, error) {
	if attempt.Consecutive <= 0 {
		return false, nil
	}
	cleared := false
	err := s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
			DELETE FROM failed_attempt
			WHERE id=1 AND phase=? AND signature=? AND detail=?
			  AND consecutive=? AND observed_at=?`,
			attempt.Phase, attempt.Signature, attempt.Detail,
			attempt.Consecutive, unixNano(attempt.ObservedAt),
		)
		if err != nil {
			return fmt.Errorf("resolve failed attempt: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect failed attempt resolution: %w", err)
		}
		if rows == 0 {
			return nil
		}
		if findingID != "" {
			if _, err := tx.ExecContext(ctx, `
				UPDATE findings SET resolved=1,
					resolved_at=CASE WHEN resolved=0 THEN ? ELSE resolved_at END
				WHERE id=?`, time.Now().UTC().UnixNano(), findingID); err != nil {
				return fmt.Errorf("resolve finding %s with failed attempt: %w", findingID, err)
			}
		}
		cleared = true
		return nil
	})
	return cleared, err
}

// SetTargetCleanupRequested durably coordinates final target metadata cleanup
// between the cutover controller and the run process.
func (s *Store) SetTargetCleanupRequested(ctx context.Context, requested bool) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		if !requested {
			_, err := tx.ExecContext(ctx, "DELETE FROM steps WHERE name='target.cleanup.requested'")
			return err
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO steps(name,detail,completed,completed_at)
			VALUES('target.cleanup.requested','after run shutdown',1,?)
			ON CONFLICT(name) DO UPDATE SET detail=excluded.detail,completed=1,completed_at=excluded.completed_at`,
			time.Now().UTC().UnixNano())
		return err
	})
}

// TransitionPhase advances the lifecycle by one phase. Repeating the current
// phase is a no-op, which makes orchestration retries safe.
func (s *Store) TransitionPhase(ctx context.Context, next Phase) error {
	if _, ok := phaseOrder[next]; !ok {
		return fmt.Errorf("%w: unknown phase %q", ErrInvalidPhaseTransition, next)
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		var current Phase
		if err := tx.QueryRowContext(ctx, "SELECT phase FROM migration WHERE id = 1").Scan(&current); err != nil {
			return fmt.Errorf("read current phase: %w", err)
		}
		if next == current {
			return nil
		}
		if phaseOrder[next] != phaseOrder[current]+1 {
			return fmt.Errorf("%w: %s to %s", ErrInvalidPhaseTransition, current, next)
		}
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE migration SET phase = ?, updated_at = ? WHERE id = 1",
			next, time.Now().UTC().UnixNano(),
		); err != nil {
			return fmt.Errorf("update phase: %w", err)
		}
		return nil
	})
}

// SetSnapshot records the source objects that anchor the migration. Once set,
// changing any value is rejected rather than silently adopting foreign objects.
func (s *Store) SetSnapshot(ctx context.Context, slot, snapshot, consistentPoint string) error {
	if slot == "" || snapshot == "" || consistentPoint == "" {
		return errors.New("slot, snapshot, and consistent point are required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		var haveSlot, haveSnapshot, havePoint string
		if err := tx.QueryRowContext(
			ctx, `
			SELECT slot_name, snapshot_name, consistent_point FROM migration WHERE id = 1`,
		).Scan(&haveSlot, &haveSnapshot, &havePoint); err != nil {
			return fmt.Errorf("read snapshot state: %w", err)
		}
		if haveSlot != "" || haveSnapshot != "" || havePoint != "" {
			if haveSlot == slot && haveSnapshot == snapshot && havePoint == consistentPoint {
				return nil
			}
			return fmt.Errorf("snapshot state already set to slot=%q snapshot=%q point=%q",
				haveSlot, haveSnapshot, havePoint)
		}
		if _, err := tx.ExecContext(
			ctx, `
			UPDATE migration
			SET slot_name = ?, snapshot_name = ?, consistent_point = ?, updated_at = ?
			WHERE id = 1`,
			slot, snapshot, consistentPoint, time.Now().UTC().UnixNano(),
		); err != nil {
			return fmt.Errorf("set snapshot state: %w", err)
		}
		return nil
	})
}

// SetEndPosition records the immutable cutover drain position.
func (s *Store) SetEndPosition(ctx context.Context, lsn string) error {
	if lsn == "" {
		return errors.New("end position is required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		var have string
		if err := tx.QueryRowContext(ctx, "SELECT end_position FROM migration WHERE id = 1").Scan(&have); err != nil {
			return fmt.Errorf("read end position: %w", err)
		}
		if have != "" {
			if have == lsn {
				return nil
			}
			return fmt.Errorf("end position already set to %q", have)
		}
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE migration SET end_position = ?, updated_at = ? WHERE id = 1",
			lsn, time.Now().UTC().UnixNano(),
		); err != nil {
			return fmt.Errorf("set end position: %w", err)
		}
		return nil
	})
}

// UpsertTable adds or refreshes table inventory without clearing completion.
func (s *Store) UpsertTable(ctx context.Context, table Table) error {
	if table.OID == 0 || table.Schema == "" || table.Name == "" {
		return errors.New("table OID, schema, and name are required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO tables (oid, schema_name, table_name, estimated_rows, bytes, parts_total)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(oid) DO UPDATE SET
				schema_name=excluded.schema_name, table_name=excluded.table_name,
				estimated_rows=excluded.estimated_rows, bytes=excluded.bytes,
				parts_total=excluded.parts_total`,
			table.OID, table.Schema, table.Name, table.EstimatedRows, table.Bytes, table.PartsTotal,
		)
		if err != nil {
			return fmt.Errorf("upsert table %d: %w", table.OID, err)
		}
		return nil
	})
}

// CompleteTable durably marks a table complete.
func (s *Store) CompleteTable(ctx context.Context, oid uint32) error {
	return s.complete(ctx, "tables", "oid", oid)
}

// UpsertPart adds or refreshes copy work without clearing completion or metrics.
func (s *Store) UpsertPart(ctx context.Context, part Part) error {
	if part.TableOID == 0 || part.ID == "" {
		return errors.New("part table OID and ID are required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO parts (table_oid, part_id, range_start, range_end)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(table_oid, part_id) DO UPDATE SET
				range_start=excluded.range_start, range_end=excluded.range_end`,
			part.TableOID, part.ID, part.RangeStart, part.RangeEnd,
		)
		if err != nil {
			return fmt.Errorf("upsert part %d/%s: %w", part.TableOID, part.ID, err)
		}
		return nil
	})
}

// CompletePart records absolute copy metrics and marks a part complete. A
// repeated call is a no-op and preserves the first durable completion.
func (s *Store) CompletePart(ctx context.Context, tableOID uint32, id string, rows, bytes int64, duration time.Duration) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(
			ctx, `
			UPDATE parts SET rows_copied=?, bytes_copied=?, duration_ns=?,
				completed=1, completed_at=?
			WHERE table_oid=? AND part_id=? AND completed=0`,
			rows, bytes, duration.Nanoseconds(), time.Now().UTC().UnixNano(), tableOID, id,
		)
		if err != nil {
			return fmt.Errorf("complete part %d/%s: %w", tableOID, id, err)
		}
		if n, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("inspect part completion: %w", err)
		} else if n == 0 {
			var exists int
			if err := tx.QueryRowContext(
				ctx,
				"SELECT count(*) FROM parts WHERE table_oid=? AND part_id=?",
				tableOID, id,
			).Scan(&exists); err != nil {
				return fmt.Errorf("check part %d/%s: %w", tableOID, id, err)
			}
			if exists == 0 {
				return fmt.Errorf("part %d/%s does not exist", tableOID, id)
			}
		}
		return nil
	})
}

// UpsertIndex adds or refreshes index inventory without clearing completion.
func (s *Store) UpsertIndex(ctx context.Context, index Index) error {
	if index.OID == 0 || index.TableOID == 0 || index.Name == "" {
		return errors.New("index OID, table OID, and name are required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO indexes (oid, table_oid, name, definition, bytes)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(oid) DO UPDATE SET table_oid=excluded.table_oid,
				name=excluded.name, definition=excluded.definition, bytes=excluded.bytes`,
			index.OID, index.TableOID, index.Name, index.Definition, index.Bytes,
		)
		if err != nil {
			return fmt.Errorf("upsert index %d: %w", index.OID, err)
		}
		return nil
	})
}

// CompleteIndex durably marks an index complete.
func (s *Store) CompleteIndex(ctx context.Context, oid uint32) error {
	return s.complete(ctx, "indexes", "oid", oid)
}

// RecordIndexTargetDefinition remembers how the target renders an index that
// pgmigrate has created or adopted. Server-deparsed SQL is not comparable
// across two databases, so the target's own rendering is the only sound
// expectation to hold it to on a later resume.
func (s *Store) RecordIndexTargetDefinition(ctx context.Context, oid uint32, definition string) error {
	return s.recordTargetDefinition(ctx, "indexes", oid, definition)
}

// IndexTargetDefinition returns the recorded target rendering, empty when
// pgmigrate has not yet observed the index on the target.
func (s *Store) IndexTargetDefinition(ctx context.Context, oid uint32) (string, error) {
	return s.targetDefinition(ctx, "indexes", oid)
}

// UpsertConstraint adds or refreshes constraint inventory without clearing completion.
func (s *Store) UpsertConstraint(ctx context.Context, constraint Constraint) error {
	if constraint.OID == 0 || constraint.TableOID == 0 || constraint.Name == "" {
		return errors.New("constraint OID, table OID, and name are required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO constraints (oid, table_oid, name, kind, definition)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(oid) DO UPDATE SET table_oid=excluded.table_oid,
				name=excluded.name, kind=excluded.kind, definition=excluded.definition`,
			constraint.OID, constraint.TableOID, constraint.Name, constraint.Kind, constraint.Definition,
		)
		if err != nil {
			return fmt.Errorf("upsert constraint %d: %w", constraint.OID, err)
		}
		return nil
	})
}

// CompleteConstraint durably marks a constraint complete.
func (s *Store) CompleteConstraint(ctx context.Context, oid uint32) error {
	return s.complete(ctx, "constraints", "oid", oid)
}

// RecordConstraintTargetDefinition remembers how the target renders a
// constraint pgmigrate has created or adopted.
func (s *Store) RecordConstraintTargetDefinition(ctx context.Context, oid uint32, definition string) error {
	return s.recordTargetDefinition(ctx, "constraints", oid, definition)
}

// ConstraintTargetDefinition returns the recorded target rendering, empty when
// pgmigrate has not yet observed the constraint on the target.
func (s *Store) ConstraintTargetDefinition(ctx context.Context, oid uint32) (string, error) {
	return s.targetDefinition(ctx, "constraints", oid)
}

func (s *Store) recordTargetDefinition(ctx context.Context, table string, oid uint32, definition string) error {
	if definition == "" {
		return fmt.Errorf("%s %d target definition is required", table, oid)
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		query := fmt.Sprintf("UPDATE %s SET target_definition=? WHERE oid=?", table)
		result, err := tx.ExecContext(ctx, query, definition, oid)
		if err != nil {
			return fmt.Errorf("record %s %d target definition: %w", table, oid, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("record %s %d target definition: unknown %s", table, oid, table)
		}
		return nil
	})
}

func (s *Store) targetDefinition(ctx context.Context, table string, oid uint32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	var definition string
	query := fmt.Sprintf("SELECT target_definition FROM %s WHERE oid=?", table)
	err := s.db.QueryRowContext(ctx, query, oid).Scan(&definition)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %s %d target definition: %w", table, oid, err)
	}
	return definition, nil
}

// TableCompleted reports whether a table copy is durably complete.
func (s *Store) TableCompleted(ctx context.Context, oid uint32) (bool, error) {
	return s.completed(ctx, "tables", "oid", oid)
}

// PartCompleted reports whether a table part is durably complete.
func (s *Store) PartCompleted(ctx context.Context, tableOID uint32, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	var completed bool
	err := s.db.QueryRowContext(
		ctx,
		"SELECT completed FROM parts WHERE table_oid=? AND part_id=?",
		tableOID, id,
	).Scan(&completed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read part %d/%s completion: %w", tableOID, id, err)
	}
	return completed, nil
}

// IndexCompleted reports whether an index build is durably complete.
func (s *Store) IndexCompleted(ctx context.Context, oid uint32) (bool, error) {
	return s.completed(ctx, "indexes", "oid", oid)
}

// ConstraintCompleted reports whether a constraint operation is durably complete.
func (s *Store) ConstraintCompleted(ctx context.Context, oid uint32) (bool, error) {
	return s.completed(ctx, "constraints", "oid", oid)
}

func (s *Store) complete(ctx context.Context, table, key string, value any) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		query := fmt.Sprintf(
			"UPDATE %s SET completed=1, completed_at=? WHERE %s=? AND completed=0",
			table, key,
		)
		result, err := tx.ExecContext(ctx, query, time.Now().UTC().UnixNano(), value)
		if err != nil {
			return fmt.Errorf("complete %s %v: %w", table, value, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect %s completion: %w", table, err)
		}
		if n != 0 {
			return nil
		}
		var exists int
		check := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s=?", table, key)
		if err := tx.QueryRowContext(ctx, check, value).Scan(&exists); err != nil {
			return fmt.Errorf("check %s %v: %w", table, value, err)
		}
		if exists == 0 {
			return fmt.Errorf("%s %v does not exist", table, value)
		}
		return nil
	})
}

func (s *Store) completed(ctx context.Context, table, key string, value any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	var completed bool
	query := fmt.Sprintf("SELECT completed FROM %s WHERE %s=?", table, key)
	err := s.db.QueryRowContext(ctx, query, value).Scan(&completed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read %s %v completion: %w", table, value, err)
	}
	return completed, nil
}

// UpdateApplyProgress replaces the status copy of target-origin progress.
func (s *Store) UpdateApplyProgress(ctx context.Context, progress ApplyProgress) error {
	updatedAt := progress.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO apply_progress (id, staged_lsn, applied_lsn, txns, rows_applied, updated_at)
			VALUES (1, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET staged_lsn=excluded.staged_lsn,
				applied_lsn=excluded.applied_lsn, txns=excluded.txns,
				rows_applied=excluded.rows_applied, updated_at=excluded.updated_at`,
			progress.StagedLSN, progress.AppliedLSN, progress.Txns, progress.Rows,
			updatedAt.UTC().UnixNano(),
		)
		if err != nil {
			return fmt.Errorf("update apply progress: %w", err)
		}
		return nil
	})
}

// UpsertFinding stores the current form of a finding while preserving its
// original observation time.
func (s *Store) UpsertFinding(ctx context.Context, finding Finding) error {
	if finding.ID == "" || finding.Kind == "" || finding.Severity == "" || finding.Message == "" {
		return errors.New("finding ID, kind, severity, and message are required")
	}
	observed := finding.ObservedAt
	if observed.IsZero() {
		observed = time.Now().UTC()
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO findings (id, kind, severity, message, resolved, observed_at, resolved_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET kind=excluded.kind, severity=excluded.severity,
				message=excluded.message, resolved=excluded.resolved,
				resolved_at=excluded.resolved_at`,
			finding.ID, finding.Kind, finding.Severity, finding.Message, finding.Resolved,
			unixNano(observed), unixNano(finding.ResolvedAt),
		)
		if err != nil {
			return fmt.Errorf("upsert finding %s: %w", finding.ID, err)
		}
		return nil
	})
}

// ResolveFinding idempotently resolves a finding.
func (s *Store) ResolveFinding(ctx context.Context, id string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(
			ctx, `
			UPDATE findings SET resolved=1,
				resolved_at=CASE WHEN resolved=0 THEN ? ELSE resolved_at END
			WHERE id=?`,
			time.Now().UTC().UnixNano(), id,
		)
		if err != nil {
			return fmt.Errorf("resolve finding %s: %w", id, err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("inspect finding resolution: %w", err)
		}
		if n == 0 {
			return fmt.Errorf("finding %s does not exist", id)
		}
		return nil
	})
}

// CompleteStep idempotently records an orchestrator action.
func (s *Store) CompleteStep(ctx context.Context, name, detail string) error {
	if name == "" {
		return errors.New("step name is required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO steps (name, detail, completed, completed_at)
			VALUES (?, ?, 1, ?)
			ON CONFLICT(name) DO UPDATE SET
				detail=CASE WHEN steps.completed=0 THEN excluded.detail ELSE steps.detail END,
				completed=1,
				completed_at=CASE WHEN steps.completed=0 THEN excluded.completed_at ELSE steps.completed_at END`,
			name, detail, time.Now().UTC().UnixNano(),
		)
		if err != nil {
			return fmt.Errorf("complete step %s: %w", name, err)
		}
		return nil
	})
}

// StepCompleted reports whether a named action has completed.
func (s *Store) StepCompleted(ctx context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	var completed bool
	err := s.db.QueryRowContext(ctx, "SELECT completed FROM steps WHERE name=?", name).Scan(&completed)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read step %s: %w", name, err)
	}
	return completed, nil
}
