package state

import (
	"context"
	"database/sql"
	"fmt"
)

// Snapshot returns a transactionally consistent view of migration progress.
func (s *Store) Snapshot(ctx context.Context) (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Status{}, ErrClosed
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Status{}, fmt.Errorf("begin status snapshot: %w", err)
	}
	defer tx.Rollback()

	var status Status
	var createdAt, updatedAt int64
	if err := tx.QueryRowContext(
		ctx, `
		SELECT source_fingerprint, filter_fingerprint, slot_name, snapshot_name,
			consistent_point, phase, end_position, created_at, updated_at
		FROM migration WHERE id=1`,
	).Scan(
		&status.Migration.SourceFingerprint,
		&status.Migration.FilterFingerprint,
		&status.Migration.SlotName,
		&status.Migration.SnapshotName,
		&status.Migration.ConsistentPoint,
		&status.Migration.Phase,
		&status.Migration.EndPosition,
		&createdAt,
		&updatedAt,
	); err != nil {
		return Status{}, fmt.Errorf("read migration status: %w", err)
	}
	status.Migration.CreatedAt = fromUnixNano(createdAt)
	status.Migration.UpdatedAt = fromUnixNano(updatedAt)

	counts := []struct {
		query string
		out   *Counts
	}{
		{"SELECT coalesce(sum(completed), 0), count(*) FROM tables", &status.Tables},
		{"SELECT coalesce(sum(completed), 0), count(*) FROM parts", &status.Parts},
		{"SELECT coalesce(sum(completed), 0), count(*) FROM indexes", &status.Indexes},
		{"SELECT coalesce(sum(completed), 0), count(*) FROM constraints", &status.Constraints},
		{"SELECT coalesce(sum(complete), 0), count(*) FROM verify_tables", &status.VerifyTables},
	}
	for _, count := range counts {
		if err := tx.QueryRowContext(ctx, count.query).Scan(&count.out.Done, &count.out.Total); err != nil {
			return Status{}, fmt.Errorf("read status counts: %w", err)
		}
	}
	verifyRows, err := tx.QueryContext(ctx, verifyTableQuery)
	if err != nil {
		return Status{}, fmt.Errorf("read verification progress: %w", err)
	}
	status.Verification, err = scanVerifyTables(verifyRows)
	verifyRows.Close()
	if err != nil {
		return Status{}, err
	}
	if err := tx.QueryRowContext(
		ctx,
		"SELECT count(*) FROM findings WHERE resolved=0",
	).Scan(&status.OpenFindings); err != nil {
		return Status{}, fmt.Errorf("read finding count: %w", err)
	}
	if err := tx.QueryRowContext(
		ctx,
		"SELECT count(*) FROM steps WHERE completed=1",
	).Scan(&status.CompletedSteps); err != nil {
		return Status{}, fmt.Errorf("read step count: %w", err)
	}
	var applyUpdated int64
	if err := tx.QueryRowContext(
		ctx, `
		SELECT staged_lsn, applied_lsn, txns, rows_applied, updated_at
		FROM apply_progress WHERE id=1`,
	).Scan(
		&status.Apply.StagedLSN,
		&status.Apply.AppliedLSN,
		&status.Apply.Txns,
		&status.Apply.Rows,
		&applyUpdated,
	); err != nil {
		return Status{}, fmt.Errorf("read apply progress: %w", err)
	}
	status.Apply.UpdatedAt = fromUnixNano(applyUpdated)

	if err := tx.Commit(); err != nil {
		return Status{}, fmt.Errorf("finish status snapshot: %w", err)
	}
	return status, nil
}

// Migration returns current singleton metadata.
func (s *Store) Migration(ctx context.Context) (Migration, error) {
	status, err := s.Snapshot(ctx)
	if err != nil {
		return Migration{}, err
	}
	return status.Migration, nil
}
