package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// UpsertVerifyTable records one table's live check progress and outcome.
func (s *Store) UpsertVerifyTable(ctx context.Context, table VerifyTable) error {
	if table.TableOID == 0 {
		return errors.New("verify table OID is required")
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(
			ctx, `
			INSERT INTO verify_tables
				(table_oid, stage, source_pages, source_pages_total, sampled_rows,
				 estimated_rows, target_rows, rows_per_second, eta_ns, coverage,
				 candidate_rows, cdc_keys, cdc_observed, unresolved_rows, converged,
				 complete, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(table_oid) DO UPDATE SET
				stage=excluded.stage, source_pages=excluded.source_pages,
				source_pages_total=excluded.source_pages_total,
				sampled_rows=excluded.sampled_rows,
				estimated_rows=excluded.estimated_rows,
				target_rows=excluded.target_rows, rows_per_second=excluded.rows_per_second,
				eta_ns=excluded.eta_ns, coverage=excluded.coverage,
				candidate_rows=excluded.candidate_rows, cdc_keys=excluded.cdc_keys,
				cdc_observed=excluded.cdc_observed,
				unresolved_rows=excluded.unresolved_rows, converged=excluded.converged,
				complete=excluded.complete, updated_at=excluded.updated_at`,
			table.TableOID, table.Stage, table.SourcePages, table.SourcePagesTotal,
			table.Sampled, table.Estimated, table.TargetRows, table.Rate,
			int64(table.ETA), table.Coverage, table.Candidates, table.CDCKeys,
			table.CDCObserved, table.Unresolved,
			table.Converged, table.Complete, time.Now().UTC().UnixNano(),
		)
		if err != nil {
			return fmt.Errorf("upsert verify table %d: %w", table.TableOID, err)
		}
		return nil
	})
}

// verifyTableQuery reads progress with the table's name attached. The join is a
// left join because progress is keyed by OID and outlives an inventory edit.
const verifyTableQuery = `
	SELECT v.table_oid, coalesce(t.schema_name,''), coalesce(t.table_name,''),
	 v.stage, v.source_pages, v.source_pages_total, v.sampled_rows,
	 v.estimated_rows, v.target_rows, v.rows_per_second, v.eta_ns, v.coverage,
	 v.candidate_rows, v.cdc_keys, v.cdc_observed, v.unresolved_rows,
	 v.converged, v.complete, v.updated_at
	FROM verify_tables v
	LEFT JOIN tables t ON t.oid = v.table_oid
	ORDER BY t.schema_name, t.table_name`

// ListVerifyTables returns recorded comparison progress, ordered for stable output.
func (s *Store) ListVerifyTables(ctx context.Context) ([]VerifyTable, error) {
	rows, err := s.db.QueryContext(ctx, verifyTableQuery)
	if err != nil {
		return nil, fmt.Errorf("list verify tables: %w", err)
	}
	defer rows.Close()
	return scanVerifyTables(rows)
}

// rowScanner is satisfied by both *sql.Rows and a transaction's rows, so status and
// inventory reads share one scan.
type rowScanner interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanVerifyTables(rows rowScanner) ([]VerifyTable, error) {
	var tables []VerifyTable
	for rows.Next() {
		var (
			table   VerifyTable
			eta     int64
			updated int64
		)
		if err := rows.Scan(&table.TableOID, &table.Schema, &table.Name, &table.Stage,
			&table.SourcePages, &table.SourcePagesTotal, &table.Sampled,
			&table.Estimated, &table.TargetRows, &table.Rate, &eta, &table.Coverage,
			&table.Candidates, &table.CDCKeys, &table.CDCObserved,
			&table.Unresolved, &table.Converged, &table.Complete,
			&updated); err != nil {
			return nil, fmt.Errorf("scan verify table: %w", err)
		}
		table.ETA = time.Duration(eta)
		table.UpdatedAt = fromUnixNano(updated)
		tables = append(tables, table)
	}
	return tables, rows.Err()
}
