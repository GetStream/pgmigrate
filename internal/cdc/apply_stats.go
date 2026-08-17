package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

const applyTableProgressTable = "pgmigrate_internal.cdc_apply_table_progress"

// ApplyProgressStats are durable cumulative counters for the contiguous applied
// source prefix. They advance atomically with target replay progress.
type ApplyProgressStats struct {
	Transactions  int64 `json:"transactions"`
	Rows          int64 `json:"rows"`
	DMLStatements int64 `json:"dml_statements"`
	TargetCommits int64 `json:"target_commits"`
}

// ApplyTableStats are durable cumulative row and row-DML counters for one
// target relation. Both counters cover INSERT, UPDATE, and DELETE; TRUNCATE is
// excluded because pgoutput does not carry the number of rows it removed.
type ApplyTableStats struct {
	Schema        string `json:"schema_name"`
	Table         string `json:"table_name"`
	Rows          int64  `json:"row_changes"`
	DMLStatements int64  `json:"dml_statements"`
}

type applyTableKey struct {
	schema string
	table  string
}

type applyStats struct {
	ApplyProgressStats
	tables map[applyTableKey]*ApplyTableStats
}

func newApplyStats(sourceTransactions int) *applyStats {
	return &applyStats{
		ApplyProgressStats: ApplyProgressStats{
			Transactions:  int64(sourceTransactions),
			TargetCommits: 1,
		},
		tables: make(map[applyTableKey]*ApplyTableStats),
	}
}

func (s *applyStats) addRows(relation *targetRelation, rows int64) {
	if s == nil || relation == nil || rows <= 0 {
		return
	}
	table := s.table(relation)
	table.Rows += rows
	s.Rows += rows
}

func (s *applyStats) addDMLStatement(relation *targetRelation) {
	if s == nil || relation == nil {
		return
	}
	table := s.table(relation)
	table.DMLStatements++
	s.DMLStatements++
}

func (s *applyStats) table(relation *targetRelation) *ApplyTableStats {
	key := applyTableKey{
		schema: relation.source.Namespace,
		table:  relation.source.Name,
	}
	table := s.tables[key]
	if table == nil {
		table = &ApplyTableStats{Schema: key.schema, Table: key.table}
		s.tables[key] = table
	}
	return table
}

func (s *applyStats) tableSnapshot() []ApplyTableStats {
	if s == nil {
		return nil
	}
	tables := make([]ApplyTableStats, 0, len(s.tables))
	for _, table := range s.tables {
		tables = append(tables, *table)
	}
	slices.SortFunc(tables, func(left, right ApplyTableStats) int {
		if order := cmpString(left.Schema, right.Schema); order != 0 {
			return order
		}
		return cmpString(left.Table, right.Table)
	})
	return tables
}

func (s *applyStats) tableJSON() []byte {
	// ApplyTableStats contains only strings and integers, so encoding cannot
	// encounter an unsupported value.
	data, _ := json.Marshal(s.tableSnapshot())
	return data
}

func cmpString(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

// ReadApplyStats reads the target-authoritative counters associated with the
// current stream generation.
func ReadApplyStats(
	ctx context.Context,
	conn *pgx.Conn,
	streamID string,
	generation string,
) (ApplyProgressStats, []ApplyTableStats, error) {
	_, _, progress, tables, err := ReadApplySnapshot(
		ctx, conn, streamID, generation,
	)
	return progress, tables, err
}

// ReadApplySnapshot reads canonical progress and all per-table counters in one
// PostgreSQL statement, so the values come from the same MVCC snapshot.
func ReadApplySnapshot(
	ctx context.Context,
	conn *pgx.Conn,
	streamID string,
	generation string,
) (LSN, bool, ApplyProgressStats, []ApplyTableStats, error) {
	rows, err := conn.Query(ctx, `
		SELECT progress.remote_lsn::text,
		       progress.source_transactions,
		       progress.row_changes,
		       progress.dml_statements,
		       progress.target_commits,
		       coalesce(table_progress.schema_name, ''),
		       coalesce(table_progress.table_name, ''),
		       coalesce(table_progress.row_changes, 0),
		       coalesce(table_progress.dml_statements, 0)
		FROM `+cdcProgressTable+` AS progress
		LEFT JOIN `+applyTableProgressTable+` AS table_progress
		  ON table_progress.stream_id = progress.stream_id
		 AND table_progress.stream_generation = progress.stream_generation
		WHERE progress.stream_id = $1
		  AND progress.stream_generation = $2
		ORDER BY table_progress.schema_name, table_progress.table_name
	`, streamID, generation)
	if err != nil {
		return 0, false, ApplyProgressStats{}, nil, fmt.Errorf(
			"cdc: read apply snapshot: %w", err,
		)
	}
	defer rows.Close()
	var (
		remoteLSN string
		progress  ApplyProgressStats
		tables    []ApplyTableStats
		found     bool
	)
	for rows.Next() {
		var table ApplyTableStats
		if err := rows.Scan(
			&remoteLSN,
			&progress.Transactions,
			&progress.Rows,
			&progress.DMLStatements,
			&progress.TargetCommits,
			&table.Schema,
			&table.Table,
			&table.Rows,
			&table.DMLStatements,
		); err != nil {
			return 0, false, ApplyProgressStats{}, nil, fmt.Errorf(
				"cdc: scan apply snapshot: %w", err,
			)
		}
		found = true
		if table.Schema != "" || table.Table != "" {
			tables = append(tables, table)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, false, ApplyProgressStats{}, nil, fmt.Errorf(
			"cdc: iterate apply snapshot: %w", err,
		)
	}
	if !found {
		return 0, false, ApplyProgressStats{}, nil, nil
	}
	parsed, err := pglogrepl.ParseLSN(remoteLSN)
	if err != nil {
		return 0, false, ApplyProgressStats{}, nil, fmt.Errorf(
			"cdc: parse apply progress %q: %w", remoteLSN, err,
		)
	}
	return LSN(parsed), true, progress, tables, nil
}

// ReadApplyProgress reads the canonical target LSN and its counters from one
// row, so a local status never pairs counters with a different progress sample.
func ReadApplyProgress(
	ctx context.Context,
	conn *pgx.Conn,
	streamID string,
	generation string,
) (LSN, bool, ApplyProgressStats, error) {
	var remoteLSN string
	var progress ApplyProgressStats
	err := conn.QueryRow(ctx, `
		SELECT remote_lsn::text, source_transactions, row_changes,
		       dml_statements, target_commits
		FROM `+cdcProgressTable+`
		WHERE stream_id = $1 AND stream_generation = $2
	`, streamID, generation).Scan(
		&remoteLSN,
		&progress.Transactions,
		&progress.Rows,
		&progress.DMLStatements,
		&progress.TargetCommits,
	)
	if err == pgx.ErrNoRows {
		return 0, false, ApplyProgressStats{}, nil
	}
	if err != nil {
		return 0, false, ApplyProgressStats{}, fmt.Errorf("cdc: read apply statistics: %w", err)
	}
	parsed, err := pglogrepl.ParseLSN(remoteLSN)
	if err != nil {
		return 0, false, ApplyProgressStats{}, fmt.Errorf(
			"cdc: parse apply progress %q: %w", remoteLSN, err,
		)
	}
	return LSN(parsed), true, progress, nil
}
