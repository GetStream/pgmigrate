package replident

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Conn is the subset of *pgx.Conn this package uses.
type Conn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// inspectQuery expands each selected relation into the relations that actually
// hold rows, and reports the replica identity of each.
//
// pg_partition_tree does the expansion, returning a partitioned table's whole
// hierarchy to any depth. It returns no rows at all for a relation that is
// neither a partition nor partitioned, so the lateral join is a LEFT one and an
// ordinary table falls back to standing for itself as a level-0 leaf. Keeping
// only isleaf rows drops the intermediate parents, whose relreplident is never
// consulted because they hold no rows.
const inspectQuery = `
	WITH selected AS (
		SELECT DISTINCT
		       coalesce(tree.relid, input.oid) AS relid,
		       coalesce(tree.isleaf, true) AS isleaf,
		       coalesce(tree.level, 0) AS level
		FROM unnest($1::oid[]) AS input(oid)
		LEFT JOIN LATERAL pg_catalog.pg_partition_tree(input.oid) AS tree ON true
	)
	SELECT c.oid, n.nspname, c.relname, c.relreplident::text,
	       coalesce((SELECT x.relname
	                 FROM pg_catalog.pg_index i
	                 JOIN pg_catalog.pg_class x ON x.oid = i.indexrelid
	                 WHERE i.indrelid = c.oid AND i.indisreplident), ''),
	       EXISTS (SELECT FROM pg_catalog.pg_index i
	               WHERE i.indrelid = c.oid AND i.indisprimary AND i.indisvalid),
	       EXISTS (SELECT FROM pg_catalog.pg_index i
	               WHERE i.indrelid = c.oid AND i.indisreplident AND i.indisvalid),
	       pg_catalog.pg_has_role(c.relowner, 'USAGE'),
	       bool_or(selected.level > 0),
	       coalesce((SELECT pn.nspname || '.' || pc.relname
	                 FROM pg_catalog.pg_inherits h
	                 JOIN pg_catalog.pg_class pc ON pc.oid = h.inhparent
	                 JOIN pg_catalog.pg_namespace pn ON pn.oid = pc.relnamespace
	                 WHERE h.inhrelid = c.oid), ''),
	       pg_catalog.pg_table_size(c.oid),
	       coalesce((SELECT s.n_tup_upd + s.n_tup_del
	                 FROM pg_catalog.pg_stat_all_tables s WHERE s.relid = c.oid), 0)
	FROM selected
	JOIN pg_catalog.pg_class c ON c.oid = selected.relid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE selected.isleaf AND c.relkind = 'r'
	GROUP BY c.oid, n.nspname, c.relname, c.relreplident, c.relowner
	ORDER BY n.nspname, c.relname`

// Inspect returns every storage-bearing relation reachable from the selection,
// substituting the leaf partitions of a selected partitioned table for the
// parent.
func Inspect(ctx context.Context, conn Conn, selected []Table) ([]Relation, error) {
	if len(selected) == 0 {
		return nil, nil
	}
	oids := make([]uint32, 0, len(selected))
	for _, table := range selected {
		oids = append(oids, table.OID)
	}
	rows, err := conn.Query(ctx, inspectQuery, oids)
	if err != nil {
		return nil, fmt.Errorf("inspect replica identities: %w", err)
	}
	defer rows.Close()

	var relations []Relation
	for rows.Next() {
		var relation Relation
		if err := rows.Scan(
			&relation.OID, &relation.Schema, &relation.Name, &relation.Identity,
			&relation.IdentityIndex, &relation.HasValidPrimaryKey, &relation.HasValidIdentityIndex,
			&relation.Owned, &relation.Partition, &relation.Parent,
			&relation.SizeBytes, &relation.RowWrites,
		); err != nil {
			return nil, fmt.Errorf("scan replica identity: %w", err)
		}
		if !knownIdentity(relation.Identity) {
			return nil, fmt.Errorf("relation %s reports unrecognized replica identity %q",
				relation.Identifier(), relation.Identity)
		}
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inspect replica identities: %w", err)
	}
	return relations, nil
}

// Existing filters records down to the relations that exist on this server.
//
// It is here for the target, where the same records describe relations that may
// or may not have been created yet: a run cleaned up before its schema was
// restored has records for target tables that were never made, and that is not an
// error to report but a set of relations with nothing to restore.
func Existing(ctx context.Context, conn Conn, records []Record) ([]Record, error) {
	if len(records) == 0 {
		return nil, nil
	}
	schemas := make([]string, 0, len(records))
	names := make([]string, 0, len(records))
	for _, record := range records {
		schemas = append(schemas, record.Schema)
		names = append(names, record.Table)
	}
	rows, err := conn.Query(ctx, `
		SELECT input.schema, input.name
		FROM unnest($1::text[], $2::text[]) AS input(schema, name)
		JOIN pg_catalog.pg_namespace n ON n.nspname = input.schema
		JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = input.name
		WHERE c.relkind = 'r'`, schemas, names)
	if err != nil {
		return nil, fmt.Errorf("look up recorded relations: %w", err)
	}
	defer rows.Close()
	present := map[[2]string]bool{}
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return nil, fmt.Errorf("scan recorded relation: %w", err)
		}
		present[[2]string{schema, name}] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("look up recorded relations: %w", err)
	}
	var existing []Record
	for _, record := range records {
		if present[[2]string{record.Schema, record.Table}] {
			existing = append(existing, record)
		}
	}
	return existing, nil
}

// Recorder stores what a fallback will undo, before it is applied. Apply refuses
// to alter a relation whose original it could not record, because a source left
// at FULL with no record of what it was is a change nobody can reverse.
type Recorder interface {
	// Recorded reports whether this relation's original is already stored, which
	// it will be when an earlier attempt was interrupted after recording it.
	Recorded(ctx context.Context, oid uint32) (bool, error)
	// Record stores the original durably.
	Record(ctx context.Context, record Record) error
}

// Apply sets REPLICA IDENTITY FULL on each relation, recording what it replaced
// first. It returns the records for the relations it altered alongside any error,
// so a caller that fails partway knows what there is to undo.
//
// Ownership is checked here rather than left to PostgreSQL, so the error names
// the relation and the reason instead of surfacing a bare permission failure from
// a statement the operator never wrote.
func Apply(ctx context.Context, conn Conn, relations []Relation, recorder Recorder) ([]Record, error) {
	var applied []Record
	for _, relation := range relations {
		if !relation.Owned {
			return applied, fmt.Errorf(
				"cannot set REPLICA IDENTITY FULL on %s: the migration role does not own it",
				relation.Identifier(),
			)
		}
		record := RecordOf(relation)
		// Recording before altering is what makes an interrupted fallback
		// recoverable: a crash in between leaves a relation that was never
		// changed and a record that reverts it to itself.
		recorded, err := recorder.Recorded(ctx, relation.OID)
		if err != nil {
			return applied, err
		}
		if !recorded {
			if err := recorder.Record(ctx, record); err != nil {
				return applied, err
			}
		}
		if _, err := conn.Exec(
			ctx,
			"ALTER TABLE "+relation.Identifier()+" REPLICA IDENTITY FULL",
		); err != nil {
			return applied, fmt.Errorf("set replica identity full for %s: %w",
				relation.Identifier(), err)
		}
		applied = append(applied, record)
	}
	return applied, nil
}

// Revert restores each recorded original. It attempts every record and joins the
// failures, so one relation that refuses to move does not leave the rest at FULL.
func Revert(ctx context.Context, conn Conn, records []Record) error {
	var errs []error
	for _, record := range records {
		clause, err := record.Clause()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if _, err := conn.Exec(
			ctx,
			"ALTER TABLE "+record.Identifier()+" REPLICA IDENTITY "+clause,
		); err != nil {
			errs = append(errs, fmt.Errorf("restore replica identity for %s: %w",
				record.Identifier(), err))
		}
	}
	return errors.Join(errs...)
}
