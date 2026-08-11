package verify

import (
	"context"
	"fmt"
	"slices"
)

// KeyColumn describes one key column in index order.
type KeyColumn struct {
	Name string `json:"name"`
	// Type is the column's rendered type, used to cast a parameter back to the
	// column's own type so re-reading a row by key stays an index lookup.
	Type    string `json:"type"`
	NotNull bool   `json:"not_null"`
}

// Key is the key chosen for one table: how a sampled row is found on the target,
// and how a differing row is named and re-read.
//
// Nothing here needs the key to be orderable. Every lookup is equality, so a key on
// text carries no assumption that the two servers sort text the same way, which is
// why this design has no collation prerequisite.
type Key struct {
	Columns []KeyColumn `json:"columns,omitempty"`
	Primary bool        `json:"primary,omitempty"`
}

func (k Key) present() bool { return len(k.Columns) != 0 }

// keyLadderQuery reads every valid, unconditional, non-expression unique or
// primary-key index of one table.
const keyLadderQuery = `
	SELECT ci.relname, i.indisprimary, k.ord, a.attname,
	       pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull
	FROM pg_catalog.pg_index i
	JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
	JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, ord) ON k.ord <= i.indnkeyatts
	JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indrelid = $1 AND i.indisvalid AND i.indisready
	  AND (i.indisprimary OR i.indisunique)
	  AND i.indpred IS NULL AND i.indexprs IS NULL
	ORDER BY ci.relname, k.ord`

// chooseKey applies the key ladder: the primary key, else the narrowest unique
// index whose columns are all NOT NULL, else no key at all. A table without one is
// not an error, but it cannot be checked, and the second return value is the
// explanation the run reports instead of a comparison.
func chooseKey(ctx context.Context, db querier, oid uint32) (Key, string, error) {
	rows, err := db.Query(ctx, keyLadderQuery, oid)
	if err != nil {
		return Key{}, "", fmt.Errorf("read key candidates for table %d: %w", oid, err)
	}
	defer rows.Close()
	candidates := make(map[string]*Key)
	var order []string
	for rows.Next() {
		var (
			index   string
			primary bool
			ord     int
			column  KeyColumn
		)
		if err := rows.Scan(&index, &primary, &ord, &column.Name,
			&column.Type, &column.NotNull); err != nil {
			return Key{}, "", fmt.Errorf("scan key candidate: %w", err)
		}
		candidate, seen := candidates[index]
		if !seen {
			candidate = &Key{Primary: primary}
			candidates[index] = candidate
			order = append(order, index)
		}
		candidate.Columns = append(candidate.Columns, column)
	}
	if err := rows.Err(); err != nil {
		return Key{}, "", fmt.Errorf("iterate key candidates: %w", err)
	}
	slices.Sort(order)

	for _, name := range order {
		if candidates[name].Primary {
			return *candidates[name], "", nil
		}
	}
	// A nullable key column cannot be re-read by equality, because the row with
	// NULL in it would match nothing, so such an index is not a usable key even
	// though it is unique.
	var best *Key
	nullable := false
	for _, name := range order {
		candidate := candidates[name]
		if slices.ContainsFunc(candidate.Columns, func(c KeyColumn) bool { return !c.NotNull }) {
			nullable = true
			continue
		}
		if best == nil || len(candidate.Columns) < len(best.Columns) {
			best = candidate
		}
	}
	if best != nil {
		return *best, "", nil
	}
	switch {
	case nullable:
		return Key{}, "its only unique index allows NULL in a key column", nil
	default:
		return Key{}, "it has no primary key and no usable unique index", nil
	}
}
