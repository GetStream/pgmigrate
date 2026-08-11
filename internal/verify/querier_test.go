package verify

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// keyLadder is a querier that answers keyLadderQuery from a literal table, so the
// ladder's preference order can be tested without a server. Everything else in
// this package talks to real PostgreSQL, because everything else is SQL whose
// behaviour is the thing under test.
type keyLadder []keyLadderRow

type keyLadderRow struct {
	index   string
	primary bool
	ord     int
	name    string
	kind    string
	notNull bool
}

func (l keyLadder) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &ladderRows{rows: l, position: -1}, nil
}

func (l keyLadder) QueryRow(context.Context, string, ...any) pgx.Row {
	return errorRow{errors.New("keyLadder answers only the key ladder query")}
}

type ladderRows struct {
	rows     keyLadder
	position int
}

func (r *ladderRows) Next() bool {
	r.position++
	return r.position < len(r.rows)
}

func (r *ladderRows) Scan(dest ...any) error {
	if len(dest) != 6 {
		return fmt.Errorf("keyLadder returns 6 columns, not %d", len(dest))
	}
	row := r.rows[r.position]
	values := []any{row.index, row.primary, row.ord, row.name, row.kind, row.notNull}
	for i, value := range values {
		switch into := dest[i].(type) {
		case *string:
			*into = value.(string)
		case *bool:
			*into = value.(bool)
		case *int:
			*into = value.(int)
		default:
			return fmt.Errorf("keyLadder cannot scan column %d into %T", i, dest[i])
		}
	}
	return nil
}

func (r *ladderRows) Close()                                       {}
func (r *ladderRows) Err() error                                   { return nil }
func (r *ladderRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *ladderRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *ladderRows) Values() ([]any, error)                       { return nil, nil }
func (r *ladderRows) RawValues() [][]byte                          { return nil }
func (r *ladderRows) Conn() *pgx.Conn                              { return nil }

type errorRow struct{ err error }

func (r errorRow) Scan(...any) error { return r.err }
