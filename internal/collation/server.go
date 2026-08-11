package collation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Conn is the subset of *pgx.Conn this package uses.
type Conn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// readQuery reads the collation identity of the connected database.
//
// It goes through to_jsonb rather than naming columns directly because the column
// holding the provider locale was renamed in PostgreSQL 17: daticulocale became
// datlocale. Naming either one directly fails to parse on the other release,
// which would turn a preflight check into a version-dependent probe error, and
// asking pg_attribute first would cost a round trip to learn something a missing
// JSON key already says.
const readQuery = `
	SELECT coalesce(d.j->>'datcollate', ''),
	       coalesce(d.j->>'datctype', ''),
	       coalesce(d.j->>'datlocale', d.j->>'daticulocale', ''),
	       coalesce(d.j->>'datlocprovider', ''),
	       coalesce(d.j->>'datcollversion', '')
	FROM (
		SELECT to_jsonb(db) AS j
		FROM pg_catalog.pg_database db
		WHERE db.datname = current_database()
	) AS d`

// Read returns how the connected database collates text.
func Read(ctx context.Context, conn Conn) (Settings, error) {
	var settings Settings
	if err := conn.QueryRow(ctx, readQuery).Scan(
		&settings.Collate, &settings.CType, &settings.Locale,
		&settings.Provider, &settings.Version,
	); err != nil {
		return Settings{}, fmt.Errorf("read database collation: %w", err)
	}
	return settings, nil
}
