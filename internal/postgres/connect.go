package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SessionTimeoutNames are the GUCs that cancel a long COPY or restore. A copy
// part is one statement and holds a transaction for its whole duration, so any
// of these inherited from a role or parameter group will kill it.
var SessionTimeoutNames = []string{
	"statement_timeout",
	"lock_timeout",
	"idle_in_transaction_session_timeout",
	"idle_session_timeout",
}

// SessionTimeoutPGOPTIONS disables those GUCs for libpq subprocesses
// (pg_dump, pg_restore) that never see AfterConnect.
const SessionTimeoutPGOPTIONS = "-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0 -c idle_session_timeout=0"

// SessionTimeout is one inherited timeout, in milliseconds. Zero means disabled.
type SessionTimeout struct {
	Name         string
	Milliseconds int64
}

// Connect opens a SQL session and disables the timeouts that would cancel bulk
// work. Replication-protocol connections must not use this: SET is not a
// replication command.
func Connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := DisableSessionTimeouts(ctx, conn); err != nil {
		conn.Close(context.Background())
		return nil, err
	}
	return conn, nil
}

// DisableSessionTimeouts sets the bulk-work timeouts to 0 for the rest of this
// session. It overrides ALTER ROLE and parameter-group defaults; it does not
// persist.
func DisableSessionTimeouts(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, `
		SELECT set_config('statement_timeout','0',false),
		       set_config('lock_timeout','0',false),
		       set_config('idle_in_transaction_session_timeout','0',false),
		       set_config('idle_session_timeout','0',false)`); err != nil {
		return fmt.Errorf("disable session timeouts: %w", err)
	}
	return nil
}

// InheritedSessionTimeouts reports the values RESET would restore, which is
// what the session inherited from role, database, and parameter group, even
// after DisableSessionTimeouts.
func InheritedSessionTimeouts(ctx context.Context, conn *pgx.Conn) ([]SessionTimeout, error) {
	rows, err := conn.Query(ctx, `
		SELECT name, reset_val::bigint
		FROM pg_catalog.pg_settings
		WHERE name = ANY($1)
		ORDER BY name`, SessionTimeoutNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var timeouts []SessionTimeout
	for rows.Next() {
		var timeout SessionTimeout
		if err := rows.Scan(&timeout.Name, &timeout.Milliseconds); err != nil {
			return nil, err
		}
		timeouts = append(timeouts, timeout)
	}
	return timeouts, rows.Err()
}
