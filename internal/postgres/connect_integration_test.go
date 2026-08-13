//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPG17ConnectDisablesInheritedStatementTimeout(t *testing.T) {
	instance := pgtest.Start(t, 17)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	admin := instance.Connect(t)
	if _, err := admin.Exec(ctx, "ALTER DATABASE pgmigrate SET statement_timeout = '1s'"); err != nil {
		t.Fatal(err)
	}

	raw, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close(context.Background())
	var inherited string
	if err := raw.QueryRow(ctx, "SHOW statement_timeout").Scan(&inherited); err != nil {
		t.Fatal(err)
	}
	if inherited != "1s" && inherited != "1000ms" {
		t.Fatalf("inherited statement_timeout = %q", inherited)
	}

	conn, err := postgres.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var current string
	if err := conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != "0" {
		t.Fatalf("session statement_timeout = %q, want 0", current)
	}
	timeouts, err := postgres.InheritedSessionTimeouts(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	var statement int64 = -1
	for _, timeout := range timeouts {
		if timeout.Name == "statement_timeout" {
			statement = timeout.Milliseconds
		}
	}
	if statement != 1000 {
		t.Fatalf("reset_val statement_timeout = %d, want 1000", statement)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_sleep(1.2)"); err != nil {
		t.Fatalf("pg_sleep on a disabled-timeout session: %v", err)
	}
	_, err = raw.Exec(ctx, "SELECT pg_sleep(1.2)")
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "57014" {
		t.Fatalf("raw pg_sleep error = %v, want SQLSTATE 57014", err)
	}
}
