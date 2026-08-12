//go:build integration

package cutover

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/pgtest"
)

func TestPostgres17SequenceSynchronization(t *testing.T) {
	sourceInstance := pgtest.Start(t, 17)
	targetInstance := pgtest.Start(t, 17)
	ctx := context.Background()
	source := sourceInstance.Connect(t)
	target := targetInstance.Connect(t)
	ddl := `
		CREATE SCHEMA "odd""schema";
		CREATE SEQUENCE "odd""schema"."never called";
		CREATE SEQUENCE "odd""schema"."called";
		CREATE SEQUENCE "odd""schema"."descending" INCREMENT BY -1 MINVALUE -10000 MAXVALUE -1 START -1`
	if _, err := source.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `CREATE SEQUENCE "odd""schema"."excluded"`); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(ctx, `
		SELECT setval('"odd""schema"."never called"'::regclass,10,false);
		SELECT setval('"odd""schema"."called"'::regclass,20,true);
		SELECT setval('"odd""schema"."descending"'::regclass,-20,true)`); err != nil {
		t.Fatal(err)
	}
	connect := func(uri string) Connector {
		return func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, uri) }
	}
	selected := []Sequence{
		{Schema: `odd"schema`, Name: "never called"},
		{Schema: `odd"schema`, Name: "called"},
		{Schema: `odd"schema`, Name: "descending"},
	}
	results, err := synchronizeSequences(ctx, connect(sourceInstance.URI), connect(targetInstance.URI), 1000, selected)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("sequence results = %#v", results)
	}
	var never, called, descending int64
	if err := target.QueryRow(ctx, `SELECT nextval('"odd""schema"."never called"')`).Scan(&never); err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(ctx, `SELECT nextval('"odd""schema"."called"')`).Scan(&called); err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(ctx, `SELECT nextval('"odd""schema"."descending"')`).Scan(&descending); err != nil {
		t.Fatal(err)
	}
	if never != 1010 || called != 1021 || descending != -1021 {
		t.Fatalf("next sequence values = %d, %d, %d", never, called, descending)
	}
}
