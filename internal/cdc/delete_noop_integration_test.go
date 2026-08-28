//go:build integration

package cdc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
)

func TestPrimaryKeyDeleteNoop(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprint(major), func(t *testing.T) {
			target := pgtest.Start(t, major)
			conn := target.Connect(t)
			ctx := t.Context()
			if _, err := conn.Exec(ctx, `
				CREATE DOMAIN public.delete_key AS bigint CHECK (VALUE > 0);
				CREATE TABLE public.delete_uuid (id uuid PRIMARY KEY, value text);
				CREATE TABLE public.delete_stage (id public.delete_key PRIMARY KEY, value text);
				CREATE TABLE public.delete_composite (id text, value text, PRIMARY KEY (value, id));
				CREATE TABLE public.delete_full (id text, value text);
				CREATE TABLE public.delete_trigger (id text PRIMARY KEY, value text);
				CREATE FUNCTION public.suppress_delete() RETURNS trigger LANGUAGE plpgsql AS
				$$ BEGIN RETURN NULL; END $$;
				CREATE TRIGGER suppress_delete BEFORE DELETE ON public.delete_trigger
				FOR EACH ROW EXECUTE FUNCTION public.suppress_delete();
				ALTER TABLE public.delete_trigger ENABLE ALWAYS TRIGGER suppress_delete;
				INSERT INTO public.delete_trigger VALUES ('1', 'keep');
			`); err != nil {
				t.Fatal(err)
			}
			if err := configureApplySession(ctx, conn); err != nil {
				t.Fatal(err)
			}
			statements := newApplyStatementCache(applyStatementCacheCapacity)
			for _, tc := range []struct {
				name, table, path string
				typeOID           uint32
				rows              int
				strict            bool
			}{
				{name: "single UUID", table: "delete_uuid", path: "single", typeOID: 2950, rows: 1},
				{name: "VALUES UUID", table: "delete_uuid", path: "values", typeOID: 2950, rows: 4},
				{name: "array UUID", table: "delete_uuid", path: "array", typeOID: 2950, rows: 4},
				{name: "typed stage", table: "delete_stage", path: "stage", typeOID: 90001, rows: minimumTextCopyStageRows},
				{name: "composite catalog order", table: "delete_composite", path: "array", typeOID: 25, rows: 4},
				{name: "full identity stays strict", table: "delete_full", path: "single", typeOID: 25, rows: 1, strict: true},
				{name: "full identity batch stays strict", table: "delete_full", path: "values", typeOID: 25, rows: 4, strict: true},
				{name: "suppressed delete stays strict", table: "delete_trigger", path: "single", typeOID: 25, rows: 1, strict: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					source := Relation{
						OID: 9001, Namespace: "public", Name: tc.table, ReplicaIdentity: 'd',
						Columns: []Column{{Name: "id", Type: tc.typeOID, Flags: 1}, {Name: "value", Type: 25}},
					}
					if tc.table == "delete_composite" {
						source.Columns[1].Flags = 1
					}
					if tc.table == "delete_full" {
						source.ReplicaIdentity = 'f'
						source.Columns[1].Flags = 1
					}
					relation, err := loadTargetRelation(ctx, conn, &source)
					if err != nil {
						t.Fatal(err)
					}
					primary, safe := primaryKeyDeleteColumns(relation)
					if safe == tc.strict {
						t.Fatalf("primary-key delete safety=%t, strict=%t", safe, tc.strict)
					}
					if !safe {
						primary = batchUpdateIdentityColumns(relation)
						if tc.table == "delete_full" {
							primary = relation.columns
						}
					}
					changes := make([]Change, tc.rows)
					for i := range changes {
						id := fmt.Sprint(i + 1)
						if tc.typeOID == 2950 {
							id = fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1)
						}
						changes[i] = Change{RelationOID: source.OID, Kind: ChangeDelete, Old: replayTuple(id, "keep")}
					}
					// Run an entirely absent batch, then a mixed existing/absent batch.
					for _, populate := range []bool{false, true} {
						if populate && !tc.strict {
							for i := 0; i < len(changes); i += 2 {
								if _, err := conn.Exec(ctx, "INSERT INTO "+relation.quoted+" VALUES ($1, 'keep')",
									string((*changes[i].Old)[0].Data)); err != nil {
									t.Fatal(err)
								}
							}
						}
						replay := newApplyPipeline(ctx, conn.PgConn(), statements)
						defer replay.abort()
						replay.begin()
						switch tc.path {
						case "single":
							err = applyDelete(replay, relation, &changes[0])
						case "values":
							err = applyDeleteValueChunk(replay, relation, primary, changes)
						case "array":
							var applied bool
							applied, err = applyDeleteArrayChunk(replay, relation, primary, changes)
							if !applied && err == nil {
								t.Fatal("array path was not exercised")
							}
						case "stage":
							var applied bool
							applied, err = applyDeleteTextStage(replay, relation, primary, changes)
							if !applied && err == nil {
								t.Fatal("typed stage path was not exercised")
							}
						}
						if err == nil {
							err = replay.sync()
						}
						if tc.strict {
							var divergence *DivergenceError
							if !errors.As(err, &divergence) {
								t.Fatalf("strict delete error=%v, want divergence", err)
							}
							if !strings.Contains(err.Error(), "affected 0 rows") && !strings.Contains(err.Error(), "did not match identity ordinal") {
								t.Fatalf("strict delete failed for the wrong reason: %v", err)
							}
							if err := replay.abort(); err != nil {
								t.Fatal(err)
							}
							return
						}
						if err != nil {
							replay.abort()
							t.Fatal(err)
						}
						replay.commit()
						if err := replay.sync(); err != nil {
							t.Fatal(err)
						}
						if err := replay.close(); err != nil {
							t.Fatal(err)
						}
						var rows int
						if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+relation.quoted).Scan(&rows); err != nil {
							t.Fatal(err)
						}
						if rows != 0 {
							t.Fatalf("delete left %d rows", rows)
						}
					}
				})
			}
		})
	}
}

func TestMissingDeletePreservesTransactionAtomicity(t *testing.T) {
	target := pgtest.Start(t, 18)
	conn := target.Connect(t)
	ctx := t.Context()
	if _, err := conn.Exec(ctx, `CREATE TABLE public.noop_atomic (id text PRIMARY KEY, value text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	relation := replayTestRelation(9002, "noop_atomic")
	statements := newApplyStatementCache(applyStatementCacheCapacity)
	for _, fail := range []bool{true, false} {
		stream := fmt.Sprintf("noop-atomic-%t", fail)
		if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
			StreamID: stream, Generation: stream, FreshSetup: true, TargetHasCopiedData: true,
		}); err != nil {
			t.Fatal(err)
		}
		changes := []Change{
			{RelationOID: relation.source.OID, Kind: ChangeDelete, Old: replayTuple("same-key", "old")},
			{RelationOID: relation.source.OID, Kind: ChangeInsert, New: replayTuple("same-key", "source")},
		}
		if fail {
			changes = append(changes, Change{
				RelationOID: relation.source.OID, Kind: ChangeInsert,
				New: &Tuple{{Kind: DatumText, Data: []byte("invalid")}, {Kind: DatumNull}},
			})
		}
		transaction := replayTestTransaction(1000, relation, changes...)
		applier := &Applier{config: ApplierConfig{StreamID: stream, StreamGeneration: stream}}
		err := applier.applyTransaction(ctx, conn, newTargetRelationCache(), statements, 0, &transaction)
		if fail {
			var divergence *DivergenceError
			if !errors.As(err, &divergence) {
				t.Fatalf("SQL error=%v, want divergence", err)
			}
			if !strings.Contains(err.Error(), "SQLSTATE 23502") {
				t.Fatalf("transaction failed before the NOT NULL violation: %v", err)
			}
			if _, exists, err := postgres.ReadReplicationProgress(ctx, conn, stream); err != nil || exists {
				t.Fatalf("failed transaction published progress: exists=%t, err=%v", exists, err)
			}
			var rows int
			if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.noop_atomic").Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("failed transaction retained rows=%d, err=%v", rows, err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		assertReplayProgress(t, conn, stream, transaction.EndLSN, 1, 2)
		var value string
		if err := conn.QueryRow(ctx, "SELECT value FROM public.noop_atomic WHERE id='same-key'").Scan(&value); err != nil || value != "source" {
			t.Fatalf("delete then insert value=%q, err=%v", value, err)
		}
	}
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("NULL identity with %d deletes", count), func(t *testing.T) {
			stream := fmt.Sprintf("null-delete-%d", count)
			if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
				StreamID: stream, Generation: stream, FreshSetup: true, TargetHasCopiedData: true,
			}); err != nil {
				t.Fatal(err)
			}
			changes := make([]Change, count)
			for i := range changes {
				changes[i] = Change{RelationOID: relation.source.OID, Kind: ChangeDelete, Old: &Tuple{{Kind: DatumNull}, {Kind: DatumNull}}}
			}
			transaction := replayTestTransaction(2000, relation, changes...)
			applier := &Applier{config: ApplierConfig{StreamID: stream, StreamGeneration: stream}}
			err := applier.applyTransaction(ctx, conn, newTargetRelationCache(), statements, 0, &transaction)
			var divergence *DivergenceError
			if !errors.As(err, &divergence) || !strings.Contains(err.Error(), "primary key contains NULL") {
				t.Fatalf("invalid DELETE identity error=%v", err)
			}
			if _, exists, err := postgres.ReadReplicationProgress(ctx, conn, stream); err != nil || exists {
				t.Fatalf("invalid identity published progress: exists=%t, err=%v", exists, err)
			}
		})
	}
}

func TestDeleteNoopKeepsResultValidation(t *testing.T) {
	target := pgtest.Start(t, 18)
	conn := target.Connect(t)
	statements := newApplyStatementCache(applyStatementCacheCapacity)
	for _, tc := range []struct {
		name, query, want string
		ordinals          int
		allowMissing      bool
	}{
		{name: "absent ordinal", query: "SELECT 0 WHERE false", ordinals: 1, allowMissing: true},
		{name: "duplicate ordinal", query: "SELECT 0 UNION ALL SELECT 0", ordinals: 1, allowMissing: true, want: "more than once"},
		{name: "negative ordinal", query: "SELECT -1", ordinals: 1, allowMissing: true, want: "invalid identity ordinal"},
		{name: "out of range ordinal", query: "SELECT 1", ordinals: 1, allowMissing: true, want: "invalid identity ordinal"},
		{name: "malformed ordinal", query: "SELECT 'bad'", ordinals: 1, allowMissing: true, want: "invalid identity ordinal"},
		{name: "extra column", query: "SELECT 0, 0", ordinals: 1, allowMissing: true, want: "identity columns, expected 1"},
		{name: "too many affected rows", query: "SELECT 0 UNION ALL SELECT 1", allowMissing: true, want: "affected 2 rows, expected 1"},
		{name: "strict row count", query: "SELECT 0 WHERE false", want: "affected 0 rows, expected 1"},
		{name: "strict ordinal", query: "SELECT 0 WHERE false", ordinals: 1, want: "did not match identity ordinal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replay := newApplyPipeline(t.Context(), conn.PgConn(), statements)
			defer replay.abort()
			replay.begin()
			err := replay.queue(tc.query, nil, applyExpectation{
				kind: ChangeDelete, expectedRows: 1, expectedOrdinals: tc.ordinals, allowMissingRows: tc.allowMissing,
			})
			if err == nil {
				err = replay.sync()
			}
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var divergence *DivergenceError
				if !errors.As(err, &divergence) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("validation error=%v, want divergence containing %q", err, tc.want)
				}
			}
		})
	}
}
