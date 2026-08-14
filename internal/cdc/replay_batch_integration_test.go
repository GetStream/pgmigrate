//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
)

func TestPG17ReplayBatchCollapsesSerializedCommitLane(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE public.replay_unbatched (id integer PRIMARY KEY);
		CREATE TABLE public.replay_batched (id integer PRIMARY KEY);
		CREATE TABLE public.replay_batch_guard (id integer PRIMARY KEY);
	`); err != nil {
		t.Fatal(err)
	}

	const transactionCount = 1024
	run := func(table, stream string, batchSize int) time.Duration {
		t.Helper()
		directory := t.TempDir()
		writer, _, err := OpenWriter(WriterConfig{Directory: directory})
		if err != nil {
			t.Fatal(err)
		}
		relation := Relation{
			OID: 3001, Namespace: "public", Name: table, ReplicaIdentity: 'd',
			Columns: []Column{{Name: "id", Type: 23, Flags: 1}},
		}
		for i := 1; i <= transactionCount; i++ {
			value := Tuple{{Kind: DatumText, Data: []byte(fmt.Sprint(i))}}
			transaction := Transaction{
				CommitLSN: LSN(i*2 - 1), EndLSN: LSN(i * 2), Relations: []Relation{relation},
				Changes: []Change{{RelationOID: relation.OID, Kind: ChangeInsert, New: &value}},
			}
			if _, err := writer.AppendFrame(&transaction); err != nil {
				t.Fatal(err)
			}
		}
		durableLSN, err := writer.Sync()
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		durable := new(DurableWatermark)
		durable.Publish(durableLSN)
		applier, err := NewApplier(ApplierConfig{
			ConnString: target.URI, Directory: directory,
			Workers: 8, BatchSize: batchSize, Window: 512,
			StreamID: stream, StreamGeneration: stream + "-generation", Durable: durable,
			EndPosition: func(context.Context) (LSN, bool, error) {
				return durableLSN, true, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		if err := applier.Run(ctx); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(started)
		var rows int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public."+table).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != transactionCount {
			t.Fatalf("%s rows = %d, want %d", table, rows, transactionCount)
		}
		progress, exists, err := postgres.ReadProgress(ctx, conn, stream)
		if err != nil || !exists || LSN(progress) != durableLSN {
			t.Fatalf("%s progress = %x/%t (%v), want %x", stream, progress, exists, err, durableLSN)
		}
		return elapsed
	}

	unbatched := run("replay_unbatched", "replay-unbatched", 1)
	batched := run("replay_batched", "replay-batched", 64)
	t.Logf("1024 same-table transactions: unbatched=%s batched=%s speedup=%.1fx",
		unbatched, batched, float64(unbatched)/float64(batched))
	if batched*2 >= unbatched {
		t.Fatalf("batched replay %s is not at least 2x faster than unbatched %s", batched, unbatched)
	}

	const guardStream = "replay-batch-guard"
	if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
		StreamID: guardStream, Generation: "correct-generation", FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, conn); err != nil {
		t.Fatal(err)
	}
	relation := Relation{
		OID: 3002, Namespace: "public", Name: "replay_batch_guard", ReplicaIdentity: 'd',
		Columns: []Column{{Name: "id", Type: 23, Flags: 1}},
	}
	transactions := make([]Transaction, 2)
	for i := range transactions {
		value := Tuple{{Kind: DatumText, Data: []byte(fmt.Sprint(i + 1))}}
		transactions[i] = Transaction{
			CommitLSN: LSN(i*2 + 1), EndLSN: LSN(i*2 + 2), Relations: []Relation{relation},
			Changes: []Change{{RelationOID: relation.OID, Kind: ChangeInsert, New: &value}},
		}
	}
	guard := &Applier{config: ApplierConfig{
		StreamID: guardStream, StreamGeneration: "wrong-generation",
	}}
	prepared, err := guard.prepareTransactions(
		ctx, conn, newTargetRelationCache(), newApplyStatementCache(applyStatementCacheCapacity), transactions,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.commitPreparedTransaction(prepared, transactions[1].EndLSN); !errors.Is(err, ErrStreamGenerationMismatch) {
		t.Fatalf("batch progress guard error = %v", err)
	}
	var rows int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.replay_batch_guard").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("failed batch committed %d rows", rows)
	}
	if _, exists, err := postgres.ReadProgress(ctx, conn, guardStream); err != nil || exists {
		t.Fatalf("failed batch progress exists = %t (%v)", exists, err)
	}
}
