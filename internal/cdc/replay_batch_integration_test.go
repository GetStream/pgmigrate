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

	const transactionCount = 10_000
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
	t.Logf("%d same-table transactions: unbatched=%s batched=%s speedup=%.1fx rate=%.0f events/s",
		transactionCount, unbatched, batched, float64(unbatched)/float64(batched),
		float64(transactionCount)/batched.Seconds())
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

func TestPG17ReplayScalesAcrossIndependentCommitLanes(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	const (
		lanes            = 16
		transactionCount = 10_000
	)
	for _, prefix := range []string{"serial_lane", "parallel_lane"} {
		for lane := range lanes {
			if _, err := conn.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE public.%s_%02d (id integer PRIMARY KEY)", prefix, lane,
			)); err != nil {
				t.Fatal(err)
			}
		}
	}

	run := func(prefix, stream string, workers int) time.Duration {
		t.Helper()
		directory := t.TempDir()
		writer, _, err := OpenWriter(WriterConfig{Directory: directory})
		if err != nil {
			t.Fatal(err)
		}
		for i := range transactionCount {
			lane := i % lanes
			relation := Relation{
				OID: uint32(4000 + lane), Namespace: "public",
				Name: fmt.Sprintf("%s_%02d", prefix, lane), ReplicaIdentity: 'd',
				Columns: []Column{{Name: "id", Type: 23, Flags: 1}},
			}
			value := Tuple{{Kind: DatumText, Data: []byte(fmt.Sprint(i/lanes + 1))}}
			transaction := Transaction{
				CommitLSN: LSN(i*2 + 1), EndLSN: LSN(i*2 + 2), Relations: []Relation{relation},
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
			Workers: workers, BatchSize: 64, Window: workers * 8,
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
		return time.Since(started)
	}

	serial := run("serial_lane", "serial-lanes", 1)
	parallel := run("parallel_lane", "parallel-lanes", lanes)
	t.Logf("%d transactions across %d tables: serial=%s parallel=%s speedup=%.1fx rate=%.0f events/s",
		transactionCount, lanes, serial, parallel, float64(serial)/float64(parallel),
		float64(transactionCount)/parallel.Seconds())
	if parallel*3 >= serial {
		t.Fatalf("parallel replay %s is not at least 3x faster than serial %s", parallel, serial)
	}
}

func TestPG17ConcurrentReplayRecoversAnOutOfOrderDurableCommit(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE public.receipt_first (id integer PRIMARY KEY);
		CREATE TABLE public.receipt_second (id integer PRIMARY KEY);
	`); err != nil {
		t.Fatal(err)
	}

	relation := func(oid uint32, name string) Relation {
		return Relation{
			OID: oid, Namespace: "public", Name: name, ReplicaIdentity: 'd',
			Columns: []Column{{Name: "id", Type: 23, Flags: 1}},
		}
	}
	value := func(id string) *Tuple {
		result := Tuple{{Kind: DatumText, Data: []byte(id)}}
		return &result
	}
	firstRelation := relation(5001, "receipt_first")
	secondRelation := relation(5002, "receipt_second")
	transactions := []Transaction{
		{
			CommitLSN: 1, EndLSN: 2, Relations: []Relation{firstRelation},
			Changes: []Change{{RelationOID: firstRelation.OID, Kind: ChangeInsert, New: value("1")}},
		},
		{
			CommitLSN: 3, EndLSN: 4, Relations: []Relation{secondRelation},
			Changes: []Change{{RelationOID: secondRelation.OID, Kind: ChangeInsert, New: value("2")}},
		},
		{
			CommitLSN: 5, EndLSN: 6, Relations: []Relation{secondRelation},
			Changes: []Change{{RelationOID: secondRelation.OID, Kind: ChangeInsert, New: value("3")}},
		},
	}

	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	for i := range transactions {
		if _, err := writer.AppendFrame(&transactions[i]); err != nil {
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

	const (
		stream     = "receipt-recovery"
		generation = "receipt-recovery-generation"
	)
	if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
		StreamID: stream, Generation: generation, FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := configureApplySession(ctx, conn); err != nil {
		t.Fatal(err)
	}
	manual := &Applier{config: ApplierConfig{
		StreamID: stream, StreamGeneration: generation,
	}}
	prepared, err := manual.prepareTransactions(
		ctx, conn, newTargetRelationCache(), newApplyStatementCache(applyStatementCacheCapacity),
		transactions[1:],
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manual.commitPreparedReplay(prepared, transactions[1:]); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := postgres.ReadProgress(ctx, conn, stream); err != nil || exists {
		t.Fatalf("canonical progress exists before recovery = %t (%v)", exists, err)
	}
	var firstReceipt, lastReceipt string
	if err := conn.QueryRow(ctx, `
		SELECT first_lsn::text, last_lsn::text
		FROM `+streamReplayReceiptTable+`
		WHERE stream_id = $1 AND stream_generation = $2
	`, stream, generation).Scan(&firstReceipt, &lastReceipt); err != nil {
		t.Fatal(err)
	}
	if firstReceipt != "0/4" || lastReceipt != "0/6" {
		t.Fatalf("durable receipt range = %s..%s, want 0/4..0/6", firstReceipt, lastReceipt)
	}

	durable := new(DurableWatermark)
	durable.Publish(durableLSN)
	applier, err := NewApplier(ApplierConfig{
		ConnString: target.URI, Directory: directory,
		// A restart with lower concurrency must still recognize receipts made
		// by a previous multi-worker run.
		Workers: 1, BatchSize: 1, Window: 8,
		StreamID: stream, StreamGeneration: generation,
		TargetHasCopiedData: true, Durable: durable,
		EndPosition: func(context.Context) (LSN, bool, error) {
			return durableLSN, true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Run(ctx); err != nil {
		t.Fatal(err)
	}

	for table, expected := range map[string]int{"receipt_first": 1, "receipt_second": 2} {
		var count int
		if err := conn.QueryRow(ctx,
			"SELECT count(*) FROM public."+table,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != expected {
			t.Fatalf("%s rows = %d, want %d", table, count, expected)
		}
	}
	progress, exists, err := postgres.ReadProgress(ctx, conn, stream)
	if err != nil || !exists || LSN(progress) != durableLSN {
		t.Fatalf("recovered progress = %x/%t (%v), want %x", progress, exists, err, durableLSN)
	}
	var receipts int
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM `+streamReplayReceiptTable+`
		WHERE stream_id = $1 AND stream_generation = $2
	`, stream, generation).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatalf("checkpoint left %d replay receipts, want 0", receipts)
	}
}
