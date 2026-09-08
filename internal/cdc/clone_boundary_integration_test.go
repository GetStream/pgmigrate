//go:build integration

package cdc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/jackc/pglogrepl"
)

// A restored database already contains the transaction ending exactly at its
// seed. Obtain that exact end from pgoutput, rather than sampling a WAL position
// that background activity might have advanced beyond the commit record.
func TestPGCloneSeedAtCommitEnd(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprint(major), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			source := pgtest.Start(t, major)
			conn := source.Connect(t)
			if _, err := conn.Exec(ctx, `CREATE TABLE boundary(id int PRIMARY KEY);
				CREATE PUBLICATION boundary FOR TABLE boundary`); err != nil {
				t.Fatal(err)
			}
			repl := source.ReplicationConnect(t)
			if _, err := pglogrepl.CreateReplicationSlot(ctx, repl, "boundary", "pgoutput", pglogrepl.CreateReplicationSlotOptions{
				Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT",
			}); err != nil {
				t.Fatal(err)
			}
			_ = repl.Close(ctx)
			spanning := source.Connect(t)
			tx, err := spanning.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err := tx.Exec(ctx, "INSERT INTO boundary VALUES(2)"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, "INSERT INTO boundary VALUES(1)"); err != nil {
				t.Fatal(err)
			}
			rows, err := conn.Query(ctx, `SELECT data FROM pg_logical_slot_peek_binary_changes(
				'boundary', NULL, NULL, 'proto_version', '1', 'publication_names', 'boundary')`)
			if err != nil {
				t.Fatal(err)
			}
			var seed LSN
			for rows.Next() {
				var data []byte
				if err := rows.Scan(&data); err != nil {
					t.Fatal(err)
				}
				message, err := pglogrepl.Parse(data)
				if err != nil {
					t.Fatal(err)
				}
				if commit, ok := message.(*pglogrepl.CommitMessage); ok {
					seed = LSN(commit.TransactionEndLSN)
				}
			}
			if err := rows.Err(); err != nil || seed == 0 {
				t.Fatalf("read exact commit boundary: seed=%v err=%v", seed, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, "INSERT INTO boundary VALUES(3)"); err != nil {
				t.Fatal(err)
			}
			transactions := make(chan Transaction, 4)
			textMode := false
			receiver, err := NewReceiver(ReceiverConfig{
				ConnString: source.URI, Slot: "boundary", Publication: "boundary",
				StartLSN: seed, Transactions: transactions, Durable: new(DurableWatermark), Binary: &textMode,
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- receiver.Run(ctx) }()
			defer func() { cancel(); <-done }()
			for _, want := range []string{"2", "3"} {
				select {
				case transaction := <-transactions:
					if transaction.EndLSN <= seed || len(transaction.Changes) != 1 ||
						transaction.Changes[0].New == nil || string((*transaction.Changes[0].New)[0].Data) != want {
						t.Fatalf("after exact seed %s: got %+v, want id=%s", pglogrepl.LSN(seed), transaction, want)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
