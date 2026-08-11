package cdc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestReplicationConnectionForcesDatabaseMode(t *testing.T) {
	t.Parallel()
	config, err := replicationConnConfig("postgres://user:pass@localhost/db?sslmode=disable&replication=0")
	if err != nil {
		t.Fatal(err)
	}
	if got := config.RuntimeParams["replication"]; got != "database" {
		t.Fatalf("replication runtime parameter = %q, want database", got)
	}
}

func TestReceiverReconnectResumesOnlyFromDurableLSN(t *testing.T) {
	t.Parallel()
	watermark := new(DurableWatermark)
	const initial LSN = 0x80

	if got := receiverResumeLSN(initial, watermark); got != initial {
		t.Fatalf("resume before fsync = %x, want initial %x", got, initial)
	}

	// Simulate a transaction delivered and appended before reconnect, but not
	// fsynced. The receiver must request it again, and the persister must
	// suppress the duplicate local frame without losing the transaction.
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	delivered := testTransaction(0x100)
	if _, err := writer.AppendFrame(&delivered); err != nil {
		t.Fatal(err)
	}
	if got := receiverResumeLSN(initial, watermark); got != initial {
		t.Fatalf("resume after unsynced delivery = %x, want initial %x", got, initial)
	}
	input := make(chan Transaction, 2)
	input <- delivered
	input <- testTransaction(0x200)
	close(input)
	persister, err := NewPersister(PersisterConfig{
		Writer:       writer,
		Transactions: input,
		Durable:      watermark,
		SyncInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := persister.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := watermark.Load(); got != 0x201 {
		t.Fatalf("durable watermark after replay = %x, want 201", got)
	}
	transactions, err := ReadTransactionsAfter(directory, 0, watermark.Load())
	if err != nil {
		t.Fatal(err)
	}
	if len(transactions) != 2 {
		t.Fatalf("staged transactions = %d, want two without duplicate", len(transactions))
	}
}

func TestRetryClassificationOnlyAllowsConnections(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"connection SQLSTATE", &pgconn.PgError{Code: "08006"}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"invalid protocol", errors.New("cdc: decode pgoutput: unknown message type"), false},
		{"corrupt segment", errors.New("cdc: checksum mismatch in segment"), false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := isConnectionError(test.err); got != test.want {
				t.Fatalf("isConnectionError(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestUniqueViolationIsDivergence(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	err := classifyApplyError(relation, ChangeInsert, &pgconn.PgError{
		Code:    "23505",
		Message: "duplicate key value violates unique constraint",
	})
	var divergence *DivergenceError
	if !errors.As(err, &divergence) {
		t.Fatalf("error type = %T, want DivergenceError", err)
	}
	if !strings.Contains(divergence.Reason, "23505") {
		t.Fatalf("divergence reason = %q", divergence.Reason)
	}
}

func TestCorruptSegmentIsTerminalForApplier(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "0000000000000001.seg")
	if err := os.WriteFile(path, []byte{1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	applier := &Applier{config: ApplierConfig{
		Directory: directory,
		Durable:   new(DurableWatermark),
	}}
	applier.config.Durable.Publish(10)
	_, _, err := applier.applyAvailable(context.Background(), nil, 0)
	if err == nil {
		t.Fatal("expected corrupt segment error")
	}
	if isConnectionError(err) {
		t.Fatalf("corrupt segment was classified for reconnect: %v", err)
	}
}

func TestInvalidPGOutputIsTerminalForReceiver(t *testing.T) {
	t.Parallel()
	_, err := NewPGOutputDecoder().Decode([]byte{'?'})
	if err == nil {
		t.Fatal("expected invalid protocol error")
	}
	if isConnectionError(err) {
		t.Fatalf("invalid protocol was classified for reconnect: %v", err)
	}
}

func TestCanceledDeliveryCleansTransactionSpill(t *testing.T) {
	t.Parallel()
	transaction := decodeSpillTestTransaction(t, PGOutputDecoderConfig{
		SpillThreshold: 64,
		SpillDirectory: t.TempDir(),
	})
	path := transaction.Spill.Path()
	output := make(chan Transaction, 1)
	output <- Transaction{}
	receiver := &Receiver{config: ReceiverConfig{
		Transactions:     output,
		FeedbackInterval: time.Hour,
		Backpressure:     time.Hour,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	nextFeedback := time.Now().Add(time.Hour)
	if err := receiver.deliver(ctx, nil, *transaction, &nextFeedback); !errors.Is(err, context.Canceled) {
		t.Fatalf("delivery error = %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill survived canceled delivery: %v", err)
	}
}
