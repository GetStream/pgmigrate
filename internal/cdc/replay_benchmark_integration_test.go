//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

const (
	cdcReplayBenchmarkTransactions = 50_000
	cdcReplayInsertsPerTransaction = 5
	cdcReplayUpdatesPerTransaction = 4
	cdcReplayDeletesPerTransaction = 1
	cdcReplayChangesPerTransaction = cdcReplayInsertsPerTransaction +
		cdcReplayUpdatesPerTransaction + cdcReplayDeletesPerTransaction
)

// TestPG17CDCReplayThroughput measures the database-facing replay path rather
// than an in-memory approximation. It creates source WAL through ordinary SQL,
// receives and fsyncs pgoutput transactions into segments, then times a cold
// applier draining that durable backlog into an independently seeded target.
//
// The fixture intentionally resembles an application workload: each source
// transaction inserts immutable JSON events, updates indexed account rows, and
// deletes expiring sessions. Exact table digests are compared after replay so a
// faster result cannot trade away correctness. The test is opt-in because its
// absolute throughput assertion is meaningful only on a dedicated local or CI
// benchmark worker.
func TestPG17CDCReplayThroughput(t *testing.T) {
	if os.Getenv("PGMIGRATE_CDC_BENCHMARK") != "1" {
		t.Skip("set PGMIGRATE_CDC_BENCHMARK=1 to run the CDC replay benchmark")
	}

	transactionCount := benchmarkPositiveIntEnv(
		t, "PGMIGRATE_CDC_BENCH_TRANSACTIONS", cdcReplayBenchmarkTransactions,
	)
	minimumRate := benchmarkPositiveFloatEnv(
		t, "PGMIGRATE_CDC_BENCH_MIN_CHANGES_PER_SECOND", 200_000,
	)
	accountCount := 20_000
	accountCount = benchmarkPositiveIntEnv(
		t, "PGMIGRATE_CDC_BENCH_ACCOUNT_COUNT", accountCount,
	)
	barrierEvery := benchmarkNonNegativeIntEnv(
		t, "PGMIGRATE_CDC_BENCH_BARRIER_EVERY", 0,
	)
	replayWorkers := benchmarkPositiveIntEnv(
		t, "PGMIGRATE_CDC_BENCH_REPLAY_WORKERS", 8,
	)
	replayBatchBytes := benchmarkPositiveIntEnv(
		t, "PGMIGRATE_CDC_BENCH_REPLAY_BATCH_BYTES", 8<<20,
	)
	replayBatchChanges := benchmarkPositiveIntEnv(
		t, "PGMIGRATE_CDC_BENCH_REPLAY_BATCH_CHANGES", 32_768,
	)
	sessionCount := transactionCount * cdcReplayDeletesPerTransaction
	expectedChanges := transactionCount * cdcReplayChangesPerTransaction
	if barrierEvery > 0 {
		expectedChanges += (transactionCount-1)/barrierEvery + 1
	}

	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	sourceSQL := source.Connect(t)
	targetSQL := target.Connect(t)
	ctx := context.Background()

	fixture := cdcReplayFixtureSQL(accountCount, sessionCount)
	if _, err := sourceSQL.Exec(ctx, fixture); err != nil {
		t.Fatalf("create source benchmark fixture: %v", err)
	}
	if _, err := targetSQL.Exec(ctx, fixture); err != nil {
		t.Fatalf("create target benchmark fixture: %v", err)
	}
	if _, err := sourceSQL.Exec(ctx, `
		CREATE PUBLICATION pgmigrate_cdc_replay_benchmark
		FOR TABLE cdc_benchmark.accounts,
		          cdc_benchmark.events,
		          cdc_benchmark.sessions,
		          cdc_benchmark.guarded
	`); err != nil {
		t.Fatalf("create benchmark publication: %v", err)
	}

	replication := source.ReplicationConnect(t)
	slot, err := pglogrepl.CreateReplicationSlot(
		ctx,
		replication,
		"pgmigrate_cdc_replay_benchmark",
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "NOEXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		t.Fatalf("create benchmark replication slot: %v", err)
	}
	startLSN, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		t.Fatalf("parse benchmark start LSN: %v", err)
	}

	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{
		Directory: directory, RotationBytes: 64 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	writerClosed := false
	t.Cleanup(func() {
		if !writerClosed {
			_ = writer.Close()
		}
	})

	transactions := make(chan Transaction, 256)
	durable := new(DurableWatermark)
	persister, err := NewPersister(PersisterConfig{
		Writer: writer, Transactions: transactions, Durable: durable,
		SyncBytes: 4 << 20, SyncInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewReceiver(ReceiverConfig{
		ConnString: source.URI, Slot: "pgmigrate_cdc_replay_benchmark",
		Publication: "pgmigrate_cdc_replay_benchmark", StartLSN: LSN(startLSN),
		Transactions: transactions, Durable: durable,
		FeedbackInterval: 50 * time.Millisecond, Backpressure: 30 * time.Second,
		SpillThreshold: 8 << 20, SpillDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}

	persistCtx, cancelPersister := context.WithCancel(context.Background())
	defer cancelPersister()
	persistDone := make(chan error, 1)
	go func() { persistDone <- persister.Run(persistCtx) }()
	receiveCtx, cancelReceiver := context.WithCancel(context.Background())
	defer cancelReceiver()
	receiveDone := make(chan error, 1)
	go func() { receiveDone <- receiver.Run(receiveCtx) }()
	receiverStopped := false
	persisterStopped := false
	transactionsClosed := false
	t.Cleanup(func() {
		cancelReceiver()
		if !receiverStopped {
			select {
			case err := <-receiveDone:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("clean up benchmark receiver: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("timed out cleaning up benchmark receiver")
			}
			receiverStopped = true
		}
		if !transactionsClosed {
			close(transactions)
			transactionsClosed = true
		}
		if !persisterStopped {
			select {
			case err := <-persistDone:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("clean up benchmark persister: %v", err)
				}
			case <-time.After(10 * time.Second):
				cancelPersister()
				t.Error("timed out cleaning up benchmark persister")
			}
			persisterStopped = true
		}
	})

	for batch := 0; batch < transactionCount; batch++ {
		var inserted, updated, deleted, guarded int
		if err := sourceSQL.QueryRow(
			ctx, cdcReplayWorkloadSQL, batch, accountCount, barrierEvery,
		).Scan(&inserted, &updated, &deleted, &guarded); err != nil {
			t.Fatalf("emit CDC benchmark transaction %d: %v", batch, err)
		}
		expectedGuarded := 0
		if barrierEvery > 0 && batch%barrierEvery == 0 {
			expectedGuarded = 1
		}
		if inserted != cdcReplayInsertsPerTransaction ||
			updated != cdcReplayUpdatesPerTransaction ||
			deleted != cdcReplayDeletesPerTransaction || guarded != expectedGuarded {
			t.Fatalf(
				"benchmark transaction %d changed insert/update/delete/guarded=%d/%d/%d/%d, want %d/%d/%d/%d",
				batch, inserted, updated, deleted, guarded,
				cdcReplayInsertsPerTransaction, cdcReplayUpdatesPerTransaction,
				cdcReplayDeletesPerTransaction, expectedGuarded,
			)
		}
	}

	var markerText string
	if err := sourceSQL.QueryRow(
		ctx,
		"SELECT pg_catalog.pg_logical_emit_message(false, $1, $2)::text",
		CutoverMessagePrefix,
		"cdc-replay-throughput-boundary",
	).Scan(&markerText); err != nil {
		t.Fatalf("emit benchmark boundary: %v", err)
	}
	markerLSN, err := pglogrepl.ParseLSN(markerText)
	if err != nil {
		t.Fatalf("parse benchmark boundary %q: %v", markerText, err)
	}

	deadline := time.NewTimer(2 * time.Minute)
	ticker := time.NewTicker(10 * time.Millisecond)
	for durable.Load() < LSN(markerLSN) {
		select {
		case err := <-receiveDone:
			receiverStopped = true
			t.Fatalf("receiver stopped before benchmark backlog was durable: %v", err)
		case err := <-persistDone:
			persisterStopped = true
			t.Fatalf("persister stopped before benchmark backlog was durable: %v", err)
		case <-deadline.C:
			t.Fatalf(
				"timed out staging benchmark backlog: durable=%s marker=%s",
				pglogrepl.LSN(durable.Load()), markerLSN,
			)
		case <-ticker.C:
		}
	}
	deadline.Stop()
	ticker.Stop()

	cancelReceiver()
	if !receiverStopped {
		if err := <-receiveDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stop benchmark receiver: %v", err)
		}
		receiverStopped = true
	}
	close(transactions)
	transactionsClosed = true
	if !persisterStopped {
		if err := <-persistDone; err != nil {
			t.Fatalf("stop benchmark persister: %v", err)
		}
		persisterStopped = true
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close benchmark segment writer: %v", err)
	}
	writerClosed = true

	const streamID = "cdc-replay-throughput"
	const streamGeneration = "cdc-replay-throughput-v1"
	// Create the observer-visible control tables before starting the clock. This
	// avoids treating a harmless first-query race as a replay failure and keeps
	// the result focused on draining CDC rather than one-time schema bootstrap.
	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID: streamID, Generation: streamGeneration,
		FreshSetup: true, TargetHasCopiedData: true,
	}); err != nil {
		t.Fatalf("initialize benchmark replay identity: %v", err)
	}
	applier, err := NewApplier(ApplierConfig{
		ConnString: target.URI, Directory: directory,
		StreamID: streamID, StreamGeneration: streamGeneration,
		FreshSetup: true, TargetHasCopiedData: true, Durable: durable,
		PollInterval: time.Millisecond, ReplayWorkers: replayWorkers,
		BatchMaxDataBytes: int64(replayBatchBytes), BatchMaxChanges: replayBatchChanges,
	})
	if err != nil {
		t.Fatal(err)
	}

	applyCtx, cancelApply := context.WithCancel(context.Background())
	applyDone := make(chan error, 1)
	stopCPUProfile := benchmarkCPUProfile(
		t, os.Getenv("PGMIGRATE_CDC_BENCH_CPU_PROFILE"),
	)
	defer stopCPUProfile()
	started := time.Now()
	go func() { applyDone <- applier.Run(applyCtx) }()
	applyTicker := time.NewTicker(5 * time.Millisecond)
	applyDeadline := time.NewTimer(2 * time.Minute)
	for {
		progress, exists, err := postgres.ReadProgress(ctx, targetSQL, streamID)
		if err != nil {
			cancelApply()
			t.Fatalf("read benchmark replay progress: %v", err)
		}
		if exists && LSN(progress) >= LSN(markerLSN) {
			break
		}
		select {
		case err := <-applyDone:
			cancelApply()
			t.Fatalf("applier stopped before benchmark boundary: %v", err)
		case <-applyDeadline.C:
			cancelApply()
			t.Fatalf("timed out replaying %d benchmark changes", expectedChanges)
		case <-applyTicker.C:
		}
	}
	elapsed := time.Since(started)
	stopCPUProfile()
	applyDeadline.Stop()
	applyTicker.Stop()
	cancelApply()
	if err := <-applyDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("stop benchmark applier: %v", err)
	}

	for _, table := range []string{"accounts", "events", "sessions", "guarded"} {
		sourceCount, sourceDigest := benchmarkTableDigest(t, sourceSQL, table)
		targetCount, targetDigest := benchmarkTableDigest(t, targetSQL, table)
		if sourceCount != targetCount || sourceDigest != targetDigest {
			t.Fatalf(
				"%s differs after replay: source=%d/%s target=%d/%s",
				table, sourceCount, sourceDigest, targetCount, targetDigest,
			)
		}
	}

	rate := float64(expectedChanges) / elapsed.Seconds()
	t.Logf(
		"cdc_replay changes=%d source_transactions=%d accounts=%d barrier_every=%d replay_workers=%d replay_batch_bytes=%d replay_batch_changes=%d elapsed=%s changes_per_second=%.0f target=%.0f",
		expectedChanges, transactionCount, accountCount, barrierEvery,
		replayWorkers, replayBatchBytes, replayBatchChanges,
		elapsed.Round(time.Millisecond), rate, minimumRate,
	)
	if rate < minimumRate {
		t.Fatalf(
			"CDC replay throughput %.0f changes/s is below the %.0f changes/s target",
			rate, minimumRate,
		)
	}
}

func benchmarkCPUProfile(t *testing.T, path string) func() {
	t.Helper()
	if path == "" {
		return func() {}
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create CDC benchmark CPU profile: %v", err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		t.Fatalf("start CDC benchmark CPU profile: %v", err)
	}
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		pprof.StopCPUProfile()
		if err := file.Close(); err != nil {
			t.Errorf("close CDC benchmark CPU profile: %v", err)
		}
	}
}

func cdcReplayFixtureSQL(accountCount, sessionCount int) string {
	return fmt.Sprintf(`
		CREATE SCHEMA cdc_benchmark;
		CREATE TABLE cdc_benchmark.accounts (
			id bigint PRIMARY KEY,
			tenant_id integer NOT NULL,
			balance bigint NOT NULL,
			revision integer NOT NULL,
			metadata jsonb NOT NULL,
			updated_at timestamptz NOT NULL
		);
		CREATE INDEX accounts_tenant_revision_idx
			ON cdc_benchmark.accounts (tenant_id, revision);
		CREATE INDEX accounts_active_revision_idx
			ON cdc_benchmark.accounts (tenant_id, revision)
			WHERE revision >= 0;
		CREATE INDEX accounts_segment_idx
			ON cdc_benchmark.accounts ((metadata ->> 'segment'));
		CREATE INDEX accounts_updated_day_idx
			ON cdc_benchmark.accounts ((date_trunc('day', updated_at AT TIME ZONE 'UTC')));
		CREATE INDEX accounts_positive_balance_idx
			ON cdc_benchmark.accounts (balance) WHERE balance > 0;
		CREATE INDEX accounts_tenant_band_00_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 1 AND 32;
		CREATE INDEX accounts_tenant_band_01_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 33 AND 64;
		CREATE INDEX accounts_tenant_band_02_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 65 AND 96;
		CREATE INDEX accounts_tenant_band_03_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 97 AND 128;
		CREATE INDEX accounts_tenant_band_04_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 129 AND 160;
		CREATE INDEX accounts_tenant_band_05_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 161 AND 192;
		CREATE INDEX accounts_tenant_band_06_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 193 AND 224;
		CREATE INDEX accounts_tenant_band_07_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 225 AND 256;
		CREATE INDEX accounts_tenant_band_08_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 257 AND 288;
		CREATE INDEX accounts_tenant_band_09_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 289 AND 320;
		CREATE INDEX accounts_tenant_band_10_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 321 AND 352;
		CREATE INDEX accounts_tenant_band_11_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 353 AND 384;
		CREATE INDEX accounts_tenant_band_12_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 385 AND 416;
		CREATE INDEX accounts_tenant_band_13_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 417 AND 448;
		CREATE INDEX accounts_tenant_band_14_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 449 AND 476;
		CREATE INDEX accounts_tenant_band_15_idx ON cdc_benchmark.accounts (id) WHERE tenant_id BETWEEN 477 AND 500;
		CREATE INDEX accounts_tenant_band_desc_00_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 1 AND 32;
		CREATE INDEX accounts_tenant_band_desc_01_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 33 AND 64;
		CREATE INDEX accounts_tenant_band_desc_02_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 65 AND 96;
		CREATE INDEX accounts_tenant_band_desc_03_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 97 AND 128;
		CREATE INDEX accounts_tenant_band_desc_04_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 129 AND 160;
		CREATE INDEX accounts_tenant_band_desc_05_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 161 AND 192;
		CREATE INDEX accounts_tenant_band_desc_06_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 193 AND 224;
		CREATE INDEX accounts_tenant_band_desc_07_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 225 AND 256;
		CREATE INDEX accounts_tenant_band_desc_08_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 257 AND 288;
		CREATE INDEX accounts_tenant_band_desc_09_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 289 AND 320;
		CREATE INDEX accounts_tenant_band_desc_10_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 321 AND 352;
		CREATE INDEX accounts_tenant_band_desc_11_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 353 AND 384;
		CREATE INDEX accounts_tenant_band_desc_12_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 385 AND 416;
		CREATE INDEX accounts_tenant_band_desc_13_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 417 AND 448;
		CREATE INDEX accounts_tenant_band_desc_14_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 449 AND 476;
		CREATE INDEX accounts_tenant_band_desc_15_idx ON cdc_benchmark.accounts (id DESC) WHERE tenant_id BETWEEN 477 AND 500;

		CREATE TABLE cdc_benchmark.events (
			id bigint PRIMARY KEY,
			account_id bigint NOT NULL REFERENCES cdc_benchmark.accounts(id),
			kind smallint NOT NULL,
			payload jsonb NOT NULL,
			created_at timestamptz NOT NULL
		);
		CREATE INDEX events_account_created_idx
			ON cdc_benchmark.events (account_id, created_at DESC);
		CREATE INDEX events_kind_created_idx
			ON cdc_benchmark.events (kind, created_at DESC);

		CREATE TABLE cdc_benchmark.sessions (
			id bigint PRIMARY KEY,
			account_id bigint NOT NULL REFERENCES cdc_benchmark.accounts(id),
			token text NOT NULL,
			expires_at timestamptz NOT NULL
		);
		CREATE INDEX sessions_account_idx ON cdc_benchmark.sessions (account_id);
		CREATE INDEX sessions_expiry_idx ON cdc_benchmark.sessions (expires_at);

		CREATE TABLE cdc_benchmark.guarded (
			id bigint PRIMARY KEY,
			revision integer NOT NULL,
			payload text NOT NULL CHECK (payload <> '')
		);

		INSERT INTO cdc_benchmark.accounts
		SELECT id,
		       ((id - 1) %% 500) + 1,
		       100000 + id * 17,
		       0,
		       jsonb_build_object('segment', id %% 7, 'seed', md5(id::text)),
		       TIMESTAMPTZ '2026-01-01 00:00:00+00' + id * interval '1 second'
		FROM generate_series(1, %d) AS id;

		INSERT INTO cdc_benchmark.sessions
		SELECT id,
		       ((id - 1) %% %d) + 1,
		       md5('session-' || id::text),
		       TIMESTAMPTZ '2026-02-01 00:00:00+00' + id * interval '1 second'
		FROM generate_series(1, %d) AS id;

		INSERT INTO cdc_benchmark.guarded
		SELECT id, 0, md5('guarded-' || id::text)
		FROM generate_series(1, 1024) AS id;
	`, accountCount, accountCount, sessionCount)
}

const cdcReplayWorkloadSQL = `
	WITH inserted AS (
		INSERT INTO cdc_benchmark.events (id, account_id, kind, payload, created_at)
		SELECT $1::bigint * 5 + item,
		       (($1::bigint * 5 + item - 1) % $2::bigint) + 1,
		       (($1::bigint + item) % 8)::smallint,
		       jsonb_build_object(
			       'trace_id', md5($1::text || ':' || item::text),
			       'body', repeat(md5(($1::bigint * 5 + item)::text), 4)
		       ),
		       TIMESTAMPTZ '2026-03-01 00:00:00+00' +
			       ($1::bigint * 5 + item) * interval '1 microsecond'
		FROM generate_series(1, 5) AS item
		RETURNING 1
	),
	updated AS (
		UPDATE cdc_benchmark.accounts AS account
		SET balance = account.balance + (($1::bigint % 17) - 8),
		    revision = account.revision + 1,
		    metadata = jsonb_set(account.metadata, '{last_batch}', to_jsonb($1::bigint), true),
		    updated_at = TIMESTAMPTZ '2026-03-01 00:00:00+00' +
			    $1::bigint * interval '1 millisecond'
		FROM generate_series(1, 4) AS item
		WHERE account.id = (($1::bigint * 4 + item - 1) % $2::bigint) + 1
		RETURNING 1
	),
	deleted AS (
		DELETE FROM cdc_benchmark.sessions AS session
		USING generate_series(1, 1) AS item
		WHERE session.id = $1::bigint + item
		RETURNING 1
	),
	guarded AS (
		UPDATE cdc_benchmark.guarded AS guarded_row
		SET revision = guarded_row.revision + 1,
		    payload = md5($1::text || ':' || guarded_row.id::text)
		WHERE $1::bigint % NULLIF($3::bigint, 0) = 0
		  AND guarded_row.id =
		      (($1::bigint / NULLIF($3::bigint, 0)) % 1024) + 1
		RETURNING 1
	)
	SELECT (SELECT count(*) FROM inserted),
	       (SELECT count(*) FROM updated),
	       (SELECT count(*) FROM deleted),
	       (SELECT count(*) FROM guarded)
`

func benchmarkTableDigest(t *testing.T, conn *pgx.Conn, table string) (int64, string) {
	t.Helper()
	query := fmt.Sprintf(`
		SELECT count(*),
		       COALESCE(
			       md5(string_agg(md5(to_jsonb(row_value)::text), '' ORDER BY id)),
			       md5('')
		       )
		FROM cdc_benchmark.%s AS row_value
	`, table)
	var count int64
	var digest string
	if err := conn.QueryRow(context.Background(), query).Scan(&count, &digest); err != nil {
		t.Fatalf("digest cdc_benchmark.%s: %v", table, err)
	}
	return count, digest
}

func benchmarkPositiveIntEnv(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer, got %q", name, value)
	}
	return parsed
}

func benchmarkPositiveFloatEnv(t *testing.T, name string, fallback float64) float64 {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive number, got %q", name, value)
	}
	return parsed
}

func benchmarkNonNegativeIntEnv(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		t.Fatalf("%s must be a non-negative integer, got %q", name, value)
	}
	return parsed
}
