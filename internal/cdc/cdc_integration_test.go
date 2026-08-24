//go:build integration

package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/jackc/pglogrepl"
)

func TestPG17LiveWALStageApplyCrashRetry(t *testing.T) {
	source := pgtest.Start(t, 17)
	target := pgtest.Start(t, 17)
	sourceSQL := source.Connect(t)
	targetSQL := target.Connect(t)
	ctx := context.Background()

	if _, err := sourceSQL.Exec(ctx, `
		CREATE TYPE public.cdc_mood AS ENUM ('sad', 'ok', 'happy');
		CREATE DOMAIN public.cdc_code AS text CHECK (VALUE <> '');
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := targetSQL.Exec(ctx, `
		CREATE TYPE public.cdc_oid_shift AS ENUM ('shift');
		CREATE TYPE public.cdc_mood AS ENUM ('sad', 'ok', 'happy');
		CREATE DOMAIN public.cdc_code AS text CHECK (VALUE <> '');
	`); err != nil {
		t.Fatal(err)
	}
	schema := `
		CREATE TABLE public.cdc_items (
			id integer PRIMARY KEY,
			payload text,
			note text,
			note_length integer GENERATED ALWAYS AS (length(note)) STORED
		);
		CREATE TABLE public.cdc_truncated (
			id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY
		);
		CREATE TABLE public.cdc_truncated_child (
			id integer PRIMARY KEY,
			parent_id integer REFERENCES public.cdc_truncated(id)
		);
		CREATE TABLE public.cdc_custom (
			id integer PRIMARY KEY,
			mood public.cdc_mood,
			code public.cdc_code,
			moods public.cdc_mood[]
		);
		-- An empty string is a present zero-length value, not NULL. The two are
		-- one byte apart in pgoutput and were indistinguishable in the apply
		-- path, so the required column catches the loud case and the optional
		-- column catches the silent one.
		CREATE TABLE public.cdc_empty (
			id integer PRIMARY KEY,
			required text NOT NULL DEFAULT '',
			optional text,
			absent text
		);
		ALTER TABLE public.cdc_empty REPLICA IDENTITY FULL;
	`
	if _, err := sourceSQL.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := targetSQL.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceSQL.Exec(ctx, `
		CREATE TABLE public.cdc_generated_only (answer integer);
		ALTER TABLE public.cdc_generated_only REPLICA IDENTITY FULL;
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := targetSQL.Exec(ctx, `
		CREATE TABLE public.cdc_generated_only (
			answer integer GENERATED ALWAYS AS (42) STORED
		);
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceSQL.Exec(ctx, "CREATE PUBLICATION pgmigrate_cdc_test FOR TABLE cdc_items, cdc_truncated, cdc_truncated_child, cdc_custom, cdc_generated_only, cdc_empty"); err != nil {
		t.Fatal(err)
	}

	replication := source.ReplicationConnect(t)
	slot, err := pglogrepl.CreateReplicationSlot(
		ctx,
		replication,
		"pgmigrate_cdc_test",
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "NOEXPORT_SNAPSHOT",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	startLSN, err := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	spillDirectory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	input := make(chan Transaction, 2)
	watermark := new(DurableWatermark)
	persister, err := NewPersister(PersisterConfig{
		Writer:       writer,
		Transactions: input,
		Durable:      watermark,
		SyncBytes:    1,
		SyncInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	textMode := false
	receiver, err := NewReceiver(ReceiverConfig{
		ConnString:       source.URI,
		Slot:             "pgmigrate_cdc_test",
		Publication:      "pgmigrate_cdc_test",
		StartLSN:         LSN(startLSN),
		Transactions:     input,
		Durable:          watermark,
		FeedbackInterval: 20 * time.Millisecond,
		Backpressure:     5 * time.Second,
		SpillThreshold:   64,
		SpillDirectory:   spillDirectory,
		Binary:           &textMode,
	})
	if err != nil {
		t.Fatal(err)
	}

	persistDone := make(chan error, 1)
	go func() { persistDone <- persister.Run(context.Background()) }()
	receiveCtx, stopReceiver := context.WithCancel(context.Background())
	receiveDone := make(chan error, 1)
	go func() { receiveDone <- receiver.Run(receiveCtx) }()

	large := strings.Repeat("x", 17<<20)
	statements := []struct {
		sql  string
		args []any
	}{
		{"INSERT INTO cdc_items (id, payload, note) VALUES (1, $1, 'before')", []any{large}},
		{"UPDATE cdc_items SET note = 'after' WHERE id = 1", nil},
		{"INSERT INTO cdc_items (id, payload, note) VALUES (2, NULL, 'delete')", nil},
		{"DELETE FROM cdc_items WHERE id = 2", nil},
		{"INSERT INTO cdc_truncated DEFAULT VALUES", nil},
		{"INSERT INTO cdc_truncated_child VALUES (1, 1)", nil},
		{"TRUNCATE cdc_truncated RESTART IDENTITY CASCADE", nil},
		{"INSERT INTO cdc_custom VALUES (1, 'ok', 'code-1', ARRAY['sad','happy']::cdc_mood[])", nil},
		{"UPDATE cdc_custom SET mood = 'happy', code = 'code-2' WHERE id = 1", nil},
		{"INSERT INTO cdc_generated_only VALUES (42)", nil},
		{"UPDATE cdc_generated_only SET answer = 42", nil},
		{"INSERT INTO cdc_generated_only VALUES (42)", nil},
		{"INSERT INTO cdc_empty (id, optional, absent) VALUES (1, '', NULL)", nil},
		{"INSERT INTO cdc_empty (id, required, optional, absent) VALUES (2, 'kept', 'kept', 'kept')", nil},
		// Keyed on an empty string under REPLICA IDENTITY FULL: the predicate
		// binds the same parameter, so a nil binding would look for a NULL and
		// match no row at all.
		{"UPDATE cdc_empty SET absent = 'set' WHERE id = 1", nil},
		{"INSERT INTO cdc_empty (id, optional) VALUES (3, '')", nil},
		{"DELETE FROM cdc_empty WHERE id = 3", nil},
	}
	for index, statement := range statements {
		if _, err := sourceSQL.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			waitFor(t, 30*time.Second, func() bool {
				transactions, err := ReadTransactionsAfter(directory, 0, watermark.Load())
				return err == nil && len(transactions) == 1
			})
			if _, err := sourceSQL.Exec(ctx, `
				SELECT pg_terminate_backend(active_pid)
				FROM pg_replication_slots
				WHERE slot_name='pgmigrate_cdc_test' AND active_pid IS NOT NULL`); err != nil {
				t.Fatal(err)
			}
		}
	}

	waitFor(t, 30*time.Second, func() bool {
		transactions, err := ReadTransactionsAfter(directory, 0, watermark.Load())
		return err == nil && len(transactions) == len(statements)
	})
	stopReceiver()
	if err := <-receiveDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(input)
	if err := <-persistDone; err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	spillEntries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(spillEntries) != 0 {
		t.Fatalf("spill directory contains %d files after append", len(spillEntries))
	}

	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Nanosecond,
		Catalog:   writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The sampler is what makes the replication path checkable, and the applier is
	// the only thing that can feed it. Wiring it here rather than in a test of its
	// own is deliberate: this is the only place the spilled path runs, and a
	// 17 MB row spills, so a change collected from disk is covered too.
	samples := &collectingSampler{}
	applier, err := NewApplier(ApplierConfig{
		ConnString:       target.URI,
		Directory:        directory,
		StreamID:         "pg17-live-wal",
		StreamGeneration: "migration-generation-1",
		Durable:          watermark,
		PollInterval:     5 * time.Millisecond,
		AfterProgress:    pruner.OnProgress,
		Sampler:          samples,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, crash := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- applier.Run(firstCtx) }()
	waitFor(t, 30*time.Second, func() bool {
		_, exists, err := postgres.ReadProgress(ctx, targetSQL, "pg17-live-wal")
		return err == nil && exists
	})
	crash()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	retryCtx, stopRetry := context.WithCancel(context.Background())
	retryDone := make(chan error, 1)
	go func() { retryDone <- applier.Run(retryCtx) }()
	waitForOrError(t, 30*time.Second, retryDone, func() bool {
		progress, exists, err := postgres.ReadProgress(ctx, targetSQL, "pg17-live-wal")
		return err == nil && exists && LSN(progress) == watermark.Load()
	})
	stopRetry()
	if err := <-retryDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	replay, exists, err := postgres.ReadReplicationProgress(ctx, targetSQL, "pg17-live-wal")
	if err != nil {
		t.Fatal(err)
	}
	if !exists || replay.Transactions != int64(len(statements)) || replay.Rows < int64(len(statements)) {
		t.Fatalf(
			"replay counters after crash/retry = %+v exists=%t, want %d transactions and at least %d changes",
			replay, exists, len(statements), len(statements),
		)
	}
	segments, err := listSegments(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) > 2 {
		t.Fatalf("segments after applied progress pruning = %d, want at most two", len(segments))
	}
	if _, err := Recover(directory); err != nil {
		t.Fatalf("recover pruned segment set: %v", err)
	}

	// Every applied change has to have been reported by identity, because a row
	// verification is never told about is a row only the base copy's sample can
	// reach, and on a bloated heap that sample will not find it. The insert of
	// id 1 carries 17 MB and so arrives through the spilled path.
	for _, want := range []struct {
		table string
		kind  ChangeKind
		key   string
	}{
		{"cdc_items", ChangeInsert, "1"},
		{"cdc_items", ChangeUpdate, "1"},
		{"cdc_items", ChangeInsert, "2"},
		{"cdc_items", ChangeDelete, "2"},
		{"cdc_empty", ChangeDelete, "3"},
	} {
		if !samples.saw(want.table, want.kind, want.key) {
			t.Errorf("the applier applied kind %d of %s id %s without reporting it: %v",
				want.kind, want.table, want.key, samples.all())
		}
	}
	// A truncate names no row, so it cannot be sampled. Recording one would put a
	// key in the reservoir that no side can be asked about.
	if samples.saw("cdc_truncated", ChangeTruncate, "") {
		t.Error("a truncate was reported as a sample, which names no row to check")
	}

	var payload, note string
	var noteLength int
	if err := targetSQL.QueryRow(ctx, "SELECT payload, note, note_length FROM cdc_items WHERE id = 1").
		Scan(&payload, &note, &noteLength); err != nil {
		t.Fatal(err)
	}
	if payload != large || note != "after" || noteLength != len(note) {
		t.Fatalf("target row payload bytes/note/generated = %d/%q/%d", len(payload), note, noteLength)
	}
	var mood, code string
	var moods []string
	if err := targetSQL.QueryRow(
		ctx,
		"SELECT mood::text, code::text, moods::text[] FROM cdc_custom WHERE id = 1",
	).Scan(&mood, &code, &moods); err != nil {
		t.Fatal(err)
	}
	if mood != "happy" || code != "code-2" || !slices.Equal(moods, []string{"sad", "happy"}) {
		t.Fatalf("custom target values = %q/%q/%v", mood, code, moods)
	}
	var generatedCount, generatedMin, generatedMax int
	if err := targetSQL.QueryRow(
		ctx,
		"SELECT count(*), min(answer), max(answer) FROM cdc_generated_only",
	).Scan(&generatedCount, &generatedMin, &generatedMax); err != nil {
		t.Fatal(err)
	}
	if generatedCount != 2 || generatedMin != 42 || generatedMax != 42 {
		t.Fatalf("generated-only target count/min/max = %d/%d/%d", generatedCount, generatedMin, generatedMax)
	}
	// An empty string must arrive as an empty string. NOT NULL only makes the
	// failure loud; the nullable column is where it would otherwise be silent.
	var required, optional string
	var absent *string
	if err := targetSQL.QueryRow(
		ctx,
		"SELECT required, optional, absent FROM cdc_empty WHERE id = 1",
	).Scan(&required, &optional, &absent); err != nil {
		t.Fatal(err)
	}
	if required != "" || optional != "" || absent == nil || *absent != "set" {
		t.Fatalf("empty-string target row = %q/%q/%v", required, optional, absent)
	}
	var emptyOptional, nullOptional int
	if err := targetSQL.QueryRow(
		ctx, `
		SELECT count(*) FILTER (WHERE optional = ''), count(*) FILTER (WHERE optional IS NULL)
		FROM cdc_empty`,
	).Scan(&emptyOptional, &nullOptional); err != nil {
		t.Fatal(err)
	}
	if emptyOptional != 1 || nullOptional != 0 {
		t.Fatalf("target empty/null optional counts = %d/%d, want 1/0", emptyOptional, nullOptional)
	}
	for _, table := range []string{"cdc_items", "cdc_truncated", "cdc_truncated_child", "cdc_custom", "cdc_generated_only", "cdc_empty"} {
		var sourceCount, targetCount int
		if err := sourceSQL.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&sourceCount); err != nil {
			t.Fatal(err)
		}
		if err := targetSQL.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&targetCount); err != nil {
			t.Fatal(err)
		}
		if sourceCount != targetCount {
			t.Fatalf("%s counts source/target = %d/%d", table, sourceCount, targetCount)
		}
	}
	var restartedID int
	if err := targetSQL.QueryRow(ctx, "INSERT INTO cdc_truncated DEFAULT VALUES RETURNING id").Scan(&restartedID); err != nil {
		t.Fatal(err)
	}
	if restartedID != 1 {
		t.Fatalf("target identity after replicated restart = %d, want 1", restartedID)
	}

	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID:   "pg17-live-wal",
		Generation: "different-generation",
	}); !errors.Is(err, ErrStreamGenerationMismatch) {
		t.Fatalf("generation mismatch error = %v", err)
	}
	if _, err := targetSQL.Exec(
		ctx,
		"DELETE FROM pgmigrate_internal.replication_progress WHERE stream_id = $1",
		"pg17-live-wal",
	); err != nil {
		t.Fatal(err)
	}
	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID:            "pg17-live-wal",
		Generation:          "migration-generation-1",
		TargetHasCopiedData: true,
	}); !errors.Is(err, ErrMissingTargetProgress) {
		t.Fatalf("missing cleaned progress error = %v", err)
	}
	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID:            "brand-new-stream",
		Generation:          "brand-new-generation",
		TargetHasCopiedData: true,
	}); !errors.Is(err, ErrMissingTargetProgress) {
		t.Fatalf("copied target without identity error = %v", err)
	}
	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID:            "explicit-fresh-stream",
		Generation:          "explicit-fresh-generation",
		TargetHasCopiedData: true,
		FreshSetup:          true,
	}); err != nil {
		t.Fatalf("explicit fresh setup: %v", err)
	}
}

// collectingSampler stands in for the reservoir and keeps every identity the
// applier reported. Observe runs on the apply goroutine, so it takes a lock even
// though the assertions only read once both appliers have stopped.
type collectingSampler struct {
	mu      sync.Mutex
	samples []KeySample
}

func (c *collectingSampler) Observe(sample KeySample) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples = append(c.samples, sample)
}

func (c *collectingSampler) saw(table string, kind ChangeKind, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sample := range c.samples {
		if sample.Table != table || sample.Kind != kind || len(sample.Columns) == 0 {
			continue
		}
		if string(sample.Columns[0].Datum.Data) == key {
			return true
		}
	}
	return false
}

func (c *collectingSampler) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := make([]string, 0, len(c.samples))
	for _, sample := range c.samples {
		key := ""
		if len(sample.Columns) > 0 {
			key = string(sample.Columns[0].Datum.Data)
		}
		seen = append(seen, fmt.Sprintf("%s kind %d key %s", sample.Table, sample.Kind, key))
	}
	slices.Sort(seen)
	return slices.Compact(seen)
}

// TestPG17WaitUntilDoesNotScanStagedSegments is the catchup wait: the boundary
// is already the durable EndLSN. Scanning the segment directory to "normalize"
// it would decode the whole backlog after a long copy, which is how a shard
// sat on the first file at the memory ceiling with apply idle.
func TestPG17WaitUntilDoesNotScanStagedSegments(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	if err := postgres.EnsureProgressTable(ctx, conn); err != nil {
		t.Fatal(err)
	}
	if err := postgres.UpdateProgress(ctx, conn, "catchup", 0x100); err != nil {
		t.Fatal(err)
	}
	watermark := new(DurableWatermark)
	watermark.Publish(0x100)
	applier, err := NewApplier(ApplierConfig{
		ConnString:   target.URI,
		Directory:    filepath.Join(t.TempDir(), "empty-cdc"),
		StreamID:     "catchup",
		Durable:      watermark,
		PollInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := applier.WaitUntil(waitCtx, 0x100); err != nil {
		t.Fatal(err)
	}
}

func TestPG17ApplySessionKeepsReplicaRoleConnectionLocal(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	applyConn := target.Connect(t)
	if _, err := applyConn.Exec(ctx, `
		DO $$ BEGIN
			EXECUTE format('ALTER DATABASE %I SET synchronous_commit = off', current_database());
		END $$
	`); err != nil {
		t.Fatal(err)
	}
	applyConn.Close(ctx)
	applyConn = target.Connect(t)
	if err := configureApplySession(ctx, applyConn); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		tx, err := applyConn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var role, synchronousCommit string
		if err := tx.QueryRow(ctx, "SELECT current_setting('session_replication_role'), current_setting('synchronous_commit')").Scan(&role, &synchronousCommit); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if role != "replica" {
			_ = tx.Rollback(ctx)
			t.Fatalf("apply transaction %d role=%q, want replica", i+1, role)
		}
		if synchronousCommit != "on" {
			_ = tx.Rollback(ctx)
			t.Fatalf("apply transaction %d synchronous_commit=%q, want on", i+1, synchronousCommit)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	other := target.Connect(t)
	var role, synchronousCommit string
	if err := other.QueryRow(ctx, "SELECT current_setting('session_replication_role'), current_setting('synchronous_commit')").Scan(&role, &synchronousCommit); err != nil {
		t.Fatal(err)
	}
	if role != "origin" {
		t.Fatalf("unrelated target connection role=%q, want origin", role)
	}
	if synchronousCommit != "off" {
		t.Fatalf("unrelated target connection synchronous_commit=%q, want off", synchronousCommit)
	}
}

func TestPG17TargetRelationCacheInvalidatesChangedDefinition(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE public.relation_cache_items (
			id bigint PRIMARY KEY,
			value text NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	cache := newTargetRelationCache()
	source := Relation{
		OID: 991, Namespace: "public", Name: "relation_cache_items", ReplicaIdentity: 'd',
		Columns: []Column{{Name: "id", Type: 20, Flags: 1}, {Name: "value", Type: 25}},
	}
	first, err := cache.resolve(ctx, tx, &source, loadTargetRelation)
	if err != nil {
		t.Fatal(err)
	}
	again, err := cache.resolve(ctx, tx, &source, loadTargetRelation)
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatal("identical source relation missed the target metadata cache")
	}
	source.ReplicaIdentity = 'f'
	changed, err := cache.resolve(ctx, tx, &source, loadTargetRelation)
	if err != nil {
		t.Fatal(err)
	}
	if changed == first || changed.source.ReplicaIdentity != 'f' {
		t.Fatal("changed source relation definition reused stale target metadata")
	}
}

func TestPG17TransactionalProgressUpsert(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	const (
		stream     = "progress-upsert"
		generation = "progress-upsert-generation"
	)
	if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
		StreamID: stream, Generation: generation, FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE public.progress_upsert_data (id integer PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateStreamProgress(ctx, tx, stream, generation, 10, 2, 20); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var started bool
	var identityXID string
	if err := conn.QueryRow(ctx, `
		SELECT progress_started, xmin::text
		FROM `+streamIdentityTable+`
		WHERE stream_id = $1
	`, stream).Scan(&started, &identityXID); err != nil {
		t.Fatal(err)
	}
	if !started {
		t.Fatal("first progress did not mark the stream identity as started")
	}

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateStreamProgress(ctx, tx, stream, generation, 11, 3, 30); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var laterIdentityXID string
	if err := conn.QueryRow(ctx, `
		SELECT xmin::text FROM `+streamIdentityTable+` WHERE stream_id = $1
	`, stream).Scan(&laterIdentityXID); err != nil {
		t.Fatal(err)
	}
	if laterIdentityXID != identityXID {
		t.Fatalf("later progress rewrote identity row xmin %s -> %s", identityXID, laterIdentityXID)
	}

	restarted := target.Connect(t)
	if err := EnsureStreamProgressIdentity(ctx, restarted, StreamIdentityConfig{
		StreamID: stream, Generation: generation,
	}); err != nil {
		t.Fatal(err)
	}
	tx, err = restarted.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateStreamProgress(ctx, tx, stream, generation, 12, 5, 50); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	progress, exists, err := postgres.ReadProgress(ctx, restarted, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || LSN(progress) != 12 {
		t.Fatalf("restart progress=%x exists=%t, want 12", progress, exists)
	}
	replay, exists, err := postgres.ReadReplicationProgress(ctx, restarted, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || replay.Transactions != 10 || replay.Rows != 100 || replay.UpdatedAt.IsZero() {
		t.Fatalf("restart replay progress=%+v exists=%t, want 10 transactions/100 rows", replay, exists)
	}

	tx, err = restarted.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO public.progress_upsert_data VALUES (1)"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := updateStreamProgress(ctx, tx, stream, "wrong-generation", 13, 7, 70); !errors.Is(err, ErrStreamGenerationMismatch) {
		_ = tx.Rollback(ctx)
		t.Fatalf("generation mismatch error=%v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := restarted.QueryRow(ctx, "SELECT count(*) FROM public.progress_upsert_data").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("generation mismatch committed %d replay rows", count)
	}
	progress, exists, err = postgres.ReadProgress(ctx, restarted, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || LSN(progress) != 12 {
		t.Fatalf("generation mismatch changed progress to %x exists=%t", progress, exists)
	}
	replay, exists, err = postgres.ReadReplicationProgress(ctx, restarted, stream)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || replay.Transactions != 10 || replay.Rows != 100 {
		t.Fatalf("generation mismatch changed replay counters: %+v exists=%t", replay, exists)
	}

	tx, err = restarted.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := updateStreamProgress(ctx, tx, "missing-identity", generation, 1, 11, 110); !errors.Is(err, ErrStreamGenerationMismatch) {
		_ = tx.Rollback(ctx)
		t.Fatalf("missing identity error=%v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPG17PipelinedApplyPreservesAtomicOrderedReplay(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	if _, err := conn.Exec(ctx, `
		CREATE DOMAIN public.pipeline_stage_key AS bigint CHECK (VALUE > 0);
		CREATE TYPE public.pipeline_stage_mood AS ENUM ('calm', 'fast');
		CREATE TABLE public.pipeline_mixed (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_checked (
			id integer PRIMARY KEY,
			value text CHECK (value <> 'bad')
		);
		CREATE TABLE public.pipeline_missing (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_binary (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_prepared (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_cache_after_failure (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_batch (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_batch_checked (
			id integer PRIMARY KEY,
			value text CHECK (value <> 'bad')
		);
		CREATE TABLE public.pipeline_selective_update (
			id integer PRIMARY KEY,
			indexed_value text NOT NULL,
			other_value text NOT NULL,
			unique_a text NOT NULL,
			unique_b text NOT NULL,
			indexed_length integer GENERATED ALWAYS AS (length(indexed_value)) STORED,
			UNIQUE (unique_a, unique_b)
		);
		CREATE INDEX pipeline_selective_update_partial
			ON public.pipeline_selective_update (indexed_value)
			WHERE indexed_value <> '';
		CREATE INDEX pipeline_selective_update_expression
			ON public.pipeline_selective_update ((lower(indexed_value)));
		CREATE TABLE public.pipeline_unique_indexed (
			id integer PRIMARY KEY,
			value text NOT NULL
		);
		CREATE UNIQUE INDEX pipeline_unique_indexed_partial
			ON public.pipeline_unique_indexed (value) WHERE value <> '';
		CREATE TABLE public.pipeline_batch_deferred (
			id integer PRIMARY KEY,
			value text,
			UNIQUE (value) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TABLE public.pipeline_update_batch (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_update_unique (
			id integer PRIMARY KEY,
			value text NOT NULL UNIQUE
		);
		CREATE TABLE public.pipeline_update_batch_duplicates (id integer NOT NULL, value text);
		CREATE TABLE public.pipeline_copy (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_progress_guard (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_epoch_a (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_epoch_b (id integer PRIMARY KEY, value text);
		CREATE TABLE public.pipeline_stage (
			id public.pipeline_stage_key PRIMARY KEY,
			mood public.pipeline_stage_mood NOT NULL,
			note text
		);
		CREATE INDEX pipeline_stage_note_partial
			ON public.pipeline_stage ((lower(note))) WHERE note IS NOT NULL;
		CREATE TABLE public.pipeline_stage_duplicates (
			id public.pipeline_stage_key NOT NULL,
			value text
		);
		CREATE TABLE public.pipeline_deferred_commit (
			id integer PRIMARY KEY,
			value text,
			UNIQUE (value) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TABLE public.pipeline_spill (id integer PRIMARY KEY, value integer NOT NULL);
		INSERT INTO public.pipeline_spill
		SELECT id, 0 FROM generate_series(1, 257) AS id;
	`); err != nil {
		t.Fatal(err)
	}

	tuple := func(datums ...TupleDatum) *Tuple {
		value := Tuple(datums)
		return &value
	}
	text := func(value string) TupleDatum {
		return TupleDatum{Kind: DatumText, Data: []byte(value)}
	}
	relation := func(oid uint32, name string, valueOID uint32) Relation {
		return Relation{
			OID: oid, Namespace: "public", Name: name, ReplicaIdentity: 'd',
			Columns: []Column{
				{Name: "id", Type: 23, Flags: 1},
				{Name: "value", Type: valueOID},
			},
		}
	}
	stageRelation := func(oid uint32, name string) Relation {
		return Relation{
			OID: oid, Namespace: "public", Name: name, ReplicaIdentity: 'd',
			Columns: []Column{
				{Name: "id", Type: 90001, Flags: 1},
				{Name: "mood", Type: 90002},
				{Name: "note", Type: 25},
			},
		}
	}
	selectiveRelation := func(oid uint32) Relation {
		return Relation{
			OID: oid, Namespace: "public", Name: "pipeline_selective_update", ReplicaIdentity: 'd',
			Columns: []Column{
				{Name: "id", Type: 23, Flags: 1},
				{Name: "indexed_value", Type: 25},
				{Name: "other_value", Type: 25},
				{Name: "unique_a", Type: 25},
				{Name: "unique_b", Type: 25},
			},
		}
	}
	relationCache := newTargetRelationCache()
	statementCache := newApplyStatementCache(applyStatementCacheCapacity)
	apply := func(stream string, transaction *Transaction) error {
		generation := stream + "-generation"
		if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
			StreamID: stream, Generation: generation, FreshSetup: true,
		}); err != nil {
			return err
		}
		applier := &Applier{config: ApplierConfig{
			StreamID: stream, StreamGeneration: generation,
		}}
		return applier.applyTransaction(ctx, conn, relationCache, statementCache, transaction)
	}
	applyBatch := func(
		stream string, progress LSN, transactions []Transaction,
	) (bool, LSN, error) {
		generation := stream + "-generation"
		if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
			StreamID: stream, Generation: generation, FreshSetup: true,
		}); err != nil {
			return false, progress, err
		}
		applier := &Applier{config: ApplierConfig{
			StreamID: stream, StreamGeneration: generation,
		}}
		return applier.applyTransactionBatch(
			ctx, conn, relationCache, statementCache, transactions, progress,
		)
	}
	assertProgress := func(t *testing.T, stream string, want LSN) {
		t.Helper()
		progress, exists, err := postgres.ReadProgress(ctx, conn, stream)
		if err != nil {
			t.Fatal(err)
		}
		if (!exists && want != 0) || (exists && LSN(progress) != want) {
			t.Fatalf("%s progress=%x exists=%t, want %x", stream, progress, exists, want)
		}
	}

	t.Run("catalog gates cross-transaction relation lanes", func(t *testing.T) {
		plainSource := relation(1190, "pipeline_copy", 25)
		plain, err := relationCache.resolve(ctx, conn, &plainSource, loadTargetRelation)
		if err != nil {
			t.Fatal(err)
		}
		if !plain.capabilities.relationLane {
			t.Fatal("plain built-in relation was not eligible for relation-lane replay")
		}
		checkedSource := relation(1191, "pipeline_batch_checked", 25)
		checked, err := relationCache.resolve(ctx, conn, &checkedSource, loadTargetRelation)
		if err != nil {
			t.Fatal(err)
		}
		if checked.capabilities.relationLane {
			t.Fatal("checked relation was eligible for relation-lane replay")
		}
		selectiveSource := selectiveRelation(1193)
		selective, err := relationCache.resolve(ctx, conn, &selectiveSource, loadTargetRelation)
		if err != nil {
			t.Fatal(err)
		}
		if !selective.capabilities.relationLane || !selective.capabilities.keyedSetDML ||
			!selective.capabilities.selectiveUpdates {
			t.Fatalf("selective relation capabilities=%+v", selective.capabilities)
		}
		uniqueIndexedSource := relation(1194, "pipeline_unique_indexed", 25)
		uniqueIndexed, err := relationCache.resolve(ctx, conn, &uniqueIndexedSource, loadTargetRelation)
		if err != nil {
			t.Fatal(err)
		}
		if uniqueIndexed.capabilities.relationLane || uniqueIndexed.capabilities.keyedSetDML ||
			uniqueIndexed.capabilities.selectiveUpdates {
			t.Fatalf("unique partial indexed relation capabilities=%+v", uniqueIndexed.capabilities)
		}
		customSource := stageRelation(1192, "pipeline_stage")
		custom, err := relationCache.resolve(ctx, conn, &customSource, loadTargetRelation)
		if err != nil {
			t.Fatal(err)
		}
		if custom.capabilities.relationLane || !custom.capabilities.keyedSetDML ||
			custom.capabilities.binaryCopy || !custom.capabilities.textCopyStage ||
			!custom.capabilities.selectiveUpdates {
			t.Fatalf("custom relation capabilities=%+v", custom.capabilities)
		}
	})

	t.Run("selective replay preserves values and HOT-updates unindexed columns", func(t *testing.T) {
		source := selectiveRelation(1195)
		targetRelation, err := relationCache.resolve(ctx, conn, &source, loadTargetRelation)
		if err != nil {
			t.Fatal(err)
		}
		// Force the scalar VALUES inspection transport used when a real target
		// column has no usable array parameter representation, and the bitmap
		// heap path used by a target relation too large to remain cached.
		otherArrayOID := targetRelation.columns[2].arrayOID
		targetRelation.columns[2].arrayOID = 0
		targetRelation.heapBytes = selectiveBitmapMinHeapBytes
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_selective_update
				(id, indexed_value, other_value, unique_a, unique_b)
			VALUES (1, 'indexed-old', 'other-old', 'a', 'b'),
			       (2, 'indexed-two', 'other-two', 'c', 'd');
			SELECT pg_stat_reset_single_table_counters('public.pipeline_selective_update'::regclass);
		`); err != nil {
			t.Fatal(err)
		}
		transaction := Transaction{
			CommitLSN: 410, EndLSN: 411, Relations: []Relation{source},
			Changes: []Change{
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), text("indexed-old"), text("other-old"), text("a"), text("b")),
					New: tuple(text("1"), text("indexed-old"), text("other-middle"), text("a"), text("b")),
				},
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), text("indexed-old"), text("other-middle"), text("a"), text("b")),
					New: tuple(text("1"), text("indexed-old"), text("other-new"), text("a"), text("b")),
				},
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("2"), text("indexed-two"), text("other-two"), text("c"), text("d")),
					New: tuple(text("2"), text("indexed-new"), text("other-two"), text("c-new"), text("d-new")),
				},
			},
		}
		if err := apply("pipeline-selective-update", &transaction); err != nil {
			t.Fatal(err)
		}
		var values string
		if err := conn.QueryRow(ctx, `
			SELECT string_agg(indexed_value || ':' || other_value, ',' ORDER BY id)
			FROM public.pipeline_selective_update
		`).Scan(&values); err != nil {
			t.Fatal(err)
		}
		if values != "indexed-old:other-new,indexed-new:other-two" {
			t.Fatalf("selective values=%q", values)
		}
		var uniqueValues string
		if err := conn.QueryRow(ctx, `
			SELECT unique_a || ':' || unique_b
			FROM public.pipeline_selective_update WHERE id = 2
		`).Scan(&uniqueValues); err != nil {
			t.Fatal(err)
		}
		if uniqueValues != "c-new:d-new" {
			t.Fatalf("selective unique values=%q", uniqueValues)
		}
		// Restore the array transport and exercise the same bitmap target lookup
		// through its compact unnest input.
		targetRelation.columns[2].arrayOID = otherArrayOID
		arrayTransaction := Transaction{
			CommitLSN: 412, EndLSN: 413, Relations: []Relation{source},
			Changes: []Change{{
				RelationOID: source.OID, Kind: ChangeUpdate,
				Old: tuple(text("1"), text("indexed-old"), text("other-new"), text("a"), text("b")),
				New: tuple(text("1"), text("indexed-old"), text("other-array"), text("a"), text("b")),
			}},
		}
		if err := apply("pipeline-selective-update-array", &arrayTransaction); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `
			SELECT other_value FROM public.pipeline_selective_update WHERE id = 1
		`).Scan(&values); err != nil {
			t.Fatal(err)
		}
		if values != "other-array" {
			t.Fatalf("selective array value=%q", values)
		}
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_selective_update
				(id, indexed_value, other_value, unique_a, unique_b)
			VALUES (3, 'indexed-three', 'other-three', 'e', 'f')
		`); err != nil {
			t.Fatal(err)
		}
		deleteTransaction := Transaction{
			CommitLSN: 414, EndLSN: 415, Relations: []Relation{source},
			Changes: []Change{
				{
					RelationOID: source.OID, Kind: ChangeDelete,
					Old: tuple(text("2"), text("indexed-new"), text("other-two"), text("c-new"), text("d-new")),
				},
				{
					RelationOID: source.OID, Kind: ChangeDelete,
					Old: tuple(text("3"), text("indexed-three"), text("other-three"), text("e"), text("f")),
				},
			},
		}
		if err := apply("pipeline-selective-delete-array", &deleteTransaction); err != nil {
			t.Fatal(err)
		}
		var remaining int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM public.pipeline_selective_update
		`).Scan(&remaining); err != nil {
			t.Fatal(err)
		}
		if remaining != 1 {
			t.Fatalf("selective rows after bitmap delete=%d, want 1", remaining)
		}
		if _, err := conn.Exec(ctx, "SELECT pg_stat_force_next_flush()"); err != nil {
			t.Fatal(err)
		}
		var hotUpdates int64
		if err := conn.QueryRow(ctx, `
			SELECT n_tup_hot_upd FROM pg_stat_user_tables
			WHERE relid = 'public.pipeline_selective_update'::regclass
		`).Scan(&hotUpdates); err != nil {
			t.Fatal(err)
		}
		if hotUpdates < 1 {
			t.Fatalf("selective replay produced %d HOT updates, want at least 1", hotUpdates)
		}
	})

	t.Run("custom types use an atomic typed COPY stage", func(t *testing.T) {
		source := stageRelation(1193, "pipeline_stage")
		insert := Transaction{
			CommitLSN: 300, EndLSN: 301, Relations: []Relation{source},
			Changes: make([]Change, 0, 128),
		}
		for id := 1; id <= 128; id++ {
			note := fmt.Sprintf("note-%d", id)
			switch id {
			case 1:
				note = ""
			case 2:
				note = `\N`
			case 3:
				note = "tab\tline\nslash\\end"
			}
			mood := "calm"
			if id%2 == 0 {
				mood = "fast"
			}
			insert.Changes = append(insert.Changes, Change{
				RelationOID: source.OID, Kind: ChangeInsert,
				New: tuple(text(strconv.Itoa(id)), text(mood), text(note)),
			})
		}
		applied, next, err := applyBatch("pipeline-stage-insert", 0, []Transaction{insert})
		if err != nil || !applied || next != insert.EndLSN {
			t.Fatalf("staged insert applied=%t progress=%x err=%v", applied, next, err)
		}
		var count, stages int
		var notes string
		if err := conn.QueryRow(ctx, `
			SELECT count(*), string_agg(note, '|' ORDER BY id)
			FROM public.pipeline_stage WHERE id <= 3
		`).Scan(&count, &notes); err != nil {
			t.Fatal(err)
		}
		if count != 3 || notes != "|\\N|tab\tline\nslash\\end" {
			t.Fatalf("staged escaped rows count=%d notes=%q", count, notes)
		}
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_catalog.pg_class
			WHERE relnamespace = pg_my_temp_schema()
			  AND relname LIKE 'pgmigrate_stage_%'
		`).Scan(&stages); err != nil {
			t.Fatal(err)
		}
		if stages == 0 {
			t.Fatal("typed COPY did not create a temporary stage")
		}
		assertProgress(t, "pipeline-stage-insert", insert.EndLSN)

		update := Transaction{
			CommitLSN: 301, EndLSN: 302, Relations: []Relation{source},
			Changes: make([]Change, 0, 128),
		}
		for id := 1; id <= 128; id++ {
			update.Changes = append(update.Changes, Change{
				RelationOID: source.OID, Kind: ChangeUpdate,
				Old: tuple(text(strconv.Itoa(id)), TupleDatum{Kind: DatumNull}, TupleDatum{Kind: DatumNull}),
				New: tuple(text(strconv.Itoa(id)), text("fast"), text(fmt.Sprintf("updated-%d", id))),
			})
		}
		applied, next, err = applyBatch("pipeline-stage-update", 0, []Transaction{update})
		if err != nil || !applied || next != update.EndLSN {
			t.Fatalf("staged update applied=%t progress=%x err=%v", applied, next, err)
		}
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM public.pipeline_stage
			WHERE mood = 'fast' AND note = 'updated-' || id::text
		`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 128 {
			t.Fatalf("staged updates=%d, want 128", count)
		}
		assertProgress(t, "pipeline-stage-update", update.EndLSN)

		deleteTransaction := Transaction{
			CommitLSN: 302, EndLSN: 303, Relations: []Relation{source},
			Changes: make([]Change, 0, 128),
		}
		for id := 1; id <= 128; id++ {
			deleteTransaction.Changes = append(deleteTransaction.Changes, Change{
				RelationOID: source.OID, Kind: ChangeDelete,
				Old: tuple(text(strconv.Itoa(id)), TupleDatum{Kind: DatumNull}, TupleDatum{Kind: DatumNull}),
			})
		}
		applied, next, err = applyBatch(
			"pipeline-stage-delete", 0, []Transaction{deleteTransaction},
		)
		if err != nil || !applied || next != deleteTransaction.EndLSN {
			t.Fatalf("staged delete applied=%t progress=%x err=%v", applied, next, err)
		}
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.pipeline_stage").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("rows after staged delete=%d", count)
		}
		assertProgress(t, "pipeline-stage-delete", deleteTransaction.EndLSN)
	})

	t.Run("typed stage missing match rolls back every row and progress", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_stage
			SELECT id, 'calm', 'original' FROM generate_series(1, 64) AS id
		`); err != nil {
			t.Fatal(err)
		}
		source := stageRelation(1194, "pipeline_stage")
		transaction := Transaction{
			CommitLSN: 310, EndLSN: 311, Relations: []Relation{source},
			Changes: make([]Change, 0, 64),
		}
		for ordinal := 0; ordinal < 64; ordinal++ {
			id := ordinal + 1
			if ordinal == 63 {
				id = 999
			}
			transaction.Changes = append(transaction.Changes, Change{
				RelationOID: source.OID, Kind: ChangeUpdate,
				Old: tuple(text(strconv.Itoa(id)), TupleDatum{Kind: DatumNull}, TupleDatum{Kind: DatumNull}),
				New: tuple(text(strconv.Itoa(id)), text("fast"), text("changed")),
			})
		}
		applied, next, err := applyBatch("pipeline-stage-missing", 0, []Transaction{transaction})
		var divergence *DivergenceError
		if !errors.As(err, &divergence) ||
			(!strings.Contains(err.Error(), "identity ordinal 63") &&
				!strings.Contains(err.Error(), "selective inspection did not match source row 63")) {
			t.Fatalf("missing staged match error=%v", err)
		}
		if applied || next != 0 {
			t.Fatalf("missing staged match applied=%t progress=%x", applied, next)
		}
		var changed int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM public.pipeline_stage WHERE note <> 'original'
		`).Scan(&changed); err != nil {
			t.Fatal(err)
		}
		if changed != 0 {
			t.Fatalf("failed staged update retained %d changed rows", changed)
		}
		assertProgress(t, "pipeline-stage-missing", 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after staged divergence=%q, want idle", status)
		}
		if _, err := conn.Exec(ctx, "TRUNCATE public.pipeline_stage"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("typed stage duplicate target match rolls back", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_stage_duplicates
			SELECT id, 'original' FROM generate_series(1, 64) AS id;
			INSERT INTO public.pipeline_stage_duplicates VALUES (1, 'original');
		`); err != nil {
			t.Fatal(err)
		}
		source := Relation{
			OID: 1195, Namespace: "public", Name: "pipeline_stage_duplicates", ReplicaIdentity: 'd',
			Columns: []Column{
				{Name: "id", Type: 90001, Flags: 1},
				{Name: "value", Type: 25},
			},
		}
		transaction := Transaction{
			CommitLSN: 320, EndLSN: 321, Relations: []Relation{source},
			Changes: make([]Change, 0, 64),
		}
		for id := 1; id <= 64; id++ {
			transaction.Changes = append(transaction.Changes, Change{
				RelationOID: source.OID, Kind: ChangeUpdate,
				Old: tuple(text(strconv.Itoa(id)), TupleDatum{Kind: DatumNull}),
				New: tuple(text(strconv.Itoa(id)), text("changed")),
			})
		}
		applied, next, err := applyBatch("pipeline-stage-duplicate", 0, []Transaction{transaction})
		var divergence *DivergenceError
		if !errors.As(err, &divergence) || !strings.Contains(err.Error(), "identity ordinal 0 more than once") {
			t.Fatalf("duplicate staged match error=%v", err)
		}
		if applied || next != 0 {
			t.Fatalf("duplicate staged match applied=%t progress=%x", applied, next)
		}
		var changed int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM public.pipeline_stage_duplicates WHERE value <> 'original'
		`).Scan(&changed); err != nil {
			t.Fatal(err)
		}
		if changed != 0 {
			t.Fatalf("duplicate staged match retained %d changed rows", changed)
		}
		assertProgress(t, "pipeline-stage-duplicate", 0)
	})

	t.Run("ordered barrier isolates safe epochs without weakening atomicity", func(t *testing.T) {
		sourceA := relation(1196, "pipeline_epoch_a", 25)
		sourceB := relation(1197, "pipeline_epoch_b", 25)
		checked := relation(1198, "pipeline_batch_checked", 25)
		transactions := []Transaction{
			{CommitLSN: 400, EndLSN: 401, Relations: []Relation{sourceA}, Changes: []Change{{
				RelationOID: sourceA.OID, Kind: ChangeInsert, New: tuple(text("1"), text("before-a")),
			}}},
			{CommitLSN: 401, EndLSN: 402, Relations: []Relation{sourceB}, Changes: []Change{{
				RelationOID: sourceB.OID, Kind: ChangeInsert, New: tuple(text("1"), text("before-b")),
			}}},
			{CommitLSN: 402, EndLSN: 403, Relations: []Relation{checked}, Changes: []Change{{
				RelationOID: checked.OID, Kind: ChangeInsert, New: tuple(text("10"), text("bad")),
			}}},
			{CommitLSN: 403, EndLSN: 404, Relations: []Relation{sourceA}, Changes: []Change{{
				RelationOID: sourceA.OID, Kind: ChangeInsert, New: tuple(text("2"), text("after-a")),
			}}},
			{CommitLSN: 404, EndLSN: 405, Relations: []Relation{sourceB}, Changes: []Change{{
				RelationOID: sourceB.OID, Kind: ChangeInsert, New: tuple(text("2"), text("after-b")),
			}}},
		}
		applied, next, err := applyBatch("pipeline-epoch-failure", 0, transactions)
		var divergence *DivergenceError
		if !errors.As(err, &divergence) || applied || next != 0 {
			t.Fatalf("epoch failure applied=%t progress=%x err=%v", applied, next, err)
		}
		var rows int
		if err := conn.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM public.pipeline_epoch_a) +
			       (SELECT count(*) FROM public.pipeline_epoch_b)
		`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatalf("failed ordered barrier retained %d safe-epoch rows", rows)
		}
		assertProgress(t, "pipeline-epoch-failure", 0)

		transactions[2].Changes[0].New = tuple(text("10"), text("good"))
		applied, next, err = applyBatch("pipeline-epoch-success", 0, transactions)
		if err != nil || !applied || next != 405 {
			t.Fatalf("epoch success applied=%t progress=%x err=%v", applied, next, err)
		}
		if err := conn.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM public.pipeline_epoch_a) +
			       (SELECT count(*) FROM public.pipeline_epoch_b)
		`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 4 {
			t.Fatalf("successful ordered barrier rows=%d, want 4", rows)
		}
		assertProgress(t, "pipeline-epoch-success", 405)
		if _, err := conn.Exec(ctx, `
			TRUNCATE public.pipeline_epoch_a, public.pipeline_epoch_b,
			         public.pipeline_batch_checked
		`); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("bounded source transaction group commits at final progress", func(t *testing.T) {
		source := relation(1112, "pipeline_batch", 25)
		transactions := []Transaction{
			{
				CommitLSN: 200, EndLSN: 201, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("1"), text("first")),
				}},
			},
			{
				CommitLSN: 201, EndLSN: 202, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("1"), text("second")),
				}},
			},
			{
				CommitLSN: 202, EndLSN: 203, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("2"), text("third")),
				}},
			},
		}
		applied, next, err := applyBatch("pipeline-batch-success", 0, transactions)
		if err != nil {
			t.Fatal(err)
		}
		if !applied || next != 203 {
			t.Fatalf("batch applied=%t progress=%x, want true/203", applied, next)
		}
		var values string
		if err := conn.QueryRow(ctx, `
			SELECT string_agg(value, ',' ORDER BY id) FROM public.pipeline_batch
		`).Scan(&values); err != nil {
			t.Fatal(err)
		}
		if values != "second,third" {
			t.Fatalf("batched values=%q, want second,third", values)
		}
		assertProgress(t, "pipeline-batch-success", 203)
	})

	t.Run("mid-batch SQL failure rolls back the whole group", func(t *testing.T) {
		source := relation(1113, "pipeline_batch_checked", 25)
		transactions := []Transaction{
			{
				CommitLSN: 210, EndLSN: 211, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("1"), text("before")),
				}},
			},
			{
				CommitLSN: 211, EndLSN: 212, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("2"), text("bad")),
				}},
			},
			{
				CommitLSN: 212, EndLSN: 213, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("3"), text("after")),
				}},
			},
		}
		applied, next, err := applyBatch("pipeline-batch-failure", 0, transactions)
		var divergence *DivergenceError
		if !errors.As(err, &divergence) {
			t.Fatalf("batch SQL error=%v, want divergence", err)
		}
		if applied || next != 0 {
			t.Fatalf("failed batch applied=%t progress=%x, want false/0", applied, next)
		}
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.pipeline_batch_checked").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("failed batch retained %d rows", count)
		}
		assertProgress(t, "pipeline-batch-failure", 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after failed batch=%q, want idle", status)
		}
	})

	t.Run("batched deferred commit failure rolls back data and progress", func(t *testing.T) {
		source := relation(1114, "pipeline_batch_deferred", 25)
		transactions := []Transaction{
			{
				CommitLSN: 220, EndLSN: 221, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("1"), text("duplicate")),
				}},
			},
			{
				CommitLSN: 221, EndLSN: 222, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("2"), text("duplicate")),
				}},
			},
		}
		applied, next, err := applyBatch("pipeline-batch-deferred", 0, transactions)
		var divergence *DivergenceError
		if !errors.As(err, &divergence) {
			t.Fatalf("batched deferred commit error=%v, want divergence", err)
		}
		if applied || next != 0 {
			t.Fatalf("failed deferred batch applied=%t progress=%x, want false/0", applied, next)
		}
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.pipeline_batch_deferred").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("failed deferred batch retained %d rows", count)
		}
		assertProgress(t, "pipeline-batch-deferred", 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after failed deferred batch=%q, want idle", status)
		}
	})

	t.Run("consecutive keyed updates use one checked set operation", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_update_batch
			VALUES (1, 'old-1'), (2, 'old-2'), (3, 'old-3'), (4, 'old-4')
		`); err != nil {
			t.Fatal(err)
		}
		source := relation(1115, "pipeline_update_batch", 25)
		transaction := &Transaction{
			CommitLSN: 230, EndLSN: 231, Relations: []Relation{source},
		}
		for id := 1; id <= 4; id++ {
			transaction.Changes = append(transaction.Changes, Change{
				RelationOID: source.OID, Kind: ChangeUpdate,
				Old: tuple(text(strconv.Itoa(id)), TupleDatum{Kind: DatumNull}),
				New: tuple(text(strconv.Itoa(id)), text(fmt.Sprintf("new-%d", id))),
			})
		}
		if err := apply("pipeline-update-batch", transaction); err != nil {
			t.Fatal(err)
		}
		var values string
		if err := conn.QueryRow(ctx, `
			SELECT string_agg(value, ',' ORDER BY id) FROM public.pipeline_update_batch
		`).Scan(&values); err != nil {
			t.Fatal(err)
		}
		if values != "new-1,new-2,new-3,new-4" {
			t.Fatalf("batched update values=%q", values)
		}
		var prepared int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_catalog.pg_prepared_statements
			WHERE statement LIKE 'UPDATE "public"."pipeline_update_batch" AS pgmigrate_target%'
			  AND statement LIKE '%RETURNING pgmigrate_batch.ordinal%'
		`).Scan(&prepared); err != nil {
			t.Fatal(err)
		}
		if prepared != 1 {
			t.Fatalf("set-based update prepared statements=%d, want 1", prepared)
		}
		assertProgress(t, "pipeline-update-batch", 231)
	})

	t.Run("unique-value transitions preserve source statement order", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_update_unique
			VALUES (1, 'a'), (2, 'b'), (3, 'c')
		`); err != nil {
			t.Fatal(err)
		}
		source := relation(1199, "pipeline_update_unique", 25)
		transaction := &Transaction{
			CommitLSN: 330, EndLSN: 331, Relations: []Relation{source},
			Changes: []Change{
				{RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("1"), text("temporary"))},
				{RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("2"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("2"), text("a"))},
				{RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("3"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("3"), text("b"))},
			},
		}
		if err := apply("pipeline-update-unique", transaction); err != nil {
			t.Fatal(err)
		}
		var values string
		if err := conn.QueryRow(ctx, `
			SELECT string_agg(value, ',' ORDER BY id)
			FROM public.pipeline_update_unique
		`).Scan(&values); err != nil {
			t.Fatal(err)
		}
		if values != "temporary,a,b" {
			t.Fatalf("unique transition values=%q", values)
		}
		var setStatements int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM pg_catalog.pg_prepared_statements
			WHERE statement LIKE 'UPDATE "public"."pipeline_update_unique" AS pgmigrate_target%'
			  AND statement LIKE '%RETURNING pgmigrate_batch.ordinal%'
		`).Scan(&setStatements); err != nil {
			t.Fatal(err)
		}
		if setStatements != 0 {
			t.Fatalf("unique transition used %d set statements", setStatements)
		}
		assertProgress(t, "pipeline-update-unique", transaction.EndLSN)
	})

	t.Run("repeated source identity remains sequential", func(t *testing.T) {
		source := relation(1115, "pipeline_update_batch", 25)
		transaction := &Transaction{
			CommitLSN: 231, EndLSN: 232, Relations: []Relation{source},
			Changes: []Change{
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("1"), text("first")),
				},
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("1"), text("second")),
				},
			},
		}
		if err := apply("pipeline-update-repeated-identity", transaction); err != nil {
			t.Fatal(err)
		}
		var value string
		if err := conn.QueryRow(ctx, `
			SELECT value FROM public.pipeline_update_batch WHERE id = 1
		`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value != "second" {
			t.Fatalf("repeated identity value=%q, want second", value)
		}
		assertProgress(t, "pipeline-update-repeated-identity", 232)
	})

	t.Run("duplicate target match in update batch rolls back", func(t *testing.T) {
		if _, err := conn.Exec(ctx, `
			INSERT INTO public.pipeline_update_batch_duplicates
			VALUES (1, 'original'), (1, 'original')
		`); err != nil {
			t.Fatal(err)
		}
		source := relation(1116, "pipeline_update_batch_duplicates", 25)
		transaction := &Transaction{
			CommitLSN: 240, EndLSN: 241, Relations: []Relation{source},
			Changes: []Change{
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("1"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("1"), text("changed")),
				},
				{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("2"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("2"), text("missing")),
				},
			},
		}
		err := apply("pipeline-update-duplicate-target", transaction)
		var divergence *DivergenceError
		if !errors.As(err, &divergence) || !strings.Contains(err.Error(), "identity ordinal 0 more than once") {
			t.Fatalf("duplicate target update error=%v, want ordinal divergence", err)
		}
		var changed int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM public.pipeline_update_batch_duplicates WHERE value <> 'original'
		`).Scan(&changed); err != nil {
			t.Fatal(err)
		}
		if changed != 0 {
			t.Fatalf("duplicate target update retained %d changed rows", changed)
		}
		assertProgress(t, "pipeline-update-duplicate-target", 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after duplicate target=%q, want idle", status)
		}
	})

	t.Run("binary copy failure rolls back data and progress", func(t *testing.T) {
		source := relation(1117, "pipeline_copy", 25)
		transaction := &Transaction{
			CommitLSN: 250, EndLSN: 251, Relations: []Relation{source},
			Changes: make([]Change, 0, 300),
		}
		for id := 1; id <= 300; id++ {
			encodedID := make([]byte, 4)
			binary.BigEndian.PutUint32(encodedID, uint32(id))
			if id == 300 {
				binary.BigEndian.PutUint32(encodedID, 150)
			}
			transaction.Changes = append(transaction.Changes, Change{
				RelationOID: source.OID, Kind: ChangeInsert,
				New: tuple(
					TupleDatum{Kind: DatumBinary, Data: encodedID},
					TupleDatum{Kind: DatumBinary, Data: []byte("copied")},
				),
			})
		}
		err := apply("pipeline-copy-failure", transaction)
		var divergence *DivergenceError
		if !errors.As(err, &divergence) {
			t.Fatalf("binary copy error=%v, want divergence", err)
		}
		var count int
		if err := conn.QueryRow(ctx, `
			SELECT count(*) FROM public.pipeline_copy
		`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("failed binary copy retained %d rows", count)
		}
		assertProgress(t, "pipeline-copy-failure", 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after failed binary copy=%q, want idle", status)
		}
	})

	t.Run("mixed DML remains ordered", func(t *testing.T) {
		source := relation(1101, "pipeline_mixed", 25)
		transaction := &Transaction{
			CommitLSN: 10, EndLSN: 11, Relations: []Relation{source},
			Changes: []Change{
				{RelationOID: source.OID, Kind: ChangeInsert, New: tuple(text("1"), text("first"))},
				{RelationOID: source.OID, Kind: ChangeUpdate, Old: tuple(text("1"), TupleDatum{Kind: DatumNull}), New: tuple(text("1"), text("updated"))},
				{RelationOID: source.OID, Kind: ChangeInsert, New: tuple(text("2"), text("second"))},
				{RelationOID: source.OID, Kind: ChangeDelete, Old: tuple(text("1"), TupleDatum{Kind: DatumNull})},
			},
		}
		if err := apply("pipeline-mixed", transaction); err != nil {
			t.Fatal(err)
		}
		var id int
		var value string
		if err := conn.QueryRow(ctx, "SELECT id, value FROM public.pipeline_mixed").Scan(&id, &value); err != nil {
			t.Fatal(err)
		}
		if id != 2 || value != "second" {
			t.Fatalf("mixed replay row=%d/%q, want 2/second", id, value)
		}
		assertProgress(t, "pipeline-mixed", transaction.EndLSN)
	})

	t.Run("prepared DML is reused across source transactions", func(t *testing.T) {
		source := relation(1108, "pipeline_prepared", 25)
		for id, endLSN := range []LSN{61, 62} {
			transaction := &Transaction{
				CommitLSN: endLSN - 1, EndLSN: endLSN, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text(fmt.Sprint(id+1)), text("prepared")),
				}},
			}
			if err := apply("pipeline-prepared", transaction); err != nil {
				t.Fatal(err)
			}
		}
		var prepared int
		if err := conn.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_catalog.pg_prepared_statements
			WHERE statement LIKE 'INSERT INTO "public"."pipeline_prepared"%'
		`).Scan(&prepared); err != nil {
			t.Fatal(err)
		}
		if prepared != 1 {
			t.Fatalf("prepared INSERT statements=%d, want one reused statement", prepared)
		}
		assertProgress(t, "pipeline-prepared", 62)
	})

	t.Run("prepared DML eviction deallocates the server statement", func(t *testing.T) {
		evictionConn := target.Connect(t)
		source := relation(1109, "pipeline_prepared", 25)
		const stream = "pipeline-prepared-eviction"
		const generation = stream + "-generation"
		if err := EnsureStreamProgressIdentity(ctx, evictionConn, StreamIdentityConfig{
			StreamID: stream, Generation: generation, FreshSetup: true,
		}); err != nil {
			t.Fatal(err)
		}
		applier := &Applier{config: ApplierConfig{
			StreamID: stream, StreamGeneration: generation,
		}}
		evictionRelations := newTargetRelationCache()
		evictionStatements := newApplyStatementCache(1)
		if err := applier.applyTransaction(
			ctx, evictionConn, evictionRelations, evictionStatements,
			&Transaction{
				CommitLSN: 69, EndLSN: 70, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("100"), text("before")),
				}},
			},
		); err != nil {
			t.Fatal(err)
		}
		var evictedName string
		if err := evictionConn.QueryRow(
			ctx, `
				SELECT name FROM pg_catalog.pg_prepared_statements
				WHERE name LIKE 'pgmigrate_cdc_%'
			`,
		).Scan(&evictedName); err != nil {
			t.Fatal(err)
		}
		if err := applier.applyTransaction(
			ctx, evictionConn, evictionRelations, evictionStatements,
			&Transaction{
				CommitLSN: 70, EndLSN: 71, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeUpdate,
					Old: tuple(text("100"), TupleDatum{Kind: DatumNull}),
					New: tuple(text("100"), text("after")),
				}},
			},
		); err != nil {
			t.Fatal(err)
		}
		var oldCount, preparedCount int
		if err := evictionConn.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE name = $1), count(*)
			FROM pg_catalog.pg_prepared_statements
			WHERE name LIKE 'pgmigrate_cdc_%'
		`, evictedName).Scan(&oldCount, &preparedCount); err != nil {
			t.Fatal(err)
		}
		if oldCount != 0 || preparedCount != 1 {
			t.Fatalf("after eviction old/current prepared statements=%d/%d, want 0/1", oldCount, preparedCount)
		}
	})

	t.Run("SQL failure rolls back data and progress", func(t *testing.T) {
		source := relation(1102, "pipeline_checked", 25)
		transaction := &Transaction{
			CommitLSN: 20, EndLSN: 21, Relations: []Relation{source},
			Changes: []Change{
				{RelationOID: source.OID, Kind: ChangeInsert, New: tuple(text("1"), text("good"))},
				{RelationOID: source.OID, Kind: ChangeUpdate, Old: tuple(text("1"), TupleDatum{Kind: DatumNull}), New: tuple(text("1"), text("bad"))},
				{RelationOID: source.OID, Kind: ChangeInsert, New: tuple(text("2"), text("later"))},
			},
		}
		var divergence *DivergenceError
		if err := apply("pipeline-sql-failure", transaction); !errors.As(err, &divergence) {
			t.Fatalf("pipeline SQL error=%v, want divergence", err)
		}
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.pipeline_checked").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("failed pipeline committed %d rows", count)
		}
		assertProgress(t, "pipeline-sql-failure", 0)
	})

	t.Run("pipeline failure cannot poison the prepared statement cache", func(t *testing.T) {
		checked := relation(1200, "pipeline_checked", 25)
		prepared := relation(1201, "pipeline_cache_after_failure", 25)
		failed := &Transaction{
			CommitLSN: 340, EndLSN: 341, Relations: []Relation{checked, prepared},
			Changes: []Change{
				{RelationOID: checked.OID, Kind: ChangeInsert, New: tuple(text("20"), text("bad"))},
				{RelationOID: prepared.OID, Kind: ChangeInsert, New: tuple(text("200"), text("skipped"))},
			},
		}
		var divergence *DivergenceError
		if err := apply("pipeline-cache-poison-failure", failed); !errors.As(err, &divergence) {
			t.Fatalf("pipeline cache setup error=%v, want divergence", err)
		}
		recovery := &Transaction{
			CommitLSN: 341, EndLSN: 342, Relations: []Relation{prepared},
			Changes: []Change{{
				RelationOID: prepared.OID, Kind: ChangeInsert,
				New: tuple(text("200"), text("recovered")),
			}},
		}
		if err := apply("pipeline-cache-poison-recovery", recovery); err != nil {
			t.Fatal(err)
		}
		var value string
		if err := conn.QueryRow(ctx, `
			SELECT value FROM public.pipeline_cache_after_failure WHERE id = 200
		`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value != "recovered" {
			t.Fatalf("prepared cache recovery value=%q", value)
		}
		assertProgress(t, "pipeline-cache-poison-failure", 0)
		assertProgress(t, "pipeline-cache-poison-recovery", recovery.EndLSN)
	})

	t.Run("progress mismatch aborts before queued commit", func(t *testing.T) {
		const stream = "pipeline-progress-guard"
		const generation = stream + "-generation"
		if err := EnsureStreamProgressIdentity(ctx, conn, StreamIdentityConfig{
			StreamID: stream, Generation: generation, FreshSetup: true,
		}); err != nil {
			t.Fatal(err)
		}
		source := relation(1110, "pipeline_progress_guard", 25)
		applier := &Applier{config: ApplierConfig{
			StreamID: stream, StreamGeneration: "wrong-generation",
		}}
		err := applier.applyTransaction(
			ctx, conn, relationCache, statementCache,
			&Transaction{
				CommitLSN: 79, EndLSN: 80, Relations: []Relation{source},
				Changes: []Change{{
					RelationOID: source.OID, Kind: ChangeInsert,
					New: tuple(text("1"), text("must roll back")),
				}},
			},
		)
		if !errors.Is(err, ErrStreamGenerationMismatch) {
			t.Fatalf("progress mismatch error=%v", err)
		}
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.pipeline_progress_guard").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("progress mismatch committed %d rows", count)
		}
		assertProgress(t, stream, 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after progress mismatch=%q, want idle", status)
		}
	})

	t.Run("deferred commit failure rolls back data and progress", func(t *testing.T) {
		source := relation(1111, "pipeline_deferred_commit", 25)
		transaction := &Transaction{
			CommitLSN: 89, EndLSN: 90, Relations: []Relation{source},
			Changes: []Change{
				{RelationOID: source.OID, Kind: ChangeInsert, New: tuple(text("1"), text("duplicate"))},
				{RelationOID: source.OID, Kind: ChangeInsert, New: tuple(text("2"), text("duplicate"))},
			},
		}
		var divergence *DivergenceError
		if err := apply("pipeline-deferred-commit", transaction); !errors.As(err, &divergence) {
			t.Fatalf("deferred commit error=%v, want divergence", err)
		}
		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM public.pipeline_deferred_commit").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("failed commit retained %d rows", count)
		}
		assertProgress(t, "pipeline-deferred-commit", 0)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after failed commit=%q, want idle", status)
		}
	})

	t.Run("empty source transaction advances progress", func(t *testing.T) {
		transaction := &Transaction{CommitLSN: 99, EndLSN: 100}
		if err := apply("pipeline-empty", transaction); err != nil {
			t.Fatal(err)
		}
		assertProgress(t, "pipeline-empty", transaction.EndLSN)
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("connection status after empty transaction=%q, want idle", status)
		}
	})

	for _, kind := range []ChangeKind{ChangeUpdate, ChangeDelete} {
		t.Run("zero-row "+changeKindName(kind)+" rolls back progress", func(t *testing.T) {
			source := relation(1103+uint32(kind), "pipeline_missing", 25)
			change := Change{
				RelationOID: source.OID, Kind: kind,
				Old: tuple(text("404"), TupleDatum{Kind: DatumNull}),
			}
			if kind == ChangeUpdate {
				change.New = tuple(text("404"), text("missing"))
			}
			stream := "pipeline-zero-" + changeKindName(kind)
			var divergence *DivergenceError
			err := apply(stream, &Transaction{
				CommitLSN: 30 + LSN(kind), EndLSN: 31 + LSN(kind),
				Relations: []Relation{source}, Changes: []Change{change},
			})
			if !errors.As(err, &divergence) {
				t.Fatalf("zero-row %s error=%v, want divergence", changeKindName(kind), err)
			}
			assertProgress(t, stream, 0)
		})
	}

	t.Run("binary and null parameters", func(t *testing.T) {
		source := relation(1106, "pipeline_binary", 25)
		id := make([]byte, 4)
		binary.BigEndian.PutUint32(id, 7)
		transaction := &Transaction{
			CommitLSN: 40, EndLSN: 41, Relations: []Relation{source},
			Changes: []Change{{
				RelationOID: source.OID, Kind: ChangeInsert,
				New: tuple(
					TupleDatum{Kind: DatumBinary, Data: id},
					TupleDatum{Kind: DatumNull},
				),
			}},
		}
		if err := apply("pipeline-binary", transaction); err != nil {
			t.Fatal(err)
		}
		var gotID int
		var null bool
		if err := conn.QueryRow(
			ctx, "SELECT id, value IS NULL FROM public.pipeline_binary",
		).Scan(&gotID, &null); err != nil {
			t.Fatal(err)
		}
		if gotID != 7 || !null {
			t.Fatalf("binary/null row=%d null=%t, want 7/true", gotID, null)
		}
	})

	t.Run("spilled transaction crosses pipeline windows", func(t *testing.T) {
		source := relation(1107, "pipeline_spill", 23)
		spill, err := newTransactionSpill(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer spill.closeAndRemove()
		for id := 1; id <= applyPipelineWindow+1; id++ {
			change := Change{
				RelationOID: source.OID, Kind: ChangeUpdate,
				Old: tuple(text(fmt.Sprint(id)), TupleDatum{Kind: DatumNull}),
				New: tuple(text(fmt.Sprint(id)), text("1")),
			}
			if err := spill.appendChange(&change); err != nil {
				t.Fatal(err)
			}
		}
		transaction := &Transaction{
			CommitLSN: 50, EndLSN: 51, Relations: []Relation{source}, Spill: spill,
		}
		if err := apply("pipeline-spill", transaction); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := conn.QueryRow(
			ctx, "SELECT count(*) FROM public.pipeline_spill WHERE value = 1",
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != applyPipelineWindow+1 {
			t.Fatalf("updated spill rows=%d, want %d", count, applyPipelineWindow+1)
		}
		assertProgress(t, "pipeline-spill", transaction.EndLSN)
	})
}

func TestPG17ApplierStartsBeforeReadingUnappliedSuffix(t *testing.T) {
	target := pgtest.Start(t, 17)
	ctx := context.Background()
	targetSQL := target.Connect(t)
	if _, err := targetSQL.Exec(ctx, `
		CREATE TABLE public.catchup_probe (
			id bigint PRIMARY KEY,
			value text NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory, RotationBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	var lastEnd LSN
	for i := 1; i <= 256; i++ {
		value := fmt.Sprint(i)
		row := Tuple{
			{Kind: DatumText, Data: []byte(value)},
			{Kind: DatumText, Data: []byte("value-" + value)},
		}
		transaction := Transaction{
			CommitLSN:  LSN(i * 0x10),
			EndLSN:     LSN(i*0x10 + 1),
			CommitTime: time.Unix(int64(i), 0).UTC(),
			Relations: []Relation{{
				OID:             4242,
				Namespace:       "public",
				Name:            "catchup_probe",
				ReplicaIdentity: 'd',
				Columns: []Column{
					{Name: "id", Type: 20, Flags: 1},
					{Name: "value", Type: 25},
				},
			}},
			Changes: []Change{{
				RelationOID: 4242,
				Kind:        ChangeInsert,
				New:         &row,
			}},
		}
		lastEnd = transaction.EndLSN
		if err := writer.Append(&transaction); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	ranges := writer.SegmentCatalog().snapshot()
	last := ranges[len(ranges)-1]
	file, err := os.OpenFile(last.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, frameHeaderSize); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	const streamID = "catchup-starts-before-suffix"
	const generation = "generation-1"
	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID: streamID, Generation: generation, FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := postgres.UpdateProgress(ctx, targetSQL, streamID, 0); err != nil {
		t.Fatal(err)
	}
	if err := EnsureStreamProgressIdentity(ctx, targetSQL, StreamIdentityConfig{
		StreamID: streamID, Generation: generation, FreshSetup: true,
	}); err != nil {
		t.Fatal(err)
	}
	watermark := new(DurableWatermark)
	watermark.Publish(lastEnd)
	pruner, err := NewSegmentPruner(SegmentPrunerConfig{
		Directory: directory,
		Interval:  time.Nanosecond,
		Catalog:   writer.SegmentCatalog(),
	})
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewApplier(ApplierConfig{
		ConnString:           target.URI,
		Directory:            directory,
		StreamID:             streamID,
		StreamGeneration:     generation,
		TargetHasCopiedData:  true,
		Durable:              watermark,
		PollInterval:         time.Millisecond,
		AfterProgress:        pruner.OnProgress,
		ReaderSpillDirectory: filepath.Join(t.TempDir(), "reader-spill"),
	})
	if err != nil {
		t.Fatal(err)
	}
	applyCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.Run(applyCtx) }()
	deadline := time.Now().Add(30 * time.Second)
	var progress pglogrepl.LSN
	for time.Now().Before(deadline) {
		progress, _, err = postgres.ReadProgress(ctx, targetSQL, streamID)
		if err != nil {
			stop()
			t.Fatal(err)
		}
		if progress > 0 {
			break
		}
		select {
		case err := <-done:
			stop()
			t.Fatalf("applier reached corrupt suffix before first progress: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if progress == 0 {
		stop()
		t.Fatal("target progress did not move before the unapplied suffix")
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func waitFor(t testing.TB, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for CDC condition")
}

func waitForOrError(t testing.TB, timeout time.Duration, done <-chan error, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("CDC worker exited before condition: %v", err)
		default:
		}
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for CDC condition")
}
