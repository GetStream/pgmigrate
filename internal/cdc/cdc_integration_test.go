//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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
	if err := configureApplySession(ctx, applyConn); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		tx, err := applyConn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var role string
		if err := tx.QueryRow(ctx, "SHOW session_replication_role").Scan(&role); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if role != "replica" {
			_ = tx.Rollback(ctx)
			t.Fatalf("apply transaction %d role=%q, want replica", i+1, role)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	other := target.Connect(t)
	var role string
	if err := other.QueryRow(ctx, "SHOW session_replication_role").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if role != "origin" {
		t.Fatalf("unrelated target connection role=%q, want origin", role)
	}
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
