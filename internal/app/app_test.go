package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/cdc"
	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/copy"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/preflight"
	"github.com/GetStream/pgmigrate/internal/schema"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestWALHeadroomAlarm(t *testing.T) {
	const maxWALSize = 1 << 20
	tests := map[string]struct {
		usage walUsage
		alarm bool
	}{
		"bounded below limit": {
			usage: walUsage{Retained: 10, SlotLimit: 100, MaxWALSize: maxWALSize},
		},
		"bounded at limit": {
			usage: walUsage{Retained: 100, SlotLimit: 100, MaxWALSize: maxWALSize},
			alarm: true,
		},
		"unbounded within max_wal_size": {
			usage: walUsage{
				Retained: maxWALSize / 2, SlotLimit: -1, MaxWALSize: maxWALSize,
				DirectoryBytes: 8 * maxWALSize,
			},
		},
		"unbounded directory grown": {
			usage: walUsage{
				Retained: 2 * maxWALSize, SlotLimit: -1, MaxWALSize: maxWALSize,
				DirectoryBytes: walGrowthFactor * maxWALSize,
			},
			alarm: true,
		},
		"unbounded directory stable": {
			usage: walUsage{
				Retained: 2 * maxWALSize, SlotLimit: -1, MaxWALSize: maxWALSize,
				DirectoryBytes: 2 * maxWALSize,
			},
		},
		"unbounded directory unavailable": {
			usage: walUsage{
				Retained: walGrowthFactor * maxWALSize, SlotLimit: -1, MaxWALSize: maxWALSize,
				DirectoryBytes: -1,
			},
			alarm: true,
		},
		"unbounded directory unavailable below threshold": {
			usage: walUsage{
				Retained: 2 * maxWALSize, SlotLimit: -1, MaxWALSize: maxWALSize,
				DirectoryBytes: -1,
			},
		},
		"unbounded without max_wal_size": {
			usage: walUsage{Retained: 1 << 40, SlotLimit: -1, DirectoryBytes: 1 << 40},
		},
	}
	for name, test := range tests {
		message, alarm := walHeadroomAlarm(test.usage)
		if alarm != test.alarm {
			t.Errorf("%s: alarm = %v, want %v (%q)", name, alarm, test.alarm, message)
		}
		if alarm && message == "" {
			t.Errorf("%s: alarm without message", name)
		}
	}
}

// TestPrintFindingsKeepsAParagraphedMessageReadable covers the output an operator
// actually reads when preflight refuses a run. A message with paragraphs, such as
// the two collations shown side by side, has to stay visibly one finding, and a
// preflight that stopped early has to say so rather than let the checks it never
// ran read as checks that passed.
func TestPrintFindingsKeepsAParagraphedMessageReadable(t *testing.T) {
	var out bytes.Buffer
	printFindings(&out, preflight.Result{
		Incomplete: true,
		Findings: []preflight.Finding{{
			ID: "collation-locale", Kind: "collation", Severity: preflight.SeverityError,
			Message: "source and target use incompatible collations\n\n" +
				"  source:      en_US.UTF-8 [libc]\n  target:      de_DE.UTF8 [libc]\n\n" +
				"rerun with --allow-collation-change.",
		}},
	})
	printed := out.String()
	t.Log("\n" + printed)
	if !strings.HasPrefix(printed,
		"error collation-locale: source and target use incompatible collations\n") {
		t.Errorf("the first line does not head the finding:\n%s", printed)
	}
	for _, want := range []string{
		"    en_US.UTF-8", "    de_DE.UTF8", "\n\n",
		"preflight stopped here; the remaining checks did not run\n",
	} {
		if !strings.Contains(printed, want) {
			t.Errorf("output omits %q:\n%s", want, printed)
		}
	}
	// A continuation line that starts at column zero reads as another finding.
	for _, line := range strings.Split(strings.TrimRight(printed, "\n"), "\n")[1:] {
		if line != "" && !strings.HasPrefix(line, "    ") &&
			!strings.HasPrefix(line, "preflight stopped") {
			t.Errorf("unindented continuation line %q:\n%s", line, printed)
		}
	}
}

func TestManualEndPositionRejectsInvalidLSNBeforeStateAccess(t *testing.T) {
	if err := setManualEndPosition(context.Background(), "invalid", t.TempDir(), 0, nil); err == nil || !strings.Contains(err.Error(), "parse manual end position") {
		t.Fatalf("error = %v", err)
	}
}

func TestCompletedCutoverRerunReadsReportAfterTargetCleanup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := state.Open(ctx, dir, state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []state.Phase{
		state.PhaseSetup, state.PhaseSchema, state.PhaseCopy, state.PhaseIndexes,
		state.PhaseCatchup, state.PhaseFollow, state.PhaseDrained, state.PhaseCutover, state.PhaseComplete,
	} {
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CompleteStep(ctx, "target.cleanup.completed", "run stopped"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	if err := os.WriteFile(filepath.Join(dir, "cutover-report.json"),
		[]byte(`{"version":1,"end_position":"0/123"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = (App{Out: &output}).Cutover(ctx, config.Config{
		Dir: dir, Source: "not-used", Target: "not-used",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"end_position":"0/123"`) {
		t.Fatalf("report output = %s", output.String())
	}
}

// TestAwaitCatchupStopsWhenTheApplierStops covers the supervision gap that made
// every other CDC defect invisible: catch-up waits on target progress that only
// the applier advances, so an applier that exits must end the wait rather than
// leave its error unread while the wait polls forever.
func TestAwaitCatchupStopsWhenTheApplierStops(t *testing.T) {
	t.Parallel()
	forever := func(ctx context.Context, _ cdc.LSN) error {
		<-ctx.Done()
		return ctx.Err()
	}
	boom := errors.New("cdc: divergence applying insert change to public.t: not-null violation")

	t.Run("failure surfaces", func(t *testing.T) {
		t.Parallel()
		applier := make(chan error, 1)
		applier <- boom
		if err := awaitCatchup(context.Background(), 1, forever, applier); !errors.Is(err, boom) {
			t.Fatalf("error = %v, want %v", err, boom)
		}
	})

	// An applier that returns nil has still stopped advancing progress, so the
	// wait can never finish. Reporting no error there would hang just as badly.
	t.Run("silent exit surfaces", func(t *testing.T) {
		t.Parallel()
		applier := make(chan error, 1)
		applier <- nil
		err := awaitCatchup(context.Background(), 1, forever, applier)
		if err == nil || !strings.Contains(err.Error(), "stopped before catch-up") {
			t.Fatalf("error = %v", err)
		}
	})

	// The wait must still be able to complete on its own, and must not be left
	// running once it has.
	t.Run("wait completes", func(t *testing.T) {
		t.Parallel()
		stopped := make(chan struct{})
		wait := func(ctx context.Context, boundary cdc.LSN) error {
			go func() { <-ctx.Done(); close(stopped) }()
			if boundary != 7 {
				return fmt.Errorf("boundary = %d, want 7", boundary)
			}
			return nil
		}
		if err := awaitCatchup(context.Background(), 7, wait, make(chan error, 1)); err != nil {
			t.Fatal(err)
		}
		<-stopped
	})
}

// TestRepeatedBaseCopyFailureStopsRestarting covers the amplification that makes
// any copy-phase defect fatal rather than slow: restarting drops the target and
// copies again from a new snapshot, so a failure that repeats never finishes.
func TestRepeatedBaseCopyFailureStopsRestarting(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := state.Open(ctx, dir, state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, phase := range []state.Phase{state.PhaseSetup, state.PhaseSchema, state.PhaseCopy} {
		if err := store.TransitionPhase(ctx, phase); err != nil {
			t.Fatal(err)
		}
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Dir: dir}
	exhausted := &pgconn.PgError{Code: "53000", Message: "no empty local buffer available"}
	failure := fmt.Errorf("copy part pk-000003 of %q: %w", "messages", exhausted)

	// A first failure is indistinguishable from a transient one, so it restarts.
	recordFailedAttempt(ctx, io.Discard, dir, store, failure)
	if err := guardRepeatedBaseCopyFailure(ctx, cfg, store, migration); err != nil {
		t.Fatalf("first restart refused: %v", err)
	}
	// The same failure again is deterministic, whatever its wording that run.
	recordFailedAttempt(ctx, io.Discard, dir, store, failure)
	recordFailedAttempt(ctx, io.Discard, dir, store, fmt.Errorf("copy part pk-000001 of %q: %w", "messages", exhausted))
	err = guardRepeatedBaseCopyFailure(ctx, cfg, store, migration)
	if !errors.Is(err, errRepeatedBaseCopyFailure) {
		t.Fatalf("second restart error = %v", err)
	}
	if !strings.Contains(err.Error(), "--retry-base-copy") || !strings.Contains(err.Error(), "no empty local buffer") {
		t.Fatalf("refusal is not actionable: %v", err)
	}
	// Refusing must not overwrite the record it read, or the next start would see
	// a first failure again and resume the loop.
	recordFailedAttempt(ctx, io.Discard, dir, store, err)
	if err := guardRepeatedBaseCopyFailure(ctx, cfg, store, migration); !errors.Is(err, errRepeatedBaseCopyFailure) {
		t.Fatalf("refusal did not persist: %v", err)
	}
	// An operator who has fixed the cause gets one explicit override.
	forced := cfg
	forced.RetryBaseCopy = true
	if err := guardRepeatedBaseCopyFailure(ctx, forced, store, migration); err != nil {
		t.Fatalf("forced restart refused: %v", err)
	}
	if err := guardRepeatedBaseCopyFailure(ctx, cfg, store, migration); err != nil {
		t.Fatalf("restart refused after override cleared the record: %v", err)
	}
	// A deliberate stop is not a failure.
	recordFailedAttempt(ctx, io.Discard, dir, store, context.Canceled)
	recordFailedAttempt(ctx, io.Discard, dir, store, fmt.Errorf("shutting down: %w", context.Canceled))
	attempt, err := store.FailedAttempt(ctx)
	if err != nil || attempt.Consecutive != 0 {
		t.Fatalf("cancelled run recorded as failure %#v err=%v", attempt, err)
	}
}

func TestFailureSignatureGroupsRetriesOfTheSameCause(t *testing.T) {
	buffers := &pgconn.PgError{Code: "53000", Message: "no empty local buffer available"}
	first := failureSignature(fmt.Errorf("copy part pk-000003 of %q: %w", "a", buffers))
	second := failureSignature(fmt.Errorf("copy part pk-000007 of %q: %w", "b", buffers))
	if first != second {
		t.Fatalf("same SQLSTATE signed differently: %s and %s", first, second)
	}
	if disk := failureSignature(&pgconn.PgError{Code: "53100"}); disk == first {
		t.Fatalf("different SQLSTATE signed alike: %s", disk)
	}
	plain := failureSignature(errors.New("pg_restore failed: exit status 1"))
	if plain == first || plain != failureSignature(errors.New("pg_restore failed: exit status 1")) {
		t.Fatalf("unstable signature for a non-PostgreSQL error: %s", plain)
	}
}

func TestTargetProgressPassesFailureOnlyMonotonically(t *testing.T) {
	t.Parallel()
	baseline := postgres.ReplicationProgress{
		RemoteLSN: 10, Transactions: 20, Rows: 30,
	}
	tests := map[string]struct {
		current postgres.ReplicationProgress
		passed  bool
		err     bool
	}{
		"equal baseline": {
			current: baseline,
		},
		"row-neutral transaction advanced": {
			current: postgres.ReplicationProgress{RemoteLSN: 11, Transactions: 21, Rows: 30},
			passed:  true,
		},
		"only remote LSN advanced": {
			current: postgres.ReplicationProgress{RemoteLSN: 11, Transactions: 20, Rows: 30},
			passed:  true,
		},
		"remote LSN regressed": {
			current: postgres.ReplicationProgress{RemoteLSN: 9, Transactions: 21, Rows: 31},
			err:     true,
		},
		"transaction count regressed": {
			current: postgres.ReplicationProgress{RemoteLSN: 11, Transactions: 19, Rows: 31},
			err:     true,
		},
		"row count regressed": {
			current: postgres.ReplicationProgress{RemoteLSN: 11, Transactions: 21, Rows: 29},
			err:     true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			passed, err := targetProgressPassedFailure(baseline, test.current)
			if (err != nil) != test.err || passed != test.passed {
				t.Fatalf("passed=%t err=%v, want passed=%t err=%t", passed, err, test.passed, test.err)
			}
		})
	}
}

func TestStreamGenerationAndBinaryModeAreStable(t *testing.T) {
	first := streamGeneration("source", "filter")
	if first == "" || first != streamGeneration("source", "filter") {
		t.Fatalf("unstable stream generation %q", first)
	}
	if first == streamGeneration("source", "other") {
		t.Fatal("filter identity did not affect stream generation")
	}
	builtins := []copy.Table{{Columns: []copy.Column{{TypeOID: 23}, {TypeOID: 25}}}}
	if !cdcBinaryMode(builtins, 17, 17) {
		t.Fatal("same-major built-in columns did not select binary CDC")
	}
	if cdcBinaryMode(builtins, 16, 17) {
		t.Fatal("cross-major migration selected binary CDC")
	}
	custom := []copy.Table{{Columns: []copy.Column{{TypeOID: 16384}}}}
	if cdcBinaryMode(custom, 17, 17) {
		t.Fatal("custom type selected binary CDC")
	}
}

func TestFormatCDCRecoveryProgress(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		progress cdc.RecoveryProgress
		want     string
	}{
		{
			name: "measuring",
			progress: cdc.RecoveryProgress{
				FilesTotal: 4, BytesTotal: 4 << 30,
			},
			want: "CDC recovery: 0/4 files checked · 0 B/4.0 GiB scanned · 0.0 MiB/s · ETA measuring",
		},
		{
			name: "rate and ETA",
			progress: cdc.RecoveryProgress{
				FilesChecked: 2, FilesTotal: 4,
				BytesScanned: 2 << 30, BytesTotal: 4 << 30, Elapsed: 2 * time.Second,
			},
			want: "CDC recovery: 2/4 files checked · 2.0 GiB/4.0 GiB scanned · 1024.0 MiB/s · ETA 2s",
		},
		{
			name: "complete",
			progress: cdc.RecoveryProgress{
				FilesChecked: 4, FilesTotal: 4,
				BytesScanned: 4 << 30, BytesTotal: 4 << 30, Elapsed: 4 * time.Second,
			},
			want: "CDC recovery: 4/4 files checked · 4.0 GiB/4.0 GiB scanned · 1024.0 MiB/s · ETA 0s",
		},
		{
			name: "repaired tail is explicit",
			progress: cdc.RecoveryProgress{
				FilesChecked: 1, FilesTotal: 1,
				BytesScanned: 8, BytesTotal: 1024, BytesTruncated: 1016, Elapsed: time.Second,
			},
			want: "CDC recovery: 1/1 files checked · 8 B/1024 B scanned · 1016 B invalid tail repaired · 0.0 MiB/s · ETA 0s",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := formatCDCRecoveryProgress(test.progress); got != test.want {
				t.Fatalf("formatCDCRecoveryProgress() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestExactArchiveSelectionExcludesUnselectedTables also pins the restore order
// to pg_dump's own. Extensions were once hoisted to the front, which lifted
// CREATE EXTENSION ... WITH SCHEMA above the CREATE SCHEMA it requires.
func TestExactArchiveSelectionExcludesUnselectedTables(t *testing.T) {
	selection := schema.DumpSelection{
		Tables:     []schema.QualifiedName{{Schema: "app", Name: "included"}},
		Extensions: []string{"citext"},
	}
	entries := []schema.TOCEntry{
		{DumpID: 1, Description: "SCHEMA", Namespace: "-", Tag: "app"},
		{DumpID: 6, Description: "EXTENSION", Namespace: "-", Tag: "citext"},
		{DumpID: 2, Description: "TABLE", Namespace: "app", Tag: "included"},
		{DumpID: 3, Description: "TABLE", Namespace: "app", Tag: "excluded"},
		{DumpID: 4, Description: "INDEX", Namespace: "app", Tag: "included included_pkey"},
		{DumpID: 5, Description: "INDEX", Namespace: "app", Tag: "excluded excluded_pkey"},
	}
	filtered := exactArchiveEntries(entries, selection)
	var got []string
	for _, entry := range filtered {
		if strings.Contains(entry.Tag, "excluded") {
			t.Fatalf("excluded table object survived: %#v", entry)
		}
		got = append(got, entry.Tag)
	}
	want := []string{"app", "citext", "included", "included included_pkey"}
	if !slices.Equal(got, want) {
		t.Fatalf("selected entries = %v, want %v", got, want)
	}
}

// TestCommentSelectionAndLookupUseTheEntryNamespace covers the two ways a
// COMMENT entry's schema goes missing. pg_dump names the commented object in the
// tag without its schema and carries the schema in a separate column, so
// matching the bare name against schema-qualified selections dropped every
// table and column comment, and looking the object up unqualified resolved it
// through search_path instead of its own schema.
func TestCommentSelectionAndLookupUseTheEntryNamespace(t *testing.T) {
	selection := schema.DumpSelection{Tables: []schema.QualifiedName{{Schema: "app", Name: "orders"}}}
	for _, test := range []struct {
		entry    schema.TOCEntry
		selected bool
		arg      string
	}{
		{schema.TOCEntry{Description: "COMMENT", Namespace: "app", Tag: "TABLE orders"}, true, `"app".orders`},
		{schema.TOCEntry{Description: "COMMENT", Namespace: "app", Tag: "COLUMN orders.note"}, true, `"app".orders`},
		{schema.TOCEntry{Description: "COMMENT", Namespace: "other", Tag: "TABLE orders"}, false, `"other".orders`},
		{schema.TOCEntry{Description: "COMMENT", Namespace: "-", Tag: "SCHEMA app"}, true, "app"},
	} {
		if got := selectedTOCEntry(test.entry, selection); got != test.selected {
			t.Errorf("selectedTOCEntry(%s %s) = %v, want %v",
				test.entry.Namespace, test.entry.Tag, got, test.selected)
		}
		_, args, err := commentLookup(test.entry)
		if err != nil {
			t.Errorf("commentLookup(%s): %v", test.entry.Tag, err)
			continue
		}
		if args[0] != test.arg {
			t.Errorf("commentLookup(%s %s) looks up %q, want %q",
				test.entry.Namespace, test.entry.Tag, args[0], test.arg)
		}
	}
	if _, _, err := commentLookup(schema.TOCEntry{Description: "COMMENT", Tag: "DATABASE app"}); err == nil {
		t.Error("commentLookup accepted an object class it cannot resolve")
	}
	// A materialized view must not be truncated to its first word.
	_, args, err := commentLookup(schema.TOCEntry{
		Description: "COMMENT", Namespace: "app", Tag: "MATERIALIZED VIEW summary",
	})
	if err != nil || args[0] != `"app".summary` {
		t.Errorf("materialized view comment lookup = %v, %v", args, err)
	}
}

func TestSequencesRejectsNegativeOffset(t *testing.T) {
	err := App{}.Sequences(context.Background(), config.Config{SequenceOffset: -1})
	if err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Errorf("Sequences with a negative offset = %v, want a negative-offset error", err)
	}
}
