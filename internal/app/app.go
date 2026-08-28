// Package app composes the migration subsystems into command lifecycles.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/GetStream/pgmigrate/internal/cdc"
	"github.com/GetStream/pgmigrate/internal/config"
	pgcopy "github.com/GetStream/pgmigrate/internal/copy"
	"github.com/GetStream/pgmigrate/internal/cutover"
	"github.com/GetStream/pgmigrate/internal/indexbuild"
	"github.com/GetStream/pgmigrate/internal/observe"
	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/preflight"
	"github.com/GetStream/pgmigrate/internal/schema"
	"github.com/GetStream/pgmigrate/internal/setup"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/GetStream/pgmigrate/internal/verify"
)

var errComplete = errors.New("migration complete")

const cdcDivergenceFindingID = "cdc-divergence"

type App struct {
	Out io.Writer
	// Progress receives human-readable progress, separately from Out so a machine
	// consumer can read the result without a live counter interleaved into it.
	Progress io.Writer
}

func (a App) output() io.Writer {
	if a.Out != nil {
		return a.Out
	}
	return os.Stdout
}

func (a App) progressOutput() io.Writer {
	if a.Progress != nil {
		return a.Progress
	}
	return os.Stderr
}

func connector(dsn string) func(context.Context) (*pgx.Conn, error) {
	return func(ctx context.Context) (*pgx.Conn, error) { return postgres.Connect(ctx, dsn) }
}

func loadFilter(path string) (config.Filter, error) {
	if path == "" {
		return config.ParseFilter(strings.NewReader(""))
	}
	return config.LoadFilter(path)
}

func sourceFingerprint(ctx context.Context, dsn string) (string, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("parse source DSN: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return "", fmt.Errorf("connect source identity: %w", err)
	}
	defer conn.Close(context.Background())
	identity, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return "", fmt.Errorf("identify source: %w", err)
	}
	return setup.SourceFingerprint(identity.SystemID, identity.DBName), nil
}

func inventory(ctx context.Context, cfg config.Config, filter config.Filter) ([]pgcopy.Table, error) {
	conn, err := postgres.Connect(ctx, cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("connect source inventory: %w", err)
	}
	defer conn.Close(context.Background())
	return pgcopy.Inventory(ctx, conn, filter.Match)
}

func toPreflight(tables []pgcopy.Table) []preflight.Table {
	out := make([]preflight.Table, len(tables))
	for i, table := range tables {
		out[i] = preflight.Table{OID: table.OID, Schema: table.Schema, Name: table.Name}
	}
	return out
}

func toSetup(tables []pgcopy.Table) []setup.Table {
	out := make([]setup.Table, len(tables))
	for i, table := range tables {
		out[i] = setup.Table{Schema: table.Schema, Name: table.Name}
	}
	return out
}

func persistPreparation(ctx context.Context, store *state.Store, tables []pgcopy.Table, result preflight.Result) error {
	for _, table := range tables {
		if err := store.UpsertTable(ctx, state.Table{
			OID: table.OID, Schema: table.Schema, Name: table.Name,
			EstimatedRows: table.EstimatedRows, Bytes: table.Bytes,
		}); err != nil {
			return err
		}
	}
	for _, finding := range result.Findings {
		if err := store.UpsertFinding(ctx, state.Finding{
			ID: finding.ID, Kind: finding.Kind, Severity: string(finding.Severity), Message: finding.Message,
		}); err != nil {
			return err
		}
	}
	return nil
}

func printFindings(w io.Writer, result preflight.Result) {
	if len(result.Findings) == 0 {
		fmt.Fprintln(w, "preflight: ok")
		return
	}
	for _, finding := range result.Findings {
		// A finding whose message needs paragraphs, such as one showing two
		// collations side by side, is indented under its own heading so it reads as
		// belonging to that finding rather than as further findings.
		lines := strings.Split(finding.Message, "\n")
		fmt.Fprintf(w, "%s %s: %s\n", finding.Severity, finding.ID, lines[0])
		for _, line := range lines[1:] {
			if line == "" {
				fmt.Fprintln(w)
				continue
			}
			fmt.Fprintf(w, "    %s\n", line)
		}
	}
	if result.Incomplete {
		fmt.Fprintln(w, "\npreflight stopped here; the remaining checks did not run")
	}
}

func (a App) Preflight(ctx context.Context, cfg config.Config) error {
	filter, err := loadFilter(cfg.TableFilter)
	if err != nil {
		return err
	}
	fingerprint, err := sourceFingerprint(ctx, cfg.Source)
	if err != nil {
		return err
	}
	tables, err := inventory(ctx, cfg, filter)
	if err != nil {
		return err
	}
	store, err := state.Open(ctx, cfg.Dir, state.Fingerprints{Source: fingerprint, Filter: filter.Fingerprint()})
	if err != nil {
		return err
	}
	defer store.Close()
	preflightSelection, err := dumpSelection(ctx, cfg.Source, "", tables)
	if err != nil {
		return err
	}
	preflightConfig := preflight.Config{
		SourceDSN: cfg.Source, TargetDSN: cfg.Target, Tables: toPreflight(tables),
		RequiredExtensions:  preflightSelection.Extensions,
		AcknowledgeWarnings: cfg.AckWarnings, AllowCollationChange: cfg.AllowCollationChange,
		PGDumpPath: cfg.PGDumpPath, PGRestorePath: cfg.PGRestorePath,
		SequenceOffset: cfg.SequenceOffset, WALSampleDuration: cfg.WALSampleDuration,
	}
	if err := applyTuningPreflight(cfg, &preflightConfig); err != nil {
		return err
	}
	result, err := preflight.Run(ctx, preflightConfig)
	if err != nil {
		return err
	}
	if err := persistPreparation(ctx, store, tables, result); err != nil {
		return err
	}
	printFindings(a.output(), result)
	return preflight.Gate(result.Findings, nil, cfg.AckWarnings)
}

func migrationID(dir string) string {
	sum := setup.SourceFingerprint(filepath.Base(filepath.Clean(dir)), "migration")
	return sum[:16]
}

func endPosition(store *state.Store) func(context.Context) (cdc.LSN, bool, error) {
	return func(ctx context.Context) (cdc.LSN, bool, error) {
		migration, err := store.Migration(ctx)
		if err != nil {
			return 0, false, err
		}
		if migration.EndPosition == "" {
			return 0, false, nil
		}
		lsn, err := pglogrepl.ParseLSN(migration.EndPosition)
		return cdc.LSN(lsn), err == nil, err
	}
}

func streamGeneration(sourceFingerprint, filterFingerprint string) string {
	sum := sha256.Sum256([]byte("pgmigrate-cdc-v1\x00" + sourceFingerprint + "\x00" + filterFingerprint))
	return fmt.Sprintf("%x", sum[:])
}

func cdcBinaryMode(tables []pgcopy.Table, sourceMajor, targetMajor int) bool {
	if sourceMajor != targetMajor {
		return false
	}
	relations := make([]cdc.Relation, len(tables))
	for i, table := range tables {
		relations[i].OID = table.OID
		relations[i].Columns = make([]cdc.Column, len(table.Columns))
		for j, column := range table.Columns {
			relations[i].Columns[j] = cdc.Column{Name: column.Name, Type: column.TypeOID}
		}
	}
	return cdc.PGOutputBinarySafe(relations)
}

func cdcRecoveryProgress(output io.Writer) func(cdc.RecoveryProgress) {
	return func(progress cdc.RecoveryProgress) {
		_, _ = fmt.Fprintln(output, formatCDCRecoveryProgress(progress))
	}
}

func formatCDCRecoveryProgress(progress cdc.RecoveryProgress) string {
	rate := float64(0)
	if progress.Elapsed > 0 {
		rate = float64(progress.BytesScanned) / progress.Elapsed.Seconds()
	}
	eta := "measuring"
	remaining := progress.BytesTotal - progress.BytesScanned
	if progress.FilesChecked == progress.FilesTotal {
		eta = "0s"
	} else if rate > 0 && remaining > 0 {
		etaDuration := time.Duration(float64(remaining) / rate * float64(time.Second))
		eta = etaDuration.Round(time.Second).String()
	}
	repair := ""
	if progress.BytesTruncated > 0 {
		repair = fmt.Sprintf(" · %s invalid tail repaired", formatCDCRecoveryBytes(progress.BytesTruncated))
	}
	return fmt.Sprintf(
		"CDC recovery: %d/%d files checked · %s/%s scanned%s · %.1f MiB/s · ETA %s",
		progress.FilesChecked,
		progress.FilesTotal,
		formatCDCRecoveryBytes(progress.BytesScanned),
		formatCDCRecoveryBytes(progress.BytesTotal),
		repair,
		rate/float64(1<<20),
		eta,
	)
}

func formatCDCRecoveryBytes(bytes int64) string {
	if bytes < 1<<20 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes >= 1<<30 {
		return fmt.Sprintf("%.1f GiB", float64(bytes)/float64(1<<30))
	}
	return fmt.Sprintf("%.1f MiB", float64(bytes)/float64(1<<20))
}

func persistCDCBinaryMode(ctx context.Context, store *state.Store, binary bool) error {
	return store.CompleteStep(ctx, "cdc.binary", strconv.FormatBool(binary))
}

func loadCDCBinaryMode(ctx context.Context, store *state.Store) (bool, error) {
	steps, err := store.ListSteps(ctx)
	if err != nil {
		return false, err
	}
	for _, step := range steps {
		if step.Name == "cdc.binary" && step.Completed {
			binary, err := strconv.ParseBool(step.Detail)
			if err != nil {
				return false, fmt.Errorf("decode durable CDC binary mode: %w", err)
			}
			return binary, nil
		}
	}
	return false, errors.New("durable CDC binary mode is missing")
}

func initializeTargetProgress(ctx context.Context, targetDSN, streamID, generation string) error {
	conn, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	identity := cdc.StreamIdentityConfig{
		StreamID: streamID, Generation: generation, FreshSetup: true,
	}
	if err := cdc.EnsureStreamProgressIdentity(ctx, conn, identity); err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err := postgres.UpdateProgress(ctx, tx, streamID, 0); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return cdc.EnsureStreamProgressIdentity(ctx, conn, identity)
}

func validateTargetProgress(ctx context.Context, targetDSN, streamID, generation string) error {
	conn, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	return cdc.EnsureStreamProgressIdentity(ctx, conn, cdc.StreamIdentityConfig{
		StreamID: streamID, Generation: generation, TargetHasCopiedData: true,
	})
}

func setManualEndPosition(
	ctx context.Context,
	value, directory string,
	durable cdc.LSN,
	store *state.Store,
) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	requested, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return fmt.Errorf("parse manual end position: %w", err)
	}
	resolution, err := cdc.NormalizeEndPosition(directory, cdc.LSN(requested), durable)
	if err != nil {
		return err
	}
	if !resolution.Exact {
		return fmt.Errorf(
			"manual end position %s is not an exact durable transaction/KEEP boundary; nearest prior boundary is %s",
			value, pglogrepl.LSN(resolution.Boundary).String(),
		)
	}
	return store.SetEndPosition(ctx, pglogrepl.LSN(resolution.Boundary).String())
}

func finalizeTargetCleanup(ctx context.Context, targetDSN string, store *state.Store) error {
	requested, err := store.StepCompleted(ctx, "target.cleanup.requested")
	if err != nil || !requested {
		return err
	}
	done, err := store.StepCompleted(ctx, "target.cleanup.completed")
	if err != nil || done {
		return err
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	if err := validateTargetOnly(ctx, targetDSN, migration); err != nil {
		return err
	}
	target, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	_, cleanupErr := target.Exec(ctx, "DROP SCHEMA IF EXISTS pgmigrate_internal CASCADE")
	target.Close(context.Background())
	if cleanupErr != nil {
		return fmt.Errorf("finalize target metadata cleanup: %w", cleanupErr)
	}
	return store.CompleteStep(ctx, "target.cleanup.completed", "run stopped")
}

func (a App) Run(ctx context.Context, cfg config.Config) (runErr error) {
	filter, err := loadFilter(cfg.TableFilter)
	if err != nil {
		return err
	}
	fingerprint, err := sourceFingerprint(ctx, cfg.Source)
	if err != nil {
		return err
	}
	tables, err := inventory(ctx, cfg, filter)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return errors.New("table filter selected no tables")
	}
	_, stateErr := os.Stat(filepath.Join(cfg.Dir, "state.db"))
	hadState := stateErr == nil
	store, err := state.Open(ctx, cfg.Dir, state.Fingerprints{Source: fingerprint, Filter: filter.Fingerprint()})
	if err != nil {
		return err
	}
	defer store.Close()
	defer func() { recordFailedAttempt(ctx, a.output(), cfg.Dir, store, runErr) }()
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	if migration.Phase == state.PhaseComplete {
		return finalizeTargetCleanup(ctx, cfg.Target, store)
	}
	if hadState && (migration.Phase == state.PhaseIndexes || migration.Phase == state.PhaseCatchup ||
		migration.Phase == state.PhaseFollow || migration.Phase == state.PhaseDrained ||
		migration.Phase == state.PhaseCutover) {
		return a.resumePostCopy(ctx, cfg, store, migration, tables)
	}
	if migration.Phase == state.PhaseSetup || migration.Phase == state.PhaseSchema || migration.Phase == state.PhaseCopy {
		if err := guardRepeatedBaseCopyFailure(ctx, cfg, store, migration); err != nil {
			return err
		}
		if err := resetInterruptedBaseCopy(ctx, cfg, store, tables, migration); err != nil {
			return fmt.Errorf("restart base copy from a fresh snapshot: %w", err)
		}
		tables, err = inventory(ctx, cfg, filter)
		if err != nil {
			return err
		}
	}
	preflightConfig := preflight.Config{
		SourceDSN: cfg.Source, TargetDSN: cfg.Target, Tables: toPreflight(tables),
		AcknowledgeWarnings: cfg.AckWarnings, AllowCollationChange: cfg.AllowCollationChange,
		PGDumpPath: cfg.PGDumpPath, PGRestorePath: cfg.PGRestorePath,
		SequenceOffset: cfg.SequenceOffset, WALSampleDuration: cfg.WALSampleDuration,
	}
	if err := applyTuningPreflight(cfg, &preflightConfig); err != nil {
		return err
	}
	result, err := preflight.Run(ctx, preflightConfig)
	if err != nil {
		return err
	}
	if err := persistPreparation(ctx, store, tables, result); err != nil {
		return err
	}
	binaryMode := cdcBinaryMode(tables, result.SourceMajor, result.TargetMajor)
	if err := persistCDCBinaryMode(ctx, store, binaryMode); err != nil {
		return err
	}
	if err := preflight.Gate(result.Findings, nil, cfg.AckWarnings); err != nil {
		printFindings(a.output(), result)
		return err
	}
	if err := transition(ctx, cfg, store, state.PhaseSetup); err != nil {
		return err
	}
	// The fallback runs here, past the gate that carries the operator's consent
	// and before setup creates the publication. Publishing a relation that cannot
	// identify its rows makes every production UPDATE and DELETE on it fail, so
	// the order is not a preference.
	if err := a.applyReplicaIdentityFallback(ctx, cfg, store, tables); err != nil {
		return err
	}
	holder, err := setup.Run(ctx, setup.Config{
		SourceDSN: cfg.Source, TargetDSN: cfg.Target, Dir: cfg.Dir,
		MigrationID: migrationID(cfg.Dir), Tables: toSetup(tables),
	}, store)
	if err != nil {
		return err
	}
	defer holder.Close(context.Background())
	generation := streamGeneration(fingerprint, filter.Fingerprint())
	if err := recordTargetIdentity(ctx, cfg.Target, fingerprint, filter.Fingerprint(), holder.Snapshot.Slot, generation); err != nil {
		return err
	}
	if err := initializeTargetProgress(ctx, cfg.Target, holder.Snapshot.Slot, generation); err != nil {
		return err
	}
	// Tuning belongs here: after the target exists and before anything bulk is
	// written to it, and inside the setup phase so it needs no phase of its own.
	sessionGUCs, err := tuneTarget(ctx, cfg, store)
	if err != nil {
		return err
	}

	cdcDir := filepath.Join(cfg.Dir, "cdc")
	writer, recovery, err := cdc.OpenWriter(cdc.WriterConfig{
		Directory:        cdcDir,
		RecoveryProgress: cdcRecoveryProgress(a.progressOutput()),
	})
	if err != nil {
		return err
	}
	defer writer.Close()
	durable := &cdc.DurableWatermark{}
	durable.Publish(recovery.DurableLSN)
	if err := setManualEndPosition(ctx, cfg.EndPosition, cdcDir, durable.Load(), store); err != nil {
		return err
	}
	transactions := make(chan cdc.Transaction, max(2, cfg.Workers*2))
	persister, err := cdc.NewPersister(cdc.PersisterConfig{Writer: writer, Transactions: transactions, Durable: durable})
	if err != nil {
		return err
	}
	start, err := pglogrepl.ParseLSN(holder.Snapshot.ConsistentPoint)
	if err != nil {
		return fmt.Errorf("parse consistent point: %w", err)
	}
	receiver, err := cdc.NewReceiver(cdc.ReceiverConfig{
		ConnString: cfg.Source, Slot: holder.Snapshot.Slot, Publication: holder.Snapshot.Publication,
		StartLSN: cdc.LSN(start), Transactions: transactions, Durable: durable,
		SpillDirectory: filepath.Join(cdcDir, "spill"), EndPosition: endPosition(store), Binary: &binaryMode,
	})
	if err != nil {
		return err
	}

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return runReceiverContinuous(groupCtx, receiver, store) })
	group.Go(func() error { return persister.Run(groupCtx) })
	group.Go(func() error {
		return monitorProgress(groupCtx, store, cfg.Target, holder.Snapshot.Slot, durable, cfg.Dir, nil)
	})
	group.Go(func() error { return followChecks(groupCtx, cfg, store, holder.Snapshot.Slot) })
	watchCtx, stopSnapshotWatch := context.WithCancel(groupCtx)
	defer stopSnapshotWatch()
	group.Go(func() error {
		err := <-holder.Watchdog(watchCtx, time.Second)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("source snapshot holder lost; base copy must restart from a fresh snapshot: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		if err := transition(groupCtx, cfg, store, state.PhaseSchema); err != nil {
			return err
		}
		archive := filepath.Join(cfg.Dir, "dump", "schema.dump")
		service := schemaService(cfg, store)
		snapshotTables, err := pgcopy.InventorySnapshot(groupCtx, connector(cfg.Source), holder.Snapshot.Name, filter.Match)
		if err != nil {
			return err
		}
		selection, err := dumpSelection(groupCtx, cfg.Source, holder.Snapshot.Name, snapshotTables)
		if err != nil {
			return err
		}
		selectionJSON, err := json.Marshal(selection)
		if err != nil {
			return err
		}
		if err := store.CompleteStep(groupCtx, "schema.selection", string(selectionJSON)); err != nil {
			return err
		}
		if err := service.Dump(groupCtx, cfg.Source, holder.Snapshot.Name, archive); err != nil {
			return fmt.Errorf("schema phase: dump source archive %s: %w", archive, err)
		}
		entries, err := service.List(groupCtx, archive)
		if err != nil {
			return fmt.Errorf("schema phase: read table of contents of %s: %w", archive, err)
		}
		entries = exactArchiveEntries(entries, selection)
		restoreArgs := []string{}
		if cfg.RestoreJobs > 1 {
			restoreArgs = append(restoreArgs, "--jobs", strconv.Itoa(cfg.RestoreJobs))
		}
		if err := service.RestorePreData(groupCtx, cfg.Target, archive, filepath.Join(cfg.Dir, "dump", "predata.list"), entries, restoreArgs...); err != nil {
			return fmt.Errorf("schema phase: restore pre-data from %s: %w", archive, err)
		}

		if err := transition(groupCtx, cfg, store, state.PhaseCopy); err != nil {
			return err
		}
		if err := pauseForCrashTest(groupCtx, state.PhaseCopy); err != nil {
			return err
		}
		var parts []pgcopy.Part
		for _, table := range snapshotTables {
			format := pgcopy.ConservativeFormat(table, result.SourceMajor, result.TargetMajor)
			parts = append(parts, pgcopy.Plan(table, cfg.SplitThreshold, cfg.Workers, format)...)
		}
		runner := pgcopy.Runner{
			Source: connector(cfg.Source), Target: connector(cfg.Target), Snapshot: holder.Snapshot.Name,
			Workers: cfg.Workers, State: store, TargetSessionGUCs: sessionGUCs,
		}
		if err := runner.Run(groupCtx, parts); err != nil {
			return err
		}
		stopSnapshotWatch()
		if err := holder.Close(context.Background()); err != nil {
			return err
		}

		if err := transition(groupCtx, cfg, store, state.PhaseIndexes); err != nil {
			return err
		}
		// Past this point a restart resumes instead of discarding the base copy,
		// so an earlier failure no longer says anything about the next attempt.
		if err := store.ClearFailedAttempt(groupCtx); err != nil {
			return err
		}
		if err := pauseForCrashTest(groupCtx, state.PhaseIndexes); err != nil {
			return err
		}
		source, err := postgres.Connect(groupCtx, cfg.Source)
		if err != nil {
			return err
		}
		selected := make(map[uint32]bool, len(snapshotTables))
		for _, table := range snapshotTables {
			selected[table.OID] = true
		}
		indexes, constraints, err := indexbuild.Inventory(groupCtx, source, func(oid uint32) bool { return selected[oid] })
		source.Close(context.Background())
		if err != nil {
			return err
		}
		indexRunner := indexbuild.Runner{
			Target: connector(cfg.Target), Workers: cfg.Workers, State: store,
			SessionGUCs: sessionGUCs,
			Log:         func(event string, values map[string]any) { logEvent(cfg.Dir, event, values) },
			AfterManaged: func(ctx context.Context) error {
				return restoreDeferredOnce(ctx, cfg, store, service, archive, entries, restoreArgs)
			},
		}
		if err := indexRunner.Run(groupCtx, indexes, constraints); err != nil {
			return err
		}
		if err := vacuumTarget(groupCtx, cfg, store, sessionGUCs); err != nil {
			return err
		}
		if err := transition(groupCtx, cfg, store, state.PhaseCatchup); err != nil {
			return err
		}
		if err := pauseForCrashTest(groupCtx, state.PhaseCatchup); err != nil {
			return err
		}
		return runApplierToFollow(
			groupCtx, cfg, store, holder.Snapshot, durable, writer.SegmentCatalog(), state.PhaseCatchup,
		)
	})
	if cfg.Metrics != "" {
		group.Go(func() error { return serveMetrics(groupCtx, cfg.Metrics, store) })
	}
	group.Go(func() error {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			migration, err := store.Migration(groupCtx)
			if err != nil {
				return err
			}
			if migration.Phase == state.PhaseComplete {
				return errComplete
			}
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case <-ticker.C:
			}
		}
	})
	err = group.Wait()
	if errors.Is(err, errComplete) {
		return finalizeTargetCleanup(context.Background(), cfg.Target, store)
	}
	return err
}

// RestartBaseCopy is the explicit lossless recovery path for a post-copy run
// whose source logical slot has disappeared. It refuses to reset a reusable
// stream: creating a new slot under an old snapshot would silently skip WAL,
// while resetting a healthy stream would discard valid durable work.
func (a App) RestartBaseCopy(ctx context.Context, cfg config.Config) error {
	if err := prepareFreshSnapshotRestart(ctx, cfg); err != nil {
		return err
	}
	fmt.Fprintln(a.progressOutput(), "fresh-snapshot restart: old pgmigrate-owned stream and target base state removed; starting a new lossless snapshot")
	return a.Run(ctx, cfg)
}

func prepareFreshSnapshotRestart(ctx context.Context, cfg config.Config) error {
	filter, err := loadFilter(cfg.TableFilter)
	if err != nil {
		return err
	}
	fingerprint, err := sourceFingerprint(ctx, cfg.Source)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, "state.db")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("fresh-snapshot restart requires an existing migration")
		}
		return fmt.Errorf("inspect durable migration state: %w", err)
	}
	store, err := state.Open(ctx, cfg.Dir, state.Fingerprints{Source: fingerprint, Filter: filter.Fingerprint()})
	if err != nil {
		return err
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		store.Close()
		return err
	}
	switch migration.Phase {
	case state.PhaseIndexes, state.PhaseCatchup, state.PhaseFollow:
	default:
		store.Close()
		return fmt.Errorf("fresh-snapshot restart is unavailable in %s phase", migration.Phase)
	}
	recordedTables, err := store.ListTables(ctx)
	if err != nil {
		store.Close()
		return err
	}
	if len(recordedTables) == 0 {
		store.Close()
		return errors.New("fresh-snapshot restart requires a durable table inventory")
	}
	tables := make([]pgcopy.Table, len(recordedTables))
	for index, table := range recordedTables {
		tables[index] = pgcopy.Table{
			OID: table.OID, Schema: table.Schema, Name: table.Name,
			EstimatedRows: table.EstimatedRows, Bytes: table.Bytes,
		}
	}
	snapshot, err := readSnapshot(cfg.Dir)
	if errors.Is(err, os.ErrNotExist) {
		publication, _ := setup.Names(migration.SourceFingerprint, migrationID(cfg.Dir))
		snapshot = setup.Snapshot{
			SourceFingerprint: migration.SourceFingerprint,
			Publication:       publication,
			Slot:              migration.SlotName,
			Name:              migration.SnapshotName,
			ConsistentPoint:   migration.ConsistentPoint,
		}
	} else if err != nil {
		store.Close()
		return fmt.Errorf("read durable snapshot before restart: %w", err)
	}
	resumeErr := setup.ValidateResume(ctx, setup.Config{
		SourceDSN: cfg.Source,
		Tables:    toSetup(tables),
	}, snapshot)
	if resumeErr == nil {
		store.Close()
		return errors.New("source replication slot is reusable; resume the migration instead of restarting the base copy")
	}
	if !errors.Is(resumeErr, setup.ErrResumeSlotMissing) &&
		!errors.Is(resumeErr, setup.ErrResumePublicationMissing) {
		store.Close()
		return fmt.Errorf("refuse fresh-snapshot restart without a proven missing source CDC object: %w", resumeErr)
	}
	if err := resetInterruptedBaseCopy(ctx, cfg, store, tables, migration); err != nil {
		store.Close()
		return fmt.Errorf("restart base copy from a fresh snapshot: %w", err)
	}
	if err := store.Close(); err != nil {
		return fmt.Errorf("close reset migration state: %w", err)
	}
	return nil
}

func (a App) resumePostCopy(
	ctx context.Context,
	cfg config.Config,
	store *state.Store,
	migration state.Migration,
	tables []pgcopy.Table,
) error {
	if err := validateTargetIdentity(ctx, cfg, store); err != nil {
		return err
	}
	generation := streamGeneration(migration.SourceFingerprint, migration.FilterFingerprint)
	if err := validateTargetProgress(ctx, cfg.Target, migration.SlotName, generation); err != nil {
		return err
	}
	failureBaseline, err := captureFailedAttemptProgress(
		ctx, store, cfg.Target, migration.SlotName,
	)
	if err != nil {
		return err
	}
	binaryMode, err := loadCDCBinaryMode(ctx, store)
	if err != nil {
		return err
	}
	snapshot, err := readSnapshot(cfg.Dir)
	if err != nil {
		return fmt.Errorf("read post-copy snapshot metadata: %w", err)
	}
	if snapshot.Slot != migration.SlotName || snapshot.ConsistentPoint != migration.ConsistentPoint {
		return errors.New("snapshot metadata does not match durable migration state")
	}
	if err := setup.ValidateResume(ctx, setup.Config{
		SourceDSN: cfg.Source,
		Tables:    toSetup(tables),
	}, snapshot); err != nil {
		return fmt.Errorf("validate source CDC stream before local recovery: %w", err)
	}
	if err := resolveSupersededIndexFinding(ctx, store, migration); err != nil {
		return err
	}
	cdcDir := filepath.Join(cfg.Dir, "cdc")
	writer, recovery, err := cdc.OpenWriter(cdc.WriterConfig{
		Directory:        cdcDir,
		RecoveryProgress: cdcRecoveryProgress(a.progressOutput()),
	})
	if err != nil {
		return err
	}
	defer writer.Close()
	durable := &cdc.DurableWatermark{}
	durable.Publish(recovery.DurableLSN)
	if err := setManualEndPosition(ctx, cfg.EndPosition, cdcDir, durable.Load(), store); err != nil {
		return err
	}
	transactions := make(chan cdc.Transaction, max(2, cfg.Workers*2))
	persister, err := cdc.NewPersister(cdc.PersisterConfig{
		Writer: writer, Transactions: transactions, Durable: durable,
	})
	if err != nil {
		return err
	}
	start, err := pglogrepl.ParseLSN(snapshot.ConsistentPoint)
	if err != nil {
		return err
	}
	receiver, err := cdc.NewReceiver(cdc.ReceiverConfig{
		ConnString: cfg.Source, Slot: snapshot.Slot, Publication: snapshot.Publication,
		StartLSN: cdc.LSN(start), Transactions: transactions, Durable: durable,
		SpillDirectory: filepath.Join(cdcDir, "spill"), EndPosition: endPosition(store), Binary: &binaryMode,
	})
	if err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return runReceiverContinuous(groupCtx, receiver, store) })
	group.Go(func() error { return persister.Run(groupCtx) })
	group.Go(func() error {
		return monitorProgress(
			groupCtx, store, cfg.Target, snapshot.Slot, durable, cfg.Dir, failureBaseline,
		)
	})
	group.Go(func() error { return followChecks(groupCtx, cfg, store, snapshot.Slot) })
	group.Go(func() error {
		phase := migration.Phase
		if phase == state.PhaseIndexes {
			if err := pauseForCrashTest(groupCtx, state.PhaseIndexes); err != nil {
				return err
			}
			if err := resumeIndexes(groupCtx, cfg, store); err != nil {
				return err
			}
			if err := transition(groupCtx, cfg, store, state.PhaseCatchup); err != nil {
				return err
			}
			phase = state.PhaseCatchup
		}
		if phase == state.PhaseCatchup {
			if err := pauseForCrashTest(groupCtx, state.PhaseCatchup); err != nil {
				return err
			}
		}
		return runApplierToFollow(
			groupCtx, cfg, store, snapshot, durable, writer.SegmentCatalog(), phase,
		)
	})
	group.Go(func() error {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			current, err := store.Migration(groupCtx)
			if err != nil {
				return err
			}
			if current.Phase == state.PhaseComplete {
				return errComplete
			}
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			case <-ticker.C:
			}
		}
	})
	if cfg.Metrics != "" {
		group.Go(func() error { return serveMetrics(groupCtx, cfg.Metrics, store) })
	}
	logEvent(cfg.Dir, "resume", map[string]any{"phase": migration.Phase})
	err = group.Wait()
	if errors.Is(err, errComplete) {
		return finalizeTargetCleanup(context.Background(), cfg.Target, store)
	}
	logEvent(cfg.Dir, "error", map[string]any{"error": fmt.Sprint(err)})
	return err
}

// A divergence cannot originate in indexes because replay has not started.
// After a proven fresh-snapshot reset, older binaries cleared the failed
// attempt on entering indexes but could leave its divergence finding open.
// Retire that orphan only at this pre-replay boundary and only when no current
// failure exists; catchup/follow findings still require durable replay progress.
func resolveSupersededIndexFinding(
	ctx context.Context,
	store *state.Store,
	migration state.Migration,
) error {
	if migration.Phase != state.PhaseIndexes {
		return nil
	}
	attempt, err := store.FailedAttempt(ctx)
	if err != nil {
		return err
	}
	if attempt.Consecutive != 0 {
		return nil
	}
	return store.ResolveFinding(ctx, cdcDivergenceFindingID)
}

func resumeIndexes(ctx context.Context, cfg config.Config, store *state.Store) error {
	archive := filepath.Join(cfg.Dir, "dump", "schema.dump")
	service := schemaService(cfg, store)
	entries, err := service.List(ctx, archive)
	if err != nil {
		return err
	}
	tables, err := store.ListTables(ctx)
	if err != nil {
		return err
	}
	selection, err := loadSchemaSelection(ctx, store)
	if err != nil {
		return err
	}
	entries = exactArchiveEntries(entries, selection)
	selected := make(map[uint32]bool, len(tables))
	for _, table := range tables {
		selected[table.OID] = true
	}
	source, err := postgres.Connect(ctx, cfg.Source)
	if err != nil {
		return err
	}
	indexes, constraints, err := indexbuild.Inventory(ctx, source, func(oid uint32) bool { return selected[oid] })
	source.Close(context.Background())
	if err != nil {
		return err
	}
	restoreArgs := []string{}
	if cfg.RestoreJobs > 1 {
		restoreArgs = append(restoreArgs, "--jobs", strconv.Itoa(cfg.RestoreJobs))
	}
	// A resumed index build needs the same session tuning as a fresh one. Tuning
	// is idempotent, so re-deriving here plans nothing that is already applied
	// and never overwrites a recorded original.
	sessionGUCs, err := tuneTarget(ctx, cfg, store)
	if err != nil {
		return err
	}
	runner := indexbuild.Runner{
		Target: connector(cfg.Target), Workers: cfg.Workers, State: store,
		SessionGUCs: sessionGUCs,
		Log:         func(event string, values map[string]any) { logEvent(cfg.Dir, event, values) },
		AfterManaged: func(ctx context.Context) error {
			return restoreDeferredOnce(ctx, cfg, store, service, archive, entries, restoreArgs)
		},
	}
	if err := runner.Run(ctx, indexes, constraints); err != nil {
		return err
	}
	return vacuumTarget(ctx, cfg, store, sessionGUCs)
}

func restoreDeferredOnce(
	ctx context.Context,
	cfg config.Config,
	store *state.Store,
	service schema.Service,
	archive string,
	entries []schema.TOCEntry,
	extraArgs []string,
) error {
	return service.RestoreDeferred(ctx, cfg.Target, archive,
		filepath.Join(cfg.Dir, "dump", "postdata.list"), entries, extraArgs...)
}

func schemaService(cfg config.Config, store *state.Store) schema.Service {
	return schema.Service{
		Tools:           schema.Tools{Dump: cfg.PGDumpPath, Restore: cfg.PGRestorePath},
		DeferredMarkers: store,
		InspectDeferred: inspectDeferred(cfg.Dir, cfg.Source),
	}
}

func exactArchiveEntries(entries []schema.TOCEntry, selection schema.DumpSelection) []schema.TOCEntry {
	dumpIDs := make(map[int64]bool)
	for _, entry := range entries {
		if selectedTOCEntry(entry, selection) {
			dumpIDs[entry.DumpID] = true
		}
	}
	return schema.FilterTOC(entries, nil, dumpIDs)
}

func loadSchemaSelection(ctx context.Context, store *state.Store) (schema.DumpSelection, error) {
	steps, err := store.ListSteps(ctx)
	if err != nil {
		return schema.DumpSelection{}, err
	}
	for _, step := range steps {
		if step.Name == "schema.selection" && step.Completed {
			var selection schema.DumpSelection
			if err := json.Unmarshal([]byte(step.Detail), &selection); err != nil {
				return selection, fmt.Errorf("decode durable schema selection: %w", err)
			}
			return selection, nil
		}
	}
	return schema.DumpSelection{}, errors.New("durable schema selection is missing")
}

func selectedTOCEntry(entry schema.TOCEntry, selection schema.DumpSelection) bool {
	objects := make(map[schema.CatalogObject]bool, len(selection.Objects))
	for _, object := range selection.Objects {
		objects[object] = true
	}
	if entry.ObjectOID != 0 && objects[schema.CatalogObject{
		CatalogOID: entry.CatalogOID, ObjectOID: entry.ObjectOID,
	}] {
		return true
	}
	tables := map[string]bool{}
	schemas := map[string]bool{}
	sequences := map[string]bool{}
	extensions := map[string]bool{}
	for _, table := range selection.Tables {
		tables[table.Schema+"."+table.Name] = true
		schemas[table.Schema] = true
	}
	// A partition's own TABLE, TABLE ATTACH, INDEX and CONSTRAINT entries are
	// named after the partition, so they are only retained by matching it.
	for _, partition := range selection.Partitions {
		tables[partition.Schema+"."+partition.Name] = true
		schemas[partition.Schema] = true
	}
	for _, relation := range selection.DependentRelations {
		sequences[relation.Schema+"."+relation.Name] = true
		schemas[relation.Schema] = true
	}
	for _, extension := range selection.Extensions {
		extensions[extension] = true
	}
	qualified := func(name string) string { return entry.Namespace + "." + name }
	first := func(value string) string {
		if field, _, ok := strings.Cut(value, " "); ok {
			return field
		}
		return value
	}
	switch entry.Description {
	case "PUBLICATION", "PUBLICATION TABLE", "TABLE DATA", "MATERIALIZED VIEW DATA", "SEQUENCE SET":
		return false
	case "SCHEMA":
		return schemas[entry.Tag]
	case "EXTENSION":
		return extensions[entry.Tag]
	case "TABLE":
		return tables[qualified(entry.Tag)]
	case "SEQUENCE":
		return sequences[qualified(entry.Tag)]
	case "SEQUENCE OWNED BY":
		return sequences[qualified(first(entry.Tag))]
	case "INDEX ATTACH":
		// The tag names the partition's index, not its table, so this entry can
		// never be matched by relation name. indexbuild attaches partition
		// indexes from the source catalog instead, which is also what keeps the
		// partitioned parent's index from staying invalid.
		return false
	case "INDEX", "CONSTRAINT", "CHECK CONSTRAINT", "FK CONSTRAINT",
		"TRIGGER", "RULE", "POLICY", "DEFAULT", "TABLE ATTACH":
		return tables[qualified(first(entry.Tag))]
	case "VIEW", "MATERIALIZED VIEW":
		return tables[qualified(entry.Tag)]
	case "COMMENT", "ACL":
		upper := strings.ToUpper(entry.Tag)
		for _, prefix := range []string{"TABLE ", "COLUMN "} {
			if strings.HasPrefix(upper, prefix) {
				identity := strings.TrimSpace(entry.Tag[len(prefix):])
				if prefix == "COLUMN " {
					if dot := strings.LastIndex(identity, "."); dot > 0 {
						identity = identity[:dot]
					}
				}
				// The tag names the relation without its schema; the entry's
				// namespace column carries it. Matching the bare name against
				// schema-qualified keys silently dropped every table and column
				// comment from the restore list.
				return tables[qualified(strings.Trim(identity, `"`))]
			}
		}
		return entry.Namespace == "-" || schemas[entry.Namespace]
	default:
		// Types, functions, domains, operators and casts in a selected schema
		// are schema dependencies that pg_dump cannot select individually.
		return schemas[entry.Namespace] || entry.Namespace == "-" && entry.Description == "DEFAULT ACL"
	}
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func dumpSelection(ctx context.Context, sourceDSN, snapshot string, tables []pgcopy.Table) (schema.DumpSelection, error) {
	selection := schema.DumpSelection{Tables: make([]schema.QualifiedName, len(tables))}
	oids := make([]uint32, len(tables))
	for i, table := range tables {
		selection.Tables[i] = schema.QualifiedName{Schema: table.Schema, Name: table.Name}
		oids[i] = table.OID
	}
	conn, err := postgres.Connect(ctx, sourceDSN)
	if err != nil {
		return selection, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return selection, err
	}
	defer tx.Rollback(ctx)
	if snapshot != "" {
		// Snapshot import must be the transaction's first statement.
		if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT "+quoteLiteral(snapshot)); err != nil {
			return selection, fmt.Errorf("import schema selection snapshot: %w", err)
		}
	}
	// Partition descendants of the selected tables, at any depth. The copy
	// inventory represents a partitioned table by its root, so the children never
	// reach selection on their own, and an archive filtered without them leaves
	// the target holding a partitioned table with no partitions that rejects
	// every insert.
	rows, err := tx.Query(ctx, `
		WITH RECURSIVE descendants(oid) AS (
			SELECT unnest($1::oid[])
			UNION
			SELECT c.oid
			FROM pg_catalog.pg_inherits h
			JOIN descendants d ON d.oid=h.inhparent
			JOIN pg_catalog.pg_class c ON c.oid=h.inhrelid AND c.relispartition
		)
		SELECT n.nspname, c.relname
		FROM descendants d
		JOIN pg_catalog.pg_class c ON c.oid=d.oid
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE c.relispartition
		ORDER BY n.nspname, c.relname`, oids)
	if err != nil {
		return selection, fmt.Errorf("inventory selected partitions: %w", err)
	}
	for rows.Next() {
		var relation schema.QualifiedName
		if err := rows.Scan(&relation.Schema, &relation.Name); err != nil {
			rows.Close()
			return selection, err
		}
		selection.Partitions = append(selection.Partitions, relation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return selection, err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `
		SELECT DISTINCT n.nspname, s.relname
		FROM pg_catalog.pg_class s
		JOIN pg_catalog.pg_namespace n ON n.oid=s.relnamespace
		WHERE s.relkind='S' AND (
			EXISTS (
				SELECT 1 FROM pg_catalog.pg_depend d
				WHERE d.classid='pg_catalog.pg_class'::regclass AND d.objid=s.oid
				  AND d.refclassid='pg_catalog.pg_class'::regclass AND d.refobjid=ANY($1::oid[])
			) OR EXISTS (
				SELECT 1 FROM pg_catalog.pg_attrdef ad
				JOIN pg_catalog.pg_depend d
				  ON d.classid='pg_catalog.pg_attrdef'::regclass AND d.objid=ad.oid
				WHERE ad.adrelid=ANY($1::oid[])
				  AND d.refclassid='pg_catalog.pg_class'::regclass AND d.refobjid=s.oid
			)
		)
		ORDER BY n.nspname,s.relname`, oids)
	if err != nil {
		return selection, fmt.Errorf("inventory selected sequence dependencies: %w", err)
	}
	for rows.Next() {
		var relation schema.QualifiedName
		if err := rows.Scan(&relation.Schema, &relation.Name); err != nil {
			rows.Close()
			return selection, err
		}
		selection.DependentRelations = append(selection.DependentRelations, relation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return selection, err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `
		WITH RECURSIVE used_types(oid) AS (
			SELECT DISTINCT a.atttypid
			FROM pg_catalog.pg_attribute a
			WHERE a.attrelid=ANY($1::oid[]) AND a.attnum>0 AND NOT a.attisdropped
			UNION
			SELECT dependency.oid
			FROM pg_catalog.pg_type t
			JOIN used_types u ON u.oid=t.oid
			CROSS JOIN LATERAL (VALUES (t.typbasetype),(t.typelem)) dependency(oid)
			WHERE dependency.oid<>0
		)
		SELECT DISTINCT e.extname
		FROM used_types u
		JOIN pg_catalog.pg_depend d
		  ON d.classid='pg_catalog.pg_type'::regclass AND d.objid=u.oid
		 AND d.refclassid='pg_catalog.pg_extension'::regclass
		JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid
		ORDER BY e.extname`, oids)
	if err != nil {
		return selection, fmt.Errorf("inventory selected extension dependencies: %w", err)
	}
	for rows.Next() {
		var extension string
		if err := rows.Scan(&extension); err != nil {
			rows.Close()
			return selection, err
		}
		selection.Extensions = append(selection.Extensions, extension)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return selection, err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `
		WITH RECURSIVE selected(oid) AS (
			SELECT unnest($1::oid[])
			UNION
			SELECT c.oid
			FROM pg_catalog.pg_inherits h
			JOIN selected s ON s.oid=h.inhparent
			JOIN pg_catalog.pg_class c ON c.oid=h.inhrelid AND c.relispartition
		), seeds(classid,objid) AS (
			SELECT 'pg_catalog.pg_class'::regclass::oid, oid FROM selected
			UNION SELECT 'pg_catalog.pg_class'::regclass::oid, i.indexrelid
				FROM pg_catalog.pg_index i JOIN selected s ON s.oid=i.indrelid
			UNION SELECT 'pg_catalog.pg_constraint'::regclass::oid, c.oid
				FROM pg_catalog.pg_constraint c JOIN selected s ON s.oid=c.conrelid
			UNION SELECT 'pg_catalog.pg_attrdef'::regclass::oid, a.oid
				FROM pg_catalog.pg_attrdef a JOIN selected s ON s.oid=a.adrelid
			UNION SELECT 'pg_catalog.pg_trigger'::regclass::oid, t.oid
				FROM pg_catalog.pg_trigger t JOIN selected s ON s.oid=t.tgrelid
				WHERE NOT t.tgisinternal
			UNION SELECT 'pg_catalog.pg_rewrite'::regclass::oid, r.oid
				FROM pg_catalog.pg_rewrite r JOIN selected s ON s.oid=r.ev_class
				WHERE r.rulename<>'_RETURN'
			UNION SELECT 'pg_catalog.pg_policy'::regclass::oid, p.oid
				FROM pg_catalog.pg_policy p JOIN selected s ON s.oid=p.polrelid
			UNION SELECT 'pg_catalog.pg_class'::regclass::oid, d.objid
				FROM pg_catalog.pg_depend d JOIN selected s ON s.oid=d.refobjid
				JOIN pg_catalog.pg_class c ON c.oid=d.objid AND c.relkind='S'
				WHERE d.classid='pg_catalog.pg_class'::regclass
				  AND d.refclassid='pg_catalog.pg_class'::regclass
		), closure(classid,objid) AS (
			SELECT classid,objid FROM seeds
			UNION
			SELECT d.refclassid,d.refobjid
			FROM closure c
			JOIN pg_catalog.pg_depend d ON d.classid=c.classid AND d.objid=c.objid
			WHERE d.deptype<>'p'
		)
		SELECT DISTINCT classid::bigint,objid::bigint
		FROM closure
		ORDER BY classid,objid`, oids)
	if err != nil {
		return selection, fmt.Errorf("inventory recursive schema dependencies: %w", err)
	}
	for rows.Next() {
		var object schema.CatalogObject
		if err := rows.Scan(&object.CatalogOID, &object.ObjectOID); err != nil {
			rows.Close()
			return selection, err
		}
		selection.Objects = append(selection.Objects, object)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return selection, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return selection, err
	}
	return selection, nil
}

func inspectDeferred(dir, sourceDSN string) schema.DeferredInspector {
	return func(ctx context.Context, target *pgx.Conn, entry schema.TOCEntry) (schema.DeferredStatus, error) {
		source, err := postgres.Connect(ctx, sourceDSN)
		if err != nil {
			return schema.DeferredStatus{}, err
		}
		defer source.Close(context.Background())
		if err := postgres.PinSearchPath(ctx, source); err != nil {
			return schema.DeferredStatus{}, err
		}
		switch entry.Description {
		case "TRIGGER":
			return inspectNamedDeferred(ctx, dir, source, target, entry, `
				SELECT n.nspname,c.relname,t.tgname,pg_catalog.pg_get_triggerdef(t.oid,true)
				FROM pg_catalog.pg_trigger t
				JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE t.oid=$1`, `
				SELECT pg_catalog.pg_get_triggerdef(t.oid,true)
				FROM pg_catalog.pg_trigger t
				JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=$1 AND c.relname=$2 AND t.tgname=$3`)
		case "RULE":
			return inspectNamedDeferred(ctx, dir, source, target, entry, `
				SELECT n.nspname,c.relname,r.rulename,pg_catalog.pg_get_ruledef(r.oid,true)
				FROM pg_catalog.pg_rewrite r
				JOIN pg_catalog.pg_class c ON c.oid=r.ev_class
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE r.oid=$1`, `
				SELECT pg_catalog.pg_get_ruledef(r.oid,true)
				FROM pg_catalog.pg_rewrite r
				JOIN pg_catalog.pg_class c ON c.oid=r.ev_class
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=$1 AND c.relname=$2 AND r.rulename=$3`)
		case "POLICY":
			return inspectNamedDeferred(ctx, dir, source, target, entry, `
				SELECT n.nspname,c.relname,p.polname,
				       concat_ws('|',p.polcmd,p.polpermissive,
				         ARRAY(SELECT r.rolname FROM unnest(p.polroles) role_oid
				               JOIN pg_catalog.pg_roles r ON r.oid=role_oid ORDER BY r.rolname)::text,
				         pg_catalog.pg_get_expr(p.polqual,p.polrelid,true),
				         pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,true))
				FROM pg_catalog.pg_policy p
				JOIN pg_catalog.pg_class c ON c.oid=p.polrelid
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE p.oid=$1`, `
				SELECT concat_ws('|',p.polcmd,p.polpermissive,
				         ARRAY(SELECT r.rolname FROM unnest(p.polroles) role_oid
				               JOIN pg_catalog.pg_roles r ON r.oid=role_oid ORDER BY r.rolname)::text,
				         pg_catalog.pg_get_expr(p.polqual,p.polrelid,true),
				         pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,true))
				FROM pg_catalog.pg_policy p
				JOIN pg_catalog.pg_class c ON c.oid=p.polrelid
				JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
				WHERE n.nspname=$1 AND c.relname=$2 AND p.polname=$3`)
		case "COMMENT":
			return inspectComment(ctx, source, target, entry)
		default:
			return schema.DeferredStatus{}, fmt.Errorf("unsupported deferred object class %q", entry.Description)
		}
	}
}

// inspectNamedDeferred locates a trigger, rule, or policy on the target by the
// name its source counterpart carries.
//
// Both definitions are server-deparsed SQL, so a difference between them is not
// evidence of divergence: the text depends on the reading session's search_path
// and on the stored parse tree, which PostgreSQL rewrites in ways deparsing does
// not reproduce. pg_restore created the object from the source's own dump, so
// its identity is established by existing under the expected name. A rendering
// difference is logged, because it is the only visible trace of such an
// artifact, and never refused.
func inspectNamedDeferred(
	ctx context.Context,
	dir string,
	source, target *pgx.Conn,
	entry schema.TOCEntry,
	sourceSQL, targetSQL string,
) (schema.DeferredStatus, error) {
	var namespace, relation, name, expected string
	if err := source.QueryRow(ctx, sourceSQL, entry.ObjectOID).Scan(&namespace, &relation, &name, &expected); err != nil {
		return schema.DeferredStatus{}, fmt.Errorf("inspect source %s %d: %w", entry.Description, entry.ObjectOID, err)
	}
	var actual string
	err := target.QueryRow(ctx, targetSQL, namespace, relation, name).Scan(&actual)
	if errors.Is(err, pgx.ErrNoRows) {
		return schema.DeferredStatus{}, nil
	}
	if err != nil {
		return schema.DeferredStatus{}, err
	}
	if actual != expected {
		logEvent(dir, "deferred_rendering_differs", map[string]any{
			"kind": entry.Description, "schema": namespace, "relation": relation,
			"name": name, "target": actual, "source": expected,
		})
	}
	return schema.DeferredStatus{Exists: true, Definition: actual}, nil
}

// commentKinds are the object classes a COMMENT archive entry can name, sorted
// longest first so that MATERIALIZED VIEW and FOREIGN TABLE are matched whole
// rather than truncated to a shorter kind that prefixes them.
var commentKinds = func() []string {
	kinds := []string{
		"AGGREGATE", "COLUMN", "DOMAIN", "EXTENSION", "FOREIGN TABLE", "FUNCTION",
		"INDEX", "MATERIALIZED VIEW", "PROCEDURE", "SCHEMA", "SEQUENCE", "TABLE",
		"TYPE", "VIEW",
	}
	slices.SortStableFunc(kinds, func(a, b string) int { return len(b) - len(a) })
	return kinds
}()

// commentLookup returns a query and arguments that read the comment on the
// object a COMMENT archive entry describes, for use against either database.
//
// pg_dump writes every COMMENT entry with a nil catalog identity, so the entry's
// object OID is always zero and the object can only be resolved by name. The
// tag names it without its schema; the entry's namespace column carries that,
// unquoted, while the tag's identifier is already quoted where necessary.
func commentLookup(entry schema.TOCEntry) (string, []any, error) {
	var kind, identity string
	for _, candidate := range commentKinds {
		if rest, ok := strings.CutPrefix(entry.Tag, candidate+" "); ok {
			kind, identity = candidate, rest
			break
		}
	}
	if identity == "" {
		return "", nil, fmt.Errorf("unsupported comment target %q in schema %q", entry.Tag, entry.Namespace)
	}
	qualify := func(name string) string {
		if entry.Namespace == "" || entry.Namespace == "-" {
			return name
		}
		return pgx.Identifier{entry.Namespace}.Sanitize() + "." + name
	}
	switch kind {
	case "TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE", "SEQUENCE", "INDEX":
		return "SELECT pg_catalog.obj_description(pg_catalog.to_regclass($1),'pg_class')",
			[]any{qualify(identity)}, nil
	case "COLUMN":
		dot := strings.LastIndex(identity, ".")
		if dot < 1 {
			return "", nil, fmt.Errorf("unrecognized column comment identity %q", entry.Tag)
		}
		return `SELECT pg_catalog.col_description(a.attrelid,a.attnum)
			FROM pg_catalog.pg_attribute a
			WHERE a.attrelid=pg_catalog.to_regclass($1) AND a.attname=$2`,
			[]any{qualify(identity[:dot]), strings.Trim(identity[dot+1:], `"`)}, nil
	case "SCHEMA":
		return "SELECT pg_catalog.obj_description(pg_catalog.to_regnamespace($1),'pg_namespace')",
			[]any{identity}, nil
	case "TYPE", "DOMAIN":
		return "SELECT pg_catalog.obj_description(pg_catalog.to_regtype($1),'pg_type')",
			[]any{qualify(identity)}, nil
	default:
		return "SELECT pg_catalog.obj_description(pg_catalog.to_regprocedure($1),'pg_proc')",
			[]any{qualify(identity)}, nil
	}
}

// commentValue reads one comment, reporting a missing object as no comment.
func commentValue(ctx context.Context, conn *pgx.Conn, sql string, args []any) (*string, error) {
	var value *string
	if err := conn.QueryRow(ctx, sql, args...).Scan(&value); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return value, nil
}

func inspectComment(ctx context.Context, source, target *pgx.Conn, entry schema.TOCEntry) (schema.DeferredStatus, error) {
	sql, args, err := commentLookup(entry)
	if err != nil {
		return schema.DeferredStatus{}, err
	}
	expected, err := commentValue(ctx, source, sql, args)
	if err != nil {
		return schema.DeferredStatus{}, fmt.Errorf("inspect source comment on %s: %w", entry.Tag, err)
	}
	if expected == nil {
		return schema.DeferredStatus{}, fmt.Errorf("source comment on %s %s is missing", entry.Namespace, entry.Tag)
	}
	actual, err := commentValue(ctx, target, sql, args)
	if err != nil {
		return schema.DeferredStatus{}, fmt.Errorf("inspect target comment on %s: %w", entry.Tag, err)
	}
	if actual == nil {
		return schema.DeferredStatus{}, nil
	}
	// A comment is a stored literal, not deparsed SQL, so the two sides are
	// directly comparable and a difference is real divergence.
	return schema.DeferredStatus{Exists: true, Diverged: *actual != *expected, Definition: *actual}, nil
}

func runApplierToFollow(
	ctx context.Context,
	cfg config.Config,
	store *state.Store,
	snapshot setup.Snapshot,
	durable *cdc.DurableWatermark,
	segments *cdc.SegmentCatalog,
	phase state.Phase,
) error {
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	pruner, err := cdc.NewSegmentPruner(cdc.SegmentPrunerConfig{
		Directory: filepath.Join(cfg.Dir, "cdc"),
		Interval:  cfg.SegmentPruneInterval,
		Catalog:   segments,
	})
	if err != nil {
		return err
	}
	// The reservoir is what makes the replication path checkable at all: a sample
	// of the source heap finds the rows the base copy wrote, and misses the ones
	// the applier wrote almost entirely.
	var sampler *cdcSampler
	if cfg.CDCSampleRows > 0 {
		sampler, err = newCDCSampler(ctx, store, cfg.CDCSampleRows)
		if err != nil {
			return err
		}
		defer sampler.Close()
	}
	applier, err := cdc.NewApplier(cdc.ApplierConfig{
		ConnString: cfg.Target, Directory: filepath.Join(cfg.Dir, "cdc"),
		StreamID: snapshot.Slot, StreamGeneration: streamGeneration(
			migration.SourceFingerprint, migration.FilterFingerprint,
		), TargetHasCopiedData: true, Durable: durable, EndPosition: endPosition(store),
		AfterProgress:     pruner.OnProgress,
		Sampler:           samplerOrNil(sampler),
		ReplayWorkers:     cfg.ReplayWorkers,
		BatchMaxDataBytes: cfg.ReplayBatchBytes,
		BatchMaxChanges:   cfg.ReplayBatchChanges,
	})
	if err != nil {
		return err
	}
	if phase == state.PhaseFollow || phase == state.PhaseDrained || phase == state.PhaseCutover {
		return runApplierWithPruner(ctx, applier, pruner, store)
	}
	boundary := durable.Load()
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- runApplierWithPruner(applyCtx, applier, pruner, store) }()
	if err := awaitCatchup(ctx, boundary, applier.WaitUntil, result); err != nil {
		return err
	}
	if err := transition(ctx, cfg, store, state.PhaseFollow); err != nil {
		return err
	}
	if err := pauseForCrashTest(ctx, state.PhaseFollow); err != nil {
		return err
	}
	logEvent(cfg.Dir, "phase", map[string]any{"phase": state.PhaseFollow, "catchup_boundary": pglogrepl.LSN(boundary).String()})
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func runApplierWithPruner(
	ctx context.Context,
	applier *cdc.Applier,
	pruner *cdc.SegmentPruner,
	store *state.Store,
) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return pruner.Run(groupCtx) })
	group.Go(func() error { return runApplierContinuous(groupCtx, applier, store) })
	return group.Wait()
}

// awaitCatchup waits for target apply progress to reach boundary while watching
// the applier that produces it. The wait polls a row on the target that only the
// applier advances, so an applier exit has to end the wait as well: reading the
// applier's result only after the wait returned left the process polling forever
// for progress nothing could make, holding a replication slot open on the
// source, reporting a healthy phase, with the explanation unread in a channel.
func awaitCatchup(
	ctx context.Context,
	boundary cdc.LSN,
	wait func(context.Context, cdc.LSN) error,
	applier <-chan error,
) error {
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	waited := make(chan error, 1)
	go func() { waited <- wait(waitCtx, boundary) }()
	select {
	case err := <-applier:
		if err == nil {
			err = errors.New("the CDC applier stopped before catch-up reached its boundary")
		}
		return err
	case err := <-waited:
		return err
	}
}

func runReceiverContinuous(ctx context.Context, receiver *cdc.Receiver, store *state.Store) error {
	for {
		err := receiver.Run(ctx)
		if err != nil {
			return err
		}
		if err := waitForRecoveredFollow(ctx, store); err != nil {
			return err
		}
	}
}

func runApplierContinuous(ctx context.Context, applier *cdc.Applier, store *state.Store) error {
	for {
		err := applier.Run(ctx)
		if err != nil {
			return err
		}
		if err := waitForRecoveredFollow(ctx, store); err != nil {
			return err
		}
	}
}

func waitForRecoveredFollow(ctx context.Context, store *state.Store) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		migration, err := store.Migration(ctx)
		if err != nil {
			return err
		}
		if migration.Phase == state.PhaseFollow && migration.EndPosition == "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// walUsage describes source WAL pressure attributable to the migration slot.
// SlotLimit is negative when max_slot_wal_keep_size is unbounded. DirectoryBytes
// is negative when pg_ls_waldir() is not executable by the migration role.
type walUsage struct {
	Retained       int64
	SlotLimit      int64
	MaxWALSize     int64
	DirectoryBytes int64
}

// walGrowthFactor is the multiple of max_wal_size that the WAL directory must
// exceed before unbounded slot retention is treated as a disk-exhaustion risk.
// PostgreSQL drives checkpoints to keep the directory near max_wal_size, so
// sustained growth well past it means WAL is not being recycled.
const walGrowthFactor = 4

func readWALUsage(ctx context.Context, conn *pgx.Conn, slot string) (walUsage, error) {
	var usage walUsage
	if err := conn.QueryRow(
		ctx, `
		SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint,
		       CASE WHEN current_setting('max_slot_wal_keep_size')='-1' THEN -1
		            ELSE pg_size_bytes(current_setting('max_slot_wal_keep_size')) END,
		       pg_size_bytes(current_setting('max_wal_size'))
		FROM pg_replication_slots WHERE slot_name=$1`, slot,
	).Scan(&usage.Retained, &usage.SlotLimit, &usage.MaxWALSize); err != nil {
		return walUsage{}, err
	}
	// pg_ls_waldir() requires pg_monitor and is unavailable to some managed
	// roles, so a failure degrades to the retention-only signal.
	if err := conn.QueryRow(
		ctx,
		"SELECT coalesce(sum(size),0)::bigint FROM pg_catalog.pg_ls_waldir()",
	).Scan(&usage.DirectoryBytes); err != nil {
		usage.DirectoryBytes = -1
	}
	return usage, nil
}

// walHeadroomAlarm reports whether the migration slot is endangering source WAL
// storage. A bounded max_slot_wal_keep_size has PostgreSQL's own invalidation as
// a backstop, so the alarm fires as retention approaches it. When retention is
// unbounded there is no backstop, so growth of the WAL directory beyond
// max_wal_size becomes the signal.
func walHeadroomAlarm(usage walUsage) (string, bool) {
	if usage.SlotLimit >= 0 {
		if usage.Retained >= usage.SlotLimit {
			return fmt.Sprintf("slot retains %d bytes, reaching max_slot_wal_keep_size %d",
				usage.Retained, usage.SlotLimit), true
		}
		return "", false
	}
	if usage.MaxWALSize <= 0 || usage.Retained <= usage.MaxWALSize {
		return "", false
	}
	threshold := walGrowthFactor * usage.MaxWALSize
	if usage.DirectoryBytes < 0 {
		if usage.Retained >= threshold {
			return fmt.Sprintf(
				"max_slot_wal_keep_size is unbounded and the slot retains %d bytes, over %d times max_wal_size %d; "+
					"WAL directory size is unavailable to this role",
				usage.Retained, walGrowthFactor, usage.MaxWALSize,
			), true
		}
		return "", false
	}
	if usage.DirectoryBytes >= threshold {
		return fmt.Sprintf(
			"max_slot_wal_keep_size is unbounded, the source WAL directory holds %d bytes against max_wal_size %d, "+
				"and the slot retains %d bytes of it; the source WAL volume can fill",
			usage.DirectoryBytes, usage.MaxWALSize, usage.Retained,
		), true
	}
	return "", false
}

// followChecks watches source WAL pressure for as long as the migration follows.
//
// It deliberately does not compare data. A drift check that runs on a timer has to
// read whole tables to say anything, which on a large shard is a full read of the
// database per tick inside a per-tick timeout it cannot meet: it burns source I/O
// and reports nothing. Data comparison belongs to verify, which reads by page range
// and can be run against a following migration whenever an operator wants it.
func followChecks(ctx context.Context, cfg config.Config, store *state.Store, slot string) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		migration, err := store.Migration(ctx)
		if err != nil {
			return err
		}
		if migration.Phase != state.PhaseFollow {
			continue
		}
		conn, err := postgres.Connect(ctx, cfg.Source)
		if err != nil {
			_ = store.UpsertFinding(ctx, state.Finding{ID: "follow-source-health", Kind: "health", Severity: "error", Message: err.Error()})
			continue
		}
		usage, err := readWALUsage(ctx, conn, slot)
		conn.Close(context.Background())
		if err != nil {
			_ = store.UpsertFinding(ctx, state.Finding{ID: "follow-source-health", Kind: "health", Severity: "error", Message: err.Error()})
			continue
		}
		_ = store.ResolveFinding(ctx, "follow-source-health")
		if message, exceeded := walHeadroomAlarm(usage); exceeded {
			_ = store.UpsertFinding(ctx, state.Finding{
				ID: "follow-wal-headroom", Kind: "replication", Severity: "error", Message: message,
			})
		} else {
			_ = store.ResolveFinding(ctx, "follow-wal-headroom")
		}
		logEvent(cfg.Dir, "follow_health", map[string]any{
			"retained_wal_bytes":   usage.Retained,
			"slot_wal_limit_bytes": usage.SlotLimit,
			"max_wal_size_bytes":   usage.MaxWALSize,
			"wal_directory_bytes":  usage.DirectoryBytes,
		})
	}
}

func logEvent(dir, event string, values map[string]any) {
	record := map[string]any{"time": time.Now().UTC(), "event": event}
	for key, value := range values {
		record[key] = value
	}
	data, err := json.Marshal(record)
	if err != nil {
		return
	}
	file, err := os.OpenFile(filepath.Join(dir, "log", "pgmigrate.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = file.Write(append(data, '\n'))
	_ = file.Close()
}

func transition(ctx context.Context, cfg config.Config, store *state.Store, phase state.Phase) error {
	if err := store.TransitionPhase(ctx, phase); err != nil {
		logEvent(cfg.Dir, "error", map[string]any{"phase": phase, "error": err.Error()})
		return err
	}
	logEvent(cfg.Dir, "phase", map[string]any{"phase": phase})
	return nil
}

// errRepeatedBaseCopyFailure is returned instead of restarting a base copy that
// has already failed the same way twice. It is never itself recorded as a
// failure, so the refusal is stable across restarts rather than resetting the
// counter it depends on.
var errRepeatedBaseCopyFailure = errors.New("refusing to restart the base copy after repeated identical failures")

// baseCopyFailureLimit is how many consecutive identical failures are tolerated
// before a restart is refused, so exactly one automatic retry is allowed.
const baseCopyFailureLimit = 2

// guardRepeatedBaseCopyFailure refuses to discard base-copy progress for a
// failure that has already proved it repeats. Restarting setup, schema, or copy
// drops the target tables and copies from a new snapshot, because the old
// snapshot died with the process that exported it. Under a supervisor that turns
// a deterministic failure into an endless loop that can never finish and that
// re-runs destructive work every cycle, so the second identical failure stops
// and asks for a decision instead.
func guardRepeatedBaseCopyFailure(ctx context.Context, cfg config.Config, store *state.Store, migration state.Migration) error {
	attempt, err := store.FailedAttempt(ctx)
	if err != nil {
		return err
	}
	if cfg.RetryBaseCopy {
		if attempt.Consecutive > 0 {
			logEvent(cfg.Dir, "base_copy_retry_forced", map[string]any{
				"phase": attempt.Phase, "consecutive": attempt.Consecutive, "detail": attempt.Detail,
			})
		}
		return store.ClearFailedAttempt(ctx)
	}
	if attempt.Consecutive < baseCopyFailureLimit {
		return nil
	}
	logEvent(cfg.Dir, "base_copy_restart_refused", map[string]any{
		"phase": attempt.Phase, "consecutive": attempt.Consecutive, "detail": attempt.Detail,
	})
	return fmt.Errorf("%w: the last %d runs all failed in phase %s with the same error, and restarting from phase %s would drop the target tables and copy again from a new snapshot. Fix the cause, then re-run with --retry-base-copy to try again. Last error: %s",
		errRepeatedBaseCopyFailure, attempt.Consecutive, attempt.Phase, migration.Phase, attempt.Detail)
}

// recordFailedAttempt durably notes how this run died, for the run that follows
// it, and leaves the same account where an operator looks first: the run log and,
// for divergence, an open finding. A cancelled run is a deliberate stop rather
// than a failure, and the refusal above must not overwrite the record that
// produced it.
func recordFailedAttempt(ctx context.Context, out io.Writer, dir string, store *state.Store, runErr error) {
	if runErr == nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, errRepeatedBaseCopyFailure) {
		return
	}
	migration, err := store.Migration(context.WithoutCancel(ctx))
	if err != nil {
		return
	}
	detail := runErr.Error()
	if limit := 512; len(detail) > limit {
		detail = strings.ToValidUTF8(detail[:limit], "") + "..."
	}
	logEvent(dir, "error", map[string]any{"phase": migration.Phase, "error": detail})
	// Divergence is permanent and needs a person, so it also becomes a finding
	// that `status` reports rather than only a line in a log file.
	var divergence *cdc.DivergenceError
	if errors.As(runErr, &divergence) {
		_ = store.UpsertFinding(context.WithoutCancel(ctx), state.Finding{
			ID: cdcDivergenceFindingID, Kind: "divergence", Severity: "error", Message: detail,
		})
	}
	if err := store.RecordFailedAttempt(context.WithoutCancel(ctx), migration.Phase, failureSignature(runErr), detail); err != nil {
		fmt.Fprintf(out, "warning: could not record how this run failed: %v\n", err)
	}
}

// failureSignature identifies a failure closely enough to recognize it happening
// again, leaving out the parts that differ between attempts such as generated
// names, byte counts, and object identifiers.
func failureSignature(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "" {
		return "sqlstate:" + pgErr.Code
	}
	return fmt.Sprintf("error:%x", sha256.Sum256([]byte(err.Error())))
}

func pauseForCrashTest(ctx context.Context, phase state.Phase) error {
	if os.Getenv("PGMIGRATE_TEST_PAUSE_PHASE") != string(phase) {
		return nil
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type failedAttemptProgress struct {
	attempt  state.FailedAttempt
	progress postgres.ReplicationProgress
}

func captureFailedAttemptProgress(
	ctx context.Context,
	store *state.Store,
	targetDSN string,
	streamID string,
) (*failedAttemptProgress, error) {
	attempt, err := store.FailedAttempt(ctx)
	if err != nil {
		return nil, err
	}
	if attempt.Consecutive == 0 {
		return nil, nil
	}
	conn, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	progress, exists, err := postgres.ReadReplicationProgress(ctx, conn, streamID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, cdc.ErrMissingTargetProgress
	}
	return &failedAttemptProgress{attempt: attempt, progress: progress}, nil
}

func targetProgressPassedFailure(
	baseline postgres.ReplicationProgress,
	current postgres.ReplicationProgress,
) (bool, error) {
	if current.RemoteLSN < baseline.RemoteLSN ||
		current.Transactions < baseline.Transactions ||
		current.Rows < baseline.Rows {
		return false, fmt.Errorf(
			"target replication progress regressed after resume: lsn %s/%s, transactions %d/%d, rows %d/%d",
			current.RemoteLSN, baseline.RemoteLSN,
			current.Transactions, baseline.Transactions,
			current.Rows, baseline.Rows,
		)
	}
	return current.RemoteLSN > baseline.RemoteLSN ||
		current.Transactions > baseline.Transactions ||
		current.Rows > baseline.Rows, nil
}

func monitorProgress(
	ctx context.Context,
	store *state.Store,
	targetDSN string,
	streamID string,
	durable *cdc.DurableWatermark,
	dir string,
	failureBaseline *failedAttemptProgress,
) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	nextLog := time.Now()
	for {
		conn, err := postgres.Connect(ctx, targetDSN)
		if err != nil {
			return err
		}
		progress, _, readErr := postgres.ReadReplicationProgress(ctx, conn, streamID)
		conn.Close(context.Background())
		if readErr != nil {
			return readErr
		}
		passedFailure := false
		if failureBaseline != nil {
			passedFailure, err = targetProgressPassedFailure(failureBaseline.progress, progress)
			if err != nil {
				return err
			}
		}
		if err := store.UpdateApplyProgress(ctx, state.ApplyProgress{
			StagedLSN:  pglogrepl.LSN(durable.Load()).String(),
			AppliedLSN: progress.RemoteLSN.String(),
			Txns:       progress.Transactions,
			Rows:       progress.Rows,
			UpdatedAt:  progress.UpdatedAt,
		}); err != nil {
			return err
		}
		if failureBaseline != nil && passedFailure {
			if _, err := store.ResolveFailedAttempt(
				ctx, failureBaseline.attempt, cdcDivergenceFindingID,
			); err != nil {
				return err
			}
			// Whether it cleared or found a newer attempt, this baseline has been
			// consumed and must never act on a later failure.
			failureBaseline = nil
		}
		if !time.Now().Before(nextLog) {
			logEvent(dir, "progress", map[string]any{
				"staged_lsn":   pglogrepl.LSN(durable.Load()).String(),
				"applied_lsn":  progress.RemoteLSN.String(),
				"transactions": progress.Transactions,
				"rows":         progress.Rows,
			})
			nextLog = time.Now().Add(5 * time.Second)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func serveMetrics(ctx context.Context, address string, store *state.Store) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(observe.NewRegistry(store), promhttp.HandlerOpts{}))
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return ctx.Err()
	}
	return err
}

func resetInterruptedBaseCopy(ctx context.Context, cfg config.Config, store *state.Store, tables []pgcopy.Table, migration state.Migration) error {
	snapshot, err := readSnapshot(cfg.Dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read prior snapshot metadata: %w", err)
	}
	target, err := postgres.Connect(ctx, cfg.Target)
	if err != nil {
		return err
	}
	defer target.Close(context.Background())
	var targetSource, targetFilter, targetStream, targetGeneration string
	identityErr := target.QueryRow(ctx, `
		SELECT source_fingerprint, filter_fingerprint, stream_id, stream_generation
		FROM pgmigrate_internal.migration_identity WHERE id=1`).Scan(
		&targetSource, &targetFilter, &targetStream, &targetGeneration,
	)
	if identityErr == nil && (targetSource != migration.SourceFingerprint ||
		targetFilter != migration.FilterFingerprint || targetStream != migration.SlotName ||
		targetGeneration != streamGeneration(migration.SourceFingerprint, migration.FilterFingerprint)) {
		return errors.New("target migration identity does not match durable state")
	}
	if identityErr != nil && !isUndefinedTargetIdentity(identityErr) {
		return fmt.Errorf("validate target migration identity: %w", identityErr)
	}
	for _, table := range tables {
		var owner, current string
		err := target.QueryRow(ctx, `
			SELECT pg_get_userbyid(c.relowner), current_user
			FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname=$1 AND c.relname=$2`, table.Schema, table.Name).Scan(&owner, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if owner != current {
			return fmt.Errorf("refuse to clean target %s.%s owned by %s", table.Schema, table.Name, owner)
		}
	}
	if snapshot.Slot == "" && migration.SlotName == "" {
		if err := setup.RecoverStale(ctx, setup.Config{
			SourceDSN: cfg.Source, TargetDSN: cfg.Target, Dir: cfg.Dir,
			MigrationID: migrationID(cfg.Dir), Tables: toSetup(tables),
		}, setup.ResumeConfirmation{NoSnapshot: true}); err != nil {
			return fmt.Errorf("recover stale setup objects: %w", err)
		}
	}
	// Reverting here is not optional housekeeping. ResetBaseCopy below deletes
	// every step that is not a preflight step, which includes the records of what
	// tuning has to undo, so a reset that skipped this would leave the target
	// permanently configured for a bulk load with nothing left to describe how it
	// used to be. The fresh attempt re-tunes from the restored values.
	if err := revertTargetTuning(ctx, cfg.Target, store); err != nil {
		return err
	}
	// Dropping the publication has to come before the identities go back. A
	// published relation with no usable identity rejects every UPDATE and DELETE,
	// so restoring the original identity first would break production writes for
	// as long as the publication survived.
	if snapshot.Slot != "" {
		if err := setup.CleanupOwned(ctx, cfg.Source, snapshot.Publication, snapshot.Slot, toSetup(tables), snapshot.Failover); err != nil {
			return err
		}
	} else if migration.SlotName != "" {
		publication, _ := setup.Names(migration.SourceFingerprint, migrationID(cfg.Dir))
		if err := setup.CleanupOwned(ctx, cfg.Source, publication, migration.SlotName, toSetup(tables), false); err != nil {
			return err
		}
	}
	// Same reasoning as the tuning revert above: ResetBaseCopy deletes the records
	// of what to restore, so a reset that skipped this would leave the source at
	// FULL permanently with nothing left describing what it used to be.
	if err := restoreReplicaIdentities(ctx, cfg.Source, store); err != nil {
		return err
	}
	schemas := map[string]bool{}
	for _, table := range tables {
		schemas[table.Schema] = true
		var owner, current string
		err := target.QueryRow(ctx, `
			SELECT pg_get_userbyid(c.relowner), current_user
			FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname=$1 AND c.relname=$2`, table.Schema, table.Name).Scan(&owner, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if owner != current {
			return fmt.Errorf("refuse to remove target %s.%s owned by %s", table.Schema, table.Name, owner)
		}
		if _, err := target.Exec(ctx, "DROP TABLE "+pgx.Identifier{table.Schema, table.Name}.Sanitize()+" CASCADE"); err != nil {
			return err
		}
	}
	// pg_restore emits inherited partition constraint drops before its table
	// drops. PostgreSQL rejects those while the partition still exists, so remove
	// the already ownership-validated migration tables first. The archive cleanup
	// then removes the remaining functions, types, comments, and public-schema
	// objects with IF EXISTS semantics.
	archive := filepath.Join(cfg.Dir, "dump", "schema.dump")
	if _, err := os.Stat(archive); err == nil {
		service := schema.Service{Tools: schema.Tools{Restore: cfg.PGRestorePath}}
		if err := service.Clean(ctx, cfg.Target, archive); err != nil {
			return fmt.Errorf("clean prior schema archive: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for name := range schemas {
		if name == "public" {
			continue
		}
		var owner, current string
		err := target.QueryRow(ctx, `
			SELECT pg_get_userbyid(n.nspowner), current_user
			FROM pg_namespace n WHERE n.nspname=$1`, name).Scan(&owner, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		if owner != current {
			return fmt.Errorf("refuse to remove target schema %s owned by %s", name, owner)
		}
		var remaining int
		if err := target.QueryRow(ctx, `
			SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname=$1 AND c.relkind IN ('r','p','v','m','f')`, name).Scan(&remaining); err != nil {
			return err
		}
		if remaining != 0 {
			return fmt.Errorf("refuse to remove target schema %s containing %d untracked relation(s)", name, remaining)
		}
		if _, err := target.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			return err
		}
	}
	_, _ = target.Exec(ctx, "DROP SCHEMA IF EXISTS pgmigrate_internal CASCADE")
	// Keep the post-copy phase durable until every snapshot-bound file has been
	// removed and its empty directories recreated. If the pod dies anywhere
	// before ResetBaseCopy commits, retrying this cleanup is idempotent. Once the
	// state says preflight, no stale snapshot, dump, or CDC segment can survive.
	for _, path := range []string{filepath.Join(cfg.Dir, "snapshot.json"), filepath.Join(cfg.Dir, "dump"), filepath.Join(cfg.Dir, "cdc")} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	for _, path := range []string{filepath.Join(cfg.Dir, "dump"), filepath.Join(cfg.Dir, "cdc")} {
		if err := os.MkdirAll(path, 0o750); err != nil {
			return err
		}
	}
	if migration.Phase == state.PhaseIndexes || migration.Phase == state.PhaseCatchup || migration.Phase == state.PhaseFollow {
		return store.ResetForFreshSnapshot(ctx, cdcDivergenceFindingID)
	}
	return store.ResetBaseCopy(ctx)
}

func recordTargetIdentity(ctx context.Context, targetDSN, sourceFingerprint, filterFingerprint, streamID, generation string) error {
	conn, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if _, err = conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS pgmigrate_internal"); err != nil {
		return err
	}
	if _, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS pgmigrate_internal.migration_identity (
			id integer PRIMARY KEY CHECK (id=1),
			source_fingerprint text NOT NULL,
			filter_fingerprint text NOT NULL,
			stream_id text NOT NULL,
			stream_generation text NOT NULL
		)`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `
		INSERT INTO pgmigrate_internal.migration_identity(
			id,source_fingerprint,filter_fingerprint,stream_id,stream_generation)
		VALUES(1,$1,$2,$3,$4)
		ON CONFLICT(id) DO NOTHING`,
		sourceFingerprint, filterFingerprint, streamID, generation)
	if err != nil {
		return err
	}
	var haveSource, haveFilter, haveStream, haveGeneration string
	if err := conn.QueryRow(ctx, `
		SELECT source_fingerprint,filter_fingerprint,stream_id,stream_generation
		FROM pgmigrate_internal.migration_identity WHERE id=1`).Scan(
		&haveSource, &haveFilter, &haveStream, &haveGeneration,
	); err != nil {
		return err
	}
	if haveSource != sourceFingerprint || haveFilter != filterFingerprint ||
		haveStream != streamID || haveGeneration != generation {
		return errors.New("target migration identity collision")
	}
	return nil
}

func isUndefinedTargetIdentity(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "3F000")
}

func validateTargetIdentity(ctx context.Context, cfg config.Config, store *state.Store) error {
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	suppliedSource, err := sourceFingerprint(ctx, cfg.Source)
	if err != nil {
		return err
	}
	if suppliedSource != migration.SourceFingerprint {
		return errors.New("supplied source does not match migration identity")
	}
	return validateTargetOnly(ctx, cfg.Target, migration)
}

func validateTargetOnly(ctx context.Context, targetDSN string, migration state.Migration) error {
	conn, err := postgres.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var source, filter, stream, generation string
	if err := conn.QueryRow(ctx, `
		SELECT source_fingerprint,filter_fingerprint,stream_id,stream_generation
		FROM pgmigrate_internal.migration_identity WHERE id=1`).Scan(
		&source, &filter, &stream, &generation,
	); err != nil {
		return fmt.Errorf("read target migration identity: %w", err)
	}
	if source != migration.SourceFingerprint || filter != migration.FilterFingerprint ||
		stream != migration.SlotName ||
		generation != streamGeneration(migration.SourceFingerprint, migration.FilterFingerprint) {
		return errors.New("supplied target does not match migration identity")
	}
	return nil
}

func readSnapshot(dir string) (setup.Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		return setup.Snapshot{}, err
	}
	var snapshot setup.Snapshot
	err = json.Unmarshal(data, &snapshot)
	return snapshot, err
}

func (a App) Status(ctx context.Context, cfg config.Config) error {
	for {
		store, err := state.OpenReadOnly(ctx, cfg.Dir)
		if err != nil {
			return err
		}
		snapshot, captureErr := observe.Capture(ctx, store, time.Now().UTC())
		closeErr := store.Close()
		if captureErr != nil {
			return captureErr
		}
		if closeErr != nil {
			return closeErr
		}
		if cfg.StatusJSON {
			err = observe.RenderJSON(a.output(), snapshot)
		} else {
			err = observe.RenderText(a.output(), snapshot)
		}
		if err != nil || cfg.StatusWatch <= 0 {
			return err
		}
		timer := time.NewTimer(cfg.StatusWatch)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func verification(ctx context.Context, cfg config.Config, store *state.Store, progressOut io.Writer) (result verify.Result, runErr error) {
	tables, err := store.ListTables(ctx)
	if err != nil {
		return verify.Result{}, err
	}
	verifyTables, err := verificationInventory(ctx, cfg, tables)
	if err != nil {
		return verify.Result{}, err
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		return verify.Result{}, err
	}
	progress := newVerifyProgress(cfg, store, progressOut, tables)
	defer progress.finish()
	capabilities, err := sourceCapabilities(ctx, cfg.Source)
	if err != nil {
		return verify.Result{}, err
	}
	boundary := newMarker(cfg, "verify:", capabilities)
	defer boundary.close()
	// While the migration is following, a heap-sample row may simply be in flight.
	// These two hooks are what tell that apart from a defect: mark a source
	// position, wait for apply to pass it, look again.
	mark, wait := recheckHooks(cfg, boundary, migration.SlotName, migration.Phase)
	audit, err := newVerificationAudit(cfg.Dir)
	if err != nil {
		return verify.Result{}, err
	}
	defer func() { runErr = errors.Join(runErr, audit.finish(result.Complete, result.Converged, runErr)) }()
	return verify.Run(ctx, verify.Config{
		Source: connector(cfg.Source), Target: connector(cfg.Target), Tables: verifyTables,
		Progress:        progress,
		Audit:           audit.write,
		Workers:         cfg.VerifyWorkers,
		SampleRows:      cfg.VerifySampleRows,
		SampleWindows:   cfg.VerifySampleWindows,
		BatchRows:       cfg.VerifyBatchRows,
		DutyCycle:       cfg.VerifyDutyCycle,
		TableTimeout:    cfg.VerifyTableTimeout,
		ConvergeTimeout: cfg.VerifyConvergeTimeout,
		CDCKeys:         recordedCDCKeys(store),
		CDCRows:         cfg.VerifyCDCRows,
		Boundary:        mark,
		WaitApplied:     wait,
	})
}

func verificationInventory(ctx context.Context, cfg config.Config, tables []state.Table) ([]verify.Table, error) {
	selected := make(map[string]bool, len(tables))
	for _, table := range tables {
		selected[table.Schema+"\x00"+table.Name] = true
	}
	source, err := postgres.Connect(ctx, cfg.Source)
	if err != nil {
		return nil, err
	}
	verifyTables, err := verify.Inventory(ctx, source, func(schema, table string) bool {
		return selected[schema+"\x00"+table]
	})
	source.Close(context.Background())
	if err != nil {
		return nil, err
	}
	return verifyTables, nil
}

// applyStallTimeout bounds how long a wait on target apply progress tolerates no
// movement at all. The bound is on progress rather than on total duration,
// because a large backlog may legitimately take a long time to drain: a wait
// that is advancing is never interrupted, and one that has stopped says so.
//
// Without it, a wait on progress that only another process advances is an
// unbounded hang. That is how a divergent apply presented in the first place, and
// at cutover it is worse: the source is already refusing writes, so an
// indefinite wait is an outage rather than a stalled migration.
const applyStallTimeout = 2 * time.Minute

func waitTargetProgress(ctx context.Context, targetDSN, streamID, wanted string) error {
	want, err := pglogrepl.ParseLSN(wanted)
	if err != nil {
		return err
	}
	var reached pglogrepl.LSN
	advancedAt := time.Now()
	for {
		conn, err := postgres.Connect(ctx, targetDSN)
		if err != nil {
			return err
		}
		got, _, readErr := postgres.ReadProgress(ctx, conn, streamID)
		conn.Close(context.Background())
		if readErr != nil {
			return readErr
		}
		if got >= want {
			return nil
		}
		if got > reached {
			reached = got
			advancedAt = time.Now()
		}
		if elapsed := time.Since(advancedAt); elapsed > applyStallTimeout {
			return fmt.Errorf(
				"target apply progress has been stuck at %s for %s while waiting for %s: the pgmigrate run process is not applying changes. Check its log for a divergence or connection failure, and whether it is still running",
				got, elapsed.Round(time.Second), want,
			)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (a App) Verify(ctx context.Context, cfg config.Config) error {
	store, err := state.OpenControl(ctx, cfg.Dir)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := validateTargetIdentity(ctx, cfg, store); err != nil {
		return err
	}
	result, err := verification(ctx, cfg, store, a.progressOutput())
	if err != nil {
		return err
	}
	if warnings := verificationWarnings(result); warnings != "" {
		fmt.Fprint(a.progressOutput(), warnings)
	}
	if rows := verificationRows(result); rows != "" {
		fmt.Fprint(a.progressOutput(), rows)
	}
	if result.Converged && result.Complete {
		fmt.Fprint(a.progressOutput(), verificationSummary(result))
	}
	if err := json.NewEncoder(a.output()).Encode(result); err != nil {
		return err
	}
	if cut := result.CutShort(); len(cut) > 0 {
		return fmt.Errorf("%w: %s", errVerificationIncomplete, strings.Join(cut, "; "))
	}
	if !result.Converged {
		return fmt.Errorf("verification diverged: %s", verificationDivergence(result))
	}
	return nil
}

// Sequences advances the target's sequences without cutting over, so the target
// can accept writes while the source still holds the traffic. Cutover runs the
// same step again against the source's final values.
func (a App) Sequences(ctx context.Context, cfg config.Config) error {
	if cfg.SequenceOffset < 0 {
		return errors.New("sequence offset must not be negative")
	}
	store, err := state.OpenControl(ctx, cfg.Dir)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := validateTargetIdentity(ctx, cfg, store); err != nil {
		return err
	}
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	switch migration.Phase {
	case state.PhaseFollow, state.PhaseDrained, state.PhaseCutover, state.PhaseComplete:
	default:
		return fmt.Errorf("sequences requires follow phase or later, current phase is %s", migration.Phase)
	}
	schemaSelection, err := loadSchemaSelection(ctx, store)
	if err != nil {
		return err
	}
	selected := make([]cutover.Sequence, len(schemaSelection.DependentRelations))
	for i, sequence := range schemaSelection.DependentRelations {
		selected[i] = cutover.Sequence{Schema: sequence.Schema, Name: sequence.Name}
	}
	results, err := cutover.SynchronizeSequences(ctx, connector(cfg.Source), connector(cfg.Target), cfg.SequenceOffset, selected)
	if err != nil {
		return err
	}
	return json.NewEncoder(a.output()).Encode(results)
}

func (a App) Cutover(ctx context.Context, cfg config.Config) error {
	store, err := state.OpenControl(ctx, cfg.Dir)
	if err != nil {
		return err
	}
	defer store.Close()
	migration, err := store.Migration(ctx)
	if err != nil {
		return err
	}
	if migration.Phase == state.PhaseComplete {
		cleanupDone, err := store.StepCompleted(ctx, "target.cleanup.completed")
		if err != nil {
			return err
		}
		if !cleanupDone {
			if err := validateTargetIdentity(ctx, cfg, store); err != nil {
				return err
			}
		}
		report, err := cutover.ReadReport(cfg.Dir)
		if err != nil {
			return err
		}
		return json.NewEncoder(a.output()).Encode(report)
	}
	if err := validateTargetIdentity(ctx, cfg, store); err != nil {
		return err
	}
	if migration.Phase != state.PhaseFollow && migration.Phase != state.PhaseDrained &&
		migration.Phase != state.PhaseCutover {
		return fmt.Errorf("cutover requires follow phase, current phase is %s", migration.Phase)
	}
	snapshot, err := readSnapshot(cfg.Dir)
	if err != nil {
		return err
	}
	recovery, err := cdc.Recover(filepath.Join(cfg.Dir, "cdc"))
	if err != nil {
		return err
	}
	if err := setManualEndPosition(ctx, cfg.EndPosition, filepath.Join(cfg.Dir, "cdc"), recovery.DurableLSN, store); err != nil {
		return err
	}
	if err := store.SetTargetCleanupRequested(ctx, !cfg.NoCleanup); err != nil {
		return err
	}
	durableTables, err := store.ListTables(ctx)
	if err != nil {
		return err
	}
	sourceTables := make([]setup.Table, len(durableTables))
	for i, table := range durableTables {
		sourceTables[i] = setup.Table{Schema: table.Schema, Name: table.Name}
	}
	schemaSelection, err := loadSchemaSelection(ctx, store)
	if err != nil {
		return err
	}
	selectedSequences := make([]cutover.Sequence, len(schemaSelection.DependentRelations))
	for i, sequence := range schemaSelection.DependentRelations {
		selectedSequences[i] = cutover.Sequence{Schema: sequence.Schema, Name: sequence.Name}
	}
	// Draining is the run process's work, in another process entirely, so this
	// wait cannot supervise it and has to bound it instead.
	waitDrain := func(ctx context.Context, end string) error {
		return waitTargetProgress(ctx, cfg.Target, snapshot.Slot, end)
	}
	report, err := cutover.Run(ctx, cutover.Config{
		Source: connector(cfg.Source), Target: connector(cfg.Target), State: store, Dir: cfg.Dir,
		WaitDrain:      waitDrain,
		Sequences:      selectedSequences,
		SequenceOffset: cfg.SequenceOffset,
		EmitBoundary: func(ctx context.Context) (string, error) {
			conn, err := postgres.Connect(ctx, cfg.Source)
			if err != nil {
				return "", err
			}
			defer conn.Close(context.Background())
			var lsn string
			err = conn.QueryRow(ctx,
				"SELECT pg_catalog.pg_logical_emit_message(false,$1,$2)::text",
				cdc.CutoverMessagePrefix, migrationID(cfg.Dir)).Scan(&lsn)
			return lsn, err
		},
		Cleanup: func(ctx context.Context) error {
			return cleanupAfterCutover(ctx, cfg, store, snapshot, sourceTables)
		},
		ToolVersion: Version(),
		AuditConfig: map[string]string{"workers": strconv.Itoa(cfg.Workers)},
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(a.output()).Encode(report)
}

// cleanupAfterCutover undoes the temporary changes this migration made to both
// servers. The reverts run before the --no-cleanup check because that flag is
// about retaining replication and metadata for inspection, not about leaving
// either server misconfigured: the target is about to serve production traffic,
// and bulk-load checkpoint settings would give it a far longer crash recovery
// than its operator expects.
func cleanupAfterCutover(ctx context.Context, cfg config.Config, store *state.Store, snapshot setup.Snapshot, tables []setup.Table) error {
	if err := revertTargetTuning(ctx, cfg.Target, store); err != nil {
		return err
	}
	// The target inherited FULL through the schema dump, which was taken after the
	// source was altered. Unlike the source it is not published, so this is safe
	// here and is done regardless of --no-cleanup: the target is about to serve
	// production and should not keep paying for a migration workaround.
	if err := restoreTargetReplicaIdentities(ctx, cfg.Target, store); err != nil {
		return err
	}
	if cfg.NoCleanup {
		// The publication is being retained, and the relations at FULL are in it.
		// Restoring their original identity now would make every UPDATE and
		// DELETE on them fail, so they stay at FULL and the finding stays open
		// naming them, which is what --no-cleanup asks for: leave things standing
		// and say what is still standing.
		return retainReplicaIdentityFallback(ctx, store)
	}
	if err := waitSlotInactive(ctx, cfg.Source, snapshot.Slot); err != nil {
		return err
	}
	// The publication goes first, then the identities. Reverting while the
	// publication still names these relations is the window in which the source
	// rejects production writes.
	if err := setup.CleanupOwned(ctx, cfg.Source, snapshot.Publication, snapshot.Slot,
		tables, snapshot.Failover); err != nil {
		return err
	}
	return restoreReplicaIdentities(ctx, cfg.Source, store)
}

func waitSlotInactive(ctx context.Context, sourceDSN, slot string) error {
	for {
		conn, err := postgres.Connect(ctx, sourceDSN)
		if err != nil {
			return err
		}
		var active bool
		err = conn.QueryRow(ctx,
			"SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name=$1", slot).Scan(&active)
		conn.Close(context.Background())
		if errors.Is(err, pgx.ErrNoRows) || err == nil && !active {
			return nil
		}
		if err != nil {
			return err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
