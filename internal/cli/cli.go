// Package cli defines the pgmigrate command-line interface.
package cli

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/GetStream/pgmigrate/internal/app"
	"github.com/GetStream/pgmigrate/internal/config"
)

// Execute runs the root command.
func Execute() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return NewRootCommand().ExecuteContext(ctx)
}

// NewRootCommand constructs a new command tree.
func NewRootCommand() *cobra.Command {
	cfg := config.FromEnvironment()

	root := &cobra.Command{
		Use:           "pgmigrate",
		Short:         "Migrate a live PostgreSQL database",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	flags := root.PersistentFlags()
	flags.StringVar(&cfg.Source, "source", cfg.Source, "source PostgreSQL connection string (or PGMIGRATE_SOURCE)")
	flags.StringVar(&cfg.Target, "target", cfg.Target, "target PostgreSQL connection string (or PGMIGRATE_TARGET)")
	flags.StringVar(&cfg.Dir, "dir", "", "migration state directory")
	flags.StringVar(&cfg.TableFilter, "table-filter", "", "path to newline-delimited schema.table globs")
	flags.BoolVar(&cfg.AckWarnings, "ack-warnings", false, "acknowledge all preflight warnings")
	flags.BoolVar(&cfg.AllowCollationChange, "allow-collation-change", false,
		"migrate to a target that collates text differently from the source")
	flags.IntVar(&cfg.Workers, "workers", cfg.Workers, "parallel copy and index-build workers (verification has --verify-workers)")
	flags.Int64Var(&cfg.SplitThreshold, "split-threshold", cfg.SplitThreshold, "table bytes per copy part")
	flags.IntVar(&cfg.RestoreJobs, "restore-jobs", cfg.RestoreJobs, "parallel pg_restore jobs")
	flags.StringVar(&cfg.PGDumpPath, "pg-dump", "", "pg_dump executable path")
	flags.StringVar(&cfg.PGRestorePath, "pg-restore", "", "pg_restore executable path")
	flags.StringVar(&cfg.Metrics, "metrics", "", "Prometheus listen address (for example :9090)")
	flags.BoolVar(&cfg.StatusJSON, "json", false, "render machine-readable JSON")
	flags.DurationVar(&cfg.StatusWatch, "watch", 0, "refresh status at this interval")
	flags.BoolVar(&cfg.NoCleanup, "no-cleanup", false, "retain replication and target metadata")
	flags.StringVar(&cfg.EndPosition, "endpos", "", "explicit cutover end LSN")
	flags.DurationVar(&cfg.WALSampleDuration, "wal-sample-duration", cfg.WALSampleDuration, "source WAL-rate sample duration")
	flags.DurationVar(&cfg.SegmentPruneInterval, "segment-prune-interval", cfg.SegmentPruneInterval, "minimum interval between applied CDC segment pruning")
	flags.BoolVar(&cfg.RetryBaseCopy, "retry-base-copy", false, "restart the base copy even though the last attempts failed the same way")
	flags.BoolVar(&cfg.SkipTargetTuning, "skip-target-tuning", false, "leave target settings alone during the bulk load")
	flags.BoolVar(&cfg.WarnOnTuningErrors, "warn-on-tuning-errors", false, "continue when a target setting cannot be tuned instead of stopping")
	flags.StringVar(&cfg.TargetMemory, "target-memory", "", "target memory for sizing tuning (for example 64GB); estimated from shared_buffers when unset")
	flags.StringVar(&cfg.MaintenanceWorkMem, "maintenance-work-mem", "", "maintenance_work_mem per index-build session; derived when unset")
	flags.IntVar(&cfg.MaxParallelMaintenance, "max-parallel-maintenance-workers", 0, "max_parallel_maintenance_workers per index-build session; derived when unset")
	flags.StringVar(&cfg.MaxWALSize, "max-wal-size", "", "max_wal_size during the bulk load; derived when unset")
	flags.StringVar(&cfg.CheckpointTimeout, "checkpoint-timeout", "", "checkpoint_timeout during the bulk load; derived when unset")
	flags.IntVar(&cfg.VerifyWorkers, "verify-workers", cfg.VerifyWorkers, "tables verified in parallel; each one reads the live source")
	flags.Int64Var(&cfg.VerifySampleRows, "verify-sample-rows", cfg.VerifySampleRows, "rows per table read from the source and checked against the target")
	flags.Int64Var(&cfg.VerifySampleWindows, "verify-sample-windows", cfg.VerifySampleWindows, "page intervals those rows are drawn from, spread across the heap")
	flags.Int64Var(&cfg.VerifyBatchRows, "verify-batch-rows", cfg.VerifyBatchRows, "keys per target lookup statement")
	flags.Float64Var(&cfg.VerifyDutyCycle, "verify-duty-cycle", cfg.VerifyDutyCycle, "fraction of the time verification may spend querying, sleeping between windows to stay under it")
	flags.DurationVar(&cfg.VerifyTableTimeout, "verify-table-timeout", cfg.VerifyTableTimeout, "time one table's check may take (0 disables)")
	flags.DurationVar(&cfg.VerifyConvergeTimeout, "verify-converge-timeout", cfg.VerifyConvergeTimeout, "how long a differing row is given to settle before it is reported")
	flags.Int64Var(&cfg.VerifyCDCRows, "verify-cdc-rows", cfg.VerifyCDCRows, "applier-recorded keys per table checked alongside the heap sample")
	flags.Int64Var(&cfg.CDCSampleRows, "cdc-sample-rows", cfg.CDCSampleRows, "applied keys kept per relation for verification to check the replication path (0 records none)")

	application := app.App{Out: root.OutOrStdout()}
	root.AddCommand(
		newDatabaseCommand("preflight", "Check whether a migration can succeed", &cfg, application.Preflight),
		newDatabaseCommand("run", "Start or resume a migration", &cfg, application.Run),
		newStateCommand("status", "Show migration progress", &cfg, false, application.Status),
		newStateCommand("verify", "Verify source and target data", &cfg, true, application.Verify),
		newStateCommand("cutover", "Finalize a migration for cutover", &cfg, true, application.Cutover),
	)

	return root
}

func newDatabaseCommand(name, summary string, cfg *config.Config, run func(context.Context, config.Config) error) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: summary,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cfg.ValidateConnections(); err != nil {
				return err
			}
			if cfg.TableFilter != "" {
				if _, err := config.LoadFilter(cfg.TableFilter); err != nil {
					return err
				}
			}
			if cfg.Workers < 1 || cfg.RestoreJobs < 1 || cfg.SplitThreshold < 1 ||
				cfg.WALSampleDuration <= 0 || cfg.SegmentPruneInterval <= 0 {
				return errors.New("workers, restore-jobs, split-threshold, wal-sample-duration, and segment-prune-interval must be positive")
			}
			if _, err := cfg.TuningOverrides(); err != nil {
				return err
			}
			if err := cfg.ValidateVerify(); err != nil {
				return err
			}
			return run(cmd.Context(), *cfg)
		},
	}
}

func newStateCommand(name, summary string, cfg *config.Config, connections bool, run func(context.Context, config.Config) error) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: summary,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var err error
			if connections {
				err = cfg.ValidateConnections()
			} else {
				err = cfg.ValidateDir()
			}
			if err != nil {
				return err
			}
			if cfg.StatusWatch < 0 || cfg.StatusWatch > 0 && cfg.StatusWatch < 10*time.Millisecond {
				return errors.New("watch interval must be zero or at least 10ms")
			}
			if connections {
				if err := cfg.ValidateVerify(); err != nil {
					return err
				}
			}
			return run(cmd.Context(), *cfg)
		},
	}
}
