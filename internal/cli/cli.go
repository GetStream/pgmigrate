// Package cli defines the pgmigrate command-line interface.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/GetStream/pgmigrate/internal/app"
	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/controller"
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
	flags.Int64Var(&cfg.SequenceOffset, "sequence-offset", cfg.SequenceOffset,
		"values each target sequence is set past the source's, leaving the source room to keep allocating")
	flags.DurationVar(&cfg.WALSampleDuration, "wal-sample-duration", cfg.WALSampleDuration, "source WAL-rate sample duration")
	flags.DurationVar(&cfg.SegmentPruneInterval, "segment-prune-interval", cfg.SegmentPruneInterval, "minimum interval between applied CDC segment pruning")
	flags.IntVar(&cfg.ReplayWorkers, "replay-workers", cfg.ReplayWorkers, "parallel target workers for independent transaction components in each durable replay claim")
	flags.Int64Var(&cfg.ReplayBatchBytes, "replay-batch-bytes", cfg.ReplayBatchBytes, "maximum decoded CDC payload covered by one durable replay claim")
	flags.IntVar(&cfg.ReplayBatchChanges, "replay-batch-changes", cfg.ReplayBatchChanges, "maximum row changes covered by one durable replay claim")
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
		newStateCommand("sequences", "Advance target sequences past the source", &cfg, true, application.Sequences),
		newStateCommand("cutover", "Finalize a migration for cutover", &cfg, true, application.Cutover),
		newControllerCommand(&cfg),
		newControllerWorkerCommand(),
	)

	return root
}

func newDatabaseCommand(name, summary string, cfg *config.Config, run func(context.Context, config.Config) error) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: summary,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := validateDatabaseConfig(*cfg); err != nil {
				return err
			}
			return run(cmd.Context(), *cfg)
		},
	}
}

func validateDatabaseConfig(cfg config.Config) error {
	if err := cfg.ValidateConnections(); err != nil {
		return err
	}
	if cfg.TableFilter != "" {
		if _, err := config.LoadFilter(cfg.TableFilter); err != nil {
			return err
		}
	}
	if cfg.Workers < 1 || cfg.RestoreJobs < 1 || cfg.SplitThreshold < 1 ||
		cfg.WALSampleDuration <= 0 || cfg.SegmentPruneInterval <= 0 ||
		cfg.ReplayBatchBytes < 1 || cfg.ReplayBatchChanges < 1 {
		return errors.New("workers, restore-jobs, split-threshold, wal-sample-duration, segment-prune-interval, replay-batch-bytes, and replay-batch-changes must be positive")
	}
	if err := config.ValidateReplayWorkers(cfg.ReplayWorkers); err != nil {
		return err
	}
	if _, err := cfg.TuningOverrides(); err != nil {
		return err
	}
	return cfg.ValidateVerify()
}

func newControllerCommand(cfg *config.Config) *cobra.Command {
	address := controller.DefaultAddress
	token := os.Getenv(controller.TokenEnv)
	command := &cobra.Command{
		Use:   "controller",
		Short: "Serve the migration controller UI",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cfg.ValidateDir(); err != nil {
				return err
			}
			server, err := controller.New(controller.Options{
				Config:  *cfg,
				Address: address,
				Token:   token,
				Out:     cmd.OutOrStdout(),
				Actions: controller.Actions{
					Preflight: controllerWorkerAction("preflight"),
					Run:       controllerWorkerAction("run"),
					Verify:    controllerWorkerAction("verify"),
				},
			})
			if err != nil {
				return err
			}
			return server.Serve(cmd.Context())
		},
	}
	command.Flags().StringVar(&address, "listen", address, "controller listen address")
	command.Flags().StringVar(&token, "token", token, "controller token (or "+controller.TokenEnv+")")
	return command
}

// controllerWorkerAction runs each controller action in a separate process.
// Besides containing panics, this contains fatal runtime failures and ordinary
// non-zero exits so the dashboard can report the error and start a fresh worker
// against the durable migration state. Credentials travel only over the
// child's anonymous stdin pipe; they are never command-line arguments.
func controllerWorkerAction(action string) controller.Action {
	return func(ctx context.Context, cfg config.Config, output io.Writer) error {
		payload, err := json.Marshal(cfg)
		if err != nil {
			return fmt.Errorf("encode %s worker configuration: %w", action, err)
		}
		executable, err := os.Executable()
		if err != nil {
			return fmt.Errorf("locate pgmigrate executable: %w", err)
		}
		command := exec.CommandContext(ctx, executable, "__controller-worker", action)
		command.Stdin = bytes.NewReader(payload)
		command.Stdout = output
		command.Stderr = output
		// Let the worker unwind database and filesystem resources on Stop before
		// CommandContext escalates after WaitDelay.
		command.Cancel = func() error { return command.Process.Signal(syscall.SIGTERM) }
		command.WaitDelay = 10 * time.Second
		if err := command.Run(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%s worker exited: %w", action, err)
		}
		return nil
	}
}

// newControllerWorkerCommand is an internal process boundary, not an operator
// command. It accepts one Config JSON document on stdin so secrets never appear
// in argv or the process environment when they were entered through the UI.
func newControllerWorkerCommand() *cobra.Command {
	return &cobra.Command{
		Use:    "__controller-worker ACTION",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			decoder := json.NewDecoder(io.LimitReader(cmd.InOrStdin(), 1<<20))
			decoder.DisallowUnknownFields()
			var cfg config.Config
			if err := decoder.Decode(&cfg); err != nil {
				return fmt.Errorf("decode controller worker configuration: %w", err)
			}
			var extra any
			if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
				if err == nil {
					return errors.New("controller worker configuration must contain exactly one JSON object")
				}
				return fmt.Errorf("decode controller worker configuration: %w", err)
			}

			application := app.App{Out: cmd.OutOrStdout(), Progress: cmd.OutOrStdout()}
			switch args[0] {
			case "preflight":
				if err := validateDatabaseConfig(cfg); err != nil {
					return err
				}
				return application.Preflight(cmd.Context(), cfg)
			case "run":
				if err := validateDatabaseConfig(cfg); err != nil {
					return err
				}
				return application.Run(cmd.Context(), cfg)
			case "verify":
				if err := cfg.ValidateConnections(); err != nil {
					return err
				}
				if err := cfg.ValidateVerify(); err != nil {
					return err
				}
				return application.Verify(cmd.Context(), cfg)
			default:
				return fmt.Errorf("unsupported controller worker action %q", args[0])
			}
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
