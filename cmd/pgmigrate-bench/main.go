package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GetStream/pgmigrate/internal/bench/cdcbench"
)

func main() {
	cfg := cdcbench.DefaultConfig()
	var jsonOutput bool
	flag.IntVar(&cfg.PostgresMajor, "postgres-major", cfg.PostgresMajor, "PostgreSQL major version")
	flag.Int64Var(&cfg.Rows, "rows", cfg.Rows, "rows in each benchmark table")
	flag.Int64Var(&cfg.BacklogUpdates, "backlog-updates", cfg.BacklogUpdates, "updates staged before replay starts")
	flag.IntVar(&cfg.UpdateBatch, "update-batch", cfg.UpdateBatch, "row updates per source transaction")
	flag.IntVar(&cfg.TrafficWorkers, "traffic-workers", cfg.TrafficWorkers, "source traffic connections")
	flag.IntVar(&cfg.RealtimeUpdates, "realtime-updates", cfg.RealtimeUpdates, "continued source updates per second")
	flag.DurationVar(&cfg.Warmup, "warmup", cfg.Warmup, "replay warmup before measurement")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "measurement duration")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "maximum final CDC drain time")
	flag.IntVar(&cfg.IndexWorkers, "index-workers", cfg.IndexWorkers, "concurrent index workers")
	flag.Float64Var(
		&cfg.MinUpdatesPerSecond,
		"min-updates-per-second",
		cfg.MinUpdatesPerSecond,
		"minimum apply rate during maintenance; zero disables the gate",
	)
	flag.BoolVar(&cfg.Maintenance, "maintenance", cfg.Maintenance, "build indexes and vacuum during replay")
	flag.StringVar(&cfg.WorkDir, "work-dir", cfg.WorkDir, "preserved CDC work directory; empty uses a temporary directory")
	flag.BoolVar(&jsonOutput, "json", false, "write final result as JSON")
	flag.Parse()

	cfg.Output = os.Stdout
	if jsonOutput {
		cfg.Output = os.Stderr
	}
	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stopSignals()
	timeout := cfg.Warmup + cfg.Duration + cfg.DrainTimeout + 5*time.Minute
	ctx, cancel := context.WithTimeout(signalCtx, timeout)
	defer cancel()
	result, err := cdcbench.Run(ctx, cfg)
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if encodeErr := encoder.Encode(result); encodeErr != nil {
			fmt.Fprintln(os.Stderr, encodeErr)
			os.Exit(1)
		}
	} else {
		fmt.Printf(
			"[cdc-bench] result overlap=%.0f updates/s overall=%.0f updates/s source=%.0f updates/s "+
				"net_drain=%.0f updates/s maintenance=%s drain=%s\n",
			result.OverlapUpdatesPerSecond,
			result.AppliedUpdatesPerSecond,
			result.SourceUpdatesPerSecond,
			result.NetDrainUpdatesPerSecond,
			result.MaintenanceDuration.Round(time.Millisecond),
			result.DrainDuration.Round(time.Millisecond),
		)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "[cdc-bench]", err)
		os.Exit(1)
	}
}
