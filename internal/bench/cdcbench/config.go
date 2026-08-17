// Package cdcbench runs a disposable, end-to-end CDC throughput benchmark.
package cdcbench

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"
)

const (
	defaultRows           = 1_048_576
	defaultBacklogUpdates = 1_048_576
	defaultUpdateBatch    = 128
)

// Config controls one benchmark run.
type Config struct {
	PostgresMajor       int
	Rows                int64
	BacklogUpdates      int64
	UpdateBatch         int
	TrafficWorkers      int
	RealtimeUpdates     int
	Warmup              time.Duration
	Duration            time.Duration
	DrainTimeout        time.Duration
	IndexWorkers        int
	MinUpdatesPerSecond float64
	Maintenance         bool
	WorkDir             string
	Output              io.Writer
}

// DefaultConfig returns a benchmark that normally completes in about two minutes.
func DefaultConfig() Config {
	workers := max(2, min(runtime.NumCPU(), 8))
	return Config{
		PostgresMajor:       17,
		Rows:                defaultRows,
		BacklogUpdates:      defaultBacklogUpdates,
		UpdateBatch:         defaultUpdateBatch,
		TrafficWorkers:      workers,
		RealtimeUpdates:     2_000,
		Warmup:              5 * time.Second,
		Duration:            45 * time.Second,
		DrainTimeout:        2 * time.Minute,
		IndexWorkers:        2,
		MinUpdatesPerSecond: 10_000,
		Maintenance:         true,
		Output:              io.Discard,
	}
}

func (c Config) validate() error {
	switch {
	case c.PostgresMajor < 16 || c.PostgresMajor > 18:
		return fmt.Errorf("PostgreSQL major must be between 16 and 18")
	case c.Rows < int64(c.UpdateBatch):
		return errors.New("rows must be at least one update batch")
	case c.UpdateBatch < 1:
		return errors.New("update batch must be positive")
	case c.Rows%int64(c.UpdateBatch) != 0:
		return errors.New("rows must be divisible by update batch")
	case c.BacklogUpdates < int64(c.UpdateBatch):
		return errors.New("backlog updates must be at least one update batch")
	case c.BacklogUpdates%int64(c.UpdateBatch) != 0:
		return errors.New("backlog updates must be divisible by update batch")
	case c.TrafficWorkers < 1:
		return errors.New("traffic workers must be positive")
	case c.RealtimeUpdates < 0:
		return errors.New("realtime updates must not be negative")
	case c.RealtimeUpdates > 0 && c.RealtimeUpdates < c.UpdateBatch:
		return errors.New("realtime updates must be zero or at least one update batch per second")
	case c.Warmup < 0:
		return errors.New("warmup must not be negative")
	case c.Duration < time.Second:
		return errors.New("duration must be at least one second")
	case c.DrainTimeout < time.Second:
		return errors.New("drain timeout must be at least one second")
	case c.IndexWorkers < 1:
		return errors.New("index workers must be positive")
	case c.MinUpdatesPerSecond < 0:
		return errors.New("minimum updates per second must not be negative")
	}
	return nil
}

// Bucket is one five-second throughput sample.
type Bucket struct {
	StartedAt         time.Duration `json:"started_at"`
	Duration          time.Duration `json:"duration"`
	AppliedUpdates    int64         `json:"applied_updates"`
	UpdatesPerSecond  float64       `json:"updates_per_second"`
	MaintenanceActive bool          `json:"maintenance_active"`
}

// Result is the machine-readable outcome of one benchmark.
type Result struct {
	PostgresMajor         int           `json:"postgres_major"`
	Rows                  int64         `json:"rows"`
	UpdateBatch           int           `json:"update_batch"`
	TrafficWorkers        int           `json:"traffic_workers"`
	RequestedRealtimeRate int           `json:"requested_realtime_updates_per_second"`
	Warmup                time.Duration `json:"warmup"`
	Duration              time.Duration `json:"duration"`
	IndexWorkers          int           `json:"index_workers"`
	Maintenance           bool          `json:"maintenance"`
	MinimumApplyRate      float64       `json:"minimum_apply_updates_per_second"`

	BacklogUpdates           int64   `json:"backlog_updates"`
	MeasuredSourceUpdates    int64   `json:"measured_source_updates"`
	MeasuredAppliedUpdates   int64   `json:"measured_applied_updates"`
	SourceUpdatesPerSecond   float64 `json:"source_updates_per_second"`
	AppliedUpdatesPerSecond  float64 `json:"applied_updates_per_second"`
	NetDrainUpdatesPerSecond float64 `json:"net_drain_updates_per_second"`

	OverlapAppliedUpdates   int64         `json:"overlap_applied_updates"`
	OverlapSourceUpdates    int64         `json:"overlap_source_updates"`
	OverlapDuration         time.Duration `json:"overlap_duration"`
	OverlapUpdatesPerSecond float64       `json:"overlap_updates_per_second"`
	OverlapSourcePerSecond  float64       `json:"overlap_source_updates_per_second"`
	BacklogAtOverlapEnd     bool          `json:"backlog_at_overlap_end"`
	MaintenanceDuration     time.Duration `json:"maintenance_duration"`
	IndexDuration           time.Duration `json:"index_duration"`
	VacuumDuration          time.Duration `json:"vacuum_duration"`

	TotalGeneratedUpdates int64         `json:"total_generated_updates"`
	TotalAppliedUpdates   int64         `json:"total_applied_updates"`
	DrainDuration         time.Duration `json:"drain_duration"`
	Buckets               []Bucket      `json:"buckets"`
}
