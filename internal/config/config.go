// Package config defines command configuration and table filtering.
package config

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/GetStream/pgmigrate/internal/tuning"
)

const (
	SourceEnv = "PGMIGRATE_SOURCE"
	TargetEnv = "PGMIGRATE_TARGET"
)

// Config contains configuration shared by pgmigrate commands.
type Config struct {
	Source                 string
	Target                 string
	Dir                    string
	TableFilter            string
	AckWarnings            bool
	AllowCollationChange   bool
	Workers                int
	SplitThreshold         int64
	RestoreJobs            int
	PGDumpPath             string
	PGRestorePath          string
	Metrics                string
	StatusJSON             bool
	StatusWatch            time.Duration
	NoCleanup              bool
	EndPosition            string
	SequenceOffset         int64
	WALSampleDuration      time.Duration
	SegmentPruneInterval   time.Duration
	RetryBaseCopy          bool
	SkipTargetTuning       bool
	WarnOnTuningErrors     bool
	TargetMemory           string
	MaintenanceWorkMem     string
	MaxParallelMaintenance int
	MaxWALSize             string
	CheckpointTimeout      string

	// Verification has its own worker and pacing settings, separate from
	// --workers. Copy and index build run against an idle target; verification
	// reads the live source, where the same parallelism is a load problem.
	VerifyWorkers         int
	VerifySampleRows      int64
	VerifySampleWindows   int64
	VerifyBatchRows       int64
	VerifyDutyCycle       float64
	VerifyTableTimeout    time.Duration
	VerifyConvergeTimeout time.Duration
	VerifyCDCRows         int64

	// CDCSampleRows bounds the reservoir of applied keys the run keeps per
	// relation, which is what lets verification check the replication path at
	// all. Zero disables the reservoir, and verification then has nothing to
	// check it with.
	CDCSampleRows int64
}

// ValidateVerify validates the verification settings.
func (c Config) ValidateVerify() error {
	switch {
	case c.VerifyWorkers < 1:
		return errors.New("verify-workers must be positive")
	case c.VerifySampleRows < 1:
		// Zero is rejected rather than read as "read everything". The target side
		// is index lookups, so an exhaustive run would be one random probe per row
		// and slower than the sequential comparison it replaced.
		return errors.New("verify-sample-rows must be at least 1; verification samples and has no exhaustive mode")
	case c.VerifySampleWindows < 1:
		return errors.New("verify-sample-windows must be at least 1")
	case c.VerifyBatchRows < 1:
		return errors.New("verify-batch-rows must be at least 1")
	case c.VerifyDutyCycle <= 0 || c.VerifyDutyCycle > 1:
		return errors.New("verify-duty-cycle must be greater than 0 and at most 1")
	case c.VerifyTableTimeout < 0:
		return errors.New("verify-table-timeout must not be negative (0 disables the timeout)")
	case c.VerifyConvergeTimeout < 0:
		return errors.New("verify-converge-timeout must not be negative")
	case c.VerifyCDCRows < 0:
		return errors.New("verify-cdc-rows must not be negative (0 falls back to the default)")
	}
	return nil
}

// TuningOverrides returns the operator-supplied target tuning values, validated.
// An empty or zero field means the value is derived from the target instead.
func (c Config) TuningOverrides() (tuning.Overrides, error) {
	overrides := tuning.Overrides{
		MaintenanceWorkMem:            strings.TrimSpace(c.MaintenanceWorkMem),
		MaxParallelMaintenanceWorkers: c.MaxParallelMaintenance,
		MaxWALSize:                    strings.TrimSpace(c.MaxWALSize),
		CheckpointTimeout:             strings.TrimSpace(c.CheckpointTimeout),
		TargetMemory:                  strings.TrimSpace(c.TargetMemory),
	}
	if err := overrides.Validate(); err != nil {
		return tuning.Overrides{}, err
	}
	return overrides, nil
}

// FromEnvironment returns configuration populated from supported environment
// variables. Command-line flags may overwrite these values.
func FromEnvironment() Config {
	return Config{
		Source:               os.Getenv(SourceEnv),
		Target:               os.Getenv(TargetEnv),
		Workers:              max(1, runtime.NumCPU()),
		SplitThreshold:       1 << 30,
		RestoreJobs:          max(1, runtime.NumCPU()/2),
		WALSampleDuration:    time.Minute,
		SegmentPruneInterval: time.Minute,
		SequenceOffset:       1_000_000,

		VerifyWorkers:         1,
		VerifySampleRows:      1_000_000,
		VerifySampleWindows:   128,
		VerifyBatchRows:       5000,
		VerifyDutyCycle:       1,
		VerifyTableTimeout:    20 * time.Minute,
		VerifyConvergeTimeout: time.Minute,
		VerifyCDCRows:         100_000,

		CDCSampleRows: 100_000,
	}
}

// ValidateConnections validates configuration for commands that connect to
// both PostgreSQL databases.
func (c Config) ValidateConnections() error {
	var missing []string
	if strings.TrimSpace(c.Source) == "" {
		missing = append(missing, "--source or "+SourceEnv)
	}
	if strings.TrimSpace(c.Target) == "" {
		missing = append(missing, "--target or "+TargetEnv)
	}
	if err := c.ValidateDir(); err != nil {
		missing = append(missing, "--dir")
	}
	if len(missing) != 0 {
		return fmt.Errorf("missing required configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

// ValidateDir validates configuration for commands that only read migration
// state.
func (c Config) ValidateDir() error {
	if strings.TrimSpace(c.Dir) == "" {
		return errors.New("migration directory is required")
	}
	return nil
}
