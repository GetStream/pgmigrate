package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/GetStream/pgmigrate/internal/config"
)

const (
	controllerConfigurationFile    = "controller-config.json"
	controllerConfigurationVersion = 1
	controllerConfigurationMaxSize = 1 << 20
)

// persistedConfiguration deliberately contains only the non-secret settings
// editable in the controller UI. Source and target DSNs, the controller token,
// the state directory, and CLI-only runtime controls can therefore never be
// serialized by this code path.
type persistedConfiguration struct {
	Version                int           `json:"version"`
	TableFilter            string        `json:"table_filter"`
	AckWarnings            bool          `json:"ack_warnings"`
	AllowCollationChange   bool          `json:"allow_collation_change"`
	Workers                int           `json:"workers"`
	SplitThreshold         int64         `json:"split_threshold"`
	RestoreJobs            int           `json:"restore_jobs"`
	PGDumpPath             string        `json:"pg_dump_path"`
	PGRestorePath          string        `json:"pg_restore_path"`
	Metrics                string        `json:"metrics"`
	WALSampleDuration      time.Duration `json:"wal_sample_duration"`
	SegmentPruneInterval   time.Duration `json:"segment_prune_interval"`
	ReplayWorkers          int           `json:"replay_workers"`
	ReplayBatchBytes       int64         `json:"replay_batch_bytes"`
	ReplayBatchChanges     int           `json:"replay_batch_changes"`
	RetryBaseCopy          bool          `json:"retry_base_copy"`
	SkipTargetTuning       bool          `json:"skip_target_tuning"`
	WarnOnTuningErrors     bool          `json:"warn_on_tuning_errors"`
	TargetMemory           string        `json:"target_memory"`
	MaintenanceWorkMem     string        `json:"maintenance_work_mem"`
	MaxParallelMaintenance int           `json:"max_parallel_maintenance_workers"`
	MaxWALSize             string        `json:"max_wal_size"`
	CheckpointTimeout      string        `json:"checkpoint_timeout"`
	VerifyWorkers          int           `json:"verify_workers"`
	VerifySampleRows       int64         `json:"verify_sample_rows"`
	VerifySampleWindows    int64         `json:"verify_sample_windows"`
	VerifyBatchRows        int64         `json:"verify_batch_rows"`
	VerifyDutyCycle        float64       `json:"verify_duty_cycle"`
	VerifyTableTimeout     time.Duration `json:"verify_table_timeout"`
	VerifyConvergeTimeout  time.Duration `json:"verify_converge_timeout"`
	VerifyCDCRows          int64         `json:"verify_cdc_rows"`
	VerifyIgnoreApps       string        `json:"verify_ignore_apps,omitempty"`
	CDCSampleRows          int64         `json:"cdc_sample_rows"`
}

func persistedConfigurationFrom(cfg config.Config) persistedConfiguration {
	return persistedConfiguration{
		Version:     controllerConfigurationVersion,
		TableFilter: cfg.TableFilter, AckWarnings: cfg.AckWarnings,
		AllowCollationChange: cfg.AllowCollationChange,
		Workers:              cfg.Workers, SplitThreshold: cfg.SplitThreshold, RestoreJobs: cfg.RestoreJobs,
		PGDumpPath: cfg.PGDumpPath, PGRestorePath: cfg.PGRestorePath, Metrics: cfg.Metrics,
		WALSampleDuration: cfg.WALSampleDuration, SegmentPruneInterval: cfg.SegmentPruneInterval,
		ReplayWorkers: cfg.ReplayWorkers, ReplayBatchBytes: cfg.ReplayBatchBytes,
		ReplayBatchChanges: cfg.ReplayBatchChanges, RetryBaseCopy: cfg.RetryBaseCopy,
		SkipTargetTuning: cfg.SkipTargetTuning, WarnOnTuningErrors: cfg.WarnOnTuningErrors,
		TargetMemory: cfg.TargetMemory, MaintenanceWorkMem: cfg.MaintenanceWorkMem,
		MaxParallelMaintenance: cfg.MaxParallelMaintenance, MaxWALSize: cfg.MaxWALSize,
		CheckpointTimeout: cfg.CheckpointTimeout,
		VerifyWorkers:     cfg.VerifyWorkers, VerifySampleRows: cfg.VerifySampleRows,
		VerifySampleWindows: cfg.VerifySampleWindows, VerifyBatchRows: cfg.VerifyBatchRows,
		VerifyDutyCycle: cfg.VerifyDutyCycle, VerifyTableTimeout: cfg.VerifyTableTimeout,
		VerifyConvergeTimeout: cfg.VerifyConvergeTimeout, VerifyCDCRows: cfg.VerifyCDCRows,
		VerifyIgnoreApps: cfg.VerifyIgnoreApps,
		CDCSampleRows:    cfg.CDCSampleRows,
	}
}

func (persisted persistedConfiguration) merge(base config.Config) config.Config {
	base.TableFilter = persisted.TableFilter
	base.AckWarnings = persisted.AckWarnings
	base.AllowCollationChange = persisted.AllowCollationChange
	base.Workers = persisted.Workers
	base.SplitThreshold = persisted.SplitThreshold
	base.RestoreJobs = persisted.RestoreJobs
	base.PGDumpPath = persisted.PGDumpPath
	base.PGRestorePath = persisted.PGRestorePath
	base.Metrics = persisted.Metrics
	base.WALSampleDuration = persisted.WALSampleDuration
	base.SegmentPruneInterval = persisted.SegmentPruneInterval
	base.ReplayWorkers = persisted.ReplayWorkers
	base.ReplayBatchBytes = persisted.ReplayBatchBytes
	base.ReplayBatchChanges = persisted.ReplayBatchChanges
	base.RetryBaseCopy = persisted.RetryBaseCopy
	base.SkipTargetTuning = persisted.SkipTargetTuning
	base.WarnOnTuningErrors = persisted.WarnOnTuningErrors
	base.TargetMemory = persisted.TargetMemory
	base.MaintenanceWorkMem = persisted.MaintenanceWorkMem
	base.MaxParallelMaintenance = persisted.MaxParallelMaintenance
	base.MaxWALSize = persisted.MaxWALSize
	base.CheckpointTimeout = persisted.CheckpointTimeout
	base.VerifyWorkers = persisted.VerifyWorkers
	base.VerifySampleRows = persisted.VerifySampleRows
	base.VerifySampleWindows = persisted.VerifySampleWindows
	base.VerifyBatchRows = persisted.VerifyBatchRows
	base.VerifyDutyCycle = persisted.VerifyDutyCycle
	base.VerifyTableTimeout = persisted.VerifyTableTimeout
	base.VerifyConvergeTimeout = persisted.VerifyConvergeTimeout
	base.VerifyCDCRows = persisted.VerifyCDCRows
	base.VerifyIgnoreApps = persisted.VerifyIgnoreApps
	base.CDCSampleRows = persisted.CDCSampleRows
	return base
}

func loadControllerConfiguration(base config.Config) (config.Config, error) {
	path := filepath.Join(base.Dir, controllerConfigurationFile)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return base, nil
	}
	if err != nil {
		return config.Config{}, fmt.Errorf("open persisted controller configuration: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return config.Config{}, fmt.Errorf("inspect persisted controller configuration: %w", err)
	}
	if info.Size() > controllerConfigurationMaxSize {
		return config.Config{}, fmt.Errorf(
			"persisted controller configuration exceeds %d bytes", controllerConfigurationMaxSize,
		)
	}

	decoder := json.NewDecoder(io.LimitReader(file, controllerConfigurationMaxSize+1))
	decoder.DisallowUnknownFields()
	var persisted persistedConfiguration
	if err := decoder.Decode(&persisted); err != nil {
		return config.Config{}, fmt.Errorf("decode persisted controller configuration: %w", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return config.Config{}, fmt.Errorf("decode persisted controller configuration: %w", err)
	}
	if persisted.Version != controllerConfigurationVersion {
		return config.Config{}, fmt.Errorf(
			"unsupported persisted controller configuration version %d", persisted.Version,
		)
	}

	candidate := persisted.merge(base)
	validation := candidate
	if strings.TrimSpace(validation.Source) == "" {
		validation.Source = "persisted-controller-source"
	}
	if strings.TrimSpace(validation.Target) == "" {
		validation.Target = "persisted-controller-target"
	}
	if err := validateConfiguration(validation); err != nil {
		return config.Config{}, fmt.Errorf("validate persisted controller configuration: %w", err)
	}
	return candidate, nil
}

func saveControllerConfiguration(path string, cfg config.Config) error {
	persisted := persistedConfigurationFrom(cfg)
	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return fmt.Errorf("encode controller configuration: %w", err)
	}
	data = append(data, '\n')

	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create controller configuration directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".controller-config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary controller configuration: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := true
	defer func() {
		_ = temporary.Close()
		if cleanup {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("secure temporary controller configuration: %w", err)
	}
	written, err := temporary.Write(data)
	if err != nil {
		return fmt.Errorf("write temporary controller configuration: %w", err)
	}
	if written != len(data) {
		return fmt.Errorf("write temporary controller configuration: %w", io.ErrShortWrite)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary controller configuration: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary controller configuration: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace controller configuration: %w", err)
	}
	cleanup = false

	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open controller configuration directory for sync: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync controller configuration directory: %w", err)
	}
	return nil
}
