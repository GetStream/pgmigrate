// Package controller serves the optional pgmigrate web controller.
package controller

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/observe"
	"github.com/GetStream/pgmigrate/internal/state"
)

const (
	// DefaultAddress keeps the controller off the network unless an operator
	// deliberately chooses a different address and supplies a token.
	DefaultAddress = "127.0.0.1:9188"
	// TokenEnv is the environment variable read by the CLI for controller auth.
	TokenEnv    = "PGMIGRATE_CONTROLLER_TOKEN"
	outputLimit = 64 << 10
)

//go:embed ui.html
var assets embed.FS

// Action is one operation the controller may supervise.
type Action func(context.Context, config.Config, io.Writer) error

// Actions are the deliberately limited operations exposed by the controller.
// Cutover and sequence advancement are intentionally absent.
type Actions struct {
	Preflight Action
	Run       Action
	Verify    Action
}

// Options configures a controller Server.
type Options struct {
	Config  config.Config
	Address string
	Token   string
	Out     io.Writer
	Actions Actions
}

// Server serves status and supervises migration and verification operations.
type Server struct {
	cfg     config.Config
	address string
	token   string
	out     io.Writer
	actions Actions

	ctx    context.Context
	cancel context.CancelFunc

	mu               sync.Mutex
	operations       map[string]operation
	nextID           int64
	configGeneration string
	configRevision   uint64
}

type operation struct {
	ID         int64
	Name       string
	State      string
	StartedAt  time.Time
	FinishedAt time.Time
	Error      string
	Cancel     context.CancelFunc
	Output     *tailBuffer
}

type operationView struct {
	ID         int64      `json:"id,omitempty"`
	Name       string     `json:"name,omitempty"`
	State      string     `json:"state"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
	Output     string     `json:"output,omitempty"`
}

type findingView struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Severity   string    `json:"severity"`
	Message    string    `json:"message"`
	ObservedAt time.Time `json:"observed_at"`
}

type failureView struct {
	Phase       state.Phase `json:"phase"`
	Signature   string      `json:"signature"`
	Detail      string      `json:"detail"`
	Consecutive int         `json:"consecutive"`
	ObservedAt  time.Time   `json:"observed_at"`
}

type copyView struct {
	Rows     int64         `json:"rows"`
	Bytes    int64         `json:"bytes"`
	Duration time.Duration `json:"duration"`
}

// configurationView is the mutable controller configuration exposed to the
// dashboard. Database credentials are deliberately represented only by
// configured flags; their values are write-only through configurationUpdate.
type configurationView struct {
	Revision               string  `json:"revision"`
	SourceConfigured       bool    `json:"source_configured"`
	TargetConfigured       bool    `json:"target_configured"`
	TableFilter            string  `json:"table_filter"`
	AckWarnings            bool    `json:"ack_warnings"`
	AllowCollationChange   bool    `json:"allow_collation_change"`
	Workers                int     `json:"workers"`
	SplitThreshold         int64   `json:"split_threshold"`
	RestoreJobs            int     `json:"restore_jobs"`
	PGDumpPath             string  `json:"pg_dump_path"`
	PGRestorePath          string  `json:"pg_restore_path"`
	Metrics                string  `json:"metrics"`
	WALSampleDuration      string  `json:"wal_sample_duration"`
	SegmentPruneInterval   string  `json:"segment_prune_interval"`
	RetryBaseCopy          bool    `json:"retry_base_copy"`
	SkipTargetTuning       bool    `json:"skip_target_tuning"`
	WarnOnTuningErrors     bool    `json:"warn_on_tuning_errors"`
	TargetMemory           string  `json:"target_memory"`
	MaintenanceWorkMem     string  `json:"maintenance_work_mem"`
	MaxParallelMaintenance int     `json:"max_parallel_maintenance_workers"`
	MaxWALSize             string  `json:"max_wal_size"`
	CheckpointTimeout      string  `json:"checkpoint_timeout"`
	VerifyWorkers          int     `json:"verify_workers"`
	VerifySampleRows       int64   `json:"verify_sample_rows"`
	VerifySampleWindows    int64   `json:"verify_sample_windows"`
	VerifyBatchRows        int64   `json:"verify_batch_rows"`
	VerifyDutyCycle        float64 `json:"verify_duty_cycle"`
	VerifyTableTimeout     string  `json:"verify_table_timeout"`
	VerifyConvergeTimeout  string  `json:"verify_converge_timeout"`
	VerifyCDCRows          int64   `json:"verify_cdc_rows"`
	CDCSampleRows          int64   `json:"cdc_sample_rows"`
}

// configurationUpdate uses pointers so callers can change a subset of the
// non-secret settings without resetting process defaults. Blank credentials
// intentionally retain the currently configured value.
type configurationUpdate struct {
	Source                 *string  `json:"source"`
	Target                 *string  `json:"target"`
	TableFilter            *string  `json:"table_filter"`
	AckWarnings            *bool    `json:"ack_warnings"`
	AllowCollationChange   *bool    `json:"allow_collation_change"`
	Workers                *int     `json:"workers"`
	SplitThreshold         *int64   `json:"split_threshold"`
	RestoreJobs            *int     `json:"restore_jobs"`
	PGDumpPath             *string  `json:"pg_dump_path"`
	PGRestorePath          *string  `json:"pg_restore_path"`
	Metrics                *string  `json:"metrics"`
	WALSampleDuration      *string  `json:"wal_sample_duration"`
	SegmentPruneInterval   *string  `json:"segment_prune_interval"`
	RetryBaseCopy          *bool    `json:"retry_base_copy"`
	SkipTargetTuning       *bool    `json:"skip_target_tuning"`
	WarnOnTuningErrors     *bool    `json:"warn_on_tuning_errors"`
	TargetMemory           *string  `json:"target_memory"`
	MaintenanceWorkMem     *string  `json:"maintenance_work_mem"`
	MaxParallelMaintenance *int     `json:"max_parallel_maintenance_workers"`
	MaxWALSize             *string  `json:"max_wal_size"`
	CheckpointTimeout      *string  `json:"checkpoint_timeout"`
	VerifyWorkers          *int     `json:"verify_workers"`
	VerifySampleRows       *int64   `json:"verify_sample_rows"`
	VerifySampleWindows    *int64   `json:"verify_sample_windows"`
	VerifyBatchRows        *int64   `json:"verify_batch_rows"`
	VerifyDutyCycle        *float64 `json:"verify_duty_cycle"`
	VerifyTableTimeout     *string  `json:"verify_table_timeout"`
	VerifyConvergeTimeout  *string  `json:"verify_converge_timeout"`
	VerifyCDCRows          *int64   `json:"verify_cdc_rows"`
	CDCSampleRows          *int64   `json:"cdc_sample_rows"`
}

type statusResponse struct {
	Snapshot              *observe.Snapshot        `json:"snapshot,omitempty"`
	Copy                  copyView                 `json:"copy"`
	Findings              []findingView            `json:"findings,omitempty"`
	Failure               *failureView             `json:"failure,omitempty"`
	Operations            map[string]operationView `json:"operations"`
	ConnectionsConfigured bool                     `json:"connections_configured"`
	TokenRequired         bool                     `json:"token_required"`
}

// New validates options and constructs a controller.
func New(options Options) (*Server, error) {
	address := strings.TrimSpace(options.Address)
	if address == "" {
		address = DefaultAddress
	}
	if err := validateAddress(address, strings.TrimSpace(options.Token)); err != nil {
		return nil, err
	}
	if err := options.Config.ValidateDir(); err != nil {
		return nil, err
	}
	if options.Actions.Preflight == nil || options.Actions.Run == nil || options.Actions.Verify == nil {
		return nil, errors.New("preflight, run, and verify controller actions are required")
	}
	configGeneration, err := newConfigurationGeneration()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := options.Out
	if out == nil {
		out = io.Discard
	}
	return &Server{
		cfg: options.Config, address: address, token: strings.TrimSpace(options.Token),
		out: out, actions: options.Actions, ctx: ctx, cancel: cancel,
		operations: map[string]operation{
			"migration":    {State: "idle"},
			"verification": {State: "idle"},
		},
		configGeneration: configGeneration,
		configRevision:   1,
	}, nil
}

func newConfigurationGeneration() (string, error) {
	var generation [16]byte
	if _, err := rand.Read(generation[:]); err != nil {
		return "", fmt.Errorf("generate controller configuration generation: %w", err)
	}
	return hex.EncodeToString(generation[:]), nil
}

func validateAddress(address, token string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("parse controller listen address: %w", err)
	}
	if isLoopback(host) || token != "" {
		return nil
	}
	return errors.New("a controller token is required when listening beyond localhost")
}

func isLoopback(host string) bool {
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Handler returns the controller HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /api/status", s.status)
	mux.HandleFunc("GET /api/config", s.getConfiguration)
	mux.HandleFunc("PUT /api/config", s.putConfiguration)
	mux.HandleFunc("POST /api/actions/{action}", s.action)
	return securityHeaders(mux)
}

// Serve listens until the supplied context is canceled.
func (s *Server) Serve(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("listen for controller: %w", err)
	}
	defer listener.Close()
	_, _ = fmt.Fprintf(s.out, "pgmigrate controller listening on http://%s\n", s.address)

	server := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	done := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()

	select {
	case err := <-done:
		s.cancel()
		return err
	case <-ctx.Done():
		s.cancel()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down controller: %w", err)
		}
		return <-done
	}
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, err := assets.ReadFile("ui.html")
	if err != nil {
		http.Error(w, "controller UI unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(data)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "controller token is missing or invalid")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	cfg := s.configurationSnapshot()
	response := statusResponse{
		Operations:            s.operationSnapshots(),
		ConnectionsConfigured: cfg.ValidateConnections() == nil,
		TokenRequired:         s.token != "",
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	store, err := state.OpenReadOnly(ctx, cfg.Dir)
	if errors.Is(err, state.ErrStateNotFound) {
		writeJSON(w, http.StatusOK, response)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer store.Close()

	snapshot, err := observe.Capture(ctx, store, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	response.Snapshot = &snapshot
	parts, err := store.ListParts(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, part := range parts {
		if !part.Completed {
			continue
		}
		response.Copy.Rows += part.Rows
		response.Copy.Bytes += part.Bytes
		response.Copy.Duration += part.Duration
	}
	findings, err := store.PendingFindings(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, finding := range findings {
		response.Findings = append(response.Findings, findingView{
			ID: finding.ID, Kind: finding.Kind, Severity: finding.Severity,
			Message: finding.Message, ObservedAt: finding.ObservedAt,
		})
	}
	attempt, err := store.FailedAttempt(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if attempt.Consecutive > 0 {
		response.Failure = &failureView{
			Phase: attempt.Phase, Signature: attempt.Signature, Detail: attempt.Detail,
			Consecutive: attempt.Consecutive, ObservedAt: attempt.ObservedAt,
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) getConfiguration(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "controller token is missing or invalid")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, s.configurationViewSnapshot())
}

func (s *Server) putConfiguration(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "controller token is missing or invalid")
		return
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var update configurationUpdate
	if err := decoder.Decode(&update); err != nil {
		writeError(w, http.StatusBadRequest, "decode configuration: "+err.Error())
		return
	}
	if err := ensureJSONEnd(decoder); err != nil {
		writeError(w, http.StatusBadRequest, "decode configuration: "+err.Error())
		return
	}
	view, err := s.updateConfiguration(update)
	if err != nil {
		var conflict *configurationConflictError
		if errors.As(err, &conflict) {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, view)
}

type configurationConflictError struct {
	operation string
}

func (e *configurationConflictError) Error() string {
	return "configuration cannot be changed while " + e.operation + " is active"
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain exactly one JSON object")
		}
		return err
	}
	return nil
}

func (s *Server) updateConfiguration(update configurationUpdate) (configurationView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range []string{"migration", "verification"} {
		if operation := s.operations[slot]; operation.active() {
			return configurationView{}, &configurationConflictError{operation: slot}
		}
	}
	candidate := s.cfg
	if err := applyConfigurationUpdate(&candidate, update); err != nil {
		return configurationView{}, err
	}
	if err := validateConfiguration(candidate); err != nil {
		return configurationView{}, err
	}
	s.cfg = candidate
	s.configRevision++
	return viewConfiguration(candidate, s.configurationRevisionLocked()), nil
}

func applyConfigurationUpdate(candidate *config.Config, update configurationUpdate) error {
	if update.Source != nil && strings.TrimSpace(*update.Source) != "" {
		candidate.Source = strings.TrimSpace(*update.Source)
	}
	if update.Target != nil && strings.TrimSpace(*update.Target) != "" {
		candidate.Target = strings.TrimSpace(*update.Target)
	}
	setIfPresent(&candidate.TableFilter, update.TableFilter)
	setIfPresent(&candidate.AckWarnings, update.AckWarnings)
	setIfPresent(&candidate.AllowCollationChange, update.AllowCollationChange)
	setIfPresent(&candidate.Workers, update.Workers)
	setIfPresent(&candidate.SplitThreshold, update.SplitThreshold)
	setIfPresent(&candidate.RestoreJobs, update.RestoreJobs)
	setIfPresent(&candidate.PGDumpPath, update.PGDumpPath)
	setIfPresent(&candidate.PGRestorePath, update.PGRestorePath)
	setIfPresent(&candidate.Metrics, update.Metrics)
	setIfPresent(&candidate.RetryBaseCopy, update.RetryBaseCopy)
	setIfPresent(&candidate.SkipTargetTuning, update.SkipTargetTuning)
	setIfPresent(&candidate.WarnOnTuningErrors, update.WarnOnTuningErrors)
	setIfPresent(&candidate.TargetMemory, update.TargetMemory)
	setIfPresent(&candidate.MaintenanceWorkMem, update.MaintenanceWorkMem)
	setIfPresent(&candidate.MaxParallelMaintenance, update.MaxParallelMaintenance)
	setIfPresent(&candidate.MaxWALSize, update.MaxWALSize)
	setIfPresent(&candidate.CheckpointTimeout, update.CheckpointTimeout)
	setIfPresent(&candidate.VerifyWorkers, update.VerifyWorkers)
	setIfPresent(&candidate.VerifySampleRows, update.VerifySampleRows)
	setIfPresent(&candidate.VerifySampleWindows, update.VerifySampleWindows)
	setIfPresent(&candidate.VerifyBatchRows, update.VerifyBatchRows)
	setIfPresent(&candidate.VerifyDutyCycle, update.VerifyDutyCycle)
	setIfPresent(&candidate.VerifyCDCRows, update.VerifyCDCRows)
	setIfPresent(&candidate.CDCSampleRows, update.CDCSampleRows)
	if err := parseDurationUpdate("wal_sample_duration", update.WALSampleDuration, &candidate.WALSampleDuration); err != nil {
		return err
	}
	if err := parseDurationUpdate("segment_prune_interval", update.SegmentPruneInterval, &candidate.SegmentPruneInterval); err != nil {
		return err
	}
	if err := parseDurationUpdate("verify_table_timeout", update.VerifyTableTimeout, &candidate.VerifyTableTimeout); err != nil {
		return err
	}
	return parseDurationUpdate("verify_converge_timeout", update.VerifyConvergeTimeout, &candidate.VerifyConvergeTimeout)
}

func setIfPresent[T any](destination *T, value *T) {
	if value != nil {
		*destination = *value
	}
}

func parseDurationUpdate(name string, value *string, destination *time.Duration) error {
	if value == nil {
		return nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(*value))
	if err != nil {
		return fmt.Errorf("%s must be a duration such as 30s or 5m", name)
	}
	*destination = duration
	return nil
}

func validateConfiguration(cfg config.Config) error {
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
	if cfg.CDCSampleRows < 0 {
		return errors.New("cdc-sample-rows must not be negative")
	}
	if cfg.Metrics != "" {
		if _, _, err := net.SplitHostPort(cfg.Metrics); err != nil {
			return fmt.Errorf("parse metrics listen address: %w", err)
		}
	}
	if _, err := cfg.TuningOverrides(); err != nil {
		return err
	}
	return cfg.ValidateVerify()
}

func viewConfiguration(cfg config.Config, revision string) configurationView {
	return configurationView{
		Revision:         revision,
		SourceConfigured: strings.TrimSpace(cfg.Source) != "", TargetConfigured: strings.TrimSpace(cfg.Target) != "",
		TableFilter: cfg.TableFilter, AckWarnings: cfg.AckWarnings, AllowCollationChange: cfg.AllowCollationChange,
		Workers: cfg.Workers, SplitThreshold: cfg.SplitThreshold, RestoreJobs: cfg.RestoreJobs,
		PGDumpPath: cfg.PGDumpPath, PGRestorePath: cfg.PGRestorePath, Metrics: cfg.Metrics,
		WALSampleDuration: cfg.WALSampleDuration.String(), SegmentPruneInterval: cfg.SegmentPruneInterval.String(),
		RetryBaseCopy: cfg.RetryBaseCopy, SkipTargetTuning: cfg.SkipTargetTuning,
		WarnOnTuningErrors: cfg.WarnOnTuningErrors, TargetMemory: cfg.TargetMemory,
		MaintenanceWorkMem: cfg.MaintenanceWorkMem, MaxParallelMaintenance: cfg.MaxParallelMaintenance,
		MaxWALSize: cfg.MaxWALSize, CheckpointTimeout: cfg.CheckpointTimeout,
		VerifyWorkers: cfg.VerifyWorkers, VerifySampleRows: cfg.VerifySampleRows,
		VerifySampleWindows: cfg.VerifySampleWindows, VerifyBatchRows: cfg.VerifyBatchRows,
		VerifyDutyCycle: cfg.VerifyDutyCycle, VerifyTableTimeout: cfg.VerifyTableTimeout.String(),
		VerifyConvergeTimeout: cfg.VerifyConvergeTimeout.String(), VerifyCDCRows: cfg.VerifyCDCRows,
		CDCSampleRows: cfg.CDCSampleRows,
	}
}

func (s *Server) configurationSnapshot() config.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

func (s *Server) configurationViewSnapshot() configurationView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return viewConfiguration(s.cfg, s.configurationRevisionLocked())
}

func (s *Server) configurationRevisionLocked() string {
	return s.configGeneration + ":" + strconv.FormatUint(s.configRevision, 10)
}

func validConfigurationRevision(revision string) bool {
	generation, sequence, ok := strings.Cut(revision, ":")
	if !ok || len(generation) != 32 {
		return false
	}
	if _, err := hex.DecodeString(generation); err != nil {
		return false
	}
	value, err := strconv.ParseUint(sequence, 10, 64)
	return err == nil && value > 0
}

func (s *Server) action(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		writeError(w, http.StatusUnauthorized, "controller token is missing or invalid")
		return
	}
	name := r.PathValue("action")
	if r.Header.Get("X-PGMigrate-Confirm") != name {
		writeError(w, http.StatusPreconditionFailed, "action confirmation header is missing")
		return
	}
	if strings.HasPrefix(name, "stop-") {
		s.stop(w, strings.TrimPrefix(name, "stop-"))
		return
	}
	action, ok := map[string]Action{
		"preflight": s.actions.Preflight,
		"run":       s.actions.Run,
		"verify":    s.actions.Verify,
	}[name]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown controller action")
		return
	}
	revision := strings.TrimSpace(r.Header.Get("X-PGMigrate-Config-Revision"))
	if !validConfigurationRevision(revision) {
		writeError(w, http.StatusPreconditionFailed, "configuration revision header is missing or invalid")
		return
	}
	if err := s.validateLifecycle(r.Context(), name); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	view, err := s.start(name, revision, action)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, view)
}

func (s *Server) validateLifecycle(ctx context.Context, action string) error {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg := s.configurationSnapshot()
	store, err := state.OpenReadOnly(readCtx, cfg.Dir)
	if errors.Is(err, state.ErrStateNotFound) {
		if action == "verify" {
			return errors.New("verification requires a migration in follow phase")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read migration phase: %w", err)
	}
	defer store.Close()
	migration, err := store.Migration(readCtx)
	if err != nil {
		return fmt.Errorf("read migration phase: %w", err)
	}
	switch action {
	case "preflight":
		if migration.Phase != state.PhasePreflight {
			return fmt.Errorf("preflight is unavailable in %s phase", migration.Phase)
		}
	case "run":
		if migration.Phase == state.PhaseComplete {
			return errors.New("migration is already complete")
		}
	case "verify":
		if migration.Phase != state.PhaseFollow {
			return fmt.Errorf("verification requires follow phase; migration is in %s", migration.Phase)
		}
	}
	return nil
}

func (s *Server) authorized(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	provided := r.Header.Get("X-PGMigrate-Token")
	return subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) == 1
}

func (s *Server) start(name string, revision string, action Action) (operationView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if revision != s.configurationRevisionLocked() {
		return operationView{}, errors.New("configuration revision is stale; review current configuration")
	}
	slot := "migration"
	if name == "verify" {
		slot = "verification"
	}
	current := s.operations[slot]
	if current.active() {
		return operationView{}, fmt.Errorf("%s is already %s", current.Name, current.State)
	}
	otherSlot := "verification"
	if slot == "verification" {
		otherSlot = "migration"
	}
	other := s.operations[otherSlot]
	if other.active() && !(name == "verify" && other.Name == "run") {
		return operationView{}, fmt.Errorf("%s cannot start while %s is %s", name, other.Name, other.State)
	}
	s.nextID++
	ctx, cancel := context.WithCancel(s.ctx)
	output := &tailBuffer{limit: outputLimit}
	operation := operation{
		ID: s.nextID, Name: name, State: "running", StartedAt: time.Now().UTC(),
		Cancel: cancel, Output: output,
	}
	s.operations[slot] = operation
	view := operation.view()
	cfg := s.cfg
	go s.execute(ctx, slot, operation.ID, output, cfg, action)
	return view, nil
}

func (s *Server) execute(ctx context.Context, slot string, id int64, output io.Writer, cfg config.Config, action Action) {
	err := action(ctx, cfg, output)

	s.mu.Lock()
	defer s.mu.Unlock()
	operation := s.operations[slot]
	if operation.ID != id {
		return
	}
	operation.FinishedAt = time.Now().UTC()
	operation.Cancel = nil
	if err == nil {
		operation.State = "succeeded"
		s.operations[slot] = operation
		return
	}
	if errors.Is(err, context.Canceled) {
		operation.State = "stopped"
		s.operations[slot] = operation
		return
	}
	operation.State = "failed"
	operation.Error = err.Error()
	s.operations[slot] = operation
}

func (s *Server) stop(w http.ResponseWriter, slot string) {
	if slot != "migration" && slot != "verification" {
		writeError(w, http.StatusNotFound, "unknown controller operation")
		return
	}
	s.mu.Lock()
	operation := s.operations[slot]
	if operation.State != "running" || operation.Cancel == nil {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, fmt.Sprintf("no %s operation is running", slot))
		return
	}
	operation.State = "stopping"
	operation.Cancel()
	s.operations[slot] = operation
	view := operation.view()
	s.mu.Unlock()
	writeJSON(w, http.StatusAccepted, view)
}

func (s *Server) operationSnapshots() map[string]operationView {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]operationView{
		"migration":    s.operations["migration"].view(),
		"verification": s.operations["verification"].view(),
	}
}

func (o operation) active() bool {
	return o.State == "running" || o.State == "stopping"
}

func (o operation) view() operationView {
	view := operationView{
		ID: o.ID, Name: o.Name, State: o.State, Error: o.Error,
	}
	if !o.StartedAt.IsZero() {
		started := o.StartedAt
		view.StartedAt = &started
	}
	if !o.FinishedAt.IsZero() {
		finished := o.FinishedAt
		view.FinishedAt = &finished
	}
	if o.Output != nil {
		view.Output = o.Output.String()
	}
	return view
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

type tailBuffer struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	if len(b.data) > b.limit {
		b.data = append([]byte(nil), b.data[len(b.data)-b.limit:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.data)
}
