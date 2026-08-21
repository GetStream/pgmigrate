// Package controller serves the optional pgmigrate web controller.
package controller

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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

	mu         sync.Mutex
	operations map[string]operation
	nextID     int64
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
	ID         int64     `json:"id,omitempty"`
	Name       string    `json:"name,omitempty"`
	State      string    `json:"state"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Error      string    `json:"error,omitempty"`
	Output     string    `json:"output,omitempty"`
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

type statusResponse struct {
	Snapshot              *observe.Snapshot        `json:"snapshot,omitempty"`
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
	}, nil
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
	response := statusResponse{
		Operations:            s.operationSnapshots(),
		ConnectionsConfigured: s.cfg.ValidateConnections() == nil,
		TokenRequired:         s.token != "",
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	store, err := state.OpenReadOnly(ctx, s.cfg.Dir)
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
	view, err := s.start(name, action)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, view)
}

func (s *Server) authorized(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	provided := r.Header.Get("X-PGMigrate-Token")
	return subtle.ConstantTimeCompare([]byte(provided), []byte(s.token)) == 1
}

func (s *Server) start(name string, action Action) (operationView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	go s.execute(ctx, slot, operation.ID, output, action)
	return view, nil
}

func (s *Server) execute(ctx context.Context, slot string, id int64, output io.Writer, action Action) {
	err := action(ctx, s.cfg, output)

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
		ID: o.ID, Name: o.Name, State: o.State, StartedAt: o.StartedAt,
		FinishedAt: o.FinishedAt, Error: o.Error,
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
