# Controller UI Configuration Implementation Blueprint

## Meta

- **Design Doc:** N/A

## Overview

Allow an authenticated operator to configure every setting used by controller-managed preflight, run, and verification from the embedded dashboard. Controller bootstrap settings remain process-owned, credentials remain write-only and memory-only, and cutover and sequence advancement remain CLI-only.

### Cross-Cutting Requirements

- The controller must continue to start idle and must not create migration state or connect to either database on startup.
- Controller token, listen address, and migration directory remain startup-only.
- Source and target DSNs are never returned by an API, logged, written to state, or stored in browser storage.
- Configuration updates are rejected while a migration or verification operation is active.
- Existing CLI behavior and lifecycle guards remain unchanged.
- All new code passes `go vet ./...`, `go test ./...`, and `go test -race ./...`.

---

## Tasks

### Task 1: Add an authenticated mutable configuration API
**Type:** code

**Subtasks:**
- Add a concurrency-safe controller configuration store initialized from CLI/environment defaults.
- Add authenticated `GET /api/config` and `PUT /api/config` routes covering every configuration field used by preflight, run, and verify, excluding status-only, cutover/sequence, directory, listener, and token settings.
- Make source and target DSNs write-only: GET reports only whether each is configured, and an omitted/blank PUT value retains the current DSN.
- Parse human-readable durations and numeric settings, validate the complete candidate configuration, and atomically replace it only when no operation is active.
- Snapshot the current configuration when an action starts so an in-flight action cannot observe later mutations.
- Add handler and concurrency tests for authentication, redaction, validation, update locking, default preservation, and action snapshots.

**Acceptance Criteria:**
- AC1.1: An authenticated client can configure source, target, and every preflight/run/verify option after controller startup.
- AC1.2: Neither config GET nor status responses contain either DSN.
- AC1.3: Invalid configuration returns HTTP 400 without changing the active configuration.
- AC1.4: Configuration updates during active migration or verification return HTTP 409.
- AC1.5: Controller and CLI unit tests pass under the race detector.

---

### Task 2: Add complete configuration forms to the embedded UI
**Type:** code

**Subtasks:**
- Add database connection, migration, copy, tuning, and verification form sections with basic settings visible and advanced settings collapsible.
- Load non-secret defaults from the config API after authentication without placing DSNs in the DOM or browser storage.
- Save configuration through the authenticated API, clearly report validation errors, and show configured/not-configured connection state.
- Keep controls disabled until a valid configuration is saved and preserve all existing lifecycle, confirmation, progress, and stop behavior.
- Add static UI regression assertions for the configuration form, write-only DSNs, and absence of DSN browser persistence.
- Update README controller documentation with configuration security and lifecycle behavior.

**Acceptance Criteria:**
- AC2.1: Every preflight/run/verify configuration field can be edited from the dashboard.
- AC2.2: Source/target inputs are password fields, remain empty after reload, and are never stored in localStorage or sessionStorage.
- AC2.3: Bootstrap settings and CLI-only cutover/sequences are not editable from the dashboard.
- AC2.4: Existing progress and action controls remain functional and accessible.
- AC2.5: Controller tests and `git diff --check` pass.

---

### Task 3: Prove UI-supplied configuration end to end
**Type:** go-tests

**Subtasks:**
- Change the controller E2E driver to start without source/target DSNs and populate the complete action configuration through the authenticated config API.
- Exercise authenticated preflight, run, live verification, final verification, CLI-only cutover, cleanup checks, and independent source/target comparison.
- Add focused coverage that controller startup alone leaves the migration directory untouched.
- Validate the dashboard in a browser from unauthenticated state through configuration save, action enablement, progress rendering, and completed-state locking.

**Acceptance Criteria:**
- AC3.1: `make controller-e2e` passes while supplying both DSNs through the controller config API.
- AC3.2: Independent table inventory, row counts, and canonical source/target digests match after cutover.
- AC3.3: Starting the controller without taking an action creates no migration state and opens no database connection.
- AC3.4: `go vet ./...`, `go test ./...`, and `go test -race ./...` pass.

## Files to Modify

- `internal/controller/controller.go` - mutable config API and action snapshots.
- `internal/controller/controller_test.go` - API, redaction, locking, and startup regression tests.
- `internal/controller/ui.html` - complete configuration dashboard.
- `test/e2e/scripts/run-migration.sh` - configure controller through API.
- `README.md` and `test/README.md` - operator and E2E documentation.

## References

- Current controller server: `internal/controller/controller.go`
- Current embedded dashboard: `internal/controller/ui.html`
- CLI configuration flags: `internal/cli/cli.go`
- Shared configuration model: `internal/config/config.go`
