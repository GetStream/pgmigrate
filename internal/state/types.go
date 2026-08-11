// Package state provides the durable, low-volume control plane for a migration.
package state

import (
	"errors"
	"fmt"
	"time"
)

var (
	// ErrLocked is returned when another process owns the migration directory.
	ErrLocked = errors.New("migration directory is locked")
	// ErrClosed is returned after a Store has been closed.
	ErrClosed = errors.New("state store is closed")
	// ErrReadOnly is returned when a mutating method is used on a status store.
	ErrReadOnly = errors.New("state store is read-only")
	// ErrStateNotFound is returned when no initialized state database exists.
	ErrStateNotFound = errors.New("migration state not found")
	// ErrInvalidPhaseTransition is returned for a non-forward lifecycle transition.
	ErrInvalidPhaseTransition = errors.New("invalid phase transition")
)

// Fingerprints bind a migration directory to its source and selected table set.
type Fingerprints struct {
	Source string
	Filter string
}

// FingerprintMismatchError reports an attempt to reuse state with different inputs.
type FingerprintMismatchError struct {
	Field string
	Have  string
	Want  string
}

func (e *FingerprintMismatchError) Error() string {
	return fmt.Sprintf("%s fingerprint mismatch: state has %q, requested %q", e.Field, e.Have, e.Want)
}

// SchemaVersionError reports a state directory this binary cannot read. Naming
// both versions at open time is the point: silently opening an incompatible
// directory and failing later, mid-migration, is the worst of the options.
type SchemaVersionError struct {
	Have int
	Want int
}

func (e *SchemaVersionError) Error() string {
	if e.Have > e.Want {
		return fmt.Sprintf(
			"migration state was written by a newer pgmigrate (state schema version %d, this binary reads %d): use that newer binary for this directory",
			e.Have, e.Want,
		)
	}
	return fmt.Sprintf(
		"migration state is at schema version %d and this binary expects %d: run pgmigrate with the same binary that owns this directory to upgrade it",
		e.Have, e.Want,
	)
}

// Phase is a durable migration lifecycle state.
type Phase string

const (
	PhasePreflight Phase = "preflight"
	PhaseSetup     Phase = "setup"
	PhaseSchema    Phase = "schema"
	PhaseCopy      Phase = "copy"
	PhaseIndexes   Phase = "indexes"
	PhaseCatchup   Phase = "catchup"
	PhaseFollow    Phase = "follow"
	PhaseDrained   Phase = "drained"
	PhaseCutover   Phase = "cutover"
	PhaseComplete  Phase = "complete"
)

var phaseOrder = map[Phase]int{
	PhasePreflight: 0,
	PhaseSetup:     1,
	PhaseSchema:    2,
	PhaseCopy:      3,
	PhaseIndexes:   4,
	PhaseCatchup:   5,
	PhaseFollow:    6,
	PhaseDrained:   7,
	PhaseCutover:   8,
	PhaseComplete:  9,
}

// Migration contains singleton migration metadata.
type Migration struct {
	SourceFingerprint string
	FilterFingerprint string
	SlotName          string
	SnapshotName      string
	ConsistentPoint   string
	Phase             Phase
	EndPosition       string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// FailedAttempt describes how the previous run died. Consecutive counts how
// many runs in a row ended in the same phase with the same signature.
type FailedAttempt struct {
	Phase       Phase
	Signature   string
	Detail      string
	Consecutive int
	ObservedAt  time.Time
}

// Table describes one source table in the durable inventory.
type Table struct {
	OID           uint32
	Schema        string
	Name          string
	EstimatedRows int64
	Bytes         int64
	PartsTotal    int64
	Completed     bool
	CompletedAt   time.Time
}

// Part describes one independently copied table range.
type Part struct {
	TableOID    uint32
	ID          string
	RangeStart  string
	RangeEnd    string
	Rows        int64
	Bytes       int64
	Duration    time.Duration
	Completed   bool
	CompletedAt time.Time
}

// Index describes one target index build.
type Index struct {
	OID         uint32
	TableOID    uint32
	Name        string
	Definition  string
	Bytes       int64
	Completed   bool
	CompletedAt time.Time
}

// Constraint describes one target constraint operation.
type Constraint struct {
	OID         uint32
	TableOID    uint32
	Name        string
	Kind        string
	Definition  string
	Completed   bool
	CompletedAt time.Time
}

// ApplyProgress is the status-only copy of target replication-origin progress.
type ApplyProgress struct {
	StagedLSN  string
	AppliedLSN string
	Txns       int64
	Rows       int64
	UpdatedAt  time.Time
}

// VerifyTable is the live progress and outcome of checking one table.
//
// Only the source is counted in pages, because only the source is read by page;
// the target is reached through the key's own index and is counted in rows. Pages
// are what the read costs, and Sampled against Estimated is what it proves, which
// is a smaller claim: a check reads a fraction of a large table.
type VerifyTable struct {
	TableOID uint32
	// Schema and Name are filled in from the table inventory when the progress is
	// read back, so a reader does not have to resolve an OID to say what is being
	// verified.
	Schema           string
	Name             string
	Stage            string
	SourcePages      int64
	SourcePagesTotal int64
	Sampled          int64
	Estimated        int64
	TargetRows       int64
	Rate             float64
	ETA              time.Duration
	Coverage         float64
	// Candidates is how many sampled rows disagreed when first read, and Unresolved
	// how many still differed once re-read against a fixed WAL position. A table can
	// have the first without the second, which is what a change in flight looks like.
	Candidates int64
	// CDCKeys is how many applier-recorded rows the separate CDC check looked at,
	// out of CDCObserved changes the applier saw. They belong next to Sampled and
	// never inside it: a heap sample cannot reach a row because it was replicated,
	// so a table can be well covered by one and not at all by the other.
	CDCKeys     int64
	CDCObserved int64
	Unresolved  int64
	Converged   bool
	Complete    bool
	UpdatedAt   time.Time
}

// Finding is a preflight result, divergence alarm, or DDL event.
type Finding struct {
	ID         string
	Kind       string
	Severity   string
	Message    string
	Resolved   bool
	ObservedAt time.Time
	ResolvedAt time.Time
}

// Step is an idempotent marker for an orchestrator action.
type Step struct {
	Name        string
	Detail      string
	Completed   bool
	CompletedAt time.Time
}

// Counts reports completed and total objects of one kind.
type Counts struct {
	Done  int64
	Total int64
}

// Status is a consistent snapshot suitable for CLI or metrics rendering.
type Status struct {
	Migration    Migration
	Tables       Counts
	Parts        Counts
	Indexes      Counts
	Constraints  Counts
	VerifyTables Counts
	// Verification is per-table comparison progress, which a count cannot express:
	// how far each side has read, and whether the table converged, are separate
	// facts from how many tables have finished.
	Verification   []VerifyTable
	OpenFindings   int64
	CompletedSteps int64
	Apply          ApplyProgress
}
