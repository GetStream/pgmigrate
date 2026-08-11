// Package tuning temporarily reconfigures a target server for a bulk load.
//
// A target left at stock settings spends most of a large migration on work the
// load does not need. The two costs that dominate are index builds sorting on
// disk at the default maintenance_work_mem, and checkpoints firing every
// max_wal_size of WAL, which on a 200 GB load is close to continuous. Both are
// settings, not code, so the tool changes them for the duration of the load and
// puts them back.
//
// Every change is derived from what the target reports about itself rather than
// from a fixed table, because the settings interact with the target's memory and
// worker limits and a value that helps a large instance can exhaust a small one.
//
// # Why synchronous_commit is only relaxed for the load
//
// Changes are scoped: some apply to the sessions that copy and build indexes,
// others to the whole server. synchronous_commit is deliberately in the first
// group and is never relaxed for the CDC applier, even though that is where the
// commit rate is highest.
//
// The applier is safe against a lost commit by itself. It writes each
// transaction's data and its progress row in one target transaction, so the two
// cannot disagree, and the source slot is advanced from the locally fsynced
// segment watermark rather than from target progress, so a target that loses a
// commit never costs source WAL. What is not safe is segment pruning: it keys
// off applied progress, so a lost commit could unlink a segment the tool still
// needs to replay. The window is small and the throughput gain on the applier's
// small transactions is minor, which is a bad trade against turning an
// impossible outcome into an unlikely one.
package tuning

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Scope is how a change has to be applied, which follows from the setting's
// context in pg_settings rather than from a decision of ours.
type Scope string

const (
	// ScopeSession is a USERSET setting, applied with set_config on each session
	// that copies or builds indexes. It needs no special privilege and reverts
	// when the session closes.
	ScopeSession Scope = "session"
	// ScopeSystem is a SIGHUP setting, applied with ALTER SYSTEM and a
	// configuration reload. It outlives the process, so it has to be recorded
	// before it is applied and reverted explicitly.
	ScopeSystem Scope = "system"
)

// Setting names this package manages. Anything absent here is left alone.
const (
	MaintenanceWorkMem            = "maintenance_work_mem"
	MaxParallelMaintenanceWorkers = "max_parallel_maintenance_workers"
	SynchronousCommit             = "synchronous_commit"
	MaxWALSize                    = "max_wal_size"
	CheckpointTimeout             = "checkpoint_timeout"
	CheckpointCompletionTarget    = "checkpoint_completion_target"
)

// Managed lists the settings this package reads and may change, in the order
// they are applied.
var Managed = []string{
	MaintenanceWorkMem,
	MaxParallelMaintenanceWorkers,
	SynchronousCommit,
	MaxWALSize,
	CheckpointTimeout,
	CheckpointCompletionTarget,
}

// Setting is one row of pg_settings, reduced to what tuning needs.
type Setting struct {
	Name string
	// Value is the setting as PostgreSQL reports it, already in Unit terms.
	Value string
	// Unit is pg_settings.unit: "8kB", "kB", "MB", "ms", "min", "s", or empty
	// for a plain number or enum.
	Unit string
	// Context is pg_settings.context, which decides Scope.
	Context string
	// FromAutoConf reports whether the running value comes from
	// postgresql.auto.conf, which is where ALTER SYSTEM writes. It decides
	// whether a revert resets the setting or writes the old value back.
	FromAutoConf bool
	// AlterSystem reports has_parameter_privilege(name, 'ALTER SYSTEM'). It is
	// false on managed services, where ALTER SYSTEM is refused regardless of
	// role, so it is the difference between planning a system change and
	// explaining why one is not possible.
	AlterSystem bool
}

// Canonical renders the setting's value the way SHOW does, which is both what a
// person needs to read it and what ALTER SYSTEM needs to restore it.
//
// pg_settings splits a value from its unit, so max_wal_size reads as "1024" with
// unit "MB". A bare "1024" would in fact restore correctly, because PostgreSQL
// reads a unit-less value in the parameter's own unit, but "1024" reads as a
// mystery in a log line and in a preflight finding, and "1GB" does not.
func (s Setting) Canonical() string {
	if multiplier, ok := sizeMultiplier(s.Unit); ok {
		// A size unit multiplies a plain count, so "8kB" means blocks of 8 kB
		// rather than a suffix that can be appended to the value.
		if count, err := strconv.ParseInt(s.Value, 10, 64); err == nil {
			return FormatBytes(count * multiplier)
		}
	}
	if timeUnit(s.Unit) {
		if seconds, err := parseDurationSeconds(s.Value, s.Unit); err == nil {
			return FormatDurationSeconds(seconds)
		}
	}
	return s.Value
}

func sizeMultiplier(unit string) (int64, bool) {
	switch unit {
	case "B":
		return 1, true
	case "kB":
		return 1 << 10, true
	case "8kB":
		return 8 << 10, true
	case "16kB":
		return 16 << 10, true
	case "MB":
		return 1 << 20, true
	case "GB":
		return 1 << 30, true
	case "TB":
		return 1 << 40, true
	default:
		return 0, false
	}
}

func timeUnit(unit string) bool {
	switch unit {
	case "us", "ms", "s", "min", "h", "d":
		return true
	default:
		return false
	}
}

// Target is what a server reports about itself.
type Target struct {
	Settings map[string]Setting
	// SharedBuffersBytes is the only proxy PostgreSQL offers for host memory.
	// See Derive for how far that is trusted.
	SharedBuffersBytes int64
	MaxWorkerProcesses int
}

// Overrides are operator-supplied values that replace a derived one. A zero
// value means "derive".
type Overrides struct {
	MaintenanceWorkMem            string
	MaxParallelMaintenanceWorkers int
	MaxWALSize                    string
	CheckpointTimeout             string
	// TargetMemory replaces the estimate derived from shared_buffers.
	TargetMemory string
	// TargetMemoryBytes is TargetMemory parsed, populated by Validate.
	TargetMemoryBytes int64
}

// Validate parses and checks the overrides, so a mistyped value is rejected when
// the command starts rather than partway through tuning a live target.
func (o *Overrides) Validate() error {
	for flag, value := range map[string]*string{
		"--maintenance-work-mem": &o.MaintenanceWorkMem,
		"--max-wal-size":         &o.MaxWALSize,
	} {
		if *value == "" {
			continue
		}
		if _, err := ParseBytes(*value); err != nil {
			return fmt.Errorf("%s: %w", flag, err)
		}
	}
	if o.CheckpointTimeout != "" {
		if _, err := parseDurationSeconds(o.CheckpointTimeout, ""); err != nil {
			return fmt.Errorf("--checkpoint-timeout: %w", err)
		}
	}
	if o.MaxParallelMaintenanceWorkers < 0 {
		return errors.New("--max-parallel-maintenance-workers must not be negative")
	}
	if o.TargetMemory != "" {
		bytes, err := ParseBytes(o.TargetMemory)
		if err != nil {
			return fmt.Errorf("--target-memory: %w", err)
		}
		if bytes <= 0 {
			return errors.New("--target-memory must be positive")
		}
		o.TargetMemoryBytes = bytes
	}
	// ALTER SYSTEM interpolates values, so an override that would not survive
	// that has to be refused here rather than at the statement.
	for flag, value := range map[string]string{
		"--maintenance-work-mem": o.MaintenanceWorkMem,
		"--max-wal-size":         o.MaxWALSize,
		"--checkpoint-timeout":   o.CheckpointTimeout,
	} {
		if value != "" && !validValue(value) {
			return fmt.Errorf("%s: %q is not a usable setting value", flag, value)
		}
	}
	return nil
}

// Change is one setting to move, with what is needed to put it back.
type Change struct {
	Name  string `json:"name"`
	From  string `json:"from"`
	To    string `json:"to"`
	Scope Scope  `json:"scope"`
	// ResetOnRevert distinguishes the two ways a system setting can be undone.
	// ALTER SYSTEM writes postgresql.auto.conf, so for a setting that was not
	// already there the faithful revert is RESET, which restores whatever the
	// server was inheriting from postgresql.conf or its built-in default.
	// Writing the old value back instead would pin it in auto.conf and silently
	// take precedence over a later edit to postgresql.conf. Only a setting that
	// was already in auto.conf is reverted by writing its value back.
	ResetOnRevert bool `json:"reset_on_revert,omitempty"`
}

// Plan is an ordered set of changes plus what could not be planned.
type Plan struct {
	Changes []Change
	// Blocked names settings worth changing that this target will not allow,
	// each mapped to the reason. These are reported, never attempted.
	Blocked map[string]string
	// MemoryBytes is the memory figure the plan was derived from, recorded so
	// the log explains why maintenance_work_mem came out as it did.
	MemoryBytes int64
	// MemoryEstimated is true when MemoryBytes came from shared_buffers rather
	// than from an operator.
	MemoryEstimated bool
}

// SessionGUCs returns the session-scoped changes as name/value pairs, for the
// runners that open their own connections.
func (p Plan) SessionGUCs() map[string]string {
	gucs := map[string]string{}
	for _, change := range p.Changes {
		if change.Scope == ScopeSession {
			gucs[change.Name] = change.To
		}
	}
	return gucs
}

// SystemChanges returns the changes that need ALTER SYSTEM.
func (p Plan) SystemChanges() []Change {
	var changes []Change
	for _, change := range p.Changes {
		if change.Scope == ScopeSystem {
			changes = append(changes, change)
		}
	}
	return changes
}

// Empty reports whether there is nothing to do.
func (p Plan) Empty() bool { return len(p.Changes) == 0 }

// Bounds on the derived maintenance_work_mem. The floor is PostgreSQL's default,
// so a target too small to improve on is left alone rather than lowered. The
// ceiling is where a btree sort stops benefiting from more memory well before it
// stops consuming it.
const (
	minMaintenanceWorkMem = 64 << 20
	maxMaintenanceWorkMem = 16 << 30
	// memoryBudgetFraction is the share of target memory the concurrent index
	// builds may divide between them. The rest is left for shared_buffers, the
	// copy sessions, and everything else on the instance.
	memoryBudgetFraction = 0.5
	// sharedBuffersMemoryRatio turns shared_buffers into a host-memory estimate.
	// Managed providers set shared_buffers to about a quarter of memory, so this
	// is deliberately the conservative end of that convention.
	sharedBuffersMemoryRatio = 4
)

// Derive produces the changes to apply. It is pure: everything it needs is in
// target and overrides, so the interesting decisions are testable without a
// server.
//
// workers is the number of sessions that will build indexes concurrently, which
// is what makes per-session memory a product rather than a single value.
func Derive(target Target, overrides Overrides, workers int) (Plan, error) {
	if workers < 1 {
		return Plan{}, errors.New("tuning: workers must be positive")
	}
	plan := Plan{Blocked: map[string]string{}}

	memory, estimated := overrides.TargetMemoryBytes, false
	if memory <= 0 {
		memory, estimated = target.SharedBuffersBytes*sharedBuffersMemoryRatio, true
	}
	plan.MemoryBytes, plan.MemoryEstimated = memory, estimated

	workMem, err := deriveMaintenanceWorkMem(overrides.MaintenanceWorkMem, memory, workers)
	if err != nil {
		return Plan{}, err
	}
	plan.add(target, Change{Name: MaintenanceWorkMem, To: workMem})

	if parallel := deriveParallelMaintenanceWorkers(overrides.MaxParallelMaintenanceWorkers, target); parallel > 0 {
		plan.add(target, Change{Name: MaxParallelMaintenanceWorkers, To: strconv.Itoa(parallel)})
	} else {
		plan.Blocked[MaxParallelMaintenanceWorkers] = fmt.Sprintf(
			"max_worker_processes is %d, which leaves no room for parallel maintenance workers; raising it needs a restart",
			target.MaxWorkerProcesses,
		)
	}

	plan.add(target, Change{Name: SynchronousCommit, To: "off"})
	plan.add(target, Change{Name: MaxWALSize, To: firstNonEmpty(overrides.MaxWALSize, "64GB")})
	plan.add(target, Change{Name: CheckpointTimeout, To: firstNonEmpty(overrides.CheckpointTimeout, "30min")})
	plan.add(target, Change{Name: CheckpointCompletionTarget, To: "0.9"})
	return plan, nil
}

// add records a change unless the target does not report the setting, already
// sits at a value at least as good, or will not permit the change.
func (p *Plan) add(target Target, change Change) {
	setting, ok := target.Settings[change.Name]
	if !ok {
		p.Blocked[change.Name] = "the target does not report this setting"
		return
	}
	change.From = setting.Canonical()
	scope, err := scopeFor(setting)
	if err != nil {
		p.Blocked[change.Name] = err.Error()
		return
	}
	change.Scope = scope
	if scope == ScopeSystem {
		if !setting.AlterSystem {
			p.Blocked[change.Name] = "changing it needs ALTER SYSTEM, which this target does not permit"
			return
		}
		change.ResetOnRevert = !setting.FromAutoConf
	}
	if alreadySatisfies(setting, change.To) {
		return
	}
	p.Changes = append(p.Changes, change)
}

// scopeFor maps a pg_settings context onto how the change has to be made.
func scopeFor(setting Setting) (Scope, error) {
	switch setting.Context {
	case "user", "superuser":
		return ScopeSession, nil
	case "sighup", "superuser-backend":
		return ScopeSystem, nil
	case "postmaster":
		return "", errors.New("changing it needs a server restart")
	case "internal":
		return "", errors.New("it is fixed at compile time")
	default:
		return "", fmt.Errorf("unrecognized setting context %q", setting.Context)
	}
}

// alreadySatisfies reports whether the current value is at least as good as the
// proposed one, so that a target an operator has already tuned is left alone and
// a rerun plans nothing.
func alreadySatisfies(setting Setting, proposed string) bool {
	current := setting.Canonical()
	if strings.EqualFold(current, proposed) {
		return true
	}
	switch setting.Name {
	case SynchronousCommit:
		// Anything weaker than on is already at least as fast, and "off" is the
		// weakest, so only "on" and the synchronous-replication levels move.
		return strings.EqualFold(current, "off") || strings.EqualFold(current, "local")
	case MaintenanceWorkMem, MaxWALSize:
		have, haveErr := ParseBytes(current)
		want, wantErr := ParseBytes(proposed)
		return haveErr == nil && wantErr == nil && have >= want
	case CheckpointTimeout:
		have, haveErr := parseDurationSeconds(current, "")
		want, wantErr := parseDurationSeconds(proposed, "")
		return haveErr == nil && wantErr == nil && have >= want
	case MaxParallelMaintenanceWorkers:
		have, haveErr := strconv.Atoi(current)
		want, wantErr := strconv.Atoi(proposed)
		return haveErr == nil && wantErr == nil && have >= want
	case CheckpointCompletionTarget:
		have, haveErr := strconv.ParseFloat(current, 64)
		want, wantErr := strconv.ParseFloat(proposed, 64)
		return haveErr == nil && wantErr == nil && have >= want
	}
	return false
}

func deriveMaintenanceWorkMem(override string, memory int64, workers int) (string, error) {
	if override != "" {
		if _, err := ParseBytes(override); err != nil {
			return "", fmt.Errorf("tuning: maintenance_work_mem override: %w", err)
		}
		return override, nil
	}
	if memory <= 0 {
		// Without a memory figure there is no safe way to size a per-session
		// allocation that this many sessions will hold at once.
		return FormatBytes(minMaintenanceWorkMem), nil
	}
	budget := int64(float64(memory)*memoryBudgetFraction) / int64(workers)
	return FormatBytes(min(max(budget, minMaintenanceWorkMem), maxMaintenanceWorkMem)), nil
}

func deriveParallelMaintenanceWorkers(override int, target Target) int {
	want := 4
	if override > 0 {
		want = override
	}
	// One worker process has to remain for the leader's own use, and
	// max_worker_processes cannot be raised without a restart.
	if headroom := target.MaxWorkerProcesses - 1; headroom < want {
		want = headroom
	}
	return max(want, 0)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// ParseBytes reads a PostgreSQL size, which is a number with an optional unit
// suffix and no space, defaulting to bytes.
func ParseBytes(value string) (int64, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, errors.New("empty size")
	}
	digits := strings.TrimRight(trimmed, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ")
	unit := strings.TrimSpace(trimmed[len(digits):])
	number, err := strconv.ParseInt(strings.TrimSpace(digits), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", value)
	}
	multiplier := int64(1)
	switch strings.ToLower(unit) {
	case "", "b":
	case "8kb":
		multiplier = 8 << 10
	case "kb":
		multiplier = 1 << 10
	case "mb":
		multiplier = 1 << 20
	case "gb":
		multiplier = 1 << 30
	case "tb":
		multiplier = 1 << 40
	default:
		return 0, fmt.Errorf("unknown size unit %q in %q", unit, value)
	}
	return number * multiplier, nil
}

// FormatBytes renders a size in the largest unit that divides it exactly, so
// derived values read as an operator would have written them.
func FormatBytes(bytes int64) string {
	for _, unit := range []struct {
		suffix string
		size   int64
	}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"kB", 1 << 10}} {
		if bytes >= unit.size && bytes%unit.size == 0 {
			return strconv.FormatInt(bytes/unit.size, 10) + unit.suffix
		}
	}
	return strconv.FormatInt(bytes, 10) + "B"
}

// FormatDurationSeconds renders a duration in the largest unit that divides it
// exactly, matching how SHOW renders a time setting.
func FormatDurationSeconds(seconds float64) string {
	whole := int64(seconds)
	if float64(whole) != seconds {
		return strconv.FormatInt(int64(seconds*1000), 10) + "ms"
	}
	for _, unit := range []struct {
		suffix string
		size   int64
	}{{"d", 86400}, {"h", 3600}, {"min", 60}} {
		if whole >= unit.size && whole%unit.size == 0 {
			return strconv.FormatInt(whole/unit.size, 10) + unit.suffix
		}
	}
	return strconv.FormatInt(whole, 10) + "s"
}

// parseDurationSeconds reads a PostgreSQL time setting. unit is the pg_settings
// unit for a reported value and empty for a literal like "30min".
func parseDurationSeconds(value, unit string) (float64, error) {
	trimmed := strings.TrimSpace(value)
	digits := strings.TrimRight(trimmed, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ")
	suffix := strings.ToLower(strings.TrimSpace(trimmed[len(digits):]))
	if suffix == "" {
		suffix = strings.ToLower(unit)
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(digits), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", value)
	}
	switch suffix {
	case "", "s":
		return number, nil
	case "us":
		return number / 1_000_000, nil
	case "ms":
		return number / 1000, nil
	case "min":
		return number * 60, nil
	case "h":
		return number * 3600, nil
	case "d":
		return number * 86400, nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q in %q", suffix, value)
	}
}
