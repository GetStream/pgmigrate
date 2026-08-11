package tuning

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Conn is the subset of *pgx.Conn this package uses.
type Conn interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Recorder stores what a change will undo, before that change is made. Apply
// calls Record before every system change and refuses to proceed if it fails,
// because an ALTER SYSTEM whose original value was never written down is a
// change nothing can revert.
type Recorder interface {
	// Recorded reports whether a setting's original value is already stored,
	// which it will be when a previous attempt was interrupted after recording.
	Recorded(ctx context.Context, name string) (bool, error)
	// Record stores the change durably.
	Record(ctx context.Context, change Change) error
}

// Observe reads the managed settings, the memory proxy, and whether this target
// permits ALTER SYSTEM.
//
// has_parameter_privilege answers the question that matters on a managed service:
// RDS and Cloud SQL refuse ALTER SYSTEM whatever the role, and asking here means
// preflight can say so before a multi-hour copy rather than the run discovering
// it by failing.
func Observe(ctx context.Context, conn Conn) (Target, error) {
	target := Target{Settings: map[string]Setting{}}
	rows, err := conn.Query(ctx, `
		SELECT name, setting, coalesce(unit,''), context,
		       coalesce(sourcefile,'') LIKE '%postgresql.auto.conf',
		       has_parameter_privilege(name, 'ALTER SYSTEM')
		FROM pg_settings
		WHERE name = ANY($1)`, Managed)
	if err != nil {
		return Target{}, fmt.Errorf("tuning: read target settings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var setting Setting
		if err := rows.Scan(
			&setting.Name, &setting.Value, &setting.Unit, &setting.Context,
			&setting.FromAutoConf, &setting.AlterSystem,
		); err != nil {
			return Target{}, fmt.Errorf("tuning: scan target setting: %w", err)
		}
		target.Settings[setting.Name] = setting
	}
	if err := rows.Err(); err != nil {
		return Target{}, fmt.Errorf("tuning: read target settings: %w", err)
	}

	var sharedBuffers, blockSize, workerProcesses string
	if err := conn.QueryRow(
		ctx, `
		SELECT current_setting('shared_buffers'), current_setting('block_size'),
		       current_setting('max_worker_processes')`,
	).Scan(&sharedBuffers, &blockSize, &workerProcesses); err != nil {
		return Target{}, fmt.Errorf("tuning: read target resources: %w", err)
	}
	// current_setting renders shared_buffers with a unit suffix, but which one
	// depends on the value, so parse it rather than assuming blocks.
	buffers, err := ParseBytes(sharedBuffers)
	if err != nil {
		// Older renderings report plain blocks.
		blocks, blocksErr := strconv.ParseInt(sharedBuffers, 10, 64)
		size, sizeErr := strconv.ParseInt(blockSize, 10, 64)
		if blocksErr != nil || sizeErr != nil {
			return Target{}, fmt.Errorf("tuning: interpret shared_buffers %q: %w", sharedBuffers, err)
		}
		buffers = blocks * size
	}
	target.SharedBuffersBytes = buffers
	if target.MaxWorkerProcesses, err = strconv.Atoi(workerProcesses); err != nil {
		return Target{}, fmt.Errorf("tuning: interpret max_worker_processes %q: %w", workerProcesses, err)
	}
	return target, nil
}

// Apply makes the system-scoped changes in plan, recording each original value
// before changing it. Session-scoped changes are not applied here: they belong to
// the sessions that copy and build indexes, which open their own connections, so
// Plan.SessionGUCs is threaded into those runners instead.
//
// The returned changes are those actually applied, in order. On failure the
// changes already applied are returned alongside the error, so a caller running
// best-effort knows what state the target is in.
func Apply(ctx context.Context, conn Conn, plan Plan, recorder Recorder) ([]Change, error) {
	var applied []Change
	var failure error
	for _, change := range plan.SystemChanges() {
		// Recording first is what makes an interrupted apply recoverable: a
		// crash between the record and the ALTER leaves a revertible setting
		// that was never changed, which reverts to itself.
		recorded, err := recorder.Recorded(ctx, change.Name)
		if err != nil {
			failure = err
			break
		}
		if !recorded {
			if err := recorder.Record(ctx, change); err != nil {
				failure = err
				break
			}
		}
		if err := alterSystemSet(ctx, conn, change.Name, change.To); err != nil {
			failure = err
			break
		}
		applied = append(applied, change)
	}
	// Reload even when a later change failed. ALTER SYSTEM writes
	// postgresql.auto.conf without activating anything, so returning here without
	// a reload would leave the successful changes inert but recorded as applied,
	// and they would then take effect at whatever unrelated reload came next.
	if len(applied) != 0 {
		if err := reload(ctx, conn); err != nil {
			return applied, errors.Join(failure, err)
		}
	}
	return applied, failure
}

// Revert restores the recorded changes. It reverts every change it can and
// returns the joined errors, so one setting that refuses to move does not leave
// the rest tuned for a bulk load.
func Revert(ctx context.Context, conn Conn, changes []Change) error {
	var errs []error
	reverted := false
	for _, change := range changes {
		if change.Scope != ScopeSystem {
			// Session settings died with the sessions that set them.
			continue
		}
		var err error
		if change.ResetOnRevert {
			err = alterSystemReset(ctx, conn, change.Name)
		} else {
			err = alterSystemSet(ctx, conn, change.Name, change.From)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		reverted = true
	}
	if reverted {
		if err := reload(ctx, conn); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func reload(ctx context.Context, conn Conn) error {
	if _, err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("tuning: reload target configuration: %w", err)
	}
	return nil
}

// ALTER SYSTEM takes neither the setting name nor its value as a bind parameter,
// so both are interpolated. managed and validValue below are what keeps that
// safe; nothing reaches here that an operator has not already had validated by
// the flag parser, but a setting name is close enough to an identifier that an
// allowlist is cheaper than reasoning about it.
func alterSystemSet(ctx context.Context, conn Conn, name, value string) error {
	if err := checkSettable(name, value); err != nil {
		return err
	}
	statement := fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", name, value)
	if _, err := conn.Exec(ctx, statement); err != nil {
		return fmt.Errorf("tuning: set %s to %s on the target: %w", name, value, err)
	}
	return nil
}

func alterSystemReset(ctx context.Context, conn Conn, name string) error {
	if !managed(name) {
		return fmt.Errorf("tuning: %s is not a managed setting", name)
	}
	if _, err := conn.Exec(ctx, "ALTER SYSTEM RESET "+name); err != nil {
		return fmt.Errorf("tuning: reset %s on the target: %w", name, err)
	}
	return nil
}

func checkSettable(name, value string) error {
	if !managed(name) {
		return fmt.Errorf("tuning: %s is not a managed setting", name)
	}
	if !validValue(value) {
		return fmt.Errorf("tuning: %q is not a usable value for %s", value, name)
	}
	return nil
}

func managed(name string) bool {
	for _, candidate := range Managed {
		if candidate == name {
			return true
		}
	}
	return false
}

// validValue accepts the shapes PostgreSQL settings actually take: a number with
// an optional unit, or a bare word such as "off".
func validValue(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '.', r == '-':
		default:
			return false
		}
	}
	return true
}

// ApplySession sets the plan's session-scoped settings on conn and returns those
// that took effect, together with the joined errors for any that did not.
//
// This is how session settings are checked before the load starts: the copy and
// index-build runners open many short-lived connections, and a setting that is
// going to be refused should be found out once here rather than by failing an
// index build hours later. A caller running best effort keeps the returned subset
// and reports the error; a strict caller treats any error as fatal.
func ApplySession(ctx context.Context, conn Conn, plan Plan) (map[string]string, error) {
	gucs := plan.SessionGUCs()
	names := make([]string, 0, len(gucs))
	for name := range gucs {
		names = append(names, name)
	}
	// Stable order keeps a failure reproducible.
	slices.Sort(names)

	applied := make(map[string]string, len(gucs))
	var errs []error
	for _, name := range names {
		if err := SetSession(ctx, conn, name, gucs[name]); err != nil {
			errs = append(errs, err)
			continue
		}
		applied[name] = gucs[name]
	}
	return applied, errors.Join(errs...)
}

// SetSession applies one session-scoped setting. Unlike ALTER SYSTEM this binds
// both name and value, so it needs no allowlist.
func SetSession(ctx context.Context, conn Conn, name, value string) error {
	if _, err := conn.Exec(ctx, "SELECT set_config($1,$2,false)", name, value); err != nil {
		return fmt.Errorf("tuning: set %s to %s for this session: %w", name, value, err)
	}
	return nil
}
