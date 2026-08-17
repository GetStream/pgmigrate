// Package indexbuild reconstructs indexes and deferred constraints after copy.
package indexbuild

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/GetStream/pgmigrate/internal/postgres"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/GetStream/pgmigrate/internal/tuning"
	"github.com/jackc/pgx/v5"
)

type Index struct {
	OID, TableOID                   uint32
	Schema, Table, Name, Definition string
	Bytes                           int64
	// SelectedOID is the copied table this index belongs to: the index's own
	// table, or the root of its partition tree when the table is a partition.
	SelectedOID uint32
	// Partitioned marks an index on a partitioned parent. Such an index is
	// metadata only: it is rendered ON ONLY, holds no data, and stays invalid
	// until every partition's matching index is attached to it.
	Partitioned bool
	// ParentIndexSchema and ParentIndexName name the partitioned index this one
	// must be attached to, empty for an index outside a partition tree.
	ParentIndexSchema, ParentIndexName string
	// Expression marks an index with an expression key or a partial predicate.
	// Those are stored as parse trees, which PostgreSQL may normalize into a
	// shape that deparses differently from the text it was given, so their
	// rendering cannot be compared across two databases.
	Expression bool
	// Unique indexes and replica-identity indexes must exist before CDC replay
	// starts. Without them, replay can silently accept duplicate keys or turn
	// keyed UPDATE/DELETE operations into heap scans.
	Unique, ReplicaIdentity        bool
	ConstraintOID                  uint32
	ConstraintName, ConstraintType string
	// ConstraintDefinition is pg_get_constraintdef for the primary key or
	// unique constraint this index backs, needed where the constraint must be
	// declared directly instead of adopting a pre-built index.
	ConstraintDefinition        string
	ConstraintDeferrable        bool
	ConstraintInitiallyDeferred bool
}

type Constraint struct {
	OID, TableOID                         uint32
	Schema, Table, Name, Kind, Definition string
	// SelectedOID is the copied table this constraint belongs to.
	SelectedOID uint32
	// Partitioned marks a constraint on a partitioned parent, where PostgreSQL
	// applies it to every partition and rejects some clauses ordinary tables
	// accept.
	Partitioned bool
	// ReferencesPartitioned marks a foreign key whose referenced table is
	// partitioned. PostgreSQL then keeps one clone of the constraint per
	// partition, and VALIDATE CONSTRAINT marks only the constraint it is given,
	// leaving those clones unvalidated for good.
	ReferencesPartitioned bool
}

// CollisionError reports an existing target object that does not match the
// source catalog identity and definition.
type CollisionError struct {
	Kind, Schema, Name, Have, Want string
}

func (e *CollisionError) Error() string {
	return fmt.Sprintf("%s collision for %s.%s: have %q, want %q", e.Kind, e.Schema, e.Name, e.Have, e.Want)
}

// Inventory reads server-rendered definitions so expression, predicate,
// collation, opclass, include, deferrability, and action clauses are retained.
// Indexes and constraints of partition children are included: the children hold
// the data, and their indexes must exist and be attached before the partitioned
// parent's own index becomes valid.
func Inventory(ctx context.Context, source *pgx.Conn, selected func(uint32) bool) ([]Index, []Constraint, error) {
	if err := postgres.PinSearchPath(ctx, source); err != nil {
		return nil, nil, err
	}
	rows, err := source.Query(ctx, `
		SELECT i.indexrelid, i.indrelid,
		       coalesce(pg_catalog.pg_partition_root(t.oid), t.oid)::oid,
		       nt.nspname, t.relname, x.relname,
		       pg_get_indexdef(i.indexrelid), pg_relation_size(i.indexrelid),
		       t.relkind='p', i.indexprs IS NOT NULL OR i.indpred IS NOT NULL,
		       i.indisunique, i.indisreplident,
		       coalesce(np.nspname,''), coalesce(xp.relname,''),
		       coalesce(c.oid,0), coalesce(c.conname,''), coalesce(c.contype::text,''),
		       coalesce(pg_get_constraintdef(c.oid,true),''),
		       coalesce(c.condeferrable,false),coalesce(c.condeferred,false)
		FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid
		JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace nt ON nt.oid=t.relnamespace
		LEFT JOIN pg_inherits h ON h.inhrelid=x.oid
		LEFT JOIN pg_class xp ON xp.oid=h.inhparent
		LEFT JOIN pg_namespace np ON np.oid=xp.relnamespace
		LEFT JOIN pg_constraint c ON c.conindid=i.indexrelid AND c.contype IN ('p','u','x')
		WHERE nt.nspname NOT IN ('pg_catalog','information_schema')
		  AND nt.nspname !~ '^pg_toast' AND coalesce(c.contype::text,'') <> 'x'`)
	if err != nil {
		return nil, nil, fmt.Errorf("inventory indexes: %w", err)
	}
	var indexes []Index
	for rows.Next() {
		var x Index
		if err := rows.Scan(&x.OID, &x.TableOID, &x.SelectedOID, &x.Schema, &x.Table, &x.Name,
			&x.Definition, &x.Bytes, &x.Partitioned, &x.Expression,
			&x.Unique, &x.ReplicaIdentity,
			&x.ParentIndexSchema, &x.ParentIndexName,
			&x.ConstraintOID, &x.ConstraintName, &x.ConstraintType, &x.ConstraintDefinition,
			&x.ConstraintDeferrable, &x.ConstraintInitiallyDeferred); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if selected == nil || selected(x.SelectedOID) {
			indexes = append(indexes, x)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	// Inherited copies of a parent's constraint are excluded: adding the
	// constraint to the parent creates them, and pg_dump likewise never emits
	// them separately.
	rows, err = source.Query(ctx, `
		SELECT c.oid,c.conrelid,
		       coalesce(pg_catalog.pg_partition_root(t.oid), t.oid)::oid,
		       n.nspname,t.relname,c.conname,c.contype::text,
		       pg_get_constraintdef(c.oid,true), t.relkind='p',
		       coalesce(f.relkind='p',false)
		FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
		JOIN pg_namespace n ON n.oid=t.relnamespace
		LEFT JOIN pg_class f ON f.oid=c.confrelid
		WHERE c.contype IN ('f','c','x') AND c.conislocal
		  AND n.nspname NOT IN ('pg_catalog','information_schema')
		  AND n.nspname !~ '^pg_toast'`)
	if err != nil {
		return nil, nil, fmt.Errorf("inventory constraints: %w", err)
	}
	defer rows.Close()
	var constraints []Constraint
	for rows.Next() {
		var c Constraint
		if err := rows.Scan(&c.OID, &c.TableOID, &c.SelectedOID, &c.Schema, &c.Table, &c.Name,
			&c.Kind, &c.Definition, &c.Partitioned, &c.ReferencesPartitioned); err != nil {
			return nil, nil, err
		}
		if selected == nil || selected(c.SelectedOID) {
			constraints = append(constraints, c)
		}
	}
	return indexes, constraints, rows.Err()
}

type State interface {
	UpsertIndex(context.Context, state.Index) error
	CompleteIndex(context.Context, uint32) error
	IndexCompleted(context.Context, uint32) (bool, error)
	RecordIndexTargetDefinition(context.Context, uint32, string) error
	IndexTargetDefinition(context.Context, uint32) (string, error)
	UpsertConstraint(context.Context, state.Constraint) error
	CompleteConstraint(context.Context, uint32) error
	ConstraintCompleted(context.Context, uint32) (bool, error)
	RecordConstraintTargetDefinition(context.Context, uint32, string) error
	ConstraintTargetDefinition(context.Context, uint32) (string, error)
}

type Runner struct {
	Target  func(context.Context) (*pgx.Conn, error)
	Workers int
	State   State
	// SessionGUCs are settings applied to every worker session, which is where
	// index builds get the memory and parallelism the target was tuned for.
	// Empty leaves the target's own defaults in place.
	SessionGUCs                   map[string]string
	MaintenanceWorkMem            string
	MaxParallelMaintenanceWorkers int
	AfterManaged                  func(context.Context) error
	Log                           func(event string, values map[string]any)
}

// ReplayPlan separates objects required for correct CDC replay from secondary
// indexes that can be built concurrently with replay.
type ReplayPlan struct {
	CriticalIndexes []Index
	DeferredIndexes []Index
	Constraints     []Constraint
}

// PlanReplay classifies all uniqueness, replica identity, constraint-backed,
// and partitioned-parent indexes as replay critical. Partition children inherit
// criticality from their parent index.
func PlanReplay(indexes []Index, constraints []Constraint) ReplayPlan {
	criticalNames := make(map[string]bool, len(indexes))
	isCritical := make(map[uint32]bool, len(indexes))
	for _, x := range indexes {
		critical := x.ConstraintOID != 0 || x.Unique || x.ReplicaIdentity || x.Partitioned
		isCritical[x.OID] = critical
		if critical {
			criticalNames[indexName(x.Schema, x.Name)] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for _, x := range indexes {
			if isCritical[x.OID] || x.ParentIndexName == "" ||
				!criticalNames[indexName(x.ParentIndexSchema, x.ParentIndexName)] {
				continue
			}
			isCritical[x.OID] = true
			criticalNames[indexName(x.Schema, x.Name)] = true
			changed = true
		}
	}
	plan := ReplayPlan{Constraints: append([]Constraint(nil), constraints...)}
	for _, x := range indexes {
		if isCritical[x.OID] {
			plan.CriticalIndexes = append(plan.CriticalIndexes, x)
		} else {
			plan.DeferredIndexes = append(plan.DeferredIndexes, x)
		}
	}
	return plan
}

func indexName(schema, name string) string { return schema + "\x00" + name }

// Run builds largest indexes first, attaches PK/unique constraints, creates
// foreign keys NOT VALID then validates them, and restores managed post-data.
func (r Runner) Run(ctx context.Context, indexes []Index, constraints []Constraint) error {
	if err := r.prepare(ctx, indexes, constraints); err != nil {
		return err
	}
	indexes = LargestFirst(indexes)
	if err := r.runIndexes(ctx, indexes, false); err != nil {
		return err
	}
	if err := r.attachPartitionIndexes(ctx, indexes); err != nil {
		return err
	}
	if err := r.runConstraints(ctx, constraints); err != nil {
		return err
	}
	return r.runAfterManaged(ctx)
}

// RunCritical builds everything that must exist before CDC replay can safely
// start. It also restores deferred rules and triggers before replay so their
// appearance cannot change apply semantics mid-stream.
func (r Runner) RunCritical(ctx context.Context, plan ReplayPlan) error {
	allIndexes := append(append([]Index(nil), plan.CriticalIndexes...), plan.DeferredIndexes...)
	if err := r.prepare(ctx, allIndexes, plan.Constraints); err != nil {
		return err
	}
	indexes := LargestFirst(plan.CriticalIndexes)
	if err := r.runIndexes(ctx, indexes, false); err != nil {
		return err
	}
	if err := r.attachPartitionIndexes(ctx, indexes); err != nil {
		return err
	}
	if err := r.runConstraints(ctx, plan.Constraints); err != nil {
		return err
	}
	return r.runAfterManaged(ctx)
}

// RunDeferred builds non-unique secondary indexes with PostgreSQL's concurrent
// algorithm so target DML remains available to the CDC applier.
func (r Runner) RunDeferred(ctx context.Context, plan ReplayPlan) error {
	if err := r.prepare(ctx, plan.DeferredIndexes, nil); err != nil {
		return err
	}
	indexes := LargestFirst(plan.DeferredIndexes)
	if err := r.runIndexes(ctx, indexes, true); err != nil {
		return err
	}
	return r.attachPartitionIndexes(ctx, indexes)
}

func (r *Runner) prepare(ctx context.Context, indexes []Index, constraints []Constraint) error {
	if r.Target == nil || r.State == nil {
		return errors.New("target and state are required")
	}
	if r.Workers < 1 {
		r.Workers = 1
	}
	for _, x := range indexes {
		if err := r.State.UpsertIndex(ctx, state.Index{OID: x.OID, TableOID: x.SelectedOID, Name: x.Name, Definition: x.Definition, Bytes: x.Bytes}); err != nil {
			return err
		}
	}
	for _, c := range constraints {
		if err := r.State.UpsertConstraint(ctx, state.Constraint{OID: c.OID, TableOID: c.SelectedOID, Name: c.Name, Kind: c.Kind, Definition: c.Definition}); err != nil {
			return err
		}
	}
	return nil
}

func (r Runner) runConstraints(ctx context.Context, constraints []Constraint) error {
	var foreign []Constraint
	for _, c := range constraints {
		done, err := r.State.ConstraintCompleted(ctx, c.OID)
		if err != nil {
			return err
		}
		if done {
			continue
		}
		if err := r.ensureConstraint(ctx, c); err != nil {
			return err
		}
		if c.Kind == "f" {
			foreign = append(foreign, c)
		} else {
			if err := r.State.CompleteConstraint(ctx, c.OID); err != nil {
				return err
			}
		}
	}
	if err := r.validateForeignKeys(ctx, foreign); err != nil {
		return err
	}
	return nil
}

func (r Runner) runAfterManaged(ctx context.Context) error {
	if r.AfterManaged != nil {
		if err := r.AfterManaged(ctx); err != nil {
			return fmt.Errorf("restore deferred post-data: %w", err)
		}
	}
	// Statistics are not gathered here. The orchestrator runs VACUUM (ANALYZE)
	// over the copied tables once this phase completes, which gives the planner
	// the same statistics an ANALYZE would and additionally populates the
	// visibility map that the verifier's keys-only scan depends on. Doing it here
	// would mean vacuuming every table a second time.
	return nil
}

// LargestFirst returns a stable, independently owned build schedule.
func LargestFirst(indexes []Index) []Index {
	result := append([]Index(nil), indexes...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].Bytes > result[j].Bytes })
	return result
}

func (r Runner) runIndexes(ctx context.Context, indexes []Index, concurrently bool) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan Index)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for range r.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for x := range jobs {
				done, err := r.State.IndexCompleted(runCtx, x.OID)
				if err == nil && !done {
					err = r.ensureIndexMode(runCtx, x, concurrently)
					if err == nil {
						err = r.State.CompleteIndex(runCtx, x.OID)
					}
				}
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
						cancel()
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
send:
	for _, x := range indexes {
		select {
		case jobs <- x:
		case <-runCtx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	return first
}

func (r Runner) exec(ctx context.Context, sql string) error {
	conn, err := r.openWorker(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, sql)
	return err
}

func (r Runner) openWorker(ctx context.Context) (*pgx.Conn, error) {
	conn, err := r.Target(ctx)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close(context.Background())
		}
	}()
	if err := postgres.PinSearchPath(ctx, conn); err != nil {
		return nil, err
	}
	// SessionGUCs is applied in sorted order so that a refusal is reproducible.
	// The explicit fields below predate it and still win, so a caller that sets
	// one directly is not overridden by the derived plan.
	names := make([]string, 0, len(r.SessionGUCs))
	for name := range r.SessionGUCs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := tuning.SetSession(ctx, conn, name, r.SessionGUCs[name]); err != nil {
			return nil, err
		}
	}
	if r.MaintenanceWorkMem != "" {
		if _, err := conn.Exec(ctx, "SELECT set_config('maintenance_work_mem',$1,false)", r.MaintenanceWorkMem); err != nil {
			return nil, fmt.Errorf("set maintenance_work_mem: %w", err)
		}
	}
	if r.MaxParallelMaintenanceWorkers > 0 {
		if _, err := conn.Exec(ctx, "SELECT set_config('max_parallel_maintenance_workers',$1,false)",
			fmt.Sprintf("%d", r.MaxParallelMaintenanceWorkers)); err != nil {
			return nil, fmt.Errorf("set max_parallel_maintenance_workers: %w", err)
		}
	}
	closeOnError = false
	return conn, nil
}

type targetIndex struct {
	OID                            uint32
	TableSchema, Table, Definition string
	Expression                     bool
	Valid, Ready                   bool
	ConstraintName, ConstraintType string
	ConstraintDeferrable           bool
	ConstraintInitiallyDeferred    bool
}

func (r Runner) inspectIndex(ctx context.Context, x Index) (targetIndex, bool, error) {
	conn, err := r.openWorker(ctx)
	if err != nil {
		return targetIndex{}, false, err
	}
	defer conn.Close(context.Background())
	var got targetIndex
	err = conn.QueryRow(ctx, `
		SELECT i.indexrelid, nt.nspname, t.relname, pg_get_indexdef(i.indexrelid),
		       i.indexprs IS NOT NULL OR i.indpred IS NOT NULL,
		       i.indisvalid, i.indisready,
		       coalesce(c.conname,''),coalesce(c.contype::text,''),
		       coalesce(c.condeferrable,false),coalesce(c.condeferred,false)
		FROM pg_class ix JOIN pg_namespace ni ON ni.oid=ix.relnamespace
		JOIN pg_index i ON i.indexrelid=ix.oid
		JOIN pg_class t ON t.oid=i.indrelid
		JOIN pg_namespace nt ON nt.oid=t.relnamespace
		LEFT JOIN pg_constraint c ON c.conindid=i.indexrelid
		WHERE ni.nspname=$1 AND ix.relname=$2`, x.Schema, x.Name).
		Scan(&got.OID, &got.TableSchema, &got.Table, &got.Definition, &got.Expression,
			&got.Valid, &got.Ready,
			&got.ConstraintName, &got.ConstraintType, &got.ConstraintDeferrable,
			&got.ConstraintInitiallyDeferred)
	if errors.Is(err, pgx.ErrNoRows) {
		return targetIndex{}, false, nil
	}
	if err != nil {
		return targetIndex{}, false, err
	}
	return got, true, nil
}

func (r Runner) ensureIndex(ctx context.Context, x Index) error {
	return r.ensureIndexMode(ctx, x, false)
}

func (r Runner) ensureIndexMode(ctx context.Context, x Index, concurrently bool) error {
	got, exists, err := r.inspectIndex(ctx, x)
	if err != nil {
		return err
	}
	if exists && (!got.Valid || !got.Ready) {
		if !concurrently {
			return fmt.Errorf("index %s.%s exists but is not valid and ready", x.Schema, x.Name)
		}
		if err := r.dropInvalidConcurrentIndex(ctx, x); err != nil {
			return err
		}
		exists = false
		got = targetIndex{}
	}
	if exists {
		if err := r.matchIndex(ctx, x, got); err != nil {
			return err
		}
	} else {
		if err := r.createIndexMode(ctx, x, concurrently); err != nil {
			return err
		}
		got, exists, err = r.inspectIndex(ctx, x)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("index %s.%s is absent after creating it", x.Schema, x.Name)
		}
		if got.TableSchema != x.Schema || got.Table != x.Table {
			return &CollisionError{
				Kind: "index", Schema: x.Schema, Name: x.Name,
				Have: got.TableSchema + "." + got.Table, Want: x.Schema + "." + x.Table,
			}
		}
		if err := r.State.RecordIndexTargetDefinition(ctx, x.OID, normalize(got.Definition)); err != nil {
			return err
		}
	}
	if x.ConstraintOID == 0 {
		if got.ConstraintName != "" {
			return &CollisionError{Kind: "index constraint", Schema: x.Schema, Name: x.Name, Have: got.ConstraintName, Want: "unattached"}
		}
		return nil
	}
	if got.ConstraintName == "" {
		if x.Partitioned {
			// createIndex declares the constraint for a partitioned parent, so
			// reaching here means an earlier attempt left a bare index that
			// PostgreSQL cannot promote.
			return fmt.Errorf("index %s.%s on partitioned table %s backs constraint %s but exists without it; "+
				"drop the index and resume so the constraint can be declared directly",
				x.Schema, x.Name, x.Table, x.ConstraintName)
		}
		kind := "UNIQUE"
		if x.ConstraintType == "p" {
			kind = "PRIMARY KEY"
		}
		deferrability := ""
		if x.ConstraintDeferrable {
			deferrability = " DEFERRABLE"
			if x.ConstraintInitiallyDeferred {
				deferrability += " INITIALLY DEFERRED"
			} else {
				deferrability += " INITIALLY IMMEDIATE"
			}
		}
		if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s USING INDEX %s%s",
			pgx.Identifier{x.Schema, x.Table}.Sanitize(), pgx.Identifier{x.ConstraintName}.Sanitize(),
			kind, pgx.Identifier{x.Name}.Sanitize(), deferrability)); err != nil {
			return fmt.Errorf("attach constraint %s: %w", x.ConstraintName, err)
		}
		got, _, err = r.inspectIndex(ctx, x)
		if err != nil {
			return err
		}
	}
	if got.ConstraintName != x.ConstraintName || got.ConstraintType != x.ConstraintType ||
		got.ConstraintDeferrable != x.ConstraintDeferrable ||
		got.ConstraintInitiallyDeferred != x.ConstraintInitiallyDeferred {
		return &CollisionError{
			Kind: "index constraint", Schema: x.Schema, Name: x.Name,
			Have: fmt.Sprintf("%s %s deferrable=%v deferred=%v", got.ConstraintType, got.ConstraintName, got.ConstraintDeferrable, got.ConstraintInitiallyDeferred),
			Want: fmt.Sprintf("%s %s deferrable=%v deferred=%v", x.ConstraintType, x.ConstraintName, x.ConstraintDeferrable, x.ConstraintInitiallyDeferred),
		}
	}
	return nil
}

func (r Runner) dropInvalidConcurrentIndex(ctx context.Context, x Index) error {
	if err := r.exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+
		pgx.Identifier{x.Schema, x.Name}.Sanitize()); err != nil {
		return fmt.Errorf("drop invalid concurrent index %s.%s: %w", x.Schema, x.Name, err)
	}
	return nil
}

// matchIndex decides whether an index already on the target is the one the
// source describes.
//
// Once pgmigrate has recorded how the target renders an index, that recording is
// the expectation: it and the current rendering come from the same server, so
// comparing them is exact and catches real drift. Before that, the only
// available comparison is against the source's rendering, which is sound only
// while no parse tree is involved — pg_get_indexdef of an expression or a
// partial predicate can differ between two servers holding the same index,
// because the parser normalizes expressions in ways deparsing does not
// reproduce. Such an index is adopted with its difference logged rather than
// refused, since refusing it left migrations unable to resume past an index
// pgmigrate had itself created.
func (r Runner) matchIndex(ctx context.Context, x Index, got targetIndex) error {
	if got.TableSchema != x.Schema || got.Table != x.Table {
		return &CollisionError{
			Kind: "index", Schema: x.Schema, Name: x.Name,
			Have: got.TableSchema + "." + got.Table, Want: x.Schema + "." + x.Table,
		}
	}
	expected, err := r.State.IndexTargetDefinition(ctx, x.OID)
	if err != nil {
		return err
	}
	switch {
	case expected != "":
		if normalize(got.Definition) != expected {
			return &CollisionError{
				Kind: "index", Schema: x.Schema, Name: x.Name,
				Have: got.Definition, Want: expected,
			}
		}
		return nil
	case got.Expression != x.Expression:
		return &CollisionError{
			Kind: "index", Schema: x.Schema, Name: x.Name,
			Have: got.Definition, Want: x.Definition,
		}
	case !x.Expression && normalize(got.Definition) != normalize(x.Definition):
		return &CollisionError{
			Kind: "index", Schema: x.Schema, Name: x.Name,
			Have: got.Definition, Want: x.Definition,
		}
	}
	r.adopted("index", x.Schema, x.Name, got.Definition, x.Definition)
	return r.State.RecordIndexTargetDefinition(ctx, x.OID, normalize(got.Definition))
}

// adopted records the first sighting of an object pgmigrate did not create in
// this run. Preflight refuses a target that holds any relation, so the object
// is pgmigrate's own work from an attempt that died before recording it, or was
// restored from the source's schema dump; either way the target's rendering is
// the expectation to hold later resumes to. A rendering that differs from the
// source's is worth reporting because it is the only visible trace of a
// deparse or search_path artifact.
func (r Runner) adopted(kind, schema, name, target, source string) {
	if r.Log == nil || normalize(target) == normalize(source) {
		return
	}
	r.Log("adopted_"+kind, map[string]any{
		"schema": schema, "name": name, "target": target, "source": source,
	})
}

type targetConstraint struct {
	Kind, Definition string
	Validated        bool
}

func (r Runner) inspectConstraint(ctx context.Context, c Constraint) (targetConstraint, bool, error) {
	conn, err := r.openWorker(ctx)
	if err != nil {
		return targetConstraint{}, false, err
	}
	defer conn.Close(context.Background())
	var got targetConstraint
	err = conn.QueryRow(ctx, `
		SELECT c.contype::text,pg_get_constraintdef(c.oid,true),c.convalidated
		FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
		JOIN pg_namespace n ON n.oid=t.relnamespace
		WHERE n.nspname=$1 AND t.relname=$2 AND c.conname=$3`,
		c.Schema, c.Table, c.Name).Scan(&got.Kind, &got.Definition, &got.Validated)
	if errors.Is(err, pgx.ErrNoRows) {
		return targetConstraint{}, false, nil
	}
	if err != nil {
		return targetConstraint{}, false, err
	}
	return got, true, nil
}

// createIndex builds an index from the source's rendering, except where a
// partitioned parent forbids it: PostgreSQL rejects
// ALTER TABLE ... ADD CONSTRAINT ... USING INDEX on a partitioned table, so a
// primary key or unique constraint there cannot be built as a bare index and
// adopted afterwards. Declaring the constraint creates its index instead. ONLY
// keeps it off the partitions, which carry their own constraints and attach to
// this one.
func (r Runner) createIndex(ctx context.Context, x Index) error {
	return r.createIndexMode(ctx, x, false)
}

func (r Runner) createIndexMode(ctx context.Context, x Index, concurrently bool) error {
	if x.Partitioned && x.ConstraintOID != 0 {
		if x.ConstraintDefinition == "" {
			return fmt.Errorf("constraint %s on partitioned table %s.%s has no definition",
				x.ConstraintName, x.Schema, x.Table)
		}
		if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE ONLY %s ADD CONSTRAINT %s %s",
			pgx.Identifier{x.Schema, x.Table}.Sanitize(),
			pgx.Identifier{x.ConstraintName}.Sanitize(), x.ConstraintDefinition)); err != nil {
			return fmt.Errorf("add constraint %s to partitioned table %s.%s: %w",
				x.ConstraintName, x.Schema, x.Table, err)
		}
		return nil
	}
	definition := x.Definition
	if concurrently {
		var err error
		definition, err = concurrentIndexDefinition(x)
		if err != nil {
			return err
		}
	}
	if err := r.exec(ctx, definition); err != nil {
		return fmt.Errorf("create index %s.%s: %w", x.Schema, x.Name, err)
	}
	return nil
}

func concurrentIndexDefinition(x Index) (string, error) {
	if x.Partitioned || x.ConstraintOID != 0 || x.Unique || x.ReplicaIdentity {
		return "", fmt.Errorf("index %s.%s is replay-critical and cannot be deferred", x.Schema, x.Name)
	}
	const prefix = "CREATE INDEX"
	if len(x.Definition) <= len(prefix) ||
		!strings.EqualFold(x.Definition[:len(prefix)], prefix) ||
		x.Definition[len(prefix)] != ' ' {
		return "", fmt.Errorf("index %s.%s has unsupported definition %q", x.Schema, x.Name, x.Definition)
	}
	return x.Definition[:len(prefix)] + " CONCURRENTLY" + x.Definition[len(prefix):], nil
}

// attachPartitionIndexes attaches every partition's index to the partitioned
// index above it. A partitioned parent's index is created ON ONLY and stays
// invalid — the planner ignores it — until all of its partitions' indexes are
// attached, so this pass is what makes those indexes usable. It runs after all
// index builds because either side of an attachment may still be missing while
// builds are in flight. Re-attaching an attached index is a no-op, so the pass
// is safe to repeat on resume.
func (r Runner) attachPartitionIndexes(ctx context.Context, indexes []Index) error {
	for _, x := range indexes {
		if x.ParentIndexName == "" {
			continue
		}
		if err := r.exec(ctx, fmt.Sprintf("ALTER INDEX %s ATTACH PARTITION %s",
			pgx.Identifier{x.ParentIndexSchema, x.ParentIndexName}.Sanitize(),
			pgx.Identifier{x.Schema, x.Name}.Sanitize())); err != nil {
			return fmt.Errorf("attach index %s.%s to %s.%s: %w",
				x.Schema, x.Name, x.ParentIndexSchema, x.ParentIndexName, err)
		}
	}
	return nil
}

func (r Runner) ensureConstraint(ctx context.Context, c Constraint) error {
	got, exists, err := r.inspectConstraint(ctx, c)
	if err != nil {
		return err
	}
	if exists {
		return r.matchConstraint(ctx, c, got)
	}
	def := c.Definition
	// A foreign key is added NOT VALID and validated afterwards to keep the
	// exclusive lock short. Two shapes cannot take that route: a partitioned
	// table rejects NOT VALID before PostgreSQL 18, and a foreign key that
	// references a partitioned table is cloned once per partition, where
	// VALIDATE CONSTRAINT marks only the constraint named and leaves every clone
	// unvalidated permanently — a divergence from the source that no later
	// statement can repair. Both are added validated in one step.
	if c.Kind == "f" && !c.Partitioned && !c.ReferencesPartitioned &&
		!strings.Contains(strings.ToUpper(def), "NOT VALID") {
		def += " NOT VALID"
	}
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
		pgx.Identifier{c.Schema, c.Table}.Sanitize(), pgx.Identifier{c.Name}.Sanitize(), def)); err != nil {
		return fmt.Errorf("create constraint %s: %w", c.Name, err)
	}
	got, exists, err = r.inspectConstraint(ctx, c)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("constraint %s on %s.%s is absent after creating it", c.Name, c.Schema, c.Table)
	}
	if got.Kind != c.Kind {
		return &CollisionError{
			Kind: "constraint", Schema: c.Schema, Name: c.Name,
			Have: got.Kind, Want: c.Kind,
		}
	}
	return r.State.RecordConstraintTargetDefinition(ctx, c.OID, normalizeConstraint(got.Definition))
}

// matchConstraint decides whether a constraint already on the target is the one
// the source describes.
//
// A recorded target rendering is compared exactly, as for an index. Before that
// recording exists — which is the ordinary case for a CHECK constraint, since
// the schema restore creates it with its table — the source's rendering is not
// directly comparable: PostgreSQL pushes a cast on an array down onto its
// elements, so a constraint written years ago deparses differently from the same
// constraint reparsed today, and no DDL can make the two texts agree. Where the
// renderings differ, the source's definition is put through the target's own
// parser and the results compared, which is sound because both texts then come
// from one server.
func (r Runner) matchConstraint(ctx context.Context, c Constraint, got targetConstraint) error {
	if got.Kind != c.Kind {
		return &CollisionError{
			Kind: "constraint", Schema: c.Schema, Name: c.Name,
			Have: got.Kind, Want: c.Kind,
		}
	}
	expected, err := r.State.ConstraintTargetDefinition(ctx, c.OID)
	if err != nil {
		return err
	}
	if expected != "" {
		if normalizeConstraint(got.Definition) != expected {
			return &CollisionError{
				Kind: "constraint", Schema: c.Schema, Name: c.Name,
				Have: got.Definition, Want: expected,
			}
		}
		return nil
	}
	if normalizeConstraint(got.Definition) != normalizeConstraint(c.Definition) {
		alike, err := r.rendersAlike(ctx, c, got.Definition)
		if err != nil {
			return err
		}
		if !alike {
			return &CollisionError{
				Kind: "constraint", Schema: c.Schema, Name: c.Name,
				Have: got.Definition, Want: c.Definition,
			}
		}
	}
	r.adopted("constraint", c.Schema, c.Name, got.Definition, c.Definition)
	return r.State.RecordConstraintTargetDefinition(ctx, c.OID, normalizeConstraint(got.Definition))
}

// rendersAlike reports whether the target renders the source's definition the
// way it renders the constraint already in place. The definition is applied to
// the target under a scratch name in a transaction that is always rolled back,
// so both texts come from one parser under one search_path. NOT VALID keeps the
// probe from scanning the table.
//
// An exclusion constraint would have to build an index, and a partitioned table
// rejects NOT VALID before PostgreSQL 18, so neither is probed; those are
// adopted with the difference logged, which is safe because pgmigrate creates
// them itself and so is looking at its own earlier work.
func (r Runner) rendersAlike(ctx context.Context, c Constraint, target string) (bool, error) {
	if c.Kind != "c" && c.Kind != "f" || c.Partitioned {
		return true, nil
	}
	conn, err := r.openWorker(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	probe := fmt.Sprintf("pgmigrate_probe_%d", c.OID)
	definition := c.Definition
	if !strings.Contains(strings.ToUpper(definition), "NOT VALID") {
		definition += " NOT VALID"
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s",
		pgx.Identifier{c.Schema, c.Table}.Sanitize(),
		pgx.Identifier{probe}.Sanitize(), definition)); err != nil {
		// The target cannot hold the constraint the source describes, so the two
		// are not the same constraint.
		if r.Log != nil {
			r.Log("constraint_probe_failed", map[string]any{
				"schema": c.Schema, "name": c.Name, "error": err.Error(),
			})
		}
		return false, nil
	}
	var reparsed string
	if err := tx.QueryRow(ctx, `
		SELECT pg_get_constraintdef(c.oid,true)
		FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
		JOIN pg_namespace n ON n.oid=t.relnamespace
		WHERE n.nspname=$1 AND t.relname=$2 AND c.conname=$3`,
		c.Schema, c.Table, probe).Scan(&reparsed); err != nil {
		return false, err
	}
	return normalizeConstraint(reparsed) == normalizeConstraint(target), nil
}

func (r Runner) validateForeignKeys(ctx context.Context, constraints []Constraint) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan Constraint)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	tableLocks := map[string]*sync.Mutex{}
	for _, c := range constraints {
		key := c.Schema + "\x00" + c.Table
		if tableLocks[key] == nil {
			tableLocks[key] = &sync.Mutex{}
		}
	}
	for range r.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				lock := tableLocks[c.Schema+"\x00"+c.Table]
				lock.Lock()
				got, _, err := r.inspectConstraint(runCtx, c)
				if err == nil && !got.Validated {
					err = r.exec(runCtx, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s",
						pgx.Identifier{c.Schema, c.Table}.Sanitize(), pgx.Identifier{c.Name}.Sanitize()))
				}
				if err == nil {
					err = r.State.CompleteConstraint(runCtx, c.OID)
				}
				lock.Unlock()
				if err != nil {
					mu.Lock()
					if first == nil {
						first = fmt.Errorf("validate constraint %s: %w", c.Name, err)
						cancel()
					}
					mu.Unlock()
					return
				}
			}
		}()
	}
send:
	for _, c := range constraints {
		select {
		case jobs <- c:
		case <-runCtx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	return first
}

func normalize(value string) string { return strings.Join(strings.Fields(value), " ") }

func normalizeConstraint(value string) string {
	value = normalize(value)
	upper := strings.ToUpper(value)
	if strings.HasSuffix(upper, " NOT VALID") {
		value = strings.TrimSpace(value[:len(value)-len(" NOT VALID")])
	}
	return value
}
