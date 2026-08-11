package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/tgross/pgmigrate/internal/config"
	pgcopy "github.com/tgross/pgmigrate/internal/copy"
	"github.com/tgross/pgmigrate/internal/replident"
	"github.com/tgross/pgmigrate/internal/state"
)

// replidentStepPrefix names the steps holding the replica identity each relation
// had before the fallback. The name and the record shape are unchanged from
// earlier versions so a state directory belonging to a migration already in
// flight still reverts correctly.
const replidentStepPrefix = "replica_identity."

// replidentFinding marks a source carrying REPLICA IDENTITY FULL on pgmigrate's
// behalf. It stays open until the identities are restored, so status shows that
// production is still paying for the migration.
const replidentFinding = "replica-identity-full-forced"

// replidentRecorder stores originals in the steps table, which survives a crash
// and everything except an explicit base-copy reset.
type replidentRecorder struct{ store *state.Store }

func (r replidentRecorder) step(oid uint32) string {
	return fmt.Sprintf("%s%d", replidentStepPrefix, oid)
}

func (r replidentRecorder) Recorded(ctx context.Context, oid uint32) (bool, error) {
	return r.store.StepCompleted(ctx, r.step(oid))
}

func (r replidentRecorder) Record(ctx context.Context, record replident.Record) error {
	detail, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode replica identity record for %s: %w", record.Identifier(), err)
	}
	return r.store.CompleteStep(ctx, r.step(record.OID), string(detail))
}

// applyReplicaIdentityFallback sets REPLICA IDENTITY FULL on the source relations
// whose rows logical replication could not otherwise identify, and on no others.
//
// This has to run before the publication exists. Once a relation with no usable
// identity is published, every production UPDATE and DELETE against it fails, so
// the ALTER is not an optimization but the thing that keeps the source writable.
//
// Consent came from preflight: each relation altered here produced a warning
// naming it, and a default run stopped until --ack-warnings. There is no separate
// flag, because a flag is a worse instrument — the last one applied FULL to all
// 110 selected relations when one needed it.
func (a App) applyReplicaIdentityFallback(
	ctx context.Context, cfg config.Config, store *state.Store, tables []pgcopy.Table,
) error {
	conn, err := pgx.Connect(ctx, cfg.Source)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	relations, err := replident.Inspect(ctx, conn, replidentSelection(tables))
	if err != nil {
		return err
	}
	needy := replident.NeedFallback(relations)
	if len(needy) == 0 {
		return nil
	}
	for _, relation := range needy {
		// Named individually at warning level: this is a change to a production
		// database, and an operator reading the log afterwards should be able to
		// see exactly which relations carried the cost and why.
		logEvent(cfg.Dir, "replica_identity_full", map[string]any{
			"severity": "warning", "relation": relation.Schema + "." + relation.Name,
			"was": relation.Identity, "reason": relation.Reason(),
			"partition": relation.Partition, "bytes": relation.SizeBytes,
			"row_writes": relation.RowWrites,
		})
	}
	printReplicaIdentityFallback(a.output(), needy)

	applied, err := replident.Apply(ctx, conn, needy, replidentRecorder{store})
	if err != nil {
		// Whatever did apply is recorded, so cleanup or the next reset reverts it.
		// Returning here rather than continuing is deliberate: an unowned relation
		// cannot be published safely, and proceeding would break its writes.
		return err
	}
	return store.UpsertFinding(ctx, state.Finding{
		ID: replidentFinding, Kind: "replica-identity", Severity: "warning",
		Message: replicaIdentityMessage(applied),
	})
}

func replidentSelection(tables []pgcopy.Table) []replident.Table {
	selection := make([]replident.Table, 0, len(tables))
	for _, table := range tables {
		selection = append(selection, replident.Table{
			OID: table.OID, Schema: table.Schema, Name: table.Name,
		})
	}
	return selection
}

func replicaIdentityMessage(applied []replident.Record) string {
	names := make([]string, 0, len(applied))
	for _, record := range applied {
		names = append(names, fmt.Sprintf("%s.%s (was %s)", record.Schema, record.Table,
			identityName(record.Mode)))
	}
	return "source relations were temporarily set to REPLICA IDENTITY FULL so their " +
		"UPDATEs and DELETEs could be replicated: " + strings.Join(names, ", ")
}

func identityName(mode string) string {
	switch mode {
	case replident.IdentityDefault:
		return "DEFAULT"
	case replident.IdentityNothing:
		return "NOTHING"
	case replident.IdentityFull:
		return "FULL"
	case replident.IdentityIndex:
		return "USING INDEX"
	default:
		return mode
	}
}

func printReplicaIdentityFallback(out io.Writer, needy []replident.Relation) {
	fmt.Fprintf(out, "warning: setting REPLICA IDENTITY FULL on %d source relation(s) "+
		"for the duration of the migration:\n", len(needy))
	for _, relation := range needy {
		fmt.Fprintf(out, "  %s: it %s\n", relation.Describe(), relation.Reason())
	}
}

// restoreReplicaIdentities puts back the replica identity of every relation the
// fallback changed.
//
// Callers must drop the publication first. Restoring a relation to an identity
// that cannot identify rows while it is still published is not a no-op: it makes
// every production UPDATE and DELETE against it fail.
func restoreReplicaIdentities(ctx context.Context, sourceDSN string, store *state.Store) error {
	records, err := recordedReplicaIdentities(ctx, store)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	conn, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	if err := replident.Revert(ctx, conn, records); err != nil {
		return err
	}
	// Best effort: the finding is absent when nothing needed the fallback, and a
	// missing finding must not fail a revert that succeeded.
	_ = store.ResolveFinding(ctx, replidentFinding)
	return nil
}

// restoreTargetReplicaIdentities strips REPLICA IDENTITY FULL off the target
// relations that inherited it from the altered source.
//
// pg_dump takes the source schema after the fallback has been applied, and it
// emits the replica identity inside the CREATE TABLE table-of-contents entry
// rather than as a separate one, so pgmigrate's entry filtering cannot leave it
// out and the target arrives at FULL. Nothing during the migration is harmed by
// that, but the target is about to become production: left alone it would pay the
// WAL cost of a temporary migration workaround for the rest of its life.
//
// This runs at cutover rather than straight after the pre-data restore because a
// USING INDEX original names an index that pre-data has not created yet.
func restoreTargetReplicaIdentities(ctx context.Context, targetDSN string, store *state.Store) error {
	records, err := recordedReplicaIdentities(ctx, store)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	conn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	// A cleanup that runs before the schema was restored has records for target
	// tables that were never created, which is nothing to restore rather than a
	// failure.
	existing, err := replident.Existing(ctx, conn, records)
	if err != nil {
		return err
	}
	if err := replident.Revert(ctx, conn, existing); err != nil {
		return fmt.Errorf("restore target replica identities: %w", err)
	}
	return nil
}

func recordedReplicaIdentities(ctx context.Context, store *state.Store) ([]replident.Record, error) {
	steps, err := store.ListSteps(ctx)
	if err != nil {
		return nil, err
	}
	var records []replident.Record
	for _, step := range steps {
		if !step.Completed || !strings.HasPrefix(step.Name, replidentStepPrefix) {
			continue
		}
		var record replident.Record
		if err := json.Unmarshal([]byte(step.Detail), &record); err != nil {
			return nil, fmt.Errorf("decode %s: %w", step.Name, err)
		}
		records = append(records, record)
	}
	return records, nil
}

// retainReplicaIdentityFallback explains why FULL is still in place when
// --no-cleanup retained the publication.
//
// Reverting under --no-cleanup is what would do the damage. The retained
// publication still includes these relations, and a published relation with no
// usable identity rejects every UPDATE and DELETE, so putting the original
// identity back would leave the source unwritable. The finding is left open
// instead, which is what --no-cleanup is for: nothing is torn down silently.
func retainReplicaIdentityFallback(ctx context.Context, store *state.Store) error {
	records, err := recordedReplicaIdentities(ctx, store)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	names := make([]string, 0, len(records))
	for _, record := range records {
		names = append(names, record.Schema+"."+record.Table)
	}
	return store.UpsertFinding(ctx, state.Finding{
		ID: replidentFinding, Kind: "replica-identity", Severity: "warning",
		Message: fmt.Sprintf(
			"these source relations are still at REPLICA IDENTITY FULL because --no-cleanup "+
				"retained the publication, and restoring their original identity while they are "+
				"published would make every UPDATE and DELETE on them fail: %s. Drop the "+
				"publication, then run cleanup to restore them",
			strings.Join(names, ", "),
		),
	})
}
