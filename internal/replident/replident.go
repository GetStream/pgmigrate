// Package replident finds source relations whose rows logical replication cannot
// identify, and falls back to REPLICA IDENTITY FULL on exactly those.
//
// PostgreSQL refuses an UPDATE or DELETE on a published relation that has no
// usable replica identity:
//
//	cannot update table "t" because it does not have a replica identity
//	and publishes updates
//
// That error lands on the production application the moment the publication is
// created, so a relation in this state does not make a migration slow, it makes
// the source unwritable. The fallback therefore has to be applied before
// CREATE PUBLICATION, and reverted only after the publication is gone.
//
// # Why only the relations that need it
//
// FULL makes every UPDATE and DELETE write the whole old row to WAL instead of
// just the key, on production, for the entire migration, and it widens the
// predicate the applier builds on the target. Applying it to every selected
// relation because one of them needed it is what cost a 259 GB rehearsal its
// throughput: 110 relations were set to FULL and 109 already had a primary key.
//
// # Why leaf partitions rather than the tables that were selected
//
// A publication names partitioned parents and PostgreSQL expands them, but with
// publish_via_partition_root off it is the leaf partitions that appear in the
// stream. A parent holds no rows and produces no WAL, so its own relreplident is
// never consulted, and ALTER TABLE ... REPLICA IDENTITY does not cascade to
// partitions in any released PostgreSQL. A partitioned table without a primary
// key therefore needs every leaf altered individually, which is why Inspect
// expands the selection instead of trusting it.
package replident

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Replica identity modes, as pg_class.relreplident reports them.
const (
	IdentityDefault = "d"
	IdentityNothing = "n"
	IdentityFull    = "f"
	IdentityIndex   = "i"
)

// Table is a selected relation, which may be a partitioned parent.
type Table struct {
	OID    uint32
	Schema string
	Name   string
}

// Relation is one storage-bearing relation and everything needed to decide
// whether logical replication can identify its rows.
type Relation struct {
	OID    uint32
	Schema string
	Name   string
	// Identity is relreplident.
	Identity string
	// IdentityIndex names the index currently designated as the replica identity,
	// empty unless Identity is IdentityIndex. It is recorded so a revert restores
	// the designation and not merely the mode.
	IdentityIndex string
	// HasValidPrimaryKey reports a primary key whose index is valid. An invalid
	// index does not serve as a replica identity, so validity is part of the
	// question rather than a detail.
	HasValidPrimaryKey bool
	// HasValidIdentityIndex reports that the designated index exists and is valid.
	HasValidIdentityIndex bool
	// Owned reports whether the connected role may ALTER this relation. Only an
	// owner can change a replica identity, and on a managed service the migration
	// role often owns nothing, so this separates a fallback we can plan from one
	// we can only explain.
	Owned bool
	// Partition reports a leaf partition rather than a directly selected
	// relation, which changes how it is explained to an operator who never named
	// it.
	Partition bool
	// Parent is the schema-qualified immediate parent, empty unless Partition. A
	// leaf partition is nowhere in the operator's table selection, so a message
	// about one has to say where it came from.
	Parent string
	// SizeBytes and RowWrites say how expensive FULL will be here. Lifetime
	// UPDATE/DELETE counts are the honest predictor: a large relation nothing
	// updates costs nothing, a small hot one costs a great deal.
	SizeBytes int64
	RowWrites int64
}

// Identifier returns the schema-qualified, quoted name.
func (r Relation) Identifier() string {
	return pgx.Identifier{r.Schema, r.Name}.Sanitize()
}

// NeedsFallback reports whether logical replication cannot identify this
// relation's rows, making REPLICA IDENTITY FULL the only way to replicate its
// UPDATEs and DELETEs.
//
// Each case is one that is easy to get wrong:
//
//   - NOTHING publishes no identity at all, so it needs the fallback even when a
//     primary key exists. The setting overrides the key.
//   - DEFAULT means "use the primary key", so it needs the fallback only when
//     there is no valid primary key. A unique index is not enough: an index
//     serves as a replica identity only once REPLICA IDENTITY USING INDEX has
//     designated it.
//   - USING INDEX needs the fallback when the designated index has been dropped
//     or invalidated, which PostgreSQL allows to happen after the designation.
//
// FULL is the fallback, so it never needs one.
func NeedsFallback(relation Relation) bool {
	switch relation.Identity {
	case IdentityNothing:
		return true
	case IdentityDefault:
		return !relation.HasValidPrimaryKey
	case IdentityIndex:
		return !relation.HasValidIdentityIndex
	case IdentityFull:
		return false
	default:
		// Inspect rejects an unrecognized mode, and treating one as needing the
		// fallback keeps this total rather than quietly reporting "fine".
		return true
	}
}

// Reason says why logical replication cannot identify this relation's rows, in a
// form that tells an operator which of the three quite different problems they
// have. Empty when the relation is identifiable.
func (r Relation) Reason() string {
	switch {
	case r.Identity == IdentityNothing:
		return "is set to REPLICA IDENTITY NOTHING, which overrides any primary key"
	case r.Identity == IdentityDefault && !r.HasValidPrimaryKey:
		return "has no valid primary key, and a unique index is not a replica " +
			"identity until REPLICA IDENTITY USING INDEX designates it"
	case r.Identity == IdentityIndex && !r.HasValidIdentityIndex:
		return "designates a replica identity index that no longer exists or is no longer valid"
	case NeedsFallback(r):
		return fmt.Sprintf("reports replica identity %q", r.Identity)
	default:
		return ""
	}
}

// Describe names a relation and says what REPLICA IDENTITY FULL will cost on it.
// Size and lifetime UPDATE/DELETE counts are what decide whether that cost is
// trivial or serious, and the parent is named because a leaf partition appears
// nowhere in the operator's table selection.
func (r Relation) Describe() string {
	origin := ""
	if r.Partition && r.Parent != "" {
		origin = fmt.Sprintf(", a partition of %s", r.Parent)
	}
	return fmt.Sprintf("%s.%s%s (%d bytes, %d recorded UPDATE/DELETE rows)",
		r.Schema, r.Name, origin, r.SizeBytes, r.RowWrites)
}

// NeedFallback filters relations down to those that cannot identify their rows.
func NeedFallback(relations []Relation) []Relation {
	var needy []Relation
	for _, relation := range relations {
		if NeedsFallback(relation) {
			needy = append(needy, relation)
		}
	}
	return needy
}

// Unowned filters relations down to those the migration role cannot alter.
func Unowned(relations []Relation) []Relation {
	var unowned []Relation
	for _, relation := range relations {
		if !relation.Owned {
			unowned = append(unowned, relation)
		}
	}
	return unowned
}

func knownIdentity(identity string) bool {
	switch identity {
	case IdentityDefault, IdentityNothing, IdentityFull, IdentityIndex:
		return true
	default:
		return false
	}
}

// Record is what a fallback has to undo. The field names and JSON tags match the
// records earlier versions wrote under the same step names, so a state directory
// belonging to a migration already in flight still reverts correctly.
type Record struct {
	OID    uint32 `json:"oid"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Mode   string `json:"mode"`
	Index  string `json:"index,omitempty"`
}

// RecordOf describes how to put a relation back the way it was found.
func RecordOf(relation Relation) Record {
	return Record{
		OID:    relation.OID,
		Schema: relation.Schema,
		Table:  relation.Name,
		Mode:   relation.Identity,
		Index:  relation.IdentityIndex,
	}
}

// Identifier returns the schema-qualified, quoted name.
func (r Record) Identifier() string {
	return pgx.Identifier{r.Schema, r.Table}.Sanitize()
}

// Clause renders the ALTER TABLE clause that restores this record.
func (r Record) Clause() (string, error) {
	switch r.Mode {
	case IdentityDefault:
		return "DEFAULT", nil
	case IdentityNothing:
		return "NOTHING", nil
	case IdentityFull:
		return "FULL", nil
	case IdentityIndex:
		if r.Index == "" {
			return "", fmt.Errorf("replica identity index missing for %s", r.Identifier())
		}
		return "USING INDEX " + pgx.Identifier{r.Index}.Sanitize(), nil
	default:
		return "", fmt.Errorf("unknown replica identity mode %q for %s", r.Mode, r.Identifier())
	}
}
