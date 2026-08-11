package replident

import (
	"strings"
	"testing"
)

// TestNeedsFallbackCoversEveryIdentityShape is the whole decision, enumerated.
// Detection going wrong in either direction is expensive: a miss makes the source
// reject production UPDATEs once the publication exists, and a false positive puts
// a relation that did not need it onto the WAL-inflating path.
func TestNeedsFallbackCoversEveryIdentityShape(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name     string
		relation Relation
		want     bool
		because  string
	}{
		{
			name:     "default with a valid primary key",
			relation: Relation{Identity: IdentityDefault, HasValidPrimaryKey: true},
			want:     false,
			because:  "DEFAULT means use the primary key, and there is one",
		},
		{
			name:     "default without a primary key",
			relation: Relation{Identity: IdentityDefault},
			want:     true,
			because:  "DEFAULT with no primary key publishes no identity at all",
		},
		{
			name: "default with an invalid primary key index",
			// An invalid index cannot serve as a replica identity, so a primary
			// key whose index was invalidated leaves the relation unidentifiable
			// while still looking like it has a key.
			relation: Relation{Identity: IdentityDefault, HasValidPrimaryKey: false},
			want:     true,
			because:  "an invalid primary key index is not a usable identity",
		},
		{
			name:     "nothing, with a valid primary key",
			relation: Relation{Identity: IdentityNothing, HasValidPrimaryKey: true},
			want:     true,
			because:  "NOTHING overrides the primary key and publishes no identity",
		},
		{
			name:     "nothing, with a designated index",
			relation: Relation{Identity: IdentityNothing, HasValidIdentityIndex: true},
			want:     true,
			because:  "NOTHING overrides a designated index too",
		},
		{
			name: "using index, valid",
			relation: Relation{
				Identity: IdentityIndex, IdentityIndex: "t_key", HasValidIdentityIndex: true,
			},
			want:    false,
			because: "a valid designated index is a usable identity",
		},
		{
			name:     "using index, invalidated or dropped",
			relation: Relation{Identity: IdentityIndex, IdentityIndex: "t_key"},
			want:     true,
			because:  "PostgreSQL allows the designated index to stop being valid afterwards",
		},
		{
			name:     "full",
			relation: Relation{Identity: IdentityFull},
			want:     false,
			because:  "FULL is the fallback, so it never needs one",
		},
		{
			name:     "full without a primary key",
			relation: Relation{Identity: IdentityFull, HasValidPrimaryKey: false},
			want:     false,
			because:  "FULL identifies rows by every column, so a key is irrelevant",
		},
		{
			name:     "an unrecognized mode",
			relation: Relation{Identity: "z"},
			want:     true,
			because:  "an unknown mode must never be reported as fine",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := NeedsFallback(testCase.relation); got != testCase.want {
				t.Errorf("NeedsFallback(%+v) = %v, want %v because %s",
					testCase.relation, got, testCase.want, testCase.because)
			}
		})
	}
}

// TestNeedsFallbackIgnoresAnUndesignatedUniqueIndex pins the case the original
// proposed fix got wrong. It suggested falling back only where a relation "lacks a
// primary key and lacks a usable unique index", but a unique index is not a
// replica identity until REPLICA IDENTITY USING INDEX designates it, so that rule
// would skip a relation whose UPDATEs the source will refuse.
func TestNeedsFallbackIgnoresAnUndesignatedUniqueIndex(t *testing.T) {
	t.Parallel()
	// A unique index over NOT NULL columns exists, but nothing designated it: the
	// mode is still DEFAULT and there is no primary key.
	relation := Relation{
		Identity:              IdentityDefault,
		HasValidPrimaryKey:    false,
		HasValidIdentityIndex: false,
	}
	if !NeedsFallback(relation) {
		t.Fatal("a relation with only an undesignated unique index was treated as identifiable")
	}
}

func TestNeedFallbackFiltersAndPreservesOrder(t *testing.T) {
	t.Parallel()
	relations := []Relation{
		{Name: "keyed", Identity: IdentityDefault, HasValidPrimaryKey: true},
		{Name: "unkeyed", Identity: IdentityDefault},
		{Name: "already_full", Identity: IdentityFull},
		{Name: "nothing", Identity: IdentityNothing},
	}
	needy := NeedFallback(relations)
	var names []string
	for _, relation := range needy {
		names = append(names, relation.Name)
	}
	if got := strings.Join(names, ","); got != "unkeyed,nothing" {
		t.Fatalf("needy relations = %q, want \"unkeyed,nothing\"", got)
	}
}

func TestUnownedFiltersRelationsTheRoleCannotAlter(t *testing.T) {
	t.Parallel()
	relations := []Relation{
		{Name: "mine", Owned: true},
		{Name: "theirs"},
	}
	unowned := Unowned(relations)
	if len(unowned) != 1 || unowned[0].Name != "theirs" {
		t.Fatalf("unowned = %+v, want just theirs", unowned)
	}
}

func TestRecordClauseRestoresEveryMode(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		record Record
		want   string
	}{
		{Record{Mode: IdentityDefault}, "DEFAULT"},
		{Record{Mode: IdentityNothing}, "NOTHING"},
		{Record{Mode: IdentityFull}, "FULL"},
		{Record{Mode: IdentityIndex, Index: "t_key"}, `USING INDEX "t_key"`},
		// A revert has to restore the designation and not merely the mode, so the
		// index name is quoted rather than interpolated bare.
		{Record{Mode: IdentityIndex, Index: `we"ird`}, `USING INDEX "we""ird"`},
	} {
		got, err := testCase.record.Clause()
		if err != nil {
			t.Errorf("mode %q: %v", testCase.record.Mode, err)
			continue
		}
		if got != testCase.want {
			t.Errorf("mode %q clause = %q, want %q", testCase.record.Mode, got, testCase.want)
		}
	}
}

func TestRecordClauseRefusesAnIncompleteOrUnknownRecord(t *testing.T) {
	t.Parallel()
	// Restoring USING INDEX without the index name would silently produce invalid
	// SQL, so it has to be an error rather than a guess.
	if _, err := (Record{Mode: IdentityIndex, Schema: "s", Table: "t"}).Clause(); err == nil {
		t.Error("accepted a USING INDEX record with no index name")
	}
	if _, err := (Record{Mode: "z", Schema: "s", Table: "t"}).Clause(); err == nil {
		t.Error("accepted an unknown replica identity mode")
	}
}

func TestRecordOfCarriesTheDesignatedIndex(t *testing.T) {
	t.Parallel()
	record := RecordOf(Relation{
		OID: 42, Schema: "app", Name: "events",
		Identity: IdentityIndex, IdentityIndex: "events_key", HasValidIdentityIndex: true,
	})
	want := Record{OID: 42, Schema: "app", Table: "events", Mode: IdentityIndex, Index: "events_key"}
	if record != want {
		t.Fatalf("RecordOf = %+v, want %+v", record, want)
	}
	clause, err := record.Clause()
	if err != nil {
		t.Fatal(err)
	}
	if clause != `USING INDEX "events_key"` {
		t.Fatalf("clause = %q", clause)
	}
}

func TestIdentifiersAreQuoted(t *testing.T) {
	t.Parallel()
	// Relation names reach ALTER TABLE by interpolation, so quoting is what keeps
	// a mixed-case or awkward name working rather than being a nicety.
	relation := Relation{Schema: "MySchema", Name: "we\"ird"}
	if got := relation.Identifier(); got != `"MySchema"."we""ird"` {
		t.Errorf("Relation.Identifier() = %s", got)
	}
	record := Record{Schema: "MySchema", Table: "we\"ird"}
	if got := record.Identifier(); got != `"MySchema"."we""ird"` {
		t.Errorf("Record.Identifier() = %s", got)
	}
}
