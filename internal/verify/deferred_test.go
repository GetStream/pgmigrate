package verify

import (
	"testing"
	"time"
)

func TestCDCRecheckDefaultsToOneMinute(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()
	if cfg.CDCRecheckDelay != time.Minute {
		t.Fatalf("delay=%s", cfg.CDCRecheckDelay)
	}
}

func TestUnchangedSourceRequiresSameVersionAndContents(t *testing.T) {
	original := rowEntry{key: []string{"1"}, hash: 42, version: "100"}
	for _, tc := range []struct {
		name         string
		row          rowEntry
		absent, want bool
	}{
		{name: "unchanged", row: original, want: true},
		{name: "no-op update", row: rowEntry{key: original.key, hash: 42, version: "101"}},
		{name: "changed contents", row: rowEntry{key: original.key, hash: 43, version: "100"}},
		{name: "deleted", absent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make(rowSet)
			if !tc.absent {
				rows[identity(original.key)] = tc.row
			}
			if got := unchangedSource(original, rows); got != tc.want {
				t.Fatalf("unchanged=%t want %t", got, tc.want)
			}
		})
	}
}

func TestAdvancedTargetRequiresAPresentChangedRow(t *testing.T) {
	row := rowEntry{key: []string{"1"}, hash: 42, version: "100"}
	key := identity(row.key)
	for _, tc := range []struct {
		name         string
		old, current rowSet
		want         bool
	}{
		{name: "unchanged", old: rowSet{key: row}, current: rowSet{key: row}},
		{name: "appeared", current: rowSet{key: row}, want: true},
		{name: "disappeared", old: rowSet{key: row}},
		{name: "still absent"},
		{name: "new version same contents", old: rowSet{key: row}, current: rowSet{key: {key: row.key, hash: 42, version: "101"}}, want: true},
		{name: "new contents", old: rowSet{key: row}, current: rowSet{key: {key: row.key, hash: 43, version: "101"}}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := advancedTarget(key, tc.old, tc.current); got != tc.want {
				t.Fatalf("advanced=%t want=%t", got, tc.want)
			}
		})
	}
}
