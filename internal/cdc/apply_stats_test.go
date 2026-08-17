package cdc

import (
	"encoding/json"
	"testing"
)

func TestApplyStatsCountRowsAndStatementsByTable(t *testing.T) {
	stats := newApplyStats(8)
	second := &targetRelation{source: Relation{Namespace: "z", Name: "second"}}
	first := &targetRelation{source: Relation{Namespace: "a", Name: "first"}}

	stats.addRows(second, 5)
	stats.addDMLStatement(second)
	stats.addRows(first, 3)
	stats.addDMLStatement(first)
	stats.addDMLStatement(first)

	if stats.ApplyProgressStats != (ApplyProgressStats{
		Transactions: 8, Rows: 8, DMLStatements: 3, TargetCommits: 1,
	}) {
		t.Fatalf("global stats = %#v", stats.ApplyProgressStats)
	}
	var tables []ApplyTableStats
	if err := json.Unmarshal(stats.tableJSON(), &tables); err != nil {
		t.Fatal(err)
	}
	want := []ApplyTableStats{
		{Schema: "a", Table: "first", Rows: 3, DMLStatements: 2},
		{Schema: "z", Table: "second", Rows: 5, DMLStatements: 1},
	}
	if len(tables) != len(want) {
		t.Fatalf("table stats = %#v", tables)
	}
	for i := range want {
		if tables[i] != want[i] {
			t.Fatalf("table stats[%d] = %#v, want %#v", i, tables[i], want[i])
		}
	}
}

func TestApplyStatsIgnoreUnknownOrNonRowWork(t *testing.T) {
	stats := newApplyStats(1)
	stats.addRows(nil, 1)
	stats.addRows(&targetRelation{}, 0)
	stats.addDMLStatement(nil)
	if stats.Rows != 0 || stats.DMLStatements != 0 || len(stats.tables) != 0 {
		t.Fatalf("empty work changed stats: %#v", stats)
	}
}
