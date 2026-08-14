package cdc

import "testing"

func TestApplyStatementCacheKeysSQLAndParameterOIDs(t *testing.T) {
	t.Parallel()
	cache := newApplyStatementCache(4)
	first, added, evicted := cache.acquire("INSERT INTO target VALUES ($1)", []uint32{23})
	if first == nil || !added || evicted != nil {
		t.Fatalf("first acquire = %#v added=%t evicted=%#v", first, added, evicted)
	}
	again, added, evicted := cache.acquire("INSERT INTO target VALUES ($1)", []uint32{23})
	if again != first || added || evicted != nil {
		t.Fatalf("repeat acquire = %#v added=%t evicted=%#v", again, added, evicted)
	}
	otherOID, added, evicted := cache.acquire("INSERT INTO target VALUES ($1)", []uint32{25})
	if otherOID == nil || otherOID == first || !added || evicted != nil {
		t.Fatalf("other OID acquire = %#v added=%t evicted=%#v", otherOID, added, evicted)
	}
	otherSQL, added, evicted := cache.acquire("INSERT INTO target VALUES ($1),($2)", []uint32{23, 23})
	if otherSQL == nil || otherSQL == first || !added || evicted != nil {
		t.Fatalf("other SQL acquire = %#v added=%t evicted=%#v", otherSQL, added, evicted)
	}
}

func TestApplyStatementCacheEvictsLeastRecentlyUsed(t *testing.T) {
	t.Parallel()
	cache := newApplyStatementCache(2)
	first, _, _ := cache.acquire("SELECT $1", []uint32{23})
	second, _, _ := cache.acquire("SELECT $1", []uint32{25})
	if cached, added, evicted := cache.acquire("SELECT $1", []uint32{23}); cached != first || added || evicted != nil {
		t.Fatalf("touch first = %#v added=%t evicted=%#v", cached, added, evicted)
	}
	third, added, evicted := cache.acquire("SELECT $1", []uint32{20})
	if third == nil || !added || evicted != second {
		t.Fatalf("third acquire = %#v added=%t evicted=%#v, want second", third, added, evicted)
	}
	if cached, added, _ := cache.acquire("SELECT $1", []uint32{25}); cached == second || !added {
		t.Fatalf("evicted statement remained cached: %#v added=%t", cached, added)
	}
}

func TestApplyStatementCacheCanDisablePreparation(t *testing.T) {
	t.Parallel()
	cache := newApplyStatementCache(0)
	statement, added, evicted := cache.acquire("SELECT $1", []uint32{23})
	if statement != nil || added || evicted != nil {
		t.Fatalf("disabled cache acquire = %#v added=%t evicted=%#v", statement, added, evicted)
	}
}
