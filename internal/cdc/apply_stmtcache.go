package cdc

import (
	"container/list"
	"encoding/binary"
	"fmt"
)

const applyStatementCacheCapacity = 1024

type applyStatementKey struct {
	sql  string
	oids string
}

type applyPreparedStatement struct {
	key  applyStatementKey
	name string
	lru  *list.Element
}

// applyStatementCache tracks named statements owned by one target connection.
// Exact SQL and parameter OIDs define a reusable parse; bind formats remain
// per-execution. The bounded LRU prevents a long-lived follow session from
// accumulating every rare INSERT tail size or UPDATE column mask it has seen.
type applyStatementCache struct {
	capacity int
	nextID   uint64
	entries  map[applyStatementKey]*applyPreparedStatement
	lru      list.List
}

func newApplyStatementCache(capacity int) *applyStatementCache {
	if capacity < 0 {
		capacity = 0
	}
	return &applyStatementCache{
		capacity: capacity,
		entries:  make(map[applyStatementKey]*applyPreparedStatement, capacity),
	}
}

func applyStatementKeyFor(sql string, oids []uint32) applyStatementKey {
	encoded := make([]byte, len(oids)*4)
	for i, oid := range oids {
		binary.BigEndian.PutUint32(encoded[i*4:], oid)
	}
	return applyStatementKey{sql: sql, oids: string(encoded)}
}

// acquire returns a cached or newly admitted statement and any statement that
// must be deallocated before the new name is prepared. A zero-capacity cache
// deliberately falls back to unnamed execution.
func (c *applyStatementCache) acquire(
	sql string,
	oids []uint32,
) (statement *applyPreparedStatement, added bool, evicted *applyPreparedStatement) {
	if c == nil || c.capacity == 0 {
		return nil, false, nil
	}
	key := applyStatementKeyFor(sql, oids)
	if statement = c.entries[key]; statement != nil {
		c.lru.MoveToFront(statement.lru)
		return statement, false, nil
	}
	if len(c.entries) >= c.capacity {
		element := c.lru.Back()
		evicted = element.Value.(*applyPreparedStatement)
		delete(c.entries, evicted.key)
		c.lru.Remove(element)
		evicted.lru = nil
	}
	c.nextID++
	statement = &applyPreparedStatement{
		key:  key,
		name: fmt.Sprintf("pgmigrate_cdc_%d", c.nextID),
	}
	statement.lru = c.lru.PushFront(statement)
	c.entries[key] = statement
	return statement, true, evicted
}
