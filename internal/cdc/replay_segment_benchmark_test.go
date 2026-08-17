package cdc

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const replaySegmentEnvironment = "PGMIGRATE_REPLAY_SEGMENT"
const replayStateEnvironment = "PGMIGRATE_REPLAY_STATE"

type replaySegmentRelationStats struct {
	relation      Relation
	transactions  uint64
	inserts       uint64
	updates       uint64
	deletes       uint64
	truncates     uint64
	columnUpdates []uint64
	updateKeys    map[uint64]uint32
	repeatedKeys  uint64
	maxKeyUpdates uint32
}

func (s replaySegmentRelationStats) changes() uint64 {
	return s.inserts + s.updates + s.deletes + s.truncates
}

type replaySegmentStats struct {
	transactions  uint64
	changes       uint64
	payloadBytes  uint64
	multiRelation uint64
	maxChanges    uint32
	maxPayload    uint64
	firstCommit   time.Time
	lastCommit    time.Time
	changeBuckets [8]uint64
	relations     map[uint32]*replaySegmentRelationStats
}

func TestReplaySegmentProfile(t *testing.T) {
	path := os.Getenv(replaySegmentEnvironment)
	if path == "" {
		t.Skip("set " + replaySegmentEnvironment + " to profile an external segment")
	}
	started := time.Now()
	stats := scanReplaySegment(t, path)
	stateRelations := readReplayStateRelations(t, os.Getenv(replayStateEnvironment))
	elapsed := time.Since(started)
	t.Logf(
		"segment transactions=%d changes=%d payload=%.2f MiB elapsed=%s throughput=%.2f MiB/s txns/s=%.0f changes/s=%.0f",
		stats.transactions,
		stats.changes,
		float64(stats.payloadBytes)/(1<<20),
		elapsed,
		float64(stats.payloadBytes)/(1<<20)/elapsed.Seconds(),
		float64(stats.transactions)/elapsed.Seconds(),
		float64(stats.changes)/elapsed.Seconds(),
	)
	t.Logf(
		"segment span=%s multi_relation=%d max_changes=%d max_payload=%.2f MiB change_buckets=%v",
		stats.lastCommit.Sub(stats.firstCommit),
		stats.multiRelation,
		stats.maxChanges,
		float64(stats.maxPayload)/(1<<20),
		stats.changeBuckets,
	)
	var potentialIndexEntries uint64
	for oid, relation := range stats.relations {
		potentialIndexEntries += relation.changes() * uint64(stateRelations[oid].indexes)
	}
	if len(stateRelations) != 0 {
		t.Logf("state potential_index_entries=%d", potentialIndexEntries)
	}
	relations := make([]*replaySegmentRelationStats, 0, len(stats.relations))
	for _, relation := range stats.relations {
		relations = append(relations, relation)
	}
	slices.SortFunc(relations, func(left, right *replaySegmentRelationStats) int {
		switch {
		case left.changes() > right.changes():
			return -1
		case left.changes() < right.changes():
			return 1
		default:
			return 0
		}
	})
	for i, relation := range relations {
		if i == 20 {
			break
		}
		state := stateRelations[relation.relation.OID]
		t.Logf(
			"relation oid=%d name=%q.%q identity=%q key_columns=%d transactions=%d changes=%d insert=%d update=%d delete=%d truncate=%d table_gib=%.2f indexes=%d potential_index_entries=%d update_keys=%d repeated_key_updates=%d max_updates_per_key=%d updated_columns=%s",
			relation.relation.OID,
			relation.relation.Namespace,
			relation.relation.Name,
			relation.relation.ReplicaIdentity,
			replayKeyColumnCount(&relation.relation),
			relation.transactions,
			relation.changes(),
			relation.inserts,
			relation.updates,
			relation.deletes,
			relation.truncates,
			float64(state.bytes)/(1<<30),
			state.indexes,
			relation.changes()*uint64(state.indexes),
			len(relation.updateKeys),
			relation.repeatedKeys,
			relation.maxKeyUpdates,
			replayTopUpdatedColumns(relation, 8),
		)
	}
}

type replayStateRelationStats struct {
	bytes   uint64
	indexes int
}

func readReplayStateRelations(
	tb testing.TB,
	path string,
) map[uint32]replayStateRelationStats {
	tb.Helper()
	result := make(map[uint32]replayStateRelationStats)
	if path == "" {
		return result
	}
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`
		SELECT tables.oid, tables.bytes, count(indexes.oid)
		FROM tables
		LEFT JOIN indexes ON indexes.table_oid = tables.oid
		GROUP BY tables.oid, tables.bytes
	`)
	if err != nil {
		tb.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var oid uint32
		var relation replayStateRelationStats
		if err := rows.Scan(&oid, &relation.bytes, &relation.indexes); err != nil {
			tb.Fatal(err)
		}
		result[oid] = relation
	}
	if err := rows.Err(); err != nil {
		tb.Fatal(err)
	}
	return result
}

func BenchmarkReplaySegmentDecode(b *testing.B) {
	path := os.Getenv(replaySegmentEnvironment)
	if path == "" {
		b.Skip("set " + replaySegmentEnvironment + " to benchmark an external segment")
	}
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()
	var transactions, changes uint64
	for range b.N {
		decodedTransactions, decodedChanges := decodeReplaySegment(b, path)
		transactions += decodedTransactions
		changes += decodedChanges
	}
	b.StopTimer()
	b.ReportMetric(float64(transactions)/b.Elapsed().Seconds(), "txns/s")
	b.ReportMetric(float64(changes)/b.Elapsed().Seconds(), "changes/s")
}

func decodeReplaySegment(tb testing.TB, path string) (uint64, uint64) {
	tb.Helper()
	reader := openExternalSegmentReader(tb, path)
	defer reader.Close()
	var transactions, changes uint64
	for {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return transactions, changes
		}
		if err != nil {
			tb.Fatal(err)
		}
		transactions++
		changes += uint64(transaction.ChangeCount())
		if err := transaction.CleanupSpill(); err != nil {
			tb.Fatal(err)
		}
	}
}

func scanReplaySegment(tb testing.TB, path string) replaySegmentStats {
	tb.Helper()
	reader := openExternalSegmentReader(tb, path)
	defer reader.Close()
	stats := replaySegmentStats{relations: make(map[uint32]*replaySegmentRelationStats)}
	for {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return stats
		}
		if err != nil {
			tb.Fatal(err)
		}
		payload, err := transactionPayloadSize(&transaction)
		if err != nil {
			tb.Fatal(err)
		}
		changeCount := transaction.ChangeCount()
		stats.transactions++
		stats.changes += uint64(changeCount)
		stats.payloadBytes += payload
		if len(transaction.Relations) > 1 {
			stats.multiRelation++
		}
		if changeCount > stats.maxChanges {
			stats.maxChanges = changeCount
		}
		if payload > stats.maxPayload {
			stats.maxPayload = payload
		}
		if stats.firstCommit.IsZero() {
			stats.firstCommit = transaction.CommitTime
		}
		stats.lastCommit = transaction.CommitTime
		stats.changeBuckets[replayChangeBucket(changeCount)]++
		touched := make(map[uint32]struct{}, len(transaction.Relations))
		for i := range transaction.Relations {
			relation := &transaction.Relations[i]
			entry := stats.relations[relation.OID]
			if entry == nil {
				cloned := cloneRelation(*relation)
				entry = &replaySegmentRelationStats{
					relation:      cloned,
					columnUpdates: make([]uint64, len(cloned.Columns)),
					updateKeys:    make(map[uint64]uint32),
				}
				stats.relations[relation.OID] = entry
			}
		}
		visit := func(change Change) error {
			entry := stats.relations[change.RelationOID]
			if entry == nil {
				return nil
			}
			if _, ok := touched[change.RelationOID]; !ok {
				entry.transactions++
				touched[change.RelationOID] = struct{}{}
			}
			switch change.Kind {
			case ChangeInsert:
				entry.inserts++
			case ChangeUpdate:
				entry.updates++
				predicate := change.Old
				if predicate == nil {
					predicate = change.New
				}
				if key, ok := replayUpdateKey(&entry.relation, predicate); ok {
					previous := entry.updateKeys[key]
					entry.updateKeys[key] = previous + 1
					if previous != 0 {
						entry.repeatedKeys++
					}
					entry.maxKeyUpdates = max(entry.maxKeyUpdates, previous+1)
				}
				if change.New != nil {
					for columnIndex, datum := range *change.New {
						if datum.Kind != DatumUnchangedToast {
							entry.columnUpdates[columnIndex]++
						}
					}
				}
			case ChangeDelete:
				entry.deletes++
			case ChangeTruncate:
				entry.truncates++
			}
			return nil
		}
		if transaction.Spill != nil {
			if err := transaction.Spill.forEachChange(visit); err != nil {
				tb.Fatal(err)
			}
		} else {
			for i := range transaction.Changes {
				if err := visit(transaction.Changes[i]); err != nil {
					tb.Fatal(err)
				}
			}
		}
		if err := transaction.CleanupSpill(); err != nil {
			tb.Fatal(err)
		}
	}
}

func openExternalSegmentReader(tb testing.TB, path string) *Reader {
	tb.Helper()
	info, err := os.Stat(path)
	if err != nil {
		tb.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		tb.Fatalf("segment %q is not a regular file", path)
	}
	directory := tb.TempDir()
	name := filepath.Base(path)
	if _, _, ok := parseSegmentName(name); !ok {
		name = "0000000000000001.seg"
	}
	if err := os.Symlink(path, filepath.Join(directory, name)); err != nil {
		tb.Fatal(err)
	}
	reader, err := NewReaderWithConfig(ReaderConfig{
		Directory:      directory,
		SpillDirectory: tb.TempDir(),
		DurableEndLSN:  LSN(^uint64(0)),
	})
	if err != nil {
		tb.Fatal(err)
	}
	return reader
}

func replayChangeBucket(changes uint32) int {
	switch {
	case changes == 0:
		return 0
	case changes == 1:
		return 1
	case changes <= 4:
		return 2
	case changes <= 16:
		return 3
	case changes <= 64:
		return 4
	case changes <= 256:
		return 5
	case changes <= 1024:
		return 6
	default:
		return 7
	}
}

func replayKeyColumnCount(relation *Relation) int {
	count := 0
	for _, column := range relation.Columns {
		if column.Flags&1 != 0 {
			count++
		}
	}
	return count
}

func replayTopUpdatedColumns(relation *replaySegmentRelationStats, limit int) string {
	indexes := make([]int, len(relation.columnUpdates))
	for i := range indexes {
		indexes[i] = i
	}
	slices.SortFunc(indexes, func(left, right int) int {
		switch {
		case relation.columnUpdates[left] > relation.columnUpdates[right]:
			return -1
		case relation.columnUpdates[left] < relation.columnUpdates[right]:
			return 1
		default:
			return 0
		}
	})
	var result strings.Builder
	written := 0
	for _, columnIndex := range indexes {
		if written == limit || relation.columnUpdates[columnIndex] == 0 {
			break
		}
		if written != 0 {
			result.WriteByte(',')
		}
		fmt.Fprintf(
			&result,
			"%s:%d",
			relation.relation.Columns[columnIndex].Name,
			relation.columnUpdates[columnIndex],
		)
		written++
	}
	return result.String()
}

func replayUpdateKey(relation *Relation, tuple *Tuple) (uint64, bool) {
	if relation == nil || tuple == nil || len(*tuple) != len(relation.Columns) {
		return 0, false
	}
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	found := false
	for columnIndex, column := range relation.Columns {
		if column.Flags&1 == 0 {
			continue
		}
		found = true
		datum := (*tuple)[columnIndex]
		hash ^= uint64(datum.Kind)
		hash *= prime
		for _, value := range datum.Data {
			hash ^= uint64(value)
			hash *= prime
		}
		hash ^= uint64(len(datum.Data))
		hash *= prime
	}
	return hash, found
}
