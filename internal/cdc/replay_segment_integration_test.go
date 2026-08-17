//go:build integration

package cdc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestPG17ReplaySegmentTopologyBenchmark(t *testing.T) {
	path := os.Getenv(replaySegmentEnvironment)
	if path == "" {
		t.Skip("set " + replaySegmentEnvironment + " to benchmark an external segment topology")
	}
	relationOID := replayBenchmarkUint32(t, "PGMIGRATE_REPLAY_RELATION_OID", 0)
	maxChanges := replayBenchmarkInt(t, "PGMIGRATE_REPLAY_MAX_CHANGES", 50_000)
	workers := replayBenchmarkInt(t, "PGMIGRATE_REPLAY_WORKERS", 8)
	batchSize := replayBenchmarkInt(t, "PGMIGRATE_REPLAY_BATCH_SIZE", 128)

	if relationOID == 0 {
		stats := scanReplaySegment(t, path)
		for oid, relation := range stats.relations {
			if relationOID == 0 || relation.updates > stats.relations[relationOID].updates {
				relationOID = oid
			}
		}
	}
	indexes := replayBenchmarkInt(t, "PGMIGRATE_REPLAY_INDEXES", 0)
	if _, overridden := os.LookupEnv("PGMIGRATE_REPLAY_INDEXES"); !overridden {
		indexes = readReplayStateRelations(
			t, os.Getenv(replayStateEnvironment),
		)[relationOID].indexes
	}
	topology, sourceRelation := replayUpdateTopology(t, path, relationOID, maxChanges)
	if len(topology) == 0 {
		t.Fatalf("relation %d has no update transactions in the segment", relationOID)
	}
	changeCount := 0
	maxTransactionChanges := 0
	for _, changes := range topology {
		changeCount += len(changes)
		maxTransactionChanges = max(maxTransactionChanges, len(changes))
	}
	t.Logf(
		"topology source_oid=%d source_name=%q.%q transactions=%d changes=%d max_transaction_changes=%d indexes=%d workers=%d batch=%d",
		relationOID,
		sourceRelation.Namespace,
		sourceRelation.Name,
		len(topology),
		changeCount,
		maxTransactionChanges,
		indexes,
		workers,
		batchSize,
	)

	target := pgtest.Start(t, 17)
	ctx := context.Background()
	conn := target.Connect(t)
	// The synthetic target keeps production values out of the fixture. It seeds
	// only replica identity columns, then preserves source transaction sizes and
	// transmitted-column masks. Non-key values therefore model a changed-value
	// stress case for secondary-index maintenance.
	relation, expectedColumnWrites := createReplayShapeTarget(
		t, ctx, conn, sourceRelation, changeCount, indexes,
	)

	transactions := make([]Transaction, len(topology))
	nextID := 1
	for transactionIndex, shapes := range topology {
		transaction := Transaction{
			CommitLSN: LSN(transactionIndex*2 + 1),
			EndLSN:    LSN(transactionIndex*2 + 2),
			Relations: []Relation{relation},
			Changes:   make([]Change, len(shapes)),
		}
		for changeIndex, shape := range shapes {
			oldTuple := make(Tuple, len(relation.Columns))
			newTuple := make(Tuple, len(relation.Columns))
			for columnIndex, column := range relation.Columns {
				if column.Flags&1 != 0 {
					value := []byte(fmt.Sprintf("%d-%d", nextID, columnIndex))
					oldTuple[columnIndex] = TupleDatum{Kind: DatumText, Data: value}
					if shape[columnIndex] {
						newTuple[columnIndex] = TupleDatum{Kind: DatumText, Data: value}
					} else {
						newTuple[columnIndex] = TupleDatum{Kind: DatumUnchangedToast}
					}
					continue
				}
				oldTuple[columnIndex] = TupleDatum{Kind: DatumUnchangedToast}
				if shape[columnIndex] {
					newTuple[columnIndex] = TupleDatum{
						Kind: DatumText,
						Data: []byte(strconv.Itoa(nextID + columnIndex + 1)),
					}
					expectedColumnWrites[columnIndex]++
				} else {
					newTuple[columnIndex] = TupleDatum{Kind: DatumUnchangedToast}
				}
			}
			transaction.Changes[changeIndex] = Change{
				RelationOID: relation.OID,
				Kind:        ChangeUpdate,
				Old:         &oldTuple,
				New:         &newTuple,
			}
			nextID++
		}
		transactions[transactionIndex] = transaction
	}
	directory := t.TempDir()
	writer, _, err := OpenWriter(WriterConfig{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	for transactionIndex := range transactions {
		if _, err := writer.AppendFrame(&transactions[transactionIndex]); err != nil {
			t.Fatal(err)
		}
	}
	durableLSN, err := writer.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	durable := new(DurableWatermark)
	durable.Publish(durableLSN)
	applier, err := NewApplier(ApplierConfig{
		ConnString: target.URI,
		Directory:  directory,
		Workers:    workers,
		BatchSize:  batchSize,
		Window:     max(1024, workers*8),
		StreamID:   "replay-segment-topology",
		Durable:    durable,
		EndPosition: func(context.Context) (LSN, bool, error) {
			return durableLSN, true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := applier.Run(ctx); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	verifyReplayShapeTarget(t, ctx, conn, relation, changeCount, expectedColumnWrites)
	t.Logf(
		"replay elapsed=%s changes/s=%.0f source_transactions/s=%.0f",
		elapsed,
		float64(changeCount)/elapsed.Seconds(),
		float64(len(topology))/elapsed.Seconds(),
	)
}

func replayUpdateTopology(
	tb testing.TB,
	path string,
	relationOID uint32,
	maxChanges int,
) ([][]replayUpdateShape, Relation) {
	tb.Helper()
	reader := openExternalSegmentReader(tb, path)
	defer reader.Close()
	var topology [][]replayUpdateShape
	var relation Relation
	total := 0
	for total < maxChanges {
		transaction, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		for i := range transaction.Relations {
			if transaction.Relations[i].OID == relationOID {
				relation = cloneRelation(transaction.Relations[i])
				break
			}
		}
		var updates []replayUpdateShape
		visit := func(change Change) error {
			if change.RelationOID != relationOID || change.Kind != ChangeUpdate || change.New == nil {
				return nil
			}
			shape := make(replayUpdateShape, len(*change.New))
			for columnIndex, datum := range *change.New {
				shape[columnIndex] = datum.Kind != DatumUnchangedToast
			}
			updates = append(updates, shape)
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
		if len(updates) == 0 {
			continue
		}
		topology = append(topology, updates)
		total += len(updates)
	}
	return topology, relation
}

type replayUpdateShape []bool

func createReplayShapeTarget(
	tb testing.TB,
	ctx context.Context,
	conn *pgx.Conn,
	source Relation,
	rows int,
	indexes int,
) (Relation, []int) {
	tb.Helper()
	relation := Relation{
		OID:             1,
		Namespace:       "public",
		Name:            "replay_segment_target",
		ReplicaIdentity: source.ReplicaIdentity,
		Columns:         make([]Column, len(source.Columns)),
	}
	var definition strings.Builder
	definition.WriteString("CREATE TABLE public.replay_segment_target (")
	var keys []int
	var nonKeys []int
	for columnIndex, sourceColumn := range source.Columns {
		if columnIndex != 0 {
			definition.WriteByte(',')
		}
		name := fmt.Sprintf("column_%d", columnIndex)
		relation.Columns[columnIndex] = Column{
			Name: name, Type: pgtype.TextOID, Flags: sourceColumn.Flags,
		}
		definition.WriteString(pgx.Identifier{name}.Sanitize())
		definition.WriteString(" text")
		if sourceColumn.Flags&1 != 0 {
			definition.WriteString(" NOT NULL")
			keys = append(keys, columnIndex)
		} else {
			nonKeys = append(nonKeys, columnIndex)
		}
	}
	if len(keys) == 0 {
		tb.Fatalf("source relation %d has no replica identity columns", source.OID)
	}
	definition.WriteString(", PRIMARY KEY (")
	for keyIndex, columnIndex := range keys {
		if keyIndex != 0 {
			definition.WriteByte(',')
		}
		definition.WriteString(pgx.Identifier{relation.Columns[columnIndex].Name}.Sanitize())
	}
	definition.WriteString("))")
	if _, err := conn.Exec(ctx, definition.String()); err != nil {
		tb.Fatal(err)
	}

	var seed strings.Builder
	seed.WriteString("INSERT INTO public.replay_segment_target (")
	for keyIndex, columnIndex := range keys {
		if keyIndex != 0 {
			seed.WriteByte(',')
		}
		seed.WriteString(pgx.Identifier{relation.Columns[columnIndex].Name}.Sanitize())
	}
	seed.WriteString(") SELECT ")
	for keyIndex, columnIndex := range keys {
		if keyIndex != 0 {
			seed.WriteByte(',')
		}
		fmt.Fprintf(&seed, "id::text || '-%d'", columnIndex)
	}
	seed.WriteString(" FROM generate_series(1, $1) AS id")
	if _, err := conn.Exec(ctx, seed.String(), rows); err != nil {
		tb.Fatal(err)
	}

	indexColumns := nonKeys
	if len(indexColumns) == 0 {
		indexColumns = keys
	}
	for index := range indexes {
		column := relation.Columns[indexColumns[index%len(indexColumns)]].Name
		if _, err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE INDEX replay_segment_target_%d ON public.replay_segment_target (%s)",
			index,
			pgx.Identifier{column}.Sanitize(),
		)); err != nil {
			tb.Fatal(err)
		}
	}
	return relation, make([]int, len(relation.Columns))
}

func verifyReplayShapeTarget(
	tb testing.TB,
	ctx context.Context,
	conn *pgx.Conn,
	relation Relation,
	expectedRows int,
	expectedColumnWrites []int,
) {
	tb.Helper()
	var query strings.Builder
	query.WriteString("SELECT count(*)")
	var columns []int
	for columnIndex, column := range relation.Columns {
		if column.Flags&1 != 0 {
			continue
		}
		query.WriteString(",count(")
		query.WriteString(pgx.Identifier{column.Name}.Sanitize())
		query.WriteByte(')')
		columns = append(columns, columnIndex)
	}
	query.WriteString(" FROM public.replay_segment_target")
	values := make([]int64, 1+len(columns))
	destinations := make([]any, len(values))
	for i := range values {
		destinations[i] = &values[i]
	}
	if err := conn.QueryRow(ctx, query.String()).Scan(destinations...); err != nil {
		tb.Fatal(err)
	}
	if values[0] != int64(expectedRows) {
		tb.Fatalf("target rows = %d, want %d", values[0], expectedRows)
	}
	for resultIndex, columnIndex := range columns {
		if values[resultIndex+1] != int64(expectedColumnWrites[columnIndex]) {
			tb.Fatalf(
				"target column %d writes = %d, want %d",
				columnIndex, values[resultIndex+1], expectedColumnWrites[columnIndex],
			)
		}
	}
}

func replayBenchmarkInt(tb testing.TB, name string, fallback int) int {
	tb.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		tb.Fatalf("invalid %s %q", name, value)
	}
	return parsed
}

func replayBenchmarkUint32(tb testing.TB, name string, fallback uint32) uint32 {
	tb.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		tb.Fatalf("invalid %s %q", name, value)
	}
	return uint32(parsed)
}
