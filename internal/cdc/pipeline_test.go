package cdc

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPersisterSquashesBatchIntoOneDurableWatermark(t *testing.T) {
	t.Parallel()
	writer, _, err := OpenWriter(WriterConfig{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	input := make(chan Transaction, 3)
	watermark := new(DurableWatermark)
	persister, err := NewPersister(PersisterConfig{
		Writer:       writer,
		Transactions: input,
		Durable:      watermark,
		SyncBytes:    1 << 30,
		SyncInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, lsn := range []LSN{0x10, 0x20, 0x30} {
		input <- testTransaction(lsn)
	}
	close(input)
	if err := persister.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := watermark.Load(); got != 0x31 {
		t.Fatalf("durable watermark = %x, want 31", got)
	}
}

func TestDurableWatermarkIsMonotonic(t *testing.T) {
	t.Parallel()
	watermark := new(DurableWatermark)
	watermark.Publish(20)
	watermark.Publish(10)
	if got := watermark.Load(); got != 20 {
		t.Fatalf("watermark = %d, want 20", got)
	}
}

func TestTargetRelationCacheReloadsOnlyForChangedSourceDefinition(t *testing.T) {
	t.Parallel()
	cache := newTargetRelationCache()
	source := Relation{
		OID: 7, Namespace: "public", Name: "items", ReplicaIdentity: 'd',
		Columns: []Column{{Name: "id", Type: 20, Flags: 1}, {Name: "value", Type: 25}},
	}
	loads := 0
	loader := func(_ context.Context, _ pgx.Tx, relation *Relation) (*targetRelation, error) {
		loads++
		return &targetRelation{source: *relation, quoted: relation.Name}, nil
	}
	first, err := cache.resolve(context.Background(), nil, &source, loader)
	if err != nil {
		t.Fatal(err)
	}
	again, err := cache.resolve(context.Background(), nil, &source, loader)
	if err != nil {
		t.Fatal(err)
	}
	if loads != 1 || again != first {
		t.Fatalf("unchanged relation loads=%d same=%t, want one load and same result", loads, again == first)
	}

	source.Columns[1].Flags = 1
	changed, err := cache.resolve(context.Background(), nil, &source, loader)
	if err != nil {
		t.Fatal(err)
	}
	if loads != 2 || changed == first {
		t.Fatalf("changed relation loads=%d reused=%t, want reload", loads, changed == first)
	}
	if first.source.Columns[1].Flags != 0 {
		t.Fatal("cached source definition aliases the caller's mutable column slice")
	}
}

func TestApplyPreparationQuotesAndUsesReplicaIdentity(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	var sql strings.Builder
	params := make([]rawParam, 0, 1)
	tuple := Tuple{
		{Kind: DatumText, Data: []byte("7")},
		{Kind: DatumText, Data: []byte("ignored")},
	}
	if err := appendPredicate(&sql, &params, relation, tuple, ChangeDelete); err != nil {
		t.Fatal(err)
	}
	if got, want := sql.String(), `"select" = $1`; got != want {
		t.Fatalf("predicate = %q, want %q", got, want)
	}
	if len(params) != 1 || string(params[0].data) != "7" || params[0].oid != 23 {
		t.Fatalf("parameters = %#v", params)
	}
}

// TestApplyPredicateStaysIndexableOnNotNullColumns guards apply throughput.
// IS NOT DISTINCT FROM has no btree strategy, so building the whole predicate
// from it made every UPDATE and DELETE a sequential scan and catch-up could
// never converge on a large table. Equality is equivalent on a NOT NULL column
// and lets the planner reach the replica identity index; nullable columns, which
// only appear under REPLICA IDENTITY FULL, still need the NULL-safe form.
func TestApplyPredicateStaysIndexableOnNotNullColumns(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	relation.source.ReplicaIdentity = 'f'
	var sql strings.Builder
	params := make([]rawParam, 0, 2)
	tuple := Tuple{
		{Kind: DatumText, Data: []byte("7")},
		{Kind: DatumText, Data: []byte("body")},
	}
	if err := appendPredicate(&sql, &params, relation, tuple, ChangeUpdate); err != nil {
		t.Fatal(err)
	}
	want := `"select" = $1 AND "payload" IS NOT DISTINCT FROM $2`
	if got := sql.String(); got != want {
		t.Fatalf("predicate = %q, want %q", got, want)
	}
	if strings.Count(sql.String(), "IS NOT DISTINCT FROM") != 1 {
		t.Fatalf("nullable columns should be the only NULL-safe comparisons: %q", sql.String())
	}
}

// TestApplyPreparationDistinguishesNullFromEmpty guards the bind-parameter
// contract: nil means SQL NULL and non-nil zero-length means a zero-length
// value. Inferring nullness from the data pointer applied every empty string as
// NULL, which corrupted the target wherever the column allowed it.
func TestApplyPreparationDistinguishesNullFromEmpty(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	relation.source.Columns[1].Type = 25
	tests := map[string]struct {
		datum  TupleDatum
		isNull bool
	}{
		"null":              {datum: TupleDatum{Kind: DatumNull}, isNull: true},
		"empty text":        {datum: TupleDatum{Kind: DatumText, Data: []byte{}}, isNull: false},
		"nil-data text":     {datum: TupleDatum{Kind: DatumText}, isNull: false},
		"empty binary":      {datum: TupleDatum{Kind: DatumBinary, Data: []byte{}}, isNull: false},
		"nil-data binary":   {datum: TupleDatum{Kind: DatumBinary}, isNull: false},
		"non-empty text":    {datum: TupleDatum{Kind: DatumText, Data: []byte("x")}, isNull: false},
		"non-empty by kind": {datum: TupleDatum{Kind: DatumBinary, Data: []byte{0}}, isNull: false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			param, err := datumParam(relation, 1, test.datum, ChangeInsert)
			if err != nil {
				t.Fatal(err)
			}
			if param.isNull != test.isNull {
				t.Fatalf("isNull = %t, want %t", param.isNull, test.isNull)
			}
			values := paramValues([]rawParam{param})
			if (values[0] == nil) != test.isNull {
				t.Fatalf("bound value nil = %t, want %t", values[0] == nil, test.isNull)
			}
			if !test.isNull && len(values[0]) != len(test.datum.Data) {
				t.Fatalf("bound value length = %d, want %d", len(values[0]), len(test.datum.Data))
			}
		})
	}
}

func TestApplyPreparationRejectsBinaryTypeMismatch(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	relation.source.Columns[0].Type = 25
	_, err := datumParam(relation, 0, TupleDatum{Kind: DatumBinary, Data: []byte{1}}, ChangeInsert)
	if err == nil {
		t.Fatal("expected binary OID mismatch")
	}
	var divergence *DivergenceError
	if !strings.Contains(err.Error(), "source OID") || !errors.As(err, &divergence) {
		t.Fatalf("error = %v", err)
	}
}

func TestApplyPreparationPreservesTruncateOptionsAndBoundaries(t *testing.T) {
	t.Parallel()
	change := Change{
		Kind:                    ChangeTruncate,
		TruncateCascade:         true,
		TruncateRestartIdentity: true,
	}
	var sql strings.Builder
	appendTruncateOptions(&sql, change)
	if got, want := sql.String(), " RESTART IDENTITY CASCADE"; got != want {
		t.Fatalf("truncate suffix = %q, want %q", got, want)
	}
	if sameTruncateOptions(change, Change{Kind: ChangeTruncate, TruncateCascade: true}) {
		t.Fatal("different truncate options were incorrectly batch-compatible")
	}
}

func TestInsertChunkRowsStaysBelowBindLimit(t *testing.T) {
	t.Parallel()
	for _, columns := range []int{1, 2, 100, 1000, 70000} {
		rows := insertChunkRows(columns)
		if rows < 1 || rows > 1000 {
			t.Fatalf("columns %d produced %d rows", columns, rows)
		}
		if columns <= 65535 && rows*columns > 65535 {
			t.Fatalf("columns %d rows %d exceed bind limit", columns, rows)
		}
	}
}

func TestPGOutputBinarySafetySelection(t *testing.T) {
	t.Parallel()
	builtins := []Relation{{Columns: []Column{{Type: 23}, {Type: 1007}}}}
	if !PGOutputBinarySafe(builtins) {
		t.Fatal("built-in types were not binary safe")
	}
	custom := []Relation{{Columns: []Column{{Type: 20000}}}}
	if PGOutputBinarySafe(custom) {
		t.Fatal("user-defined type was binary safe")
	}
	args := pgoutputPluginArgs("pub", false)
	if !slices.Contains(args, "binary 'false'") {
		t.Fatalf("text pgoutput arguments = %v", args)
	}
}

func BenchmarkApplyPreparation(b *testing.B) {
	relation := preparationRelation()
	tuple := Tuple{
		{Kind: DatumText, Data: []byte("7")},
		{Kind: DatumText, Data: []byte("payload")},
	}
	b.ReportAllocs()
	for range b.N {
		var sql strings.Builder
		params := make([]rawParam, 0, 1)
		if err := appendPredicate(&sql, &params, relation, tuple, ChangeDelete); err != nil {
			b.Fatal(err)
		}
	}
}

func preparationRelation() *targetRelation {
	return &targetRelation{
		source: Relation{
			OID:             1,
			Namespace:       "Odd Schema",
			Name:            "order",
			ReplicaIdentity: 'd',
			Columns: []Column{
				{Name: "select", Type: 23, Flags: 1},
				{Name: "payload", Type: 25},
			},
		},
		quoted: `"Odd Schema"."order"`,
		columns: []targetColumn{
			{name: "select", quoted: `"select"`, oid: 23, key: true, sourceIndex: 0, notNull: true},
			{name: "payload", quoted: `"payload"`, oid: 25, sourceIndex: 1},
		},
	}
}
