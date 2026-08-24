package cdc

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
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
	loader := func(_ context.Context, _ targetRelationQuerier, relation *Relation) (*targetRelation, error) {
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

func TestBatchIdentityPredicateUsesExactBTreeRowBounds(t *testing.T) {
	t.Parallel()
	columns := []targetColumn{
		{quoted: `"app_pk"`, notNull: true},
		{quoted: `"user_id"`, notNull: true},
		{quoted: `"channel_cid"`, notNull: true},
	}
	var sql strings.Builder
	writeBatchIdentityPredicate(&sql, columns, "identity_", 2)
	want := `ROW(pgmigrate_target."app_pk",pgmigrate_target."user_id",pgmigrate_target."channel_cid")>=` +
		`ROW(pgmigrate_batch.identity_2,pgmigrate_batch.identity_3,pgmigrate_batch.identity_4) AND ` +
		`ROW(pgmigrate_target."app_pk",pgmigrate_target."user_id",pgmigrate_target."channel_cid")<=` +
		`ROW(pgmigrate_batch.identity_2,pgmigrate_batch.identity_3,pgmigrate_batch.identity_4)`
	if got := sql.String(); got != want {
		t.Fatalf("predicate = %q, want %q", got, want)
	}
}

func TestSelectiveTargetRowsCTEUsesExactIdentityEquality(t *testing.T) {
	t.Parallel()
	relation := &targetRelation{
		quoted: `"shard_schema"."channels"`,
		columns: []targetColumn{
			{name: "app_pk", quoted: `"app_pk"`},
			{name: "cid", quoted: `"cid"`},
			{name: "custom", quoted: `"custom"`},
		},
	}
	var sql strings.Builder
	writeSelectiveTargetRowsCTE(
		&sql,
		relation,
		relation.columns[:2],
		[]int{2},
		[][]int{{7, 8}, {9, 10}},
	)
	got := sql.String()
	want := `WHERE (pgmigrate_bitmap_target."app_pk"=$7 AND pgmigrate_bitmap_target."cid"=$8) OR ` +
		`(pgmigrate_bitmap_target."app_pk"=$9 AND pgmigrate_bitmap_target."cid"=$10)`
	if !strings.Contains(got, want) {
		t.Fatalf("bitmap predicate = %q, want exact composite identities %q", got, want)
	}
	if strings.Contains(got, ">=") || strings.Contains(got, "<=") {
		t.Fatalf("bitmap predicate contains a range bound: %q", got)
	}
}

func TestSelectiveBitmapUsesTargetCacheEvidence(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		relation targetRelation
		want     bool
	}{
		"small heap uses direct primary-key probes": {
			relation: targetRelation{heapBytes: selectiveBitmapMinHeapBytes - 1},
		},
		"large heap without enough evidence stays conservative": {
			relation: targetRelation{heapBytes: selectiveBitmapMinHeapBytes},
			want:     true,
		},
		"large cold heap uses bitmap reads": {
			relation: targetRelation{
				heapBytes:      selectiveBitmapMinHeapBytes,
				heapBlocksRead: 600_000, heapBlocksHit: 400_000,
			},
			want: true,
		},
		"large resident heap keeps direct primary-key probes": {
			relation: targetRelation{
				heapBytes:      selectiveBitmapMinHeapBytes,
				heapBlocksRead: 1_000, heapBlocksHit: selectiveDirectMinHeapBlocks,
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := useSelectiveBitmap(&test.relation); got != test.want {
				t.Fatalf("useSelectiveBitmap() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestResidentCompositeUpdateSkipsTextStageWithoutBitmapGuard(t *testing.T) {
	t.Parallel()
	relation := &targetRelation{
		heapBytes:      selectiveBitmapMinHeapBytes,
		heapBlocksRead: 1_000,
		heapBlocksHit:  selectiveDirectMinHeapBlocks,
		capabilities: targetRelationCapabilities{
			selectiveUpdates: true,
			textCopyStage:    true,
		},
	}
	identityColumns := []targetColumn{{quoted: `"app_pk"`}, {quoted: `"id"`}}
	if useSelectiveBitmap(relation) {
		t.Fatal("test relation must exercise the cache-resident path")
	}
	if useExactIdentityMembership(relation, identityColumns) {
		t.Fatal("composite identity enabled the BitmapOr guard")
	}
	changes := make([]Change, minimumTextCopyStageRows)
	applied, err := applyUpdateTextStage(nil, relation, identityColumns, nil, changes)
	if err != nil {
		t.Fatal(err)
	}
	if applied {
		t.Fatal("cache-resident composite update used text staging")
	}
}

func TestBatchIdentityPredicateKeepsSingleColumnEquality(t *testing.T) {
	t.Parallel()
	var sql strings.Builder
	writeBatchIdentityPredicate(&sql, []targetColumn{{quoted: `"id"`, notNull: true}}, "column_", 7)
	want := `pgmigrate_target."id"=pgmigrate_batch.column_7`
	if got := sql.String(); got != want {
		t.Fatalf("predicate = %q, want %q", got, want)
	}
}

func TestCompositeUpdatePredicateForcesPrimaryKeyCTIDLookup(t *testing.T) {
	t.Parallel()
	var sql strings.Builder
	relation := &targetRelation{quoted: `"public"."items"`}
	identity := []targetColumn{{quoted: `"app_pk"`}, {quoted: `"id"`}}
	writeCompositeIdentityCTIDPredicate(&sql, relation, identity, "identity_", 0)
	want := `pgmigrate_target.ctid=(SELECT pgmigrate_lookup.ctid FROM "public"."items" AS pgmigrate_lookup WHERE pgmigrate_lookup."app_pk"=pgmigrate_batch.identity_0 AND pgmigrate_lookup."id"=pgmigrate_batch.identity_1 OFFSET 0)`
	if got := sql.String(); got != want {
		t.Fatalf("predicate = %q, want %q", got, want)
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

func TestArrayParamPreservesBinaryValuesAndNulls(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	column := relation.columns[0]
	column.arrayOID = 1007
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, 7)
	param, supported, err := arrayParamForColumn(relation, column, []TupleDatum{
		{Kind: DatumBinary, Data: value},
		{Kind: DatumNull},
	}, ChangeUpdate)
	if err != nil {
		t.Fatal(err)
	}
	if !supported || param.oid != 1007 || param.format != 1 {
		t.Fatalf("binary array supported=%t oid=%d format=%d", supported, param.oid, param.format)
	}
	if len(param.data) != 32 {
		t.Fatalf("binary array bytes=%d, want 32", len(param.data))
	}
	wantWords := []uint32{1, 1, 23, 2, 1, 4, 7, ^uint32(0)}
	for i, want := range wantWords {
		if got := binary.BigEndian.Uint32(param.data[i*4:]); got != want {
			t.Fatalf("binary array word %d=%d, want %d", i, got, want)
		}
	}
}

func TestArrayParamTextEscapingAndMixedFallback(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	column := relation.columns[1]
	column.arrayOID = 1009
	param, supported, err := arrayParamForColumn(relation, column, []TupleDatum{
		{Kind: DatumText, Data: []byte(`a,"\`)},
		{Kind: DatumNull},
	}, ChangeInsert)
	if err != nil {
		t.Fatal(err)
	}
	if !supported || param.format != 0 || string(param.data) != `{"a,\"\\",NULL}` {
		t.Fatalf("text array supported=%t format=%d data=%q", supported, param.format, param.data)
	}
	_, supported, err = arrayParamForColumn(relation, column, []TupleDatum{
		{Kind: DatumText, Data: []byte("7")},
		{Kind: DatumBinary, Data: []byte("7")},
	}, ChangeInsert)
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		t.Fatal("mixed text/binary array unexpectedly supported")
	}
}

func TestTextCopyStageDataEscapesNullEmptyAndControlBytes(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	relation.source.Columns[1].Type = 20000
	column := relation.columns[1]
	column.oid = 20000
	data, supported, err := textCopyStageData(
		relation,
		ChangeInsert,
		[]targetColumn{column},
		[]TupleDatum{
			{Kind: DatumNull},
			{Kind: DatumText},
			{Kind: DatumText, Data: []byte(`\N`)},
			{Kind: DatumText, Data: []byte("a\tb\nc\r\\d\be\ff\vg")},
		},
		4,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !supported {
		t.Fatal("text stage unexpectedly unsupported")
	}
	want := "0\t\\N\n1\t\n2\t\\\\N\n3\ta\\tb\\nc\\r\\\\d\\be\\ff\\vg\n"
	if string(data) != want {
		t.Fatalf("stage data = %q, want %q", data, want)
	}

	_, supported, err = textCopyStageData(
		relation,
		ChangeInsert,
		[]targetColumn{column},
		[]TupleDatum{{Kind: DatumBinary}},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		t.Fatal("binary datum unexpectedly supported by text stage")
	}
}

func TestTextCopyStageNameTracksShapeAndOperation(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	relation.columns[1].oid = 20000
	insert := textCopyStageName(relation, ChangeInsert, relation.columns)
	if insert != textCopyStageName(relation, ChangeInsert, relation.columns) {
		t.Fatal("stage name is not deterministic")
	}
	if insert == textCopyStageName(relation, ChangeUpdate, relation.columns) {
		t.Fatal("insert and update stages share a name")
	}
	if insert == textCopyStageName(relation, ChangeInsert, relation.columns[:1]) {
		t.Fatal("different stage shapes share a name")
	}
}

func TestRelationReplayPlanIsolatesOrderedBarriers(t *testing.T) {
	t.Parallel()
	safeA := &targetRelation{capabilities: targetRelationCapabilities{relationLane: true}}
	safeB := &targetRelation{capabilities: targetRelationCapabilities{relationLane: true}}
	ordered := &targetRelation{}
	transactions := []Transaction{
		{Changes: []Change{
			{RelationOID: 1, Kind: ChangeInsert},
			{RelationOID: 2, Kind: ChangeInsert},
		}},
		{Changes: []Change{
			{RelationOID: 3, Kind: ChangeInsert},
			{RelationOID: 3, Kind: ChangeUpdate},
		}},
		{Changes: []Change{
			{RelationOID: 1, Kind: ChangeDelete},
			{RelationOID: 2, Kind: ChangeUpdate},
		}},
	}
	relations := []map[uint32]*targetRelation{
		{1: safeA, 2: safeB},
		{3: ordered},
		{1: safeA, 2: safeB},
	}
	steps, planned, err := planRelationBatchedChanges(
		relations, transactions, make([]*sampleCollector, len(transactions)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !planned || len(steps) != 3 {
		t.Fatalf("planned=%t steps=%d, want true/3", planned, len(steps))
	}
	if steps[0].ordered || len(steps[0].lanes) != 2 ||
		!steps[1].ordered || len(steps[1].items) != 2 ||
		steps[2].ordered || len(steps[2].lanes) != 2 {
		t.Fatalf("unexpected replay plan: %#v", steps)
	}
	if steps[1].items[0].transactionIndex != 1 || steps[1].items[1].transactionIndex != 1 {
		t.Fatalf("ordered barrier lost source transaction: %#v", steps[1].items)
	}
}

func TestRelationReplayPlanTreatsTruncateAsBarrierAndSpillAsFallback(t *testing.T) {
	t.Parallel()
	safe := &targetRelation{capabilities: targetRelationCapabilities{relationLane: true}}
	transactions := []Transaction{{Changes: []Change{
		{RelationOID: 1, Kind: ChangeInsert},
		{RelationOID: 1, Kind: ChangeTruncate},
		{RelationOID: 1, Kind: ChangeInsert},
	}}}
	steps, planned, err := planRelationBatchedChanges(
		[]map[uint32]*targetRelation{{1: safe}}, transactions, []*sampleCollector{nil},
	)
	if err != nil || !planned || len(steps) != 3 || !steps[1].ordered {
		t.Fatalf("truncate plan=%#v planned=%t err=%v", steps, planned, err)
	}

	transactions[0].Spill = &TransactionSpill{}
	steps, planned, err = planRelationBatchedChanges(
		[]map[uint32]*targetRelation{{1: safe}}, transactions, []*sampleCollector{nil},
	)
	if err != nil || planned || steps != nil {
		t.Fatalf("spill plan=%#v planned=%t err=%v", steps, planned, err)
	}
}

func TestBatchUpdateOmitsUnchangedIdentityAndRejectsUniqueAssignments(t *testing.T) {
	t.Parallel()
	relation := preparationRelation()
	relation.columns[0].conflicting = true
	change := Change{
		Old: &Tuple{
			{Kind: DatumText, Data: []byte("7")},
			{Kind: DatumNull},
		},
		New: &Tuple{
			{Kind: DatumText, Data: []byte("7")},
			{Kind: DatumText, Data: []byte("new")},
		},
	}
	setColumns := updateSetColumnIndexes(relation, &change)
	if !slices.Equal(setColumns, []int{1}) || !updateSetColumnsBatchSafe(relation, setColumns) {
		t.Fatalf("unchanged identity set columns=%v safe=%t", setColumns, updateSetColumnsBatchSafe(relation, setColumns))
	}

	(*change.New)[0].Data = []byte("8")
	setColumns = updateSetColumnIndexes(relation, &change)
	if !slices.Equal(setColumns, []int{0, 1}) || updateSetColumnsBatchSafe(relation, setColumns) {
		t.Fatalf("changed identity set columns=%v safe=%t", setColumns, updateSetColumnsBatchSafe(relation, setColumns))
	}

	(*change.New)[0].Data = []byte("7")
	relation.columns[1].conflicting = true
	setColumns = updateSetColumnIndexes(relation, &change)
	if updateSetColumnsBatchSafe(relation, setColumns) {
		t.Fatalf("unique non-identity assignment was batch safe: %v", setColumns)
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
		capabilities: targetRelationCapabilities{
			relationLane: true, keyedSetDML: true, binaryCopy: true, textCopyStage: true,
		},
		columns: []targetColumn{
			{name: "select", quoted: `"select"`, oid: 23, key: true, sourceIndex: 0, notNull: true},
			{name: "payload", quoted: `"payload"`, oid: 25, sourceIndex: 1},
		},
	}
}
