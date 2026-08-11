package cdc

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestPGOutputDecodeGoldenTransaction(t *testing.T) {
	t.Parallel()
	decoder := NewPGOutputDecoder()
	messages := [][]byte{
		relationMessage(42, "Odd Schema", "order", 'd', []Column{
			{Name: "id", Type: 23, Flags: 1},
			{Name: "note", Type: 25},
			{Name: "raw", Type: 17},
		}),
		beginMessage(0x100, time.Unix(1_700_000_000, 0)),
		insertMessage(42, Tuple{
			{Kind: DatumText, Data: []byte("7")},
			{Kind: DatumNull},
			{Kind: DatumBinary, Data: []byte{0, 1, 2}},
		}),
		updateMessage(42, nil, Tuple{
			{Kind: DatumText, Data: []byte("7")},
			{Kind: DatumUnchangedToast},
			{Kind: DatumBinary, Data: []byte{3, 4}},
		}),
		deleteMessage(42, Tuple{
			{Kind: DatumText, Data: []byte("7")},
			{Kind: DatumNull},
			{Kind: DatumNull},
		}),
		truncateMessage(42),
		commitMessage(0x100, 0x118, time.Unix(1_700_000_001, 123_000)),
	}
	var got *Transaction
	for _, message := range messages {
		transaction, err := decoder.Decode(message)
		if err != nil {
			t.Fatal(err)
		}
		if transaction != nil {
			got = transaction
		}
	}
	if got == nil {
		t.Fatal("commit did not produce a transaction")
	}
	if got.CommitLSN != 0x100 || got.EndLSN != 0x118 {
		t.Fatalf("transaction LSNs = %x/%x", got.CommitLSN, got.EndLSN)
	}
	if len(got.Relations) != 1 || got.Relations[0].Name != "order" {
		t.Fatalf("required relations = %#v", got.Relations)
	}
	if kinds := changeKinds(got.Changes); !reflect.DeepEqual(kinds, []ChangeKind{
		ChangeInsert, ChangeUpdate, ChangeDelete, ChangeTruncate,
	}) {
		t.Fatalf("change kinds = %v", kinds)
	}
	if datum := (*got.Changes[0].New)[2]; datum.Kind != DatumBinary || !reflect.DeepEqual(datum.Data, []byte{0, 1, 2}) {
		t.Fatalf("binary datum = %#v", datum)
	}
	if datum := (*got.Changes[1].New)[1]; datum.Kind != DatumUnchangedToast || datum.Data != nil {
		t.Fatalf("unchanged TOAST datum = %#v", datum)
	}
}

func TestPGOutputRelationCacheRefreshAndRequiredSubset(t *testing.T) {
	t.Parallel()
	decoder := NewPGOutputDecoder()
	for _, message := range [][]byte{
		relationMessage(1, "public", "unused", 'd', []Column{{Name: "id", Type: 23, Flags: 1}}),
		relationMessage(2, "public", "used", 'd', []Column{{Name: "old", Type: 23, Flags: 1}}),
		relationMessage(2, "public", "used", 'd', []Column{{Name: "id", Type: 23, Flags: 1}}),
		beginMessage(10, time.Time{}),
		insertMessage(2, Tuple{{Kind: DatumText, Data: []byte("1")}}),
	} {
		if _, err := decoder.Decode(message); err != nil {
			t.Fatal(err)
		}
	}
	transaction, err := decoder.Decode(commitMessage(10, 11, time.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(transaction.Relations) != 1 || transaction.Relations[0].OID != 2 ||
		transaction.Relations[0].Columns[0].Name != "id" {
		t.Fatalf("relations = %#v", transaction.Relations)
	}
}

func TestPGOutputCutoverMessageProducesKeepTransaction(t *testing.T) {
	t.Parallel()
	decoder := NewPGOutputDecoder()
	tx, err := decoder.Decode(logicalMessage(false, 0x1234, CutoverMessagePrefix, "keep"))
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil || tx.CommitLSN != 0x1234 || tx.EndLSN != 0x1234 || len(tx.Changes) != 0 {
		t.Fatalf("KEEP transaction = %#v", tx)
	}
}

func TestPGOutputIgnoresForeignAndTransactionalMessages(t *testing.T) {
	t.Parallel()
	decoder := NewPGOutputDecoder()
	for _, message := range [][]byte{
		logicalMessage(false, 0x10, "foreign", "ignored"),
		logicalMessage(true, 0x20, CutoverMessagePrefix, "ignored"),
	} {
		tx, err := decoder.Decode(message)
		if err != nil {
			t.Fatal(err)
		}
		if tx != nil {
			t.Fatalf("unexpected transaction = %#v", tx)
		}
	}
}

func TestPGOutputPreservesTruncateOptions(t *testing.T) {
	t.Parallel()
	decoder := NewPGOutputDecoder()
	for _, message := range [][]byte{
		relationMessage(1, "public", "t", 'd', []Column{{Name: "id", Type: 23, Flags: 1}}),
		beginMessage(10, time.Time{}),
	} {
		if _, err := decoder.Decode(message); err != nil {
			t.Fatal(err)
		}
	}
	message := truncateMessage(1)
	message[5] = 3
	if _, err := decoder.Decode(message); err != nil {
		t.Fatal(err)
	}
	transaction, err := decoder.Decode(commitMessage(10, 11, time.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	change := transaction.Changes[0]
	if !change.TruncateCascade || !change.TruncateRestartIdentity {
		t.Fatalf("truncate options = cascade %v/restart %v", change.TruncateCascade, change.TruncateRestartIdentity)
	}
}

func FuzzPGOutputDecode(f *testing.F) {
	f.Add(relationMessage(1, "public", "t", 'd', []Column{{Name: "id", Type: 23, Flags: 1}}))
	f.Add(insertMessage(1, Tuple{{Kind: DatumText, Data: []byte("1")}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		decoder := NewPGOutputDecoder()
		_, _ = decoder.Decode(data)
	})
}

func BenchmarkPGOutputDecode(b *testing.B) {
	relation := relationMessage(1, "public", "bench", 'd', []Column{
		{Name: "id", Type: 23, Flags: 1},
		{Name: "value", Type: 25},
	})
	insert := insertMessage(1, Tuple{
		{Kind: DatumText, Data: []byte("1")},
		{Kind: DatumText, Data: []byte("value")},
	})
	begin := beginMessage(10, time.Time{})
	commit := commitMessage(10, 11, time.Time{})
	b.ReportAllocs()
	for range b.N {
		decoder := NewPGOutputDecoder()
		for _, message := range [][]byte{relation, begin, insert, commit} {
			if _, err := decoder.Decode(message); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkPGOutputDecodeManyChanges(b *testing.B) {
	relation := relationMessage(1, "public", "bench_many", 'd', []Column{
		{Name: "id", Type: 23, Flags: 1},
		{Name: "value", Type: 25},
	})
	insert := insertMessage(1, Tuple{
		{Kind: DatumText, Data: []byte("1")},
		{Kind: DatumText, Data: []byte("value")},
	})
	begin := beginMessage(10, time.Time{})
	commit := commitMessage(10, 11, time.Time{})
	spillDirectory := b.TempDir()
	b.ReportAllocs()
	for range b.N {
		decoder, err := NewPGOutputDecoderWithConfig(PGOutputDecoderConfig{
			SpillThreshold: 1 << 30,
			SpillDirectory: spillDirectory,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := decoder.Decode(relation); err != nil {
			b.Fatal(err)
		}
		if _, err := decoder.Decode(begin); err != nil {
			b.Fatal(err)
		}
		for range 1000 {
			if _, err := decoder.Decode(insert); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := decoder.Decode(commit); err != nil {
			b.Fatal(err)
		}
		_ = decoder.Close()
	}
}

func relationMessage(oid uint32, namespace, name string, identity byte, columns []Column) []byte {
	data := []byte{'R'}
	data = binary.BigEndian.AppendUint32(data, oid)
	data = appendCString(data, namespace)
	data = appendCString(data, name)
	data = append(data, identity)
	data = binary.BigEndian.AppendUint16(data, uint16(len(columns)))
	for _, column := range columns {
		data = append(data, column.Flags)
		data = appendCString(data, column.Name)
		data = binary.BigEndian.AppendUint32(data, column.Type)
		data = binary.BigEndian.AppendUint32(data, ^uint32(0))
	}
	return data
}

func beginMessage(lsn uint64, timestamp time.Time) []byte {
	data := []byte{'B'}
	data = binary.BigEndian.AppendUint64(data, lsn)
	data = binary.BigEndian.AppendUint64(data, uint64(pgMicroseconds(timestamp)))
	return binary.BigEndian.AppendUint32(data, 1)
}

func commitMessage(commit, end uint64, timestamp time.Time) []byte {
	data := []byte{'C', 0}
	data = binary.BigEndian.AppendUint64(data, commit)
	data = binary.BigEndian.AppendUint64(data, end)
	return binary.BigEndian.AppendUint64(data, uint64(pgMicroseconds(timestamp)))
}

func insertMessage(oid uint32, tuple Tuple) []byte {
	data := []byte{'I'}
	data = binary.BigEndian.AppendUint32(data, oid)
	data = append(data, 'N')
	return appendWireTuple(data, tuple)
}

func updateMessage(oid uint32, old *Tuple, tuple Tuple) []byte {
	data := []byte{'U'}
	data = binary.BigEndian.AppendUint32(data, oid)
	if old != nil {
		data = append(data, 'K')
		data = appendWireTuple(data, *old)
	}
	data = append(data, 'N')
	return appendWireTuple(data, tuple)
}

func deleteMessage(oid uint32, tuple Tuple) []byte {
	data := []byte{'D'}
	data = binary.BigEndian.AppendUint32(data, oid)
	data = append(data, 'K')
	return appendWireTuple(data, tuple)
}

func truncateMessage(oids ...uint32) []byte {
	data := []byte{'T'}
	data = binary.BigEndian.AppendUint32(data, uint32(len(oids)))
	data = append(data, 0)
	for _, oid := range oids {
		data = binary.BigEndian.AppendUint32(data, oid)
	}
	return data
}

func logicalMessage(transactional bool, lsn uint64, prefix, content string) []byte {
	flag := byte(0)
	if transactional {
		flag = 1
	}
	data := []byte{'M', flag}
	data = binary.BigEndian.AppendUint64(data, lsn)
	data = appendCString(data, prefix)
	data = binary.BigEndian.AppendUint32(data, uint32(len(content)))
	return append(data, content...)
}

func appendWireTuple(data []byte, tuple Tuple) []byte {
	data = binary.BigEndian.AppendUint16(data, uint16(len(tuple)))
	for _, datum := range tuple {
		switch datum.Kind {
		case DatumNull:
			data = append(data, 'n')
		case DatumUnchangedToast:
			data = append(data, 'u')
		case DatumText:
			data = append(data, 't')
			data = binary.BigEndian.AppendUint32(data, uint32(len(datum.Data)))
			data = append(data, datum.Data...)
		case DatumBinary:
			data = append(data, 'b')
			data = binary.BigEndian.AppendUint32(data, uint32(len(datum.Data)))
			data = append(data, datum.Data...)
		}
	}
	return data
}

func appendCString(data []byte, value string) []byte {
	data = append(data, value...)
	return append(data, 0)
}

func pgMicroseconds(timestamp time.Time) int64 {
	epoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return timestamp.UTC().Sub(epoch).Microseconds()
}

func changeKinds(changes []Change) []ChangeKind {
	result := make([]ChangeKind, len(changes))
	for i := range changes {
		result[i] = changes[i].Kind
	}
	return result
}
