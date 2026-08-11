package cdc

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestStreamingDecoderRejectsCountsBeyondRemainingBytes(t *testing.T) {
	t.Parallel()
	prefix := func() []byte {
		payload := append([]byte(nil), payloadMagic[:]...)
		payload = append(payload, payloadVersion)
		payload = binary.LittleEndian.AppendUint64(payload, 1)
		payload = binary.LittleEndian.AppendUint64(payload, 2)
		payload = binary.LittleEndian.AppendUint64(payload, 0)
		payload = binary.LittleEndian.AppendUint32(payload, 0)
		return payload
	}
	tests := map[string][]byte{
		"relations": binary.LittleEndian.AppendUint32(prefix(), ^uint32(0)),
		"columns": func() []byte {
			payload := binary.LittleEndian.AppendUint32(prefix(), 1)
			payload = binary.LittleEndian.AppendUint32(payload, 1)
			payload = append(payload, 0)
			payload = binary.LittleEndian.AppendUint32(payload, 0)
			payload = binary.LittleEndian.AppendUint32(payload, 0)
			payload = binary.LittleEndian.AppendUint32(payload, ^uint32(0))
			return binary.LittleEndian.AppendUint32(payload, 0)
		}(),
		"changes": func() []byte {
			payload := binary.LittleEndian.AppendUint32(prefix(), 0)
			return binary.LittleEndian.AppendUint32(payload, ^uint32(0))
		}(),
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			reader := &boundedEncodedReader{reader: bytes.NewReader(payload), remaining: int64(len(payload))}
			if _, err := decodeTransactionPrefix(reader); err == nil {
				t.Fatal("expected impossible count to fail")
			}
		})
	}

	change := binary.LittleEndian.AppendUint32(nil, 1)
	change = append(change, byte(ChangeInsert), 0, 1)
	change = binary.LittleEndian.AppendUint32(change, ^uint32(0))
	reader := &boundedEncodedReader{reader: bytes.NewReader(change), remaining: int64(len(change))}
	if _, err := decodeChangeReader(reader, payloadVersion); err == nil {
		t.Fatal("expected impossible datum count to fail")
	}
}

func FuzzStreamingDecoderBoundedCounts(f *testing.F) {
	transaction := testTransaction(1)
	payload, err := MarshalTransaction(&transaction)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(payload)
	f.Add([]byte("PGMC"))
	f.Fuzz(func(t *testing.T, payload []byte) {
		reader := &boundedEncodedReader{reader: bytes.NewReader(payload), remaining: int64(len(payload))}
		_, _ = decodeTransactionPrefix(reader)
	})
}

func TestTransactionCodecRoundTrip(t *testing.T) {
	t.Parallel()
	tx := testTransaction(0x10)
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalTransaction(&tx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, again) {
		t.Fatal("encoding is not deterministic")
	}
	decoded, err := UnmarshalTransaction(payload)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := MarshalTransaction(&decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTrip, payload) {
		t.Fatal("round trip encoding changed")
	}
}

func TestAppendTransactionReusesDestination(t *testing.T) {
	t.Parallel()
	tx := testTransaction(1)
	dst := make([]byte, 3, 4096)
	copy(dst, "pre")
	result, err := AppendTransaction(dst, &tx)
	if err != nil {
		t.Fatal(err)
	}
	if string(result[:3]) != "pre" {
		t.Fatalf("destination prefix changed: %q", result[:3])
	}
	decoded, err := UnmarshalTransaction(result[3:])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.CommitLSN != tx.CommitLSN {
		t.Fatalf("CommitLSN = %x, want %x", decoded.CommitLSN, tx.CommitLSN)
	}
}

// TestCodecPreservesPresentZeroLengthDatums compares decoded values rather than
// re-encoded bytes. Re-encoding cannot see this class of defect at all: a
// present datum decoded as nil and one decoded as empty encode to the identical
// length-zero payload, so TestTransactionCodecRoundTrip passed while the
// resident decoder was collapsing an empty string into something the apply path
// then wrote as NULL.
func TestCodecPreservesPresentZeroLengthDatums(t *testing.T) {
	t.Parallel()
	tuple := Tuple{
		{Kind: DatumText, Data: []byte{}},
		{Kind: DatumBinary, Data: []byte{}},
		{Kind: DatumText, Data: []byte("value")},
		{Kind: DatumNull},
		{Kind: DatumUnchangedToast},
	}
	tx := Transaction{
		CommitLSN: 1,
		EndLSN:    2,
		Relations: []Relation{{OID: 1, Namespace: "public", Name: "t"}},
		Changes:   []Change{{RelationOID: 1, Kind: ChangeInsert, New: &tuple}},
	}
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		t.Fatal(err)
	}

	assert := func(t *testing.T, decoder string, decoded Tuple) {
		t.Helper()
		if len(decoded) != len(tuple) {
			t.Fatalf("%s decoded %d datums, want %d", decoder, len(decoded), len(tuple))
		}
		for i := range tuple {
			if decoded[i].Kind != tuple[i].Kind {
				t.Fatalf("%s datum %d kind = %d, want %d", decoder, i, decoded[i].Kind, tuple[i].Kind)
			}
			present := tuple[i].Kind == DatumText || tuple[i].Kind == DatumBinary
			if present && decoded[i].Data == nil {
				t.Fatalf("%s datum %d decoded a present value as nil", decoder, i)
			}
			if !present && decoded[i].Data != nil {
				t.Fatalf("%s datum %d decoded an absent value as %q", decoder, i, decoded[i].Data)
			}
			if !bytes.Equal(decoded[i].Data, tuple[i].Data) {
				t.Fatalf("%s datum %d = %q, want %q", decoder, i, decoded[i].Data, tuple[i].Data)
			}
		}
	}

	resident, err := UnmarshalTransaction(payload)
	if err != nil {
		t.Fatal(err)
	}
	assert(t, "resident", *resident.Changes[0].New)

	reader := &boundedEncodedReader{reader: bytes.NewReader(payload), remaining: int64(len(payload))}
	prefix, err := decodeTransactionPrefix(reader)
	if err != nil {
		t.Fatal(err)
	}
	streamed, err := decodeChangeReader(reader, prefix.version)
	if err != nil {
		t.Fatal(err)
	}
	assert(t, "streamed", *streamed.New)
}

func TestCodecRejectsInvalidDatumState(t *testing.T) {
	t.Parallel()
	tuple := Tuple{{Kind: DatumNull, Data: []byte("not-null")}}
	tx := Transaction{
		CommitLSN: 1,
		EndLSN:    2,
		Changes: []Change{{
			RelationOID: 1,
			Kind:        ChangeInsert,
			New:         &tuple,
		}},
	}
	if _, err := MarshalTransaction(&tx); err == nil {
		t.Fatal("expected invalid null datum to fail")
	}
}

func TestCodecRejectsTruncatedAndTrailingPayload(t *testing.T) {
	t.Parallel()
	tx := testTransaction(1)
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{payload[:len(payload)-1], append(payload, 0)} {
		if _, err := UnmarshalTransaction(data); err == nil {
			t.Fatal("expected malformed payload to fail")
		}
	}
}

func TestCodecPreservesTruncateOptionsAndReadsVersionOne(t *testing.T) {
	t.Parallel()
	tx := Transaction{
		CommitLSN: 1,
		EndLSN:    2,
		Changes: []Change{{
			RelationOID:             42,
			Kind:                    ChangeTruncate,
			TruncateCascade:         true,
			TruncateRestartIdentity: true,
		}},
	}
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalTransaction(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.Changes[0].TruncateCascade || !decoded.Changes[0].TruncateRestartIdentity {
		t.Fatalf("truncate options were not preserved: %#v", decoded.Changes[0])
	}

	// Version 1 encoded no truncate option byte. Keep old staged segments
	// readable after introducing the version 2 option field.
	const versionOffset = 4
	const truncateOptionOffset = 46
	versionOne := append([]byte(nil), payload...)
	versionOne[versionOffset] = 1
	versionOne = append(versionOne[:truncateOptionOffset], versionOne[truncateOptionOffset+1:]...)
	decoded, err = UnmarshalTransaction(versionOne)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Changes[0].TruncateCascade || decoded.Changes[0].TruncateRestartIdentity {
		t.Fatalf("version 1 unexpectedly decoded truncate options: %#v", decoded.Changes[0])
	}
}

func FuzzUnmarshalTransaction(f *testing.F) {
	tx := testTransaction(0x100)
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(payload)
	f.Add([]byte{})
	f.Add([]byte("PGMC\x01"))

	f.Fuzz(func(t *testing.T, data []byte) {
		tx, err := UnmarshalTransaction(data)
		if err != nil {
			return
		}
		first, err := MarshalTransaction(&tx)
		if err != nil {
			t.Fatalf("marshal decoded transaction: %v", err)
		}
		second, err := MarshalTransaction(&tx)
		if err != nil {
			t.Fatalf("marshal decoded transaction again: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatal("decoded transaction did not re-encode deterministically")
		}
		if _, err := UnmarshalTransaction(first); err != nil {
			t.Fatalf("decode re-encoded transaction: %v", err)
		}
	})
}

func BenchmarkAppendTransaction(b *testing.B) {
	tx := testTransaction(1)
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		b.Fatal(err)
	}
	buffer := make([]byte, 0, len(payload))
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		buffer = buffer[:0]
		buffer, err = AppendTransaction(buffer, &tx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalTransaction(b *testing.B) {
	tx := testTransaction(1)
	payload, err := MarshalTransaction(&tx)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		if _, err := UnmarshalTransaction(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func testTransaction(lsn LSN) Transaction {
	oldTuple := Tuple{
		{Kind: DatumText, Data: []byte("42")},
		{Kind: DatumUnchangedToast},
		{Kind: DatumNull},
	}
	newTuple := Tuple{
		{Kind: DatumBinary, Data: []byte{0, 1, 2, 255}},
		{Kind: DatumText, Data: []byte{}},
		{Kind: DatumNull},
	}
	return Transaction{
		CommitLSN:  lsn,
		EndLSN:     lsn + 1,
		CommitTime: time.Unix(1_700_000_000, 123456789).UTC(),
		Relations: []Relation{{
			OID:             1234,
			Namespace:       "public",
			Name:            "widgets",
			ReplicaIdentity: 'd',
			Columns: []Column{
				{Name: "id", Type: 23, Flags: 1},
				{Name: "payload", Type: 17},
				{Name: "optional", Type: 25},
			},
		}},
		Changes: []Change{
			{RelationOID: 1234, Kind: ChangeUpdate, Old: &oldTuple, New: &newTuple},
			{RelationOID: 1234, Kind: ChangeDelete, Old: &oldTuple},
			{RelationOID: 1234, Kind: ChangeTruncate},
		},
	}
}
