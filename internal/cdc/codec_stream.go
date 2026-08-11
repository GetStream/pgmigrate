package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

type transactionPrefix struct {
	transaction Transaction
	version     byte
	changes     uint32
}

type remainingEncodedReader interface {
	io.Reader
	Remaining() int64
}

type boundedEncodedReader struct {
	reader    io.Reader
	remaining int64
}

func (r *boundedEncodedReader) Read(buffer []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(buffer)) > r.remaining {
		buffer = buffer[:r.remaining]
	}
	n, err := r.reader.Read(buffer)
	r.remaining -= int64(n)
	return n, err
}

func (r *boundedEncodedReader) Remaining() int64 { return r.remaining }

type teeEncodedReader struct {
	reader remainingEncodedReader
	writer io.Writer
}

func (r *teeEncodedReader) Read(buffer []byte) (int, error) {
	n, err := r.reader.Read(buffer)
	if n != 0 {
		if writeErr := writeBytes(r.writer, buffer[:n]); writeErr != nil {
			return n, writeErr
		}
	}
	return n, err
}

func (r *teeEncodedReader) Remaining() int64 { return r.reader.Remaining() }

func decodeTransactionPrefix(reader io.Reader) (transactionPrefix, error) {
	var result transactionPrefix
	magic, err := readN(reader, 4)
	if err != nil || string(magic) != string(payloadMagic[:]) {
		return result, errors.New("cdc: invalid payload magic")
	}
	if result.version, err = readByte(reader); err != nil {
		return result, err
	}
	if result.version != 1 && result.version != payloadVersion {
		return result, fmt.Errorf("cdc: unsupported payload version %d", result.version)
	}
	commit, err := readUint64(reader)
	if err != nil {
		return result, err
	}
	end, err := readUint64(reader)
	if err != nil {
		return result, err
	}
	seconds, err := readUint64(reader)
	if err != nil {
		return result, err
	}
	nanos, err := readUint32(reader)
	if err != nil || nanos >= 1_000_000_000 {
		return result, errors.New("cdc: invalid commit timestamp")
	}
	result.transaction.CommitLSN = LSN(commit)
	result.transaction.EndLSN = LSN(end)
	result.transaction.CommitTime = time.Unix(int64(seconds), int64(nanos)).UTC()
	relationCount, err := readUint32(reader)
	if err != nil {
		return result, err
	}
	if err := validateEncodedCount(reader, relationCount, 17, 4, "relation"); err != nil {
		return result, err
	}
	result.transaction.Relations = make([]Relation, int(relationCount))
	for i := range result.transaction.Relations {
		if err := decodeRelationReader(reader, &result.transaction.Relations[i]); err != nil {
			return result, fmt.Errorf("cdc: relation %d: %w", i, err)
		}
	}
	result.changes, err = readUint32(reader)
	if err == nil {
		err = validateEncodedCount(reader, result.changes, 7, 0, "change")
	}
	return result, err
}

func decodeRelationReader(reader io.Reader, relation *Relation) error {
	var err error
	if relation.OID, err = readUint32(reader); err != nil {
		return err
	}
	if relation.ReplicaIdentity, err = readByte(reader); err != nil {
		return err
	}
	namespace, err := readLengthBytes(reader)
	if err != nil {
		return err
	}
	relation.Namespace = string(namespace)
	name, err := readLengthBytes(reader)
	if err != nil {
		return err
	}
	relation.Name = string(name)
	count, err := readUint32(reader)
	if err != nil {
		return err
	}
	if err := validateEncodedCount(reader, count, 9, 0, "column"); err != nil {
		return err
	}
	relation.Columns = make([]Column, int(count))
	for i := range relation.Columns {
		if relation.Columns[i].Type, err = readUint32(reader); err != nil {
			return err
		}
		if relation.Columns[i].Flags, err = readByte(reader); err != nil {
			return err
		}
		value, err := readLengthBytes(reader)
		if err != nil {
			return err
		}
		relation.Columns[i].Name = string(value)
	}
	return nil
}

func decodeChangeReader(reader io.Reader, version byte) (Change, error) {
	var change Change
	var err error
	if change.RelationOID, err = readUint32(reader); err != nil {
		return change, err
	}
	kind, err := readByte(reader)
	if err != nil {
		return change, err
	}
	change.Kind = ChangeKind(kind)
	switch change.Kind {
	case ChangeInsert, ChangeUpdate, ChangeDelete, ChangeTruncate:
	default:
		return change, fmt.Errorf("invalid kind %d", change.Kind)
	}
	if version >= 2 && change.Kind == ChangeTruncate {
		options, err := readByte(reader)
		if err != nil || options&^byte(3) != 0 {
			return change, fmt.Errorf("invalid truncate options %02x", options)
		}
		change.TruncateCascade = options&1 != 0
		change.TruncateRestartIdentity = options&2 != 0
	}
	if change.Old, err = decodeTupleReader(reader); err != nil {
		return change, fmt.Errorf("old tuple: %w", err)
	}
	if change.New, err = decodeTupleReader(reader); err != nil {
		return change, fmt.Errorf("new tuple: %w", err)
	}
	return change, nil
}

func decodeTupleReader(reader io.Reader) (*Tuple, error) {
	present, err := readByte(reader)
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	if present != 1 {
		return nil, fmt.Errorf("invalid tuple presence %d", present)
	}
	count, err := readUint32(reader)
	if err != nil {
		return nil, err
	}
	if err := validateEncodedCount(reader, count, 1, 0, "datum"); err != nil {
		return nil, err
	}
	tuple := make(Tuple, int(count))
	for i := range tuple {
		kind, err := readByte(reader)
		if err != nil {
			return nil, err
		}
		tuple[i].Kind = DatumKind(kind)
		switch tuple[i].Kind {
		case DatumNull, DatumUnchangedToast:
		case DatumText, DatumBinary:
			tuple[i].Data, err = readLengthBytes(reader)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("datum %d: invalid kind %d", i, tuple[i].Kind)
		}
	}
	return &tuple, nil
}

func readLengthBytes(reader io.Reader) ([]byte, error) {
	length, err := readUint32(reader)
	if err != nil {
		return nil, err
	}
	return readN(reader, int(length))
}

func readByte(reader io.Reader) (byte, error) {
	var value [1]byte
	_, err := io.ReadFull(reader, value[:])
	return value[0], err
}

func readUint32(reader io.Reader) (uint32, error) {
	var value [4]byte
	_, err := io.ReadFull(reader, value[:])
	return binary.LittleEndian.Uint32(value[:]), err
}

func readUint64(reader io.Reader) (uint64, error) {
	var value [8]byte
	_, err := io.ReadFull(reader, value[:])
	return binary.LittleEndian.Uint64(value[:]), err
}

func readN(reader io.Reader, length int) ([]byte, error) {
	if length < 0 {
		return nil, errors.New("cdc: invalid encoded length")
	}
	if bounded, ok := reader.(remainingEncodedReader); ok && int64(length) > bounded.Remaining() {
		return nil, io.ErrUnexpectedEOF
	}
	value := make([]byte, length)
	_, err := io.ReadFull(reader, value)
	return value, err
}

func validateEncodedCount(
	reader io.Reader,
	count uint32,
	minimumItemBytes uint64,
	reserve uint64,
	kind string,
) error {
	bounded, ok := reader.(remainingEncodedReader)
	if !ok {
		return nil
	}
	remaining := bounded.Remaining()
	if remaining < 0 || uint64(remaining) < reserve {
		return io.ErrUnexpectedEOF
	}
	available := uint64(remaining) - reserve
	if minimumItemBytes != 0 && uint64(count) > available/minimumItemBytes {
		return fmt.Errorf("cdc: encoded %s count %d exceeds remaining %d bytes", kind, count, remaining)
	}
	return nil
}
