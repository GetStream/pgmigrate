package cdc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	payloadVersion     = 2
	maxPayloadSize     = uint64(^uint32(0))
	streamPayloadBytes = uint64(16 << 20)
)

var payloadMagic = [4]byte{'P', 'G', 'M', 'C'}

// MarshalTransaction returns the deterministic binary representation of tx.
func MarshalTransaction(tx *Transaction) ([]byte, error) {
	return AppendTransaction(nil, tx)
}

// AppendTransaction appends the deterministic binary representation of tx to
// dst. Callers may reuse dst's backing storage between calls.
func AppendTransaction(dst []byte, tx *Transaction) ([]byte, error) {
	if tx == nil {
		return nil, errors.New("cdc: nil transaction")
	}
	if tx.Spill != nil {
		return nil, errors.New("cdc: spilled transaction requires WriteTransaction")
	}
	if len(tx.Relations) > int(^uint32(0)) || len(tx.Changes) > int(^uint32(0)) {
		return nil, errors.New("cdc: transaction collection too large")
	}
	size, err := transactionPayloadSize(tx)
	if err != nil {
		return nil, err
	}
	if size > maxPayloadSize {
		return nil, fmt.Errorf("cdc: payload exceeds %d bytes", maxPayloadSize)
	}

	start := len(dst)
	dst, err = appendTransactionPrefix(dst, tx, uint32(len(tx.Changes)))
	if err != nil {
		return nil, err
	}
	for i := range tx.Changes {
		dst, err = appendChange(dst, &tx.Changes[i])
		if err != nil {
			return nil, fmt.Errorf("cdc: change %d: %w", i, err)
		}
	}
	if uint64(len(dst)-start) != size {
		return nil, errors.New("cdc: transaction encoded size mismatch")
	}
	return dst, nil
}

func appendTransactionPrefix(dst []byte, tx *Transaction, changeCount uint32) ([]byte, error) {
	if len(tx.Relations) > int(^uint32(0)) {
		return nil, errors.New("cdc: transaction relation collection too large")
	}
	dst = append(dst, payloadMagic[:]...)
	dst = append(dst, payloadVersion)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(tx.CommitLSN))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(tx.EndLSN))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(tx.CommitTime.Unix()))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(tx.CommitTime.Nanosecond()))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(tx.Relations)))
	for i := range tx.Relations {
		var err error
		dst, err = appendRelation(dst, &tx.Relations[i])
		if err != nil {
			return nil, fmt.Errorf("cdc: relation %d: %w", i, err)
		}
	}
	dst = binary.LittleEndian.AppendUint32(dst, changeCount)
	return dst, nil
}

// WriteTransaction streams the deterministic payload representation to
// writer. Spilled change bytes are copied directly from disk.
func WriteTransaction(writer io.Writer, tx *Transaction) (uint64, error) {
	size, err := transactionPayloadSize(tx)
	if err != nil {
		return 0, err
	}
	if size > maxPayloadSize {
		return 0, fmt.Errorf("cdc: payload exceeds %d bytes", maxPayloadSize)
	}
	prefix, err := appendTransactionPrefix(nil, tx, tx.ChangeCount())
	if err != nil {
		return 0, err
	}
	if err := writeBytes(writer, prefix); err != nil {
		return 0, err
	}
	if tx.Spill != nil {
		reader, changeBytes, _, err := tx.Spill.reader()
		if err != nil {
			return 0, err
		}
		written, err := io.CopyN(writer, reader, int64(changeBytes))
		if err != nil {
			return 0, fmt.Errorf("cdc: stream transaction spill: %w", err)
		}
		if uint64(written) != changeBytes {
			return 0, io.ErrUnexpectedEOF
		}
		return size, nil
	}
	for i := range tx.Changes {
		if _, err := writeChangeTo(writer, &tx.Changes[i]); err != nil {
			return 0, fmt.Errorf("cdc: change %d: %w", i, err)
		}
	}
	return size, nil
}

func transactionPayloadSize(tx *Transaction) (uint64, error) {
	if tx == nil {
		return 0, errors.New("cdc: nil transaction")
	}
	if len(tx.Relations) > int(^uint32(0)) {
		return 0, errors.New("cdc: transaction relation collection too large")
	}
	if tx.Spill != nil && len(tx.Changes) != 0 {
		return 0, errors.New("cdc: transaction has both resident and spilled changes")
	}
	size := uint64(4 + 1 + 8 + 8 + 8 + 4 + 4 + 4)
	for i := range tx.Relations {
		relationSize, err := encodedRelationSize(&tx.Relations[i])
		if err != nil {
			return 0, fmt.Errorf("cdc: relation %d: %w", i, err)
		}
		size += relationSize
	}
	if tx.Spill != nil {
		size += tx.Spill.changeBytes
		return size, nil
	}
	if len(tx.Changes) > int(^uint32(0)) {
		return 0, errors.New("cdc: transaction change collection too large")
	}
	for i := range tx.Changes {
		changeSize, err := encodedChangeSize(&tx.Changes[i])
		if err != nil {
			return 0, fmt.Errorf("cdc: change %d: %w", i, err)
		}
		size += changeSize
	}
	return size, nil
}

func encodedRelationSize(relation *Relation) (uint64, error) {
	if len(relation.Namespace) > int(^uint32(0)) || len(relation.Name) > int(^uint32(0)) {
		return 0, errors.New("relation name too large")
	}
	if len(relation.Columns) > int(^uint32(0)) {
		return 0, errors.New("too many columns")
	}
	size := uint64(4 + 1 + 4 + len(relation.Namespace) + 4 + len(relation.Name) + 4)
	for i := range relation.Columns {
		if len(relation.Columns[i].Name) > int(^uint32(0)) {
			return 0, fmt.Errorf("column %d name too large", i)
		}
		size += uint64(4 + 1 + 4 + len(relation.Columns[i].Name))
	}
	return size, nil
}

func appendRelation(dst []byte, rel *Relation) ([]byte, error) {
	if len(rel.Columns) > int(^uint32(0)) {
		return nil, errors.New("too many columns")
	}
	var err error
	dst = binary.LittleEndian.AppendUint32(dst, rel.OID)
	dst = append(dst, rel.ReplicaIdentity)
	if dst, err = appendBytes(dst, []byte(rel.Namespace)); err != nil {
		return nil, fmt.Errorf("namespace: %w", err)
	}
	if dst, err = appendBytes(dst, []byte(rel.Name)); err != nil {
		return nil, fmt.Errorf("name: %w", err)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(rel.Columns)))
	for i := range rel.Columns {
		col := &rel.Columns[i]
		dst = binary.LittleEndian.AppendUint32(dst, col.Type)
		dst = append(dst, col.Flags)
		if dst, err = appendBytes(dst, []byte(col.Name)); err != nil {
			return nil, fmt.Errorf("column %d name: %w", i, err)
		}
	}
	return dst, nil
}

func appendChange(dst []byte, change *Change) ([]byte, error) {
	switch change.Kind {
	case ChangeInsert, ChangeUpdate, ChangeDelete, ChangeTruncate:
	default:
		return nil, fmt.Errorf("invalid kind %d", change.Kind)
	}
	dst = binary.LittleEndian.AppendUint32(dst, change.RelationOID)
	dst = append(dst, byte(change.Kind))
	if change.Kind == ChangeTruncate {
		var options byte
		if change.TruncateCascade {
			options |= 1
		}
		if change.TruncateRestartIdentity {
			options |= 2
		}
		dst = append(dst, options)
	} else if change.TruncateCascade || change.TruncateRestartIdentity {
		return nil, errors.New("truncate options on non-truncate change")
	}
	var err error
	if dst, err = appendTuple(dst, change.Old); err != nil {
		return nil, fmt.Errorf("old tuple: %w", err)
	}
	if dst, err = appendTuple(dst, change.New); err != nil {
		return nil, fmt.Errorf("new tuple: %w", err)
	}
	return dst, nil
}

func appendTuple(dst []byte, tuple *Tuple) ([]byte, error) {
	if tuple == nil {
		return append(dst, 0), nil
	}
	if len(*tuple) > int(^uint32(0)) {
		return nil, errors.New("too many tuple datums")
	}
	dst = append(dst, 1)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(*tuple)))
	for i := range *tuple {
		datum := &(*tuple)[i]
		switch datum.Kind {
		case DatumNull, DatumUnchangedToast:
			if len(datum.Data) != 0 {
				return nil, fmt.Errorf("datum %d: kind %d must not contain data", i, datum.Kind)
			}
			dst = append(dst, byte(datum.Kind))
		case DatumText, DatumBinary:
			dst = append(dst, byte(datum.Kind))
			var err error
			if dst, err = appendBytes(dst, datum.Data); err != nil {
				return nil, fmt.Errorf("datum %d: %w", i, err)
			}
		default:
			return nil, fmt.Errorf("datum %d: invalid kind %d", i, datum.Kind)
		}
	}
	return dst, nil
}

func encodedChangeSize(change *Change) (uint64, error) {
	switch change.Kind {
	case ChangeInsert, ChangeUpdate, ChangeDelete, ChangeTruncate:
	default:
		return 0, fmt.Errorf("invalid kind %d", change.Kind)
	}
	if change.Kind != ChangeTruncate && (change.TruncateCascade || change.TruncateRestartIdentity) {
		return 0, errors.New("truncate options on non-truncate change")
	}
	size := uint64(4 + 1)
	if change.Kind == ChangeTruncate {
		size++
	}
	oldSize, err := encodedTupleSize(change.Old)
	if err != nil {
		return 0, fmt.Errorf("old tuple: %w", err)
	}
	newSize, err := encodedTupleSize(change.New)
	if err != nil {
		return 0, fmt.Errorf("new tuple: %w", err)
	}
	return size + oldSize + newSize, nil
}

func encodedTupleSize(tuple *Tuple) (uint64, error) {
	if tuple == nil {
		return 1, nil
	}
	if len(*tuple) > int(^uint32(0)) {
		return 0, errors.New("too many tuple datums")
	}
	size := uint64(1 + 4)
	for i := range *tuple {
		datum := &(*tuple)[i]
		size++
		switch datum.Kind {
		case DatumNull, DatumUnchangedToast:
			if len(datum.Data) != 0 {
				return 0, fmt.Errorf("datum %d: kind %d must not contain data", i, datum.Kind)
			}
		case DatumText, DatumBinary:
			if uint64(len(datum.Data)) > uint64(^uint32(0)) {
				return 0, fmt.Errorf("datum %d: value too large", i)
			}
			size += 4 + uint64(len(datum.Data))
		default:
			return 0, fmt.Errorf("datum %d: invalid kind %d", i, datum.Kind)
		}
	}
	return size, nil
}

func writeChangeTo(writer io.Writer, change *Change) (uint64, error) {
	size, err := encodedChangeSize(change)
	if err != nil {
		return 0, err
	}
	var fixed [5]byte
	binary.LittleEndian.PutUint32(fixed[:4], change.RelationOID)
	fixed[4] = byte(change.Kind)
	if err := writeBytes(writer, fixed[:]); err != nil {
		return 0, err
	}
	if change.Kind == ChangeTruncate {
		var options byte
		if change.TruncateCascade {
			options |= 1
		}
		if change.TruncateRestartIdentity {
			options |= 2
		}
		if err := writeBytes(writer, []byte{options}); err != nil {
			return 0, err
		}
	}
	if err := writeTupleTo(writer, change.Old); err != nil {
		return 0, fmt.Errorf("old tuple: %w", err)
	}
	if err := writeTupleTo(writer, change.New); err != nil {
		return 0, fmt.Errorf("new tuple: %w", err)
	}
	return size, nil
}

func writeTupleTo(writer io.Writer, tuple *Tuple) error {
	if tuple == nil {
		return writeBytes(writer, []byte{0})
	}
	var count [5]byte
	count[0] = 1
	binary.LittleEndian.PutUint32(count[1:], uint32(len(*tuple)))
	if err := writeBytes(writer, count[:]); err != nil {
		return err
	}
	for i := range *tuple {
		datum := &(*tuple)[i]
		if err := writeBytes(writer, []byte{byte(datum.Kind)}); err != nil {
			return err
		}
		switch datum.Kind {
		case DatumNull, DatumUnchangedToast:
		case DatumText, DatumBinary:
			var length [4]byte
			binary.LittleEndian.PutUint32(length[:], uint32(len(datum.Data)))
			if err := writeBytes(writer, length[:]); err != nil {
				return err
			}
			if err := writeBytes(writer, datum.Data); err != nil {
				return err
			}
		default:
			return fmt.Errorf("datum %d: invalid kind %d", i, datum.Kind)
		}
	}
	return nil
}

func writeBytes(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if written > 0 {
			data = data[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func appendBytes(dst, value []byte) ([]byte, error) {
	if len(value) > int(^uint32(0)) {
		return nil, errors.New("value too large")
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(value)))
	return append(dst, value...), nil
}

// UnmarshalTransaction decodes one complete transaction payload.
func UnmarshalTransaction(data []byte) (Transaction, error) {
	if uint64(len(data)) > maxPayloadSize {
		return Transaction{}, fmt.Errorf("cdc: payload exceeds %d bytes", maxPayloadSize)
	}
	d := decoder{data: data}
	if magic, err := d.take(4); err != nil || string(magic) != string(payloadMagic[:]) {
		return Transaction{}, errors.New("cdc: invalid payload magic")
	}
	version, err := d.u8()
	if err != nil {
		return Transaction{}, err
	}
	if version != 1 && version != payloadVersion {
		return Transaction{}, fmt.Errorf("cdc: unsupported payload version %d", version)
	}
	d.version = version

	var tx Transaction
	if value, err := d.u64(); err != nil {
		return Transaction{}, err
	} else {
		tx.CommitLSN = LSN(value)
	}
	if value, err := d.u64(); err != nil {
		return Transaction{}, err
	} else {
		tx.EndLSN = LSN(value)
	}
	seconds, err := d.u64()
	if err != nil {
		return Transaction{}, err
	}
	nanos, err := d.u32()
	if err != nil {
		return Transaction{}, err
	}
	if nanos >= 1_000_000_000 {
		return Transaction{}, errors.New("cdc: invalid commit timestamp")
	}
	tx.CommitTime = time.Unix(int64(seconds), int64(nanos)).UTC()

	relationCount, err := d.count(17)
	if err != nil {
		return Transaction{}, fmt.Errorf("cdc: relations: %w", err)
	}
	tx.Relations = make([]Relation, relationCount)
	for i := range tx.Relations {
		if err := d.relation(&tx.Relations[i]); err != nil {
			return Transaction{}, fmt.Errorf("cdc: relation %d: %w", i, err)
		}
	}
	changeCount, err := d.count(7)
	if err != nil {
		return Transaction{}, fmt.Errorf("cdc: changes: %w", err)
	}
	tx.Changes = make([]Change, changeCount)
	for i := range tx.Changes {
		if err := d.change(&tx.Changes[i]); err != nil {
			return Transaction{}, fmt.Errorf("cdc: change %d: %w", i, err)
		}
	}
	if len(d.data) != 0 {
		return Transaction{}, fmt.Errorf("cdc: %d trailing payload bytes", len(d.data))
	}
	return tx, nil
}

type decoder struct {
	data    []byte
	version byte
}

func (d *decoder) take(n int) ([]byte, error) {
	if n < 0 || n > len(d.data) {
		return nil, ioErr()
	}
	value := d.data[:n]
	d.data = d.data[n:]
	return value, nil
}

func (d *decoder) u8() (byte, error) {
	value, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (d *decoder) u32() (uint32, error) {
	value, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (d *decoder) u64() (uint64, error) {
	value, err := d.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (d *decoder) count(minItemBytes int) (int, error) {
	value, err := d.u32()
	if err != nil {
		return 0, err
	}
	if minItemBytes > 0 && uint64(value) > uint64(len(d.data)/minItemBytes) {
		return 0, errors.New("collection count exceeds payload")
	}
	return int(value), nil
}

func (d *decoder) bytes() ([]byte, error) {
	size, err := d.u32()
	if err != nil {
		return nil, err
	}
	return d.take(int(size))
}

func (d *decoder) relation(rel *Relation) error {
	var err error
	if rel.OID, err = d.u32(); err != nil {
		return err
	}
	if rel.ReplicaIdentity, err = d.u8(); err != nil {
		return err
	}
	namespace, err := d.bytes()
	if err != nil {
		return err
	}
	rel.Namespace = string(namespace)
	name, err := d.bytes()
	if err != nil {
		return err
	}
	rel.Name = string(name)
	count, err := d.count(9)
	if err != nil {
		return err
	}
	rel.Columns = make([]Column, count)
	for i := range rel.Columns {
		if rel.Columns[i].Type, err = d.u32(); err != nil {
			return err
		}
		if rel.Columns[i].Flags, err = d.u8(); err != nil {
			return err
		}
		value, err := d.bytes()
		if err != nil {
			return err
		}
		rel.Columns[i].Name = string(value)
	}
	return nil
}

func (d *decoder) change(change *Change) error {
	var err error
	if change.RelationOID, err = d.u32(); err != nil {
		return err
	}
	kind, err := d.u8()
	if err != nil {
		return err
	}
	change.Kind = ChangeKind(kind)
	switch change.Kind {
	case ChangeInsert, ChangeUpdate, ChangeDelete, ChangeTruncate:
	default:
		return fmt.Errorf("invalid kind %d", change.Kind)
	}
	if d.version >= 2 && change.Kind == ChangeTruncate {
		options, err := d.u8()
		if err != nil {
			return err
		}
		if options&^byte(3) != 0 {
			return fmt.Errorf("invalid truncate options %02x", options)
		}
		change.TruncateCascade = options&1 != 0
		change.TruncateRestartIdentity = options&2 != 0
	}
	if change.Old, err = d.tuple(); err != nil {
		return fmt.Errorf("old tuple: %w", err)
	}
	if change.New, err = d.tuple(); err != nil {
		return fmt.Errorf("new tuple: %w", err)
	}
	return nil
}

func (d *decoder) tuple() (*Tuple, error) {
	present, err := d.u8()
	if err != nil {
		return nil, err
	}
	if present == 0 {
		return nil, nil
	}
	if present != 1 {
		return nil, fmt.Errorf("invalid tuple presence %d", present)
	}
	count, err := d.count(1)
	if err != nil {
		return nil, err
	}
	tuple := make(Tuple, count)
	for i := range tuple {
		kind, err := d.u8()
		if err != nil {
			return nil, err
		}
		tuple[i].Kind = DatumKind(kind)
		switch tuple[i].Kind {
		case DatumNull, DatumUnchangedToast:
		case DatumText, DatumBinary:
			value, err := d.bytes()
			if err != nil {
				return nil, err
			}
			// make, not append to a nil slice: appending nothing yields nil,
			// which would make a present zero-length value indistinguishable
			// from an absent one for every later consumer.
			data := make([]byte, len(value))
			copy(data, value)
			tuple[i].Data = data
		default:
			return nil, fmt.Errorf("datum %d: invalid kind %d", i, tuple[i].Kind)
		}
	}
	return &tuple, nil
}

func ioErr() error {
	return errors.New("cdc: truncated payload")
}
