package cdc

import "time"

// LSN is a PostgreSQL write-ahead log position.
type LSN uint64

// Relation describes a relation as emitted by logical replication.
type Relation struct {
	OID             uint32
	Namespace       string
	Name            string
	ReplicaIdentity byte
	Columns         []Column
}

// Column describes one relation column.
type Column struct {
	Name  string
	Type  uint32
	Flags byte
}

// DatumKind identifies the representation of a tuple datum.
type DatumKind byte

const (
	DatumNull DatumKind = iota
	DatumText
	DatumBinary
	DatumUnchangedToast
)

// TupleDatum preserves a logical replication datum without coercion.
// Data is populated only for DatumText and DatumBinary.
type TupleDatum struct {
	Kind DatumKind
	Data []byte
}

// Tuple is an ordered set of column datums.
type Tuple []TupleDatum

// ChangeKind identifies a row-level change.
type ChangeKind byte

const (
	ChangeInsert ChangeKind = iota + 1
	ChangeUpdate
	ChangeDelete
	ChangeTruncate
)

// Change is a logical change against RelationOID. Old and New are nil when
// that tuple was not present in the source message.
type Change struct {
	RelationOID             uint32
	Kind                    ChangeKind
	Old                     *Tuple
	New                     *Tuple
	TruncateCascade         bool
	TruncateRestartIdentity bool
}

// Transaction is the atomic unit stored in a segment record.
// Relations contains the metadata needed by Changes, making each transaction
// independently decodable.
type Transaction struct {
	CommitLSN  LSN
	EndLSN     LSN
	CommitTime time.Time
	Relations  []Relation
	Changes    []Change
	// Spill is non-nil when Changes were encoded to disk by PGOutputDecoder.
	// Callers should treat it as opaque and use the codec/segment APIs.
	Spill *TransactionSpill
}

// ChangeCount returns the number of changes regardless of whether the
// transaction is resident in memory or spilled.
func (tx *Transaction) ChangeCount() uint32 {
	if tx == nil {
		return 0
	}
	if tx.Spill != nil {
		return tx.Spill.changeCount
	}
	return uint32(len(tx.Changes))
}

// IsSpilled reports whether encoded changes are backed by a temporary file.
func (tx *Transaction) IsSpilled() bool {
	return tx != nil && tx.Spill != nil
}

// CleanupSpill removes a spill that will not be appended. Successful segment
// append calls this automatically.
func (tx *Transaction) CleanupSpill() error {
	if tx == nil || tx.Spill == nil {
		return nil
	}
	err := tx.Spill.closeAndRemove()
	tx.Spill = nil
	return err
}
