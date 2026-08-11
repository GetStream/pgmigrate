package cdc

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
)

const DefaultDecoderSpillThreshold = int64(256 << 20)

// CutoverMessagePrefix identifies pgmigrate's nontransactional KEEP boundary.
const CutoverMessagePrefix = "pgmigrate_cutover"

var (
	defaultSpillSetup sync.Once
	defaultSpillError error
)

type PGOutputDecoderConfig struct {
	SpillThreshold int64
	SpillDirectory string
}

// PGOutputDecoder incrementally assembles protocol-v1 pgoutput messages into
// complete transactions. Relation messages are cached across transactions.
// It is not safe for concurrent use.
type PGOutputDecoder struct {
	relations      map[uint32]Relation
	current        *Transaction
	required       map[uint32]struct{}
	spillThreshold int64
	spillDirectory string
	encodedSize    uint64
	initErr        error
	closed         bool
}

// NewPGOutputDecoder creates a decoder with an empty relation cache.
func NewPGOutputDecoder() *PGOutputDecoder {
	directory := os.TempDir()
	defaultSpillSetup.Do(func() {
		if err := mkdirAllDurable(directory, 0o750); err != nil {
			defaultSpillError = err
			return
		}
		defaultSpillError = cleanupOrphanSpillsOnce(directory)
	})
	return &PGOutputDecoder{
		relations:      make(map[uint32]Relation),
		spillThreshold: DefaultDecoderSpillThreshold,
		spillDirectory: directory,
		initErr:        defaultSpillError,
	}
}

func NewPGOutputDecoderWithConfig(config PGOutputDecoderConfig) (*PGOutputDecoder, error) {
	if config.SpillThreshold < 0 {
		return nil, errors.New("cdc: decoder spill threshold must not be negative")
	}
	if config.SpillThreshold == 0 {
		config.SpillThreshold = DefaultDecoderSpillThreshold
	}
	if config.SpillDirectory == "" {
		config.SpillDirectory = os.TempDir()
	}
	if err := mkdirAllDurable(config.SpillDirectory, 0o750); err != nil {
		return nil, err
	}
	if err := cleanupOrphanSpillsOnce(config.SpillDirectory); err != nil {
		return nil, err
	}
	return &PGOutputDecoder{
		relations:      make(map[uint32]Relation),
		spillThreshold: config.SpillThreshold,
		spillDirectory: config.SpillDirectory,
	}, nil
}

// Decode consumes one pgoutput message. A non-nil transaction is returned
// only for Commit, after all required cached relation metadata has been added.
func (d *PGOutputDecoder) Decode(data []byte) (tx *Transaction, err error) {
	if d.closed {
		return nil, errors.New("cdc: decode with closed pgoutput decoder")
	}
	if d.initErr != nil {
		return nil, d.initErr
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("cdc: empty pgoutput message")
	}
	// pglogrepl's protocol-v1 decoder assumes trusted server input and some
	// malformed tuple lengths can panic. Convert those into ordinary decode
	// errors so fuzzing and a broken stream cannot take down the process.
	defer func() {
		if recovered := recover(); recovered != nil {
			tx = nil
			err = fmt.Errorf("cdc: malformed pgoutput message: %v", recovered)
		}
		if err != nil {
			_ = d.abortActive()
		}
	}()

	message, err := pglogrepl.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("cdc: decode pgoutput: %w", err)
	}
	switch message := message.(type) {
	case *pglogrepl.RelationMessage:
		relation := Relation{
			OID:             message.RelationID,
			Namespace:       message.Namespace,
			Name:            message.RelationName,
			ReplicaIdentity: message.ReplicaIdentity,
			Columns:         make([]Column, len(message.Columns)),
		}
		for i, column := range message.Columns {
			relation.Columns[i] = Column{
				Name:  column.Name,
				Type:  column.DataType,
				Flags: column.Flags,
			}
		}
		d.relations[relation.OID] = relation
		return nil, nil

	case *pglogrepl.BeginMessage:
		if d.current != nil {
			return nil, fmt.Errorf("cdc: pgoutput Begin while transaction is active")
		}
		d.current = &Transaction{
			CommitLSN:  LSN(message.FinalLSN),
			CommitTime: message.CommitTime.UTC(),
		}
		d.encodedSize = 4 + 1 + 8 + 8 + 8 + 4 + 4 + 4
		d.required = make(map[uint32]struct{})
		return nil, nil

	case *pglogrepl.InsertMessage:
		newTuple, err := decodePGTuple(message.Tuple)
		if err != nil {
			return nil, err
		}
		return nil, d.addChange(Change{
			RelationOID: message.RelationID,
			Kind:        ChangeInsert,
			New:         newTuple,
		})

	case *pglogrepl.UpdateMessage:
		oldTuple, err := decodePGTuple(message.OldTuple)
		if err != nil {
			return nil, err
		}
		newTuple, err := decodePGTuple(message.NewTuple)
		if err != nil {
			return nil, err
		}
		return nil, d.addChange(Change{
			RelationOID: message.RelationID,
			Kind:        ChangeUpdate,
			Old:         oldTuple,
			New:         newTuple,
		})

	case *pglogrepl.DeleteMessage:
		oldTuple, err := decodePGTuple(message.OldTuple)
		if err != nil {
			return nil, err
		}
		return nil, d.addChange(Change{
			RelationOID: message.RelationID,
			Kind:        ChangeDelete,
			Old:         oldTuple,
		})

	case *pglogrepl.TruncateMessage:
		const knownTruncateOptions = pglogrepl.TruncateOptionCascade | pglogrepl.TruncateOptionRestartIdentity
		if message.Option&^knownTruncateOptions != 0 {
			return nil, fmt.Errorf("cdc: unknown pgoutput TRUNCATE options %02x", message.Option)
		}
		for _, relationID := range message.RelationIDs {
			if err := d.addChange(Change{
				RelationOID:             relationID,
				Kind:                    ChangeTruncate,
				TruncateCascade:         message.Option&pglogrepl.TruncateOptionCascade != 0,
				TruncateRestartIdentity: message.Option&pglogrepl.TruncateOptionRestartIdentity != 0,
			}); err != nil {
				return nil, err
			}
		}
		return nil, nil

	case *pglogrepl.CommitMessage:
		if d.current == nil {
			return nil, fmt.Errorf("cdc: pgoutput Commit without Begin")
		}
		if LSN(message.CommitLSN) != d.current.CommitLSN {
			return nil, fmt.Errorf(
				"cdc: pgoutput commit LSN %x differs from Begin final LSN %x",
				message.CommitLSN, d.current.CommitLSN,
			)
		}
		if message.TransactionEndLSN < message.CommitLSN {
			return nil, fmt.Errorf("cdc: pgoutput transaction EndLSN precedes CommitLSN")
		}
		d.current.CommitTime = message.CommitTime.UTC()
		d.current.EndLSN = LSN(message.TransactionEndLSN)
		result := d.current
		d.current = nil
		d.required = nil
		d.encodedSize = 0
		return result, nil

	case *pglogrepl.LogicalDecodingMessage:
		if message.Prefix != CutoverMessagePrefix || message.Transactional {
			return nil, nil
		}
		if d.current != nil {
			return nil, fmt.Errorf("cdc: nontransactional cutover message while transaction is active")
		}
		if message.LSN == 0 {
			return nil, fmt.Errorf("cdc: cutover message has zero LSN")
		}
		// Represent KEEP as an empty transaction. Segment durability and target
		// progress then use the same ordered boundary as ordinary commits.
		return &Transaction{
			CommitLSN:  LSN(message.LSN),
			EndLSN:     LSN(message.LSN),
			CommitTime: time.Now().UTC(),
		}, nil

	default:
		// Type, Origin, and logical decoding messages do not alter row state.
		return nil, nil
	}
}

func (d *PGOutputDecoder) addChange(change Change) error {
	if d.current == nil {
		return fmt.Errorf("cdc: pgoutput row change outside transaction")
	}
	relation, ok := d.relations[change.RelationOID]
	if !ok {
		return fmt.Errorf("cdc: pgoutput relation %d is not cached", change.RelationOID)
	}
	if _, ok := d.required[change.RelationOID]; !ok {
		d.required[change.RelationOID] = struct{}{}
		d.current.Relations = append(d.current.Relations, cloneRelation(relation))
		relationSize, err := encodedRelationSize(&relation)
		if err != nil {
			return err
		}
		d.encodedSize += relationSize
	}
	changeSize, err := encodedChangeSize(&change)
	if err != nil {
		return err
	}
	d.encodedSize += changeSize
	if d.current.Spill != nil {
		return d.current.Spill.appendChange(&change)
	}
	d.current.Changes = append(d.current.Changes, change)
	if d.encodedSize > uint64(d.spillThreshold) {
		return d.spillCurrent()
	}
	return nil
}

func (d *PGOutputDecoder) spillCurrent() error {
	spill, err := newTransactionSpill(d.spillDirectory)
	if err != nil {
		return err
	}
	for i := range d.current.Changes {
		if err := spill.appendChange(&d.current.Changes[i]); err != nil {
			_ = spill.closeAndRemove()
			return err
		}
	}
	d.current.Changes = nil
	d.current.Spill = spill
	return nil
}

func (d *PGOutputDecoder) abortActive() error {
	if d.current == nil {
		return nil
	}
	err := d.current.CleanupSpill()
	d.current = nil
	d.required = nil
	d.encodedSize = 0
	return err
}

// Close aborts an incomplete transaction and releases its spill file.
// Spills attached to already-returned transactions are owned by those
// transactions and remain valid until append or CleanupSpill.
func (d *PGOutputDecoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	return d.abortActive()
}

func decodePGTuple(tuple *pglogrepl.TupleData) (*Tuple, error) {
	if tuple == nil {
		return nil, nil
	}
	result := make(Tuple, len(tuple.Columns))
	for i, column := range tuple.Columns {
		switch column.DataType {
		case pglogrepl.TupleDataTypeNull:
			result[i].Kind = DatumNull
		case pglogrepl.TupleDataTypeText:
			result[i].Kind = DatumText
			result[i].Data = column.Data
		case pglogrepl.TupleDataTypeBinary:
			result[i].Kind = DatumBinary
			result[i].Data = column.Data
		case pglogrepl.TupleDataTypeToast:
			result[i].Kind = DatumUnchangedToast
		default:
			return nil, fmt.Errorf("cdc: unsupported pgoutput tuple kind %q", column.DataType)
		}
	}
	return &result, nil
}

func cloneRelation(relation Relation) Relation {
	relation.Columns = append([]Column(nil), relation.Columns...)
	return relation
}
