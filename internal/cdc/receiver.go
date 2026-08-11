package cdc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var replicationNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// DurableWatermark publishes the highest fsynced complete transaction EndLSN.
type DurableWatermark struct {
	value atomic.Uint64
}

func (w *DurableWatermark) Load() LSN {
	if w == nil {
		return 0
	}
	return LSN(w.value.Load())
}

// Publish raises the durable watermark. A lower value is ignored.
func (w *DurableWatermark) Publish(lsn LSN) {
	if w == nil {
		return
	}
	for {
		old := w.value.Load()
		if uint64(lsn) <= old || w.value.CompareAndSwap(old, uint64(lsn)) {
			return
		}
	}
}

// BackpressureError means the bounded handoff remained blocked long enough
// that the replication session was deliberately closed instead of pretending
// WAL could be drained indefinitely.
type BackpressureError struct {
	Duration time.Duration
}

func (e *BackpressureError) Error() string {
	return fmt.Sprintf("cdc: replication handoff blocked for %s; reconnect required", e.Duration)
}

type ReceiverConfig struct {
	ConnString       string
	Slot             string
	Publication      string
	StartLSN         LSN
	Transactions     chan<- Transaction
	Durable          *DurableWatermark
	FeedbackInterval time.Duration
	WriteTimeout     time.Duration
	Backpressure     time.Duration
	ReconnectDelay   time.Duration
	SpillThreshold   int64
	SpillDirectory   string
	// Binary selects pgoutput binary tuples. Nil preserves the default true.
	Binary *bool
	// EndPosition returns an optional inclusive cutover boundary.
	EndPosition func(context.Context) (LSN, bool, error)
}

// Receiver owns its replication connection. Run reconnects transport failures
// from the latest durable EndLSN and exits on cancellation, invalid protocol
// data, or prolonged handoff backpressure.
type Receiver struct {
	config ReceiverConfig
}

func NewReceiver(config ReceiverConfig) (*Receiver, error) {
	if config.ConnString == "" {
		return nil, errors.New("cdc: receiver connection string is required")
	}
	if !replicationNamePattern.MatchString(config.Slot) {
		return nil, errors.New("cdc: receiver slot must be an unquoted PostgreSQL identifier")
	}
	if config.Publication == "" || strings.IndexByte(config.Publication, 0) >= 0 {
		return nil, errors.New("cdc: receiver publication is required")
	}
	if config.Transactions == nil || cap(config.Transactions) == 0 {
		return nil, errors.New("cdc: receiver requires a bounded buffered transaction channel")
	}
	if config.Durable == nil {
		return nil, errors.New("cdc: receiver durable watermark is required")
	}
	if config.FeedbackInterval <= 0 {
		config.FeedbackInterval = 10 * time.Second
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 5 * time.Second
	}
	if config.Backpressure <= 0 {
		config.Backpressure = 30 * time.Second
	}
	if config.ReconnectDelay <= 0 {
		config.ReconnectDelay = time.Second
	}
	return &Receiver{config: config}, nil
}

func (r *Receiver) Run(ctx context.Context) error {
	for {
		err := r.runSession(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var backpressure *BackpressureError
		if errors.As(err, &backpressure) {
			return err
		}
		if !isConnectionError(err) {
			return err
		}
		timer := time.NewTimer(r.config.ReconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Receiver) runSession(ctx context.Context) error {
	connConfig, err := replicationConnConfig(r.config.ConnString)
	if err != nil {
		return err
	}
	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return fmt.Errorf("cdc: connect replication receiver: %w", err)
	}
	defer conn.Close(context.Background())

	start := receiverResumeLSN(r.config.StartLSN, r.config.Durable)
	binaryMode := true
	if r.config.Binary != nil {
		binaryMode = *r.config.Binary
	}
	err = pglogrepl.StartReplication(ctx, conn, r.config.Slot, pglogrepl.LSN(start), pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: pgoutputPluginArgs(r.config.Publication, binaryMode),
	})
	if err != nil {
		return fmt.Errorf("cdc: start pgoutput replication: %w", err)
	}

	var decoder *PGOutputDecoder
	if r.config.SpillThreshold == 0 && r.config.SpillDirectory == "" {
		decoder = NewPGOutputDecoder()
	} else {
		decoder, err = NewPGOutputDecoderWithConfig(PGOutputDecoderConfig{
			SpillThreshold: r.config.SpillThreshold,
			SpillDirectory: r.config.SpillDirectory,
		})
		if err != nil {
			return err
		}
	}
	defer decoder.Close()
	nextFeedback := time.Now().Add(r.config.FeedbackInterval)
	for {
		if r.config.EndPosition != nil {
			end, set, err := r.config.EndPosition(ctx)
			if err != nil {
				return err
			}
			if set && r.config.Durable.Load() >= end {
				return nil
			}
		}
		wait := time.Until(nextFeedback)
		if wait <= 0 {
			if err := r.feedback(conn, false); err != nil {
				return err
			}
			nextFeedback = time.Now().Add(r.config.FeedbackInterval)
			continue
		}
		receiveCtx, cancel := context.WithTimeout(ctx, wait)
		message, receiveErr := conn.ReceiveMessage(receiveCtx)
		cancel()
		if receiveErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(receiveErr, context.DeadlineExceeded) {
				if err := r.feedback(conn, false); err != nil {
					return err
				}
				nextFeedback = time.Now().Add(r.config.FeedbackInterval)
				continue
			}
			return fmt.Errorf("cdc: receive replication message: %w", receiveErr)
		}

		copyData, ok := message.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("cdc: parse primary keepalive: %w", err)
			}
			if keepalive.ReplyRequested {
				if err := r.feedback(conn, true); err != nil {
					return err
				}
				nextFeedback = time.Now().Add(r.config.FeedbackInterval)
			}

		case pglogrepl.XLogDataByteID:
			xlog, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("cdc: parse XLogData: %w", err)
			}
			transaction, err := decoder.Decode(xlog.WALData)
			if err != nil {
				return err
			}
			if transaction != nil {
				if err := r.deliver(ctx, conn, *transaction, &nextFeedback); err != nil {
					return err
				}
			}
		}
	}
}

func pgoutputPluginArgs(publication string, binaryMode bool) []string {
	publication = strings.ReplaceAll(publication, "'", "''")
	binaryValue := "false"
	if binaryMode {
		binaryValue = "true"
	}
	return []string{
		"proto_version '1'",
		"publication_names '" + publication + "'",
		"binary '" + binaryValue + "'",
		"messages 'true'",
		"streaming 'false'",
	}
}

// PGOutputBinarySafe reports whether all selected columns use stable built-in
// PostgreSQL OIDs. Domains, enums, and arrays of user-defined types require
// text pgoutput because their OIDs differ between clusters.
func PGOutputBinarySafe(relations []Relation) bool {
	const firstNormalObjectID = uint32(16384)
	for i := range relations {
		for j := range relations[i].Columns {
			oid := relations[i].Columns[j].Type
			if oid == 0 || oid >= firstNormalObjectID {
				return false
			}
		}
	}
	return true
}

func replicationConnConfig(connString string) (*pgconn.Config, error) {
	config, err := pgconn.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("cdc: parse replication receiver connection string: %w", err)
	}
	config.RuntimeParams["replication"] = "database"
	return config, nil
}

func receiverResumeLSN(initial LSN, durable *DurableWatermark) LSN {
	if durableLSN := durable.Load(); durableLSN != 0 {
		return durableLSN
	}
	return initial
}

func (r *Receiver) deliver(
	ctx context.Context,
	conn *pgconn.PgConn,
	transaction Transaction,
	nextFeedback *time.Time,
) (err error) {
	delivered := false
	defer func() {
		if !delivered {
			_ = transaction.CleanupSpill()
		}
	}()
	blocked := time.NewTimer(r.config.Backpressure)
	defer blocked.Stop()
	feedback := time.NewTicker(r.config.FeedbackInterval)
	defer feedback.Stop()
	for {
		select {
		case r.config.Transactions <- transaction:
			delivered = true
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-blocked.C:
			return &BackpressureError{Duration: r.config.Backpressure}
		case <-feedback.C:
			if err := r.feedback(conn, false); err != nil {
				return err
			}
			*nextFeedback = time.Now().Add(r.config.FeedbackInterval)
		}
	}
}

func (r *Receiver) feedback(conn *pgconn.PgConn, requested bool) error {
	deadline := time.Now().Add(r.config.WriteTimeout)
	if err := conn.Conn().SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("cdc: set replication feedback write deadline: %w", err)
	}
	defer conn.Conn().SetWriteDeadline(time.Time{})
	durable := pglogrepl.LSN(r.config.Durable.Load())
	if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: durable,
		WALFlushPosition: durable,
		WALApplyPosition: durable,
		ReplyRequested:   requested,
	}); err != nil {
		return fmt.Errorf("cdc: send replication feedback: %w", err)
	}
	return nil
}
