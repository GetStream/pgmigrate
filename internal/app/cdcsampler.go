package app

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/tgross/pgmigrate/internal/cdc"
	"github.com/tgross/pgmigrate/internal/state"
	"github.com/tgross/pgmigrate/internal/verify"
)

const (
	// cdcSampleFlushRows and cdcSampleFlushInterval bound how often the reservoir
	// touches SQLite. The state database runs with synchronous=FULL on a single
	// connection, so a write per sample would be an fsync per sample on the same
	// connection the migration uses for its own state.
	cdcSampleFlushRows     = 1000
	cdcSampleFlushInterval = 2 * time.Second
	// cdcSamplePendingLimit is the safety valve, not the normal bound. Samples
	// waiting to be written are held by slot and replace each other, so the
	// buffer is bounded by the distinct slots chosen between two flushes rather
	// than by the rate of change. This only fires if the store has stopped
	// accepting writes entirely.
	cdcSamplePendingLimit = 200_000
)

// cdcSampler keeps a bounded, uniform sample of the rows the applier writes.
//
// Verification cannot find those rows by looking at the source. It samples the
// heap by physical position, and on a bloated heap position says nothing about
// when a row was written, so the replicated rows are missed almost entirely — the
// pass ends up checking the base copy, which is the part that was never in doubt.
// This is the other half: the applier reports what it wrote, and verification
// checks those exact rows by key.
//
// The sample is a reservoir rather than the most recent N. A window of recent
// changes would never look at the catch-up burst, which is where the spill and
// out-of-order paths run and so where a defect is most likely; a prefix would
// never look at anything else. Uniformity over the whole stream costs nothing
// extra and covers recent changes in proportion to how many there have been.
type cdcSampler struct {
	store *state.Store
	// capacity is the hard bound on rows held per relation. Slots are numbered
	// in [0, capacity) by this process, which is what keeps the row count bounded
	// without ever asking SQLite to count anything.
	capacity int64

	mu      sync.Mutex
	streams map[string]*cdcStream
	// pending holds accepted samples by the slot they occupy, so a slot chosen
	// twice before a flush keeps only the later change.
	//
	// It is a map rather than a queue because a queue can only shed load by
	// dropping, and dropping is not neutral here: a dropped sample leaves the
	// slot holding an older change, so the retained set drifts towards the
	// beginning of the stream by exactly the drop rate. Coalescing by slot sheds
	// the same load with no bias, because the change it discards is one that was
	// about to be overwritten anyway.
	pending map[cdcSlot]state.CDCSample
	render  *datumRenderer

	wake   chan struct{}
	stop   chan struct{}
	done   chan struct{}
	closed sync.Once
}

type cdcSlot struct {
	schema, table string
	index         int64
}

type cdcStream struct {
	schema, table string
	// observed is every change seen, and is the denominator for coverage. It is
	// also the reservoir's k: the odds a change is kept are capacity/observed.
	observed int64
	retained int64
	dropped  int64
	dirty    bool
}

func newCDCSampler(ctx context.Context, store *state.Store, capacity int64) (*cdcSampler, error) {
	sampler := &cdcSampler{
		store: store, capacity: capacity,
		streams: make(map[string]*cdcStream),
		pending: make(map[cdcSlot]state.CDCSample),
		render:  newDatumRenderer(),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	// Resuming the counters is not an optimization. An applier that restarted
	// with them at zero would allocate slot 0 again and overwrite a sample of the
	// whole stream with a short prefix of whatever it saw after the restart.
	streams, err := store.LoadCDCSampleStreams(ctx)
	if err != nil {
		return nil, err
	}
	for _, stream := range streams {
		sampler.streams[stream.Schema+"."+stream.Table] = &cdcStream{
			schema: stream.Schema, table: stream.Table,
			observed: stream.Observed,
			retained: min(stream.Retained, capacity),
			dropped:  stream.Dropped,
		}
	}
	go sampler.run()
	return sampler, nil
}

// Observe implements cdc.KeySampler. It runs on the apply loop and never blocks
// it: the reservoir arithmetic is a comparison and a random number, rendering the
// key is a decode and an encode of a couple of columns, and the write itself
// happens elsewhere.
func (s *cdcSampler) Observe(sample cdc.KeySample) {
	if s == nil || s.capacity <= 0 {
		return
	}
	s.mu.Lock()
	name := sample.Schema + "." + sample.Table
	stream, known := s.streams[name]
	if !known {
		stream = &cdcStream{schema: sample.Schema, table: sample.Table}
		s.streams[name] = stream
	}
	stream.observed++
	stream.dirty = true
	slot := int64(-1)
	switch {
	case stream.retained < s.capacity:
		slot = stream.retained
		stream.retained++
	default:
		// Algorithm R: the change replaces a retained one with probability
		// capacity/observed, which keeps every change seen so far equally likely
		// to be held and makes acceptance fall away as the relation gets hotter.
		if candidate := rand.Int64N(stream.observed); candidate < s.capacity {
			slot = candidate
		}
	}
	s.mu.Unlock()
	if slot < 0 {
		return
	}

	key := make(map[string]string, len(sample.Columns))
	for _, column := range sample.Columns {
		// A NULL column is left out rather than failing the sample. Under REPLICA
		// IDENTITY FULL — which pgmigrate itself sets on a table with no primary
		// key — every column is part of the identity, and a nullable one among
		// them is ordinary. Refusing the whole change would empty the reservoir
		// for exactly the tables whose replication is least certain, while the
		// columns the check keys rows on are usually not the NULL ones. A key
		// column that is missing is caught where it can be judged: the check
		// projects its own columns by name and reports the table as unchecked.
		if column.Datum.Kind == cdc.DatumNull {
			continue
		}
		text, ok := s.render.render(column.OID, column.Datum)
		if !ok {
			s.drop(name, slot)
			return
		}
		key[column.Name] = text
	}
	if len(key) == 0 {
		s.drop(name, slot)
		return
	}

	s.mu.Lock()
	if len(s.pending) >= cdcSamplePendingLimit {
		s.mu.Unlock()
		s.drop(name, slot)
		return
	}
	s.pending[cdcSlot{sample.Schema, sample.Table, slot}] = state.CDCSample{
		Schema: sample.Schema, Table: sample.Table, Index: slot,
		Key: key, Kind: sampleKindName(sample.Kind),
		LSN: pglogrepl.LSN(sample.LSN).String(), ObservedAt: time.Now().UTC(),
	}
	full := len(s.pending) >= cdcSampleFlushRows
	s.mu.Unlock()
	if full {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// drop records a sample that was accepted and then could not be kept, and gives
// its slot back when the slot was one the reservoir had just filled.
//
// Handing the slot back matters for a relation whose every key is unrenderable —
// an enum in the primary key, say. Without it, retained would climb to the
// capacity while nothing was ever stored, so the counters would describe a full
// reservoir that a restart would then refuse to refill.
func (s *cdcSampler) drop(name string, slot int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, known := s.streams[name]
	if !known {
		return
	}
	stream.dropped++
	stream.dirty = true
	if slot >= 0 && slot == stream.retained-1 {
		stream.retained--
	}
}

// run writes the buffer out, on a timer and whenever it reaches a batch.
func (s *cdcSampler) run() {
	defer close(s.done)
	ticker := time.NewTicker(cdcSampleFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			s.write()
			return
		case <-s.wake:
			s.write()
		case <-ticker.C:
			s.write()
		}
	}
}

// write persists a batch together with the counters that describe it.
//
// A failure is counted and dropped rather than returned. The reservoir is a
// diagnostic: failing the migration because a sample could not be written would
// trade the thing that matters for the thing that observes it.
func (s *cdcSampler) write() {
	samples, counters := s.snapshot()
	if len(samples) == 0 && len(counters) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 30*time.Second)
	defer cancel()
	if err := s.store.PutCDCSamples(ctx, samples, counters); err != nil {
		// The samples are gone, which costs coverage and nothing else — but it
		// has to be counted, or a store that has stopped accepting writes leaves
		// a reservoir that looks thin for no stated reason. The counters are put
		// back too, because they are what a restart resumes from.
		lost := make(map[string]int64, len(counters))
		for _, sample := range samples {
			lost[sample.Schema+"."+sample.Table]++
		}
		s.mu.Lock()
		for name, count := range lost {
			if stream, known := s.streams[name]; known {
				stream.dropped += count
			}
		}
		for _, counter := range counters {
			if stream, known := s.streams[counter.Schema+"."+counter.Table]; known {
				stream.dirty = true
			}
		}
		s.mu.Unlock()
	}
}

// snapshot takes the buffered samples and the counters that changed since the
// last write, clearing both. An idle relation's counters are not rewritten every
// two seconds.
func (s *cdcSampler) snapshot() ([]state.CDCSample, []state.CDCSampleStream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	samples := make([]state.CDCSample, 0, len(s.pending))
	for _, sample := range s.pending {
		samples = append(samples, sample)
	}
	clear(s.pending)
	var counters []state.CDCSampleStream
	for _, stream := range s.streams {
		if !stream.dirty {
			continue
		}
		stream.dirty = false
		counters = append(counters, state.CDCSampleStream{
			Schema: stream.schema, Table: stream.table,
			Observed: stream.observed, Retained: stream.retained,
			Dropped: stream.dropped,
		})
	}
	return samples, counters
}

// samplerOrNil keeps a disabled reservoir out of the applier as a nil interface
// rather than as an interface holding a nil pointer. The second is not nil, and
// the applier would collect a sample per change to hand to something that
// discards it.
func samplerOrNil(sampler *cdcSampler) cdc.KeySampler {
	if sampler == nil {
		return nil
	}
	return sampler
}

// Close drains what is queued and writes it. It is safe to call more than once.
func (s *cdcSampler) Close() {
	if s == nil {
		return
	}
	s.closed.Do(func() {
		close(s.stop)
		<-s.done
	})
}

// datumRenderer turns replication datums into text a query can bind.
//
// Replication is decoded with binary 'true', so a key arrives as the type's
// binary representation, while the lookup binds key values as text. The rendered
// text only has to be a valid *input* literal for the column's type — it is cast
// back on the way in, never compared as a string — so a canonical form that
// differs from the source's output form is fine and none is reproduced.
//
// It owns its pgtype.Map and a lock, because a Map is not safe for concurrent
// use and sharing one package-wide crashed under a parallel test. The lock is
// uncontended in practice: the applier calls Observe from one goroutine.
type datumRenderer struct {
	mu    sync.Mutex
	types *pgtype.Map
}

func newDatumRenderer() *datumRenderer {
	return &datumRenderer{types: pgtype.NewMap()}
}

// render returns false for anything it cannot turn into a literal the lookup can
// bind, which includes a NULL: there is no text that stands for one. Observe
// decides what that means for the sample, because only it knows whether the rest
// of the identity is still enough to name the row.
func (r *datumRenderer) render(oid uint32, datum cdc.TupleDatum) (string, bool) {
	switch datum.Kind {
	case cdc.DatumText:
		return string(datum.Data), true
	case cdc.DatumBinary:
		return r.renderBinary(oid, datum.Data)
	default:
		return "", false
	}
}

// renderBinary decodes and re-encodes one built-in type. Key columns are
// overwhelmingly integers, text and UUIDs, and a type the map does not know is
// reported rather than guessed at.
func (r *datumRenderer) renderBinary(oid uint32, data []byte) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kind, known := r.types.TypeForOID(oid)
	if !known {
		return "", false
	}
	value, err := kind.Codec.DecodeValue(r.types, oid, pgtype.BinaryFormatCode, data)
	if err != nil {
		return "", false
	}
	text, err := r.types.Encode(oid, pgtype.TextFormatCode, value, nil)
	if err != nil || text == nil {
		return "", false
	}
	return string(text), true
}

// recordedCDCKeys reads one relation's reservoir for verification.
//
// The counters are read even when no keys were retained, because "the applier saw
// four million changes here and kept none" and "nothing has been applied here" are
// different findings and only the counters tell them apart.
func recordedCDCKeys(store *state.Store) verify.CDCKeys {
	return func(ctx context.Context, schema, table string) (verify.CDCRecorded, error) {
		samples, err := store.CDCSamples(ctx, schema, table)
		if err != nil {
			return verify.CDCRecorded{}, err
		}
		recorded := verify.CDCRecorded{Keys: make([]verify.CDCKey, 0, len(samples))}
		for _, sample := range samples {
			recorded.Keys = append(recorded.Keys,
				verify.CDCKey{Key: sample.Key, Kind: sample.Kind})
		}
		observed, dropped, err := store.CDCSampleCounters(ctx, schema, table)
		if err != nil {
			return verify.CDCRecorded{}, err
		}
		recorded.Observed, recorded.Dropped = observed, dropped
		return recorded, nil
	}
}

func sampleKindName(kind cdc.ChangeKind) string {
	switch kind {
	case cdc.ChangeInsert:
		return "insert"
	case cdc.ChangeUpdate:
		return "update"
	case cdc.ChangeDelete:
		return "delete"
	default:
		return fmt.Sprintf("kind_%d", byte(kind))
	}
}
