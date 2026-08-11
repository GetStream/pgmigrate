package app

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/tgross/pgmigrate/internal/cdc"
	"github.com/tgross/pgmigrate/internal/state"
)

func openSamplerStore(t *testing.T, dir string) *state.Store {
	t.Helper()
	store, err := state.Open(context.Background(), dir,
		state.Fingerprints{Source: "source", Filter: "filter"})
	if err != nil {
		t.Fatalf("state.Open() error = %v", err)
	}
	return store
}

// observeText feeds the sampler one change whose key is the change's ordinal, so
// a test can tell which part of the stream a retained sample came from.
func observeText(sampler *cdcSampler, ordinal int) {
	sampler.Observe(cdc.KeySample{
		Schema: "s", Table: "a", Kind: cdc.ChangeInsert,
		Columns: []cdc.SampleColumn{{
			Name: "id", OID: 25,
			Datum: cdc.TupleDatum{Kind: cdc.DatumText, Data: []byte(strconv.Itoa(ordinal))},
		}},
	})
}

func TestReservoirNeverExceedsItsCapacity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openSamplerStore(t, dir)
	sampler, err := newCDCSampler(ctx, store, 100)
	if err != nil {
		t.Fatalf("newCDCSampler() error = %v", err)
	}
	for i := range 10_000 {
		observeText(sampler, i)
	}
	sampler.Close()

	samples, err := store.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(samples) != 100 {
		t.Fatalf("the reservoir holds %d rows, want exactly its capacity of 100", len(samples))
	}
	observed, _, err := store.CDCSampleCounters(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSampleCounters() error = %v", err)
	}
	if observed != 10_000 {
		t.Errorf("observed = %d, want 10000: it is the denominator for coverage, not a count of what was kept", observed)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestReservoirSamplesTheWholeStream is the property the whole design rests on.
// Keeping the most recent N would never look at the catch-up burst, which is
// where the spill and out-of-order paths run and so where a defect is most
// likely; keeping the first N would never look at anything else. Either failure
// shows up here as retained keys clustered in one decile of arrival order.
func TestReservoirSamplesTheWholeStream(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openSamplerStore(t, dir)
	sampler, err := newCDCSampler(ctx, store, 1000)
	if err != nil {
		t.Fatalf("newCDCSampler() error = %v", err)
	}
	const changes = 100_000
	for i := range changes {
		observeText(sampler, i)
	}
	sampler.Close()

	samples, err := store.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(samples) != 1000 {
		t.Fatalf("the reservoir holds %d rows, want 1000", len(samples))
	}
	var deciles [10]int
	for _, sample := range samples {
		ordinal, err := strconv.Atoi(sample.Key["id"])
		if err != nil {
			t.Fatalf("retained key %q is not an ordinal: %v", sample.Key["id"], err)
		}
		deciles[ordinal*10/changes]++
	}
	// Each decile expects 100 of the 1000 retained keys. The bounds are wide
	// enough that the test does not flake on the sampling itself and narrow
	// enough that a prefix or a suffix cannot pass: either would leave eight of
	// the ten deciles empty.
	for decile, count := range deciles {
		if count < 40 || count > 180 {
			t.Errorf("decile %d holds %d of the 1000 retained keys, want roughly 100: %v",
				decile, count, deciles)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestReservoirResumesAfterARestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openSamplerStore(t, dir)
	sampler, err := newCDCSampler(ctx, store, 100)
	if err != nil {
		t.Fatalf("newCDCSampler() error = %v", err)
	}
	for i := range 500 {
		observeText(sampler, i)
	}
	sampler.Close()
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := openSamplerStore(t, dir)
	resumed, err := newCDCSampler(ctx, reopened, 100)
	if err != nil {
		t.Fatalf("newCDCSampler() after restart error = %v", err)
	}
	for i := 500; i < 1000; i++ {
		observeText(resumed, i)
	}
	resumed.Close()

	observed, _, err := reopened.CDCSampleCounters(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSampleCounters() error = %v", err)
	}
	if observed != 1000 {
		t.Errorf("observed = %d, want 1000: a restart that forgot the counter would refill the reservoir from slot 0", observed)
	}
	samples, err := reopened.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(samples) != 100 {
		t.Errorf("the reservoir holds %d rows after a restart, want 100", len(samples))
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestObserveDoesNotBlockTheApplyLoop is the trade the queue exists to make.
// Apply latency is worth more than a sample, so a reservoir that cannot keep up
// drops rather than waits.
func TestObserveDoesNotBlockTheApplyLoop(t *testing.T) {
	store := openSamplerStore(t, t.TempDir())
	// Built by hand, with no writer goroutine and a buffer already at its safety
	// valve, so every sample is refused for the length of the test.
	sampler := &cdcSampler{
		store: store, capacity: 1_000_000,
		streams: map[string]*cdcStream{},
		pending: make(map[cdcSlot]state.CDCSample, cdcSamplePendingLimit),
		render:  newDatumRenderer(),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	for i := range cdcSamplePendingLimit {
		sampler.pending[cdcSlot{"filler", "filler", int64(i)}] = state.CDCSample{}
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for i := range 10_000 {
			observeText(sampler, i)
		}
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Observe() blocked once the queue filled, which would stall the apply loop")
	}
	if sampler.streams["s.a"].dropped == 0 {
		t.Error("no drops were counted, so a reservoir that fell behind would look complete")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestReservoirKeepsAFullIdentityRowWithNullColumns covers the table shape this
// is most needed on. pgmigrate sets REPLICA IDENTITY FULL on a table with no
// primary key, so every column becomes part of the recorded identity and a
// nullable one among them is ordinary. Failing the sample over it would leave the
// reservoir empty for the tables whose replication is least certain, while the
// column the check keys rows on is right there.
func TestReservoirKeepsAFullIdentityRowWithNullColumns(t *testing.T) {
	ctx := context.Background()
	store := openSamplerStore(t, t.TempDir())
	sampler, err := newCDCSampler(ctx, store, 10)
	if err != nil {
		t.Fatalf("newCDCSampler() error = %v", err)
	}
	sampler.Observe(cdc.KeySample{
		Schema: "s", Table: "a", Kind: cdc.ChangeUpdate,
		Columns: []cdc.SampleColumn{
			{Name: "id", OID: 25, Datum: cdc.TupleDatum{Kind: cdc.DatumText, Data: []byte("7")}},
			{Name: "optional", OID: 25, Datum: cdc.TupleDatum{Kind: cdc.DatumNull}},
		},
	})
	sampler.Close()

	samples, err := store.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("the reservoir holds %d rows, want the change kept for its id", len(samples))
	}
	if samples[0].Key["id"] != "7" {
		t.Errorf("recorded key = %v, want id 7", samples[0].Key)
	}
	if _, present := samples[0].Key["optional"]; present {
		t.Errorf("recorded key = %v, want the NULL column left out rather than named as a value", samples[0].Key)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestReservoirGivesBackTheSlotOfAKeyItCannotName is the case of a relation whose
// key type nothing can render — an enum in the primary key, say. The reservoir has
// to stay refillable: a slot counted as filled and never written would leave the
// counters describing a full reservoir holding nothing, and a restart resuming
// from them would never write to it again.
func TestReservoirGivesBackTheSlotOfAKeyItCannotName(t *testing.T) {
	ctx := context.Background()
	store := openSamplerStore(t, t.TempDir())
	sampler, err := newCDCSampler(ctx, store, 4)
	if err != nil {
		t.Fatalf("newCDCSampler() error = %v", err)
	}
	for range 20 {
		sampler.Observe(cdc.KeySample{
			Schema: "s", Table: "a", Kind: cdc.ChangeInsert,
			Columns: []cdc.SampleColumn{{
				Name: "mood", OID: 1_000_000,
				Datum: cdc.TupleDatum{Kind: cdc.DatumBinary, Data: []byte{1}},
			}},
		})
	}
	stream := sampler.streams["s.a"]
	if stream.retained != 0 {
		t.Errorf("retained = %d after 20 unrenderable keys, want 0: the reservoir holds none of them", stream.retained)
	}
	if stream.dropped != 20 {
		t.Errorf("dropped = %d, want 20: a relation contributing nothing has to say so", stream.dropped)
	}
	// The relation is still sampled the moment a key can be named, which a
	// reservoir that had counted itself full would not be.
	observeText(sampler, 1)
	sampler.Close()
	samples, err := store.CDCSamples(ctx, "s", "a")
	if err != nil {
		t.Fatalf("CDCSamples() error = %v", err)
	}
	if len(samples) != 1 {
		t.Errorf("the reservoir holds %d rows, want the 1 key it could name", len(samples))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestRenderBinaryDatums covers the format mismatch between the two halves of
// this feature: replication is decoded with binary 'true', and the lookup binds
// key values as text. A type that cannot be rendered has to say so rather than
// produce a key that matches nothing on either side.
func TestRenderBinaryDatums(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		oid   uint32
		value any
		want  string
	}{
		{"int4", 23, int32(1397531), "1397531"},
		{"int8", 20, int64(9007199254740993), "9007199254740993"},
		{"text", 25, "hello", "hello"},
		// A v4 UUID is the common primary key on the tables this was built for,
		// and it is the type whose binary form looks least like its text form:
		// sixteen bytes against a hyphenated hex string.
		{
			"uuid", 2950,
			[16]byte{
				0x8f, 0xa9, 0x08, 0x64, 0x11, 0x22, 0x43, 0x55,
				0x86, 0x77, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
			},
			"8fa90864-1122-4355-8677-99aabbccddee",
		},
		{
			"timestamptz", 1184,
			time.Date(2026, 8, 11, 6, 30, 0, 0, time.UTC),
			"2026-08-11 06:30:00Z",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			renderer := newDatumRenderer()
			encoded, err := renderer.types.Encode(testCase.oid, 1, testCase.value, nil)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			got, ok := renderer.render(testCase.oid, cdc.TupleDatum{
				Kind: cdc.DatumBinary, Data: encoded,
			})
			if !ok {
				t.Fatalf("renderDatum() refused a %s", testCase.name)
			}
			if got != testCase.want {
				t.Errorf("renderDatum() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestRenderDatumRefusesWhatItCannotName(t *testing.T) {
	t.Parallel()
	renderer := newDatumRenderer()
	if _, ok := renderer.render(25, cdc.TupleDatum{Kind: cdc.DatumNull}); ok {
		t.Error("renderDatum() accepted a NULL key, which would match nothing on either side")
	}
	if _, ok := renderer.render(1_000_000, cdc.TupleDatum{
		Kind: cdc.DatumBinary, Data: []byte{1},
	}); ok {
		t.Error("renderDatum() guessed at a type it does not know")
	}
	if got, ok := renderer.render(25, cdc.TupleDatum{
		Kind: cdc.DatumText, Data: []byte("already text"),
	}); !ok || got != "already text" {
		t.Errorf("renderDatum() = %q,%v for a text datum, want it used as-is", got, ok)
	}
}

func TestSampleKindNames(t *testing.T) {
	t.Parallel()
	for kind, want := range map[cdc.ChangeKind]string{
		cdc.ChangeInsert: "insert",
		cdc.ChangeUpdate: "update",
		cdc.ChangeDelete: "delete",
	} {
		if got := sampleKindName(kind); got != want {
			t.Errorf("sampleKindName(%v) = %q, want %q", kind, got, want)
		}
	}
}

func TestSamplerOrNilKeepsADisabledReservoirOutOfTheApplier(t *testing.T) {
	t.Parallel()
	if samplerOrNil(nil) != nil {
		t.Fatal("samplerOrNil(nil) is not nil, so the applier would collect samples for something that discards them")
	}
}
