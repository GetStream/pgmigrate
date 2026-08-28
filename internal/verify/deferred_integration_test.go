//go:build integration

package verify

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/jackc/pgx/v5"
)

type progressFunc func(Progress)

func (f progressFunc) Update(p Progress) { f(p) }

type auditRecorder struct {
	mu     sync.Mutex
	events []AuditEvent
}

func (a *auditRecorder) record(events []AuditEvent) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, events...)
	return nil
}
func (a *auditRecorder) all() []AuditEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]AuditEvent(nil), a.events...)
}

func TestCDCDeferredOutcomesAndAudit(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`)
	}
	tables := inventoryOf(t, source, "public", "items")
	for _, tc := range []struct {
		name, initial, changeSource, changeTarget, outcome string
		kind                                               DiffKind
	}{
		{name: "target catches up", initial: `UPDATE public.items SET payload='old'`, changeTarget: `UPDATE public.items SET payload='new'`, outcome: "converged"},
		{name: "missing target arrives", initial: `DELETE FROM public.items`, changeTarget: `INSERT INTO public.items VALUES(1,'new')`, outcome: "converged"},
		{name: "unchanged source still different", initial: `UPDATE public.items SET payload='old'`, outcome: "unresolved", kind: DiffDifferent},
		{name: "unchanged source still missing", initial: `DELETE FROM public.items`, outcome: "unresolved", kind: DiffSourceOnly},
		{name: "source and target both advance", initial: `UPDATE public.items SET payload='old'`, changeSource: `UPDATE public.items SET payload='newer'`, changeTarget: `UPDATE public.items SET payload='middle'`, outcome: "advanced"},
		{name: "changed source and matching advanced target", initial: `UPDATE public.items SET payload='old'`, changeSource: `UPDATE public.items SET payload='newer'`, changeTarget: `UPDATE public.items SET payload='newer'`, outcome: "converged"},
		{name: "changed source matching stalled target still fails", initial: `UPDATE public.items SET payload='old'`, changeSource: `UPDATE public.items SET payload='old'`, outcome: "unresolved", kind: DiffTargetStalled},
		{name: "source and target both deleted", initial: `UPDATE public.items SET payload='old'`, changeSource: `DELETE FROM public.items`, changeTarget: `DELETE FROM public.items`, outcome: "advanced"},
		{name: "source update requires advancement", initial: `UPDATE public.items SET payload='old'`, changeSource: `UPDATE public.items SET payload='newer'`, outcome: "unresolved", kind: DiffTargetStalled},
		{name: "source deletion requires advancement", initial: `UPDATE public.items SET payload='old'`, changeSource: `DELETE FROM public.items`, outcome: "unresolved", kind: DiffTargetStalled},
		{name: "source no-op update requires advancement", initial: `UPDATE public.items SET payload='old'`, changeSource: `UPDATE public.items SET payload=payload`, outcome: "unresolved", kind: DiffTargetStalled},
		{name: "source change and revert requires advancement", initial: `UPDATE public.items SET payload='old'`, changeSource: `UPDATE public.items SET payload='newer'; UPDATE public.items SET payload='new'`, outcome: "unresolved", kind: DiffTargetStalled},
		{name: "source reinsert requires advancement", initial: `UPDATE public.items SET payload='old'`, changeSource: `DELETE FROM public.items; INSERT INTO public.items VALUES(1,'new')`, outcome: "unresolved", kind: DiffTargetStalled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, c := range []*pgx.Conn{source, target} {
				exec(t, c, `TRUNCATE public.items`, `INSERT INTO public.items VALUES(1,'new')`)
			}
			var audit auditRecorder
			var pending, rechecking, reads atomic.Int32
			cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: tables, CDCRecheckDelay: 40 * time.Millisecond}
			cfg.CDCKeys = func(ctx context.Context, _, _ string) (CDCRecorded, error) {
				reads.Add(1)
				_, err := target.Exec(ctx, tc.initial)
				// Duplicate reservoir entries must retain just one candidate snapshot.
				return CDCRecorded{Observed: 2, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}, {Key: map[string]string{"id": "1"}}}}, err
			}
			cfg.Audit = func(events []AuditEvent) error {
				audit.record(events)
				if events[0].Outcome == "deferred" {
					if tc.changeSource != "" {
						if _, err := source.Exec(context.Background(), tc.changeSource); err != nil {
							return err
						}
					}
					if tc.changeTarget != "" {
						if _, err := target.Exec(context.Background(), tc.changeTarget); err != nil {
							return err
						}
					}
				}
				return nil
			}
			cfg.Progress = progressFunc(func(p Progress) {
				if p.Stage == StageCDCDeferred {
					pending.Add(1)
					if p.Complete || p.Converged || p.Unresolved != 0 || p.CDCPending != 1 {
						t.Errorf("premature result: %+v", p)
					}
				}
				if p.Stage == StageCDCRechecking {
					rechecking.Add(1)
				}
			})
			cfg.Boundary = func(context.Context, *pgx.Conn) (string, error) {
				return "", errors.New("CDC must defer rather than wait on a WAL marker")
			}
			cfg.WaitApplied = func(context.Context, string) error { return errors.New("unexpected WAL wait") }
			got, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			table := got.Tables[0]
			divergent := tc.outcome == "unresolved"
			if !got.Complete || got.Converged == divergent || table.CDC.Candidates != 1 || table.CDC.Pending != 0 {
				t.Fatalf("result: %+v", got)
			}
			if reads.Load() != 1 || pending.Load() != 1 || rechecking.Load() != 1 {
				t.Fatalf("reads=%d pending=%d rechecking=%d", reads.Load(), pending.Load(), rechecking.Load())
			}
			if divergent && (len(table.Unresolved) != 1 || table.Unresolved[0].Kind != tc.kind) {
				t.Fatalf("lost mismatch: %+v", table)
			}
			wantMatched, wantChanged, wantAdvanced := 0, 0, 0
			if tc.outcome == "converged" {
				wantMatched = 1
			}
			if tc.changeSource != "" {
				wantChanged = 1
			}
			if tc.outcome == "advanced" {
				wantAdvanced = 1
			}
			if table.CDC.InFlight != wantMatched || table.CDC.SourceChanged != wantChanged || table.CDC.Advanced != wantAdvanced {
				t.Fatalf("incorrect matched/changed/advanced counters: %+v", table.CDC)
			}

			events := audit.all()
			if len(events) != 2 || events[0].Outcome != "deferred" || events[1].Outcome != tc.outcome {
				t.Fatalf("audit trail: %+v", events)
			}
			first, last := events[0], events[1]
			if last.SourceChanged != (tc.changeSource != "") {
				t.Fatalf("source-change audit missing: %+v", last)
			}
			if last.Time.Sub(first.Time) < cfg.CDCRecheckDelay {
				t.Fatalf("rechecked too early: %s", last.Time.Sub(first.Time))
			}
			if first.Table != "public.items" || first.Key[0] != "1" || !first.Source.Present || first.Source.Version == "" || first.Source.Hash == "" || first.Target == nil {
				t.Fatalf("missing original snapshots: %+v", first)
			}
			if last.OriginalSource.Hash != first.Source.Hash || last.OriginalSource.Version != first.Source.Version || last.Source == nil || last.SourceAfter == nil || last.Target == nil {
				t.Fatalf("missing recheck snapshots: %+v", last)
			}
			if tc.changeSource != "" && last.Source.Present && last.Source.Version == first.Source.Version {
				t.Fatal("source touch not captured by xmin")
			}
		})
	}
}

func TestCDCRechecksWhileOtherTablesAreStillRunning(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.early(id bigint PRIMARY KEY,payload text)`, `CREATE TABLE public.later(id bigint PRIMARY KEY,payload text)`)
	}
	tables := append(inventoryOf(t, source, "public", "early"), inventoryOf(t, source, "public", "later")...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rechecked := make(chan struct{})
	var pending atomic.Bool
	cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: tables, Workers: 1, CDCRecheckDelay: 100 * time.Millisecond}
	cfg.Progress = progressFunc(func(p Progress) {
		if p.Stage == StageCDCDeferred {
			pending.Store(true)
		}
	})
	cfg.CDCKeys = func(ctx context.Context, _, name string) (CDCRecorded, error) {
		if name == "early" {
			_, err := source.Exec(ctx, `INSERT INTO public.early VALUES(1,'new')`)
			return CDCRecorded{Observed: 1, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}}}, err
		}
		if !pending.Load() {
			return CDCRecorded{}, errors.New("initial mismatch did not become pending")
		}
		select {
		case <-rechecked:
			return CDCRecorded{}, nil
		case <-ctx.Done():
			return CDCRecorded{}, ctx.Err()
		}
	}
	cfg.Audit = func(events []AuditEvent) error {
		if events[0].Outcome == "deferred" {
			_, err := target.Exec(ctx, `INSERT INTO public.early VALUES(1,'new')`)
			return err
		}
		if events[0].Outcome == "converged" {
			close(rechecked)
		}
		return nil
	}
	got, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Complete || !got.Converged || got.Tables[0].CDC.InFlight != 1 {
		t.Fatalf("result: %+v", got)
	}
}

func TestCDCDeferredCancellationTimeoutAndAuditFailure(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`)
	}
	tables := inventoryOf(t, source, "public", "items")
	for _, mode := range []string{"cancel pending", "cancel recheck", "timeout", "delay excludes timeout", "initial audit failure", "recheck audit failure"} {
		t.Run(mode, func(t *testing.T) {
			for _, c := range []*pgx.Conn{source, target} {
				exec(t, c, `TRUNCATE public.items`, `INSERT INTO public.items VALUES(1,'new')`)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var audit auditRecorder
			auditErr := errors.New("audit disk unavailable")
			var connects atomic.Int32
			cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: tables, CDCRecheckDelay: 20 * time.Millisecond}
			cfg.CDCKeys = func(ctx context.Context, _, _ string) (CDCRecorded, error) {
				_, err := target.Exec(ctx, `UPDATE public.items SET payload='old'`)
				return CDCRecorded{Observed: 1, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}}}, err
			}
			cfg.Progress = progressFunc(func(p Progress) {
				if mode == "cancel pending" && p.Stage == StageCDCDeferred || mode == "cancel recheck" && p.Stage == StageCDCRechecking {
					cancel()
				}
			})
			cfg.Audit = func(events []AuditEvent) error {
				if mode == "initial audit failure" && events[0].Outcome == "deferred" || mode == "recheck audit failure" && events[0].Outcome == "unresolved" {
					return auditErr
				}
				return audit.record(events)
			}
			if mode == "timeout" || mode == "delay excludes timeout" {
				cfg.TableTimeout = 300 * time.Millisecond
				cfg.CDCRecheckDelay = 400 * time.Millisecond
				if mode == "timeout" {
					cfg.Source = func(ctx context.Context) (*pgx.Conn, error) {
						if connects.Add(1) == 2 {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						return connector(s.URI)(ctx)
					}
				}
			}
			got, err := Run(ctx, cfg)
			switch mode {
			case "timeout":
				if err != nil || got.Complete || got.Converged || got.Tables[0].CDC.Pending != 1 || got.Tables[0].CutShort != "table timeout reached" {
					t.Fatalf("timeout result=%+v error=%v", got, err)
				}
			case "delay excludes timeout":
				if err != nil || !got.Complete || got.Converged || len(got.Tables[0].Unresolved) != 1 {
					t.Fatalf("delay consumed active budget: %+v error=%v", got, err)
				}
			case "initial audit failure", "recheck audit failure":
				if !errors.Is(err, auditErr) {
					t.Fatalf("lost audit error: %v", err)
				}
			default:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel error: %v", err)
				}
			}
			events := audit.all()
			if mode == "initial audit failure" {
				return
			}
			want := "incomplete"
			if mode == "delay excludes timeout" {
				want = "unresolved"
			}
			if len(events) != 2 || events[0].Outcome != "deferred" || events[1].Outcome != want || events[1].Key[0] != "1" {
				t.Fatalf("lost pending audit: %+v", events)
			}
		})
	}
}

func TestCDCDeferredChecksEveryCandidateBeyondHeapThreshold(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`)
	}
	var audit auditRecorder
	cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: inventoryOf(t, source, "public", "items"), RowThreshold: 1, CDCRecheckDelay: time.Millisecond, Audit: audit.record}
	cfg.CDCKeys = func(ctx context.Context, _, _ string) (CDCRecorded, error) {
		_, err := source.Exec(ctx, `INSERT INTO public.items VALUES(1,'new'),(2,'new')`)
		return CDCRecorded{Observed: 2, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}, {Key: map[string]string{"id": "2"}}}}, err
	}
	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Complete || got.Converged || len(got.Tables[0].Unresolved) != 2 {
		t.Fatalf("candidate dropped: %+v", got)
	}
	events := audit.all()
	if len(events) != 4 {
		t.Fatalf("missing audit: %+v", events)
	}
	for _, e := range events {
		if e.Kind != DiffSourceOnly || e.Target.Present {
			t.Fatalf("missing row audit: %+v", e)
		}
	}
}

func TestHeapAuditsAllInitialMismatchesBeyondRecheckThreshold(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`)
	}
	exec(t, source, `INSERT INTO public.items VALUES(1,'new'),(2,'new'),(3,'new')`)
	var audit auditRecorder
	cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: inventoryOf(t, source, "public", "items"), RowThreshold: 1, Audit: audit.record}
	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got.Converged {
		t.Fatal("missing target rows passed")
	}
	events := audit.all()
	if len(events) != 4 {
		t.Fatalf("wanted 3 initial observations and 1 recheck: %+v", events)
	}
	for _, e := range events[:3] {
		if e.Outcome != "observed" || e.Stratum != "heap" || !e.Source.Present || e.Target.Present {
			t.Fatalf("bad heap audit: %+v", e)
		}
	}
	cfg.Audit = func([]AuditEvent) error { return errors.New("audit unavailable") }
	if _, err := Run(context.Background(), cfg); err == nil {
		t.Fatal("heap audit failure silently ignored")
	}
}

type beforeQuery struct{ start func(string) }

func (q beforeQuery) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	q.start(data.SQL)
	return ctx
}
func (q beforeQuery) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestCDCSourceTouchDuringTargetReadRequiresTargetAdvancement(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`, `INSERT INTO public.items VALUES(1,'new')`)
	}
	var connects atomic.Int32
	var touched atomic.Bool
	var audit auditRecorder
	cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: inventoryOf(t, source, "public", "items"), CDCRecheckDelay: time.Millisecond, Audit: audit.record}
	cfg.CDCKeys = func(ctx context.Context, _, _ string) (CDCRecorded, error) {
		_, err := target.Exec(ctx, `UPDATE public.items SET payload='old'`)
		return CDCRecorded{Observed: 1, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}}}, err
	}
	cfg.Target = func(ctx context.Context) (*pgx.Conn, error) {
		config, err := pgx.ParseConfig(d.URI)
		if err != nil {
			return nil, err
		}
		if connects.Add(1) == 2 {
			config.Tracer = beforeQuery{start: func(query string) {
				if strings.Contains(query, "FROM (VALUES") {
					if _, err := source.Exec(ctx, `UPDATE public.items SET payload=payload`); err != nil {
						t.Error(err)
					}
					touched.Store(true)
				}
			}}
		}
		return pgx.ConnectConfig(ctx, config)
	}
	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !touched.Load() || got.Converged || got.Tables[0].CDC.SourceChanged != 1 || got.Tables[0].CDC.InFlight != 0 {
		t.Fatalf("source touch incorrectly cleared a stalled target: %+v", got)
	}
	events := audit.all()
	if len(events) != 2 {
		t.Fatalf("audit: %+v", events)
	}
	last := events[1]
	if last.Outcome != "unresolved" || last.Kind != DiffTargetStalled || !last.SourceChanged || last.Source.Version != last.OriginalSource.Version || last.SourceAfter.Version == last.OriginalSource.Version {
		t.Fatalf("target read not bracketed by source snapshots: %+v", last)
	}
}

func TestCDCTargetAdvancementAcceptedAfterOneDeferredCheck(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`)
	}
	tables := inventoryOf(t, source, "public", "items")
	for _, mode := range []string{"target contents advanced", "target no-op write", "missing target appeared", "target stalled", "target disappeared"} {
		t.Run(mode, func(t *testing.T) {
			for _, c := range []*pgx.Conn{source, target} {
				exec(t, c, `TRUNCATE public.items`, `INSERT INTO public.items VALUES(1,'new')`)
			}
			var audit auditRecorder
			var rechecks atomic.Int32
			cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: tables, CDCRecheckDelay: 30 * time.Millisecond}
			cfg.CDCKeys = func(ctx context.Context, _, _ string) (CDCRecorded, error) {
				sql := `UPDATE public.items SET payload='old'`
				if mode == "missing target appeared" {
					sql = `DELETE FROM public.items`
				}
				_, err := target.Exec(ctx, sql)
				return CDCRecorded{Observed: 1, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}}}, err
			}
			cfg.Audit = func(events []AuditEvent) error {
				audit.record(events)
				if events[0].Outcome != "deferred" {
					return nil
				}
				sql := `UPDATE public.items SET payload='middle'`
				switch mode {
				case "target no-op write":
					sql = `UPDATE public.items SET payload=payload`
				case "missing target appeared":
					sql = `INSERT INTO public.items VALUES(1,'middle')`
				case "target disappeared":
					sql = `DELETE FROM public.items`
				case "target stalled":
					return nil
				}
				_, err := target.Exec(context.Background(), sql)
				return err
			}
			cfg.Progress = progressFunc(func(p Progress) {
				if p.Stage == StageCDCRechecking {
					rechecks.Add(1)
				}
			})
			got, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			table := got.Tables[0]
			accepted := mode != "target stalled" && mode != "target disappeared"
			if !got.Complete || got.Converged != accepted || table.CDC.Pending != 0 || table.CDC.InFlight != 0 || rechecks.Load() != 1 {
				t.Fatalf("expected exactly one deferred decision, not a retry loop: %+v rechecks=%d", got, rechecks.Load())
			}
			want := "unresolved"
			if accepted {
				want = "advanced"
				if table.CDC.Advanced != 1 || len(table.Unresolved) != 0 || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "accepted as progressing") {
					t.Fatalf("progress not accepted separately from equality: %+v", got)
				}
			} else if table.CDC.Advanced != 0 || len(table.Unresolved) != 1 {
				t.Fatalf("stalled target accepted: %+v", got)
			}
			events := audit.all()
			if len(events) != 2 || events[0].Outcome != "deferred" || events[1].Outcome != want {
				t.Fatalf("unexpected retries or missing audit: %+v", events)
			}
			first, last := events[0], events[1]
			if last.Time.Sub(first.Time) < cfg.CDCRecheckDelay || *last.PreviousTarget != *first.Target || *last.OriginalSource != *first.Source {
				t.Fatalf("snapshot comparison incorrect: %+v", events)
			}
			if accepted && (!last.Target.Present || last.Target.Hash == last.Source.Hash) {
				t.Fatal("test requires advancement without equality")
			}
		})
	}
}

func TestCDCMixedOutcomesRetainStalledRowsWhileOthersAdvance(t *testing.T) {
	s, d := pgtest.Start(t, 17), pgtest.Start(t, 17)
	source, target := s.Connect(t), d.Connect(t)
	for _, c := range []*pgx.Conn{source, target} {
		exec(t, c, `CREATE TABLE public.items(id bigint PRIMARY KEY,payload text)`)
	}
	var audit auditRecorder
	var rechecks atomic.Int32
	cfg := Config{Source: connector(s.URI), Target: connector(d.URI), Tables: inventoryOf(t, source, "public", "items"), CDCRecheckDelay: 30 * time.Millisecond}
	cfg.CDCKeys = func(ctx context.Context, _, _ string) (CDCRecorded, error) {
		_, err := source.Exec(ctx, `INSERT INTO public.items VALUES(1,'new'),(2,'new'),(3,'new')`)
		return CDCRecorded{Observed: 3, Keys: []CDCKey{{Key: map[string]string{"id": "1"}}, {Key: map[string]string{"id": "2"}}, {Key: map[string]string{"id": "3"}}}}, err
	}
	cfg.Audit = func(events []AuditEvent) error {
		audit.record(events)
		sql := ""
		if events[0].Outcome == "deferred" {
			sql = `INSERT INTO public.items VALUES(1,'middle'),(3,'new')`
		}
		if events[0].Outcome == "advanced" {
			sql = `UPDATE public.items SET payload='new' WHERE id=1`
		}
		if sql != "" {
			_, err := target.Exec(context.Background(), sql)
			return err
		}
		return nil
	}
	cfg.Progress = progressFunc(func(p Progress) {
		if p.Stage == StageCDCRechecking {
			rechecks.Add(1)
		}
	})
	got, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	table := got.Tables[0]
	if !got.Complete || got.Converged || table.CDC.Pending != 0 || table.CDC.InFlight != 1 || table.CDC.Advanced != 1 || len(table.Unresolved) != 1 || table.Unresolved[0].Key[0] != "2" || rechecks.Load() != 1 {
		t.Fatalf("mixed outcomes lost: %+v", got)
	}
	if events := audit.all(); len(events) != 6 {
		t.Fatalf("missing mixed audit outcomes: %+v", events)
	}
}
