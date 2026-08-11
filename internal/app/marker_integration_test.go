//go:build integration

package app

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/tgross/pgmigrate/internal/config"
	"github.com/tgross/pgmigrate/internal/pgtest"
	"github.com/tgross/pgmigrate/internal/postgres"
	"github.com/tgross/pgmigrate/internal/state"
)

// TestMarkerReachesDiskOnAnIdleSource is the property the recheck rule depends on
// and the one that is easy to lose.
//
// A recheck reads the source rows, emits a marker, and waits for apply to reach the
// marked position. The walsender only reads WAL up to the flush position, and
// nothing commits a nontransactional message, so a marker that has been written but
// not flushed cannot be decoded. The wait would then depend on the source's write
// traffic: seconds against a lightly loaded source, and forever against an idle
// one. This test's source is idle, which is exactly the case that used to hang.
//
// It runs on every supported major because the mechanism differs: 17 and later
// take a flush argument, and 16 has to be flushed by a following transactional
// message.
func TestMarkerReachesDiskOnAnIdleSource(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprintf("pg%d", major), func(t *testing.T) {
			instance := pgtest.Start(t, major)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			capabilities, err := sourceCapabilities(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			if capabilities.LogicalMessageFlush != (major >= 17) {
				t.Fatalf("PostgreSQL %d reports LogicalMessageFlush=%v",
					major, capabilities.LogicalMessageFlush)
			}
			boundary := newMarker(config.Config{Source: instance.URI, Dir: t.TempDir()},
				"verify:", capabilities)
			defer boundary.close()

			position, err := boundary.emit(ctx, instance.Connect(t))
			if err != nil {
				t.Fatalf("emit marker: %v", err)
			}
			marked, err := pglogrepl.ParseLSN(position)
			if err != nil {
				t.Fatal(err)
			}

			// Nothing else is writing, so a flush position at or past the marker can
			// only have come from the marker itself.
			var flushed string
			if err := instance.Connect(t).QueryRow(ctx,
				"SELECT pg_catalog.pg_current_wal_flush_lsn()::text").Scan(&flushed); err != nil {
				t.Fatal(err)
			}
			reached, err := pglogrepl.ParseLSN(flushed)
			if err != nil {
				t.Fatal(err)
			}
			if reached < marked {
				t.Fatalf("the source has flushed only to %s, so the marker at %s cannot be decoded and a recheck would wait on unrelated write traffic",
					flushed, position)
			}
		})
	}
}

// TestRecheckHooksOnlyExistWhileSomethingIsApplying keeps the rule from waiting on
// a position nothing will ever reach. Outside follow no process is advancing apply,
// so a row that differs differs now, and there is nothing to wait for.
func TestRecheckHooksOnlyExistWhileSomethingIsApplying(t *testing.T) {
	cfg := config.Config{Source: "postgres:///source", Target: "postgres:///target", Dir: t.TempDir()}
	boundary := newMarker(cfg, "verify:", postgres.Capabilities{LogicalMessageFlush: true})
	defer boundary.close()
	for _, phase := range []state.Phase{state.PhasePreflight, state.PhaseCopy, state.PhaseCutover} {
		if mark, wait := recheckHooks(cfg, boundary, "slot", phase); mark != nil || wait != nil {
			t.Errorf("phase %s supplied recheck hooks", phase)
		}
	}
	mark, wait := recheckHooks(cfg, boundary, "slot", state.PhaseFollow)
	if mark == nil || wait == nil {
		t.Fatal("follow did not supply recheck hooks")
	}
}

// TestMarkerRefusesAnUnsupportedRelease keeps the capability gate honest: a
// release nobody probed must not be assumed to behave like the ones that were.
func TestMarkerRefusesAnUnsupportedRelease(t *testing.T) {
	if _, err := postgres.CapabilitiesForMajor(15); err == nil {
		t.Fatal("PostgreSQL 15 was accepted")
	}
}
