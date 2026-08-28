//go:build integration

package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/pgtest"
	"github.com/GetStream/pgmigrate/internal/setup"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresDiagnostics(t *testing.T) {
	for _, major := range pgtest.Majors(t) {
		t.Run(fmt.Sprint(major), func(t *testing.T) {
			instance := pgtest.Start(t, major)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			admin := instance.Connect(t)
			var systemID, database, lsn string
			if err := admin.QueryRow(ctx, `SELECT system_identifier::text, current_database(), pg_current_wal_lsn()::text FROM pg_control_system()`).Scan(&systemID, &database, &lsn); err != nil {
				t.Fatal(err)
			}
			cfg := config.Config{Source: instance.URI, Target: instance.URI, Dir: t.TempDir()}
			writer, err := state.Open(ctx, cfg.Dir, state.Fingerprints{Source: setup.SourceFingerprint(systemID, database), Filter: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if err := writer.UpdateApplyProgress(ctx, state.ApplyProgress{StagedLSN: lsn, AppliedLSN: "0/1", UpdatedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			server := newTestServer(t, cfg, "test-token", noOpActions())
			response := request(t, server, http.MethodGet, "/api/diagnostics", "", "test-token")
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", response.Code, response.Body.String())
			}
			var view diagnosticsView
			decode(t, response, &view)
			if view.WAL.Error != "" || view.WAL.TotalBytes == nil || view.Source.Error != "" || view.Target.Error != "" {
				t.Fatalf("diagnostics = %+v", view)
			}
			var total, captured string
			if err := admin.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)::text, pg_wal_lsn_diff($1::pg_lsn,$3::pg_lsn)::text`, view.WAL.SourceLSN, view.WAL.AppliedLSN, view.WAL.StagedLSN).Scan(&total, &captured); err != nil {
				t.Fatal(err)
			}
			if *view.WAL.TotalBytes != total || *view.WAL.UncapturedBytes != captured {
				t.Fatalf("Go gaps disagree with PostgreSQL: %+v, SQL %s/%s", view.WAL, total, captured)
			}
			if view.WAL.SourceAt == nil || view.WAL.CheckpointAt == nil {
				t.Fatal("sample timestamps are missing")
			}

			t.Run("read-only and timeout", func(t *testing.T) {
				conn, err := diagnosticsConnect(ctx, instance.URI)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close(context.Background())
				_, err = conn.Exec(ctx, "CREATE TABLE must_not_be_created(id int)")
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
					t.Fatalf("write error = %v, want read-only transaction", err)
				}
				started := time.Now()
				_, err = conn.Exec(ctx, "SELECT pg_sleep(5)")
				if !errors.As(err, &pgErr) || pgErr.Code != "57014" || time.Since(started) > 3*time.Second {
					t.Fatalf("unbounded diagnostics query: elapsed=%s err=%v", time.Since(started), err)
				}
			})

			t.Run("wrong source identity", func(t *testing.T) {
				wrong := cfg
				wrong.Dir = t.TempDir()
				store, err := state.Open(ctx, wrong.Dir, state.Fingerprints{Source: "another-cluster", Filter: "test"})
				if err != nil {
					t.Fatal(err)
				}
				if err := store.UpdateApplyProgress(ctx, state.ApplyProgress{StagedLSN: lsn, AppliedLSN: "0/1"}); err != nil {
					t.Fatal(err)
				}
				store.Close()
				got := collectDiagnostics(ctx, wrong)
				if got.WAL.TotalBytes != nil || !strings.Contains(got.WAL.Error, "source identity") {
					t.Fatalf("unrelated source was compared: %+v", got.WAL)
				}
			})

			if _, err := admin.Exec(ctx, `CREATE TABLE public.progress_fixture(id int PRIMARY KEY, payload text) WITH (autovacuum_enabled=false);
				INSERT INTO public.progress_fixture SELECT i, repeat(md5(i::text),16) FROM generate_series(1,5000) i`); err != nil {
				t.Fatal(err)
			}
			monitor, err := diagnosticsConnect(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer monitor.Close(context.Background())
			t.Run("concurrent index and restricted observer", func(t *testing.T) {
				blocker, err := admin.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(context.Background())
				if _, err := blocker.Exec(ctx, "INSERT INTO public.progress_fixture VALUES (6000,'blocker')"); err != nil {
					t.Fatal(err)
				}
				builder := instance.Connect(t)
				buildCtx, stopBuild := context.WithCancel(ctx)
				defer stopBuild()
				finished := make(chan error, 1)
				go func() {
					_, err := builder.Exec(buildCtx, "CREATE INDEX CONCURRENTLY progress_payload_idx ON public.progress_fixture(payload)")
					finished <- err
				}()
				job := waitMaintenance(t, ctx, monitor, func(job maintenanceJob) bool {
					return job.Kind == "index" && strings.HasPrefix(job.Phase, "waiting for")
				})
				if job.Table != "public.progress_fixture" || job.Command != "CREATE INDEX CONCURRENTLY" || job.Unit != "lockers" || job.LockerPID == 0 {
					t.Fatalf("index progress = %+v", job)
				}
				otherCfg, err := pgx.ParseConfig(instance.URI)
				if err != nil {
					t.Fatal(err)
				}
				otherCfg.Database = "postgres"
				other, err := pgx.ConnectConfig(ctx, otherCfg)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close(context.Background())
				if got := readMaintenance(ctx, other); got.Error != "" || len(got.Jobs) != 0 || got.Restricted {
					t.Fatalf("another database's job leaked into results: %+v", got)
				}
				roleAdmin := instance.Connect(t)
				if _, err := roleAdmin.Exec(ctx, "CREATE ROLE progress_observer LOGIN PASSWORD 'test'"); err != nil {
					t.Fatal(err)
				}
				observerCfg, err := pgx.ParseConfig(instance.URI)
				if err != nil {
					t.Fatal(err)
				}
				observerCfg.User, observerCfg.Password = "progress_observer", "test"
				observer, err := pgx.ConnectConfig(ctx, observerCfg)
				if err != nil {
					t.Fatal(err)
				}
				defer observer.Close(context.Background())
				restricted := readMaintenance(ctx, observer)
				if restricted.Error != "" || !restricted.Restricted || len(restricted.Jobs) != 0 {
					t.Fatalf("restricted progress = %+v", restricted)
				}
				if _, err := roleAdmin.Exec(ctx, "GRANT pg_read_all_stats TO progress_observer"); err != nil {
					t.Fatal(err)
				}
				visible := readMaintenance(ctx, observer)
				if visible.Restricted || len(visible.Jobs) == 0 {
					t.Fatalf("granted progress = %+v", visible)
				}
				if err := blocker.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-finished:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if after := readMaintenance(ctx, monitor); after.Error != "" || len(after.Jobs) != 0 {
					t.Fatalf("completed index remains active: %+v", after)
				}
				var ready, valid bool
				if err := monitor.QueryRow(ctx, "SELECT indisready, indisvalid FROM pg_index WHERE indexrelid='public.progress_payload_idx'::regclass").Scan(&ready, &valid); err != nil || !ready || !valid {
					t.Fatalf("index completion flags = %v/%v, %v", ready, valid, err)
				}
			})

			t.Run("live vacuum", func(t *testing.T) {
				worker := instance.Connect(t)
				if _, err := worker.Exec(ctx, "SET vacuum_cost_delay='20ms'; SET vacuum_cost_limit=1"); err != nil {
					t.Fatal(err)
				}
				vacuumCtx, stopVacuum := context.WithCancel(ctx)
				finished := make(chan error, 1)
				defer func() { stopVacuum(); <-finished }()
				go func() { _, err := worker.Exec(vacuumCtx, "VACUUM public.progress_fixture"); finished <- err }()
				job := waitMaintenance(t, ctx, monitor, func(job maintenanceJob) bool {
					return job.Kind == "vacuum" && job.Phase == "scanning heap" && job.Total > 0
				})
				if job.Table != "public.progress_fixture" || job.Percent == nil || job.Unit != "heap blocks scanned" {
					t.Fatalf("vacuum progress = %+v", job)
				}
			})
			t.Run("vacuum full index rebuild", func(t *testing.T) {
				// Hold the rebuild inside a test-only index expression so this
				// otherwise brief phase can be observed deterministically.
				if _, err := admin.Exec(ctx, `CREATE FUNCTION public.progress_gate(i int) RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$
					BEGIN PERFORM pg_advisory_xact_lock(987654); RETURN i; END$$;
					CREATE INDEX progress_gate_idx ON public.progress_fixture(public.progress_gate(id))`); err != nil {
					t.Fatal(err)
				}
				gate := instance.Connect(t)
				if _, err := gate.Exec(ctx, "SELECT pg_advisory_lock(987654)"); err != nil {
					t.Fatal(err)
				}
				worker := instance.Connect(t)
				vacuumCtx, stopVacuum := context.WithCancel(ctx)
				finished := make(chan error, 1)
				defer func() { stopVacuum(); <-finished }()
				go func() { _, err := worker.Exec(vacuumCtx, "VACUUM FULL public.progress_fixture"); finished <- err }()
				job := waitMaintenance(t, ctx, monitor, func(job maintenanceJob) bool {
					return job.Kind == "vacuum_full" && job.Phase == "rebuilding index"
				})
				if job.Table != "public.progress_fixture" || job.Command != "VACUUM FULL" || job.Percent != nil {
					t.Fatalf("VACUUM FULL rebuild must not show a finished heap scan as 100%%: %+v", job)
				}
			})
		})
	}
}

func waitMaintenance(t *testing.T, ctx context.Context, conn *pgx.Conn, match func(maintenanceJob) bool) maintenanceJob {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		view := readMaintenance(ctx, conn)
		if view.Error != "" {
			t.Fatal(view.Error)
		}
		for _, job := range view.Jobs {
			if match(job) {
				return job
			}
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("maintenance did not reach expected phase: %+v", view)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
