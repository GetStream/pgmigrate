//go:build integration

package pgtest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Instance is a disposable PostgreSQL server with logical replication enabled.
type Instance struct {
	Major     int
	URI       string
	Container *tcpostgres.PostgresContainer
}

// Majors returns the PostgreSQL majors selected for an integration run.
//
// PGTEST_MAJORS may contain a comma-separated subset of 16, 17, and 18. All
// three supported releases are selected when the variable is unset.
func Majors(t testing.TB) []int {
	t.Helper()

	value := strings.TrimSpace(os.Getenv("PGTEST_MAJORS"))
	if value == "" {
		return []int{16, 17, 18}
	}

	parts := strings.Split(value, ",")
	majors := make([]int, 0, len(parts))
	seen := make(map[int]bool, len(parts))
	for _, part := range parts {
		major, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || major < 16 || major > 18 {
			t.Fatalf("PGTEST_MAJORS contains unsupported PostgreSQL major %q", part)
		}
		if !seen[major] {
			majors = append(majors, major)
			seen[major] = true
		}
	}
	if len(majors) == 0 {
		t.Fatal("PGTEST_MAJORS did not select a PostgreSQL major")
	}
	return majors
}

// Start launches a PostgreSQL 16, 17, or 18 container configured for logical
// replication and registers its termination with t.Cleanup.
func Start(t testing.TB, major int) *Instance {
	t.Helper()
	if major < 16 || major > 18 {
		t.Fatalf("unsupported PostgreSQL test major %d", major)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := tcpostgres.Run(
		ctx,
		fmt.Sprintf("postgres:%d-alpine", major),
		tcpostgres.WithDatabase("pgmigrate"),
		tcpostgres.WithUsername("pgmigrate"),
		tcpostgres.WithPassword("pgmigrate"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs(
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
			"-c", "track_commit_timestamp=on",
		),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL %d: %v", major, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate PostgreSQL %d: %v", major, err)
		}
	})

	uri, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("PostgreSQL %d connection string: %v", major, err)
	}
	return &Instance{Major: major, URI: uri, Container: container}
}

// Connect opens a regular SQL connection to the instance.
func (instance *Instance) Connect(t testing.TB) *pgx.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatalf("connect to PostgreSQL %d: %v", instance.Major, err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL %d connection: %v", instance.Major, err)
		}
	})
	return conn
}

// ReplicationConnect opens a connection in PostgreSQL replication-protocol
// database mode for use with pglogrepl.
func (instance *Instance) ReplicationConnect(t testing.TB) *pgconn.PgConn {
	t.Helper()

	config, err := pgconn.ParseConfig(instance.URI)
	if err != nil {
		t.Fatalf("parse PostgreSQL %d replication connection: %v", instance.Major, err)
	}
	config.RuntimeParams["replication"] = "database"

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect to PostgreSQL %d replication protocol: %v", instance.Major, err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close PostgreSQL %d replication connection: %v", instance.Major, err)
		}
	})
	return conn
}
