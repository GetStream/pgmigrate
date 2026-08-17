package cdcbench

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type postgresInstance struct {
	URI       string
	container *tcpostgres.PostgresContainer
}

func startPostgres(ctx context.Context, major int) (*postgresInstance, error) {
	startCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	container, err := tcpostgres.Run(
		startCtx,
		fmt.Sprintf("postgres:%d-alpine", major),
		tcpostgres.WithDatabase("pgmigrate_bench"),
		tcpostgres.WithUsername("pgmigrate"),
		tcpostgres.WithPassword("pgmigrate"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs(
			"-c", "fsync=on",
			"-c", "synchronous_commit=on",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
			"-c", "max_connections=100",
			"-c", "shared_buffers=512MB",
			"-c", "max_wal_size=4GB",
			"-c", "checkpoint_timeout=30min",
			"-c", "autovacuum=off",
		),
	)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL %d: %w", major, err)
	}
	uri, err := container.ConnectionString(startCtx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, fmt.Errorf("read PostgreSQL %d connection string: %w", major, err)
	}
	return &postgresInstance{URI: uri, container: container}, nil
}

func (p *postgresInstance) close() error {
	if p == nil || p.container == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return p.container.Terminate(ctx)
}
