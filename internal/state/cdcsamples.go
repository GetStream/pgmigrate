package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CDCSample is one change the applier applied, reduced to what identifies its
// row. Key is by column name rather than by position, because the applier keys a
// change on the replica identity and verification keys a row on the primary key,
// and those are allowed to differ.
type CDCSample struct {
	Schema, Table string
	// Index is the reservoir slot this sample occupies, allocated by the sampler
	// in [0, cap). It is what bounds the rows held per relation without ever
	// counting them.
	Index      int64
	Key        map[string]string
	Kind       string
	LSN        string
	ObservedAt time.Time
}

// CDCSampleStream is one relation's reservoir counters.
type CDCSampleStream struct {
	Schema, Table string
	// Observed is every change seen for the relation, which is the denominator
	// for how much of the CDC stream a check looked at.
	Observed int64
	Retained int64
	// Dropped counts changes whose identity could not be recorded, so a relation
	// that is silently contributing nothing to the reservoir says so.
	Dropped int64
}

// PutCDCSamples writes a batch of samples and the counters that go with them.
//
// Both go in one transaction so that a restart cannot resume from counters that
// disagree with the rows: counters ahead of the rows would leave slots that are
// never written, and counters behind them would overwrite retained samples.
func (s *Store) PutCDCSamples(ctx context.Context, samples []CDCSample, streams []CDCSampleStream) error {
	if len(samples) == 0 && len(streams) == 0 {
		return nil
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		if len(samples) > 0 {
			statement, err := tx.PrepareContext(ctx, `
				INSERT INTO cdc_samples
					(schema_name, table_name, sample_index, key, kind, lsn, observed_at)
				VALUES (?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(schema_name, table_name, sample_index) DO UPDATE SET
					key=excluded.key, kind=excluded.kind, lsn=excluded.lsn,
					observed_at=excluded.observed_at`)
			if err != nil {
				return fmt.Errorf("prepare cdc sample insert: %w", err)
			}
			defer statement.Close()
			for _, sample := range samples {
				encoded, err := json.Marshal(sample.Key)
				if err != nil {
					return fmt.Errorf("encode cdc sample key for %s.%s: %w",
						sample.Schema, sample.Table, err)
				}
				if _, err := statement.ExecContext(
					ctx,
					sample.Schema, sample.Table, sample.Index, string(encoded),
					sample.Kind, sample.LSN, unixNano(sample.ObservedAt),
				); err != nil {
					return fmt.Errorf("insert cdc sample for %s.%s: %w",
						sample.Schema, sample.Table, err)
				}
			}
		}
		now := time.Now().UTC().UnixNano()
		for _, stream := range streams {
			if _, err := tx.ExecContext(
				ctx, `
				INSERT INTO cdc_sample_streams
					(schema_name, table_name, observed, retained, dropped, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)
				ON CONFLICT(schema_name, table_name) DO UPDATE SET
					observed=excluded.observed, retained=excluded.retained,
					dropped=excluded.dropped, updated_at=excluded.updated_at`,
				stream.Schema, stream.Table, stream.Observed, stream.Retained,
				stream.Dropped, now,
			); err != nil {
				return fmt.Errorf("record cdc sample stream for %s.%s: %w",
					stream.Schema, stream.Table, err)
			}
		}
		return nil
	})
}

// LoadCDCSampleStreams returns every relation's reservoir counters, which is how
// a restarted applier resumes sampling rather than starting the reservoir again.
func (s *Store) LoadCDCSampleStreams(ctx context.Context) ([]CDCSampleStream, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT schema_name, table_name, observed, retained, dropped
		FROM cdc_sample_streams ORDER BY schema_name, table_name`)
	if err != nil {
		return nil, fmt.Errorf("load cdc sample streams: %w", err)
	}
	defer rows.Close()
	var streams []CDCSampleStream
	for rows.Next() {
		var stream CDCSampleStream
		if err := rows.Scan(&stream.Schema, &stream.Table,
			&stream.Observed, &stream.Retained, &stream.Dropped); err != nil {
			return nil, fmt.Errorf("scan cdc sample stream: %w", err)
		}
		streams = append(streams, stream)
	}
	return streams, rows.Err()
}

// CDCSampleCounters is how many changes the applier saw for one relation and how
// many of them it could not record. A relation it never saw returns zeroes, which
// is the honest answer and is not the same as having seen changes and kept none.
//
// The two are read together because either alone misleads. Fewer keys than changes
// is the ordinary shape of a full reservoir, so it is only the dropped count that
// distinguishes a sample from a relation contributing nothing.
func (s *Store) CDCSampleCounters(ctx context.Context, schema, table string) (observed, dropped int64, err error) {
	err = s.db.QueryRowContext(
		ctx,
		"SELECT observed, dropped FROM cdc_sample_streams WHERE schema_name = ? AND table_name = ?",
		schema, table,
	).Scan(&observed, &dropped)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read cdc sample counters for %s.%s: %w", schema, table, err)
	}
	return observed, dropped, nil
}

// CDCSamples reads one relation's retained samples, all of them: the reservoir is
// bounded to the sampler's capacity as it is written, so there is nothing here to
// bound again. What a check looks at is bounded by the check's own budget, which
// spans a partitioned table's leaves and so cannot be applied one relation at a
// time.
func (s *Store) CDCSamples(ctx context.Context, schema, table string) ([]CDCSample, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT sample_index, key, kind, lsn, observed_at FROM cdc_samples
		WHERE schema_name = ? AND table_name = ? ORDER BY sample_index`,
		schema, table)
	if err != nil {
		return nil, fmt.Errorf("read cdc samples for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var samples []CDCSample
	for rows.Next() {
		sample := CDCSample{Schema: schema, Table: table}
		var (
			encoded  string
			observed int64
		)
		if err := rows.Scan(&sample.Index, &encoded, &sample.Kind,
			&sample.LSN, &observed); err != nil {
			return nil, fmt.Errorf("scan cdc sample for %s.%s: %w", schema, table, err)
		}
		if err := json.Unmarshal([]byte(encoded), &sample.Key); err != nil {
			return nil, fmt.Errorf("decode cdc sample key for %s.%s: %w", schema, table, err)
		}
		sample.ObservedAt = fromUnixNano(observed)
		samples = append(samples, sample)
	}
	return samples, rows.Err()
}
