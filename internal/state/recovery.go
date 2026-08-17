package state

import (
	"context"
	"database/sql"
	"fmt"
)

// UpdateRecoveryProgress atomically replaces the singleton startup recovery
// status. Repeating the same observation leaves the same durable row.
func (s *Store) UpdateRecoveryProgress(ctx context.Context, progress RecoveryProgress) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO recovery_progress (
				id, total_bytes, trusted_bytes, scanned_bytes,
				total_segments, trusted_segments, scanned_segments,
				elapsed_ns, scan_bytes_per_second, fallback_reason,
				manifest_rebuilt
			)
			VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				total_bytes=excluded.total_bytes,
				trusted_bytes=excluded.trusted_bytes,
				scanned_bytes=excluded.scanned_bytes,
				total_segments=excluded.total_segments,
				trusted_segments=excluded.trusted_segments,
				scanned_segments=excluded.scanned_segments,
				elapsed_ns=excluded.elapsed_ns,
				scan_bytes_per_second=excluded.scan_bytes_per_second,
				fallback_reason=excluded.fallback_reason,
				manifest_rebuilt=excluded.manifest_rebuilt`,
			progress.TotalBytes,
			progress.TrustedBytes,
			progress.ScannedBytes,
			progress.TotalSegments,
			progress.TrustedSegments,
			progress.ScannedSegments,
			progress.Elapsed.Nanoseconds(),
			progress.ScanBytesPerSecond,
			progress.FallbackReason,
			progress.ManifestRebuilt,
		)
		if err != nil {
			return fmt.Errorf("update recovery progress: %w", err)
		}
		return nil
	})
}
