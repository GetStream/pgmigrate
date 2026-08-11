// Package postgres records PostgreSQL-version assumptions used by migrations.
//
// Logical replication slots can export a transaction snapshot, but PostgreSQL
// keeps that snapshot valid only while the replication connection remains
// command-idle. Sequence values are not part of logical replication and still
// require an explicit synchronization step for every supported release,
// including PostgreSQL 18.
//
// PostgreSQL 16-18 durably advance replication-origin progress for an explicit
// rollback even though its DML is absent. Origins therefore are not a safe
// resume authority; pgmigrate records progress transactionally with target DML
// in the target-local progress table managed by this package.
package postgres

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

const (
	// MinSupportedMajor is the oldest PostgreSQL release supported by pgmigrate.
	MinSupportedMajor = 16
	// MaxSupportedMajor is the newest PostgreSQL release whose assumptions have
	// been probed by the integration suite.
	MaxSupportedMajor = 18
)

// Capabilities describes migration-relevant behavior for a PostgreSQL major
// release.
type Capabilities struct {
	ReplicationSlotFailover      bool
	SequenceSyncRequired         bool
	ExportedSnapshotRequiresIdle bool
	// LogicalMessageFlush reports that pg_logical_emit_message accepts a flush
	// argument. Without it a nontransactional message is written but not flushed,
	// so the walsender cannot decode it until some later commit flushes the WAL
	// past it, and anything waiting for that message waits on the source's write
	// traffic instead of on itself.
	LogicalMessageFlush bool
}

// CapabilitiesForMajor returns the probed capabilities for a supported major.
func CapabilitiesForMajor(major int) (Capabilities, error) {
	if major < MinSupportedMajor || major > MaxSupportedMajor {
		return Capabilities{}, fmt.Errorf(
			"unsupported PostgreSQL major %d (supported: %d-%d)",
			major,
			MinSupportedMajor,
			MaxSupportedMajor,
		)
	}

	return Capabilities{
		ReplicationSlotFailover:      major >= 17,
		SequenceSyncRequired:         true,
		ExportedSnapshotRequiresIdle: true,
		LogicalMessageFlush:          major >= 17,
	}, nil
}

// CopyFormat is the COPY encoding safe for a source/target major-version pair.
type CopyFormat string

const (
	// CopyFormatBinary is safe only when source and target have the same major.
	CopyFormatBinary CopyFormat = "binary"
	// CopyFormatText is the portable fallback across PostgreSQL major releases.
	CopyFormatText CopyFormat = "text"
)

// CopyFormatForMajors chooses binary COPY only for equal major releases.
func CopyFormatForMajors(sourceMajor, targetMajor int) CopyFormat {
	if sourceMajor == targetMajor {
		return CopyFormatBinary
	}
	return CopyFormatText
}

// ShouldSkipRemoteTransaction reports whether incoming is covered by the
// authoritative target-local progress returned by ReadProgress.
func ShouldSkipRemoteTransaction(applied, incoming pglogrepl.LSN) bool {
	return incoming <= applied
}
