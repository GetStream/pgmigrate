package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestCapabilitiesForMajor(t *testing.T) {
	tests := []struct {
		major    int
		failover bool
	}{
		{major: 16, failover: false},
		{major: 17, failover: true},
		{major: 18, failover: true},
	}

	for _, test := range tests {
		capabilities, err := CapabilitiesForMajor(test.major)
		if err != nil {
			t.Fatalf("PostgreSQL %d capabilities: %v", test.major, err)
		}
		if capabilities.ReplicationSlotFailover != test.failover {
			t.Errorf(
				"PostgreSQL %d failover slots = %v, want %v",
				test.major,
				capabilities.ReplicationSlotFailover,
				test.failover,
			)
		}
		if !capabilities.SequenceSyncRequired {
			t.Errorf("PostgreSQL %d must retain explicit sequence sync", test.major)
		}
		if !capabilities.ExportedSnapshotRequiresIdle {
			t.Errorf("PostgreSQL %d must keep snapshot exporter command-idle", test.major)
		}
	}
}

func TestCapabilitiesForMajorRejectsUnprobedVersions(t *testing.T) {
	for _, major := range []int{15, 19} {
		if _, err := CapabilitiesForMajor(major); err == nil {
			t.Errorf("CapabilitiesForMajor(%d) unexpectedly succeeded", major)
		}
	}
}

func TestCopyFormatForMajors(t *testing.T) {
	tests := []struct {
		source int
		target int
		want   CopyFormat
	}{
		{source: 16, target: 16, want: CopyFormatBinary},
		{source: 17, target: 17, want: CopyFormatBinary},
		{source: 18, target: 18, want: CopyFormatBinary},
		{source: 16, target: 17, want: CopyFormatText},
		{source: 17, target: 18, want: CopyFormatText},
		{source: 18, target: 16, want: CopyFormatText},
	}

	for _, test := range tests {
		if got := CopyFormatForMajors(test.source, test.target); got != test.want {
			t.Errorf(
				"CopyFormatForMajors(%d, %d) = %q, want %q",
				test.source,
				test.target,
				got,
				test.want,
			)
		}
	}
}

func TestShouldSkipRemoteTransaction(t *testing.T) {
	applied := pglogrepl.LSN(0x30)

	if !ShouldSkipRemoteTransaction(applied, pglogrepl.LSN(0x20)) {
		t.Error("transaction below durable target progress was not skipped")
	}
	if !ShouldSkipRemoteTransaction(applied, applied) {
		t.Error("transaction equal to durable target progress was not skipped")
	}
	if ShouldSkipRemoteTransaction(applied, pglogrepl.LSN(0x40)) {
		t.Error("transaction above replication-origin progress was skipped")
	}
}
