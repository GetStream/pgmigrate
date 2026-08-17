package observe

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/pgmigrate/internal/state"
)

type staticProvider struct{ status state.Status }

func (p staticProvider) Snapshot(context.Context) (state.Status, error) { return p.status, nil }

func TestCaptureRenderAndRegistry(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	provider := staticProvider{status: state.Status{
		Migration: state.Migration{Phase: state.PhaseFollow},
		Tables:    state.Counts{Done: 2, Total: 3},
		Apply: state.ApplyProgress{
			StagedLSN: "0/110", AppliedLSN: "0/100", Txns: 7, Rows: 9,
			UpdatedAt: now.Add(-2 * time.Second),
		},
	}}
	snapshot, err := Capture(context.Background(), provider, now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Apply.LagBytes != 16 || snapshot.Apply.StaleFor != 2*time.Second {
		t.Fatalf("apply snapshot = %#v", snapshot.Apply)
	}
	var jsonOut, textOut bytes.Buffer
	if err := RenderJSON(&jsonOut, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := RenderText(&textOut, snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonOut.String(), `"lag_bytes": 16`) ||
		!strings.Contains(textOut.String(), "tables: 2/3") {
		t.Fatalf("JSON:\n%s\ntext:\n%s", jsonOut.String(), textOut.String())
	}
	registry := NewRegistry(provider)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) == 0 {
		t.Fatal("private registry gathered no metrics")
	}
}

func TestApplyRatesAndCompressionAreRenderedAndExported(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 15, 18, 0, 0, 0, time.UTC)
	provider := staticProvider{status: state.Status{
		Migration: state.Migration{Phase: state.PhaseCatchup},
		Apply: state.ApplyProgress{
			StagedLSN: "0/200", AppliedLSN: "0/180",
			Txns: 120, Rows: 600, DMLStatements: 200, TargetCommits: 30,
			RowsPerSecond: 75, UpdatedAt: now,
		},
		ApplyTables: []state.ApplyTableProgress{{
			Schema: "public", Table: "messages", Rows: 600,
			DMLStatements: 200, RowsPerSecond: 75, UpdatedAt: now,
		}},
	}}
	snapshot, err := Capture(context.Background(), provider, now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Apply.RowsPerDMLStatement != 3 ||
		snapshot.Apply.TransactionsPerTargetCommit != 4 ||
		len(snapshot.Apply.Tables) != 1 ||
		snapshot.Apply.Tables[0].Table != "public.messages" ||
		snapshot.Apply.Tables[0].RowsPerSecond != 75 {
		t.Fatalf("apply snapshot = %#v", snapshot.Apply)
	}
	var text bytes.Buffer
	if err := RenderText(&text, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"75 rows/s", "3.00 rows/statement", "4.00 txns/commit",
		"apply public.messages:",
	} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("status does not report %q:\n%s", want, text.String())
		}
	}
	families, err := NewRegistry(provider).Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool)
	for _, family := range families {
		got[family.GetName()] = true
	}
	for _, name := range []string{
		"pgmigrate_apply_rows_per_second",
		"pgmigrate_apply_compression_ratio",
		"pgmigrate_apply_table_total",
		"pgmigrate_apply_table_rows_per_second",
		"pgmigrate_apply_table_compression_ratio",
	} {
		if !got[name] {
			t.Errorf("missing metric family %s", name)
		}
	}
}

// TestVerificationReportsWhatFractionWasReadAndWhatDiffered pins the two
// distinctions a status has to draw: how much of a table was looked at, because a
// check reads a fraction of a large one and reporting only "done" would read as a
// claim about the whole table, and rows that merely differed when first read, which
// is not the same claim as rows that still differ after being re-read.
func TestVerificationReportsWhatFractionWasReadAndWhatDiffered(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	provider := staticProvider{status: state.Status{
		Migration:    state.Migration{Phase: state.PhaseFollow},
		VerifyTables: state.Counts{Done: 1, Total: 2},
		Verification: []state.VerifyTable{
			{
				Schema: "public", Name: "settled", Stage: "done",
				SourcePages: 172672, SourcePagesTotal: 11294175,
				Sampled: 1000000, Estimated: 130900000, TargetRows: 1000000,
				Coverage: 0.00764, Candidates: 2,
				Converged: true, Complete: true, UpdatedAt: now,
			},
			{
				Schema: "public", Name: "broken", Stage: "done",
				SourcePages: 10, SourcePagesTotal: 10,
				Sampled: 40, Estimated: 40, TargetRows: 36,
				Coverage: 1, Candidates: 6, Unresolved: 4,
				Complete: true, UpdatedAt: now,
			},
		},
	}}
	snapshot, err := Capture(context.Background(), provider, now)
	if err != nil {
		t.Fatal(err)
	}
	var textOut bytes.Buffer
	if err := RenderText(&textOut, snapshot); err != nil {
		t.Fatal(err)
	}
	text := textOut.String()
	for _, want := range []string{
		"verify: 1/2",
		"1000000/130900000 rows sampled (0.76%)",
		"172672/11294175 source pages",
		"2 rows settled while rechecking",
		"4 rows diverged",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("status does not report %q:\n%s", want, text)
		}
	}
	registry := NewRegistry(provider)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var pages, rows, divergent bool
	for _, family := range families {
		switch family.GetName() {
		case "pgmigrate_verify_pages":
			pages = len(family.GetMetric()) == 4 // two tables, done and total
		case "pgmigrate_verify_rows":
			rows = len(family.GetMetric()) == 6 // two tables, sampled, estimated, target
		case "pgmigrate_verify_divergent":
			divergent = len(family.GetMetric()) == 4
		}
	}
	if !pages || !rows || !divergent {
		t.Errorf("verification metrics = pages %t, rows %t, divergent %t", pages, rows, divergent)
	}
}

func TestRecoveryProgressIsRenderedAndExported(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 15, 20, 0, 0, 0, time.UTC)
	provider := staticProvider{status: state.Status{
		Migration: state.Migration{Phase: state.PhaseCatchup},
		Recovery: state.RecoveryProgress{
			TotalBytes:         16 << 20,
			TrustedBytes:       12 << 20,
			ScannedBytes:       4 << 20,
			TotalSegments:      8,
			TrustedSegments:    6,
			ScannedSegments:    2,
			Elapsed:            2 * time.Second,
			ScanBytesPerSecond: 2 << 20,
			FallbackReason:     "catalog checksum mismatch",
			ManifestRebuilt:    true,
		},
	}}
	snapshot, err := Capture(context.Background(), provider, now)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Recovery.ScannedBytes != 4<<20 ||
		snapshot.Recovery.ScannedSegments != 2 ||
		snapshot.Recovery.Elapsed != 2*time.Second ||
		snapshot.Recovery.ScanBytesPerSecond != 2<<20 ||
		snapshot.Recovery.FallbackReason != "catalog checksum mismatch" ||
		!snapshot.Recovery.ManifestRebuilt {
		t.Fatalf("recovery snapshot = %#v", snapshot.Recovery)
	}

	var jsonOut, textOut bytes.Buffer
	if err := RenderJSON(&jsonOut, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := RenderText(&textOut, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"trusted_bytes": 12582912`,
		`"scan_bytes_per_second": 2097152`,
		`"manifest_rebuilt": true`,
	} {
		if !strings.Contains(jsonOut.String(), want) {
			t.Errorf("JSON does not report %q:\n%s", want, jsonOut.String())
		}
	}
	for _, want := range []string{
		"12582912/16777216 bytes trusted, 4194304 scanned",
		"6/8 segments trusted, 2 scanned",
		"2s elapsed, 2097152 scan bytes/s",
		"recovery fallback: catalog checksum mismatch",
	} {
		if !strings.Contains(textOut.String(), want) {
			t.Errorf("text does not report %q:\n%s", want, textOut.String())
		}
	}

	families, err := NewRegistry(provider).Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]map[string]float64)
	for _, family := range families {
		switch family.GetName() {
		case "pgmigrate_recovery_bytes", "pgmigrate_recovery_segments":
			values := make(map[string]float64)
			for _, metric := range family.GetMetric() {
				kind := ""
				for _, label := range metric.GetLabel() {
					if label.GetName() == "kind" {
						kind = label.GetValue()
					}
				}
				values[kind] = metric.GetGauge().GetValue()
			}
			byName[family.GetName()] = values
		case "pgmigrate_recovery_elapsed_seconds",
			"pgmigrate_recovery_scan_bytes_per_second":
			byName[family.GetName()] = map[string]float64{
				"": family.GetMetric()[0].GetGauge().GetValue(),
			}
		}
	}
	for name, want := range map[string]map[string]float64{
		"pgmigrate_recovery_bytes": {
			"total": 16 << 20, "trusted": 12 << 20, "scanned": 4 << 20,
		},
		"pgmigrate_recovery_segments": {
			"total": 8, "trusted": 6, "scanned": 2,
		},
		"pgmigrate_recovery_elapsed_seconds": {
			"": 2,
		},
		"pgmigrate_recovery_scan_bytes_per_second": {
			"": 2 << 20,
		},
	} {
		got, ok := byName[name]
		if !ok {
			t.Errorf("missing metric family %s", name)
			continue
		}
		if len(got) != len(want) {
			t.Errorf("%s metric count = %d, want %d", name, len(got), len(want))
		}
		for label, value := range want {
			if got[label] != value {
				t.Errorf("%s{%q} = %v, want %v", name, label, got[label], value)
			}
		}
	}
}

func TestLagClampsTargetAhead(t *testing.T) {
	t.Parallel()
	lag, err := lagBytes("0/10", "0/20")
	if err != nil || lag != 0 {
		t.Fatalf("lagBytes = %d, %v", lag, err)
	}
}
