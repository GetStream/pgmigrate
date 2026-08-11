package observe

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tgross/pgmigrate/internal/state"
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

func TestLagClampsTargetAhead(t *testing.T) {
	t.Parallel()
	lag, err := lagBytes("0/10", "0/20")
	if err != nil || lag != 0 {
		t.Fatalf("lagBytes = %d, %v", lag, err)
	}
}
