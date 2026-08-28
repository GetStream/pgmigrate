package app

import (
	"bytes"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/config"
	"github.com/GetStream/pgmigrate/internal/state"
	"github.com/GetStream/pgmigrate/internal/verify"
)

func TestDeferredCDCProgressIsNotCompleteOrDivergent(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	p := newVerifyProgress(config.Config{Dir: t.TempDir()}, nil, &output, []state.Table{{OID: 1, Schema: "public", Name: "items"}})
	p.Update(verify.Progress{Table: "public.items", Stage: verify.StageDone, Complete: true, Converged: true})
	p.Update(verify.Progress{Table: "public.items", Stage: verify.StageCDCDeferred, CDCPending: 41, CDCKeys: 100})
	got := p.merged["public.items"]
	if got.Complete || got.Converged || got.Unresolved != 0 || got.Stage != string(verify.StageCDCDeferred) {
		t.Fatalf("pending progress = %+v", got)
	}
	if !strings.Contains(output.String(), "41 CDC rows pending (not final divergence)") {
		t.Fatalf("missing provisional explanation: %s", output.String())
	}
	p.Update(verify.Progress{Table: "public.items", Stage: verify.StageCDCRechecking, CDCPending: 41})
	if got.Complete || got.Converged {
		t.Fatalf("rechecking marked complete: %+v", got)
	}
	p.Update(verify.Progress{Table: "public.items", Stage: verify.StageDone, Complete: true, Converged: true})
	if !got.Complete || !got.Converged || got.Unresolved != 0 {
		t.Fatalf("final progress = %+v", got)
	}
}

func TestCDCSummaryDistinguishesChangedAdvancedAndPending(t *testing.T) {
	result := verify.Result{Tables: []verify.TableResult{{CDC: verify.CDCResult{Keys: 10, Observed: 20, SourceChanged: 2, Pending: 3, Advanced: 4}}}}
	got := cdcSummary(result)
	for _, want := range []string{"10 of 20 applied rows checked", "2 source-changed CDC rows required target advancement", "3 CDC rows still pending", "4 CDC target rows advanced without matching; accepted as progressing"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q missing %q", got, want)
		}
	}
}

func TestTargetStalledFailureIsReported(t *testing.T) {
	if got := diffKinds([]verify.RowDiff{{Kind: verify.DiffTargetStalled}}); got != "1 target_stalled" {
		t.Fatalf("stalled-target failure hidden: %q", got)
	}
}
