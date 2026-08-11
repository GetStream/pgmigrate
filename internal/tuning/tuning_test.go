package tuning

import (
	"strings"
	"testing"
)

// stockTarget is a plausible untuned server: PostgreSQL defaults for everything
// this package manages, 8 GB of shared_buffers, and permission to ALTER SYSTEM.
func stockTarget() Target {
	return Target{
		SharedBuffersBytes: 8 << 30,
		MaxWorkerProcesses: 8,
		Settings: map[string]Setting{
			MaintenanceWorkMem:            {Name: MaintenanceWorkMem, Value: "65536", Unit: "kB", Context: "user"},
			MaxParallelMaintenanceWorkers: {Name: MaxParallelMaintenanceWorkers, Value: "2", Context: "user"},
			SynchronousCommit:             {Name: SynchronousCommit, Value: "on", Context: "user"},
			MaxWALSize:                    {Name: MaxWALSize, Value: "1024", Unit: "MB", Context: "sighup", AlterSystem: true},
			CheckpointTimeout:             {Name: CheckpointTimeout, Value: "300", Unit: "s", Context: "sighup", AlterSystem: true},
			CheckpointCompletionTarget:    {Name: CheckpointCompletionTarget, Value: "0.9", Context: "sighup", AlterSystem: true},
		},
	}
}

func changeFor(t *testing.T, plan Plan, name string) Change {
	t.Helper()
	for _, change := range plan.Changes {
		if change.Name == name {
			return change
		}
	}
	t.Fatalf("plan has no change for %s (blocked: %v)", name, plan.Blocked)
	return Change{}
}

func hasChange(plan Plan, name string) bool {
	for _, change := range plan.Changes {
		if change.Name == name {
			return true
		}
	}
	return false
}

func TestDeriveSizesMaintenanceWorkMemToFitTargetMemory(t *testing.T) {
	t.Parallel()
	// The whole point of deriving rather than using a fixed value: the product
	// of the per-session setting and the number of concurrent index builds has
	// to fit the target, so more workers must mean less memory each.
	memory := int64(8) << 30
	target := stockTarget()
	target.SharedBuffersBytes = memory / sharedBuffersMemoryRatio

	previous := int64(1) << 62
	for _, workers := range []int{1, 2, 4, 8, 16} {
		plan, err := Derive(target, Overrides{}, workers)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseBytes(changeFor(t, plan, MaintenanceWorkMem).To)
		if err != nil {
			t.Fatal(err)
		}
		if product := got * int64(workers); product > memory {
			t.Errorf("workers=%d: %d x %d = %d exceeds target memory %d",
				workers, workers, got, product, memory)
		}
		if got > previous {
			t.Errorf("workers=%d: memory per worker rose to %d from %d", workers, got, previous)
		}
		previous = got
	}
}

func TestDeriveClampsMaintenanceWorkMem(t *testing.T) {
	t.Parallel()
	t.Run("a tiny target is never given less than the default", func(t *testing.T) {
		target := stockTarget()
		target.SharedBuffersBytes = 16 << 20
		plan, err := Derive(target, Overrides{}, 8)
		if err != nil {
			t.Fatal(err)
		}
		// The budget works out below the default, so there is nothing to gain
		// and the setting is left alone rather than lowered.
		if hasChange(plan, MaintenanceWorkMem) {
			t.Fatalf("lowered maintenance_work_mem on a small target: %v",
				changeFor(t, plan, MaintenanceWorkMem))
		}
	})
	t.Run("a huge target is capped", func(t *testing.T) {
		target := stockTarget()
		target.SharedBuffersBytes = 2 << 40
		plan, err := Derive(target, Overrides{}, 1)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseBytes(changeFor(t, plan, MaintenanceWorkMem).To)
		if err != nil {
			t.Fatal(err)
		}
		if got != maxMaintenanceWorkMem {
			t.Fatalf("maintenance_work_mem = %d, want the %d cap", got, maxMaintenanceWorkMem)
		}
	})
}

func TestDeriveEstimatesMemoryFromSharedBuffersUnlessOverridden(t *testing.T) {
	t.Parallel()
	target := stockTarget()

	estimated, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !estimated.MemoryEstimated {
		t.Error("memory derived from shared_buffers is not marked as an estimate")
	}
	if want := target.SharedBuffersBytes * sharedBuffersMemoryRatio; estimated.MemoryBytes != want {
		t.Errorf("estimated memory = %d, want %d", estimated.MemoryBytes, want)
	}

	override := int64(64) << 30
	supplied, err := Derive(target, Overrides{TargetMemoryBytes: override}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if supplied.MemoryEstimated {
		t.Error("operator-supplied memory is marked as an estimate")
	}
	if supplied.MemoryBytes != override {
		t.Errorf("memory = %d, want the supplied %d", supplied.MemoryBytes, override)
	}
	if supplied.MemoryBytes <= estimated.MemoryBytes {
		t.Fatal("test is not exercising the override: it matches the estimate")
	}
	if same := changeFor(t, supplied, MaintenanceWorkMem).To ==
		changeFor(t, estimated, MaintenanceWorkMem).To; same {
		t.Error("the memory override did not change the derived maintenance_work_mem")
	}
}

func TestDeriveHonoursExplicitOverrides(t *testing.T) {
	t.Parallel()
	plan, err := Derive(stockTarget(), Overrides{
		MaintenanceWorkMem:            "3GB",
		MaxParallelMaintenanceWorkers: 3,
		MaxWALSize:                    "16GB",
		CheckpointTimeout:             "15min",
	}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		MaintenanceWorkMem:            "3GB",
		MaxParallelMaintenanceWorkers: "3",
		MaxWALSize:                    "16GB",
		CheckpointTimeout:             "15min",
	} {
		if got := changeFor(t, plan, name).To; got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestDeriveRejectsAnUnparseableOverride(t *testing.T) {
	t.Parallel()
	if _, err := Derive(stockTarget(), Overrides{MaintenanceWorkMem: "several"}, 4); err == nil {
		t.Fatal("accepted an unparseable maintenance_work_mem override")
	}
}

func TestDerivePlansNothingForAnAlreadyTunedTarget(t *testing.T) {
	t.Parallel()
	// An operator who has already applied the tuning table, or a second run
	// against a target that is still tuned, must produce no changes: otherwise
	// a rerun would record a bulk-load value as the value to revert to.
	target := stockTarget()
	target.Settings[MaintenanceWorkMem] = Setting{
		Name: MaintenanceWorkMem, Value: "8388608", Unit: "kB", Context: "user",
	}
	target.Settings[SynchronousCommit] = Setting{
		Name: SynchronousCommit, Value: "off", Context: "user",
	}
	target.Settings[MaxParallelMaintenanceWorkers] = Setting{
		Name: MaxParallelMaintenanceWorkers, Value: "4", Context: "user",
	}
	target.Settings[MaxWALSize] = Setting{
		Name: MaxWALSize, Value: "65536", Unit: "MB", Context: "sighup", AlterSystem: true,
	}
	target.Settings[CheckpointTimeout] = Setting{
		Name: CheckpointTimeout, Value: "1800", Unit: "s", Context: "sighup", AlterSystem: true,
	}

	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Empty() {
		t.Fatalf("planned changes against an already-tuned target: %v", plan.Changes)
	}
}

func TestDeriveNeverWeakensASetting(t *testing.T) {
	t.Parallel()
	// A target tuned beyond what we would ask for must be left as it is, not
	// pulled down to our value.
	target := stockTarget()
	target.Settings[MaxWALSize] = Setting{
		Name: MaxWALSize, Value: "131072", Unit: "MB", Context: "sighup", AlterSystem: true,
	}
	target.Settings[CheckpointTimeout] = Setting{
		Name: CheckpointTimeout, Value: "3600", Unit: "s", Context: "sighup", AlterSystem: true,
	}
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{MaxWALSize, CheckpointTimeout} {
		if hasChange(plan, name) {
			t.Errorf("lowered %s: %v", name, changeFor(t, plan, name))
		}
	}
}

func TestDeriveBlocksSystemChangesWithoutAlterSystem(t *testing.T) {
	t.Parallel()
	// The managed-service case, which is the common one: session settings still
	// apply, the rest are reported so preflight can explain them.
	target := stockTarget()
	for _, name := range []string{MaxWALSize, CheckpointTimeout, CheckpointCompletionTarget} {
		setting := target.Settings[name]
		setting.AlterSystem = false
		target.Settings[name] = setting
	}
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.SystemChanges()) != 0 {
		t.Fatalf("planned system changes on a target that refuses ALTER SYSTEM: %v", plan.SystemChanges())
	}
	if len(plan.SessionGUCs()) == 0 {
		t.Fatal("no session tuning planned; the load would run entirely untuned")
	}
	for _, name := range []string{MaxWALSize, CheckpointTimeout} {
		reason, ok := plan.Blocked[name]
		if !ok {
			t.Errorf("%s is neither planned nor reported as blocked", name)
			continue
		}
		if !strings.Contains(reason, "ALTER SYSTEM") {
			t.Errorf("%s blocked for an unhelpful reason: %q", name, reason)
		}
	}
}

func TestDeriveResetsOnRevertOnlyWhenNotAlreadyInAutoConf(t *testing.T) {
	t.Parallel()
	// Reverting by RESET restores what the server was inheriting. That is right
	// for a setting we are the first to pin, and wrong for one that was already
	// in postgresql.auto.conf, where RESET would discard the operator's value.
	target := stockTarget()
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !changeFor(t, plan, MaxWALSize).ResetOnRevert {
		t.Error("a setting we are the first to pin should revert by RESET")
	}

	setting := target.Settings[MaxWALSize]
	setting.FromAutoConf = true
	target.Settings[MaxWALSize] = setting
	plan, err = Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	change := changeFor(t, plan, MaxWALSize)
	if change.ResetOnRevert {
		t.Error("a setting already in postgresql.auto.conf must revert to its recorded value, not RESET")
	}
	// Recorded the way SHOW renders it, so the log and the finding read the same
	// as the server does and the value can go straight back to ALTER SYSTEM.
	if change.From != "1GB" {
		t.Errorf("recorded original = %q, want 1GB", change.From)
	}
}

func TestDeriveScopesSettingsByContext(t *testing.T) {
	t.Parallel()
	plan, err := Derive(stockTarget(), Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]Scope{
		MaintenanceWorkMem:            ScopeSession,
		MaxParallelMaintenanceWorkers: ScopeSession,
		SynchronousCommit:             ScopeSession,
		MaxWALSize:                    ScopeSystem,
		CheckpointTimeout:             ScopeSystem,
	} {
		if got := changeFor(t, plan, name).Scope; got != want {
			t.Errorf("%s scope = %q, want %q", name, got, want)
		}
	}
	// synchronous_commit must be a session change and nothing else, because the
	// CDC applier's sessions must keep the default. A system-wide change would
	// reach the applier and weaken segment pruning's safety.
	if _, ok := plan.SessionGUCs()[SynchronousCommit]; !ok {
		t.Error("synchronous_commit is not among the session settings")
	}
	for _, change := range plan.SystemChanges() {
		if change.Name == SynchronousCommit {
			t.Fatal("synchronous_commit planned as a system change; it would reach the CDC applier")
		}
	}
}

func TestDeriveLeavesParallelWorkersAloneWithoutWorkerHeadroom(t *testing.T) {
	t.Parallel()
	target := stockTarget()
	target.MaxWorkerProcesses = 1
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if hasChange(plan, MaxParallelMaintenanceWorkers) {
		t.Fatal("planned parallel maintenance workers with no worker headroom")
	}
	if reason := plan.Blocked[MaxParallelMaintenanceWorkers]; !strings.Contains(reason, "restart") {
		t.Errorf("blocked reason does not mention the restart requirement: %q", reason)
	}
}

func TestDeriveCapsParallelWorkersToWorkerProcesses(t *testing.T) {
	t.Parallel()
	target := stockTarget()
	target.MaxWorkerProcesses = 3
	// Start below the cap so the derived value is visible as a change rather
	// than being suppressed for already satisfying the request.
	target.Settings[MaxParallelMaintenanceWorkers] = Setting{
		Name: MaxParallelMaintenanceWorkers, Value: "0", Context: "user",
	}
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if got := changeFor(t, plan, MaxParallelMaintenanceWorkers).To; got != "2" {
		t.Fatalf("max_parallel_maintenance_workers = %q, want 2 to leave the leader a worker", got)
	}
}

func TestDeriveRequiresPositiveWorkers(t *testing.T) {
	t.Parallel()
	if _, err := Derive(stockTarget(), Overrides{}, 0); err == nil {
		t.Fatal("accepted zero workers")
	}
}

func TestDeriveBlocksSettingsNeedingARestart(t *testing.T) {
	t.Parallel()
	target := stockTarget()
	target.Settings[MaxWALSize] = Setting{
		Name: MaxWALSize, Value: "1024", Unit: "MB", Context: "postmaster", AlterSystem: true,
	}
	plan, err := Derive(target, Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if hasChange(plan, MaxWALSize) {
		t.Fatal("planned a change that needs a restart")
	}
	if reason := plan.Blocked[MaxWALSize]; !strings.Contains(reason, "restart") {
		t.Errorf("blocked reason = %q, want it to mention a restart", reason)
	}
}

func TestParseBytesReadsPostgreSQLSizes(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]int64{
		"64MB":  64 << 20,
		"2GB":   2 << 30,
		"8192":  8192,
		"512kB": 512 << 10,
		"1TB":   1 << 40,
		"16":    16,
	} {
		got, err := ParseBytes(input)
		if err != nil {
			t.Errorf("%s: %v", input, err)
			continue
		}
		if got != want {
			t.Errorf("ParseBytes(%q) = %d, want %d", input, got, want)
		}
	}
	for _, input := range []string{"", "lots", "12PB", "1.5GB"} {
		if _, err := ParseBytes(input); err == nil {
			t.Errorf("ParseBytes(%q) accepted an invalid size", input)
		}
	}
}

func TestFormatBytesRoundTripsThroughParseBytes(t *testing.T) {
	t.Parallel()
	for _, want := range []int64{
		64 << 20, 2 << 30, 16 << 30, 1536 << 20, 1 << 10, 12345,
	} {
		rendered := FormatBytes(want)
		got, err := ParseBytes(rendered)
		if err != nil {
			t.Errorf("FormatBytes(%d) = %q, which does not parse: %v", want, rendered, err)
			continue
		}
		if got != want {
			t.Errorf("FormatBytes(%d) = %q, which parses back as %d", want, rendered, got)
		}
	}
}

func TestCanonicalRendersASettingTheWayShowDoes(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		setting Setting
		want    string
	}{
		{Setting{Value: "1024", Unit: "MB"}, "1GB"},
		{Setting{Value: "65536", Unit: "kB"}, "64MB"},
		{Setting{Value: "16384", Unit: "8kB"}, "128MB"},
		{Setting{Value: "300", Unit: "s"}, "5min"},
		{Setting{Value: "1800", Unit: "s"}, "30min"},
		{Setting{Value: "3600", Unit: "s"}, "1h"},
		{Setting{Value: "250", Unit: "ms"}, "250ms"},
		{Setting{Value: "off"}, "off"},
		{Setting{Value: "0.9"}, "0.9"},
		{Setting{Value: "4"}, "4"},
	} {
		if got := testCase.setting.Canonical(); got != testCase.want {
			t.Errorf("Setting{%q,%q}.Canonical() = %q, want %q",
				testCase.setting.Value, testCase.setting.Unit, got, testCase.want)
		}
	}
}

func TestParseDurationSecondsUsesTheReportedUnit(t *testing.T) {
	t.Parallel()
	// A pg_settings value carries its unit in a separate column, so "300" with
	// unit "s" and the literal "30min" both have to work.
	for _, testCase := range []struct {
		value, unit string
		want        float64
	}{
		{"300", "s", 300},
		{"30min", "", 1800},
		{"5", "min", 300},
		{"1000", "ms", 1},
		{"1h", "", 3600},
	} {
		got, err := parseDurationSeconds(testCase.value, testCase.unit)
		if err != nil {
			t.Errorf("%q/%q: %v", testCase.value, testCase.unit, err)
			continue
		}
		if got != testCase.want {
			t.Errorf("parseDurationSeconds(%q,%q) = %v, want %v",
				testCase.value, testCase.unit, got, testCase.want)
		}
	}
}

func TestSessionGUCsAndSystemChangesPartitionThePlan(t *testing.T) {
	t.Parallel()
	plan, err := Derive(stockTarget(), Overrides{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if total := len(plan.SessionGUCs()) + len(plan.SystemChanges()); total != len(plan.Changes) {
		t.Fatalf("session (%d) plus system (%d) changes do not account for all %d changes",
			len(plan.SessionGUCs()), len(plan.SystemChanges()), len(plan.Changes))
	}
}

func TestValidValueRejectsQuotesAndStatements(t *testing.T) {
	t.Parallel()
	// ALTER SYSTEM cannot bind parameters, so the value is interpolated and has
	// to be checked.
	for _, value := range []string{"64GB", "off", "0.9", "30min", "4", "-1"} {
		if !validValue(value) {
			t.Errorf("rejected the legitimate value %q", value)
		}
	}
	for _, value := range []string{
		"", "64GB'", "off; DROP TABLE t", "a'--", `a"b`, "1 GB", "$(x)",
	} {
		if validValue(value) {
			t.Errorf("accepted the unsafe value %q", value)
		}
	}
}

func TestOverridesValidateRejectsBadValuesAndParsesMemory(t *testing.T) {
	t.Parallel()
	valid := Overrides{
		MaintenanceWorkMem:            "4GB",
		MaxParallelMaintenanceWorkers: 4,
		MaxWALSize:                    "32GB",
		CheckpointTimeout:             "20min",
		TargetMemory:                  "128GB",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("rejected valid overrides: %v", err)
	}
	if want := int64(128) << 30; valid.TargetMemoryBytes != want {
		t.Errorf("TargetMemoryBytes = %d, want %d", valid.TargetMemoryBytes, want)
	}

	for name, overrides := range map[string]Overrides{
		"unparseable work mem":     {MaintenanceWorkMem: "plenty"},
		"unparseable wal size":     {MaxWALSize: "big"},
		"unparseable timeout":      {CheckpointTimeout: "soon"},
		"negative parallel":        {MaxParallelMaintenanceWorkers: -1},
		"unparseable memory":       {TargetMemory: "some"},
		"zero memory":              {TargetMemory: "0"},
		"quote in an override":     {MaxWALSize: "1GB'"},
		"statement in an override": {MaintenanceWorkMem: "1GB; DROP TABLE t"},
	} {
		if err := overrides.Validate(); err == nil {
			t.Errorf("%s: accepted %+v", name, overrides)
		}
	}
}

func TestAlterSystemRefusesAnUnmanagedSetting(t *testing.T) {
	t.Parallel()
	// The allowlist is what keeps the interpolated statement safe, so it has to
	// hold even when the caller is wrong.
	if err := checkSettable("archive_command", "64GB"); err == nil {
		t.Fatal("accepted a setting outside the managed set")
	}
	if err := checkSettable(MaxWALSize, "64GB'; SELECT 1"); err == nil {
		t.Fatal("accepted an unsafe value for a managed setting")
	}
	if err := checkSettable(MaxWALSize, "64GB"); err != nil {
		t.Fatalf("rejected a legitimate managed change: %v", err)
	}
}
