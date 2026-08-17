package indexbuild

import "testing"

func TestLargestFirst(t *testing.T) {
	input := []Index{{Name: "a", Bytes: 1}, {Name: "b", Bytes: 9}, {Name: "c", Bytes: 9}}
	got := LargestFirst(input)
	if got[0].Name != "b" || got[1].Name != "c" || got[2].Name != "a" {
		t.Fatalf("schedule = %#v", got)
	}
	if input[0].Name != "a" {
		t.Fatal("schedule mutated caller input")
	}
}

func TestPlanReplayKeepsCorrectnessIndexesCritical(t *testing.T) {
	indexes := []Index{
		{OID: 1, Schema: "s", Name: "plain"},
		{OID: 2, Schema: "s", Name: "unique", Unique: true},
		{OID: 3, Schema: "s", Name: "identity", ReplicaIdentity: true},
		{OID: 4, Schema: "s", Name: "pk", ConstraintOID: 40},
		{OID: 5, Schema: "s", Name: "parent", Partitioned: true},
		{OID: 6, Schema: "s", Name: "child", ParentIndexSchema: "s", ParentIndexName: "parent"},
	}
	plan := PlanReplay(indexes, []Constraint{{OID: 7}})
	if len(plan.DeferredIndexes) != 1 || plan.DeferredIndexes[0].Name != "plain" {
		t.Fatalf("deferred indexes = %#v, want only plain", plan.DeferredIndexes)
	}
	if len(plan.CriticalIndexes) != 5 {
		t.Fatalf("critical indexes = %#v, want five", plan.CriticalIndexes)
	}
	if len(plan.Constraints) != 1 || plan.Constraints[0].OID != 7 {
		t.Fatalf("constraints = %#v", plan.Constraints)
	}
}

func TestConcurrentIndexDefinition(t *testing.T) {
	got, err := concurrentIndexDefinition(Index{
		Schema: "public", Name: "items_payload_idx",
		Definition: "CREATE INDEX items_payload_idx ON public.items USING btree (payload)",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE INDEX CONCURRENTLY items_payload_idx ON public.items USING btree (payload)"
	if got != want {
		t.Fatalf("definition = %q, want %q", got, want)
	}
	if _, err := concurrentIndexDefinition(Index{
		Schema: "public", Name: "items_key", Unique: true,
		Definition: "CREATE UNIQUE INDEX items_key ON public.items (id)",
	}); err == nil {
		t.Fatal("unique index was accepted for deferred concurrent build")
	}
}
