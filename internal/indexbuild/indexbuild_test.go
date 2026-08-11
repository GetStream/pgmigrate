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
