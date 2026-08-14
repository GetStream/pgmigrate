package cli

import (
	"slices"
	"strings"
	"testing"
)

func TestSequencesIsItsOwnCommand(t *testing.T) {
	root := NewRootCommand()
	var names []string
	for _, command := range root.Commands() {
		names = append(names, command.Name())
	}
	if !slices.Contains(names, "sequences") {
		t.Fatalf("root commands = %s, want one named sequences", strings.Join(names, ", "))
	}
	offset := root.PersistentFlags().Lookup("sequence-offset")
	if offset == nil {
		t.Fatal("sequence-offset flag is missing")
	}
	// Cutover leaves the source room to keep allocating, and so must a
	// standalone run: a zero default would hand both databases the same values.
	if offset.DefValue != "1000000" {
		t.Errorf("sequence-offset defaults to %s, want 1000000", offset.DefValue)
	}
}

func TestReplayWorkersFlagHasConcurrentDefault(t *testing.T) {
	flags := NewRootCommand().PersistentFlags()
	flag := flags.Lookup("replay-workers")
	if flag == nil {
		t.Fatal("replay-workers flag is missing")
	}
	if flag.DefValue == "0" || flag.DefValue == "1" {
		t.Fatalf("replay-workers default = %s, want concurrent replay", flag.DefValue)
	}
	for _, name := range []string{"replay-batch-size", "replay-window"} {
		if value := flags.Lookup(name); value == nil || value.DefValue == "0" || value.DefValue == "1" {
			t.Fatalf("%s default is not throughput-oriented: %#v", name, value)
		}
	}
}
