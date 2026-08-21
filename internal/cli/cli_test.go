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

func TestControllerIsItsOwnCommand(t *testing.T) {
	root := NewRootCommand()
	command, _, err := root.Find([]string{"controller"})
	if err != nil {
		t.Fatal(err)
	}
	if command == root || command.Name() != "controller" {
		t.Fatalf("command = %q, want controller", command.Name())
	}
	listen := command.Flags().Lookup("listen")
	if listen == nil || listen.DefValue != "127.0.0.1:9188" {
		t.Fatalf("listen flag = %#v, want localhost default", listen)
	}
}
