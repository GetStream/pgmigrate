package cli

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/GetStream/pgmigrate/internal/config"
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

func TestControllerWorkerIsHiddenAndRejectsUnknownAction(t *testing.T) {
	t.Parallel()

	command := newControllerWorkerCommand()
	if !command.Hidden {
		t.Fatal("controller worker command is visible")
	}
	payload, err := json.Marshal(config.FromEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	command.SetIn(bytes.NewReader(payload))
	command.SetArgs([]string{"unknown"})
	if err := command.Execute(); err == nil || !strings.Contains(err.Error(), "unsupported controller worker action") {
		t.Fatalf("Execute() error = %v, want unsupported action", err)
	}
}

func TestControllerWorkerRejectsMultipleConfigurationDocuments(t *testing.T) {
	t.Parallel()

	command := newControllerWorkerCommand()
	command.SetIn(strings.NewReader("{}\n{}\n"))
	command.SetArgs([]string{"run"})
	if err := command.Execute(); err == nil || !strings.Contains(err.Error(), "exactly one JSON object") {
		t.Fatalf("Execute() error = %v, want exactly one object", err)
	}
}
