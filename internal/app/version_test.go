package app

import "testing"

func TestVersionReportsTheBuildOrAdmitsItIsOne(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		version  string
		revision string
		modified bool
		want     string
	}{
		{name: "installed release", version: "v0.1.0", want: "v0.1.0"},
		{
			name: "built from a clean checkout", version: "(devel)",
			revision: "a956947f0e1c4b8d9a2f", want: "devel+a956947f0e1c",
		},
		{
			name: "built from a dirty checkout", version: "(devel)",
			revision: "a956947f0e1c4b8d9a2f", modified: true,
			want: "devel+a956947f0e1c.dirty",
		},
		// Neither a version nor a commit is what a `go test` binary and a stripped
		// build look like, and the report has to say something.
		{name: "no build information at all", want: "devel"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := formatVersion(test.version, test.revision, test.modified); got != test.want {
				t.Errorf("formatVersion(%q, %q, %v) = %q, want %q",
					test.version, test.revision, test.modified, got, test.want)
			}
		})
	}
}

// The report is the only record of which build performed a cutover, so an empty
// string in that field is a defect rather than a cosmetic gap.
func TestVersionIsNeverEmpty(t *testing.T) {
	t.Parallel()
	if Version() == "" {
		t.Error("Version() is empty")
	}
}
