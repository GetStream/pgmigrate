package setup

import "testing"

func TestValidateCloneLSN(t *testing.T) {
	for _, test := range []struct {
		name, slot, seed, current string
		valid                     bool
	}{
		{"inside", "0/10", "0/20", "0/30", true},
		{"at slot", "0/10", "0/10", "0/30", true},
		{"at flushed end", "0/10", "0/30", "0/30", true},
		{"stale clone", "0/20", "0/10", "0/30", false},
		{"foreign timeline", "0/10", "0/40", "0/30", false},
		{"zero", "0/10", "0/0", "0/30", false},
		{"malformed", "0/10", "not an LSN", "0/30", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateCloneLSN(test.slot, test.seed, test.current); (err == nil) != test.valid {
				t.Fatalf("validateCloneLSN = %v, valid = %v", err, test.valid)
			}
		})
	}
}
