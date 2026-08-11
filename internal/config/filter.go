package config

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

const maxFilterLineBytes = 1024 * 1024

// Filter selects schema-qualified table names using ordered glob rules.
// Positive rules include matches and rules prefixed with "!" exclude matches.
// The last matching rule wins.
type Filter struct {
	rules      []filterRule
	hasInclude bool
	normalized string
}

type filterRule struct {
	pattern string
	exclude bool
}

// ParseFilter parses one schema.table glob per line. Empty lines and lines
// whose first non-space character is '#' are ignored.
func ParseFilter(r io.Reader) (Filter, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 4096), maxFilterLineBytes)

	var filter Filter
	var normalized []string
	for line := 1; scanner.Scan(); line++ {
		value := strings.TrimSpace(scanner.Text())
		if value == "" || strings.HasPrefix(value, "#") {
			continue
		}

		rule := filterRule{}
		if strings.HasPrefix(value, "!") {
			rule.exclude = true
			value = strings.TrimSpace(strings.TrimPrefix(value, "!"))
		}
		if value == "" {
			return Filter{}, fmt.Errorf("table filter line %d: empty pattern", line)
		}
		if strings.Count(value, ".") != 1 {
			return Filter{}, fmt.Errorf("table filter line %d: pattern %q must be schema.table", line, value)
		}
		if _, err := path.Match(value, "schema.table"); err != nil {
			return Filter{}, fmt.Errorf("table filter line %d: invalid glob %q: %w", line, value, err)
		}

		rule.pattern = value
		filter.rules = append(filter.rules, rule)
		if !rule.exclude {
			filter.hasInclude = true
		}
		prefix := ""
		if rule.exclude {
			prefix = "!"
		}
		normalized = append(normalized, prefix+value)
	}
	if err := scanner.Err(); err != nil {
		return Filter{}, fmt.Errorf("read table filter: %w", err)
	}

	filter.normalized = strings.Join(normalized, "\n")
	return filter, nil
}

// LoadFilter reads and parses a table filter file.
func LoadFilter(filename string) (Filter, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Filter{}, fmt.Errorf("open table filter: %w", err)
	}
	defer file.Close()

	filter, err := ParseFilter(file)
	if err != nil {
		return Filter{}, fmt.Errorf("%s: %w", filename, err)
	}
	return filter, nil
}

// Match reports whether a schema-qualified table name is selected.
func (f Filter) Match(schema, table string) bool {
	name := schema + "." + table
	included := !f.hasInclude
	for _, rule := range f.rules {
		matched, err := path.Match(rule.pattern, name)
		if err == nil && matched {
			included = !rule.exclude
		}
	}
	return included
}

// Normalized returns the canonical, comment-free filter representation.
func (f Filter) Normalized() string {
	return f.normalized
}

// Fingerprint returns a stable SHA-256 fingerprint of the normalized filter.
func (f Filter) Fingerprint() string {
	sum := sha256.Sum256([]byte(f.normalized))
	return hex.EncodeToString(sum[:])
}
