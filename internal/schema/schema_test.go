package schema

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestParseTOCAndUseList(t *testing.T) {
	input := []byte(`; Archive created at 2026-01-01
2; 2615 2200 SCHEMA - public owner
17; 1259 42 TABLE public odd table owner
33; 1259 84 INDEX public odd index owner
40; 2606 85 FK CONSTRAINT public child child_parent_fkey owner
`)
	entries, err := ParseTOC(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 || entries[1].Tag != "odd table" {
		t.Fatalf("unexpected entries: %#v", entries)
	}
	if Classify(entries[0]) != PreData || Classify(entries[2]) != ManagedIndex || Classify(entries[3]) != DeferredPostData {
		t.Fatal("entries were misclassified")
	}
	list := string(UseList(entries, func(entry TOCEntry) bool { return Classify(entry) == PreData }))
	if !strings.Contains(list, "\n;33;") || !strings.Contains(list, "\n;40;") {
		t.Fatalf("excluded entries not commented:\n%s", list)
	}
}

// parseOne parses a single TOC line and fails the test on error.
func parseOne(t *testing.T, line string) TOCEntry {
	t.Helper()
	entries, err := ParseTOC([]byte(line + "\n"))
	if err != nil {
		t.Fatalf("ParseTOC(%q): %v", line, err)
	}
	if len(entries) != 1 {
		t.Fatalf("ParseTOC(%q) returned %d entries", line, len(entries))
	}
	return entries[0]
}

// TestParseTOCTextSearchObjects covers the four text-search descriptions. A
// user-defined text-search configuration aborted the first managed-cloud
// migration during the schema phase.
func TestParseTOCTextSearchObjects(t *testing.T) {
	tests := []struct {
		line                               string
		description, namespace, tag, owner string
	}{
		{
			"2683; 3602 16882 TEXT SEARCH CONFIGURATION shard_schema unaccent_simple shard_admin",
			"TEXT SEARCH CONFIGURATION", "shard_schema", "unaccent_simple", "shard_admin",
		},
		{
			"2684; 3600 16883 TEXT SEARCH DICTIONARY shard_schema unaccent_dict shard_admin",
			"TEXT SEARCH DICTIONARY", "shard_schema", "unaccent_dict", "shard_admin",
		},
		{
			"2685; 3601 16884 TEXT SEARCH PARSER shard_schema custom parser shard_admin",
			"TEXT SEARCH PARSER", "shard_schema", "custom parser", "shard_admin",
		},
		{
			"2686; 3764 16885 TEXT SEARCH TEMPLATE shard_schema custom_template shard_admin",
			"TEXT SEARCH TEMPLATE", "shard_schema", "custom_template", "shard_admin",
		},
	}
	for _, test := range tests {
		entry := parseOne(t, test.line)
		if entry.Description != test.description || entry.Namespace != test.namespace ||
			entry.Tag != test.tag || entry.Owner != test.owner {
			t.Errorf("parsed %q as description=%q namespace=%q tag=%q owner=%q",
				test.line, entry.Description, entry.Namespace, entry.Tag, entry.Owner)
		}
		if Classify(entry) != PreData {
			t.Errorf("%s must restore with pre-data, got class %v", test.description, Classify(entry))
		}
	}
}

// TestParseTOCPrefixShadowing covers descriptions that a shorter listed
// description prefixes. These parse without error today, so the assertions are
// on the parsed fields rather than on the absence of an error.
func TestParseTOCPrefixShadowing(t *testing.T) {
	tests := []struct {
		line                               string
		description, namespace, tag, owner string
		class                              Class
	}{
		{
			"3001; 2616 17000 OPERATOR CLASS public gin_trgm_ops shard_admin",
			"OPERATOR CLASS", "public", "gin_trgm_ops", "shard_admin", PreData,
		},
		{
			"3002; 2753 17001 OPERATOR FAMILY public gin_trgm_family shard_admin",
			"OPERATOR FAMILY", "public", "gin_trgm_family", "shard_admin", PreData,
		},
		{
			"3003; 6106 17002 PUBLICATION TABLES IN SCHEMA - pgmigrate_pub shard_admin",
			"PUBLICATION TABLES IN SCHEMA", "-", "pgmigrate_pub", "shard_admin", Skipped,
		},
		{
			"3004; 1259 17003 MATERIALIZED VIEW public summary shard_admin",
			"MATERIALIZED VIEW", "public", "summary", "shard_admin", PreData,
		},
		{
			"3005; 0 0 MATERIALIZED VIEW DATA public summary shard_admin",
			"MATERIALIZED VIEW DATA", "public", "summary", "shard_admin", Skipped,
		},
		{
			"3006; 1259 17004 SEQUENCE OWNED BY public order_id_seq shard_admin",
			"SEQUENCE OWNED BY", "public", "order_id_seq", "shard_admin", PreData,
		},
		{
			"3007; 3381 17005 STATISTICS public order_stats shard_admin",
			"STATISTICS", "public", "order_stats", "shard_admin", PreData,
		},
	}
	for _, test := range tests {
		entry := parseOne(t, test.line)
		if entry.Description != test.description || entry.Namespace != test.namespace ||
			entry.Tag != test.tag || entry.Owner != test.owner {
			t.Errorf("parsed %q as description=%q namespace=%q tag=%q owner=%q",
				test.line, entry.Description, entry.Namespace, entry.Tag, entry.Owner)
		}
		if got := Classify(entry); got != test.class {
			t.Errorf("Classify(%s) = %v, want %v", test.description, got, test.class)
		}
	}
}

// TestParseTOCUnownedEntries covers the empty owner column pg_restore prints
// for objects PostgreSQL does not attribute to a role, which leaves the line
// ending in a space. Every extension is unowned, and so is the comment pg_dump
// emits for it from the extension's control file, so a parser that always read
// the last field as the owner silently moved the tag into it and left the tag
// empty or truncated.
func TestParseTOCUnownedEntries(t *testing.T) {
	tests := []struct {
		line                               string
		description, namespace, tag, owner string
	}{
		{
			"2; 3079 16385 EXTENSION - citext ",
			"EXTENSION", "-", "citext", "",
		},
		{
			"3559; 0 0 COMMENT - EXTENSION citext ",
			"COMMENT", "-", "EXTENSION citext", "",
		},
		{
			"3560; 0 0 SECURITY LABEL - EXTENSION plpgsql ",
			"SECURITY LABEL", "-", "EXTENSION plpgsql", "",
		},
		{
			"8; 2615 16384 SCHEMA - shard_schema shard_admin",
			"SCHEMA", "-", "shard_schema", "shard_admin",
		},
	}
	for _, test := range tests {
		entry := parseOne(t, test.line)
		if entry.Description != test.description || entry.Namespace != test.namespace ||
			entry.Tag != test.tag || entry.Owner != test.owner {
			t.Errorf("parsed %q as description=%q namespace=%q tag=%q owner=%q, want tag=%q owner=%q",
				test.line, entry.Description, entry.Namespace, entry.Tag, entry.Owner,
				test.tag, test.owner)
		}
	}
}

// TestDescriptionEndMatchesLongestDescription requires every known description
// to be recognized in full. Fifteen descriptions are word prefixes of a longer
// one, so this fails if matching ever stops being longest-first or if a new
// description shadows an existing one.
func TestDescriptionEndMatchesLongestDescription(t *testing.T) {
	for _, description := range archiveDescriptions {
		words := strings.Fields(description)
		fields := append(append([]string(nil), words...), "public", "thing", "owner")
		if got := descriptionEnd(fields); got != len(words) {
			t.Errorf("descriptionEnd(%q ...) = %d, want %d", description, got, len(words))
		}
	}
	if descriptionEnd([]string{"NOSUCHTHING", "public", "thing", "owner"}) != 0 {
		t.Error("descriptionEnd accepted an unknown description")
	}
	if descriptionEnd(nil) != 0 {
		t.Error("descriptionEnd accepted an empty entry")
	}
}

// TestParseTOCUnknownDescriptionMessage requires an unparseable entry to fail
// closed with an actionable message rather than only a line number.
func TestParseTOCUnknownDescriptionMessage(t *testing.T) {
	line := "42; 1259 84 NOT A REAL DESCRIPTION public thing owner"
	_, err := ParseTOC([]byte(line + "\n"))
	if err == nil {
		t.Fatal("unknown description was accepted")
	}
	for _, want := range []string{"line 1", line} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q omits %q", err.Error(), want)
		}
	}
}

func TestCommandErrorUnwrap(t *testing.T) {
	err := &CommandError{Command: "pg_dump", Output: "diagnostic", Err: exec.ErrNotFound}
	if !errors.Is(err, exec.ErrNotFound) || !strings.Contains(err.Error(), "diagnostic") {
		t.Fatalf("lost command failure details: %v", err)
	}
}

func TestDeferredExcludesManagedForeignKeys(t *testing.T) {
	entries := []TOCEntry{
		{Description: "TRIGGER", Raw: "1; trigger"},
		{Description: "RULE", Raw: "2; rule"},
		{Description: "COMMENT", Raw: "3; comment"},
		{Description: "FK CONSTRAINT", Raw: "4; fk"},
	}
	list := string(UseList(entries, func(entry TOCEntry) bool {
		return Classify(entry) == DeferredPostData && entry.Description != "FK CONSTRAINT"
	}))
	if strings.Contains(list, "\n4; fk") || !strings.Contains(list, "1; trigger") ||
		!strings.Contains(list, "2; rule") || !strings.Contains(list, "3; comment") {
		t.Fatalf("deferred list:\n%s", list)
	}
}

func TestFilterTOCExcludesUnselectedObjects(t *testing.T) {
	entries := []TOCEntry{
		{DumpID: 1, ObjectOID: 10, Description: "TABLE", Tag: "included"},
		{DumpID: 2, ObjectOID: 20, Description: "TABLE", Tag: "excluded"},
		{DumpID: 3, ObjectOID: 30, Description: "SEQUENCE", Tag: "included_id_seq"},
		{DumpID: 4, ObjectOID: 40, Description: "TYPE", Tag: "needed_type"},
	}
	got := FilterTOC(entries, map[int64]bool{10: true, 30: true, 40: true}, nil)
	if len(got) != 3 {
		t.Fatalf("filtered entries: %#v", got)
	}
	for _, entry := range got {
		if entry.ObjectOID == 20 {
			t.Fatal("excluded table survived TOC filter")
		}
	}
}

// archiveOrder is pg_dump's own listing order for a database whose extensions
// live in a schema the dump has to create. Schemas precede the extensions that
// name them in CREATE EXTENSION ... WITH SCHEMA, which precede the types and
// tables that use them. Dump IDs are deliberately not ascending: pg_dump
// assigns them before its dependency sort, so listing position is the only
// ordering signal in the archive.
var archiveOrder = []TOCEntry{
	{DumpID: 9, CatalogOID: 2615, ObjectOID: 2200, Description: "SCHEMA", Namespace: "-", Tag: "public", Raw: "9; 2615 2200 SCHEMA - public root"},
	{DumpID: 10, CatalogOID: 2615, ObjectOID: 16409, Description: "SCHEMA", Namespace: "-", Tag: "shard_schema", Raw: "10; 2615 16409 SCHEMA - shard_schema shard_admin"},
	{DumpID: 2, CatalogOID: 3079, ObjectOID: 16410, Description: "EXTENSION", Namespace: "-", Tag: "btree_gin", Raw: "2; 3079 16410 EXTENSION - btree_gin "},
	{DumpID: 4, CatalogOID: 3079, ObjectOID: 16412, Description: "EXTENSION", Namespace: "-", Tag: "unaccent", Raw: "4; 3079 16412 EXTENSION - unaccent "},
	{DumpID: 1077, CatalogOID: 1247, ObjectOID: 329389, Description: "TYPE", Namespace: "shard_schema", Tag: "campaign_status", Raw: "1077; 1247 329389 TYPE shard_schema campaign_status shard_admin"},
	{DumpID: 1200, CatalogOID: 1259, ObjectOID: 329400, Description: "TABLE", Namespace: "shard_schema", Tag: "message_history", Raw: "1200; 1259 329400 TABLE shard_schema message_history shard_admin"},
	{DumpID: 1300, CatalogOID: 1259, ObjectOID: 329500, Description: "INDEX", Namespace: "shard_schema", Tag: "message_history_idx", Raw: "1300; 1259 329500 INDEX shard_schema message_history_idx shard_admin"},
}

// activeUseListTags returns the tags of the entries a use-list leaves enabled,
// in the order pg_restore will apply them. Commented lines are skipped.
func activeUseListTags(t *testing.T, list []byte, entries []TOCEntry) []string {
	t.Helper()
	byRaw := make(map[string]string, len(entries))
	for _, entry := range entries {
		byRaw[entry.Raw] = entry.Tag
	}
	var tags []string
	for _, line := range strings.Split(strings.TrimRight(string(list), "\n"), "\n") {
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}
		tag, ok := byRaw[line]
		if !ok {
			t.Fatalf("use-list line %q matches no archive entry", line)
		}
		tags = append(tags, tag)
	}
	return tags
}

// TestRestorePreDataPreservesArchiveOrder pins the pre-data use-list to
// pg_dump's dependency order. pg_restore applies --use-list entries in file
// order and restores the pre-data section serially even under --jobs, so any
// reordering has to reproduce pg_dump's topological sort. Hoisting extensions
// to the front did not: it moved CREATE EXTENSION ... WITH SCHEMA shard_schema
// above the CREATE SCHEMA shard_schema that the statement requires, which
// aborted the schema phase of the first managed-cloud migration.
func TestRestorePreDataPreservesArchiveOrder(t *testing.T) {
	useList := filepath.Join(t.TempDir(), "predata.list")
	service := Service{Tools: Tools{Restore: "/usr/bin/true"}}
	if err := service.RestorePreData(context.Background(),
		"postgresql://unused", "unused.dump", useList, archiveOrder); err != nil {
		t.Fatal(err)
	}
	list, err := os.ReadFile(useList)
	if err != nil {
		t.Fatal(err)
	}
	got := activeUseListTags(t, list, archiveOrder)
	want := []string{"public", "shard_schema", "btree_gin", "unaccent", "campaign_status", "message_history"}
	if !slices.Equal(got, want) {
		t.Errorf("pre-data restore order = %v, want %v", got, want)
	}
}

// TestFilterTOCPreservesArchiveOrder requires filtering to be order-preserving.
// Removing entries from a topological order leaves one, so the surviving
// entries stay restorable without pgmigrate knowing any dependency itself.
func TestFilterTOCPreservesArchiveOrder(t *testing.T) {
	dumpIDs := make(map[int64]bool, len(archiveOrder))
	var want []string
	for _, entry := range archiveOrder {
		if entry.Tag == "public" {
			continue
		}
		dumpIDs[entry.DumpID] = true
		want = append(want, entry.Tag)
	}
	var got []string
	for _, entry := range FilterTOC(archiveOrder, nil, dumpIDs) {
		got = append(got, entry.Tag)
	}
	if !slices.Equal(got, want) {
		t.Errorf("filtered order = %v, want %v", got, want)
	}
}
