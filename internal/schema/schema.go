// Package schema creates the target's pre-data schema while retaining indexes
// and constraints for later construction.
package schema

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/tgross/pgmigrate/internal/postgres"
)

// Tools names the PostgreSQL client programs. Empty values use PATH defaults.
type Tools struct {
	Dump    string
	Restore string
}

// QualifiedName is an exact schema-qualified relation name.
type QualifiedName struct{ Schema, Name string }

// CatalogObject is an exact pg_restore TOC identity.
type CatalogObject struct {
	CatalogOID int64
	ObjectOID  int64
}

// DumpSelection contains selected tables and cataloged dependencies.
type DumpSelection struct {
	Tables             []QualifiedName
	DependentRelations []QualifiedName
	// Partitions are the partition descendants of the selected partitioned
	// tables. They are not copied in their own right — reading and writing the
	// partitioned parent reaches all of them — but they must be created and
	// attached on the target, or the parent has no partitions and rejects every
	// row written to it.
	Partitions []QualifiedName
	Extensions []string
	Objects    []CatalogObject
}

// CommandError preserves diagnostics emitted by a PostgreSQL client program.
type CommandError struct {
	Command string
	Args    []string
	Output  string
	Err     error
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("%s failed: %v: %s", e.Command, e.Err, strings.TrimSpace(e.Output))
}
func (e *CommandError) Unwrap() error { return e.Err }

// TOCEntry is one pg_restore archive table-of-contents entry.
type TOCEntry struct {
	DumpID, CatalogOID, ObjectOID      int64
	Description, Namespace, Tag, Owner string
	Raw                                string
}

// Class determines when an archive entry is restored.
type Class uint8

const (
	PreData Class = iota
	ManagedIndex
	ManagedConstraint
	DeferredPostData
	Skipped
)

// ParseTOC parses pg_restore --list output. It tolerates whitespace, comments,
// old archives with a '-' namespace, and object names containing spaces.
func ParseTOC(data []byte) ([]TOCEntry, error) {
	var result []TOCEntry
	s := bufio.NewScanner(bytes.NewReader(data))
	s.Buffer(make([]byte, 4096), 4<<20)
	for line := 1; s.Scan(); line++ {
		raw := s.Text()
		value := strings.TrimSpace(raw)
		if value == "" || strings.HasPrefix(value, ";") {
			continue
		}
		semi := strings.IndexByte(value, ';')
		if semi < 1 {
			return nil, fmt.Errorf("TOC line %d: missing semicolon", line)
		}
		id, err := strconv.ParseInt(strings.TrimSpace(value[:semi]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("TOC line %d dump ID: %w", line, err)
		}
		fields := strings.Fields(strings.TrimSpace(value[semi+1:]))
		if len(fields) < 4 {
			return nil, fmt.Errorf("TOC line %d: incomplete entry", line)
		}
		catalog, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("TOC line %d catalog OID: %w", line, err)
		}
		object, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("TOC line %d object OID: %w", line, err)
		}
		descEnd := descriptionEnd(fields[2:])
		if descEnd == 0 {
			return nil, fmt.Errorf(
				"TOC line %d: unrecognized object description in entry %q; pgmigrate cannot "+
					"determine where the description ends and the schema name begins", line, value,
			)
		}
		// pg_restore prints an empty owner column, leaving the line ending in a
		// space, for objects PostgreSQL does not attribute to a role. Every
		// EXTENSION is unowned, as is the COMMENT pg_dump emits for it from the
		// extension's control file, so taking the last field as the owner
		// unconditionally moved the tag into it.
		unowned := strings.HasSuffix(raw, " ")
		required := 2 + descEnd + 2
		if unowned {
			required--
		}
		if len(fields) < required {
			return nil, fmt.Errorf("TOC line %d: entry %q is missing a schema, tag, or owner field", line, value)
		}
		offset := 2
		desc := strings.Join(fields[offset:offset+descEnd], " ")
		offset += descEnd
		namespace := fields[offset]
		tagEnd, owner := len(fields), ""
		if !unowned {
			tagEnd--
			owner = fields[tagEnd]
		}
		tag := strings.Join(fields[offset+1:tagEnd], " ")
		result = append(result, TOCEntry{id, catalog, object, desc, namespace, tag, owner, raw})
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("scan TOC: %w", err)
	}
	return result, nil
}

// archiveDescriptions is every archive-entry description pg_dump can write,
// derived from the ARCHIVE_OPTS .description literals in src/bin/pg_dump for
// REL_16_STABLE, REL_17_STABLE, and REL_18_STABLE. Version-specific spellings
// are all present because one binary reads archives from any supported major:
// BLOB is PostgreSQL 16 and became BLOB METADATA in 17, SUBSCRIPTION TABLE
// arrived in 17, and STATISTICS DATA in 18. LARGE OBJECT and LARGE OBJECTS are
// deliberately absent; they occur only in entry tags, never as descriptions.
var archiveDescriptions = []string{
	"ACCESS METHOD",
	"ACL",
	"AGGREGATE",
	"BLOB",
	"BLOB METADATA",
	"BLOBS",
	"CAST",
	"CHECK CONSTRAINT",
	"COLLATION",
	"COMMENT",
	"CONSTRAINT",
	"CONVERSION",
	"DATABASE",
	"DATABASE PROPERTIES",
	"DEFAULT",
	"DEFAULT ACL",
	"DOMAIN",
	"ENCODING",
	"EVENT TRIGGER",
	"EXTENSION",
	"FK CONSTRAINT",
	"FOREIGN DATA WRAPPER",
	"FOREIGN TABLE",
	"FUNCTION",
	"INDEX",
	"INDEX ATTACH",
	"MATERIALIZED VIEW",
	"MATERIALIZED VIEW DATA",
	"OPERATOR",
	"OPERATOR CLASS",
	"OPERATOR FAMILY",
	"POLICY",
	"PROCEDURAL LANGUAGE",
	"PROCEDURE",
	"PUBLICATION",
	"PUBLICATION TABLE",
	"PUBLICATION TABLES IN SCHEMA",
	"ROW SECURITY",
	"RULE",
	"SCHEMA",
	"SEARCHPATH",
	"SECURITY LABEL",
	"SEQUENCE",
	"SEQUENCE OWNED BY",
	"SEQUENCE SET",
	"SERVER",
	"SHELL TYPE",
	"STATISTICS",
	"STATISTICS DATA",
	"STDSTRINGS",
	"SUBSCRIPTION",
	"SUBSCRIPTION TABLE",
	"TABLE",
	"TABLE ATTACH",
	"TABLE DATA",
	"TEXT SEARCH CONFIGURATION",
	"TEXT SEARCH DICTIONARY",
	"TEXT SEARCH PARSER",
	"TEXT SEARCH TEMPLATE",
	"TRANSFORM",
	"TRIGGER",
	"TYPE",
	"USER MAPPING",
	"VIEW",
	"pg_largeobject",
}

// descriptionWords indexes archiveDescriptions by word count so that matching
// is longest-first regardless of literal order. Fifteen descriptions are word
// prefixes of a longer one, so a shortest- or declaration-order match would
// silently consume the schema name as part of the description.
var descriptionWords, longestDescription = func() (map[string]int, int) {
	index := make(map[string]int, len(archiveDescriptions))
	longest := 0
	for _, description := range archiveDescriptions {
		words := len(strings.Fields(description))
		index[description] = words
		if words > longest {
			longest = words
		}
	}
	return index, longest
}()

// descriptionEnd returns the number of leading fields that form the entry's
// description, or zero when no known description matches.
func descriptionEnd(fields []string) int {
	words := min(longestDescription, len(fields))
	for ; words >= 1; words-- {
		if descriptionWords[strings.Join(fields[:words], " ")] == words {
			return words
		}
	}
	return 0
}

// Classify separates objects whose construction is managed by indexbuild.
func Classify(e TOCEntry) Class {
	switch e.Description {
	case "PUBLICATION", "PUBLICATION TABLE", "PUBLICATION TABLES IN SCHEMA",
		"SUBSCRIPTION", "SUBSCRIPTION TABLE":
		// Replication objects are managed by setup, and a source subscription
		// recreated on the target would start consuming an unrelated stream.
		return Skipped
	case "TABLE DATA", "SEQUENCE SET", "MATERIALIZED VIEW DATA",
		"BLOB", "BLOB METADATA", "BLOBS", "pg_largeobject", "STATISTICS DATA":
		// --schema-only suppresses every data-bearing entry. Should one reach
		// here, base copy, CDC, and cutover sequence synchronization own that
		// data; restoring it from the archive would conflict with them.
		return Skipped
	case "INDEX", "INDEX ATTACH":
		return ManagedIndex
	case "CONSTRAINT", "CHECK CONSTRAINT":
		return ManagedConstraint
	case "COMMENT":
		if strings.HasPrefix(e.Tag, "EXTENSION ") {
			// pg_dump emits a comment for every extension because CREATE
			// EXTENSION copies one out of the extension's control file. The
			// target sets its own on CREATE EXTENSION, so the source's copy is
			// not user data and two providers shipping different extension
			// versions must not read as a divergence.
			return Skipped
		}
		return DeferredPostData
	case "FK CONSTRAINT", "TRIGGER", "RULE", "POLICY":
		return DeferredPostData
	default:
		return PreData
	}
}

// DeferredMarker durably records one restored archive object. *state.Store
// satisfies this contract without schema package coupling.
type DeferredMarker interface {
	StepCompleted(context.Context, string) (bool, error)
	CompleteStep(context.Context, string, string) error
}

// DeferredStatus is returned by an exact target catalog inspector.
type DeferredStatus struct {
	// Exists reports that the target holds the object the entry names.
	Exists bool
	// Diverged reports that the target's object provably differs from the
	// source's. An inspector sets it only where the two can be compared
	// soundly. Server-deparsed SQL cannot: its text depends on the reading
	// session's search_path and on the stored parse tree, so the same object
	// renders differently on two servers.
	Diverged bool
	// Definition is the target's own rendering, carried for diagnostics.
	Definition string
}

// DeferredInspector checks trigger/rule/comment identity and exact definition.
type DeferredInspector func(context.Context, *pgx.Conn, TOCEntry) (DeferredStatus, error)

// DeferredCollisionError refuses a post-data object that the target does not
// hold as pgmigrate restored it.
type DeferredCollisionError struct {
	Entry  TOCEntry
	Reason string
	Have   string
}

func (e *DeferredCollisionError) Error() string {
	message := fmt.Sprintf("deferred object %s for %s %s.%s",
		e.Reason, e.Entry.Description, e.Entry.Namespace, e.Entry.Tag)
	if e.Have != "" {
		message += fmt.Sprintf(": have %q", e.Have)
	}
	return message
}

// RestoreDeferred restores post-data one object at a time. A target catalog
// match closes the crash-after-restore marker gap; mismatches are refused.
// FK entries are excluded because indexbuild owns their exact validation.
func (s Service) RestoreDeferred(ctx context.Context, targetURI, archive, useList string, entries []TOCEntry, extraArgs ...string) error {
	if s.DeferredMarkers == nil || s.InspectDeferred == nil {
		return errors.New("deferred markers and exact inspector are required")
	}
	conn, err := pgx.Connect(ctx, targetURI)
	if err != nil {
		return fmt.Errorf("connect deferred restore target: %w", err)
	}
	defer conn.Close(context.Background())
	if err := postgres.PinSearchPath(ctx, conn); err != nil {
		return err
	}
	for _, entry := range entries {
		if Classify(entry) != DeferredPostData || entry.Description == "FK CONSTRAINT" {
			continue
		}
		marker := fmt.Sprintf("schema:deferred:%d", entry.DumpID)
		done, err := s.DeferredMarkers.StepCompleted(ctx, marker)
		if err != nil {
			return err
		}
		status, err := s.InspectDeferred(ctx, conn, entry)
		if err != nil {
			return err
		}
		if done {
			if !status.Exists {
				return &DeferredCollisionError{Entry: entry, Reason: "vanished after pgmigrate restored it"}
			}
			if status.Diverged {
				return &DeferredCollisionError{Entry: entry, Reason: "diverged since pgmigrate restored it", Have: status.Definition}
			}
			continue
		}
		if status.Exists && status.Diverged {
			return &DeferredCollisionError{Entry: entry, Reason: "collision", Have: status.Definition}
		}
		if !status.Exists {
			if err := os.WriteFile(useList, UseList(entries, func(e TOCEntry) bool {
				return e.DumpID == entry.DumpID
			}), 0o600); err != nil {
				return fmt.Errorf("write deferred restore use-list: %w", err)
			}
			args := []string{"--dbname", targetURI, "--no-owner", "--no-privileges", "--exit-on-error", "--use-list", useList, archive}
			args = append(args, extraArgs...)
			if err := run(ctx, s.tool(s.Tools.Restore, "pg_restore"), args...); err != nil {
				return err
			}
			status, err = s.InspectDeferred(ctx, conn, entry)
			if err != nil {
				return err
			}
			if !status.Exists || status.Diverged {
				return fmt.Errorf("deferred object %d did not restore with expected definition", entry.DumpID)
			}
		}
		if err := s.DeferredMarkers.CompleteStep(ctx, marker, entry.Description+" "+entry.Namespace+"."+entry.Tag); err != nil {
			return err
		}
	}
	return nil
}

// UseList returns a pg_restore use-list containing only entries accepted by keep.
func UseList(entries []TOCEntry, keep func(TOCEntry) bool) []byte {
	var b strings.Builder
	for _, entry := range entries {
		if keep(entry) {
			b.WriteString(entry.Raw)
		} else {
			b.WriteByte(';')
			b.WriteString(entry.Raw)
		}
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// FilterTOC retains only the exact catalog objects and dependency-only archive
// entries selected by preflight. Object OIDs cover tables, sequences, types,
// functions, extensions, and table-owned post-data; dump IDs cover entries
// without a catalog OID. No implicit schema-wide objects are retained.
//
// Entries keep their archive order. pg_dump lists them in the topological order
// produced by its own dependency graph, and dropping entries from a topological
// order leaves one, so filtering alone can never make the list unrestorable.
// Reordering can: pgmigrate once hoisted every extension to the front, which
// lifted CREATE EXTENSION ... WITH SCHEMA above the CREATE SCHEMA it requires.
func FilterTOC(entries []TOCEntry, objectOIDs map[int64]bool, dumpIDs map[int64]bool) []TOCEntry {
	result := make([]TOCEntry, 0, len(entries))
	for _, entry := range entries {
		if (entry.ObjectOID != 0 && objectOIDs[entry.ObjectOID]) || dumpIDs[entry.DumpID] {
			result = append(result, entry)
		}
	}
	return result
}

// Service owns schema archive creation and restoration.
type Service struct {
	Tools           Tools
	DeferredMarkers DeferredMarker
	InspectDeferred DeferredInspector
}

func (s Service) tool(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

// Dump creates a custom-format, no-data archive using an exported snapshot.
func (s Service) Dump(ctx context.Context, sourceURI, snapshot, archive string, extraArgs ...string) error {
	if sourceURI == "" || snapshot == "" || archive == "" {
		return errors.New("source URI, snapshot, and archive are required")
	}
	if err := os.MkdirAll(filepath.Dir(archive), 0o750); err != nil {
		return fmt.Errorf("create dump directory: %w", err)
	}
	args := []string{"--dbname", sourceURI, "--format=custom", "--schema-only", "--no-owner", "--no-privileges", "--snapshot", snapshot, "--file", archive}
	args = append(args, extraArgs...)
	return run(ctx, s.tool(s.Tools.Dump, "pg_dump"), args...)
}

// List reads and parses an archive's table of contents.
func (s Service) List(ctx context.Context, archive string) ([]TOCEntry, error) {
	output, err := output(ctx, s.tool(s.Tools.Restore, "pg_restore"), "--list", archive)
	if err != nil {
		return nil, err
	}
	return ParseTOC(output)
}

// RestorePreData writes a use-list and restores all objects not delegated to
// index/constraint construction. pg_restore applies a use-list in file order
// and restores pre-data serially even under --jobs, so the list keeps pg_dump's
// dependency order unchanged.
func (s Service) RestorePreData(ctx context.Context, targetURI, archive, useList string, entries []TOCEntry, extraArgs ...string) error {
	if err := os.WriteFile(useList, UseList(entries, func(e TOCEntry) bool { return Classify(e) == PreData }), 0o600); err != nil {
		return fmt.Errorf("write restore use-list: %w", err)
	}
	args := []string{"--dbname", targetURI, "--no-owner", "--no-privileges", "--exit-on-error", "--use-list", useList, archive}
	args = append(args, extraArgs...)
	return run(ctx, s.tool(s.Tools.Restore, "pg_restore"), args...)
}

// Clean removes only objects named by a prior migration archive. The caller is
// responsible for validating migration identity and target ownership first.
func (s Service) Clean(ctx context.Context, targetURI, archive string) error {
	if targetURI == "" || archive == "" {
		return errors.New("target URI and archive are required")
	}
	sqlData, err := output(ctx, s.tool(s.Tools.Restore, "pg_restore"),
		"--clean", "--if-exists", "--schema-only", "--no-owner", "--no-privileges", "--file=-", archive)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, targetURI)
	if err != nil {
		return fmt.Errorf("connect target archive cleanup: %w", err)
	}
	defer conn.Close(context.Background())
	for _, raw := range strings.Split(string(sqlData), ";\n") {
		lines := strings.Split(raw, "\n")
		kept := lines[:0]
		for _, line := range lines {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				kept = append(kept, line)
			}
		}
		statement := strings.TrimSpace(strings.Join(kept, "\n"))
		upper := strings.ToUpper(statement)
		if !(strings.HasPrefix(upper, "DROP ") ||
			strings.HasPrefix(upper, "ALTER ") && strings.Contains(upper, " DROP ")) {
			continue
		}
		if _, err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("execute archive cleanup statement: %w", err)
		}
	}
	return nil
}

func run(ctx context.Context, name string, args ...string) error {
	_, err := output(ctx, name, args...)
	return err
}

func output(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var combined bytes.Buffer
	cmd.Stdout, cmd.Stderr = &combined, &combined
	err := cmd.Run()
	if err != nil {
		return nil, &CommandError{Command: name, Args: append([]string(nil), args...), Output: combined.String(), Err: err}
	}
	return combined.Bytes(), nil
}
