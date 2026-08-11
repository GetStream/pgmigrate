// Package collation decides whether two databases sort and compare text the same
// way, which is a question about meaning rather than about spelling.
//
// # Why canonicalization is the whole problem
//
// One locale reaches pgmigrate under several names. RDS reports en_US.UTF-8 and
// Cloud SQL reports en_US.UTF8 for what glibc treats as the same locale, so a
// literal string comparison would refuse a migration that is entirely safe, and
// operators would learn to pass the escape hatch by reflex. Folding too much is
// the opposite failure: silence about a real ordering change.
//
// Canonicalization here is therefore deliberately narrow. It folds only aliases
// that name one locale on the systems that produce them:
//
//   - codeset spelling, so UTF-8, utf-8, utf8, and UTF8 agree;
//   - letter case, which locale lookup ignores;
//   - POSIX, which is glibc's alias for C.
//
// It does not touch the locale portion otherwise. en-US and en_US stay distinct
// because they are different names in different naming schemes rather than two
// spellings of one name, and C.UTF8 stays distinct from C because a codeset is
// part of what a locale is: C sorts bytes over SQL_ASCII semantics, C.UTF8 sorts
// bytes over UTF-8 text.
package collation

import (
	"fmt"
	"strings"
)

// Locale providers, as pg_database.datlocprovider reports them.
const (
	ProviderLibc    = "c"
	ProviderICU     = "i"
	ProviderBuiltin = "b"
)

// Settings is everything about a database that decides how it collates text.
type Settings struct {
	// Collate and CType are datcollate and datctype: the LC_COLLATE and LC_CTYPE
	// the database was created with. Both matter, because LC_COLLATE decides
	// ordering and LC_CTYPE decides character classification, and upper(), lower()
	// and regex character classes follow the latter.
	Collate string
	CType   string
	// Locale is the provider's own locale, which PostgreSQL keeps in a column that
	// was renamed between the releases pgmigrate supports: daticulocale in 16,
	// datlocale in 17 and later. It is set for the ICU and builtin providers,
	// where it and not datcollate is what actually collates.
	Locale string
	// Provider is datlocprovider.
	Provider string
	// Version is datcollversion, the provider's version of the collation
	// definition. The same locale name reorders between provider versions, which
	// is why PostgreSQL records it at all.
	Version string
}

// ProviderName renders a provider for people rather than for the catalog.
func ProviderName(code string) string {
	switch code {
	case ProviderLibc:
		return "libc"
	case ProviderICU:
		return "icu"
	case ProviderBuiltin:
		return "builtin"
	case "":
		return "unknown"
	default:
		return code
	}
}

// Describe renders the locale identity of a database the way an operator would
// need to see it to recognize their own configuration.
func (s Settings) Describe() string {
	locale := s.Collate
	if s.CType != s.Collate {
		locale += " / " + s.CType
	}
	if s.Locale != "" {
		locale += " (" + s.Locale + ")"
	}
	return fmt.Sprintf("%s [%s]", locale, ProviderName(s.Provider))
}

// Canonical reduces a locale name to the form that decides equality, folding
// only the aliases documented on the package.
//
// The parts are handled separately because they carry different risks. A codeset
// is a name for an encoding and its punctuation is decoration, so UTF-8 and utf8
// fold. The locale portion is a name whose punctuation is meaningful, so nothing
// there is rewritten except the POSIX alias. A @modifier selects a collation
// variant, for example @euro or ICU's keyword syntax, and survives untouched
// apart from case.
func Canonical(locale string) string {
	value := strings.ToLower(strings.TrimSpace(locale))
	modifier := ""
	if at := strings.IndexByte(value, '@'); at >= 0 {
		value, modifier = value[:at], value[at:]
	}
	name, codeset := value, ""
	if dot := strings.IndexByte(value, '.'); dot >= 0 {
		name, codeset = value[:dot], value[dot+1:]
	}
	// glibc documents POSIX as an alias of C, and PostgreSQL passes either
	// through to setlocale unchanged, so the two name one collation.
	if name == "posix" {
		name = "c"
	}
	if codeset == "" {
		return name + modifier
	}
	// Codeset punctuation is not part of the encoding's identity: iso-8859-1 and
	// iso88591 name one encoding, as do UTF-8 and utf8.
	codeset = strings.NewReplacer("-", "", "_", "").Replace(codeset)
	return name + "." + codeset + modifier
}

// Kind says which aspect of two databases' collation differs. The three are kept
// apart because they carry different consequences and are acted on differently.
type Kind string

const (
	// KindProvider is a different collation library: libc, ICU, or builtin.
	KindProvider Kind = "provider"
	// KindLocale is a different locale, after canonicalization.
	KindLocale Kind = "locale"
	// KindVersion is one locale whose definition the two providers version
	// differently, which reorders some strings without renaming anything.
	KindVersion Kind = "version"
)

// Difference is one way two databases disagree, carrying the raw values each side
// reported so a message can show operators what they actually configured rather
// than what canonicalization made of it.
type Difference struct {
	Kind   Kind
	Source string
	Target string
}

// Compare reports the distinct ways two databases collate text differently.
//
// A version difference is reported only when the locales agree, because a
// differing locale already means differing ordering and naming the version too
// would describe one problem twice.
func Compare(source, target Settings) []Difference {
	var differences []Difference
	if source.Provider != target.Provider {
		differences = append(differences, Difference{
			Kind:   KindProvider,
			Source: ProviderName(source.Provider),
			Target: ProviderName(target.Provider),
		})
	}
	switch {
	case !sameLocale(source, target):
		differences = append(differences, Difference{
			Kind:   KindLocale,
			Source: source.Describe(),
			Target: target.Describe(),
		})
	case source.Version != target.Version && source.Version != "" && target.Version != "":
		differences = append(differences, Difference{
			Kind:   KindVersion,
			Source: source.Version,
			Target: target.Version,
		})
	}
	return differences
}

func sameLocale(source, target Settings) bool {
	if Canonical(source.Collate) != Canonical(target.Collate) ||
		Canonical(source.CType) != Canonical(target.CType) {
		return false
	}
	if !comparableProviderLocale(source, target) {
		return true
	}
	return Canonical(source.Locale) == Canonical(target.Locale)
}

// comparableProviderLocale reports whether the provider locales of two databases
// describe the same thing and can therefore be compared to each other.
//
// They do only when both use the same non-libc provider. There the provider
// locale is the operative one and datcollate is beside the point: PostgreSQL 18
// reports datcollate en_US.utf8 for a builtin-provider database whose locale is
// C.UTF-8, so two databases agreeing on datcollate can still collate
// differently.
//
// Under libc the field is unset on every release pgmigrate supports and
// datcollate is authoritative, so comparing it can only ever produce a false
// difference should a later release begin populating it. When the providers
// themselves differ, that is already reported on its own and the locale
// comparison rests on datcollate, which both providers do report.
func comparableProviderLocale(source, target Settings) bool {
	return source.Provider == target.Provider &&
		(source.Provider == ProviderICU || source.Provider == ProviderBuiltin)
}
