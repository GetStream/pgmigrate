package collation

import (
	"strings"
	"testing"
)

// TestCanonicalFoldsTheAliasesThatNameOneLocale is the corpus that decides which
// migrations pgmigrate refuses. Each group holds spellings that must agree,
// because a false difference sends an operator to --allow-collation-change for
// nothing and teaches them to pass it without reading.
func TestCanonicalFoldsTheAliasesThatNameOneLocale(t *testing.T) {
	groups := [][]string{
		// The pair that motivated this: RDS and Cloud SQL name one glibc locale.
		{"en_US.UTF-8", "en_US.UTF8", "en_US.utf8", "en_us.Utf-8", " en_US.UTF-8 "},
		{"C", "c", "POSIX", "posix", " C "},
		{"C.UTF-8", "C.UTF8", "c.utf8", "POSIX.UTF-8"},
		{"de_DE.ISO-8859-1", "de_DE.iso88591", "de_DE.ISO_8859-1"},
		{"en_US.UTF-8@euro", "en_US.utf8@EURO"},
		// ICU locale names are only case-folded, which is enough for the case
		// difference a provider can introduce and nothing more.
		{"und-u-ks-level2", "UND-u-KS-level2"},
	}
	for _, group := range groups {
		want := Canonical(group[0])
		for _, spelling := range group[1:] {
			if got := Canonical(spelling); got != want {
				t.Errorf("Canonical(%q) = %q, want %q like %q", spelling, got, want, group[0])
			}
		}
	}
}

// TestCanonicalKeepsDistinctLocalesApart guards the other direction, where
// folding too much would let a real ordering change through in silence.
func TestCanonicalKeepsDistinctLocalesApart(t *testing.T) {
	pairs := [][2]string{
		{"en_US.UTF-8", "de_DE.UTF-8"},
		// A codeset is part of what a locale is: C sorts bytes with no notion of
		// text, C.UTF8 sorts bytes of UTF-8 text.
		{"C", "C.UTF-8"},
		{"POSIX", "C.UTF8"},
		// Different naming schemes rather than different spellings of one name.
		// glibc has no en-US and ICU has no en_US.UTF-8, so treating them as one
		// would be inventing a translation between providers.
		{"en-US", "en_US"},
		{"en_US.UTF-8", "en_US.UTF-8@euro"},
		{"en_US.UTF-8", "en_US.ISO-8859-1"},
		{"und-u-ks-level2", "und-u-ks-level1"},
		{"", "C"},
	}
	for _, pair := range pairs {
		if Canonical(pair[0]) == Canonical(pair[1]) {
			t.Errorf("Canonical folded %q and %q together as %q",
				pair[0], pair[1], Canonical(pair[0]))
		}
	}
}

func TestCanonicalRendersTheFoldedForm(t *testing.T) {
	tests := map[string]string{
		"en_US.UTF-8":       "en_us.utf8",
		"POSIX":             "c",
		"C.UTF-8":           "c.utf8",
		"en_US.UTF-8@euro":  "en_us.utf8@euro",
		"en-US":             "en-us",
		"  ":                "",
		"en_US.UTF-8.extra": "en_us.utf8.extra",
	}
	for input, want := range tests {
		if got := Canonical(input); got != want {
			t.Errorf("Canonical(%q) = %q, want %q", input, got, want)
		}
	}
}

func glibcSettings() Settings {
	return Settings{
		Collate:  "en_US.UTF-8",
		CType:    "en_US.UTF-8",
		Provider: ProviderLibc,
		Version:  "2.26",
	}
}

func TestCompareClassifiesEachDifference(t *testing.T) {
	source := glibcSettings()

	spelled := source
	spelled.Collate, spelled.CType = "en_US.UTF8", "en_US.UTF8"
	if differences := Compare(source, spelled); len(differences) != 0 {
		t.Fatalf("codeset spelling reported %+v", differences)
	}

	relocated := source
	relocated.Collate, relocated.CType = "de_DE.UTF-8", "de_DE.UTF-8"
	assertKinds(t, Compare(source, relocated), KindLocale)

	// LC_CTYPE decides character classification, so upper(), lower() and regex
	// classes follow it even where ordering is unaffected. A difference there is a
	// difference.
	classified := source
	classified.CType = "de_DE.UTF-8"
	assertKinds(t, Compare(source, classified), KindLocale)

	versioned := source
	versioned.Version = "2.19"
	assertKinds(t, Compare(source, versioned), KindVersion)

	// A differing locale already means differing ordering, so naming the version
	// as well would report one problem twice.
	both := relocated
	both.Version = "2.19"
	assertKinds(t, Compare(source, both), KindLocale)

	unknownVersion := source
	unknownVersion.Version = ""
	if differences := Compare(source, unknownVersion); len(differences) != 0 {
		t.Fatalf("unknown version reported %+v", differences)
	}

	icu := source
	icu.Provider = ProviderICU
	assertKinds(t, Compare(source, icu), KindProvider)
}

// TestCompareUsesTheProviderLocaleOnlyWhereItIsComparable covers the field whose
// meaning depends on the provider. Under ICU it is the operative locale, so it
// has to be compared; under libc datcollate is authoritative and the field has
// been populated differently across releases, so comparing it would manufacture a
// hard stop out of a catalog detail.
func TestCompareUsesTheProviderLocaleOnlyWhereItIsComparable(t *testing.T) {
	icu := Settings{
		Collate: "en_US.UTF-8", CType: "en_US.UTF-8",
		Locale: "en-US", Provider: ProviderICU, Version: "153.120.55",
	}
	reordered := icu
	reordered.Locale = "und-u-ks-level2"
	assertKinds(t, Compare(icu, reordered), KindLocale)
	if differences := Compare(icu, icu); len(differences) != 0 {
		t.Fatalf("identical ICU databases reported %+v", differences)
	}

	// The builtin provider is where ignoring the field would be worst: PostgreSQL
	// 18 reports datcollate en_US.utf8 for a builtin database whose locale is
	// C.UTF-8, so datcollate alone says these two agree.
	builtin := Settings{
		Collate: "en_US.utf8", CType: "en_US.utf8",
		Locale: "C.UTF-8", Provider: ProviderBuiltin,
	}
	other := builtin
	other.Locale = "C"
	assertKinds(t, Compare(builtin, other), KindLocale)

	libc := glibcSettings()
	populated := libc
	populated.Locale = "en_US.UTF-8"
	if differences := Compare(libc, populated); len(differences) != 0 {
		t.Fatalf("libc provider locale reported %+v", differences)
	}

	// Providers differing is its own finding, and the locale comparison then rests
	// on datcollate alone rather than on two fields that mean different things.
	mixed := libc
	mixed.Provider, mixed.Locale = ProviderICU, "en-US"
	assertKinds(t, Compare(libc, mixed), KindProvider)
}

func TestDescribeNamesEveryPartThatDecidesOrdering(t *testing.T) {
	settings := Settings{
		Collate: "en_US.UTF-8", CType: "de_DE.UTF-8",
		Locale: "en-US", Provider: ProviderICU,
	}
	described := settings.Describe()
	for _, want := range []string{"en_US.UTF-8", "de_DE.UTF-8", "en-US", "icu"} {
		if !strings.Contains(described, want) {
			t.Errorf("Describe() = %q, missing %q", described, want)
		}
	}
	// The common case has LC_COLLATE and LC_CTYPE equal, and repeating one value
	// twice reads as though something differs.
	settings = glibcSettings()
	if described := settings.Describe(); strings.Count(described, "en_US.UTF-8") != 1 {
		t.Errorf("Describe() = %q, want the shared locale named once", described)
	}
}

func TestProviderNameIsReadable(t *testing.T) {
	tests := map[string]string{
		ProviderLibc: "libc", ProviderICU: "icu", ProviderBuiltin: "builtin",
		"": "unknown", "x": "x",
	}
	for code, want := range tests {
		if got := ProviderName(code); got != want {
			t.Errorf("ProviderName(%q) = %q, want %q", code, got, want)
		}
	}
}

func assertKinds(t *testing.T, differences []Difference, want ...Kind) {
	t.Helper()
	if len(differences) != len(want) {
		t.Fatalf("differences = %+v, want kinds %v", differences, want)
	}
	for i, kind := range want {
		if differences[i].Kind != kind {
			t.Fatalf("differences = %+v, want kinds %v", differences, want)
		}
		if differences[i].Source == differences[i].Target {
			t.Errorf("%s difference reports the same value on both sides: %+v",
				kind, differences[i])
		}
	}
}
