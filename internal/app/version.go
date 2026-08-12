package app

import "runtime/debug"

// Version identifies the build, for the cutover report to record beside the
// migration it audits.
//
// A released binary reports the module version it was installed from, and one
// built from a checkout reports the pseudo-version the toolchain derives from the
// commit, marked dirty when the tree had uncommitted changes. Only a build with
// no stamped version at all falls back to the commit, or to admitting it has
// neither: a report claiming a version that never existed anywhere would be worse
// than one admitting it came from somebody's working copy.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return formatVersion("", "", false)
	}
	var revision string
	var modified bool
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	return formatVersion(info.Main.Version, revision, modified)
}

func formatVersion(version, revision string, modified bool) string {
	// The toolchain spells an unreleased build "(devel)", which reads as a value
	// nobody set rather than as a statement about the build.
	if version != "" && version != "(devel)" {
		return version
	}
	if revision == "" {
		return "devel"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	if modified {
		return "devel+" + revision + ".dirty"
	}
	return "devel+" + revision
}
