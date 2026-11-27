package v1alpha1

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
)

//go:embed arc-version
var pinnedVersion string

// PinnedVersion is the ARC release these types were copied from. Renovate moves
// it, and the arc-compat CI job checks the types against that release's CRDs.
func PinnedVersion() string { return strings.TrimSpace(pinnedVersion) }

// SupportedMinors is the pinned minor and the one before it, e.g. 0.15 and 0.14.
func SupportedMinors() []string {
	major, minor, ok := majorMinor(PinnedVersion())
	if !ok {
		return nil
	}
	out := []string{fmt.Sprintf("%d.%d", major, minor)}
	if minor > 0 {
		out = append(out, fmt.Sprintf("%d.%d", major, minor-1))
	}
	return out
}

// Supported reports whether an ARC version falls in SupportedMinors. An
// unparseable version is not supported.
func Supported(version string) bool {
	major, minor, ok := majorMinor(version)
	if !ok {
		return false
	}
	want := fmt.Sprintf("%d.%d", major, minor)
	for _, m := range SupportedMinors() {
		if m == want {
			return true
		}
	}
	return false
}

func majorMinor(v string) (major, minor int, ok bool) {
	v = strings.TrimPrefix(strings.TrimSpace(v), "v")
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	var err error
	if major, err = strconv.Atoi(parts[0]); err != nil {
		return 0, 0, false
	}
	if minor, err = strconv.Atoi(parts[1]); err != nil {
		return 0, 0, false
	}
	return major, minor, true
}
