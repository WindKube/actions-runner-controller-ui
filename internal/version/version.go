// Package version records the build identity of the arc-ui binary.
//
// The exported variables are placeholders that the release build overwrites at
// link time with `-ldflags "-X ..."` — see the build stage in the repository
// Dockerfile. Nothing here may import anything outside the standard library:
// the linker can only set string variables, so any richer build-info library
// would defeat the point.
package version

import (
	"fmt"
	"runtime"
)

// Set by the linker at build time. The defaults are what a plain `go build`
// produces, which is exactly the case a developer hits locally.
var (
	// Version is the released semantic version, without a leading "v".
	Version = "dev"
	// Commit is the full git SHA the binary was built from.
	Commit = "unknown"
	// Date is the build timestamp in RFC 3339 form.
	Date = "unknown"
)

// shortCommitLen is how much of the git SHA String reports: long enough to stay
// unambiguous in practice, short enough to read in a log line.
const shortCommitLen = 7

// Info is a flattened, printable view of a build identity.
//
// It exists so String can be tested. Reading runtime.Version and GOOS/GOARCH
// inside the formatting code would make its output depend on whichever toolchain
// and machine happened to run the test.
type Info struct {
	Version   string
	Commit    string
	Date      string
	GoVersion string
	Platform  string
}

// Current returns the build identity of this binary.
func Current() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		GoVersion: runtime.Version(),
		Platform:  runtime.GOOS + "/" + runtime.GOARCH,
	}
}

// String renders the identity as one line, abbreviating the commit SHA.
func (i Info) String() string {
	commit := i.Commit
	if len(commit) > shortCommitLen {
		commit = commit[:shortCommitLen]
	}

	return fmt.Sprintf("arc-ui %s (commit %s, built %s, %s, %s)",
		i.Version, commit, i.Date, i.GoVersion, i.Platform)
}
