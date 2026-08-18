package version

import (
	"runtime"
	"strings"
	"testing"
)

func TestInfoString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Info
		want string
	}{
		{
			name: "release build abbreviates the commit",
			in: Info{
				Version:   "1.4.2",
				Commit:    "0123456789abcdef0123456789abcdef01234567",
				Date:      "2026-08-17T10:04:00Z",
				GoVersion: "go1.26.5",
				Platform:  "linux/arm64",
			},
			want: "arc-ui 1.4.2 (commit 0123456, built 2026-08-17T10:04:00Z, go1.26.5, linux/arm64)",
		},
		{
			// A commit shorter than shortCommitLen must not be sliced, or the
			// abbreviation panics on out-of-range instead of degrading.
			name: "commit shorter than the cutoff is left intact",
			in: Info{
				Version:   "dev",
				Commit:    "abc",
				Date:      "unknown",
				GoVersion: "go1.26.5",
				Platform:  "darwin/arm64",
			},
			want: "arc-ui dev (commit abc, built unknown, go1.26.5, darwin/arm64)",
		},
		{
			name: "commit exactly at the cutoff is left intact",
			in: Info{
				Version:   "0.1.0",
				Commit:    "0123456",
				Date:      "2026-01-02T03:04:05Z",
				GoVersion: "go1.26.5",
				Platform:  "linux/amd64",
			},
			want: "arc-ui 0.1.0 (commit 0123456, built 2026-01-02T03:04:05Z, go1.26.5, linux/amd64)",
		},
		{
			name: "zero value renders without panicking",
			in:   Info{},
			want: "arc-ui  (commit , built , , )",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.in.String(); got != tt.want {
				t.Errorf("String() =\n  %q\nwant\n  %q", got, tt.want)
			}
		})
	}
}

// Current is the seam the linker writes through, so assert it actually reads the
// package variables rather than hardcoding anything.
func TestCurrentReadsPackageVars(t *testing.T) {
	t.Parallel()

	got := Current()

	if got.Version != Version {
		t.Errorf("Version = %q, want %q", got.Version, Version)
	}
	if got.Commit != Commit {
		t.Errorf("Commit = %q, want %q", got.Commit, Commit)
	}
	if got.Date != Date {
		t.Errorf("Date = %q, want %q", got.Date, Date)
	}
	if got.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", got.GoVersion, runtime.Version())
	}
	if want := runtime.GOOS + "/" + runtime.GOARCH; got.Platform != want {
		t.Errorf("Platform = %q, want %q", got.Platform, want)
	}
}

// The defaults have to survive a build with no -ldflags, because that is what
// `go run ./cmd/arc-ui` and every local build produce.
func TestUninjectedDefaults(t *testing.T) {
	t.Parallel()

	line := Current().String()
	for _, want := range []string{"arc-ui dev", "commit unknown", "built unknown"} {
		if !strings.Contains(line, want) {
			t.Errorf("String() = %q, want it to contain %q", line, want)
		}
	}
}
