// Package logging builds the process logger. The logger is passed explicitly
// to whatever needs it rather than living in a package-level global.
package logging

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

// New returns a logger at the given level. Console format is for humans at a
// terminal; anything else gets structured JSON on stderr.
func New(level, format string) zerolog.Logger {
	lvl, err := zerolog.ParseLevel(strings.ToLower(level))
	if err != nil || lvl == zerolog.NoLevel {
		lvl = zerolog.InfoLevel
	}
	zerolog.TimeFieldFormat = time.RFC3339

	var out io.Writer = os.Stderr
	if format == "console" {
		out = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	}
	return zerolog.New(out).Level(lvl).With().Timestamp().Str("service", "arc-ui").Logger()
}
