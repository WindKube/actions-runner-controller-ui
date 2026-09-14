package health

import (
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

// TestTrackerLogsTransitionsOnly is the whole point of this package: a source
// that has been broken for a week must not be a week of identical warn lines.
func TestTrackerLogsTransitionsOnly(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := zerolog.New(&buf).Level(zerolog.InfoLevel)

	h := New("poll")
	h.Fail(log, "boom")
	h.Fail(log, "boom")
	h.Fail(log, "boom")
	assert.Equal(t, 1, strings.Count(buf.String(), "source unavailable"),
		"identical failures must be logged once above debug")

	h.Fail(log, "different boom")
	assert.Equal(t, 2, strings.Count(buf.String(), "source unavailable"),
		"a changed failure mode must be logged")

	h.OK(log)
	h.OK(log)
	assert.Equal(t, 1, strings.Count(buf.String(), "source recovered"), "recovery must be logged once")
}

// TestDegradeIsNotUnavailable: a fleet with nineteen healthy listeners and one
// broken is not an outage, and logging it as one is how an operator learns to
// distrust the line.
func TestDegradeIsNotUnavailable(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := zerolog.New(&buf).Level(zerolog.InfoLevel)

	h := New("scrape")
	h.Degrade(log, "1 of 20 listeners unreachable")
	h.Degrade(log, "1 of 20 listeners unreachable")

	assert.Equal(t, 1, strings.Count(buf.String(), "source degraded"), "an unchanged degradation logs once")
	assert.NotContains(t, buf.String(), "source unavailable", "a partial answer is not an outage")
}

// TestVerbNamesTheAction keeps the debug lines readable when two pollers share
// one log stream.
func TestVerbNamesTheAction(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := zerolog.New(&buf).Level(zerolog.DebugLevel)

	h := New("scrape")
	h.OK(log)
	h.OK(log)

	assert.Contains(t, buf.String(), "scrape succeeded", "the verb must name what was done")
}
