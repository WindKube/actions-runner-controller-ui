package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/store"
	"arc-ui/internal/web"
)

// countingQueries answers the store contract with fixtures and counts how often
// Stats was asked, which is the whole point of the cache under test.
type countingQueries struct {
	stats      store.Stats
	statsErr   error
	statsCalls int

	failures      []store.FailureRecord
	failuresTotal int64
	failuresErr   error
	// gotSetName records the scope the last Failures call was made with.
	gotSetName string
	gotLimit   int
}

func (q *countingQueries) Series(context.Context, store.Scope, string, []store.Metric, store.Range) (map[store.Metric][]store.Point, error) {
	return nil, nil
}

func (q *countingQueries) Churn(context.Context, string, store.Range) ([]store.Point, []store.Point, error) {
	return nil, nil, nil
}

func (q *countingQueries) Throughput(context.Context, string, store.Range) ([]store.Point, []store.Point, error) {
	return nil, nil, nil
}

func (q *countingQueries) RepoConsumption(context.Context, store.Range) ([]store.RepoTotal, error) {
	return nil, nil
}

func (q *countingQueries) Failures(_ context.Context, setName string, _ store.Range, limit int) ([]store.FailureRecord, int64, error) {
	q.gotSetName, q.gotLimit = setName, limit
	return q.failures, q.failuresTotal, q.failuresErr
}

func (q *countingQueries) Stats(context.Context) (store.Stats, error) {
	q.statsCalls++
	return q.stats, q.statsErr
}

// clock is a hand-advanced time source.
type clock struct{ at time.Time }

func (c *clock) now() time.Time { return c.at }

func fixedStats() store.Stats {
	return store.Stats{
		Path:        "/data/arc-ui.db",
		SizeBytes:   12 * 1024 * 1024,
		Samples:     1234567,
		Jobs:        42,
		Phases:      7,
		ChurnEvents: 99,
		Rows:        1234715,
		Oldest:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
}

// COUNT(*) over samples scans an index — millions of rows once the hourly tier
// has a year in it — and the SSE stream re-renders the footer on every snapshot
// change, which is every scrape interval for every connected browser. Counting
// per render would make the cheapest panel on the page the most expensive.
func TestStatsIsNotRecountedWithinItsTTL(t *testing.T) {
	t.Parallel()

	q := &countingQueries{stats: fixedStats()}
	a := New(q)
	c := &clock{at: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)}
	a.stats.clock = c.now

	first, err := a.Stats(context.Background())
	require.NoError(t, err, "first Stats")

	c.at = c.at.Add(statsTTL - time.Second)
	second, err := a.Stats(context.Background())
	require.NoError(t, err, "second Stats")

	assert.Equal(t, 1, q.statsCalls, "want one count for two renders inside the TTL")
	assert.Equal(t, first, second, "the memoised answer changed")
}

func TestStatsIsRecountedOnceItsTTLExpires(t *testing.T) {
	t.Parallel()

	q := &countingQueries{stats: fixedStats()}
	a := New(q)
	c := &clock{at: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)}
	a.stats.clock = c.now

	_, err := a.Stats(context.Background())
	require.NoError(t, err, "first Stats")

	c.at = c.at.Add(statsTTL)
	_, err = a.Stats(context.Background())
	require.NoError(t, err, "second Stats")

	assert.Equal(t, 2, q.statsCalls, "want a fresh count once the TTL has passed")
}

// A failed count is usually a database busy for milliseconds. Memoising the
// failure would keep the footer broken for a minute after the store recovered,
// and there is nothing to serve from the cache anyway.
func TestStatsErrorIsNotMemoised(t *testing.T) {
	t.Parallel()

	q := &countingQueries{statsErr: errors.New("database is locked")}
	a := New(q)

	_, err := a.Stats(context.Background())
	require.Error(t, err, "want the store error surfaced")

	q.statsErr = nil
	q.stats = fixedStats()
	got, err := a.Stats(context.Background())
	require.NoError(t, err, "want a retry after the failure")

	assert.Equal(t, 2, q.statsCalls, "a failed count must not be cached")
	assert.True(t, got.Enabled, "a store that answered is enabled")
	assert.Equal(t, int64(1234567), got.Samples, "sample count did not survive the mapping")
}

// The footer distinguishes "no store" from "an empty store", so every adapter
// answer has to claim the store exists — the zero value means the opposite.
func TestStatsMapsEveryCountAndMarksTheStoreEnabled(t *testing.T) {
	t.Parallel()

	q := &countingQueries{stats: fixedStats()}

	got, err := New(q).Stats(context.Background())
	require.NoError(t, err, "Stats")

	assert.True(t, got.Enabled, "want the store reported as present")
	assert.Equal(t, "/data/arc-ui.db", got.Path, "path")
	assert.Equal(t, int64(12*1024*1024), got.SizeBytes, "size")
	assert.Equal(t, int64(42), got.Jobs, "jobs")
	assert.Equal(t, int64(7), got.Phases, "phases")
	assert.Equal(t, int64(99), got.ChurnEvents, "churn events")
	assert.Equal(t, int64(1234715), got.Rows, "total rows")
	assert.Equal(t, fixedStats().Oldest, got.Oldest, "oldest sample")
}

// The lane shows a page and states a total, and the two are different numbers:
// six rows out of forty-one is the whole point of the footer.
func TestFailuresCarryTheirSetAndTheWindowTotal(t *testing.T) {
	t.Parallel()

	failedAt := time.Date(2026, 8, 19, 11, 30, 0, 0, time.UTC)
	q := &countingQueries{
		failures: []store.FailureRecord{
			{Runner: "runner-x", Set: "linux-x64", Reason: "ImagePullBackOff", Severe: true, At: failedAt},
		},
		failuresTotal: 41,
	}

	got, err := New(q).Failures(context.Background(), web.FleetScope, window(), 6)
	require.NoError(t, err, "Failures")

	assert.Equal(t, 41, got.Total, "want the window total, not the page size")
	require.Len(t, got.Failures, 1, "want the page")
	assert.Equal(t, "runner-x", got.Failures[0].Runner, "runner")
	assert.Equal(t, "linux-x64", got.Failures[0].Set, "set")
	assert.Equal(t, "ImagePullBackOff", got.Failures[0].Reason, "reason")
	assert.True(t, got.Failures[0].Severe, "severity did not survive the mapping")
	assert.Equal(t, failedAt, got.Failures[0].At, "timestamp")
}

// A set filter scopes the lane the same way it scopes every chart on the page.
func TestFailuresScopeBecomesTheSetName(t *testing.T) {
	t.Parallel()

	q := &countingQueries{}
	a := New(q)

	_, err := a.Failures(context.Background(), web.FleetScope, window(), 6)
	require.NoError(t, err, "fleet Failures")
	assert.Empty(t, q.gotSetName, "the fleet scope must not name a set")

	_, err = a.Failures(context.Background(), web.Set("arm64"), window(), 6)
	require.NoError(t, err, "set Failures")
	assert.Equal(t, "arm64", q.gotSetName, "a set scope must reach the store as its name")
}

func window() web.Window {
	to := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	return web.Window{From: to.Add(-6 * time.Hour), To: to, Points: 60}
}
