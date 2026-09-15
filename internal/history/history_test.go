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
// the memoised queries were asked, which is the whole point of the caches under
// test.
type countingQueries struct {
	stats      store.Stats
	statsErr   error
	statsCalls int

	facets      store.JobFacets
	facetsErr   error
	facetsCalls int

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

func (q *countingQueries) Jobs(context.Context, store.JobFilter, store.Range) ([]store.JobRecord, int64, error) {
	return nil, 0, nil
}

func (q *countingQueries) WorkflowRuns(context.Context, store.JobFilter, store.Range) ([]store.WorkflowRun, int64, error) {
	return nil, 0, nil
}

func (q *countingQueries) Job(context.Context, int) (store.JobRecord, bool, error) {
	return store.JobRecord{}, false, nil
}

func (q *countingQueries) JobSeries(context.Context, int, store.Range) ([]store.JobPoint, error) {
	return nil, nil
}

func (q *countingQueries) JobFacets(context.Context, store.Range) (store.JobFacets, error) {
	q.facetsCalls++
	return q.facets, q.facetsErr
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

// jobQueries records what it was asked and answers with fixtures, so the
// adapter's mapping can be checked without a database.
type jobQueries struct {
	countingQueries

	jobs  []store.JobRecord
	runs  []store.WorkflowRun
	total int64

	gotFilter store.JobFilter
	gotRange  store.Range
}

func (q *jobQueries) Jobs(_ context.Context, f store.JobFilter, r store.Range) ([]store.JobRecord, int64, error) {
	q.gotFilter, q.gotRange = f, r
	return q.jobs, q.total, nil
}

func (q *jobQueries) WorkflowRuns(_ context.Context, f store.JobFilter, r store.Range) ([]store.WorkflowRun, int64, error) {
	q.gotFilter, q.gotRange = f, r
	return q.runs, q.total, nil
}

func (q *jobQueries) JobSeries(context.Context, int, store.Range) ([]store.JobPoint, error) {
	return []store.JobPoint{
		{At: time.Unix(100, 0).UTC(), CPU: 1.5, Mem: 2 * gib},
		{At: time.Unix(160, 0).UTC(), CPU: 2.5, Mem: 3 * gib},
	}, nil
}

func (q *jobQueries) Job(context.Context, int) (store.JobRecord, bool, error) {
	if len(q.jobs) == 0 {
		return store.JobRecord{}, false, nil
	}
	return q.jobs[0], true, nil
}

func (q *jobQueries) JobFacets(context.Context, store.Range) (store.JobFacets, error) {
	q.facetsCalls++
	return store.JobFacets{
		Repositories: []string{"acme/api"},
		Workflows:    []string{"ci.yml"},
		Jobs:         []string{"build"},
		Sets:         []string{"linux-x64"},
	}, nil
}

func TestJobsMapOntoTheViewContract(t *testing.T) {
	t.Parallel()

	q := &jobQueries{
		total: 42,
		jobs: []store.JobRecord{{
			ID: 7, Runner: "r1", Set: "linux-x64", Repository: "acme/api",
			Workflow: "ci.yml", Job: "build", RunID: 91,
			StartedAt: time.Unix(1000, 0).UTC(), FinishedAt: time.Unix(1600, 0).UTC(),
			Succeeded: true, CPUSeconds: 600, MemByteSeconds: 4 * gib,
			CPURequest: 2, CPULimit: 4, MemRequest: 4 * gib, MemLimit: 8 * gib,
		}},
	}

	got, err := New(q).Jobs(t.Context(), web.JobFilter{Repository: "acme/api"}, window())
	require.NoError(t, err, "Jobs")

	require.Len(t, got.Jobs, 1)
	assert.Equal(t, int64(42), got.Total, "the window's total travels with the page")
	assert.Equal(t, 7, got.Jobs[0].ID)
	assert.Equal(t, "build", got.Jobs[0].Name)
	assert.InDelta(t, 600.0, got.Jobs[0].CPUSeconds, 1e-9)
	// Byte-seconds for a real job is a fourteen-digit number, so the views take
	// GiB-seconds and the conversion has to happen exactly once.
	assert.InDelta(t, 4.0, got.Jobs[0].MemGiBSecs, 1e-9, "byte-seconds should become GiB-seconds")
	// The reservations, unlike the totals, stay in the store's own units: they
	// are plotted against the usage series rather than summarised.
	assert.InDelta(t, 2.0, got.Jobs[0].CPURequest, 1e-9, "cpu request")
	assert.InDelta(t, 4.0, got.Jobs[0].CPULimit, 1e-9, "cpu limit")
	assert.InDelta(t, 4*gib, got.Jobs[0].MemRequest, 1e-3, "memory request should stay in bytes")
	assert.InDelta(t, 8*gib, got.Jobs[0].MemLimit, 1e-3, "memory limit should stay in bytes")
	assert.Equal(t, "acme/api", q.gotFilter.Repository, "the filter should reach the store")
}

// The outcome vocabularies are declared at both ends. An unrecognised value
// has to widen the listing rather than empty it, or a hand-edited URL shows a
// blank table that looks like a fleet doing nothing.
func TestOutcomeFilterMapsAndFallsBackToAny(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in   web.JobOutcome
		want store.Outcome
	}{
		{web.JobOK, store.OutcomeOK},
		{web.JobFailed, store.OutcomeFailed},
		{web.JobRunning, store.OutcomeRunning},
		{web.JobAnyOutcome, store.OutcomeAny},
		{web.JobOutcome("nonsense"), store.OutcomeAny},
	} {
		q := &jobQueries{}
		_, err := New(q).Jobs(t.Context(), web.JobFilter{Outcome: tc.in}, window())
		require.NoError(t, err)
		assert.Equal(t, tc.want, q.gotFilter.Outcome, "outcome %q", tc.in)
	}
}

func TestWorkflowRunsMapOntoTheViewContract(t *testing.T) {
	t.Parallel()

	q := &jobQueries{
		total: 9,
		runs: []store.WorkflowRun{{
			Repository: "acme/api", Workflow: "ci.yml", RunID: 91,
			Jobs: 4, Running: 1, Failed: 2,
			StartedAt:  time.Unix(1000, 0).UTC(),
			CPUSeconds: 900, MemByteSeconds: 8 * gib,
		}},
	}

	got, err := New(q).Workflows(t.Context(), web.JobFilter{}, window())
	require.NoError(t, err, "Workflows")

	require.Len(t, got.Runs, 1)
	assert.Equal(t, int64(9), got.Total)
	assert.Equal(t, 4, got.Runs[0].Jobs)
	assert.Equal(t, 2, got.Runs[0].Failed)
	assert.InDelta(t, 8.0, got.Runs[0].MemGiBSecs, 1e-9)
	assert.True(t, got.Runs[0].FinishedAt.IsZero(), "a run with a job still going has no end")
}

func TestJobSeriesSplitsIntoParallelSlices(t *testing.T) {
	t.Parallel()

	got, err := New(&jobQueries{}).JobSeries(t.Context(), 7, window())
	require.NoError(t, err, "JobSeries")

	require.Equal(t, 2, got.Len())
	assert.Len(t, got.CPU, got.Len(), "every slice is the length of the axis")
	assert.Len(t, got.Mem, got.Len())
	assert.InDelta(t, 2.5, got.CPU[1], 1e-9)
	assert.InDelta(t, 3*gib, got.Mem[1], 1e-3, "memory stays in bytes for the chart")
}

func TestFacetsMapOntoTheViewContract(t *testing.T) {
	t.Parallel()

	got, err := New(&jobQueries{}).Facets(t.Context(), window())
	require.NoError(t, err, "Facets")
	assert.Equal(t, []string{"acme/api"}, got.Repositories)
	assert.Equal(t, []string{"ci.yml"}, got.Workflows)
	assert.Equal(t, []string{"build"}, got.Jobs, "job names did not survive the mapping")
	assert.Equal(t, []string{"linux-x64"}, got.Sets)
}

// Four DISTINCTs over every job in the window, rebuilt into the filter bar on
// every snapshot change for every connected browser. The fleet bar unions these
// with the live snapshot, so the TTL delays nothing that is running.
func TestFacetsAreNotRequeriedWithinTheirTTL(t *testing.T) {
	t.Parallel()

	q := &countingQueries{}
	a := New(q)
	c := &clock{at: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)}
	a.facets.clock = c.now

	_, err := a.Facets(t.Context(), window())
	require.NoError(t, err, "first Facets")

	c.at = c.at.Add(facetsTTL - time.Second)
	_, err = a.Facets(t.Context(), window())
	require.NoError(t, err, "second Facets")
	assert.Equal(t, 1, q.facetsCalls, "want one read for two renders inside the TTL")

	c.at = c.at.Add(time.Second)
	_, err = a.Facets(t.Context(), window())
	require.NoError(t, err, "third Facets")
	assert.Equal(t, 2, q.facetsCalls, "want a fresh read once the TTL has passed")
}

// Ranges are cached apart, or switching the picker from 1h to 30d would serve
// the hour's options for the next half minute.
func TestFacetsAreCachedPerRangeWidth(t *testing.T) {
	t.Parallel()

	q := &countingQueries{}
	a := New(q)
	c := &clock{at: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)}
	a.facets.clock = c.now

	wide := window()
	narrow := web.Window{From: wide.To.Add(-time.Hour), To: wide.To, Points: 60}

	for _, w := range []web.Window{wide, narrow, wide, narrow} {
		_, err := a.Facets(t.Context(), w)
		require.NoError(t, err, "Facets")
	}
	assert.Equal(t, 2, q.facetsCalls, "want one read per width, not one per call")
}

// Same reasoning as the stats cache: a database busy for milliseconds must not
// empty every dropdown for the next half minute.
func TestFacetsErrorIsNotMemoised(t *testing.T) {
	t.Parallel()

	q := &countingQueries{facetsErr: errors.New("database is locked")}
	a := New(q)

	_, err := a.Facets(t.Context(), window())
	require.Error(t, err, "want the store error surfaced")

	q.facetsErr = nil
	q.facets = store.JobFacets{Repositories: []string{"acme/api"}}
	got, err := a.Facets(t.Context(), window())
	require.NoError(t, err, "want a retry after the failure")

	assert.Equal(t, 2, q.facetsCalls, "a failed read must not be cached")
	assert.Equal(t, []string{"acme/api"}, got.Repositories)
}
