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

const gib = 1 << 30

type fakeQueries struct {
	stats      store.Stats
	statsErr   error
	statsCalls int

	failures      []store.JobRecord
	failuresTotal int64
	gotSet        string

	jobs      []store.JobRecord
	runs      []store.WorkflowRun
	total     int64
	gotFilter store.JobFilter
	gotFrom   time.Time
}

func (q *fakeQueries) Failures(_ context.Context, set string, _, _ time.Time, _ int) ([]store.JobRecord, int64, error) {
	q.gotSet = set
	return q.failures, q.failuresTotal, nil
}

func (q *fakeQueries) Jobs(_ context.Context, f store.JobFilter, from, _ time.Time) ([]store.JobRecord, int64, error) {
	q.gotFilter, q.gotFrom = f, from
	return q.jobs, q.total, nil
}

func (q *fakeQueries) WorkflowRuns(_ context.Context, f store.JobFilter, _, _ time.Time) ([]store.WorkflowRun, int64, error) {
	q.gotFilter = f
	return q.runs, q.total, nil
}

func (q *fakeQueries) Job(context.Context, int) (store.JobRecord, bool, error) {
	if len(q.jobs) == 0 {
		return store.JobRecord{}, false, nil
	}
	return q.jobs[0], true, nil
}

func (q *fakeQueries) Facets(context.Context, time.Time, time.Time) (store.JobFacets, error) {
	return store.JobFacets{Repositories: []string{"acme/api"}, Workflows: []string{"ci.yml"}}, nil
}

func (q *fakeQueries) Stats(context.Context) (store.Stats, error) {
	q.statsCalls++
	return q.stats, q.statsErr
}

type clock struct{ at time.Time }

func (c *clock) now() time.Time { return c.at }

func fixedStats() store.Stats {
	return store.Stats{Path: "/data/arc-ui.db", SizeBytes: 12 << 20, Jobs: 42, Oldest: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)}
}

func window() web.Window {
	to := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	return web.Window{From: to.Add(-6 * time.Hour), To: to, Points: 60}
}

func TestStatsIsMemoisedForItsTTL(t *testing.T) {
	t.Parallel()

	q := &fakeQueries{stats: fixedStats()}
	a := New(q)
	c := &clock{at: time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)}
	a.stats.clock = c.now

	first, err := a.Stats(t.Context())
	require.NoError(t, err)
	c.at = c.at.Add(statsTTL - time.Second)
	second, err := a.Stats(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, q.statsCalls, "every SSE render asks; one count per TTL")
	assert.Equal(t, first, second)

	c.at = c.at.Add(time.Second)
	_, err = a.Stats(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, q.statsCalls)
}

func TestStatsErrorIsNotMemoised(t *testing.T) {
	t.Parallel()

	q := &fakeQueries{statsErr: errors.New("database is locked")}
	a := New(q)
	_, err := a.Stats(t.Context())
	require.Error(t, err)

	q.statsErr, q.stats = nil, fixedStats()
	got, err := a.Stats(t.Context())
	require.NoError(t, err)
	assert.True(t, got.Enabled, "a store that answered is enabled; the zero value means no store")
	assert.Equal(t, int64(42), got.Jobs)
	assert.Equal(t, fixedStats().Oldest, got.Oldest)
}

func TestFailuresAreFailedRunnerJobs(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 19, 11, 30, 0, 0, time.UTC)
	q := &fakeQueries{
		failures: []store.JobRecord{
			{Runner: "r1", Set: "linux-x64", FailureReason: "OOMKilled", FinishedAt: at},
			{Runner: "r2", Set: "linux-x64", FinishedAt: at},
		},
		failuresTotal: 41,
	}
	got, err := New(q).Failures(t.Context(), web.Set("linux-x64"), window(), 6)
	require.NoError(t, err)

	assert.Equal(t, "linux-x64", q.gotSet)
	assert.Equal(t, 41, got.Total, "the window total, not the page size")
	require.Len(t, got.Failures, 2)
	assert.Equal(t, "OOMKilled", got.Failures[0].Reason)
	assert.Equal(t, "runner failed", got.Failures[1].Reason, "an empty reason still says what happened")
	assert.Equal(t, at, got.Failures[0].At)
}

func TestJobsMapOntoTheViewContract(t *testing.T) {
	t.Parallel()

	q := &fakeQueries{
		total: 42,
		jobs: []store.JobRecord{{
			ID: 7, JobID: "j-7", Runner: "r1", Namespace: "arc", Set: "linux-x64", Repository: "acme/api",
			WorkflowRef: "acme/api/.github/workflows/ci.yml@refs/heads/main", Workflow: "ci.yml", Name: "build", RunID: 91,
			StartedAt: time.Unix(1000, 0).UTC(), FinishedAt: time.Unix(1600, 0).UTC(),
			Outcome: store.OutcomeFailed, FailureReason: "OOMKilled",
			CPUSeconds: 600, MemPeakBytes: 4 * gib,
			CPURequest: 2, CPULimit: 4, MemRequest: 4 * gib, MemLimit: 8 * gib,
		}},
	}

	got, err := New(q).Jobs(t.Context(), web.JobFilter{Repository: "acme/api"}, window())
	require.NoError(t, err)
	require.Len(t, got.Jobs, 1)
	j := got.Jobs[0]
	assert.Equal(t, int64(42), got.Total)
	assert.Equal(t, "j-7", j.JobID)
	assert.True(t, j.RunnerFailed)
	assert.Equal(t, "OOMKilled", j.FailureReason)
	assert.Equal(t, "arc", j.Namespace)
	assert.InDelta(t, 4*gib, j.MemPeakBytes, 1e-3)
	assert.InDelta(t, 8*gib, j.MemLimit, 1e-3)
	assert.Equal(t, "acme/api", q.gotFilter.Repository)
	assert.Equal(t, window().From, q.gotFrom)

	one, ok, err := New(q).Job(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, j, one)
}

// An unrecognised outcome widens the listing rather than emptying it.
func TestOutcomeFilterMapsAndFallsBackToAny(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in   web.JobOutcome
		want store.Outcome
	}{
		{web.JobOK, store.OutcomeSucceeded},
		{web.JobFailed, store.OutcomeFailed},
		{web.JobRunning, store.OutcomeRunning},
		{web.JobAnyOutcome, store.OutcomeAny},
		{web.JobOutcome("nonsense"), store.OutcomeAny},
	} {
		q := &fakeQueries{}
		_, err := New(q).Jobs(t.Context(), web.JobFilter{Outcome: tc.in}, window())
		require.NoError(t, err)
		assert.Equal(t, tc.want, q.gotFilter.Outcome, "outcome %q", tc.in)
	}
}

func TestWorkflowRunsAndFacetsMapOntoTheViewContract(t *testing.T) {
	t.Parallel()

	q := &fakeQueries{
		total: 9,
		runs: []store.WorkflowRun{{
			Repository: "acme/api", Workflow: "ci.yml", RunID: 91, Jobs: 4, Running: 1, Failed: 2,
			StartedAt: time.Unix(1000, 0).UTC(), CPUSeconds: 900, MemPeakBytes: 8 * gib,
		}},
	}
	got, err := New(q).Workflows(t.Context(), web.JobFilter{}, window())
	require.NoError(t, err)
	require.Len(t, got.Runs, 1)
	assert.Equal(t, int64(9), got.Total)
	assert.Equal(t, 2, got.Runs[0].Failed)
	assert.InDelta(t, 8*gib, got.Runs[0].MemPeakBytes, 1e-3)

	f, err := New(q).Facets(t.Context(), window())
	require.NoError(t, err)
	assert.Equal(t, []string{"acme/api"}, f.Repositories)
	assert.Equal(t, []string{"ci.yml"}, f.Workflows)
}
