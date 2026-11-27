package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
)

var t0 = time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.Context(), filepath.Join(t.TempDir(), "arc-ui.db"), zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func healthy() []fleet.Source {
	return []fleet.Source{{Name: fleet.SourceARCCRDs, Available: true}}
}

func busy(name, jobID string, started time.Time) fleet.Runner {
	return fleet.Runner{
		Name: name, Namespace: "arc", SetName: "linux-x64", State: fleet.StateBusy, PodPhase: "Running",
		Job: fleet.Job{
			ID: jobID, RunID: 91, Repository: "acme/api", Workflow: "ci.yml",
			WorkflowRef: "acme/api/.github/workflows/ci.yml@refs/heads/main", Name: "build " + jobID, StartedAt: started,
		},
		CPU: fleet.Resources{Request: 2, Limit: 4},
		Mem: fleet.Resources{Request: 4 << 30},
	}
}

func snap(at time.Time, runners ...fleet.Runner) fleet.Snapshot {
	return fleet.Snapshot{At: at, Runners: runners, Sources: healthy()}
}

func observe(t *testing.T, s *Store, sn fleet.Snapshot) []JobRecord {
	t.Helper()
	closed, err := s.Observe(t.Context(), sn)
	require.NoError(t, err)
	return closed
}

func allJobs(t *testing.T, s *Store) []JobRecord {
	t.Helper()
	jobs, _, err := s.Jobs(t.Context(), JobFilter{}, t0.Add(-24*time.Hour), t0.Add(24*time.Hour))
	require.NoError(t, err)
	return jobs
}

func TestAJobIsOneRowFromStartToItsRunnerLeaving(t *testing.T) {
	t.Parallel()
	s := open(t)

	assert.Empty(t, observe(t, s, snap(t0, busy("r1", "j1", t0))))
	assert.Empty(t, observe(t, s, snap(t0.Add(time.Minute), busy("r1", "j1", t0))), "re-observing is an upsert")

	jobs := allJobs(t, s)
	require.Len(t, jobs, 1)
	assert.True(t, jobs[0].Running())
	assert.Equal(t, OutcomeRunning, jobs[0].Outcome)
	assert.Equal(t, "acme/api/.github/workflows/ci.yml@refs/heads/main", jobs[0].WorkflowRef)
	assert.InDelta(t, 2.0, jobs[0].CPURequest, 1e-9)

	closed := observe(t, s, snap(t0.Add(3*time.Minute)))
	require.Len(t, closed, 1)
	assert.Equal(t, OutcomeSucceeded, closed[0].Outcome, "a runner that left without failing is a runner ok")
	assert.Equal(t, t0.Add(time.Minute), closed[0].FinishedAt, "the job ends when it was last seen, not when we noticed")
}

func TestAFailedRunnerClosesItsJobAsFailed(t *testing.T) {
	t.Parallel()
	s := open(t)

	observe(t, s, snap(t0, busy("r1", "j1", t0)))
	r := busy("r1", "j1", t0)
	r.PodPhase, r.FailureReason = "Failed", "OOMKilled"
	closed := observe(t, s, snap(t0.Add(2*time.Minute), r))

	require.Len(t, closed, 1)
	assert.Equal(t, OutcomeFailed, closed[0].Outcome)
	assert.Equal(t, "OOMKilled", closed[0].FailureReason)
	assert.Equal(t, t0.Add(2*time.Minute), closed[0].FinishedAt)

	assert.Empty(t, observe(t, s, snap(t0.Add(3*time.Minute), r)), "a closed job is never reopened or closed twice")

	failures, total, err := s.Failures(t.Context(), "linux-x64", t0, t0.Add(time.Hour), 5)
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
	require.Len(t, failures, 1)
	assert.Equal(t, "r1", failures[0].Runner)

	_, total, err = s.Failures(t.Context(), "other", t0, t0.Add(time.Hour), 5)
	require.NoError(t, err)
	assert.Zero(t, total)
}

// A half-synced informer cache looks like every runner vanished; closing on it
// would end every running job at once.
func TestAnIncompleteSnapshotClosesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)

	observe(t, s, snap(t0, busy("r1", "j1", t0)))
	partial := fleet.Snapshot{At: t0.Add(time.Minute), Sources: []fleet.Source{{Name: fleet.SourceARCCRDs, Available: false}}}
	assert.Empty(t, observe(t, s, partial))
	assert.Empty(t, observe(t, s, fleet.Snapshot{}), "a snapshot with no time is not an observation")
	assert.True(t, allJobs(t, s)[0].Running())
}

// The first complete snapshot after a restart is the sweep for rows the old
// process left open.
func TestBootSweepClosesRowsWhoseRunnerIsGone(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "arc-ui.db")

	first, err := Open(t.Context(), path, zerolog.Nop())
	require.NoError(t, err)
	observe(t, first, snap(t0, busy("r1", "j1", t0), busy("r2", "j2", t0)))
	require.NoError(t, first.Close())

	second, err := Open(t.Context(), path, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	closed := observe(t, second, snap(t0.Add(time.Hour), busy("r2", "j2", t0)))
	require.Len(t, closed, 1)
	assert.Equal(t, "j1", closed[0].JobID)
	assert.Equal(t, t0, closed[0].FinishedAt, "downtime must not stretch the job")
}

func TestListingsFilterSearchAndCount(t *testing.T) {
	t.Parallel()
	s := open(t)

	other := busy("r3", "j3", t0.Add(2*time.Minute))
	other.Job.Repository, other.Job.Workflow, other.Job.RunID = "acme/web_100%", "deploy.yml", 92
	other.SetName = "arm64"
	observe(t, s, snap(t0.Add(3*time.Minute), busy("r1", "j1", t0), busy("r2", "j2", t0.Add(time.Minute)), other))
	observe(t, s, snap(t0.Add(5*time.Minute), busy("r2", "j2", t0.Add(time.Minute)), other))

	from, to := t0.Add(-time.Hour), t0.Add(time.Hour)
	all, total, err := s.Jobs(t.Context(), JobFilter{Limit: 2}, from, to)
	require.NoError(t, err)
	assert.Equal(t, int64(3), total, "the total counts the window, not the page")
	require.Len(t, all, 2)
	assert.Equal(t, "j3", all[0].JobID, "newest first")

	running, _, err := s.Jobs(t.Context(), JobFilter{Outcome: OutcomeRunning}, from, to)
	require.NoError(t, err)
	assert.Len(t, running, 2)

	ok, _, err := s.Jobs(t.Context(), JobFilter{Outcome: OutcomeSucceeded}, from, to)
	require.NoError(t, err)
	require.Len(t, ok, 1)
	assert.Equal(t, "j1", ok[0].JobID)

	found, _, err := s.Jobs(t.Context(), JobFilter{Search: "WEB_100%"}, from, to)
	require.NoError(t, err)
	require.Len(t, found, 1, "search is case-insensitive and treats % and _ literally")
	assert.Equal(t, "j3", found[0].JobID)

	none, _, err := s.Jobs(t.Context(), JobFilter{Search: "web_1000"}, from, to)
	require.NoError(t, err)
	assert.Empty(t, none)

	bySet, _, err := s.Jobs(t.Context(), JobFilter{Set: "arm64", Workflow: "deploy.yml", Repository: "acme/web_100%", RunID: 92}, from, to)
	require.NoError(t, err)
	assert.Len(t, bySet, 1)

	before, _, err := s.Jobs(t.Context(), JobFilter{}, t0.Add(-2*time.Hour), t0.Add(-time.Hour))
	require.NoError(t, err)
	assert.Empty(t, before, "a window before any job starts holds nothing")

	runs, runTotal, err := s.WorkflowRuns(t.Context(), JobFilter{}, from, to)
	require.NoError(t, err)
	assert.Equal(t, int64(2), runTotal)
	require.Len(t, runs, 2)
	assert.Equal(t, int64(92), runs[0].RunID)
	assert.Equal(t, 2, runs[1].Jobs)
	assert.Equal(t, 1, runs[1].Running)
	assert.True(t, runs[1].FinishedAt.IsZero(), "a run with a job still going has no end")

	facets, err := s.Facets(t.Context(), from, to)
	require.NoError(t, err)
	assert.Equal(t, []string{"acme/api", "acme/web_100%"}, facets.Repositories)
	assert.Equal(t, []string{"arm64", "linux-x64"}, facets.Sets)
	assert.Equal(t, []string{"ci.yml", "deploy.yml"}, facets.Workflows)
}

func TestUsagePruneAndStats(t *testing.T) {
	t.Parallel()
	s := open(t)

	observe(t, s, snap(t0, busy("r1", "j1", t0), busy("r2", "j2", t0)))
	closed := observe(t, s, snap(t0.Add(time.Minute), busy("r2", "j2", t0)))
	require.Len(t, closed, 1)

	require.NoError(t, s.SetUsage(t.Context(), closed[0].ID, 120, 3<<30))
	j, ok, err := s.Job(t.Context(), closed[0].ID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.InDelta(t, 120.0, j.CPUSeconds, 1e-9)
	assert.InDelta(t, float64(3<<30), j.MemPeakBytes, 1)

	_, ok, err = s.Job(t.Context(), 9999)
	require.NoError(t, err)
	assert.False(t, ok)

	st, err := s.Stats(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), st.Jobs)
	assert.Equal(t, t0, st.Oldest)
	assert.Positive(t, st.SizeBytes)

	n, err := s.Prune(t.Context(), t0.Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only finished jobs age out; a running one is kept however old")
	require.NoError(t, s.Ping(t.Context()))
}

func TestOpenDropsTheLegacyTimeSeriesTables(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "arc-ui.db")

	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `CREATE TABLE samples (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	s, err := Open(t.Context(), path, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	var n int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM sqlite_master WHERE name = 'samples'`).Scan(&n))
	assert.Zero(t, n)

	_, err = Open(t.Context(), "", zerolog.Nop())
	require.Error(t, err)
}
