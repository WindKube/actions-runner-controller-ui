package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
)

// defaultRetention mirrors config.Config's defaults, so the tests exercise the
// tier windows the binary actually ships with.
var defaultRetention = Retention{
	RunnerRaw: 15 * time.Minute,
	ScopeRaw:  6 * time.Hour,
	Scope1m:   7 * 24 * time.Hour,
	Scope5m:   30 * 24 * time.Hour,
	Scope1h:   400 * 24 * time.Hour,
}

// base is a fixed instant so bucket boundaries in the assertions are stable.
// It is aligned to an hour, which means it is also aligned to every bucket
// width the store uses.
var base = time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)

func newStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nested", "dir", "arc-ui.db")
	s, err := Open(t.Context(), path, zerolog.Nop())
	require.NoError(t, err, "Open")
	t.Cleanup(func() {
		assert.NoError(t, s.Close(), "Close")
	})
	return s
}

// seed writes samples straight into the table so the compaction and retention
// tests can fabricate a timeline without pretending to scrape one.
func seed(t *testing.T, s *Store, scope Scope, id string, metric Metric, tier Tier, start time.Time, step time.Duration, n int, value func(i int) float64) {
	t.Helper()
	for i := range n {
		ts := start.Add(time.Duration(i) * step).Unix()
		_, err := s.db.ExecContext(t.Context(),
			`INSERT INTO samples (scope, scope_id, metric, tier, ts, value) VALUES (?,?,?,?,?,?)
			 ON CONFLICT (scope, scope_id, metric, tier, ts) DO UPDATE SET value = excluded.value`,
			string(scope), id, string(metric), string(tier), ts, value(i))
		require.NoError(t, err, "seed sample")
	}
}

// dump returns every sample row in a stable order, for before/after comparison.
func dump(t *testing.T, s *Store) []string {
	t.Helper()
	rows, err := s.db.QueryContext(t.Context(),
		`SELECT scope, scope_id, metric, tier, ts, value FROM samples
		 ORDER BY tier, scope, scope_id, metric, ts`)
	require.NoError(t, err, "dump")
	defer rows.Close()

	var out []string
	for rows.Next() {
		var (
			scope, id, metric, tier string
			ts                      int64
			value                   float64
		)
		require.NoError(t, rows.Scan(&scope, &id, &metric, &tier, &ts, &value), "dump scan")
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%d|%.6f", tier, scope, id, metric, ts, value))
	}
	require.NoError(t, rows.Err(), "dump rows")
	return out
}

func countTier(t *testing.T, s *Store, tier Tier, scope Scope) int {
	t.Helper()
	var n int
	q := `SELECT COUNT(*) FROM samples WHERE tier = ?`
	args := []any{string(tier)}
	if scope != "" {
		q += " AND scope = ?"
		args = append(args, string(scope))
	}
	require.NoError(t, s.db.QueryRowContext(t.Context(), q, args...).Scan(&n), "countTier")
	return n
}

func TestOpenCreatesParentDirectoryAndMigrates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "a", "b", "c", "arc-ui.db")

	s, err := Open(t.Context(), path, zerolog.Nop())
	require.NoError(t, err, "Open")
	defer s.Close()

	require.FileExists(t, path, "database file not created")
	require.NoError(t, s.Ping(t.Context()), "Ping")

	// Migrations applied means every table the store writes to answers.
	for _, table := range []string{"samples", "job_observations", "phase_transitions", "churn_events"} {
		var n int
		assert.NoError(t, s.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+table).Scan(&n), "table %s missing", table)
	}

	// Reopening an existing database must be a no-op, not a migration failure.
	require.NoError(t, s.Close(), "Close")
	again, err := Open(t.Context(), path, zerolog.Nop())
	require.NoError(t, err, "reopen")
	again.Close()
}

func TestOpenRejectsEmptyPath(t *testing.T) {
	t.Parallel()

	_, err := Open(t.Context(), "", zerolog.Nop())
	require.Error(t, err, "expected an error for an empty path")
}

// snapshot builds a one-set fleet with the given runners.
func snapshot(at time.Time, runners ...fleet.Runner) fleet.Snapshot {
	return fleet.Snapshot{
		At: at,
		Sets: []fleet.RunnerSet{{
			Name:       "linux-x64",
			Namespace:  "arc-runners",
			MinRunners: 0,
			MaxRunners: 10,
			CPURequest: 2,
			MemRequest: 4 * fleet.GiB,
		}},
		Runners: runners,
	}
}

func busyRunner(name string, at time.Time, cpu float64) fleet.Runner {
	return fleet.Runner{
		Name:      name,
		Namespace: "arc-runners",
		SetName:   "linux-x64",
		State:     fleet.StateBusy,
		CreatedAt: at.Add(-time.Minute),
		Job: fleet.Job{
			Repository: "acme/api",
			Workflow:   "ci.yml",
			Name:       "build",
			RunID:      42,
			StartedAt:  at.Add(-30 * time.Second),
		},
		CPU: fleet.Resources{Used: cpu, Request: 2, At: at},
		Mem: fleet.Resources{Used: 1 * fleet.GiB, Request: 4 * fleet.GiB, At: at},
	}
}

func TestRecordSnapshotSeriesRoundTrip(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	snap := snapshot(base,
		busyRunner("runner-a", base, 1.5),
		fleet.Runner{
			Name: "runner-b", SetName: "linux-x64", State: fleet.StateIdle,
			CreatedAt: base.Add(-2 * time.Minute),
			CPU:       fleet.Resources{Used: 0.1, Request: 2, At: base},
			Mem:       fleet.Resources{Used: 0.5 * fleet.GiB, Request: 4 * fleet.GiB, At: base},
		},
	)
	require.NoError(t, s.RecordSnapshot(ctx, snap), "RecordSnapshot")

	r := Range{From: base.Add(-time.Minute), To: base.Add(time.Minute), Points: 120}

	tests := []struct {
		name    string
		scope   Scope
		scopeID string
		metric  Metric
		want    float64
	}{
		{"fleet busy", ScopeFleet, "", MetricBusy, 1},
		{"fleet idle", ScopeFleet, "", MetricIdle, 1},
		{"fleet runners", ScopeFleet, "", MetricRunners, 2},
		{"fleet cpu used", ScopeFleet, "", MetricCPUUsed, 1.6},
		{"fleet cpu request", ScopeFleet, "", MetricCPURequest, 4},
		{"fleet capacity", ScopeFleet, "", MetricCapacity, 10},
		{"set busy", ScopeSet, "linux-x64", MetricBusy, 1},
		{"runner cpu", ScopeRunner, "runner-a", MetricCPUUsed, 1.5},
		{"runner mem", ScopeRunner, "runner-b", MetricMemUsed, 0.5 * fleet.GiB},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := s.Series(ctx, tc.scope, tc.scopeID, []Metric{tc.metric}, r)
			require.NoError(t, err, "Series")
			pts := got[tc.metric]
			require.Len(t, pts, 1, "want 1 point: %v", pts)
			assert.InDelta(t, tc.want, pts[0].Value, 1e-6)
		})
	}

	// Queued is absent because the listener metrics were not reported. A
	// stored zero here would be a lie the dashboard cannot distinguish from a
	// measurement.
	got, err := s.Series(ctx, ScopeFleet, "", []Metric{MetricQueued}, r)
	require.NoError(t, err, "Series(queued)")
	assert.Empty(t, got[MetricQueued], "queued should be absent when the listener is not reporting")
}

func TestRecordSnapshotIsIdempotent(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	snap := snapshot(base, busyRunner("runner-a", base, 1.5))

	for i := range 3 {
		require.NoError(t, s.RecordSnapshot(t.Context(), snap), "RecordSnapshot %d", i)
	}

	st, err := s.Stats(t.Context())
	require.NoError(t, err, "Stats")
	assert.Equal(t, int64(1), st.ChurnEvents, "want 1 churn event after replaying the same snapshot")
	assert.Equal(t, int64(1), st.Jobs, "want 1 job")
	assert.Equal(t, int64(1), st.Phases, "want 1 phase")
}

func TestTierFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		window time.Duration
		points int
		want   Tier
	}{
		{"15m over 60 points", 15 * time.Minute, 60, TierRaw},
		{"1h over 60 points", time.Hour, 60, Tier1m},
		{"6h over 72 points", 6 * time.Hour, 72, Tier5m},
		{"24h over 96 points", 24 * time.Hour, 96, Tier5m},
		{"7d over 84 points", 7 * 24 * time.Hour, 84, Tier1h},
		{"30d over 96 points", 30 * 24 * time.Hour, 96, Tier1h},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, bucket, ok := Range{From: base, To: base.Add(tc.window), Points: tc.points}.window()
			require.True(t, ok, "window() rejected a valid range")
			assert.Equal(t, tc.want, tierFor(bucket), "tierFor(%ds)", bucket)
		})
	}
}

func TestSeriesReadsTheChosenTier(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	now := base

	// Every tier carries a distinct constant, so the value that comes back
	// names the tier that was read.
	seed(t, s, ScopeFleet, "", MetricBusy, TierRaw, now.Add(-15*time.Minute), 15*time.Second, 60, func(int) float64 { return 1 })
	seed(t, s, ScopeFleet, "", MetricBusy, Tier1m, now.Add(-30*24*time.Hour), time.Minute, 200, func(int) float64 { return 2 })
	seed(t, s, ScopeFleet, "", MetricBusy, Tier5m, now.Add(-30*24*time.Hour), 5*time.Minute, 200, func(int) float64 { return 3 })
	seed(t, s, ScopeFleet, "", MetricBusy, Tier1h, now.Add(-30*24*time.Hour), time.Hour, 200, func(int) float64 { return 4 })

	tests := []struct {
		name string
		r    Range
		want float64
	}{
		{"15m reads raw", Range{From: now.Add(-15 * time.Minute), To: now, Points: 60}, 1},
		{"30d reads hourly", Range{From: now.Add(-30 * 24 * time.Hour), To: now, Points: 96}, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := s.Series(t.Context(), ScopeFleet, "", []Metric{MetricBusy}, tc.r)
			require.NoError(t, err, "Series")
			pts := got[MetricBusy]
			require.NotEmpty(t, pts, "no points")
			for _, p := range pts {
				require.InDelta(t, tc.want, p.Value, 1e-9, "read tier carrying %v, want the one carrying %v", p.Value, tc.want)
			}
		})
	}
}

func TestSeriesBucketCount(t *testing.T) {
	t.Parallel()

	// Scoped to a runner so the tier is raw whatever the bucket width works
	// out to; this test is about bucketing, not tier selection.
	s := newStore(t)
	start := base.Add(-10 * time.Minute)
	seed(t, s, ScopeRunner, "runner-a", MetricCPUUsed, TierRaw, start, 15*time.Second, 40, func(i int) float64 { return float64(i) })

	tests := []struct {
		name   string
		points int
	}{
		{"40 points", 40},
		{"20 points", 20},
		{"10 points", 10},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := s.Series(t.Context(), ScopeRunner, "runner-a", []Metric{MetricCPUUsed},
				Range{From: start, To: base, Points: tc.points})
			require.NoError(t, err, "Series")
			n := len(got[MetricCPUUsed])
			// Integer bucket widths cannot divide every window exactly, so the
			// contract is "roughly", not "exactly".
			assert.InDelta(t, tc.points, n, 1, "got %d buckets, want about %d", n, tc.points)
		})
	}
}

func TestCompactRollsUpAndIsIdempotent(t *testing.T) {
	t.Parallel()

	// Retention deliberately shorter than the seeded history, so the second
	// run has to cope with sources its predecessor already deleted. That is
	// the case a naive implementation gets wrong: it re-averages a boundary
	// bucket from whatever survived and the value drifts on every run.
	ret := defaultRetention
	ret.ScopeRaw = time.Hour

	s := newStore(t)
	now := base
	start := now.Add(-3 * time.Hour)
	seed(t, s, ScopeFleet, "", MetricBusy, TierRaw, start, 15*time.Second, 3*60*4, func(i int) float64 { return float64(i % 7) })
	seed(t, s, ScopeSet, "linux-x64", MetricBusy, TierRaw, start, 15*time.Second, 3*60*4, func(i int) float64 { return float64(i%5) + 0.5 })
	seed(t, s, ScopeRunner, "runner-a", MetricCPUUsed, TierRaw, now.Add(-20*time.Minute), 15*time.Second, 80, func(int) float64 { return 1 })

	require.NoError(t, s.Compact(t.Context(), now, ret), "Compact")
	first := dump(t, s)

	require.NoError(t, s.Compact(t.Context(), now, ret), "Compact again")
	second := dump(t, s)

	require.Equal(t, first, second, "rows changed between runs")

	// A third run at the same instant must also change nothing.
	require.NoError(t, s.Compact(t.Context(), now, ret), "Compact a third time")
	require.Equal(t, first, dump(t, s), "rows changed on the third run")

	// The whole chain runs in one pass: raw feeds 1m, 1m feeds 5m, 5m feeds 1h.
	for _, tier := range []Tier{Tier1m, Tier5m, Tier1h} {
		assert.NotZero(t, countTier(t, s, tier, ""), "no %s rows were derived", tier)
	}
	// Runner samples are raw-only: rolling them up would recreate the
	// per-runner history the tiering exists to avoid.
	assert.Zero(t, countTier(t, s, Tier1m, ScopeRunner), "runner rows were rolled up, want 0")
}

func TestCompactAveragesIntoBuckets(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	now := base
	// Four samples in the minute starting at now-2m, values 0..3, mean 1.5.
	minute := now.Add(-2 * time.Minute)
	seed(t, s, ScopeFleet, "", MetricBusy, TierRaw, minute, 15*time.Second, 4, func(i int) float64 { return float64(i) })

	require.NoError(t, s.Compact(t.Context(), now, defaultRetention), "Compact")

	var got float64
	err := s.db.QueryRowContext(t.Context(),
		`SELECT value FROM samples WHERE tier = ? AND scope = ? AND metric = ? AND ts = ?`,
		string(Tier1m), string(ScopeFleet), string(MetricBusy), minute.Unix()).Scan(&got)
	require.NoError(t, err, "read rolled-up bucket")
	assert.InDelta(t, 1.5, got, 1e-9, "want a bucket mean of 1.5")
}

func TestRetentionDeletesOnlyPastTheWindow(t *testing.T) {
	t.Parallel()

	now := base

	tests := []struct {
		name       string
		scope      Scope
		scopeID    string
		tier       Tier
		inside     time.Duration // age of the row that must survive
		outside    time.Duration // age of the row that must be deleted
		wantInside bool
	}{
		{"runner raw", ScopeRunner, "runner-a", TierRaw, 5 * time.Minute, 60 * time.Minute, true},
		{"scope raw", ScopeFleet, "", TierRaw, 2 * time.Hour, 24 * time.Hour, true},
		{"1m", ScopeFleet, "", Tier1m, 24 * time.Hour, 30 * 24 * time.Hour, true},
		{"5m", ScopeFleet, "", Tier5m, 10 * 24 * time.Hour, 90 * 24 * time.Hour, true},
		{"1h", ScopeFleet, "", Tier1h, 100 * 24 * time.Hour, 500 * 24 * time.Hour, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newStore(t)
			seed(t, s, tc.scope, tc.scopeID, MetricBusy, tc.tier, now.Add(-tc.inside), time.Second, 1, func(int) float64 { return 1 })
			seed(t, s, tc.scope, tc.scopeID, MetricBusy, tc.tier, now.Add(-tc.outside), time.Second, 1, func(int) float64 { return 2 })

			require.NoError(t, s.Compact(t.Context(), now, defaultRetention), "Compact")

			survived := map[int64]bool{}
			rows, err := s.db.QueryContext(t.Context(),
				`SELECT ts FROM samples WHERE tier = ? AND scope = ?`, string(tc.tier), string(tc.scope))
			require.NoError(t, err, "query")
			defer rows.Close()
			for rows.Next() {
				var ts int64
				require.NoError(t, rows.Scan(&ts), "scan")
				survived[ts] = true
			}

			assert.Contains(t, survived, now.Add(-tc.inside).Unix(), "row inside the window (%s old) was deleted", tc.inside)
			assert.NotContains(t, survived, now.Add(-tc.outside).Unix(), "row outside the window (%s old) survived", tc.outside)
		})
	}
}

// errNoRowCount is what the fake driver below fails RowsAffected with.
var errNoRowCount = errors.New("driver cannot count affected rows")

func init() { sql.Register("store-test-no-row-count", noRowCountDriver{}) }

// noRowCountDriver answers every statement with a Result that cannot say how
// many rows it touched. SQLite always can, so a fake is the only way to reach
// the path where a rollup's INSERT ... SELECT lands but its row count does not.
type noRowCountDriver struct{}

func (noRowCountDriver) Open(string) (driver.Conn, error) { return noRowCountConn{}, nil }

type noRowCountConn struct{}

func (noRowCountConn) Prepare(string) (driver.Stmt, error) { return nil, errNoRowCount }
func (noRowCountConn) Close() error                        { return nil }
func (noRowCountConn) Begin() (driver.Tx, error)           { return nil, errNoRowCount }
func (noRowCountConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return noRowCountResult{}, nil
}

type noRowCountResult struct{}

func (noRowCountResult) LastInsertId() (int64, error) { return 0, errNoRowCount }
func (noRowCountResult) RowsAffected() (int64, error) { return 0, errNoRowCount }

// TestRollupSurfacesAFailedRowCount pins the difference between "nothing was
// rolled up" and "how much was rolled up is unknown". Reporting the second as
// the first hides a broken driver behind a plausible-looking zero.
func TestRollupSurfacesAFailedRowCount(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("store-test-no-row-count", "")
	require.NoError(t, err, "open fake driver")
	t.Cleanup(func() { assert.NoError(t, db.Close(), "close fake db") })

	s := &Store{db: db, log: zerolog.Nop()}
	n, err := s.rollup(t.Context(), base, TierRaw, Tier1m, time.Hour)
	require.Error(t, err, "a rollup that cannot count what it wrote must not report success")
	assert.Zero(t, n, "no count is available, so none may be claimed")
}

// seedJob inserts a job observation straight into the table, so a retention
// test can fabricate a job that started weeks ago without replaying weeks of
// snapshots. A zero finishedAt means "still running", the same sentinel the
// column uses.
func seedJob(t *testing.T, s *Store, runner string, startedAt, finishedAt time.Time) {
	t.Helper()
	var fin int64
	if !finishedAt.IsZero() {
		fin = finishedAt.Unix()
	}
	_, err := s.db.ExecContext(t.Context(),
		`INSERT INTO job_observations
		 (runner_name, set_name, repository, workflow, job_name, run_id, started_at, finished_at, succeeded, cpu_seconds, mem_byte_seconds)
		 VALUES (?, 'linux-x64', 'acme/api', 'ci.yml', 'build', 1, ?, ?, 1, 0, 0)`,
		runner, startedAt.Unix(), fin)
	require.NoError(t, err, "seed job observation")
}

// TestRetentionKeepsJobsUntilTheyAreHistorical checks that the job sweep keys
// on when a row stopped changing, not on when it started: a job that is still
// running, or one that ran for weeks and finished a minute ago, is live data
// however old its started_at is.
func TestRetentionKeepsJobsUntilTheyAreHistorical(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	now := base
	day := 24 * time.Hour // defaultRetention.Scope5m, the job window, is 30 of these

	seedJob(t, s, "still-running", now.Add(-31*day), time.Time{})
	seedJob(t, s, "long-job-just-finished", now.Add(-31*day), now.Add(-time.Hour))
	seedJob(t, s, "finished-long-ago", now.Add(-40*day), now.Add(-31*day))
	// Never closed and far past any job's execution limit: the completion signal
	// was lost (the process was down when the runner went away) and nothing will
	// ever close this row, so retention has to.
	seedJob(t, s, "abandoned", now.Add(-60*day), time.Time{})

	require.NoError(t, s.Compact(t.Context(), now, defaultRetention), "Compact")

	rows, err := s.db.QueryContext(t.Context(), `SELECT runner_name FROM job_observations`)
	require.NoError(t, err, "query surviving jobs")
	defer rows.Close()
	var survived []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name), "scan")
		survived = append(survived, name)
	}
	require.NoError(t, rows.Err(), "read surviving jobs")

	assert.Contains(t, survived, "still-running", "a job that is still running was deleted while live")
	assert.Contains(t, survived, "long-job-just-finished", "a job that finished an hour ago was deleted")
	assert.NotContains(t, survived, "finished-long-ago", "a job that finished past the window survived")
	assert.NotContains(t, survived, "abandoned", "an abandoned open job survived forever")
}

// TestSwitchedJobKeepsItsFinalInterval covers the persistent-runner handover:
// the runner is seen on one job and then on the next, and the interval between
// those two scrapes must not fall between the two rows. It lands on the new
// job, which is the direction RecordSnapshot bills a handover — see
// TestHandoverIsNotBilledToThePreviousRepository for why the direction matters
// more than it looks.
func TestSwitchedJobKeepsItsFinalInterval(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	first := busyRunner("runner-a", base, 2.0)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, first)), "first snapshot")

	// Same runner, thirty seconds later, now carrying a different job.
	at := base.Add(30 * time.Second)
	second := busyRunner("runner-a", at, 2.0)
	second.CreatedAt = first.CreatedAt
	second.Job.RunID = 43
	second.Job.Name = "test"
	second.Job.StartedAt = at
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, second)), "second snapshot")

	jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
	require.NoError(t, err, "JobsForSet")
	require.Len(t, jobs, 2, "want both jobs: %+v", jobs)
	byName := map[string]JobRecord{}
	for _, j := range jobs {
		byName[j.Job] = j
	}

	build, ok := byName["build"]
	require.True(t, ok, "the switched-away job is missing: %+v", jobs)
	assert.False(t, build.Running(), "the switched-away job should be closed")
	// The interval is 30s at 2 cores, and a handover bills it forwards, to the
	// job the runner moved to.
	assert.InDelta(t, 60, byName["test"].CPUSeconds, 1e-9, "the final interval vanished at the handover")
	assert.Zero(t, build.CPUSeconds, "the handover interval was billed backwards, to the job the runner had already left")

	// And the fleet's cost accounting as a whole must not have lost it either.
	repos, err := s.RepoConsumption(ctx, Range{From: base.Add(-time.Hour), To: base.Add(time.Hour), Points: 10})
	require.NoError(t, err, "RepoConsumption")
	require.Len(t, repos, 1, "want 1 repo: %+v", repos)
	assert.InDelta(t, 60, repos[0].CPUSeconds, 1e-9, "repo total lost the interval: %+v", repos[0])
}

// TestHandoverIsNotBilledToThePreviousRepository pins where the straddling
// interval lands when the two jobs belong to different repositories, which on a
// persistent runner is the routine case. RecordSnapshot bills it forwards, so
// the successor's repository carries the whole interval — including the part
// its predecessor ran, which is why `e2e` here holds 240 core-seconds it cannot
// possibly have consumed by the instant it is asserted. The direction is a
// choice and not a measurement; flipping it would move the same core-seconds
// onto the other repository's panel, so it is pinned rather than argued.
func TestHandoverIsNotBilledToThePreviousRepository(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// Two scrapes on the first job, so it accrues a total of its own.
	first := busyRunner("runner-a", base, 1.0)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, first)), "first snapshot")
	second := base.Add(30 * time.Second)
	held := busyRunner("runner-a", second, 1.0)
	held.CreatedAt, held.Job = first.CreatedAt, first.Job
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(second, held)), "second snapshot")

	// The same persistent runner picks up another repository's job at this
	// scrape, and the reading taken here is a heavy one. The interval it is
	// integrated over belongs entirely to the job that just ended: `e2e` starts
	// at this instant.
	third := base.Add(60 * time.Second)
	next := busyRunner("runner-a", third, 8.0)
	next.CreatedAt = first.CreatedAt
	next.Job.Repository = "acme/frontend"
	next.Job.RunID = 43
	next.Job.Name = "e2e"
	next.Job.StartedAt = third
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(third, next)), "third snapshot")

	repos, err := s.RepoConsumption(ctx, Range{From: base.Add(-time.Hour), To: base.Add(time.Hour), Points: 10})
	require.NoError(t, err, "RepoConsumption")
	require.Len(t, repos, 2, "want both repositories: %+v", repos)
	byRepo := map[string]RepoTotal{}
	for _, rt := range repos {
		byRepo[rt.Repository] = rt
	}

	assert.InDelta(t, 30, byRepo["acme/api"].CPUSeconds, 1e-9,
		"the old repository was billed for the interval that straddled the handover: %+v", repos)
	assert.InDelta(t, 240, byRepo["acme/frontend"].CPUSeconds, 1e-9,
		"the handover interval did not land on the job the runner had moved to: %+v", repos)
	// Whatever the attribution, the fleet must not lose or invent core-seconds.
	assert.InDelta(t, 270, byRepo["acme/api"].CPUSeconds+byRepo["acme/frontend"].CPUSeconds, 1e-9,
		"the fleet total changed: %+v", repos)
}

// TestBulkUpsertAccumulatesPerRow pins the other half of that: one statement
// carries an increment for every runner in the snapshot and shares a single
// conflict clause between them, so an increment that resolved to a literal, or
// to some other row's value, would look fine with one runner and be wrong with
// two.
func TestBulkUpsertAccumulatesPerRow(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// Two runners, two jobs, deliberately different CPU readings.
	scrape := func(at time.Time) fleet.Snapshot {
		a := busyRunner("runner-a", at, 1.0)
		a.CreatedAt = base.Add(-time.Minute)
		b := busyRunner("runner-b", at, 3.0)
		b.CreatedAt = base.Add(-time.Minute)
		b.Job.RunID = 43
		b.Job.Name = "test"
		return snapshot(at, a, b)
	}
	require.NoError(t, s.RecordSnapshot(ctx, scrape(base)), "first snapshot")
	require.NoError(t, s.RecordSnapshot(ctx, scrape(base.Add(30*time.Second))), "second snapshot")

	jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
	require.NoError(t, err, "JobsForSet")
	require.Len(t, jobs, 2, "want both jobs: %+v", jobs)
	byName := map[string]JobRecord{}
	for _, j := range jobs {
		byName[j.Job] = j
	}
	assert.InDelta(t, 30, byName["build"].CPUSeconds, 1e-9, "runner-a's job took someone else's increment: %+v", jobs)
	assert.InDelta(t, 90, byName["test"].CPUSeconds, 1e-9, "runner-b's job took someone else's increment: %+v", jobs)
}

// TestDuplicateRunnerInOneSnapshotIsBilledOnce covers the hazard that comes
// with accumulating in the database: a runner listed twice would resolve
// against the row its own statement had just inserted, and the increment would
// land twice. The sibling write paths converge on a repeat by construction —
// samples take the new value, churn ignores the conflict — so this was the one
// path that had to be made to.
func TestDuplicateRunnerInOneSnapshotIsBilledOnce(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, busyRunner("runner-a", base, 2.0))), "first snapshot")

	at := base.Add(30 * time.Second)
	dup := busyRunner("runner-a", at, 2.0)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, dup, dup)), "duplicated snapshot")

	jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
	require.NoError(t, err, "JobsForSet")
	require.Len(t, jobs, 1, "want 1 job: %+v", jobs)
	assert.InDelta(t, 60, jobs[0].CPUSeconds, 1e-9, "30s at 2 cores, billed once: %+v", jobs)

	repos, err := s.RepoConsumption(ctx, Range{From: base.Add(-time.Hour), To: base.Add(time.Hour), Points: 10})
	require.NoError(t, err, "RepoConsumption")
	require.Len(t, repos, 1, "want 1 repo: %+v", repos)
	assert.InDelta(t, 60, repos[0].CPUSeconds, 1e-9, "the repo total was billed twice: %+v", repos[0])
	assert.InDelta(t, 30*fleet.GiB, repos[0].MemByteSeconds, 1, "memory was billed twice: %+v", repos[0])

	st, err := s.Stats(ctx)
	require.NoError(t, err, "Stats")
	assert.Equal(t, int64(1), st.Jobs, "want 1 job row")
	assert.Equal(t, int64(1), st.Phases, "want 1 phase")
	assert.Equal(t, int64(1), st.ChurnEvents, "want 1 churn event")
}

// TestJobCostSurvivesAProcessRestart pins the cost columns as accumulating
// rather than as a mirror of an in-memory running total. A restarted process
// knows nothing about a job that is still running, and writing its empty
// accumulator over the row would erase every core-second recorded before it.
func TestJobCostSurvivesAProcessRestart(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "arc-ui.db")
	open := func() *Store {
		s, err := Open(ctx, path, zerolog.Nop())
		require.NoError(t, err, "Open")
		return s
	}
	// The job is the same throughout: one long build that outlives the process.
	scrape := func(s *Store, at time.Time, createdAt time.Time, job fleet.Job) {
		t.Helper()
		r := busyRunner("runner-a", at, 2.0)
		r.CreatedAt, r.Job = createdAt, job
		require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, r)), "snapshot at %s", at)
	}
	cpuSeconds := func(s *Store) float64 {
		t.Helper()
		jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
		require.NoError(t, err, "JobsForSet")
		require.Len(t, jobs, 1, "want 1 job: %+v", jobs)
		return jobs[0].CPUSeconds
	}

	proto := busyRunner("runner-a", base, 2.0)

	s := open()
	scrape(s, base, proto.CreatedAt, proto.Job)
	scrape(s, base.Add(30*time.Second), proto.CreatedAt, proto.Job)
	require.InDelta(t, 60, cpuSeconds(s), 1e-9, "30s at 2 cores")
	require.NoError(t, s.Close(), "close before the restart")

	// Restart. The interval spanning it is genuinely unknowable — there is no
	// previous observation to integrate from — but the total already written
	// is not in question.
	s = open()
	t.Cleanup(func() { assert.NoError(t, s.Close(), "Close") })
	scrape(s, base.Add(60*time.Second), proto.CreatedAt, proto.Job)
	assert.InDelta(t, 60, cpuSeconds(s), 1e-9, "the first scrape after a restart erased the cost recorded before it")

	scrape(s, base.Add(90*time.Second), proto.CreatedAt, proto.Job)
	assert.InDelta(t, 120, cpuSeconds(s), 1e-9, "cost accounting did not resume after the restart")
}

// TestRetentionDoesNotOverflowTheAbandonedJobWindow pins the one arithmetic
// hazard in the sweep list: maxJobRuntime is added to a configured window, and
// time.Duration is int64 nanoseconds, so a window near the ceiling wraps
// negative — which unixCutoff turns into a cutoff in the *future*, matching
// every open job row including one that started seconds ago.
func TestRetentionDoesNotOverflowTheAbandonedJobWindow(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	now := base
	seedJob(t, s, "started-a-minute-ago", now.Add(-time.Minute), time.Time{})

	ret := defaultRetention
	ret.Scope5m = math.MaxInt64 - time.Hour // ~292 years, within maxJobRuntime of the ceiling

	require.NoError(t, s.Compact(t.Context(), now, ret), "Compact")

	var n int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM job_observations`).Scan(&n), "count jobs")
	assert.Equal(t, 1, n, "a job that started a minute ago was swept by a retention window of ~292 years")
}

// TestRetentionExpiresAJobWithANegativeFinishedAt closes the gap between the
// two job sweeps. They partition the table on finished_at > 0 and finished_at
// = 0; a row on the wrong side of both is immortal, which is the one property
// no row in this database may have.
func TestRetentionExpiresAJobWithANegativeFinishedAt(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	now := base
	seedJob(t, s, "negative-finish", now.Add(-60*24*time.Hour), time.Unix(-1, 0))

	require.NoError(t, s.Compact(t.Context(), now, defaultRetention), "Compact")

	var n int
	require.NoError(t, s.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM job_observations`).Scan(&n), "count jobs")
	assert.Zero(t, n, "a job row with a negative finished_at is matched by neither sweep and never expires")
}

func TestRetentionZeroKeepsEverything(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	now := base
	seed(t, s, ScopeRunner, "runner-a", MetricCPUUsed, TierRaw, now.Add(-400*24*time.Hour), time.Second, 1, func(int) float64 { return 1 })

	// A zero window is the documented escape hatch for "keep forever".
	require.NoError(t, s.Compact(t.Context(), now, Retention{}), "Compact")
	assert.Equal(t, 1, countTier(t, s, TierRaw, ScopeRunner), "want 1 row kept under a zero retention")
}

func TestChurnAndThroughputFromConsecutiveSnapshots(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	a := busyRunner("runner-a", base, 1.0)
	b := busyRunner("runner-b", base, 2.0)
	b.Job.RunID = 43
	b.Job.Name = "test"
	c := busyRunner("runner-c", base, 1.0)
	c.Job.RunID = 44
	c.Job.Name = "lint"

	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, a, b, c)), "first snapshot")

	// runner-b finishes cleanly; runner-c is observed failing before it goes.
	next := base.Add(30 * time.Second)
	cFailed := c
	cFailed.State = fleet.StateFailed
	cFailed.FailureReason = "OOMKilled"
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(next, a, cFailed)), "second snapshot")
	last := base.Add(60 * time.Second)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(last, a)), "third snapshot")

	r := Range{From: base.Add(-5 * time.Minute), To: base.Add(5 * time.Minute), Points: 20}

	created, terminated, err := s.Churn(ctx, "linux-x64", r)
	require.NoError(t, err, "Churn")
	require.Len(t, terminated, len(created), "churn series lengths differ")
	assert.InDelta(t, 3, total(created), 1e-9, "want 3 created")
	assert.InDelta(t, 2, total(terminated), 1e-9, "want 2 terminated")

	ok, failed, err := s.Throughput(ctx, "linux-x64", r)
	require.NoError(t, err, "Throughput")
	assert.InDelta(t, 1, total(ok), 1e-9, "want 1 completed (runner-b)")
	assert.InDelta(t, 1, total(failed), 1e-9, "want 1 failed (runner-c)")

	// Fleet scope must see the same events as the only set does.
	fleetCreated, _, err := s.Churn(ctx, "", r)
	require.NoError(t, err, "Churn(fleet)")
	assert.InDelta(t, 3, total(fleetCreated), 1e-9, "want 3 created at fleet scope")
}

func total(pts []Point) float64 {
	var sum float64
	for _, p := range pts {
		sum += p.Value
	}
	return sum
}

func TestJobsPhasesAndRepoConsumption(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// Two scrapes thirty seconds apart, so a job accrues real cpu-seconds.
	a := busyRunner("runner-a", base, 2.0)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, a)), "first snapshot")
	next := base.Add(30 * time.Second)
	a2 := busyRunner("runner-a", next, 2.0)
	a2.CreatedAt = a.CreatedAt
	a2.Job = a.Job
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(next, a2)), "second snapshot")
	// Runner goes idle, dropping its job.
	idleAt := base.Add(60 * time.Second)
	idle := a2
	idle.State = fleet.StateIdle
	idle.Job = fleet.Job{}
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(idleAt, idle)), "third snapshot")

	jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
	require.NoError(t, err, "JobsForSet")
	require.Len(t, jobs, 1, "want 1 job: %+v", jobs)
	j := jobs[0]
	assert.Equal(t, "runner-a", j.Runner, "job identity wrong: %+v", j)
	assert.Equal(t, "acme/api", j.Repository, "job identity wrong: %+v", j)
	assert.Equal(t, "build", j.Job, "job identity wrong: %+v", j)
	// 30s at 2 cores between the first and second scrape.
	assert.InDelta(t, 60, j.CPUSeconds, 1e-9, "want 60 cpu seconds")
	assert.False(t, j.Running(), "job should be closed once the runner moved off it")
	assert.True(t, j.Succeeded, "job should be recorded as succeeded")

	phases, err := s.PhasesForRunner(ctx, "runner-a")
	require.NoError(t, err, "PhasesForRunner")
	require.Len(t, phases, 2, "want busy then idle: %+v", phases)
	assert.Equal(t, string(fleet.StateBusy), phases[0].Phase, "phase order wrong: %+v", phases)
	assert.Equal(t, string(fleet.StateIdle), phases[1].Phase, "phase order wrong: %+v", phases)
	assert.Equal(t, 30*time.Second, phases[0].Duration(), "want a 30s busy phase")

	repos, err := s.RepoConsumption(ctx, Range{From: base.Add(-time.Hour), To: base.Add(time.Hour), Points: 10})
	require.NoError(t, err, "RepoConsumption")
	require.Len(t, repos, 1, "want 1 repo: %+v", repos)
	assert.Equal(t, "acme/api", repos[0].Repository, "repo total wrong: %+v", repos[0])
	assert.Equal(t, 1, repos[0].Jobs, "repo total wrong: %+v", repos[0])
	assert.InDelta(t, 60, repos[0].CPUSeconds, 1e-9, "repo total wrong: %+v", repos[0])
}

func TestRecordSnapshotDoesNotIntegrateAcrossALongGap(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	a := busyRunner("runner-a", base, 2.0)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, a)), "first snapshot")
	// An hour later. Integrating across this would credit the job with two
	// core-hours it never used.
	late := base.Add(time.Hour)
	a2 := busyRunner("runner-a", late, 2.0)
	a2.CreatedAt = a.CreatedAt
	a2.Job = a.Job
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(late, a2)), "second snapshot")

	jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
	require.NoError(t, err, "JobsForSet")
	require.Len(t, jobs, 1, "want 1 job")
	assert.Zero(t, jobs[0].CPUSeconds, "want 0 cpu seconds across an implausible gap")
}

func TestEmptyStoreReturnsEmptyNotError(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()
	r := Range{From: base.Add(-time.Hour), To: base, Points: 30}

	t.Run("series", func(t *testing.T) {
		t.Parallel()
		got, err := s.Series(ctx, ScopeFleet, "", []Metric{MetricBusy, MetricCPUUsed}, r)
		require.NoError(t, err, "Series")
		for m, pts := range got {
			assert.Empty(t, pts, "%s returned points on an empty store", m)
		}
	})

	t.Run("churn", func(t *testing.T) {
		t.Parallel()
		created, terminated, err := s.Churn(ctx, "linux-x64", r)
		require.NoError(t, err, "Churn")
		assert.Zero(t, total(created), "empty store reported churn")
		assert.Zero(t, total(terminated), "empty store reported churn")
	})

	t.Run("throughput", func(t *testing.T) {
		t.Parallel()
		ok, failed, err := s.Throughput(ctx, "", r)
		require.NoError(t, err, "Throughput")
		assert.Zero(t, total(ok), "empty store reported throughput")
		assert.Zero(t, total(failed), "empty store reported throughput")
	})

	t.Run("repos", func(t *testing.T) {
		t.Parallel()
		repos, err := s.RepoConsumption(ctx, r)
		require.NoError(t, err, "RepoConsumption")
		assert.Empty(t, repos, "want no repos")
	})

	t.Run("jobs", func(t *testing.T) {
		t.Parallel()
		jobs, err := s.JobsForSet(ctx, "linux-x64", 10)
		require.NoError(t, err, "JobsForSet")
		assert.Empty(t, jobs, "want no jobs")
	})

	t.Run("phases", func(t *testing.T) {
		t.Parallel()
		phases, err := s.PhasesForRunner(ctx, "runner-a")
		require.NoError(t, err, "PhasesForRunner")
		assert.Empty(t, phases, "want no phases")
	})

	t.Run("compact", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, s.Compact(ctx, base, defaultRetention), "Compact")
	})

	t.Run("stats", func(t *testing.T) {
		t.Parallel()
		st, err := s.Stats(ctx)
		require.NoError(t, err, "Stats")
		assert.Zero(t, st.Rows, "want no rows")
		assert.Positive(t, st.SizeBytes, "size should count the file that exists on disk")
		assert.True(t, st.Oldest.IsZero(), "oldest = %s, want zero", st.Oldest)
	})
}

// TestRecordEmptyFleet covers the scaled-to-zero cluster: no sets, no runners,
// nothing to aggregate. It gets its own store because it writes, and the
// empty-store assertions above must not race with it.
func TestRecordEmptyFleet(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	require.NoError(t, s.RecordSnapshot(t.Context(), fleet.Snapshot{At: base}), "RecordSnapshot of an empty fleet")

	// The counts are still real measurements — a fleet of zero runners is not
	// the same as no measurement — so they are stored.
	got, err := s.Series(t.Context(), ScopeFleet, "", []Metric{MetricRunners},
		Range{From: base.Add(-time.Minute), To: base.Add(time.Minute), Points: 60})
	require.NoError(t, err, "Series")
	pts := got[MetricRunners]
	require.Len(t, pts, 1, "runners = %v, want a single zero", pts)
	assert.Zero(t, pts[0].Value, "runners = %v, want a single zero", pts)
}

func TestQueriesRejectDegenerateRanges(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	tests := []struct {
		name string
		r    Range
	}{
		{"zero range", Range{}},
		{"inverted", Range{From: base, To: base.Add(-time.Hour), Points: 10}},
		{"empty window", Range{From: base, To: base, Points: 10}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := s.Series(ctx, ScopeFleet, "", []Metric{MetricBusy}, tc.r)
			assert.NoError(t, err, "Series")
			_, _, err = s.Churn(ctx, "", tc.r)
			assert.NoError(t, err, "Churn")
			_, _, err = s.Throughput(ctx, "", tc.r)
			assert.NoError(t, err, "Throughput")
			_, err = s.RepoConsumption(ctx, tc.r)
			assert.NoError(t, err, "RepoConsumption")
		})
	}
}

func TestStatsCountsRows(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	require.NoError(t, s.RecordSnapshot(t.Context(), snapshot(base, busyRunner("runner-a", base, 1))), "RecordSnapshot")

	st, err := s.Stats(t.Context())
	require.NoError(t, err, "Stats")
	assert.NotZero(t, st.Samples, "no samples counted")
	assert.Equal(t, st.Samples+st.Jobs+st.Phases+st.ChurnEvents, st.Rows, "Rows should be the sum of the per-table counts")
	assert.False(t, st.Oldest.IsZero(), "Oldest should be set once samples exist")
	assert.NotEmpty(t, st.Path, "Path should be reported")
}

// TestCompactedHistoryIsQueryable is the end-to-end shape of the thing: scrape
// for an hour, compact, and ask for the hour back at a resolution that only
// the rolled-up tier can serve.
func TestCompactedHistoryIsQueryable(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// One hour of scrapes at 15s, with a steady two busy runners.
	start := base.Add(-time.Hour)
	for i := range 240 {
		at := start.Add(time.Duration(i) * 15 * time.Second)
		snap := snapshot(at,
			busyRunner("runner-a", at, 1),
			busyRunner("runner-b", at, 1),
		)
		snap.Runners[1].Job.RunID = 43
		require.NoError(t, s.RecordSnapshot(ctx, snap), "snapshot %d", i)
	}

	rawRows := countTier(t, s, TierRaw, ScopeFleet)
	require.NoError(t, s.Compact(ctx, base, defaultRetention), "Compact")
	rolled := countTier(t, s, Tier1m, ScopeFleet)
	assert.NotZero(t, rolled, "1m tier has no rows against %d raw; rollup should be strictly coarser", rawRows)
	assert.Less(t, rolled, rawRows, "1m tier has %d rows against %d raw; rollup should be strictly coarser", rolled, rawRows)

	// A one-hour window over sixty points lands on the 1m tier, which only
	// exists because compaction ran.
	got, err := s.Series(ctx, ScopeFleet, "", []Metric{MetricBusy},
		Range{From: start, To: base, Points: 60})
	require.NoError(t, err, "Series")
	pts := got[MetricBusy]
	require.GreaterOrEqual(t, len(pts), 55, "got %d points, want about 60", len(pts))
	require.LessOrEqual(t, len(pts), 61, "got %d points, want about 60", len(pts))
	for _, p := range pts {
		require.InDelta(t, 2, p.Value, 1e-9, "busy = %v at %s, want 2 throughout", p.Value, p.At)
	}
}
