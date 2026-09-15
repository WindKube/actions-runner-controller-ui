package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"testing"
	"time"

	entsql "entgo.io/ent/dialect/sql"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
	"arc-ui/internal/store/ent"
	"arc-ui/internal/store/ent/jobobservation"
)

// defaultRetention mirrors config.Config's defaults, so the tests exercise the
// tier windows the binary actually ships with.
var defaultRetention = Retention{
	RunnerRaw: 15 * time.Minute,
	ScopeRaw:  6 * time.Hour,
	Scope1m:   7 * 24 * time.Hour,
	Scope5m:   30 * 24 * time.Hour,
	Scope1h:   400 * 24 * time.Hour,

	JobSamples: 30 * 24 * time.Hour,
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
	for _, table := range []string{
		"samples", "job_observations", "churn_events", "runner_failures",
	} {
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

// TestSwitchedJobKeepsItsFinalInterval covers the persistent-runner handover: the
// runner is seen on one job and then on the next, and the interval between those
// two scrapes must not fall between the two rows. It lands on the new job — see
// TestHandoverIsNotBilledToThePreviousRepository for why the direction matters.
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

	jobs := jobsForSet(t, s, "linux-x64")
	require.Len(t, jobs, 2, "want both jobs: %+v", jobs)
	byName := map[string]*ent.JobObservation{}
	for _, j := range jobs {
		byName[j.JobName] = j
	}

	build, ok := byName["build"]
	require.True(t, ok, "the switched-away job is missing: %+v", jobs)
	assert.NotZero(t, build.FinishedAt, "the switched-away job should be closed")
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
// persistent runner is routine. RecordSnapshot bills it forwards, so the
// successor's repository carries the whole interval — which is why `e2e` here
// holds 240 core-seconds it cannot possibly have consumed. The direction is a
// choice, not a measurement, so it is pinned rather than argued.
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
// conflict clause between them, so an increment that resolved to a literal, or to
// some other row's value, would look fine with one runner and be wrong with two.
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

	jobs := jobsForSet(t, s, "linux-x64")
	require.Len(t, jobs, 2, "want both jobs: %+v", jobs)
	byName := map[string]*ent.JobObservation{}
	for _, j := range jobs {
		byName[j.JobName] = j
	}
	assert.InDelta(t, 30, byName["build"].CPUSeconds, 1e-9, "runner-a's job took someone else's increment: %+v", jobs)
	assert.InDelta(t, 90, byName["test"].CPUSeconds, 1e-9, "runner-b's job took someone else's increment: %+v", jobs)
}

// TestDuplicateRunnerInOneSnapshotIsBilledOnce covers the hazard that comes with
// accumulating in the database: a runner listed twice would resolve against the row
// its own statement had just inserted, and the increment would land twice. The
// sibling write paths converge on a repeat by construction — samples take the new
// value, churn ignores the conflict — so this was the one path that had to be made to.
func TestDuplicateRunnerInOneSnapshotIsBilledOnce(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, busyRunner("runner-a", base, 2.0))), "first snapshot")

	at := base.Add(30 * time.Second)
	dup := busyRunner("runner-a", at, 2.0)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, dup, dup)), "duplicated snapshot")

	jobs := jobsForSet(t, s, "linux-x64")
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
		jobs := jobsForSet(t, s, "linux-x64")
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

// TestRetentionDoesNotOverflowTheAbandonedJobWindow pins the one arithmetic hazard
// in the sweep list: maxJobRuntime is added to a configured window, and
// time.Duration is int64 nanoseconds, so a window near the ceiling wraps negative —
// which unixCutoff turns into a cutoff in the *future*, matching every open job row.
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

	jobs := jobsForSet(t, s, "linux-x64")
	require.Len(t, jobs, 1, "want 1 job: %+v", jobs)
	j := jobs[0]
	assert.Equal(t, "runner-a", j.RunnerName, "job identity wrong: %+v", j)
	assert.Equal(t, "acme/api", j.Repository, "job identity wrong: %+v", j)
	assert.Equal(t, "build", j.JobName, "job identity wrong: %+v", j)
	// 30s at 2 cores between the first and second scrape.
	assert.InDelta(t, 60, j.CPUSeconds, 1e-9, "want 60 cpu seconds")
	assert.NotZero(t, j.FinishedAt, "job should be closed once the runner moved off it")
	assert.True(t, j.Succeeded, "job should be recorded as succeeded")

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

	jobs := jobsForSet(t, s, "linux-x64")
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
		jobs := jobsForSet(t, s, "linux-x64")
		assert.Empty(t, jobs, "want no jobs")
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
	require.NoError(t, s.RecordSnapshot(t.Context(), snapshot(base,
		busyRunner("runner-a", base, 1),
		failedRunner("runner-b", "Evicted", base, true),
	)), "RecordSnapshot")

	st, err := s.Stats(t.Context())
	require.NoError(t, err, "Stats")
	assert.NotZero(t, st.Samples, "no samples counted")
	assert.NotZero(t, st.Failures, "no failures counted")
	// Every table the store writes to has to be in the footer's total, or the
	// panel understates a database it is there to report the size of.
	assert.Equal(t, st.Samples+st.Jobs+st.JobSamples+st.ChurnEvents+st.Failures, st.Rows,
		"Rows should be the sum of the per-table counts")
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

// failedRunner is a runner whose pod is visibly broken.
func failedRunner(name, reason string, at time.Time, severe bool) fleet.Runner {
	state := fleet.StateFailed
	if !severe {
		state = fleet.StateIdle
	}
	return fleet.Runner{
		Name:          name,
		Namespace:     "arc-runners",
		SetName:       "linux-x64",
		State:         state,
		CreatedAt:     at.Add(-2 * time.Minute),
		FailureReason: reason,
		FailedAt:      at,
	}
}

// The failure lane was derived from the live snapshot, so a failure disappeared
// the instant ARC deleted the EphemeralRunner — which for a crash-looping runner
// is seconds. Persisting it is the difference between a lane that reports the
// selected window and one that reports only whatever is broken this instant.
func TestFailuresOutliveTheRunnerThatFailed(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	failedAt := base.Add(-time.Minute)
	require.NoError(t, s.RecordSnapshot(ctx,
		snapshot(base, failedRunner("runner-x", "ImagePullBackOff", failedAt, true))), "record the failure")
	// ARC deletes a finished ephemeral runner, so the next scrape has no trace
	// of it at all.
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base.Add(15*time.Second))), "record its absence")

	got, total, err := s.Failures(ctx, "", Range{From: base.Add(-time.Hour), To: base.Add(time.Hour)}, 10)
	require.NoError(t, err, "Failures")

	require.Len(t, got, 1, "want the failure kept after the runner disappeared")
	assert.Equal(t, "runner-x", got[0].Runner, "runner")
	assert.Equal(t, "linux-x64", got[0].Set, "set")
	assert.Equal(t, "ImagePullBackOff", got[0].Reason, "reason")
	assert.True(t, got[0].Severe, "a runner in the failed state is a severe failure")
	assert.Equal(t, failedAt.Unix(), got[0].At.Unix(),
		"want the observed failure time, not the scrape that noticed it")
	assert.Equal(t, int64(1), total, "want the window's total alongside the page")
}

// A broken runner keeps its reason for every scrape of its life. One row per
// scrape would bury every other failure in the window under the noisiest one.
func TestARepeatedFailureIsRecordedOnce(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	for i := range 4 {
		at := base.Add(time.Duration(i) * 15 * time.Second)
		require.NoError(t, s.RecordSnapshot(ctx,
			snapshot(at, failedRunner("runner-x", "ImagePullBackOff", base, true))), "scrape %d", i)
	}

	got, total, err := s.Failures(ctx, "", Range{From: base.Add(-time.Hour), To: base.Add(time.Hour)}, 10)
	require.NoError(t, err, "Failures")

	assert.Len(t, got, 1, "four scrapes of one broken runner recorded %d rows", len(got))
	assert.Equal(t, int64(1), total, "total")
}

// The lane shows a handful of rows but says how many there are, so the count has
// to come from the window rather than from the page.
func TestFailuresAreNewestFirstAndCappedWithoutCappingTheTotal(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base,
		failedRunner("runner-old", "Evicted", base.Add(-10*time.Minute), true),
		failedRunner("runner-mid", "OOMKilled", base.Add(-5*time.Minute), true),
		failedRunner("runner-new", "never registered", base.Add(-time.Minute), false),
	)), "RecordSnapshot")

	got, total, err := s.Failures(ctx, "", Range{From: base.Add(-time.Hour), To: base.Add(time.Hour)}, 2)
	require.NoError(t, err, "Failures")

	require.Len(t, got, 2, "want the page capped at the limit")
	assert.Equal(t, []string{"runner-new", "runner-mid"},
		[]string{got[0].Runner, got[1].Runner}, "want newest first")
	assert.False(t, got[0].Severe, "an idle runner with a reason is not a severe failure")
	assert.Equal(t, int64(3), total, "the total must count the window, not the page")
}

// A set filter scopes the lane the same way it scopes the charts.
func TestFailuresCanBeScopedToOneSet(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	other := failedRunner("runner-y", "Evicted", base, true)
	other.SetName = "arm64"
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base,
		failedRunner("runner-x", "OOMKilled", base, true), other)), "RecordSnapshot")

	got, total, err := s.Failures(ctx, "arm64", Range{From: base.Add(-time.Hour), To: base.Add(time.Hour)}, 10)
	require.NoError(t, err, "Failures")

	require.Len(t, got, 1, "want only the named set's failures")
	assert.Equal(t, "runner-y", got[0].Runner, "runner")
	assert.Equal(t, int64(1), total, "total must be scoped too")
}

// Failures borrow the 5-minute tier's window, which is the longest range the
// lane can be asked for. Without a sweep they would be the one table in this
// database that grows forever.
func TestRetentionExpiresFailures(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	stale := base.Add(-defaultRetention.Scope5m - time.Hour)
	require.NoError(t, s.RecordSnapshot(ctx,
		snapshot(stale, failedRunner("runner-stale", "Evicted", stale, true))), "record a stale failure")
	require.NoError(t, s.RecordSnapshot(ctx,
		snapshot(base, failedRunner("runner-fresh", "Evicted", base, true))), "record a fresh failure")

	require.NoError(t, s.Compact(ctx, base, defaultRetention), "Compact")

	got, _, err := s.Failures(ctx, "", Range{From: stale.Add(-time.Hour), To: base.Add(time.Hour)}, 10)
	require.NoError(t, err, "Failures")

	require.Len(t, got, 1, "want only the failure inside the window")
	assert.Equal(t, "runner-fresh", got[0].Runner, "the wrong row survived")
}

// ---------------------------------------------------------------------------
// Per-job resource samples
// ---------------------------------------------------------------------------

// jobSampleRows reads the raw table, because the point of most of these tests
// is how many rows exist and what is in them, not what a query projects.
func jobSampleRows(t *testing.T, s *Store, jobID int) []JobPoint {
	t.Helper()
	rows, err := s.db.QueryContext(t.Context(),
		`SELECT ts, cpu_cores, mem_bytes FROM job_samples WHERE job_id = ? ORDER BY ts`, jobID)
	require.NoError(t, err, "read job_samples")
	defer func() { assert.NoError(t, rows.Close(), "close rows") }()

	var out []JobPoint
	for rows.Next() {
		var (
			ts       int64
			cpu, mem float64
		)
		require.NoError(t, rows.Scan(&ts, &cpu, &mem), "scan job_sample")
		out = append(out, JobPoint{At: time.Unix(ts, 0).UTC(), CPU: cpu, Mem: mem})
	}
	require.NoError(t, rows.Err(), "job_sample rows")
	return out
}

// onlyJob returns the single job observation in the store, failing when there
// is not exactly one.
func onlyJob(t *testing.T, s *Store) JobRecord {
	t.Helper()
	jobs, _, err := s.Jobs(t.Context(), JobFilter{}, Range{From: base.Add(-time.Hour), To: base.Add(time.Hour)})
	require.NoError(t, err, "Jobs")
	require.Len(t, jobs, 1, "expected exactly one job observation")
	return jobs[0]
}

func TestJobSamplesRecordUsagePerBucket(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// Three scrapes inside one minute, then one in the next. The first four
	// readings must collapse to two rows, because the bucket is the unit of
	// storage and the scrape is not.
	for i, at := range []time.Time{
		base, base.Add(15 * time.Second), base.Add(30 * time.Second), base.Add(time.Minute),
	} {
		require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, busyRunner("runner-a", at, float64(i+1)))),
			"RecordSnapshot %d", i)
	}

	job := onlyJob(t, s)
	rows := jobSampleRows(t, s, job.ID)
	require.Len(t, rows, 2, "three scrapes in one minute and one in the next should be two buckets")

	assert.Equal(t, base.Unix(), rows[0].At.Unix(), "first bucket starts on the minute")
	// 1, 2 and 3 cores averaged.
	assert.InDelta(t, 2.0, rows[0].CPU, 1e-9, "readings in one bucket should be averaged")
	assert.InDelta(t, 4.0, rows[1].CPU, 1e-9, "the second bucket holds its own reading")
	assert.InDelta(t, float64(fleet.GiB), rows[0].Mem, 1e-3, "memory is averaged alongside CPU")
}

func TestJobSamplesAreNotWrittenWithoutMetrics(t *testing.T) {
	t.Parallel()

	s := newStore(t)

	// A runner holding a job that metrics-server has never answered for. A zero
	// row here would be drawn as a job that used no CPU, which is the one thing
	// the chart must not claim.
	r := busyRunner("runner-a", base, 0)
	r.CPU = fleet.Resources{Request: 2}
	r.Mem = fleet.Resources{Request: 4 * fleet.GiB}

	require.NoError(t, s.RecordSnapshot(t.Context(), snapshot(base, r)), "RecordSnapshot")

	job := onlyJob(t, s)
	assert.Empty(t, jobSampleRows(t, s, job.ID), "an unscraped runner should contribute no samples")
}

func TestJobSamplesSurviveAReplayedSnapshot(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()
	snap := snapshot(base, busyRunner("runner-a", base, 2))

	require.NoError(t, s.RecordSnapshot(ctx, snap), "first record")
	require.NoError(t, s.RecordSnapshot(ctx, snap), "replayed record")

	job := onlyJob(t, s)
	rows := jobSampleRows(t, s, job.ID)
	require.Len(t, rows, 1, "replaying one snapshot must not add a bucket")
	// The same reading folded into its own mean is still that reading, which is
	// what makes a restart that re-observes the fleet harmless here.
	assert.InDelta(t, 2.0, rows[0].CPU, 1e-9, "a replayed reading should not move the average")
}

func TestJobKeepsWhatItsRunnerReserved(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	r := busyRunner("runner-a", base, 2)
	r.CPU.Limit, r.Mem.Limit = 4, 8*fleet.GiB

	require.NoError(t, s.RecordSnapshot(t.Context(), snapshot(base, r)), "RecordSnapshot")

	job := onlyJob(t, s)
	assert.InDelta(t, 2.0, job.CPURequest, 1e-9, "cpu request")
	assert.InDelta(t, 4.0, job.CPULimit, 1e-9, "cpu limit")
	assert.InDelta(t, 4*fleet.GiB, job.MemRequest, 1e-3, "memory request")
	assert.InDelta(t, 8*fleet.GiB, job.MemLimit, 1e-3, "memory limit")

	// The list and the detail view read the job through different code paths,
	// and the chart's reference lines come from the second one.
	byID, ok, err := s.Job(t.Context(), job.ID)
	require.NoError(t, err, "Job")
	require.True(t, ok, "job %d should exist", job.ID)
	assert.Equal(t, job, byID, "Job by id should agree with the list")
}

func TestJobReservationsSurviveAScrapeThatCannotSeeThem(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	r := busyRunner("runner-a", base, 2)
	r.CPU.Limit = 4
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, r)), "first record")

	// The next scrape finds neither the pod nor its scale set in the informer
	// cache, so it reports no reservations at all. Letting that overwrite would
	// strip the chart's reference lines off a job that is still running.
	blind := busyRunner("runner-a", base.Add(time.Minute), 2)
	blind.CPU.Request, blind.CPU.Limit = 0, 0
	blind.Mem.Request = 0
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base.Add(time.Minute), blind)), "blind record")

	job := onlyJob(t, s)
	assert.InDelta(t, 2.0, job.CPURequest, 1e-9, "cpu request should survive a blind scrape")
	assert.InDelta(t, 4.0, job.CPULimit, 1e-9, "cpu limit should survive a blind scrape")
	assert.InDelta(t, 4*fleet.GiB, job.MemRequest, 1e-3, "memory request should survive a blind scrape")
}

func TestJobReservationsTakeALaterReading(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// The first scrape saw the job before it could read the pod; the second
	// one can. Zero is "not observed yet", not a value worth defending.
	blind := busyRunner("runner-a", base, 2)
	blind.CPU.Request, blind.Mem.Request = 0, 0
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(base, blind)), "blind record")
	require.NoError(t, s.RecordSnapshot(ctx,
		snapshot(base.Add(time.Minute), busyRunner("runner-a", base.Add(time.Minute), 2))), "second record")

	job := onlyJob(t, s)
	assert.InDelta(t, 2.0, job.CPURequest, 1e-9, "a later scrape should fill in what the first missed")
	assert.InDelta(t, 4*fleet.GiB, job.MemRequest, 1e-3, "memory request should be filled in too")
}

func TestJobSeriesBucketsToTheRequestedPoints(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	for i := range 10 {
		at := base.Add(time.Duration(i) * time.Minute)
		require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, busyRunner("runner-a", at, 1))),
			"RecordSnapshot %d", i)
	}

	job := onlyJob(t, s)

	full, err := s.JobSeries(ctx, job.ID, Range{From: base, To: base.Add(10 * time.Minute), Points: 60})
	require.NoError(t, err, "JobSeries")
	assert.Len(t, full, 10, "a bucket finer than the storage resolution returns the stored rows")

	coarse, err := s.JobSeries(ctx, job.ID, Range{From: base, To: base.Add(10 * time.Minute), Points: 2})
	require.NoError(t, err, "JobSeries coarse")
	assert.Len(t, coarse, 2, "a coarse request should group the stored rows")
}

func TestJobSeriesOfAnUnknownJobIsEmptyNotAnError(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	points, err := s.JobSeries(t.Context(), 12345, Range{From: base, To: base.Add(time.Hour), Points: 10})
	require.NoError(t, err, "JobSeries")
	assert.Empty(t, points, "an unknown job has no samples")
}

func TestJobSamplesExpireWithTheirJob(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// A job that ran, and finished, well outside every retention window.
	old := base.Add(-60 * 24 * time.Hour)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(old, busyRunner("runner-a", old, 1))), "record old")
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(old.Add(time.Minute))), "runner departs")

	var before int64
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_samples`).Scan(&before))
	require.NotZero(t, before, "the old job should have left samples to sweep")

	require.NoError(t, s.Compact(ctx, base, defaultRetention), "Compact")

	var jobs, samples int64
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_observations`).Scan(&jobs))
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_samples`).Scan(&samples))
	assert.Zero(t, jobs, "the expired job should be gone")
	assert.Zero(t, samples, "its samples must not outlive it — an orphan here is immortal")
}

func TestJobSamplesHonourTheirOwnRetention(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	// A job still well inside the job window, whose samples are outside the
	// shorter sample window. This is the knob an operator turns to keep a
	// month of job rows and a day of the series behind them.
	at := base.Add(-48 * time.Hour)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, busyRunner("runner-a", at, 1))), "record")
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at.Add(time.Minute))), "runner departs")

	ret := defaultRetention
	ret.JobSamples = 24 * time.Hour
	require.NoError(t, s.Compact(ctx, base, ret), "Compact")

	var jobs, samples int64
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_observations`).Scan(&jobs))
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_samples`).Scan(&samples))
	assert.NotZero(t, jobs, "the job itself is still inside its own window")
	assert.Zero(t, samples, "its samples are outside the sample window and should be swept")
}

// ---------------------------------------------------------------------------
// Job and workflow listings
// ---------------------------------------------------------------------------

// jobAt records one finished job, so the listing tests can build a window out
// of several without driving the whole snapshot diff for each.
func jobAt(t *testing.T, s *Store, repo, workflow, name string, runID int64, start time.Time, ok bool) {
	t.Helper()
	finished := start.Add(time.Minute).Unix()
	succeeded := 0
	if ok {
		succeeded = 1
	}
	_, err := s.db.ExecContext(t.Context(),
		`INSERT INTO job_observations
		   (runner_name, set_name, repository, workflow, job_name, run_id,
		    started_at, finished_at, succeeded, cpu_seconds, mem_byte_seconds)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		name+"-runner", "linux-x64", repo, workflow, name, runID,
		start.Unix(), finished, succeeded, 60.0, 60.0*fleet.GiB)
	require.NoError(t, err, "insert job observation")
}

// listRange is a window wide enough to hold everything the listing tests write.
func listRange() Range {
	return Range{From: base.Add(-2 * time.Hour), To: base.Add(2 * time.Hour), Points: 60}
}

func seedListings(t *testing.T, s *Store) {
	t.Helper()
	jobAt(t, s, "acme/api", "ci.yml", "build", 100, base.Add(-30*time.Minute), true)
	jobAt(t, s, "acme/api", "ci.yml", "test", 100, base.Add(-29*time.Minute), false)
	jobAt(t, s, "acme/web", "release.yml", "publish", 200, base.Add(-20*time.Minute), true)
	jobAt(t, s, "acme/web", "nightly.yml", "e2e_suite", 0, base.Add(-10*time.Minute), true)
}

func TestJobsFilters(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	seedListings(t, s)

	for _, tc := range []struct {
		name   string
		filter JobFilter
		want   []string
	}{
		{"unfiltered", JobFilter{}, []string{"e2e_suite", "publish", "test", "build"}},
		{"by repository", JobFilter{Repository: "acme/api"}, []string{"test", "build"}},
		{"by workflow", JobFilter{Workflow: "release.yml"}, []string{"publish"}},
		{"by run", JobFilter{RunID: 200}, []string{"publish"}},
		{"failed only", JobFilter{Outcome: OutcomeFailed}, []string{"test"}},
		{"running only", JobFilter{Outcome: OutcomeRunning}, nil},
		{"search matches the job name", JobFilter{Search: "BUIL"}, []string{"build"}},
		{"search matches the workflow", JobFilter{Search: "nightly"}, []string{"e2e_suite"}},
		{"search matches the repository", JobFilter{Search: "acme/web"}, []string{"e2e_suite", "publish"}},
		{"search misses", JobFilter{Search: "nothing-matches-this"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rows, total, err := s.Jobs(t.Context(), tc.filter, listRange())
			require.NoError(t, err, "Jobs")

			got := make([]string, 0, len(rows))
			for _, r := range rows {
				got = append(got, r.Job)
			}
			assert.Equal(t, tc.want, nilIfEmpty(got), "jobs, newest first")
			assert.Equal(t, int64(len(tc.want)), total, "the total should count the filtered window")
		})
	}
}

// A search box is free text, and LIKE's own wildcards are characters people
// type. Without escaping, "100%" matches every job and "job_1" matches "job-1".
func TestJobSearchTreatsWildcardsAsLiterals(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	jobAt(t, s, "acme/api", "ci.yml", "coverage 100% gate", 1, base.Add(-5*time.Minute), true)
	jobAt(t, s, "acme/api", "ci.yml", "build_release", 2, base.Add(-4*time.Minute), true)
	jobAt(t, s, "acme/api", "ci.yml", "build-release", 3, base.Add(-3*time.Minute), true)

	for _, tc := range []struct {
		name   string
		search string
		want   []string
	}{
		{"percent is a literal", "100%", []string{"coverage 100% gate"}},
		{"underscore is a literal", "build_release", []string{"build_release"}},
		{"backslash is a literal", `\`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rows, _, err := s.Jobs(t.Context(), JobFilter{Search: tc.search}, listRange())
			require.NoError(t, err, "Jobs")

			got := make([]string, 0, len(rows))
			for _, r := range rows {
				got = append(got, r.Job)
			}
			assert.Equal(t, tc.want, nilIfEmpty(got), "a wildcard typed into the box is a character")
		})
	}
}

func TestJobsCapsThePageButNotTheTotal(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	for i := range 12 {
		jobAt(t, s, "acme/api", "ci.yml", fmt.Sprintf("job-%02d", i), int64(i),
			base.Add(-time.Duration(i)*time.Minute), true)
	}

	rows, total, err := s.Jobs(t.Context(), JobFilter{Limit: 5}, listRange())
	require.NoError(t, err, "Jobs")
	assert.Len(t, rows, 5, "the page should honour the limit")
	assert.Equal(t, int64(12), total, "the total counts the window, not the page")
}

func TestWorkflowRunsAggregateTheirJobs(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	seedListings(t, s)

	runs, total, err := s.WorkflowRuns(t.Context(), JobFilter{}, listRange())
	require.NoError(t, err, "WorkflowRuns")
	require.Len(t, runs, 3, "four jobs across three runs")
	assert.Equal(t, int64(3), total, "total counts runs, not jobs")

	byWorkflow := map[string]WorkflowRun{}
	for _, r := range runs {
		byWorkflow[r.Workflow] = r
	}

	ci := byWorkflow["ci.yml"]
	assert.Equal(t, 2, ci.Jobs, "the ci run holds both of its jobs")
	assert.Equal(t, 1, ci.Failed, "one of them failed")
	assert.Zero(t, ci.Running, "both have finished")
	assert.False(t, ci.FinishedAt.IsZero(), "a run with nothing outstanding has an end")
	assert.InDelta(t, 120.0, ci.CPUSeconds, 1e-9, "cost sums across the run's jobs")

	// ARC reports no run id for some jobs. They must still be reachable rather
	// than silently dropped from the tab that is meant to list everything.
	nightly := byWorkflow["nightly.yml"]
	assert.Equal(t, int64(0), nightly.RunID, "a job with no run id keeps its own row")
	assert.Equal(t, 1, nightly.Jobs, "and its job is counted")
}

func TestWorkflowRunIsUnfinishedWhileAnyJobRuns(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	jobAt(t, s, "acme/api", "ci.yml", "build", 100, base.Add(-30*time.Minute), true)

	// A second job of the same run that has not finished. MAX(finished_at)
	// over the pair is the first job's completion, which is emphatically not
	// when the run ended.
	_, err := s.db.ExecContext(t.Context(),
		`INSERT INTO job_observations
		   (runner_name, set_name, repository, workflow, job_name, run_id,
		    started_at, finished_at, succeeded, cpu_seconds, mem_byte_seconds)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		"r2", "linux-x64", "acme/api", "ci.yml", "test", 100,
		base.Add(-29*time.Minute).Unix(), 0, 0, 0.0, 0.0)
	require.NoError(t, err, "insert unfinished job")

	runs, _, err := s.WorkflowRuns(t.Context(), JobFilter{}, listRange())
	require.NoError(t, err, "WorkflowRuns")
	require.Len(t, runs, 1, "one run")
	assert.Equal(t, 1, runs[0].Running, "the unfinished job should be counted")
	assert.True(t, runs[0].FinishedAt.IsZero(),
		"a run with a job still going has not finished, whatever MAX(finished_at) says")
}

func TestJobFacetsOfferOnlyWhatTheWindowHolds(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	seedListings(t, s)
	// Well outside the window the facets are asked for.
	jobAt(t, s, "acme/ancient", "old.yml", "gone", 1, base.Add(-30*24*time.Hour), true)

	facets, err := s.JobFacets(t.Context(), listRange())
	require.NoError(t, err, "JobFacets")
	assert.Equal(t, []string{"acme/api", "acme/web"}, facets.Repositories,
		"a dropdown must not offer a value that would match nothing")
	assert.Equal(t, []string{"ci.yml", "nightly.yml", "release.yml"}, facets.Workflows)
	assert.Equal(t, []string{"build", "e2e_suite", "publish", "test"}, facets.Jobs,
		"job names are a facet in their own right, not only a search term")
	assert.Equal(t, []string{"linux-x64"}, facets.Sets)
}

func TestJobFacetsCapKeepsTheMostRecent(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	// One more than the cap, oldest first, so the value that must survive is
	// the one an ORDER BY over the name alone would drop.
	for i := range MaxFacetValues + 1 {
		jobAt(t, s, "acme/api", "ci.yml", fmt.Sprintf("job-%04d", i), int64(i),
			base.Add(-time.Hour).Add(time.Duration(i)*time.Second), true)
	}

	facets, err := s.JobFacets(t.Context(), listRange())
	require.NoError(t, err, "JobFacets")
	require.Len(t, facets.Jobs, MaxFacetValues,
		"a month of a busy fleet's job names must not all reach the filter bar")
	assert.Equal(t, "job-0001", facets.Jobs[0], "the oldest name is the one dropped")
	assert.Equal(t, fmt.Sprintf("job-%04d", MaxFacetValues), facets.Jobs[MaxFacetValues-1])
	assert.True(t, slices.IsSorted(facets.Jobs), "the cap is by recency, the order is for reading")
}

func TestJobByIDReportsMissingRatherThanFailing(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	seedListings(t, s)

	_, ok, err := s.Job(t.Context(), 999999)
	require.NoError(t, err, "a missing job is not an error")
	assert.False(t, ok, "and reports itself missing")

	want := onlyJobNamed(t, s, "build")
	got, ok, err := s.Job(t.Context(), want.ID)
	require.NoError(t, err, "Job")
	require.True(t, ok, "the job should be found")
	assert.Equal(t, "build", got.Job)
	assert.Equal(t, "acme/api", got.Repository)
}

// onlyJobNamed finds one seeded job by name.
func onlyJobNamed(t *testing.T, s *Store, name string) JobRecord {
	t.Helper()
	rows, _, err := s.Jobs(t.Context(), JobFilter{Search: name}, listRange())
	require.NoError(t, err, "Jobs")
	for _, r := range rows {
		if r.Job == name {
			return r
		}
	}
	t.Fatalf("no job named %q", name)
	return JobRecord{}
}

// nilIfEmpty normalises an empty slice to nil so the table tests can express
// "no rows" as nil without every case needing its own empty literal.
func nilIfEmpty(v []string) []string {
	if len(v) == 0 {
		return nil
	}
	return v
}

// The sample window can legitimately be set LONGER than the job window, and
// that is the only configuration in which the orphan sweep does any work: the
// ts sweep leaves these rows alone, the job they describe is deleted, and
// nothing in the retention list would ever match them again.
func TestJobSamplesAreNotOrphanedByALongerSampleWindow(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	at := base.Add(-40 * 24 * time.Hour)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, busyRunner("runner-a", at, 1))), "record")
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at.Add(time.Minute))), "runner departs")

	ret := defaultRetention
	ret.Scope5m = 30 * 24 * time.Hour    // the job expires
	ret.JobSamples = 90 * 24 * time.Hour // its samples would not
	require.NoError(t, s.Compact(ctx, base, ret), "Compact")

	var jobs, samples int64
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_observations`).Scan(&jobs))
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_samples`).Scan(&samples))
	assert.Zero(t, jobs, "the job is outside its own window")
	assert.Zero(t, samples, "and its samples must go with it rather than becoming unreachable")
}

// A job still inside its window keeps its samples even when they are older
// than the job's own retention boundary would suggest, because the sweeps are
// ordered and selected on the job's completion rather than the sample's age.
func TestSamplesOfALiveJobSurviveTheJobSweep(t *testing.T) {
	t.Parallel()

	s := newStore(t)
	ctx := t.Context()

	at := base.Add(-time.Hour)
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at, busyRunner("runner-a", at, 1))), "record")
	require.NoError(t, s.RecordSnapshot(ctx, snapshot(at.Add(time.Minute))), "runner departs")

	require.NoError(t, s.Compact(ctx, base, defaultRetention), "Compact")

	var jobs, samples int64
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_observations`).Scan(&jobs))
	require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM job_samples`).Scan(&samples))
	assert.NotZero(t, jobs, "an hour-old job is well inside every window")
	assert.NotZero(t, samples, "so its chart must still be drawable")
}

// jobsForSet reads a set's job observations straight off the ent client.
//
// The store exposes no query for this: nothing on the dashboard renders a
// per-set job list, so a public method would be production code with only a
// test to call it. The rows themselves are real — RepoConsumption and
// Throughput read the same table — and this is how the tests check what
// RecordSnapshot actually wrote into it.
func jobsForSet(t *testing.T, s *Store, setName string) []*ent.JobObservation {
	t.Helper()

	jobs, err := s.client.JobObservation.Query().
		Where(jobobservation.SetNameEQ(setName)).
		Order(jobobservation.ByStartedAt(entsql.OrderDesc()), jobobservation.ByID(entsql.OrderDesc())).
		All(t.Context())
	require.NoError(t, err, "read job observations for %q", setName)
	return jobs
}
