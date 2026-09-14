package web

import (
	"context"
	"time"

	"arc-ui/internal/fleet"
)

// Window is one bucketed query over the history store.
//
// Points is a request, not a promise. The store buckets [From, To) into at most
// Points buckets and returns however many it has data for; the charts scale to
// len(series), so a young store renders a short line rather than fake zeros.
type Window struct {
	From   time.Time
	To     time.Time
	Points int
}

// Scope names what a series is aggregated over. The empty string is the whole
// fleet; anything else is a runner set name.
type Scope string

// FleetScope aggregates across every set.
const FleetScope Scope = ""

// Set returns the scope for one runner set.
func Set(name string) Scope { return Scope(name) }

// ScopeSeries is the bucketed history behind the overview and set-detail charts.
// Every slice is the same length as At, or empty when the store has nothing for
// that dimension.
//
// Capacity is a series rather than a scalar because maxRunners is edited through
// the day, and a scalar would silently redraw history whenever someone rescaled.
type ScopeSeries struct {
	At []time.Time

	Busy    []float64
	Idle    []float64
	Pending []float64

	Capacity []float64

	CPUUsed    []float64 // cores
	CPURequest []float64 // cores
	MemUsed    []float64 // bytes
	MemRequest []float64 // bytes
}

// Len is the number of buckets, and the length every populated slice has.
func (s ScopeSeries) Len() int { return len(s.At) }

func (s ScopeSeries) Empty() bool { return len(s.At) == 0 }

// Utilization is busy runners as a fraction of total, per bucket. It is
// derived here rather than stored because it is a pure function of counts we
// already keep, and storing it would let the two disagree.
func (s ScopeSeries) Utilization() []float64 {
	out := make([]float64, s.Len())
	for i := range out {
		total := at(s.Busy, i) + at(s.Idle, i) + at(s.Pending, i)
		if total > 0 {
			out[i] = at(s.Busy, i) / total
		}
	}
	return out
}

// Stacked returns the three state series in the order the area chart layers
// them, bottom first.
func (s ScopeSeries) Stacked() [][]float64 {
	return [][]float64{pad(s.Busy, s.Len()), pad(s.Idle, s.Len()), pad(s.Pending, s.Len())}
}

// Peak is the largest value across the stack and the capacity line, which is
// what the y-axis has to accommodate. It never returns zero: a flat-zero chart
// still needs a non-degenerate scale to draw a baseline against.
func (s ScopeSeries) Peak() float64 {
	peak := 0.0
	for i := range s.At {
		if v := at(s.Busy, i) + at(s.Idle, i) + at(s.Pending, i); v > peak {
			peak = v
		}
		if v := at(s.Capacity, i); v > peak {
			peak = v
		}
	}
	if peak <= 0 {
		return 1
	}
	return peak
}

// CapacityLine is the level the dashed max-capacity line sits at, and whether
// there is one to draw. An unbounded set stores no capacity, so this reports
// false and the view omits the line rather than drawing it at zero.
func (s ScopeSeries) CapacityLine() (float64, bool) {
	for i := len(s.Capacity) - 1; i >= 0; i-- {
		if s.Capacity[i] > 0 {
			return s.Capacity[i], true
		}
	}
	return 0, false
}

// PeakOf is the largest value in a series, floored at one so charts drawn from
// an all-zero series still have a usable scale.
func PeakOf(series ...[]float64) float64 {
	peak := 0.0
	for _, s := range series {
		for _, v := range s {
			if v > peak {
				peak = v
			}
		}
	}
	if peak <= 0 {
		return 1
	}
	return peak
}

// RunnerSeries is the raw per-runner history behind the runner detail charts.
// This is the only place raw per-runner samples are read, which is why they
// are retained for minutes rather than days.
type RunnerSeries struct {
	At   []time.Time
	CPU  []float64 // cores
	Mem  []float64 // bytes
	Peak struct {
		CPU float64
		Mem float64
	}
}

func (s RunnerSeries) Len() int { return len(s.At) }

// Counts is a pair of per-bucket event counts, used by the throughput and
// churn charts. Up and Down are the two directions the design draws.
type Counts struct {
	At   []time.Time
	Up   []float64
	Down []float64
}

func (c Counts) Len() int { return len(c.At) }

// Peak is the largest single-bucket value in either direction, floored at one.
func (c Counts) Peak() float64 { return PeakOf(c.Up, c.Down) }

// Sum totals both directions, for the "N jobs · M failed" style captions.
func (c Counts) Sum() (up, down float64) {
	for _, v := range c.Up {
		up += v
	}
	for _, v := range c.Down {
		down += v
	}
	return up, down
}

// RepoHistory is one repository's consumption over the selected window.
//
// Distinct from fleet.RepoUsage, which is the same panel's live fallback: this one
// is integrated over time (core-seconds), that one is a snapshot of what is
// running right now.
type RepoHistory struct {
	Repository string
	Jobs       int
	CPUSeconds float64
	MemGiBSecs float64
}

// JobOutcome is how a job ended, as the Jobs tab filters and colours it.
type JobOutcome string

// The outcomes a job listing can be narrowed to. JobAnyOutcome is the zero
// value, so an unset filter matches everything.
const (
	JobAnyOutcome JobOutcome = ""
	JobOK         JobOutcome = "ok"
	JobFailed     JobOutcome = "failed"
	JobRunning    JobOutcome = "running"
)

// JobFilter narrows a job or workflow listing. Every field is optional.
type JobFilter struct {
	Repository string
	Workflow   string
	Set        string
	RunID      int64
	Outcome    JobOutcome
	// Search is matched as a case-insensitive substring over the workflow,
	// job name and repository.
	Search string
	Limit  int
}

// Job is one row of the Jobs tab, and the subject of the job detail view.
type Job struct {
	ID         int
	Runner     string
	Set        string
	Repository string
	Workflow   string
	Name       string
	RunID      int64
	StartedAt  time.Time
	// FinishedAt is zero while the job is still running.
	FinishedAt time.Time
	Succeeded  bool

	// CPUSeconds and MemGiBSecs are integrated over the job's life. They are
	// lower bounds: a job that died between two scrapes contributes nothing.
	CPUSeconds float64
	MemGiBSecs float64

	// What the runner pod reserved, in cores and bytes, which is what the
	// detail chart draws its reference lines from. Unlike the totals above
	// these stay in bytes: they are plotted against the usage series, not
	// summarised. Zero means there is no line to draw.
	CPURequest float64
	CPULimit   float64
	MemRequest float64
	MemLimit   float64
}

// Running reports whether the job had not finished when it was last observed.
func (j Job) Running() bool { return j.FinishedAt.IsZero() }

// Duration is how long the job ran, measured to now while it is still going.
func (j Job) Duration(now time.Time) time.Duration {
	if j.StartedAt.IsZero() {
		return 0
	}
	end := j.FinishedAt
	if end.IsZero() {
		end = now
	}
	return max(0, end.Sub(j.StartedAt))
}

// WorkflowRun is one row of the Workflows tab: a workflow run aggregated from
// the jobs observed for it.
type WorkflowRun struct {
	Repository string
	Workflow   string
	RunID      int64

	Jobs    int
	Running int
	Failed  int

	StartedAt time.Time
	// FinishedAt is zero while any of the run's jobs is still going.
	FinishedAt time.Time

	CPUSeconds float64
	MemGiBSecs float64
}

// Duration is how long the run has taken, measured to now while it is going.
func (w WorkflowRun) Duration(now time.Time) time.Duration {
	if w.StartedAt.IsZero() {
		return 0
	}
	end := w.FinishedAt
	if end.IsZero() {
		end = now
	}
	return max(0, end.Sub(w.StartedAt))
}

// JobList is a page of jobs plus how many the window holds.
//
// Total is the window's count, not the page's. "100 of 4,312" is what tells an
// operator the table is a sample rather than the whole story.
type JobList struct {
	Jobs  []Job
	Total int64
}

// WorkflowList is a page of workflow runs plus the window's total.
type WorkflowList struct {
	Runs  []WorkflowRun
	Total int64
}

// JobSeries is one job's resource usage over its own lifetime.
//
// Empty is the normal state for a job that ran before per-job sampling was
// switched on, or while metrics-server was unavailable. The view says so
// rather than drawing a flat zero.
type JobSeries struct {
	At  []time.Time
	CPU []float64 // cores
	Mem []float64 // bytes
}

// Len is the number of buckets.
func (s JobSeries) Len() int { return len(s.At) }

// JobFacets are the distinct values the window contains, for the filter
// dropdowns above the two tabs.
type JobFacets struct {
	Repositories []string
	Workflows    []string
	Sets         []string
}

// History is everything the views need from the time-series store.
//
// It is declared here, at the consumer, rather than exported from the store: the
// views define what a chart needs, and the store's job is to satisfy that. It also
// means the whole web layer can be tested against a hand-written series.
//
// Every method may return an error, and every view renders an empty-state panel
// rather than failing the page when one does.
type History interface {
	// Scope returns bucketed counts and resource series for the fleet or one
	// runner set.
	Scope(ctx context.Context, scope Scope, w Window) (ScopeSeries, error)

	// Runner returns raw samples for a single runner.
	Runner(ctx context.Context, name string, w Window) (RunnerSeries, error)

	// Throughput returns jobs completed (Up) and failed (Down) per bucket.
	Throughput(ctx context.Context, scope Scope, w Window) (Counts, error)

	// Churn returns runners created (Up) and destroyed (Down) per bucket.
	Churn(ctx context.Context, scope Scope, w Window) (Counts, error)

	// Repos returns per-repository consumption over the window, busiest first.
	Repos(ctx context.Context, w Window, limit int) ([]RepoHistory, error)

	// Failures returns the newest failures in the window, capped at limit.
	Failures(ctx context.Context, scope Scope, w Window, limit int) (FailureWindow, error)

	// Jobs returns the jobs overlapping the window, newest first.
	Jobs(ctx context.Context, f JobFilter, w Window) (JobList, error)

	// Workflows returns the workflow runs overlapping the window, newest
	// first.
	Workflows(ctx context.Context, f JobFilter, w Window) (WorkflowList, error)

	// Job returns one job by id. The bool is false when there is no such row,
	// which the handler turns into a 404.
	Job(ctx context.Context, id int) (Job, bool, error)

	// JobSeries returns one job's resource usage over the window.
	JobSeries(ctx context.Context, id int, w Window) (JobSeries, error)

	// Facets returns the distinct values the window contains, for the filter
	// dropdowns.
	Facets(ctx context.Context, w Window) (JobFacets, error)

	// Stats reports what the history is costing on disk. It is the only
	// question here that is not about a time window.
	Stats(ctx context.Context) (StoreStats, error)
}

// FailureWindow is the failure lane's contents. Failures is a page and Total
// counts the window, which are different numbers on any fleet worth looking at:
// "six shown of forty-one" is the whole reason the lane has a footer.
type FailureWindow struct {
	Failures []fleet.Failure
	Total    int
}

// StoreStats is what the SQLite footer reports.
//
// The file it describes is the one moving part of this dashboard nobody else
// monitors: it grows on a volume sized once at install time, and the first symptom
// of that volume filling is the history quietly stopping.
type StoreStats struct {
	// Enabled is false when the dashboard is running without a history store.
	// That is a supported configuration, so the panel has to distinguish it
	// from a store that exists and happens to be empty — zeros would read as
	// the latter.
	Enabled bool

	Path      string
	SizeBytes int64

	Samples     int64
	Jobs        int64
	JobSamples  int64
	ChurnEvents int64
	Failures    int64
	Rows        int64

	// Oldest is the timestamp of the oldest surviving sample, which is the
	// honest answer to "how far back can I look".
	Oldest time.Time
}

// NoHistory satisfies History with empty results. It is what the dashboard
// runs on before the store has been wired up, and what the tests use.
type NoHistory struct{}

func (NoHistory) Scope(context.Context, Scope, Window) (ScopeSeries, error) {
	return ScopeSeries{}, nil
}

func (NoHistory) Runner(context.Context, string, Window) (RunnerSeries, error) {
	return RunnerSeries{}, nil
}

func (NoHistory) Throughput(context.Context, Scope, Window) (Counts, error) { return Counts{}, nil }

func (NoHistory) Churn(context.Context, Scope, Window) (Counts, error) { return Counts{}, nil }

func (NoHistory) Repos(context.Context, Window, int) ([]RepoHistory, error) { return nil, nil }

func (NoHistory) Failures(context.Context, Scope, Window, int) (FailureWindow, error) {
	return FailureWindow{}, nil
}

func (NoHistory) Jobs(context.Context, JobFilter, Window) (JobList, error) { return JobList{}, nil }

func (NoHistory) Workflows(context.Context, JobFilter, Window) (WorkflowList, error) {
	return WorkflowList{}, nil
}

func (NoHistory) Job(context.Context, int) (Job, bool, error) { return Job{}, false, nil }

func (NoHistory) JobSeries(context.Context, int, Window) (JobSeries, error) {
	return JobSeries{}, nil
}

func (NoHistory) Facets(context.Context, Window) (JobFacets, error) { return JobFacets{}, nil }

func (NoHistory) Stats(context.Context) (StoreStats, error) { return StoreStats{}, nil }

// at reads a series defensively: a store that returns a short or absent slice
// for one dimension must not panic the whole page.
func at(s []float64, i int) float64 {
	if i < 0 || i >= len(s) {
		return 0
	}
	return s[i]
}

// pad returns s at exactly n elements, so the stacked chart can assume every
// layer is the same length.
func pad(s []float64, n int) []float64 {
	if len(s) == n {
		return s
	}
	out := make([]float64, n)
	copy(out, s)
	return out
}
