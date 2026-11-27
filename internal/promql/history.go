package promql

import (
	"cmp"
	"context"
	"math"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"arc-ui/internal/prom"
	"arc-ui/internal/web"
)

// Querier is the part of prom.Client the views need.
type Querier interface {
	Query(ctx context.Context, query string) ([]prom.Sample, error)
	QueryRange(ctx context.Context, query string, r prom.Range) ([]prom.Series, error)
}

// Rest is the part of web.History that is not answered from Prometheus.
type Rest interface {
	Failures(ctx context.Context, scope web.Scope, w web.Window, limit int) (web.FailureWindow, error)
	Jobs(ctx context.Context, f web.JobFilter, w web.Window) (web.JobList, error)
	Workflows(ctx context.Context, f web.JobFilter, w web.Window) (web.WorkflowList, error)
	Job(ctx context.Context, id int) (web.Job, bool, error)
	JobSeries(ctx context.Context, id int, w web.Window) (web.JobSeries, error)
	Facets(ctx context.Context, w web.Window) (web.JobFacets, error)
	Stats(ctx context.Context) (web.StoreStats, error)
}

// History answers the chart questions from Prometheus and leaves the rest to
// the job store.
type History struct {
	Rest
	Client  Querier
	Queries Queries
}

var _ web.History = History{}

// rangeOf turns a chart window into an aligned query range with one point per
// bucket.
func rangeOf(w web.Window) prom.Range {
	points := max(w.Points, 1)
	step := max(w.To.Sub(w.From)/time.Duration(points), time.Second).Truncate(time.Second)
	return prom.Range{Start: w.From, End: w.To, Step: step}.Aligned()
}

// axis is every bucket timestamp of r, so series line up even when one has gaps.
func axis(r prom.Range) []time.Time {
	var out []time.Time
	for t := r.Start; !t.After(r.End); t = t.Add(r.Step) {
		out = append(out, t.UTC())
	}
	return out
}

// layOut puts the first series of a sum() result onto the axis with missing
// buckets set to gap; no series at all is nil so it never draws as zeros.
// Fleet sums use 0, since no runner means none in use; a single pod uses NaN,
// since it did not exist before it started or after it was gone.
func layOut(series []prom.Series, at []time.Time, gap float64) []float64 {
	if len(series) == 0 {
		return nil
	}
	out := make([]float64, len(at))
	for i := range out {
		out[i] = gap
	}
	idx := make(map[int64]int, len(at))
	for i, t := range at {
		idx[t.Unix()] = i
	}
	for _, p := range series[0].Points {
		if i, ok := idx[p.At.Unix()]; ok {
			out[i] = p.Value
		}
	}
	return out
}

func setOf(scope web.Scope) string { return string(scope) }

// rangeQueries runs several range queries concurrently onto one axis. Any
// failure fails the whole chart: a half-drawn stack would misstate the fleet.
func (h History) rangeQueries(ctx context.Context, r prom.Range, queries ...string) ([][]float64, []time.Time, error) {
	return h.rangeQueriesGap(ctx, r, 0, queries...)
}

func (h History) rangeQueriesGap(ctx context.Context, r prom.Range, gap float64, queries ...string) ([][]float64, []time.Time, error) {
	at := axis(r)
	out := make([][]float64, len(queries))
	g, ctx := errgroup.WithContext(ctx)
	for i, q := range queries {
		g.Go(func() error {
			series, err := h.Client.QueryRange(ctx, q, r)
			if err != nil {
				return err
			}
			out[i] = layOut(series, at, gap)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}
	for _, v := range out {
		if v != nil {
			return out, at, nil
		}
	}
	return out, nil, nil
}

func (h History) Scope(ctx context.Context, scope web.Scope, w web.Window) (web.ScopeSeries, error) {
	r := rangeOf(w)
	set := setOf(scope)
	q := h.Queries
	v, at, err := h.rangeQueries(ctx, r,
		q.Busy(set), q.Idle(set), q.Pending(set), q.Capacity(set),
		q.CPUUsed(set, r.Step), q.Requested("cpu", set),
		q.MemUsed(set), q.Requested("memory", set),
	)
	if err != nil {
		return web.ScopeSeries{}, err
	}
	return web.ScopeSeries{
		At: at, Busy: v[0], Idle: v[1], Pending: v[2], Capacity: v[3],
		CPUUsed: v[4], CPURequest: v[5], MemUsed: v[6], MemRequest: v[7],
	}, nil
}

// Runner is one runner pod's cAdvisor usage; the pod shares the runner's name.
func (h History) Runner(ctx context.Context, name string, w web.Window) (web.RunnerSeries, error) {
	r := rangeOf(w)
	v, at, err := h.rangeQueriesGap(ctx, r, math.NaN(), h.Queries.RunnerCPU(name, r.Step), h.Queries.RunnerMem(name))
	if err != nil {
		return web.RunnerSeries{}, err
	}
	return web.RunnerSeries{At: at, CPU: v[0], Mem: v[1]}, nil
}

// Throughput is jobs that finished per bucket, by the listener's job_result.
func (h History) Throughput(ctx context.Context, scope web.Scope, w web.Window) (web.Counts, error) {
	r := rangeOf(w)
	set := setOf(scope)
	v, at, err := h.rangeQueries(ctx, r, h.Queries.Completed(set, true, r.Step), h.Queries.Completed(set, false, r.Step))
	if err != nil {
		return web.Counts{}, err
	}
	return web.Counts{At: at, Up: v[0], Down: v[1]}, nil
}

// Churn is jobs started against jobs finished per bucket. ARC runners are
// ephemeral, one per job, so this is runner churn as GitHub sees it.
func (h History) Churn(ctx context.Context, scope web.Scope, w web.Window) (web.Counts, error) {
	r := rangeOf(w)
	set := setOf(scope)
	v, at, err := h.rangeQueries(ctx, r, h.Queries.Started(set, r.Step), h.Queries.AllCompleted(set, r.Step))
	if err != nil {
		return web.Counts{}, err
	}
	return web.Counts{At: at, Up: v[0], Down: v[1]}, nil
}

// Repos ranks repositories by runner time spent on their jobs in the window.
func (h History) Repos(ctx context.Context, w web.Window, limit int) ([]web.RepoHistory, error) {
	window := max(w.To.Sub(w.From), time.Minute)
	var seconds, jobs []prom.Sample
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) { seconds, err = h.Client.Query(gctx, h.Queries.RepoSeconds(window)); return err })
	g.Go(func() (err error) { jobs, err = h.Client.Query(gctx, h.Queries.RepoJobs(window)); return err })
	if err := g.Wait(); err != nil {
		return nil, err
	}

	count := make(map[string]float64, len(jobs))
	for _, s := range jobs {
		count[repoName(s.Labels)] = s.Value
	}
	out := make([]web.RepoHistory, 0, len(seconds))
	for _, s := range seconds {
		repo := repoName(s.Labels)
		if repo == "" || s.Value <= 0 {
			continue
		}
		out = append(out, web.RepoHistory{Repository: repo, Seconds: s.Value, Jobs: int(count[repo] + 0.5)})
	}
	slices.SortFunc(out, func(a, b web.RepoHistory) int {
		return cmp.Or(cmp.Compare(b.Seconds, a.Seconds), cmp.Compare(a.Repository, b.Repository))
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// repoName rebuilds "owner/repo", the form EphemeralRunner status uses; the
// listener splits it into organization and repository labels.
func repoName(labels map[string]string) string {
	repo := labels["repository"]
	if repo == "" {
		return ""
	}
	if org := labels["organization"]; org != "" {
		return org + "/" + repo
	}
	return repo
}
