// Package history adapts the time-series store to the contract the views
// declare.
//
// It exists so that neither side has to know about the other: internal/web
// states what a chart needs, internal/store states what SQLite can answer
// efficiently, and this package is the twenty lines in between. Without it the
// views would either import ent transitively or the store would grow
// presentation types.
package history

import (
	"context"
	"slices"
	"sync"
	"time"

	"arc-ui/internal/store"
	"arc-ui/internal/web"
)

// statsTTL bounds how stale the store footer may be.
//
// COUNT(*) over samples scans an index — millions of rows once the hourly tier
// has a year in it — and the SSE stream re-renders every panel on every snapshot
// change, for every connected browser. Counted per render, the cheapest panel on
// the page would be the most expensive thing the store does.
const statsTTL = time.Minute

// Queries is the slice of the store this adapter uses. Declaring it as an
// interface keeps the adapter testable without a database.
type Queries interface {
	Series(ctx context.Context, scope store.Scope, scopeID string, metrics []store.Metric, r store.Range) (map[store.Metric][]store.Point, error)
	Churn(ctx context.Context, setName string, r store.Range) (created, terminated []store.Point, err error)
	Throughput(ctx context.Context, setName string, r store.Range) (ok, failed []store.Point, err error)
	RepoConsumption(ctx context.Context, r store.Range) ([]store.RepoTotal, error)
	Stats(ctx context.Context) (store.Stats, error)
}

// Adapter satisfies web.History using the store.
type Adapter struct {
	Q Queries

	// stats memoises Q.Stats. A nil cache counts on every call, which is what a
	// zero-valued Adapter does; New always supplies one.
	stats *statsCache
}

var _ web.History = Adapter{}

// New returns an adapter over q.
func New(q Queries) Adapter {
	return Adapter{Q: q, stats: &statsCache{clock: time.Now}}
}

// statsCache holds one memoised store.Stats answer.
type statsCache struct {
	mu    sync.Mutex
	clock func() time.Time

	at  time.Time
	val web.StoreStats
}

// Stats reports what the history is costing on disk, memoised for statsTTL.
func (a Adapter) Stats(ctx context.Context) (web.StoreStats, error) {
	if a.stats == nil {
		return a.countStats(ctx)
	}
	return a.stats.get(ctx, a.countStats)
}

// get returns the memoised value, or counts and stores a fresh one.
//
// The lock is held across the count rather than only around the fields. Every
// connected browser renders the footer on the same tick, so releasing it first
// would let a cold cache launch one COUNT(*) per stream against a database with
// one writer and a handful of readers; serialised, the first caller counts and
// the rest read what it stored.
func (c *statsCache) get(
	ctx context.Context,
	count func(context.Context) (web.StoreStats, error),
) (web.StoreStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.clock()
	if !c.at.IsZero() && now.Sub(c.at) < statsTTL {
		return c.val, nil
	}

	val, err := count(ctx)
	if err != nil {
		// Deliberately not cached: a failed count is usually a database busy
		// for milliseconds, and memoising that would keep the footer broken for
		// a minute after the store recovered. There is nothing worth serving
		// from the cache in this branch anyway.
		return web.StoreStats{}, err
	}

	c.val, c.at = val, now
	return val, nil
}

// countStats asks the store and maps its answer onto the view contract.
func (a Adapter) countStats(ctx context.Context) (web.StoreStats, error) {
	st, err := a.Q.Stats(ctx)
	if err != nil {
		return web.StoreStats{}, err
	}
	return web.StoreStats{
		// Anything the store answered proves the store is there. The zero value
		// means the opposite, and the footer renders the two differently.
		Enabled:     true,
		Path:        st.Path,
		SizeBytes:   st.SizeBytes,
		Samples:     st.Samples,
		Jobs:        st.Jobs,
		Phases:      st.Phases,
		ChurnEvents: st.ChurnEvents,
		Rows:        st.Rows,
		Oldest:      st.Oldest,
	}, nil
}

// scopeMetrics is everything the overview and set-detail charts read. They are
// fetched in one call because the store answers them from a single scan.
var scopeMetrics = []store.Metric{
	store.MetricBusy, store.MetricIdle, store.MetricPending,
	store.MetricCapacity,
	store.MetricCPUUsed, store.MetricCPURequest,
	store.MetricMemUsed, store.MetricMemRequest,
}

// Scope returns bucketed counts and resource series.
func (a Adapter) Scope(ctx context.Context, scope web.Scope, w web.Window) (web.ScopeSeries, error) {
	kind, id := storeScope(scope)

	series, err := a.Q.Series(ctx, kind, id, scopeMetrics, rng(w))
	if err != nil {
		return web.ScopeSeries{}, err
	}

	axis := axisOf(series)
	return web.ScopeSeries{
		At:         axis,
		Busy:       align(series[store.MetricBusy], axis),
		Idle:       align(series[store.MetricIdle], axis),
		Pending:    align(series[store.MetricPending], axis),
		Capacity:   align(series[store.MetricCapacity], axis),
		CPUUsed:    align(series[store.MetricCPUUsed], axis),
		CPURequest: align(series[store.MetricCPURequest], axis),
		MemUsed:    align(series[store.MetricMemUsed], axis),
		MemRequest: align(series[store.MetricMemRequest], axis),
	}, nil
}

// Runner returns raw per-runner samples.
func (a Adapter) Runner(ctx context.Context, name string, w web.Window) (web.RunnerSeries, error) {
	series, err := a.Q.Series(ctx, store.ScopeRunner, name,
		[]store.Metric{store.MetricCPUUsed, store.MetricMemUsed}, rng(w))
	if err != nil {
		return web.RunnerSeries{}, err
	}

	axis := axisOf(series)
	out := web.RunnerSeries{
		At:  axis,
		CPU: align(series[store.MetricCPUUsed], axis),
		Mem: align(series[store.MetricMemUsed], axis),
	}
	out.Peak.CPU = web.PeakOf(out.CPU)
	out.Peak.Mem = web.PeakOf(out.Mem)
	return out, nil
}

// Throughput returns jobs completed and failed per bucket.
func (a Adapter) Throughput(ctx context.Context, scope web.Scope, w web.Window) (web.Counts, error) {
	ok, failed, err := a.Q.Throughput(ctx, string(scope), rng(w))
	if err != nil {
		return web.Counts{}, err
	}
	return counts(ok, failed), nil
}

// Churn returns runners created and destroyed per bucket.
func (a Adapter) Churn(ctx context.Context, scope web.Scope, w web.Window) (web.Counts, error) {
	created, terminated, err := a.Q.Churn(ctx, string(scope), rng(w))
	if err != nil {
		return web.Counts{}, err
	}
	return counts(created, terminated), nil
}

// Repos returns per-repository consumption, busiest first.
func (a Adapter) Repos(ctx context.Context, w web.Window, limit int) ([]web.RepoHistory, error) {
	totals, err := a.Q.RepoConsumption(ctx, rng(w))
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(totals) > limit {
		totals = totals[:limit]
	}

	out := make([]web.RepoHistory, 0, len(totals))
	for _, t := range totals {
		out = append(out, web.RepoHistory{
			Repository: t.Repository,
			Jobs:       t.Jobs,
			CPUSeconds: t.CPUSeconds,
			// The store integrates byte-seconds; the panel shows GiB-seconds,
			// because byte-seconds for a real fleet is a sixteen-digit number.
			MemGiBSecs: t.MemByteSeconds / gib,
		})
	}
	return out, nil
}

const gib = 1024 * 1024 * 1024

func rng(w web.Window) store.Range {
	return store.Range{From: w.From, To: w.To, Points: w.Points}
}

// storeScope maps the view's scope onto the store's scope-and-id pair.
func storeScope(s web.Scope) (store.Scope, string) {
	if s == web.FleetScope {
		return store.ScopeFleet, ""
	}
	return store.ScopeSet, string(s)
}

// axisOf builds one time axis covering every metric returned.
//
// The metrics are bucketed identically by the store today, but they are
// returned as independent slices and a metric with no rows for a bucket is
// simply absent. Deriving the axis from the union and aligning onto it means a
// partially populated result — metrics-server down for ten minutes, say —
// leaves a gap in one series rather than shifting every later point of it
// leftwards against the others.
func axisOf(series map[store.Metric][]store.Point) []time.Time {
	seen := make(map[int64]struct{})
	var axis []time.Time
	for _, points := range series {
		for _, p := range points {
			k := p.At.UnixNano()
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			axis = append(axis, p.At)
		}
	}
	slices.SortFunc(axis, time.Time.Compare)
	return axis
}

// align projects points onto the axis, leaving zero where a bucket has no
// sample.
func align(points []store.Point, axis []time.Time) []float64 {
	if len(points) == 0 {
		return nil
	}
	byTime := make(map[int64]float64, len(points))
	for _, p := range points {
		byTime[p.At.UnixNano()] = p.Value
	}
	out := make([]float64, len(axis))
	for i, t := range axis {
		out[i] = byTime[t.UnixNano()]
	}
	return out
}

// counts merges two event series onto a shared axis.
func counts(up, down []store.Point) web.Counts {
	axis := axisOf(map[store.Metric][]store.Point{"up": up, "down": down})
	return web.Counts{
		At:   axis,
		Up:   align(up, axis),
		Down: align(down, axis),
	}
}
