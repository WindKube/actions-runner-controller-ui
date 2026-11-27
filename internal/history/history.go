// Package history adapts the job store to the half of web.History it answers,
// so the views never import ent and the store never grows presentation types.
package history

import (
	"context"
	"sync"
	"time"

	"arc-ui/internal/fleet"
	"arc-ui/internal/store"
	"arc-ui/internal/web"
)

// statsTTL bounds the footer's staleness; every SSE render asks for it.
const statsTTL = time.Minute

// Queries is the slice of the store the adapter uses.
type Queries interface {
	Failures(ctx context.Context, set string, from, to time.Time, limit int) ([]store.JobRecord, int64, error)
	Jobs(ctx context.Context, f store.JobFilter, from, to time.Time) ([]store.JobRecord, int64, error)
	WorkflowRuns(ctx context.Context, f store.JobFilter, from, to time.Time) ([]store.WorkflowRun, int64, error)
	Job(ctx context.Context, id int) (store.JobRecord, bool, error)
	Facets(ctx context.Context, from, to time.Time) (store.JobFacets, error)
	Stats(ctx context.Context) (store.Stats, error)
}

type Adapter struct {
	Q     Queries
	stats *statsCache
}

func New(q Queries) Adapter {
	return Adapter{Q: q, stats: &statsCache{clock: time.Now}}
}

type statsCache struct {
	mu    sync.Mutex
	clock func() time.Time
	at    time.Time
	val   web.StoreStats
}

// Failures lists jobs whose runner failed in the window.
func (a Adapter) Failures(ctx context.Context, scope web.Scope, w web.Window, limit int) (web.FailureWindow, error) {
	rows, total, err := a.Q.Failures(ctx, string(scope), w.From, w.To, limit)
	if err != nil {
		return web.FailureWindow{}, err
	}
	out := web.FailureWindow{Failures: make([]fleet.Failure, 0, len(rows)), Total: int(total)}
	for _, j := range rows {
		reason := j.FailureReason
		if reason == "" {
			reason = "runner failed"
		}
		out.Failures = append(out.Failures, fleet.Failure{
			Runner: j.Runner, Set: j.Set, Reason: reason, At: j.FinishedAt, Severe: true,
		})
	}
	return out, nil
}

func (a Adapter) Stats(ctx context.Context) (web.StoreStats, error) {
	if a.stats == nil {
		return a.countStats(ctx)
	}
	return a.stats.get(ctx, a.countStats)
}

func (c *statsCache) get(ctx context.Context, fetch func(context.Context) (web.StoreStats, error)) (web.StoreStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if !c.at.IsZero() && now.Sub(c.at) < statsTTL {
		return c.val, nil
	}
	v, err := fetch(ctx)
	if err != nil {
		return v, err
	}
	c.at, c.val = now, v
	return v, nil
}

func (a Adapter) countStats(ctx context.Context) (web.StoreStats, error) {
	st, err := a.Q.Stats(ctx)
	if err != nil {
		return web.StoreStats{}, err
	}
	return web.StoreStats{Enabled: true, Path: st.Path, SizeBytes: st.SizeBytes, Jobs: st.Jobs, Oldest: st.Oldest}, nil
}

func jobFilter(f web.JobFilter) store.JobFilter {
	out := store.JobFilter{
		Repository: f.Repository, Workflow: f.Workflow, Set: f.Set,
		RunID: f.RunID, Search: f.Search, Limit: f.Limit,
	}
	switch f.Outcome {
	case web.JobOK:
		out.Outcome = store.OutcomeSucceeded
	case web.JobFailed:
		out.Outcome = store.OutcomeFailed
	case web.JobRunning:
		out.Outcome = store.OutcomeRunning
	case web.JobAnyOutcome:
	}
	return out
}

func job(j store.JobRecord) web.Job {
	return web.Job{
		ID: j.ID, JobID: j.JobID, Runner: j.Runner, Namespace: j.Namespace, Set: j.Set,
		Repository: j.Repository, WorkflowRef: j.WorkflowRef, Workflow: j.Workflow, Name: j.Name, RunID: j.RunID,
		StartedAt: j.StartedAt, FinishedAt: j.FinishedAt,
		RunnerFailed: j.Outcome == store.OutcomeFailed, FailureReason: j.FailureReason,
		CPUSeconds: j.CPUSeconds, MemPeakBytes: j.MemPeakBytes,
		CPURequest: j.CPURequest, CPULimit: j.CPULimit, MemRequest: j.MemRequest, MemLimit: j.MemLimit,
	}
}

func (a Adapter) Jobs(ctx context.Context, f web.JobFilter, w web.Window) (web.JobList, error) {
	rows, total, err := a.Q.Jobs(ctx, jobFilter(f), w.From, w.To)
	if err != nil {
		return web.JobList{}, err
	}
	out := web.JobList{Jobs: make([]web.Job, 0, len(rows)), Total: total}
	for _, j := range rows {
		out.Jobs = append(out.Jobs, job(j))
	}
	return out, nil
}

func (a Adapter) Workflows(ctx context.Context, f web.JobFilter, w web.Window) (web.WorkflowList, error) {
	rows, total, err := a.Q.WorkflowRuns(ctx, jobFilter(f), w.From, w.To)
	if err != nil {
		return web.WorkflowList{}, err
	}
	out := web.WorkflowList{Runs: make([]web.WorkflowRun, 0, len(rows)), Total: total}
	for _, r := range rows {
		out.Runs = append(out.Runs, web.WorkflowRun{
			Repository: r.Repository, Workflow: r.Workflow, RunID: r.RunID,
			Jobs: r.Jobs, Running: r.Running, Failed: r.Failed,
			StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
			CPUSeconds: r.CPUSeconds, MemPeakBytes: r.MemPeakBytes,
		})
	}
	return out, nil
}

func (a Adapter) Job(ctx context.Context, id int) (web.Job, bool, error) {
	j, ok, err := a.Q.Job(ctx, id)
	if err != nil || !ok {
		return web.Job{}, ok, err
	}
	return job(j), true, nil
}

func (a Adapter) Facets(ctx context.Context, w web.Window) (web.JobFacets, error) {
	f, err := a.Q.Facets(ctx, w.From, w.To)
	if err != nil {
		return web.JobFacets{}, err
	}
	return web.JobFacets{Repositories: f.Repositories, Workflows: f.Workflows, Sets: f.Sets}, nil
}
