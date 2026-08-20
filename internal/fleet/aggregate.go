package fleet

import (
	"sort"
	"time"

	"github.com/samber/lo"
)

// Totals is the aggregate the fleet summary strip and the header KPIs render.
type Totals struct {
	Runners int
	Busy    int
	Idle    int
	Pending int
	Failed  int

	// Queued is only meaningful when QueuedKnown is true. ARC's listener
	// metrics are disabled by default, and showing "0 queued" on an install
	// that simply isn't reporting is worse than showing nothing.
	Queued      int
	QueuedKnown bool

	// Capacity is the summed maxRunners of the scoped sets. Unbounded is true
	// when any scoped set has no ceiling, in which case Capacity is a floor
	// rather than a limit and no max line should be drawn.
	Capacity  int
	Unbounded bool

	CPU Resources // cores, summed
	Mem Resources // bytes, summed

	// MetricsCovered counts runners that actually reported usage. Short-lived
	// ephemeral runners routinely die before metrics-server first scrapes
	// them, so partial coverage is normal and worth surfacing rather than
	// silently understating fleet usage.
	MetricsCovered int
}

// Utilization is busy runners as a fraction of the total.
func (t Totals) Utilization() float64 {
	if t.Runners == 0 {
		return 0
	}
	return float64(t.Busy) / float64(t.Runners)
}

// MetricsComplete reports whether every runner contributed a usage sample.
func (t Totals) MetricsComplete() bool {
	return t.Runners == 0 || t.MetricsCovered == t.Runners
}

// Aggregate totals a set of runners against the scale sets they belong to.
//
// Requests come from the sets rather than the runners because that is where
// the pod template lives, and they are scaled by the observed runner count so
// the "used of requested" ratio compares like with like.
func Aggregate(runners []Runner, sets []RunnerSet) Totals {
	t := Totals{Runners: len(runners)}

	perSet := make(map[string]int, len(sets))
	for _, r := range runners {
		switch r.State {
		case StateBusy:
			t.Busy++
		case StateIdle:
			t.Idle++
		case StatePending:
			t.Pending++
		case StateFailed:
			t.Failed++
		}
		perSet[r.SetName]++

		if r.CPU.HasUsage() || r.Mem.HasUsage() {
			t.MetricsCovered++
		}
		t.CPU.Used += r.CPU.Used
		t.Mem.Used += r.Mem.Used
		if r.CPU.At.After(t.CPU.At) {
			t.CPU.At = r.CPU.At
		}
		if r.Mem.At.After(t.Mem.At) {
			t.Mem.At = r.Mem.At
		}
	}

	for _, s := range sets {
		if s.Unbounded {
			t.Unbounded = true
		} else {
			t.Capacity += s.MaxRunners
		}
		if s.QueuedKnown {
			t.QueuedKnown = true
			t.Queued += s.Queued
		}

		n := float64(perSet[s.Name])
		t.CPU.Request += s.CPURequest * n
		t.Mem.Request += s.MemRequest * n
		t.CPU.Limit += s.CPULimit * n
		t.Mem.Limit += s.MemLimit * n
	}

	return t
}

// SetTotals is one row of the runner-set table: the set joined with the
// filtered runners that belong to it.
type SetTotals struct {
	Set     RunnerSet
	Runners []Runner
	Totals  Totals
}

// GroupBySet buckets runners under their sets and totals each bucket.
//
// Counts come from the runners we can see rather than from the set's own
// status, because the table must agree with the filter the user applied.
func GroupBySet(runners []Runner, sets []RunnerSet) []SetTotals {
	byName := lo.GroupBy(runners, func(r Runner) string { return r.SetName })

	out := make([]SetTotals, 0, len(sets))
	for _, s := range sets {
		rs := byName[s.Name]
		out = append(out, SetTotals{
			Set:     s,
			Runners: rs,
			Totals:  Aggregate(rs, []RunnerSet{s}),
		})
	}
	return out
}

// Failure is one entry in the failure lane.
type Failure struct {
	Runner string
	// Set is the scale set the runner belonged to. The lane is fleet-wide, so a
	// row without it leaves the reader guessing which set is burning pods.
	Set    string
	Reason string
	At     time.Time
	// Severe distinguishes a runner that will never work from one that merely
	// exited non-zero, which the design colours differently.
	Severe bool
}

// Failures extracts recent failures, newest first, capped at limit.
func Failures(runners []Runner, limit int) []Failure {
	out := make([]Failure, 0, limit)
	for _, r := range runners {
		if r.FailureReason == "" {
			continue
		}
		out = append(out, Failure{
			Runner: r.Name,
			Set:    r.SetName,
			Reason: r.FailureReason,
			At:     r.FailedAt,
			Severe: r.State == StateFailed,
		})
	}

	SortFailures(out)

	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// SortFailures orders a lane newest first, with undated failures last.
//
// Exported because the lane is assembled from two sources — the history store
// and the live snapshot — and rows merged from both have to end up in the same
// order as rows derived from one runner list, or the newest failure is not the
// one at the top.
func SortFailures(f []Failure) {
	sort.SliceStable(f, func(i, j int) bool {
		a, b := f[i], f[j]
		if a.At.IsZero() != b.At.IsZero() {
			return !a.At.IsZero()
		}
		return a.At.After(b.At)
	})
}

// RepoUsage is one bar in "fleet consumption by repository".
type RepoUsage struct {
	Repository string
	Runners    int
	CPUCores   float64
}

// ByRepository totals current consumption per repository, busiest first.
//
// This is live consumption, not historical job counts: it answers "who is
// using the fleet right now", which is the question an operator looking at a
// saturated fleet actually has.
func ByRepository(runners []Runner) []RepoUsage {
	working := lo.Filter(runners, func(r Runner, _ int) bool { return r.Job.Repository != "" })
	byRepo := lo.GroupBy(working, func(r Runner) string { return r.Job.Repository })

	out := make([]RepoUsage, 0, len(byRepo))
	for repo, rs := range byRepo {
		out = append(out, RepoUsage{
			Repository: repo,
			Runners:    len(rs),
			CPUCores:   lo.SumBy(rs, func(r Runner) float64 { return r.CPU.Used }),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Runners != b.Runners {
			return a.Runners > b.Runners
		}
		return a.Repository < b.Repository
	})
	return out
}
