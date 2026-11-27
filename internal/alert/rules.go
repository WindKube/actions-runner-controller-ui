package alert

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"arc-ui/internal/fleet"
	"arc-ui/internal/prom"
	"arc-ui/internal/store"
)

type Snapshotter interface {
	Snapshot() fleet.Snapshot
}

type Querier interface {
	Query(ctx context.Context, query string) ([]prom.Sample, error)
}

type FailureStore interface {
	Failures(ctx context.Context, set string, from, to time.Time, limit int) ([]store.JobRecord, int64, error)
}

// short renders 15m, 1h or 3m12s.
func short(d time.Duration) string {
	s := d.Round(time.Second).String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

func requireSource(s fleet.Snapshot, name string) error {
	src, ok := s.Source(name)
	if !ok || !src.Available {
		reason := "not reported"
		if ok && src.Reason != "" {
			reason = src.Reason
		}
		return fmt.Errorf("%s unavailable: %s", name, reason)
	}
	return nil
}

// QueuedAtMax fires for a set that has jobs waiting while it already runs its
// maximum number of runners.
func QueuedAtMax(f Snapshotter, hold time.Duration) Rule {
	return Rule{
		ID: "queued-at-max", Title: "jobs queued at max runners", Severity: Warning, For: hold,
		Threshold: "queued > 0 at max runners for " + short(hold),
		Eval: func(context.Context, time.Time) ([]Finding, error) {
			snap := f.Snapshot()
			if err := requireSource(snap, fleet.SourceARCCRDs); err != nil {
				return nil, err
			}
			var out []Finding
			known := false
			for _, s := range snap.Sets {
				if !s.QueuedKnown {
					continue
				}
				known = true
				if s.Queued > 0 && s.AtCapacity() {
					out = append(out, Finding{Set: s.Name, Message: fmt.Sprintf("%d jobs queued with %d/%d runners", s.Queued, s.Current, s.MaxRunners)})
				}
			}
			if !known && len(snap.Sets) > 0 {
				return nil, errors.New("queue depth unknown: no listener metrics for any set")
			}
			return out, nil
		},
	}
}

func ListenerDown(f Snapshotter, hold time.Duration) Rule {
	return Rule{
		ID: "listener-down", Title: "listener down", Severity: Critical, For: hold,
		Threshold: "not running for " + short(hold),
		Eval: func(context.Context, time.Time) ([]Finding, error) {
			snap := f.Snapshot()
			if err := requireSource(snap, fleet.SourceARCCRDs); err != nil {
				return nil, err
			}
			var out []Finding
			for _, s := range snap.Sets {
				if s.ListenerKnown && !s.ListenerHealthy {
					out = append(out, Finding{Set: s.Name, Message: "listener is not running; new jobs for this set are not picked up"})
				}
			}
			return out, nil
		},
	}
}

// OOMKilled counts runners killed for memory in the window, from the live
// fleet and from the job store, since a failed runner pod is soon replaced.
func OOMKilled(f Snapshotter, db FailureStore, window time.Duration) Rule {
	return Rule{
		ID: "oom-killed", Title: "runners OOMKilled", Severity: Warning,
		Threshold: "any in the last " + short(window),
		Eval: func(ctx context.Context, now time.Time) ([]Finding, error) {
			from := now.Add(-window)
			killed := map[string]map[string]bool{}
			add := func(set, runner, reason string) {
				if set == "" || !strings.Contains(reason, "OOMKilled") {
					return
				}
				if killed[set] == nil {
					killed[set] = map[string]bool{}
				}
				killed[set][runner] = true
			}
			for _, r := range f.Snapshot().Runners {
				if !r.FailedAt.Before(from) {
					add(r.SetName, r.Name, r.FailureReason)
				}
			}
			jobs, _, err := db.Failures(ctx, "", from, now.Add(time.Second), 1000)
			if err != nil {
				return nil, err
			}
			for _, j := range jobs {
				add(j.Set, j.Runner, j.FailureReason)
			}
			out := make([]Finding, 0, len(killed))
			for set, runners := range killed {
				out = append(out, Finding{Set: set, Message: fmt.Sprintf("%d runners OOMKilled in the last %s", len(runners), short(window))})
			}
			return out, nil
		},
	}
}

// FailedRising fires when a set's failed EphemeralRunner count, as the ARC
// controller exports it, grew by at least rise over the window. query must
// return one series per set with the set in the name label.
func FailedRising(f Snapshotter, q Querier, query string, rise int, window time.Duration) Rule {
	return Rule{
		ID: "failed-runners-rising", Title: "failed runners rising", Severity: Warning,
		Threshold: fmt.Sprintf("+%d in %s", rise, short(window)),
		Eval: func(ctx context.Context, _ time.Time) ([]Finding, error) {
			if err := requireSource(f.Snapshot(), fleet.SourceControllerMetrics); err != nil {
				return nil, err
			}
			rows, err := q.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			var out []Finding
			for _, s := range rows {
				if s.Value >= float64(rise) {
					out = append(out, Finding{Set: s.Labels["name"], Message: fmt.Sprintf("%.0f more failed runners in the last %s", s.Value, short(window))})
				}
			}
			return out, nil
		},
	}
}

// StartupP95 fires when a set's p95 job startup latency, from the listener's
// histogram, is over threshold.
func StartupP95(f Snapshotter, q Querier, query, setLabel string, threshold, window time.Duration) Rule {
	return Rule{
		ID: "startup-p95", Title: "slow job startup", Severity: Warning,
		Threshold: fmt.Sprintf("p95 > %s over %s", short(threshold), short(window)),
		Eval: func(ctx context.Context, _ time.Time) ([]Finding, error) {
			if err := requireSource(f.Snapshot(), fleet.SourceARCMetrics); err != nil {
				return nil, err
			}
			rows, err := q.Query(ctx, query)
			if err != nil {
				return nil, err
			}
			var out []Finding
			for _, s := range rows {
				if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) || s.Value <= threshold.Seconds() {
					continue
				}
				p95 := time.Duration(s.Value * float64(time.Second))
				out = append(out, Finding{Set: s.Labels[setLabel], Message: fmt.Sprintf("p95 startup %s over the last %s", short(p95), short(window))})
			}
			return out, nil
		},
	}
}
