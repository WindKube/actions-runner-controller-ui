// Package alert evaluates the in-app alert rules. State lives in memory: a
// restart forgets pending holds and resolved history, which is acceptable for
// an at-a-glance view that is not a paging system.
package alert

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Severity string

const (
	Warning  Severity = "warning"
	Critical Severity = "critical"
)

// Finding is one subject a rule's condition currently holds for.
type Finding struct {
	Set     string
	Message string
}

type Rule struct {
	ID        string
	Title     string
	Severity  Severity
	Threshold string
	// For is how long a finding must hold before it fires.
	For  time.Duration
	Eval func(ctx context.Context, now time.Time) ([]Finding, error)
}

type Alert struct {
	Rule       string
	Title      string
	Severity   Severity
	Set        string
	Message    string
	Since      time.Time
	FiredAt    time.Time
	ResolvedAt time.Time
}

func (a Alert) Key() string { return a.Rule + "/" + a.Set }

type RuleStatus struct {
	ID        string
	Title     string
	Severity  Severity
	Threshold string
	// Unavailable is set when the rule's source failed on the last
	// evaluation; its alerts are then held as they were, neither fired nor
	// resolved.
	Unavailable string
	Firing      int
}

type State struct {
	Evaluated time.Time
	Firing    []Alert
	Resolved  []Alert
	Rules     []RuleStatus
}

type Engine struct {
	Rules    []Rule
	Interval time.Duration
	// Keep is how long resolved alerts stay listed.
	Keep time.Duration
	// OnChange runs after an evaluation that fired or resolved something.
	OnChange func()
	Log      zerolog.Logger

	mu       sync.RWMutex
	pending  map[string]Alert
	firing   map[string]Alert
	resolved []Alert
	status   []RuleStatus
	at       time.Time
}

func (e *Engine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.Interval)
	defer ticker.Stop()
	for {
		e.Evaluate(ctx, time.Now())
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Evaluate runs every rule once and reports whether anything fired or resolved.
func (e *Engine) Evaluate(ctx context.Context, now time.Time) bool {
	type result struct {
		found []Finding
		err   error
	}
	results := make([]result, len(e.Rules))
	var wg sync.WaitGroup
	for i, r := range e.Rules {
		wg.Go(func() {
			f, err := r.Eval(ctx, now)
			results[i] = result{f, err}
		})
	}
	wg.Wait()

	e.mu.Lock()
	if e.pending == nil {
		e.pending, e.firing = map[string]Alert{}, map[string]Alert{}
	}
	changed := false
	status := make([]RuleStatus, len(e.Rules))
	for i, r := range e.Rules {
		status[i] = RuleStatus{ID: r.ID, Title: r.Title, Severity: r.Severity, Threshold: r.Threshold}
		if err := results[i].err; err != nil {
			status[i].Unavailable = err.Error()
			e.Log.Debug().Err(err).Str("rule", r.ID).Msg("alert rule unavailable")
			continue
		}
		if e.apply(r, results[i].found, now) {
			changed = true
		}
	}
	for i := range status {
		for _, a := range e.firing {
			if a.Rule == status[i].ID {
				status[i].Firing++
			}
		}
	}
	e.resolved = slices.DeleteFunc(e.resolved, func(a Alert) bool { return now.Sub(a.ResolvedAt) > e.keep() })
	e.status, e.at = status, now
	e.mu.Unlock()

	if changed && e.OnChange != nil {
		e.OnChange()
	}
	return changed
}

func (e *Engine) apply(r Rule, found []Finding, now time.Time) bool {
	changed := false
	seen := make(map[string]bool, len(found))
	for _, f := range found {
		a := Alert{Rule: r.ID, Title: r.Title, Severity: r.Severity, Set: f.Set, Message: f.Message}
		key := a.Key()
		seen[key] = true
		if cur, ok := e.firing[key]; ok {
			cur.Message = f.Message
			e.firing[key] = cur
			continue
		}
		if p, ok := e.pending[key]; ok {
			a.Since = p.Since
		} else {
			a.Since = now
		}
		if now.Sub(a.Since) >= r.For {
			a.FiredAt = now
			e.firing[key] = a
			delete(e.pending, key)
			changed = true
			continue
		}
		e.pending[key] = a
	}
	for key, a := range e.pending {
		if a.Rule == r.ID && !seen[key] {
			delete(e.pending, key)
		}
	}
	for key, a := range e.firing {
		if a.Rule == r.ID && !seen[key] {
			a.ResolvedAt = now
			e.resolved = append(e.resolved, a)
			delete(e.firing, key)
			changed = true
		}
	}
	return changed
}

func (e *Engine) keep() time.Duration {
	if e.Keep > 0 {
		return e.Keep
	}
	return 24 * time.Hour
}

// State is a copy of the current alerts, newest first.
func (e *Engine) State() State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := State{Evaluated: e.at, Rules: slices.Clone(e.status), Resolved: slices.Clone(e.resolved)}
	for _, a := range e.firing {
		s.Firing = append(s.Firing, a)
	}
	slices.SortFunc(s.Firing, func(a, b Alert) int {
		return cmp.Or(b.FiredAt.Compare(a.FiredAt), cmp.Compare(a.Key(), b.Key()))
	})
	slices.SortFunc(s.Resolved, func(a, b Alert) int { return b.ResolvedAt.Compare(a.ResolvedAt) })
	return s
}
