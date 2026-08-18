package fleet

import (
	"fmt"
	"maps"
	"slices"

	"github.com/samber/lo"
)

// AnyValue is the sentinel a select uses for "no filter on this dimension".
const AnyValue = "all"

// Filter narrows the fleet to a subset of runners. A zero Filter matches
// everything, so the unfiltered view needs no special case.
type Filter struct {
	Repo     string
	Workflow string
	Job      string
	Set      string
	State    string
}

// unset treats both the empty string and the "all" sentinel as no filter, so
// a missing query parameter and an explicitly-cleared select behave alike.
func unset(v string) bool { return v == "" || v == AnyValue }

// Match reports whether a runner passes every active dimension.
func (f Filter) Match(r Runner) bool {
	if !unset(f.Repo) && r.Job.Repository != f.Repo {
		return false
	}
	if !unset(f.Workflow) && r.Job.Workflow != f.Workflow {
		return false
	}
	if !unset(f.Job) && r.Job.Name != f.Job {
		return false
	}
	if !unset(f.Set) && r.SetName != f.Set {
		return false
	}
	if !unset(f.State) && string(r.State) != f.State {
		return false
	}
	return true
}

// Active counts how many dimensions are filtering, which drives both the
// "Clear" control's enabled state and the match summary's wording.
func (f Filter) Active() int {
	dimensions := []string{f.Repo, f.Workflow, f.Job, f.Set, f.State}
	return lo.CountBy(dimensions, func(v string) bool { return !unset(v) })
}

// MatchesSet reports whether a runner set survives the set filter. Used to
// scope capacity totals, which must follow the filter or the "of N max"
// denominator contradicts the numerator.
func (f Filter) MatchesSet(s RunnerSet) bool {
	return unset(f.Set) || s.Name == f.Set
}

// Apply returns the runners and sets the filter selects, preserving order.
func (f Filter) Apply(s Snapshot) (runners []Runner, sets []RunnerSet) {
	runners = lo.Filter(s.Runners, func(r Runner, _ int) bool { return f.Match(r) })
	sets = lo.Filter(s.Sets, func(set RunnerSet, _ int) bool { return f.MatchesSet(set) })
	return runners, sets
}

// Summary is the line at the right of the filter bar.
func (f Filter) Summary(matched, total, sets int) string {
	active := f.Active()
	if active == 0 {
		return fmt.Sprintf("%d runners · %d runnersets · no filters", total, sets)
	}
	plural := ""
	if active > 1 {
		plural = "s"
	}
	return fmt.Sprintf("%d of %d runners match %d filter%s", matched, total, active, plural)
}

// Option is one entry in a filter select.
type Option struct {
	Value    string
	Label    string
	Selected bool
}

// Select is one labelled dropdown in the filter bar.
type Select struct {
	// Key is the Datastar signal name bound to this select.
	//
	// These are flat lowercase words, which is not an accident. HTML lowercases
	// attribute names, so `data-bind:fRepo` reaches Datastar as `f-repo`; its
	// default "camel" modifier would then convert that back to `fRepo`. That
	// round trip works, but relying on it means every signal name depends on a
	// case-conversion step. A single lowercase word passes through unchanged.
	Key     string
	Label   string
	Value   string
	Options []Option
	// Filtering is true when this dimension is narrowing the view, which the
	// design signals by colouring the select's text.
	Filtering bool
}

// Selects builds the five filter dropdowns from what the fleet actually
// contains, so a dimension never offers a value that would match nothing.
func (f Filter) Selects(s Snapshot) []Select {
	repos := distinct(s.Runners, func(r Runner) string { return r.Job.Repository })
	workflows := distinct(s.Runners, func(r Runner) string { return r.Job.Workflow })
	jobs := distinct(s.Runners, func(r Runner) string { return r.Job.Name })
	setNames := distinct(s.Sets, func(set RunnerSet) string { return set.Name })
	states := lo.Map(AllStates(), func(st State, _ int) string { return string(st) })

	return []Select{
		{Key: "repo", Label: "repo", Value: f.Repo, Options: options(f.Repo, repos, "all repositories"), Filtering: !unset(f.Repo)},
		{Key: "workflow", Label: "workflow", Value: f.Workflow, Options: options(f.Workflow, workflows, "all workflows"), Filtering: !unset(f.Workflow)},
		{Key: "job", Label: "job", Value: f.Job, Options: options(f.Job, jobs, "all jobs"), Filtering: !unset(f.Job)},
		{Key: "set", Label: "runnerset", Value: f.Set, Options: options(f.Set, setNames, "all runnersets"), Filtering: !unset(f.Set)},
		{Key: "state", Label: "state", Value: f.State, Options: options(f.State, states, "all states"), Filtering: !unset(f.State)},
	}
}

// distinct collects the sorted, deduplicated values key reports for items.
func distinct[T any](items []T, key func(T) string) []string {
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		// An idle runner has no repository or job; those are absences, not values.
		if v := key(item); v != "" && v != "—" {
			seen[v] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// options prepends the "any" entry and marks the current selection. A value
// that is currently selected but no longer present in the fleet is kept, so
// the select never silently jumps to a different filter than the URL says.
func options(current string, values []string, anyLabel string) []Option {
	out := make([]Option, 0, len(values)+2)
	out = append(out, Option{Value: AnyValue, Label: anyLabel, Selected: unset(current)})

	found := false
	for _, v := range values {
		if v == current {
			found = true
		}
		out = append(out, Option{Value: v, Label: v, Selected: v == current})
	}
	if !unset(current) && !found {
		out = append(out, Option{Value: current, Label: current + " (no matches)", Selected: true})
	}
	return out
}
