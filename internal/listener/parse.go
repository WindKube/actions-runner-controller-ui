package listener

import (
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/samber/lo"
)

// Metric names ARC's listener exposes. Only the ones the dashboard reads are
// listed; gha_job_startup_duration_seconds and gha_job_execution_duration_seconds
// are histograms and are deliberately ignored.
const (
	metricAssignedJobs      = "gha_assigned_jobs"
	metricRunningJobs       = "gha_running_jobs"
	metricRegisteredRunners = "gha_registered_runners"
	metricBusyRunners       = "gha_busy_runners"
	metricIdleRunners       = "gha_idle_runners"
	metricDesiredRunners    = "gha_desired_runners"
	metricMinRunners        = "gha_min_runners"
	metricMaxRunners        = "gha_max_runners"
	metricStartedJobs       = "gha_started_jobs_total"
	metricCompletedJobs     = "gha_completed_jobs_total"
)

// collectedFamilies is every family parse asks collect for, in one list because
// the scrape filter has to know them before the parser does: a family added
// below and to Metrics but not here is dropped on the way in and reads as a
// listener that never exposed it. TestFilterCoversEveryCollectedFamily is the
// tripwire for that.
var collectedFamilies = []string{
	metricAssignedJobs,
	metricRunningJobs,
	metricRegisteredRunners,
	metricBusyRunners,
	metricIdleRunners,
	metricDesiredRunners,
	metricMinRunners,
	metricMaxRunners,
	metricStartedJobs,
	metricCompletedJobs,
}

// Label keys carrying the scale set name. ARC uses `name`; some listener
// versions and recording rules carry `runner_scale_set_name` instead, so both
// are accepted with `name` winning.
const (
	labelName         = "name"
	labelScaleSetName = "runner_scale_set_name"
	// labelNamespace does not key anything — the exported maps are keyed by bare
	// scale set name — but it is what distinguishes two different scale sets that
	// share one, which decides how their series combine. It is also the only thing
	// that can; see namespaceIndex.
	labelNamespace = "namespace"
)

// aggregation says what to do when one scale set name carries several series.
type aggregation int

const (
	// sum totals them. Correct for the counters, which upstream splits by
	// repository / job_workflow_ref / job_result: the per-set number is the
	// total across those breakdowns. Also the least-wrong answer for the job
	// gauges, which count things that really do add up.
	sum aggregation = iota
	// perScaleSet keeps one series' value. Correct for the configuration
	// gauges (min/max/desired): those are per-scale-set limits, not
	// quantities, so two same-named sets in different namespaces have two
	// independent ceilings and their sum is a limit that exists nowhere.
	perScaleSet
)

// Metrics is the parsed result, exported so it can be unit-tested.
//
// Every map is keyed by scale set name. A metric the listener did not expose is
// simply an absent map, never a map of zeros — "not reported" and "zero" are
// different answers and the dashboard renders them differently.
//
// The key is the bare name, so two scale sets sharing a name in different
// namespaces share one entry: counters are the total across both, while
// Min/Max/DesiredRunners report one of the two (see collect), because a summed
// ceiling would be a limit no set actually has.
type Metrics struct {
	AssignedJobs       map[string]float64
	RunningJobs        map[string]float64
	RegisteredRunners  map[string]float64
	BusyRunners        map[string]float64
	IdleRunners        map[string]float64
	DesiredRunners     map[string]float64
	MinRunners         map[string]float64
	MaxRunners         map[string]float64
	StartedJobsTotal   map[string]float64
	CompletedJobsTotal map[string]float64
}

// parse reads Prometheus text-format exposition into Metrics, along with the
// namespace index it built on the way, from which the scale set name collisions
// fall out — a deployment mistake worth naming, but not a parse error.
//
// It is deliberately tolerant: unknown metric families are ignored, histograms and
// summaries are skipped, and a family the listener never exposed just leaves its
// map nil. A scrape that parses cleanly but contains nothing we recognise is a
// valid — if useless — result, not an error.
//
// It returns the index rather than the collisions themselves because one listener
// serves one scale set: two same-named sets in different namespaces produce two
// separate bodies, and the collision only exists once their indexes are unioned.
func parse(r io.Reader) (Metrics, namespaceIndex, error) {
	// The validation scheme must be passed explicitly: a zero-valued
	// TextParser carries model.UnsetValidation and panics on the first metric
	// name it sees. UTF8Validation is the permissive one — we are reading
	// someone else's exposition, not deciding what is admissible.
	p := expfmt.NewTextParser(model.UTF8Validation)
	families, err := p.TextToMetricFamilies(r)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("parsing prometheus exposition: %w", err)
	}

	// One index across all three ceiling families, not one each: a name reused across
	// namespaces is a single deployment fact. Sharing it also catches the split case —
	// max from one namespace, min from another, neither family seeing two.
	ns := namespaceIndex{}
	m := Metrics{
		AssignedJobs:       collect(families, metricAssignedJobs, sum, ns),
		RunningJobs:        collect(families, metricRunningJobs, sum, ns),
		RegisteredRunners:  collect(families, metricRegisteredRunners, sum, ns),
		BusyRunners:        collect(families, metricBusyRunners, sum, ns),
		IdleRunners:        collect(families, metricIdleRunners, sum, ns),
		DesiredRunners:     collect(families, metricDesiredRunners, perScaleSet, ns),
		MinRunners:         collect(families, metricMinRunners, perScaleSet, ns),
		MaxRunners:         collect(families, metricMaxRunners, perScaleSet, ns),
		StartedJobsTotal:   collect(families, metricStartedJobs, sum, ns),
		CompletedJobsTotal: collect(families, metricCompletedJobs, sum, ns),
	}
	return m, ns, nil
}

// fold merges other into m, with the same per-family rules parse applies within one
// body.
//
// It exists because a fleet's metrics arrive as one body per listener, and the two
// aggregations have to agree: a family summed inside a body and kept first-wins
// across bodies would report a different number depending on how ARC happened to
// distribute its listeners.
func (m *Metrics) fold(other Metrics) {
	// Summed, like parse's `sum` families: these count things that add up, and
	// upstream already splits them by repository and workflow within one body.
	foldSum(&m.AssignedJobs, other.AssignedJobs)
	foldSum(&m.RunningJobs, other.RunningJobs)
	foldSum(&m.RegisteredRunners, other.RegisteredRunners)
	foldSum(&m.BusyRunners, other.BusyRunners)
	foldSum(&m.IdleRunners, other.IdleRunners)
	foldSum(&m.StartedJobsTotal, other.StartedJobsTotal)
	foldSum(&m.CompletedJobsTotal, other.CompletedJobsTotal)

	// Kept, like parse's `perScaleSet` families: these are per-scale-set limits
	// rather than quantities, and the sum of two ceilings is a ceiling that
	// exists nowhere.
	foldKeep(&m.DesiredRunners, other.DesiredRunners)
	foldKeep(&m.MinRunners, other.MinRunners)
	foldKeep(&m.MaxRunners, other.MaxRunners)
}

// foldSum adds src into dst, allocating only if there is something to add. A
// family no listener exposed stays nil, because "not reported" and "zero" are
// different answers.
func foldSum(dst *map[string]float64, src map[string]float64) {
	for set, v := range src {
		if *dst == nil {
			*dst = make(map[string]float64, len(src))
		}
		(*dst)[set] += v
	}
}

// foldKeep takes src's value only for sets dst has never seen.
func foldKeep(dst *map[string]float64, src map[string]float64) {
	for set, v := range src {
		if *dst == nil {
			*dst = make(map[string]float64, len(src))
		}
		if _, seen := (*dst)[set]; !seen {
			(*dst)[set] = v
		}
	}
}

// namespaceIndex records which namespaces claimed each scale set name.
//
// Collisions are spotted on the namespace label alone, because it is the only
// thing in the exposition that tells two same-named scale sets apart. So two
// genuinely different scale sets that share a name and carry no namespace label
// are indistinguishable from one scale set exposing the same gauge twice: the
// index sees a single namespace (""), nothing is reported, and the first series
// wins. ARC's listener always labels these gauges, but when it happens it is
// silent.
//
// The index is reported rather than logged where it is found: the body is
// untrusted and can carry tens of thousands of collisions, and only the caller
// knows how often it is being told. See collisionTracker.
type namespaceIndex map[string]map[string]struct{}

// add records that set was seen under ns, which is "" when the series carries
// no namespace label.
func (i namespaceIndex) add(set, ns string) {
	seen, ok := i[set]
	if !ok {
		seen = map[string]struct{}{}
		i[set] = seen
	}
	seen[ns] = struct{}{}
}

// collisions reports the names more than one namespace claimed: how many there
// are, and at most maxNamedCollisions of them rendered for a log line.
//
// Both the list and each name's namespaces are sorted, and every part that comes
// from the scrape body is cut down by truncateLabel, so the same deployment
// produces the same bounded report scrape after scrape. collisionTracker's dedup
// depends on that: an unstable report would look like a new fact every tick.
func (i namespaceIndex) collisions() (count int, rendered []string) {
	names := make([]string, 0, len(i))
	for set, seen := range i {
		if len(seen) > 1 {
			names = append(names, set)
		}
	}
	slices.Sort(names)

	for _, set := range names[:min(len(names), maxNamedCollisions)] {
		all := slices.Sorted(maps.Keys(i[set]))
		shown := lo.Map(all[:min(len(all), maxNamedNamespaces)], func(ns string, _ int) string {
			if ns == "" {
				return unlabelledNamespace
			}
			return truncateLabel(ns)
		})
		entry := truncateLabel(set) + " in " + strings.Join(shown, ", ")
		if extra := len(all) - len(shown); extra > 0 {
			entry += fmt.Sprintf(" and %d more", extra)
		}
		rendered = append(rendered, entry)
	}
	return len(names), rendered
}

// union folds other into i, so a name claimed by two listeners in two
// namespaces is the collision it would have been inside one body.
func (i namespaceIndex) union(other namespaceIndex) {
	for set, namespaces := range other {
		for ns := range namespaces {
			i.add(set, ns)
		}
	}
}

// collect pulls one metric family out, keyed by scale set name.
//
// Counters are looked up under both their exposed name and the bare name: classic
// text exposition keeps the `_total` suffix in the family name, OpenMetrics strips
// it, and which one a parser hands back has changed between prometheus/common
// releases.
//
// The key is the bare scale set name, so a name reused in two namespaces folds two
// different scale sets together. agg decides how; see the aggregation constants.
//
// ns is filled in for perScaleSet families only, because only they have to choose
// between namespaces.
func collect(families map[string]*dto.MetricFamily, name string, agg aggregation, ns namespaceIndex) map[string]float64 {
	mf, ok := families[name]
	if !ok {
		bare, isTotal := strings.CutSuffix(name, "_total")
		if !isTotal {
			return nil
		}
		mf, ok = families[bare]
	}
	if !ok {
		return nil
	}

	out := map[string]float64{}
	// Under perScaleSet, the namespace each key's value came from, so the
	// tie-break has something to compare against. nil under sum.
	var from map[string]string
	if agg == perScaleSet {
		from = map[string]string{}
	}

	for _, metric := range mf.GetMetric() {
		set := setName(metric)
		if set == "" {
			continue
		}
		v, ok := value(metric)
		if !ok {
			continue
		}
		if agg == sum {
			// Duplicate label sets differing only in labels we ignore
			// (namespace, repository, runner group) collapse onto one key; sum
			// them so a per-repository breakdown still totals correctly.
			out[set] += v
			continue
		}

		namespace := labelValue(metric, labelNamespace)
		ns.add(set, namespace)
		prev, seen := from[set]
		if !seen || preferNamespace(namespace, prev) {
			out[set], from[set] = v, namespace
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// preferNamespace reports whether a series from namespace ns should take over the
// key currently holding namespace prev's value. Two rules, in order:
//
//   - A labelled series beats an unlabelled one. labelValue cannot tell "no
//     namespace label" from `namespace=""` and returns "" for both, and "" sorts
//     before every real namespace — so ordering on the raw string alone would let
//     a series that says nothing about where it came from evict one that does.
//   - Otherwise the alphabetically first namespace wins, so which of two real
//     namespaces is reported does not depend on the listener's emission order.
//
// Series repeating within one namespace keep the first.
func preferNamespace(ns, prev string) bool {
	if (ns == "") != (prev == "") {
		return prev == ""
	}
	return ns < prev
}

// labelValue reads one label off a metric, empty when it is absent. An empty
// return is therefore ambiguous — absent label or `label=""` — which is why
// preferNamespace treats it as "unknown" rather than as a namespace name.
func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// setName reads the scale set name off a metric's labels.
func setName(m *dto.Metric) string {
	var fallback string
	for _, l := range m.GetLabel() {
		switch l.GetName() {
		case labelName:
			if v := l.GetValue(); v != "" {
				return v
			}
		case labelScaleSetName:
			fallback = l.GetValue()
		}
	}
	return fallback
}

// value extracts a scalar from whichever typed payload the metric carries.
// Histograms and summaries have no single value and are reported as absent.
func value(m *dto.Metric) (float64, bool) {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue(), true
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue(), true
	case m.GetUntyped() != nil:
		return m.GetUntyped().GetValue(), true
	default:
		return 0, false
	}
}

// QueueDepth derives per-set queue depth: assigned minus running, floored at 0.
//
// The two gauges are sampled independently by the listener, so a job that has just
// started can be counted as running before it stops being counted as assigned.
// That transient makes the difference negative.
func (m Metrics) QueueDepth() map[string]int {
	out := make(map[string]int, len(m.AssignedJobs))
	for set, assigned := range m.AssignedJobs {
		out[set] = floorDepth(assigned - m.RunningJobs[set])
	}
	// A set that reports running jobs but no assigned gauge still has a known
	// queue depth of zero, which is worth saying explicitly.
	for set := range m.RunningJobs {
		if _, ok := out[set]; !ok {
			out[set] = 0
		}
	}
	return out
}

// floorDepth rounds a gauge difference to a whole job, never below zero.
//
// The gauges come from an operator-supplied URL that may not be a listener at all,
// and "+Inf", "-Inf" and "NaN" are all legal Prometheus exposition, so d is not
// trusted to be finite. Anything outside int range is rejected before the
// conversion: int(math.Round(d)) is implementation-defined once the result does
// not fit, and on this toolchain +Inf becomes math.MaxInt64 — a 19-digit queue
// depth rather than anything a reader would recognise as broken.
//
// Each clause earns its place: NaN fails every ordered comparison and needs the
// explicit test; -Inf and negatives fall out of d <= 0; +Inf and finite junk too
// large to round into an int fall out of the magnitude test (float64 rounds
// math.MaxInt64 up to 2^63, so the bound is exclusive at exactly the first value
// that would overflow).
func floorDepth(d float64) int {
	if math.IsNaN(d) || d <= 0 || d >= math.MaxInt {
		return 0
	}
	return int(math.Round(d))
}
