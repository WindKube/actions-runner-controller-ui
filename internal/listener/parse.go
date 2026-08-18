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

// Label keys carrying the scale set name. ARC uses `name`; some listener
// versions and recording rules carry `runner_scale_set_name` instead, so both
// are accepted with `name` winning.
const (
	labelName         = "name"
	labelScaleSetName = "runner_scale_set_name"
	// labelNamespace does not key anything — the exported maps are keyed by
	// bare scale set name — but it is what distinguishes two different scale
	// sets that happen to share one, which decides how their series combine.
	// It is also the only thing that can: see namespaceIndex for what that
	// costs when a series does not carry it.
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
// Every map is keyed by scale set name. A metric the listener did not expose
// is simply an absent map, never a map of zeros — "not reported" and "zero"
// are different answers and the dashboard renders them differently.
//
// The key is the bare name, not namespace/name, so two scale sets sharing a
// name in different namespaces share one entry: the job counts and counters
// are the total across both, while MinRunners/MaxRunners/DesiredRunners report
// one of the two sets (see collect) because a summed ceiling would be a limit
// no set actually has.
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

// Parse reads Prometheus text-format exposition into Metrics.
//
// It is deliberately tolerant: unknown metric families are ignored, histograms
// and summaries are skipped, and a family the listener never exposed just
// leaves its map nil. A scrape that parses cleanly but contains nothing we
// recognise is a valid — if useless — result, not an error.
//
// This entry point discards the collision report a cross-namespace scale set
// name produces (see collision). Callers that scrape on a timer want it, and
// should use parse.
func Parse(r io.Reader) (Metrics, error) {
	m, _, err := parse(r)
	return m, err
}

// parse is Parse plus the scale set name collisions it noticed on the way —
// a deployment mistake worth naming, but not a parse error. Parse keeps the
// two-result signature because that is the surface outside this package
// compiles against.
func parse(r io.Reader) (Metrics, []collision, error) {
	// The validation scheme must be passed explicitly: a zero-valued
	// TextParser carries model.UnsetValidation and panics on the first metric
	// name it sees. UTF8Validation is the permissive one — we are reading
	// someone else's exposition, not deciding what is admissible.
	p := expfmt.NewTextParser(model.UTF8Validation)
	families, err := p.TextToMetricFamilies(r)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("parsing prometheus exposition: %w", err)
	}

	// One index across all three ceiling families, not one each: a name reused
	// across namespaces is a single deployment fact, and saying it once per
	// family is that one fact said three times. Sharing it also catches the
	// split case — max from one namespace, min from another, neither family
	// seeing two — which is the same mistake with a worse symptom.
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
	return m, ns.collisions(), nil
}

// collision is one scale set name that turned up under more than one distinct
// namespace label value in the families whose values cannot be added together
// (see perScaleSet). It is a property of how the exposition is labelled rather
// than of this scrape: the same labels next tick produce the same report, and
// will until someone changes them.
//
// That is why parse reports collisions rather than logging them where it finds
// them. The body is untrusted and can carry tens of thousands of them, the same
// fact repeats in every ceiling family, and a scraper re-reads all of it every
// interval — so how loudly to say this needs to know how often it is being
// told, which only the caller knows. See collisionTracker.
type collision struct {
	// set is the scale set name every namespace in namespaces laid claim to.
	set string
	// namespaces is each namespace that carried it, sorted, always at least
	// two. "" is a member like any other and means a series carried no
	// namespace label. Within one family a labelled series always beats it
	// (see preferNamespace) — but a family only that member reached, because a
	// proxy stripped the label from it alone, still reports its value.
	namespaces []string
}

// namespaceIndex records which namespaces claimed each scale set name.
//
// Collisions are spotted on the namespace label alone, because it is the only
// thing in the exposition that tells two same-named scale sets apart. So two
// genuinely different scale sets that share a name and carry no namespace label
// are indistinguishable from one scale set exposing the same gauge twice: the
// index sees a single namespace (""), nothing is reported, the first series
// wins and the rest are dropped. ARC's listener always labels these gauges, so
// this needs a listener or a proxy that does not — but when it happens it is
// silent, and nothing here promises otherwise.
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

// collisions lists the names more than one namespace claimed.
//
// Both the list and each name's namespaces are sorted, so the same deployment
// produces byte-identical reports scrape after scrape however the listener
// ordered its series. collisionTracker's dedup depends on that: an unstable
// report would look like a new fact every tick, which is the noise the report
// exists to avoid.
func (i namespaceIndex) collisions() []collision {
	var out []collision
	for set, seen := range i {
		if len(seen) > 1 {
			out = append(out, collision{set: set, namespaces: slices.Sorted(maps.Keys(seen))})
		}
	}
	slices.SortFunc(out, func(a, b collision) int { return strings.Compare(a.set, b.set) })
	return out
}

// collect pulls one metric family out, keyed by scale set name.
//
// Counters are looked up under both their exposed name and the bare name:
// classic text exposition keeps the `_total` suffix in the family name, but
// OpenMetrics strips it, and which one a parser hands back has changed
// between prometheus/common releases.
//
// The key is the bare scale set name, so a name reused in two namespaces —
// legal, and reachable whenever ARC_UI_NAMESPACES spans more than one — folds
// two different scale sets together. agg decides how: see the aggregation
// constants for why counters may be added and ceilings may not.
//
// ns is filled in for perScaleSet families only, because only they have to
// choose between namespaces. A summed family folds them together on purpose and
// has nothing to report, so passing the same index in is harmless.
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

// preferNamespace reports whether a series from namespace ns should take over
// the key currently holding namespace prev's value. Two rules, in order:
//
//   - A labelled series beats an unlabelled one. labelValue cannot tell "no
//     namespace label" from `namespace=""` and returns "" for both, and "" sorts
//     before every real namespace — so ordering on the raw string alone would
//     let a series that says nothing about where it came from evict one that
//     does, and attribute the surviving value to namespace "".
//   - Otherwise the alphabetically first namespace wins, so which of two real
//     namespaces is reported does not depend on the order the listener happened
//     to emit its series in.
//
// Series repeating within one namespace keep the first: they describe one scale
// set, and there is nothing to choose between them.
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
// The two gauges are sampled independently by the listener, so a job that has
// just started can be counted as running before it stops being counted as
// assigned. That transient makes the difference negative; reporting a negative
// queue would be nonsense, so it floors at zero.
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
// The gauges come from an operator-supplied URL that may not be a listener at
// all, and "+Inf", "-Inf" and "NaN" are all legal Prometheus exposition, so d
// is not trusted to be a sane finite number. Anything outside int range is
// rejected before the conversion: int(math.Round(d)) is implementation-defined
// once the result does not fit, and on this toolchain +Inf becomes
// math.MaxInt64 — a 19-digit queue depth on the dashboard rather than anything
// a reader would recognise as broken.
//
// Each clause earns its place: NaN fails every ordered comparison and needs the
// explicit test; -Inf and negatives fall out of d <= 0; +Inf and finite junk
// too large to round into an int fall out of the magnitude test (float64 rounds
// math.MaxInt64 up to 2^63, so the bound is exclusive at exactly the first
// value that would overflow).
func floorDepth(d float64) int {
	if math.IsNaN(d) || d <= 0 || d >= math.MaxInt {
		return 0
	}
	return int(math.Round(d))
}
