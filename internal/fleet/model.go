// Package fleet is the dashboard's domain model: the current state of every
// runner and scale set, plus the filtering and aggregation the views render.
//
// It knows nothing about Kubernetes or HTTP. Collectors push a Snapshot in;
// the web layer reads view models out. That boundary is what makes the whole
// aggregation layer testable without a cluster.
package fleet

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
)

// State is a runner's lifecycle state as the dashboard presents it.
//
// This is deliberately not ARC's EphemeralRunner phase. That phase describes
// the *pod* — a runner sitting idle waiting for work reports "Running" — so it
// cannot distinguish the two states an operator most wants to tell apart.
type State string

// Runner states, in the order the runner table sorts them.
const (
	StateBusy        State = "busy"
	StatePending     State = "pending"
	StateIdle        State = "idle"
	StateTerminating State = "terminating"
	StateFailed      State = "failed"
)

// AllStates lists every state a filter may select, in display order.
func AllStates() []State {
	return []State{StateBusy, StateIdle, StatePending, StateFailed, StateTerminating}
}

// sortRank orders the runner table: work first, problems last.
func (s State) sortRank() int {
	switch s {
	case StateBusy:
		return 0
	case StatePending:
		return 1
	case StateIdle:
		return 2
	case StateTerminating:
		return 3
	case StateFailed:
		return 4
	}
	return 5
}

// Resources is a request/limit/usage triple for one dimension.
//
// Used is only meaningful when At is non-zero: metrics-server holds no history
// and takes up to ~30s to first report a pod, so a short-lived ephemeral
// runner may live and die without ever being scraped. That must render as "—",
// never as zero, or the dashboard understates fleet usage.
type Resources struct {
	Used    float64
	Request float64
	Limit   float64
	At      time.Time
}

// HasUsage reports whether a usage sample was actually observed.
func (r Resources) HasUsage() bool { return !r.At.IsZero() }

// Efficiency is usage as a fraction of request, or zero when either is absent.
func (r Resources) Efficiency() float64 {
	if r.Request <= 0 || !r.HasUsage() {
		return 0
	}
	return r.Used / r.Request
}

// Job is the workflow job a runner is currently executing.
type Job struct {
	Repository string
	Workflow   string // filename, parsed out of jobWorkflowRef
	Name       string // jobDisplayName
	RunID      int64
	RequestID  int64
	StartedAt  time.Time
}

// Present reports whether a job is assigned.
func (j Job) Present() bool { return j.Repository != "" || j.Name != "" }

// Runner is one EphemeralRunner joined with its pod and latest metrics.
type Runner struct {
	Name      string
	Namespace string
	SetName   string
	State     State

	Job Job

	CreatedAt time.Time
	Node      string
	Image     string
	PodPhase  string
	Restarts  int32
	RunnerID  int

	// PodUID identifies the runner's pod. It is carried purely so the events
	// lookup can pin its field selector to this exact pod: runner names are
	// generated from the scale set name and do get reused, and a selector on
	// name alone will happily return an hour of a long-dead pod's events and
	// present them as this runner's.
	PodUID string

	CPU Resources // cores
	Mem Resources // bytes

	// FailureReason is the human-facing cause shown in the failure lane:
	// ImagePullBackOff, OOMKilled, exit code 1, never registered.
	FailureReason string
	// FailedAt is when the failure was observed, for the failure lane's
	// relative timestamps.
	FailedAt time.Time
}

// Age is how long the runner has existed.
func (r Runner) Age(now time.Time) time.Duration {
	if r.CreatedAt.IsZero() {
		return 0
	}
	return now.Sub(r.CreatedAt)
}

// JobAge is how long the current job has been running, or zero when idle.
func (r Runner) JobAge(now time.Time) time.Duration {
	if r.Job.StartedAt.IsZero() {
		return 0
	}
	return now.Sub(r.Job.StartedAt)
}

// RunnerSet is an AutoscalingRunnerSet joined with its live EphemeralRunnerSet
// counts and, when available, listener queue depth.
type RunnerSet struct {
	Name      string
	Namespace string

	MinRunners int
	MaxRunners int
	// Unbounded is true when maxRunners was unset. The controller substitutes
	// MaxInt32 internally, so this flag is the only way to tell "two billion"
	// from "no ceiling" and render the capacity line honestly.
	Unbounded bool

	RunnerGroup     string
	Image           string
	RunnerLabels    []string
	NodeSelector    map[string]string
	GitHubConfigURL string
	ScaleSetID      string
	Phase           string

	// Counts come from the EphemeralRunnerSet, which is authoritative.
	Current int
	Pending int
	Running int
	Failed  int

	// Queued is jobs assigned to this scale set with no runner yet. It comes
	// from the ARC listener's metrics, which are disabled by default, so
	// QueuedKnown is false on most installs.
	Queued      int
	QueuedKnown bool

	// Per-runner requests, used to compute the set's aggregate request.
	CPURequest float64
	MemRequest float64
	CPULimit   float64
	MemLimit   float64

	ListenerHealthy bool
	ListenerKnown   bool
}

// AtCapacity reports whether the set has hit its ceiling — the condition the
// design paints red.
func (s RunnerSet) AtCapacity() bool {
	return !s.Unbounded && s.MaxRunners > 0 && s.Current >= s.MaxRunners
}

// ScaledToZero reports whether the set currently has no runners at all.
func (s RunnerSet) ScaledToZero() bool { return s.Current == 0 }

// CapacityDenominator is the value capacity bars scale against. An unbounded
// set has no ceiling to draw, so fall back to the current population.
func (s RunnerSet) CapacityDenominator() float64 {
	if !s.Unbounded && s.MaxRunners > 0 {
		return float64(s.MaxRunners)
	}
	if s.Current > 0 {
		return float64(s.Current)
	}
	return 1
}

// Usage is one observed resource sample for a pod, as reported by
// metrics-server. A zero At means "never scraped", which is a routine outcome
// for short-lived ephemeral runners and must not be confused with zero usage.
type Usage struct {
	CPUCores float64
	MemBytes float64
	At       time.Time
}

// Event is a Kubernetes event shown on the runner detail page.
//
// ARC's modern controllers emit no events of their own, so everything here
// comes from the kubelet and scheduler acting on the runner pod — Scheduled,
// Pulled, Started, Failed, BackOff, Evicted, Killing.
type Event struct {
	Type    string // Normal or Warning
	Reason  string
	Message string
	At      time.Time
	Count   int32
}

// Warning reports whether an event is one the operator should notice.
func (e Event) Warning() bool { return e.Type == "Warning" }

// Source records whether one upstream data source is usable. Every source is
// optional: the dashboard boots and serves with all of them broken, naming the
// failures in the control-plane strip rather than rendering zeros.
type Source struct {
	Name string
	// Available is whether the source answered at all.
	Available bool
	// Reason explains a source that is unavailable — or, when Available is
	// true, one that answered only partly. A fleet whose listeners are half
	// reachable has real queue depth for half its sets, and painting that as an
	// outage would be as wrong as painting it as healthy.
	Reason    string
	CheckedAt time.Time
}

// ListenerTarget is one ARC listener metrics endpoint to scrape.
//
// It lives here rather than in the scraper because the two sides must not import
// each other: the collector discovers targets from its pod cache and the scraper
// consumes them, and this package is the vocabulary they already share.
type ListenerTarget struct {
	// Set and Namespace name the scale set this listener serves. They are not
	// used to key anything — the exposition carries its own labels — but they
	// are what makes a per-target error message mean something.
	Set       string
	Namespace string
	Pod       string
	URL       string
}

// Names of the data sources reported in the control-plane strip.
const (
	SourceKubernetes = "kubernetes"
	SourceARCCRDs    = "arc-crds"
	SourceMetrics    = "metrics-server"
	SourceListener   = "listener-metrics"
	SourceStore      = "store"
)

// Snapshot is the complete current state of the fleet at one instant.
type Snapshot struct {
	At      time.Time
	Org     string
	Sets    []RunnerSet
	Runners []Runner
	Sources []Source

	// ControllerVersion is the ARC controller image tag, for the health strip.
	ControllerVersion string
	ListenersReady    int
	ListenersTotal    int
}

// Source looks up one source by name.
func (s Snapshot) Source(name string) (Source, bool) {
	return lo.Find(s.Sources, func(src Source) bool { return src.Name == name })
}

// Set looks up one runner set by name.
func (s Snapshot) Set(name string) (RunnerSet, bool) {
	return lo.Find(s.Sets, func(set RunnerSet) bool { return set.Name == name })
}

// Runner looks up one runner by name.
func (s Snapshot) Runner(name string) (Runner, bool) {
	return lo.Find(s.Runners, func(r Runner) bool { return r.Name == name })
}

// SortRunners orders runners for display: busy first and longest-running
// first within that, so the work that has been going longest is at the top.
func SortRunners(runners []Runner, now time.Time) {
	sort.SliceStable(runners, func(i, j int) bool {
		ri, rj := runners[i], runners[j]
		if a, b := ri.State.sortRank(), rj.State.sortRank(); a != b {
			return a < b
		}
		if ai, aj := ri.JobAge(now), rj.JobAge(now); ai != aj {
			return ai > aj
		}
		return ri.Name < rj.Name
	})
}

// SetSort names an ordering for the runner-set table.
type SetSort string

// Runner-set orderings offered by the design.
const (
	// SortPressure puts sets that need attention first: queued work, then
	// proximity to the ceiling.
	SortPressure    SetSort = "pressure"
	SortRunnerCount SetSort = "runners"
	SortName        SetSort = "name"
)

// ParseSetSort maps a query value to a sort, defaulting to pressure.
func ParseSetSort(v string) SetSort {
	switch SetSort(v) {
	case SortRunnerCount:
		return SortRunnerCount
	case SortName:
		return SortName
	default:
		return SortPressure
	}
}

// SortSets orders the runner-set table.
func SortSets(sets []RunnerSet, by SetSort) {
	switch by {
	case SortRunnerCount:
		sort.SliceStable(sets, func(i, j int) bool {
			if sets[i].Current != sets[j].Current {
				return sets[i].Current > sets[j].Current
			}
			return sets[i].Name < sets[j].Name
		})
	case SortName:
		sort.SliceStable(sets, func(i, j int) bool { return sets[i].Name < sets[j].Name })
	default:
		sort.SliceStable(sets, func(i, j int) bool {
			a, b := sets[i], sets[j]
			if a.Queued != b.Queued {
				return a.Queued > b.Queued
			}
			return a.saturation() > b.saturation()
		})
	}
}

// saturation is how close a set is to its ceiling. Unbounded sets never
// saturate, so they sort below anything under real pressure.
func (s RunnerSet) saturation() float64 {
	if s.Unbounded || s.MaxRunners <= 0 {
		return 0
	}
	return float64(s.Current) / float64(s.MaxRunners)
}

// FormatAge renders a duration the way the design does: seconds under a
// minute, minutes and seconds under an hour, hours and minutes beyond.
// A non-positive duration is an em dash, not "0s".
func FormatAge(d time.Duration) string {
	secs := int(d.Seconds())
	switch {
	case secs <= 0:
		return "—"
	case secs < 60:
		return fmt.Sprintf("%ds", secs)
	case secs < 3600:
		return fmt.Sprintf("%dm %ds", secs/60, secs%60)
	default:
		return fmt.Sprintf("%dh %dm", secs/3600, (secs%3600)/60)
	}
}

// FormatRelative renders how long ago something happened, e.g. "4m ago".
func FormatRelative(t, now time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := now.Sub(t)
	if d < 0 {
		d = 0
	}
	secs := int(d.Seconds())
	switch {
	case secs < 60:
		return fmt.Sprintf("%ds ago", secs)
	case secs < 3600:
		return fmt.Sprintf("%dm ago", secs/60)
	case secs < 86400:
		return fmt.Sprintf("%dh ago", secs/3600)
	default:
		return fmt.Sprintf("%dd ago", secs/86400)
	}
}

// GiB is one gibibyte, the unit the design shows memory in.
const GiB = 1024 * 1024 * 1024

// FormatGiB renders a byte count as the design does, e.g. "12Gi".
//
// Sub-gibibyte values get a decimal place. At %.0f a 512Mi request renders as
// "0Gi", which is character-for-character what no request at all renders as, so
// the panel cannot tell a small request from a missing one. Anything positive
// that would still round to "0.0Gi" is shown as "<0.1Gi" rather than
// reintroducing the same ambiguity one decimal further down.
func FormatGiB(bytes float64) string {
	if bytes <= 0 {
		return "0Gi"
	}
	if gib := bytes / GiB; gib < 1 {
		if gib < 0.05 {
			return "<0.1Gi"
		}
		return fmt.Sprintf("%.1fGi", gib)
	}
	return fmt.Sprintf("%.0fGi", bytes/GiB)
}

// FormatBytes renders a byte count with the largest unit that leaves a
// readable mantissa, e.g. "512 B", "4.0 KiB", "12.0 MiB", "1.5 GiB".
//
// The SQLite file this reports on spans four orders of magnitude over an
// install's life — kibibytes on first boot, gibibytes after thirteen months of
// hourly rollups — so a fixed unit is unreadable at one end or the other.
//
// Promotion happens after rounding, not before: 1048575 bytes is 1023.999 KiB,
// which one decimal place renders as "1024.0 KiB", a quantity nobody writes.
// Binary units throughout, because the numbers are compared against ls and du.
func FormatBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}

	// The last unit has nothing to promote to, so it keeps whatever mantissa it
	// has rather than reporting a number in a unit that does not exist.
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	v, i := float64(bytes)/1024, 0
	for i < len(units)-1 && v >= 1023.95 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// FormatCores renders a core count to one decimal place.
func FormatCores(cores float64) string { return fmt.Sprintf("%.1f", cores) }

// FormatLabels joins runner labels the way the config panel shows them.
func FormatLabels(labels []string) string {
	if len(labels) == 0 {
		return "—"
	}
	return strings.Join(labels, ", ")
}
