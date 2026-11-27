// Package fleet is the dashboard's domain model: the current state of every
// runner and scale set, and the filtering and aggregation the views render. It
// knows nothing about Kubernetes or HTTP.
package fleet

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"
)

// State is a runner's state as the dashboard shows it. It is not ARC's phase,
// which describes the pod: an idle runner reports Running.
type State string

const (
	StateBusy        State = "busy"
	StatePending     State = "pending"
	StateIdle        State = "idle"
	StateTerminating State = "terminating"
	StateFailed      State = "failed"
)

func AllStates() []State {
	return []State{StateBusy, StateIdle, StatePending, StateFailed, StateTerminating}
}

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

// Resources is request/limit/usage for one dimension. Used means something only
// when At is set; a runner that was never scraped renders "—", not zero.
type Resources struct {
	Used    float64
	Request float64
	Limit   float64
	At      time.Time
}

func (r Resources) HasUsage() bool { return !r.At.IsZero() }

type Job struct {
	Repository string
	Workflow   string
	Name       string
	RunID      int64
	StartedAt  time.Time
}

func (j Job) Present() bool { return j.Repository != "" || j.Name != "" }

// Runner is one EphemeralRunner joined with its pod and latest usage.
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

	// PodUID pins event lookups to this pod; runner names get reused.
	PodUID string

	CPU Resources // cores
	Mem Resources // bytes

	FailureReason string
	FailedAt      time.Time

	// PodFailures is how many pods ARC has already replaced for this runner;
	// ControllerStatus is the EphemeralRunner's own reason and message.
	PodFailures      int
	ControllerStatus string
}

func (r Runner) Age(now time.Time) time.Duration {
	if r.CreatedAt.IsZero() {
		return 0
	}
	return now.Sub(r.CreatedAt)
}

func (r Runner) JobAge(now time.Time) time.Duration {
	if r.Job.StartedAt.IsZero() {
		return 0
	}
	return now.Sub(r.Job.StartedAt)
}

// RunnerSet is an AutoscalingRunnerSet with its live runner count.
type RunnerSet struct {
	Name      string
	Namespace string

	MinRunners int
	MaxRunners int
	Unbounded  bool

	RunnerGroup     string
	Image           string
	RunnerLabels    []string
	NodeSelector    map[string]string
	GitHubConfigURL string
	ScaleSetID      string
	Phase           string
	ContainerMode   string
	Version         string

	// Current counts the set's pending, idle and busy runners from the cluster.
	Current int

	Queued      int
	QueuedKnown bool

	CPURequest float64
	MemRequest float64
	CPULimit   float64
	MemLimit   float64

	ListenerHealthy bool
	ListenerKnown   bool
}

func (s RunnerSet) AtCapacity() bool {
	return !s.Unbounded && s.MaxRunners > 0 && s.Current >= s.MaxRunners
}

func (s RunnerSet) ScaledToZero() bool { return s.Current == 0 }

// CapacityDenominator falls back to the current population for unbounded sets.
func (s RunnerSet) CapacityDenominator() float64 {
	if !s.Unbounded && s.MaxRunners > 0 {
		return float64(s.MaxRunners)
	}
	if s.Current > 0 {
		return float64(s.Current)
	}
	return 1
}

// Usage is one resource sample for a pod. A zero At means never scraped.
type Usage struct {
	CPUCores float64
	MemBytes float64
	At       time.Time
}

type Event struct {
	Type    string
	Reason  string
	Message string
	At      time.Time
	Count   int32
}

func (e Event) Warning() bool { return e.Type == "Warning" }

// Source says whether one upstream is usable. Reason explains an outage, or a
// partial answer when Available is true.
type Source struct {
	Name      string
	Available bool
	Reason    string
	CheckedAt time.Time
}

const (
	SourceKubernetes = "kubernetes"
	SourceARCCRDs    = "arc-crds"
	SourceStore      = "store"

	SourcePrometheus        = "prometheus"
	SourceARCMetrics        = "arc-metrics"
	SourceControllerMetrics = "controller-metrics"
	SourceCAdvisor          = "cadvisor"
	SourceKSM               = "kube-state-metrics"
)

type Snapshot struct {
	At      time.Time
	Org     string
	Sets    []RunnerSet
	Runners []Runner
	Sources []Source

	// ARCVersions are the distinct app.kubernetes.io/version labels on the sets.
	ARCVersions    []string
	ListenersReady int
	ListenersTotal int
}

func (s Snapshot) Source(name string) (Source, bool) {
	return lo.Find(s.Sources, func(src Source) bool { return src.Name == name })
}

func (s Snapshot) Set(name string) (RunnerSet, bool) {
	return lo.Find(s.Sets, func(set RunnerSet) bool { return set.Name == name })
}

func (s Snapshot) Runner(name string) (Runner, bool) {
	return lo.Find(s.Runners, func(r Runner) bool { return r.Name == name })
}

// SortRunners puts busy first, longest-running job first within that.
func SortRunners(runners []Runner, now time.Time) {
	slices.SortStableFunc(runners, func(a, b Runner) int {
		return cmp.Or(
			cmp.Compare(a.State.sortRank(), b.State.sortRank()),
			cmp.Compare(b.JobAge(now), a.JobAge(now)),
			cmp.Compare(a.Name, b.Name),
		)
	})
}

type SetSort string

const (
	// SortPressure puts queued work first, then proximity to the ceiling.
	SortPressure    SetSort = "pressure"
	SortRunnerCount SetSort = "runners"
	SortName        SetSort = "name"
)

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

func SortSets(sets []RunnerSet, by SetSort) {
	switch by {
	case SortRunnerCount:
		slices.SortStableFunc(sets, func(a, b RunnerSet) int {
			return cmp.Or(cmp.Compare(b.Current, a.Current), cmp.Compare(a.Name, b.Name))
		})
	case SortName:
		slices.SortStableFunc(sets, func(a, b RunnerSet) int { return cmp.Compare(a.Name, b.Name) })
	default:
		slices.SortStableFunc(sets, func(a, b RunnerSet) int {
			return cmp.Or(cmp.Compare(b.Queued, a.Queued), cmp.Compare(b.saturation(), a.saturation()))
		})
	}
}

func (s RunnerSet) saturation() float64 {
	if s.Unbounded || s.MaxRunners <= 0 {
		return 0
	}
	return float64(s.Current) / float64(s.MaxRunners)
}

// FormatAge renders 42s, 3m 5s or 2h 10m; a non-positive duration is "—".
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

func FormatRelative(t, now time.Time) string {
	if t.IsZero() {
		return "—"
	}
	secs := int(max(0, now.Sub(t)).Seconds())
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

const GiB = 1024 * 1024 * 1024

// FormatGiB keeps a decimal under 1Gi so 512Mi does not render as "0Gi".
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

// FormatBytes uses binary units and promotes after rounding, so 1048575 bytes
// is "1.0 MiB", not "1024.0 KiB".
func FormatBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	v, i := float64(bytes)/1024, 0
	for i < len(units)-1 && v >= 1023.95 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

func FormatCores(cores float64) string { return fmt.Sprintf("%.1f", cores) }

func FormatLabels(labels []string) string {
	if len(labels) == 0 {
		return "—"
	}
	return strings.Join(labels, ", ")
}
