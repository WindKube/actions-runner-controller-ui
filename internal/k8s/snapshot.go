package k8s

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"
)

// registrationGrace is how long a runner may live without a GitHub runner id.
// Past it the runner almost always failed to reach GitHub (bad URL, credentials,
// egress) while its pod looks perfectly healthy.
const registrationGrace = 5 * time.Minute

// EventRetention is the API server's default --event-ttl.
const EventRetention = time.Hour

// SnapshotInput is everything BuildSnapshot needs, as plain objects so the ARC
// semantics are testable without a cluster.
type SnapshotInput struct {
	Now time.Time

	// Org overrides the breadcrumb org; otherwise it comes from githubConfigUrl.
	Org string

	Sets          []*arcapi.AutoscalingRunnerSet
	EphemeralSets []*arcapi.EphemeralRunnerSet
	Runners       []*arcapi.EphemeralRunner
	Listeners     []*arcapi.AutoscalingListener

	RunnerPods     []*corev1.Pod
	ControllerPods []*corev1.Pod

	// Usage is keyed "namespace/podname".
	Usage map[string]fleet.Usage

	Queue      map[string]int
	QueueKnown bool

	Sources []fleet.Source

	// RunnersDegraded means the EphemeralRunner informer is not synced or its
	// watch is failing, so an empty Runners slice cannot be trusted.
	RunnersDegraded bool

	JobStarts *JobStartTracker
}

// BuildSnapshot turns Kubernetes objects into the fleet model. It is pure.
func BuildSnapshot(in SnapshotInput) fleet.Snapshot {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}

	podsByKey := indexPods(in.RunnerPods)
	scaleSets := indexScaleSets(in.Sets)
	ersOwner := ephemeralSetOwners(in.EphemeralSets, scaleSets)
	listenerBySet := listenerHealth(in.Listeners, in.ControllerPods, in.Sets)

	snap := fleet.Snapshot{
		At:      now,
		Org:     in.Org,
		Sources: slices.Clone(in.Sources),
	}
	sortSources(snap.Sources)

	live := make(map[types.UID]struct{}, len(in.Runners))
	snap.Runners = make([]fleet.Runner, 0, len(in.Runners))
	for _, er := range in.Runners {
		if er == nil {
			continue
		}
		live[er.UID] = struct{}{}
		pod := podsByKey[objectKey(er.Namespace, er.Name)]
		setName := setNameForRunner(er, ersOwner)
		snap.Runners = append(snap.Runners, buildRunner(er, pod, setName, scaleSets.byName[objectKey(er.Namespace, setName)], in, now))
	}

	current := make(map[string]int, len(in.Sets))
	if in.RunnersDegraded {
		// The runner cache can't be counted; the ERS replica targets still can.
		for _, ers := range in.EphemeralSets {
			if ers == nil {
				continue
			}
			if set, ok := ersOwner[objectKey(ers.Namespace, ers.Name)]; ok {
				current[objectKey(ers.Namespace, set)] += ers.Spec.Replicas
			}
		}
	} else {
		for _, r := range snap.Runners {
			switch r.State {
			case fleet.StateBusy, fleet.StateIdle, fleet.StatePending:
				current[objectKey(r.Namespace, r.SetName)]++
			}
		}
	}

	versions := map[string]struct{}{}
	snap.Sets = make([]fleet.RunnerSet, 0, len(in.Sets))
	for _, ars := range in.Sets {
		if ars == nil {
			continue
		}
		if snap.Org == "" {
			snap.Org = arcapi.OrgFromConfigURL(ars.Spec.GitHubConfigURL)
		}

		set := fleet.RunnerSet{
			Name:            ars.Name,
			Namespace:       ars.Namespace,
			MinRunners:      ars.Spec.MinRunnersValue(),
			RunnerGroup:     ars.Spec.RunnerGroup,
			RunnerLabels:    ars.Spec.RunnerScaleSetLabels,
			GitHubConfigURL: ars.Spec.GitHubConfigURL,
			ScaleSetID:      ars.Annotations[arcapi.AnnotationRunnerScaleSetID],
			Phase:           ars.Status.Phase,
			NodeSelector:    ars.Spec.Template.Spec.NodeSelector,
			ContainerMode:   ars.Spec.ContainerMode(),
			Version:         ars.Labels[arcapi.LabelVersion],
			Current:         current[objectKey(ars.Namespace, ars.Name)],
		}
		if set.Version != "" {
			versions[set.Version] = struct{}{}
		}

		if maxRunners, ok := ars.Spec.MaxRunnersValue(); ok {
			set.MaxRunners = maxRunners
		} else {
			set.Unbounded = true
		}

		if c, ok := ars.Spec.RunnerContainer(); ok {
			set.Image = c.Image
			set.CPURequest = cpuCores(c.Resources.Requests, corev1.ResourceCPU)
			set.MemRequest = memBytes(c.Resources.Requests, corev1.ResourceMemory)
			set.CPULimit = cpuCores(c.Resources.Limits, corev1.ResourceCPU)
			set.MemLimit = memBytes(c.Resources.Limits, corev1.ResourceMemory)
		}

		// A set missing from the queue map has no reading, which is not zero.
		if q, ok := in.Queue[ars.Name]; ok && in.QueueKnown {
			set.Queued = q
			set.QueuedKnown = true
		}

		if lh, ok := listenerBySet[objectKey(ars.Namespace, ars.Name)]; ok {
			set.ListenerKnown = true
			set.ListenerHealthy = lh
			snap.ListenersTotal++
			if lh {
				snap.ListenersReady++
			}
		}

		snap.Sets = append(snap.Sets, set)
	}
	slices.SortFunc(snap.Sets, func(a, b fleet.RunnerSet) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Namespace, b.Namespace))
	})
	snap.ARCVersions = slices.Sorted(maps.Keys(versions))

	// Sweeping on a degraded informer would drop every tracked job start, and
	// every running job would then look freshly started.
	if !in.RunnersDegraded {
		in.JobStarts.Retain(live)
	}

	fleet.SortRunners(snap.Runners, now)
	return snap
}

func buildRunner(er *arcapi.EphemeralRunner, pod *corev1.Pod, setName string, ars *arcapi.AutoscalingRunnerSet, in SnapshotInput, now time.Time) fleet.Runner {
	r := fleet.Runner{
		Name:      er.Name,
		Namespace: er.Namespace,
		SetName:   setName,
		State:     runnerState(er, pod),
		CreatedAt: er.CreationTimestamp.Time,
		RunnerID:  er.Status.RunnerID,
	}

	if er.HasJob() {
		r.Job = fleet.Job{
			Repository: er.Status.JobRepositoryName,
			Workflow:   arcapi.WorkflowFileFromRef(er.Status.JobWorkflowRef),
			Name:       er.Status.JobDisplayName,
			RunID:      er.Status.WorkflowRunID,
		}
	}

	// ARC records no job start time, so it is our first observation of jobId,
	// falling back to the pod start for jobs already running when we booted.
	started := in.JobStarts.Observe(er.UID, er.HasJob(), now)
	if er.HasJob() {
		if started.IsZero() && pod != nil && pod.Status.StartTime != nil {
			started = pod.Status.StartTime.Time
		}
		r.Job.StartedAt = started
	}

	if pod != nil {
		r.Node = pod.Spec.NodeName
		r.PodPhase = string(pod.Status.Phase)
		r.PodUID = string(pod.UID)
		if c, ok := runnerContainerStatus(pod); ok {
			r.Restarts = c.RestartCount
			r.Image = c.Image
		}
		if c, ok := arcapi.RunnerContainer(pod.Spec.Containers); ok {
			if r.Image == "" {
				r.Image = c.Image
			}
			applyResources(&r.CPU, &r.Mem, c.Resources)
		}
	}
	if ars != nil {
		if c, ok := ars.Spec.RunnerContainer(); ok {
			if r.Image == "" {
				r.Image = c.Image
			}
			if r.CPU.Request == 0 && r.Mem.Request == 0 {
				applyResources(&r.CPU, &r.Mem, c.Resources)
			}
		}
	}

	// No sample leaves At zero, which renders as "—" rather than zero usage.
	if u, ok := in.Usage[objectKey(er.Namespace, er.Name)]; ok {
		r.CPU.Used, r.CPU.At = u.CPUCores, u.At
		r.Mem.Used, r.Mem.At = u.MemBytes, u.At
	}

	r.FailureReason, r.FailedAt = failureReason(er, pod, now)
	if r.FailedAt.IsZero() && r.FailureReason != "" {
		r.FailedAt = er.CreationTimestamp.Time
	}
	return r
}

// runnerState: busy is a non-empty jobId, never phase == Running, which idle
// runners report too.
func runnerState(er *arcapi.EphemeralRunner, pod *corev1.Pod) fleet.State {
	// ARC keeps the job fields on a finished runner, so terminal phases come
	// before the job check or a done runner counts as busy.
	if er.Status.Phase == arcapi.EphemeralRunnerPhaseFailed || podFailed(pod) {
		return fleet.StateFailed
	}
	if er.Status.Phase == arcapi.EphemeralRunnerPhaseSucceeded || er.Status.Phase == arcapi.EphemeralRunnerPhaseOutdated {
		return fleet.StateTerminating
	}
	if er.HasJob() {
		return fleet.StateBusy
	}
	// A pod being torn down goes NotReady, which is not pending.
	if er.DeletionTimestamp != nil || (pod != nil && pod.DeletionTimestamp != nil) {
		return fleet.StateTerminating
	}
	if er.Status.Phase == arcapi.EphemeralRunnerPhasePending || !runnerReady(er, pod) {
		return fleet.StatePending
	}
	return fleet.StateIdle
}

func runnerReady(er *arcapi.EphemeralRunner, pod *corev1.Pod) bool {
	if pod == nil {
		return er.Status.Ready
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podFailed(pod *corev1.Pod) bool {
	return pod != nil && pod.Status.Phase == corev1.PodFailed
}

// waitingFailures excludes ContainerCreating and PodInitializing, which are the
// normal path.
var waitingFailures = map[string]bool{
	"ImagePullBackOff":           true,
	"ErrImagePull":               true,
	"InvalidImageName":           true,
	"CrashLoopBackOff":           true,
	"CreateContainerError":       true,
	"CreateContainerConfigError": true,
	"RunContainerError":          true,
	"ImageInspectError":          true,
	"ErrImageNeverPull":          true,
}

// failureReason ranks causes by usefulness: a container that cannot start, then
// one that exited, then the controller's status, then "never registered".
func failureReason(er *arcapi.EphemeralRunner, pod *corev1.Pod, now time.Time) (string, time.Time) {
	if pod != nil {
		for _, cs := range slices.Concat(pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses) {
			if w := cs.State.Waiting; w != nil && waitingFailures[w.Reason] {
				return w.Reason, podFailureTime(pod)
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			// An OOMKill survives a restart only in LastTerminationState.
			if r, at, ok := terminatedReason(cs.LastTerminationState.Terminated); ok {
				return r, at
			}
			if r, at, ok := terminatedReason(cs.State.Terminated); ok {
				return r, at
			}
		}
		if pod.Status.Phase == corev1.PodFailed {
			reason := pod.Status.Reason
			if reason == "" {
				reason = "pod failed"
			}
			return reason, podFailureTime(pod)
		}
	}

	if parts := lo.Compact([]string{er.Status.Reason, er.Status.Message}); len(parts) > 0 {
		return strings.Join(parts, ": "), er.Status.LastFailure().Time
	}

	if er.Status.RunnerID == 0 && !er.IsDone() && er.DeletionTimestamp == nil &&
		!er.CreationTimestamp.IsZero() && now.Sub(er.CreationTimestamp.Time) > registrationGrace {
		return "never registered", er.CreationTimestamp.Time
	}

	return "", time.Time{}
}

// terminatedReason prefers the exit code over a bare "Error" reason.
func terminatedReason(t *corev1.ContainerStateTerminated) (string, time.Time, bool) {
	if t == nil {
		return "", time.Time{}, false
	}
	at := t.FinishedAt.Time
	if t.Reason != "" && t.Reason != "Error" && t.Reason != "Completed" {
		return t.Reason, at, true
	}
	if t.ExitCode != 0 {
		return fmt.Sprintf("exit code %d", t.ExitCode), at, true
	}
	return "", time.Time{}, false
}

func podFailureTime(pod *corev1.Pod) time.Time {
	if pod == nil {
		return time.Time{}
	}
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	return pod.CreationTimestamp.Time
}

// indexPods: a runner pod always shares its EphemeralRunner's namespace and name.
func indexPods(pods []*corev1.Pod) map[string]*corev1.Pod {
	out := make(map[string]*corev1.Pod, len(pods))
	for _, p := range pods {
		if p != nil {
			out[objectKey(p.Namespace, p.Name)] = p
		}
	}
	return out
}

type scaleSetIndex struct {
	byUID  map[types.UID]*arcapi.AutoscalingRunnerSet
	byName map[string]*arcapi.AutoscalingRunnerSet
}

func indexScaleSets(sets []*arcapi.AutoscalingRunnerSet) scaleSetIndex {
	idx := scaleSetIndex{
		byUID:  make(map[types.UID]*arcapi.AutoscalingRunnerSet, len(sets)),
		byName: make(map[string]*arcapi.AutoscalingRunnerSet, len(sets)),
	}
	for _, s := range sets {
		if s == nil {
			continue
		}
		idx.byUID[s.UID] = s
		idx.byName[objectKey(s.Namespace, s.Name)] = s
	}
	return idx
}

// ownerOf resolves by UID first, because names are reused when a scale set is
// deleted and recreated.
func (idx scaleSetIndex) ownerOf(ers *arcapi.EphemeralRunnerSet) *arcapi.AutoscalingRunnerSet {
	for _, ref := range ers.OwnerReferences {
		if ref.Kind != "AutoscalingRunnerSet" {
			continue
		}
		if o, ok := idx.byUID[ref.UID]; ok {
			return o
		}
		if o, ok := idx.byName[objectKey(ers.Namespace, ref.Name)]; ok {
			return o
		}
	}
	return nil
}

func ephemeralSetOwners(sets []*arcapi.EphemeralRunnerSet, owners scaleSetIndex) map[string]string {
	out := make(map[string]string, len(sets))
	for _, ers := range sets {
		if ers == nil {
			continue
		}
		if owner := owners.ownerOf(ers); owner != nil {
			out[objectKey(ers.Namespace, ers.Name)] = owner.Name
		}
	}
	return out
}

// setNameForRunner prefers the owner chain, because ARC truncates label values.
func setNameForRunner(er *arcapi.EphemeralRunner, ersOwner map[string]string) string {
	for _, ref := range er.OwnerReferences {
		if ref.Kind != "EphemeralRunnerSet" {
			continue
		}
		if name, ok := ersOwner[objectKey(er.Namespace, ref.Name)]; ok {
			return name
		}
	}
	return er.Labels[arcapi.LabelScaleSetName]
}

// listenerHealth reads the listener pod, since the listener's status is empty.
// A Stopped phase (0.15+) wins over the pod. Without the listener CRD the pod's
// scale-set labels are the fallback link.
func listenerHealth(listeners []*arcapi.AutoscalingListener, controllerPods []*corev1.Pod, sets []*arcapi.AutoscalingRunnerSet) map[string]bool {
	podsByKey := indexPods(controllerPods)
	out := make(map[string]bool, len(sets))

	for _, l := range listeners {
		if l == nil {
			continue
		}
		key := objectKey(l.Spec.AutoscalingRunnerSetNamespace, l.Spec.AutoscalingRunnerSetName)
		pod := podsByKey[objectKey(l.Namespace, l.Name)]
		out[key] = l.Spec.Phase != arcapi.AutoscalingListenerPhaseStopped && pod != nil && podReady(pod)
	}

	for _, p := range controllerPods {
		if p == nil {
			continue
		}
		name := p.Labels[arcapi.LabelScaleSetName]
		ns := p.Labels[arcapi.LabelScaleSetNamespace]
		if name == "" || ns == "" {
			continue
		}
		key := objectKey(ns, name)
		if _, known := out[key]; known {
			continue
		}
		out[key] = podReady(p)
	}
	return out
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func objectKey(namespace, name string) string { return namespace + "/" + name }

// cpuCores goes through MilliValue because Quantity.Value() rounds 250m up to 1.
func cpuCores(list corev1.ResourceList, name corev1.ResourceName) float64 {
	q, ok := list[name]
	if !ok {
		return 0
	}
	return float64(q.MilliValue()) / 1000
}

func memBytes(list corev1.ResourceList, name corev1.ResourceName) float64 {
	q, ok := list[name]
	if !ok {
		return 0
	}
	return float64(q.Value())
}

func applyResources(cpu, mem *fleet.Resources, req corev1.ResourceRequirements) {
	cpu.Request = cpuCores(req.Requests, corev1.ResourceCPU)
	mem.Request = memBytes(req.Requests, corev1.ResourceMemory)
	cpu.Limit = cpuCores(req.Limits, corev1.ResourceCPU)
	mem.Limit = memBytes(req.Limits, corev1.ResourceMemory)
}

func runnerContainerStatus(pod *corev1.Pod) (corev1.ContainerStatus, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == arcapi.RunnerContainerName {
			return cs, true
		}
	}
	if len(pod.Status.ContainerStatuses) > 0 {
		return pod.Status.ContainerStatuses[0], true
	}
	return corev1.ContainerStatus{}, false
}
