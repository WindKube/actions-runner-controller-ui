package k8s

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"
)

// registrationGrace is how long a runner may exist without a GitHub runner id
// before we call it broken.
//
// A runner registers within a couple of seconds of its pod starting; anything
// still unregistered after this has almost always failed to reach GitHub —
// wrong config URL, expired app credentials, blocked egress — and shows nothing
// else wrong on the pod, so without this check it sits in the table looking
// perfectly healthy forever.
const registrationGrace = 5 * time.Minute

// EventRetention is the API server's default --event-ttl. Events older than
// this are gone, so an empty event list for a long-lived pod is normal and the
// UI should say "no recent events" rather than implying nothing ever happened.
const EventRetention = time.Hour

// SnapshotInput is everything BuildSnapshot needs, as plain typed objects.
//
// The builder takes slices rather than informer listers on purpose: every ARC
// semantic that is easy to get wrong lives in here, and all of it has to be
// testable without a cluster.
type SnapshotInput struct {
	Now time.Time

	// Org overrides the organisation shown in the breadcrumb. When empty it is
	// derived from the first scale set's githubConfigUrl — never from the
	// organisation labels, which ARC trims to 63 characters with a "-trim"
	// suffix and which are therefore not identifiers at all.
	Org string

	ControllerVersion string

	Sets          []*arcapi.AutoscalingRunnerSet
	EphemeralSets []*arcapi.EphemeralRunnerSet
	Runners       []*arcapi.EphemeralRunner
	Listeners     []*arcapi.AutoscalingListener

	// RunnerPods are pods carrying the actions-ephemeral-runner label.
	RunnerPods []*corev1.Pod
	// ControllerPods are pods in the controller namespace, which is where the
	// listener pods live.
	ControllerPods []*corev1.Pod

	// Usage is keyed "namespace/podname".
	Usage map[string]fleet.Usage

	// Queue is listener-reported queue depth per scale set name.
	Queue      map[string]int
	QueueKnown bool

	Sources []fleet.Source

	// RunnersDegraded says the Runners slice cannot be trusted to be the whole
	// truth right now: the EphemeralRunner informer has not finished its initial
	// LIST, its watch recently failed, or it is not running at all. It is
	// specifically about that one informer — NOT the arc-crds source, which is
	// an aggregate over four resources and is probed once at boot.
	//
	// The zero value means trusted, so an input assembled by hand (every test,
	// anything without a collector) behaves as if nothing were broken.
	RunnersDegraded bool

	// JobStarts remembers when each runner first reported a job. May be nil, in
	// which case job ages fall back to pod start times.
	JobStarts *JobStartTracker
}

// BuildSnapshot turns a set of Kubernetes objects into the fleet model.
//
// It is pure: same input, same output, no clients, no clocks beyond Now.
func BuildSnapshot(in SnapshotInput) fleet.Snapshot {
	now := in.Now
	if now.IsZero() {
		now = time.Now()
	}

	podsByKey := indexPods(in.RunnerPods)
	scaleSets := indexScaleSets(in.Sets)

	ersBySet := newestEphemeralSetPerOwner(in.EphemeralSets, scaleSets)
	ersOwner := ephemeralSetOwners(in.EphemeralSets, scaleSets)
	listenerBySet := listenerHealth(in.Listeners, in.ControllerPods, in.Sets)

	snap := fleet.Snapshot{
		At:                now,
		Org:               in.Org,
		ControllerVersion: in.ControllerVersion,
		Sources:           slices.Clone(in.Sources),
	}
	sortSources(snap.Sources)

	// --- runner sets -------------------------------------------------------

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
			Phase:           ars.Status.EffectivePhase(),
			NodeSelector:    ars.Spec.Template.Spec.NodeSelector,
		}

		// A nil maxRunners means no ceiling. The controller substitutes MaxInt32
		// internally, so without this flag the capacity line would read "0 of
		// 2147483647" and the saturation bar would be permanently empty.
		if max, ok := ars.Spec.MaxRunnersValue(); ok {
			set.MaxRunners = max
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

		// Counts come from the EphemeralRunnerSet, which is what the controller
		// actually scales. The AutoscalingRunnerSet's own status is a lagging
		// mirror that upstream is in the middle of deleting field by field.
		if ers := ersBySet[objectKey(ars.Namespace, ars.Name)]; ers != nil {
			set.Current = ers.Status.CurrentReplicas
			set.Pending = ers.Status.PendingEphemeralRunners
			set.Running = ers.Status.RunningEphemeralRunners
			set.Failed = ers.Status.FailedEphemeralRunners
		} else {
			set.Current = ars.Status.CurrentRunners
			set.Pending = ars.Status.PendingEphemeralRunners
			set.Running = ars.Status.RunningEphemeralRunners
			set.Failed = ars.Status.FailedEphemeralRunners
		}

		// A set absent from the queue map is unknown, not zero: the listener
		// exports a gauge per scale set, so a missing key means we have no
		// reading rather than a reading of nothing queued.
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
	sort.Slice(snap.Sets, func(i, j int) bool {
		if snap.Sets[i].Name != snap.Sets[j].Name {
			return snap.Sets[i].Name < snap.Sets[j].Name
		}
		return snap.Sets[i].Namespace < snap.Sets[j].Namespace
	})

	// --- runners -----------------------------------------------------------

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

	// Runners come and go constantly; without this the job-start map is an
	// unbounded leak that grows for the life of the process.
	//
	// Gated, because an empty runner list has two very different causes. An
	// informer that has synced and is watching cleanly reporting nothing means
	// the fleet really is idle and the entries are dead weight. A degraded one —
	// a watch error, a re-LIST in flight, an RBAC blip — reports nothing too,
	// and evicting on that would drop every tracked job start, after which job
	// ages silently fall back to pod start times and every running job looks
	// like it just restarted.
	//
	// Skipping the sweep leaks nothing durable, because the gate is a LIVE
	// signal that heals on its own: RunnersDegraded is recomputed per snapshot
	// from the informer's HasSynced and an expiring watch-failure stamp, so the
	// next good snapshot sweeps whatever accumulated meanwhile.
	if !in.RunnersDegraded {
		in.JobStarts.Retain(live)
	}

	fleet.SortRunners(snap.Runners, now)
	return snap
}

// buildRunner joins one EphemeralRunner with its pod, its scale set and the
// latest usage sample.
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
			RequestID:  er.Status.JobRequestID,
		}
	}

	// ARC records no job start time anywhere — status.jobId simply appears — so
	// the only honest source is our own first observation of it. The pod's start
	// time is the fallback for a runner that was already busy when this process
	// started; it overstates the job by however long the runner idled first, but
	// that beats reporting the job as brand new after every restart.
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
		if c, ok := runnerContainer(pod.Spec.Containers); ok {
			if r.Image == "" {
				r.Image = c.Image
			}
			applyResources(&r.CPU, &r.Mem, c.Resources)
		}
	}
	// The pod may not exist yet, or may have been trimmed; the scale set's
	// template is then the best statement of what this runner asks for.
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

	// A missing sample leaves At zero, which the model reads as "never scraped"
	// and the view renders as an em dash. Writing zeros here would understate
	// fleet usage every time a runner died before metrics-server first saw it.
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

// runnerState maps ARC's phases onto the states the dashboard shows.
//
// The first rule is the one that matters: busy is a non-empty status.jobId, not
// phase == "Running". An idle runner sitting in the pool waiting for work
// reports Running too, so using the phase makes a fleet of idle runners look
// fully utilised.
func runnerState(er *arcapi.EphemeralRunner, pod *corev1.Pod) fleet.State {
	if er.HasJob() {
		return fleet.StateBusy
	}
	if er.Status.Phase == arcapi.EphemeralRunnerPhaseFailed || podFailed(pod) {
		return fleet.StateFailed
	}
	// Terminating outranks pending because a pod being torn down goes NotReady
	// on its way out, and reporting "pending" for something that is leaving is
	// backwards.
	if er.DeletionTimestamp != nil || (pod != nil && pod.DeletionTimestamp != nil) {
		return fleet.StateTerminating
	}
	// Succeeded and Outdated runners have finished and are waiting to be
	// reaped. Their containers have exited, so they are not ready and would
	// otherwise land in pending.
	if er.Status.Phase == arcapi.EphemeralRunnerPhaseSucceeded || er.Status.Phase == arcapi.EphemeralRunnerPhaseOutdated {
		return fleet.StateTerminating
	}
	if er.Status.Phase == arcapi.EphemeralRunnerPhasePending || !runnerReady(er, pod) {
		return fleet.StatePending
	}
	return fleet.StateIdle
}

// runnerReady prefers the pod's readiness, which is what actually gates work,
// and falls back to the EphemeralRunner's own flag when no pod exists yet.
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

// waitingFailures are container waiting reasons that mean the runner is stuck.
// ContainerCreating and PodInitializing are excluded on purpose: they are the
// normal path and flagging them would fill the failure lane with healthy pods.
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

// failureReason derives the human-facing cause shown in the failure lane, and
// when it was observed.
//
// The order is by usefulness to an operator: a container that cannot start beats
// one that exited, which beats whatever the controller wrote into status, which
// beats the inference that a runner which never registered is broken.
func failureReason(er *arcapi.EphemeralRunner, pod *corev1.Pod, now time.Time) (string, time.Time) {
	if pod != nil {
		for _, cs := range slices.Concat(pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses) {
			if w := cs.State.Waiting; w != nil && waitingFailures[w.Reason] {
				return w.Reason, podFailureTime(pod)
			}
		}
		for _, cs := range pod.Status.ContainerStatuses {
			// LastTerminationState is what survives a restart, so it is where an
			// OOMKill actually shows up once the kubelet has restarted the
			// container.
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

	// Nothing is visibly broken, but a runner that has been alive for minutes
	// without a GitHub runner id never reached GitHub at all.
	if er.Status.RunnerID == 0 && !er.IsDone() && er.DeletionTimestamp == nil &&
		!er.CreationTimestamp.IsZero() && now.Sub(er.CreationTimestamp.Time) > registrationGrace {
		return "never registered", er.CreationTimestamp.Time
	}

	return "", time.Time{}
}

// terminatedReason names a container exit. A bare "Error" reason carries no
// information the exit code does not, so the code wins in that case.
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

// --- joins ------------------------------------------------------------------

// indexPods keys pods for the runner join. The pod always shares its
// EphemeralRunner's name in the same namespace, one to one, which is the whole
// join: no label matching, no owner walking.
func indexPods(pods []*corev1.Pod) map[string]*corev1.Pod {
	out := make(map[string]*corev1.Pod, len(pods))
	for _, p := range pods {
		if p != nil {
			out[objectKey(p.Namespace, p.Name)] = p
		}
	}
	return out
}

// scaleSetIndex holds the scale sets under both keys an ownerReference can be
// resolved by, and by namespace/name for the runner join.
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

// ownerOf walks an EphemeralRunnerSet's ownerReferences back to its scale set.
func (idx scaleSetIndex) ownerOf(ers *arcapi.EphemeralRunnerSet) *arcapi.AutoscalingRunnerSet {
	for _, ref := range ers.OwnerReferences {
		if ref.Kind != "AutoscalingRunnerSet" {
			continue
		}
		// UID first: names are reused when a scale set is deleted and recreated,
		// and a stale reference would silently attribute runners to the wrong
		// generation.
		if o, ok := idx.byUID[ref.UID]; ok {
			return o
		}
		if o, ok := idx.byName[objectKey(ers.Namespace, ref.Name)]; ok {
			return o
		}
	}
	return nil
}

// newestEphemeralSetPerOwner picks the EphemeralRunnerSet whose counts a scale
// set should report.
//
// During a rollout the controller runs several generations at once, draining
// the old one while the new one fills. The newest by creation timestamp is the
// one the listener is scaling, so it is the one whose numbers mean anything.
func newestEphemeralSetPerOwner(sets []*arcapi.EphemeralRunnerSet, owners scaleSetIndex) map[string]*arcapi.EphemeralRunnerSet {
	out := make(map[string]*arcapi.EphemeralRunnerSet, len(owners.byName))
	for _, ers := range sets {
		if ers == nil {
			continue
		}
		owner := owners.ownerOf(ers)
		if owner == nil {
			continue
		}
		key := objectKey(owner.Namespace, owner.Name)
		if cur := out[key]; cur == nil || newerThan(ers, cur) {
			out[key] = ers
		}
	}
	return out
}

// ephemeralSetOwners maps each EphemeralRunnerSet to the name of its scale set,
// so a runner can be attributed by walking runner → runner set → scale set.
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

// newerThan compares creation timestamps, falling back to name so a rollout
// that creates two sets within the same second still resolves deterministically.
func newerThan(a, b *arcapi.EphemeralRunnerSet) bool {
	if !a.CreationTimestamp.Time.Equal(b.CreationTimestamp.Time) {
		return a.CreationTimestamp.After(b.CreationTimestamp.Time)
	}
	return a.Name > b.Name
}

// setNameForRunner attributes a runner to its scale set, preferring the owner
// chain over the label because ARC truncates label values at 63 characters.
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

// listenerHealth reports, per scale set, whether its listener pod is up.
//
// AutoscalingListenerStatus is an empty struct upstream, so the custom resource
// says nothing about health. The listener pod is the only evidence — and it
// lives in the controller's namespace with no ownerReference back to the scale
// set, so the correlation has to go through the listener spec's explicit
// namespace and name fields.
func listenerHealth(listeners []*arcapi.AutoscalingListener, controllerPods []*corev1.Pod, sets []*arcapi.AutoscalingRunnerSet) map[string]bool {
	podsByKey := indexPods(controllerPods)
	out := make(map[string]bool, len(sets))

	for _, l := range listeners {
		if l == nil {
			continue
		}
		key := objectKey(l.Spec.AutoscalingRunnerSetNamespace, l.Spec.AutoscalingRunnerSetName)
		// The controller names the listener pod after the listener resource, in
		// the listener's own namespace.
		pod := podsByKey[objectKey(l.Namespace, l.Name)]
		out[key] = pod != nil && podReady(pod)
	}

	// Without the listener CRD (or without permission to read it) the pods'
	// scale-set labels are the remaining link.
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
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// --- small helpers ----------------------------------------------------------

func objectKey(namespace, name string) string { return namespace + "/" + name }

// cpuCores converts a CPU quantity to cores.
//
// It goes through MilliValue because Quantity.Value() ROUNDS UP: a request of
// 250m comes back as 1, which quadruples every small runner's reported request
// and makes the whole fleet look four times more expensive than it is.
func cpuCores(list corev1.ResourceList, name corev1.ResourceName) float64 {
	q, ok := list[name]
	if !ok {
		return 0
	}
	return float64(q.MilliValue()) / 1000
}

// memBytes converts a memory quantity to bytes, where Value() is exact.
func memBytes(list corev1.ResourceList, name corev1.ResourceName) float64 {
	q, ok := list[name]
	if !ok {
		return 0
	}
	return float64(q.Value())
}

// applyResources fills a runner's request/limit columns from one container.
func applyResources(cpu, mem *fleet.Resources, req corev1.ResourceRequirements) {
	cpu.Request = cpuCores(req.Requests, corev1.ResourceCPU)
	mem.Request = memBytes(req.Requests, corev1.ResourceMemory)
	cpu.Limit = cpuCores(req.Limits, corev1.ResourceCPU)
	mem.Limit = memBytes(req.Limits, corev1.ResourceMemory)
}

func runnerContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == arcapi.RunnerContainerName {
			return c, true
		}
	}
	if len(containers) > 0 {
		return containers[0], true
	}
	return corev1.Container{}, false
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

// imageTag extracts the version an image reference pins.
//
// Registries may carry a port ("registry:5000/x"), so a colon only names a tag
// when it comes after the last slash. A digest reference pins no readable
// version at all.
func imageTag(image string) string {
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	slash := strings.LastIndex(image, "/")
	colon := strings.LastIndex(image, ":")
	if colon > slash {
		return image[colon+1:]
	}
	return ""
}
