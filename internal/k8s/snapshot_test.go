package k8s

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"
)

var testNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func ownerRef(kind, name string, uid types.UID) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: arcapi.GroupVersion.String(),
		Kind:       kind,
		Name:       name,
		UID:        uid,
	}
}

func intPtr(v int) *int { return &v }

func readyPod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			StartTime:  &metav1.Time{Time: testNow.Add(-20 * time.Minute)},
		},
	}
}

// TestRunnerStateBusyIsJobIDNotPhase pins the single most consequential
// mapping in the package: an idle runner also reports phase Running.
func TestRunnerStateBusyIsJobIDNotPhase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		er   *arcapi.EphemeralRunner
		pod  *corev1.Pod
		want fleet.State
	}{
		{
			name: "running phase with a job is busy",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, Ready: true, JobID: "job-7"},
			},
			pod:  readyPod("arc", "r1"),
			want: fleet.StateBusy,
		},
		{
			name: "running phase without a job is idle, not busy",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, Ready: true},
			},
			pod:  readyPod("arc", "r1"),
			want: fleet.StateIdle,
		},
		{
			name: "a job still counts as busy while the pod is unready",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, JobID: "job-7"},
			},
			pod:  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"}},
			want: fleet.StateBusy,
		},
		{
			name: "failed phase is failed",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseFailed},
			},
			want: fleet.StateFailed,
		},
		{
			name: "a failed pod fails the runner even when the phase lags",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     corev1.PodStatus{Phase: corev1.PodFailed},
			},
			want: fleet.StateFailed,
		},
		{
			name: "pending phase is pending",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhasePending},
			},
			want: fleet.StatePending,
		},
		{
			name: "no pod yet is pending",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning},
			},
			want: fleet.StatePending,
		},
		{
			name: "deletion beats the unready pod it causes",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "arc",
					Name:              "r1",
					DeletionTimestamp: &metav1.Time{Time: testNow},
				},
				Status: arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning},
			},
			pod:  &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"}},
			want: fleet.StateTerminating,
		},
		{
			name: "succeeded runners are terminating, not pending",
			er: &arcapi.EphemeralRunner{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseSucceeded},
			},
			want: fleet.StateTerminating,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, runnerState(tc.er, tc.pod))
		})
	}
}

// TestCPUQuantityIsNotRounded guards the trap that makes every small runner
// look four times more expensive than it is.
func TestCPUQuantityIsNotRounded(t *testing.T) {
	t.Parallel()

	q := resource.MustParse("250m")

	// Documenting the trap: this is what the obvious call would have returned.
	require.Equal(t, int64(1), q.Value(),
		"precondition changed: Quantity.Value() on 250m no longer rounds to the historical 1")

	list := corev1.ResourceList{corev1.ResourceCPU: q}
	require.InDelta(t, 0.25, cpuCores(list, corev1.ResourceCPU), 1e-9,
		"want 0.25 (Value() rounds 250m up to a whole core)")

	mem := corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")}
	require.InDelta(t, 2*1024*1024*1024, memBytes(mem, corev1.ResourceMemory), 1e-9, "want 2Gi in bytes")

	require.Zero(t, cpuCores(corev1.ResourceList{}, corev1.ResourceCPU), "cpuCores on an absent quantity, want 0")
}

func TestMaxRunnersUnbounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		max           *int
		wantMax       int
		wantUnbounded bool
	}{
		{name: "nil means no ceiling", max: nil, wantMax: 0, wantUnbounded: true},
		{name: "the controller's MaxInt32 substitution also means no ceiling", max: intPtr(arcapi.UnboundedRunners), wantMax: 0, wantUnbounded: true},
		{name: "a real ceiling is reported", max: intPtr(40), wantMax: 40, wantUnbounded: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			snap := BuildSnapshot(SnapshotInput{
				Now: testNow,
				Sets: []*arcapi.AutoscalingRunnerSet{{
					ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu"},
					Spec:       arcapi.AutoscalingRunnerSetSpec{MaxRunners: tc.max, MinRunners: intPtr(2)},
				}},
			})
			require.Len(t, snap.Sets, 1)
			set := snap.Sets[0]
			assert.Equal(t, tc.wantUnbounded, set.Unbounded, "Unbounded")
			assert.Equal(t, tc.wantMax, set.MaxRunners, "MaxRunners")
			assert.Equal(t, 2, set.MinRunners, "MinRunners")
		})
	}
}

// TestCountsComeFromNewestEphemeralRunnerSet covers a rollout, where several
// generations exist at once and only the newest is being scaled.
func TestCountsComeFromNewestEphemeralRunnerSet(t *testing.T) {
	t.Parallel()

	ars := &arcapi.AutoscalingRunnerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu", UID: "ars-uid"},
		Status: arcapi.AutoscalingRunnerSetStatus{
			CurrentRunners:          99, // the lagging mirror we must NOT use
			RunningEphemeralRunners: 99,
		},
	}
	old := &arcapi.EphemeralRunnerSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "arc",
			Name:              "ubuntu-old",
			CreationTimestamp: metav1.Time{Time: testNow.Add(-2 * time.Hour)},
			OwnerReferences:   []metav1.OwnerReference{ownerRef("AutoscalingRunnerSet", "ubuntu", "ars-uid")},
		},
		Status: arcapi.EphemeralRunnerSetStatus{CurrentReplicas: 2, RunningEphemeralRunners: 2},
	}
	current := &arcapi.EphemeralRunnerSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "arc",
			Name:              "ubuntu-new",
			CreationTimestamp: metav1.Time{Time: testNow.Add(-5 * time.Minute)},
			OwnerReferences:   []metav1.OwnerReference{ownerRef("AutoscalingRunnerSet", "ubuntu", "ars-uid")},
		},
		Status: arcapi.EphemeralRunnerSetStatus{
			CurrentReplicas:         7,
			PendingEphemeralRunners: 3,
			RunningEphemeralRunners: 4,
			FailedEphemeralRunners:  1,
		},
	}
	// Belongs to a different scale set entirely and must be ignored.
	foreign := &arcapi.EphemeralRunnerSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "arc",
			Name:              "windows-1",
			CreationTimestamp: metav1.Time{Time: testNow},
			OwnerReferences:   []metav1.OwnerReference{ownerRef("AutoscalingRunnerSet", "windows", "other-uid")},
		},
		Status: arcapi.EphemeralRunnerSetStatus{CurrentReplicas: 500},
	}

	snap := BuildSnapshot(SnapshotInput{
		Now:           testNow,
		Sets:          []*arcapi.AutoscalingRunnerSet{ars},
		EphemeralSets: []*arcapi.EphemeralRunnerSet{old, current, foreign},
	})

	set := snap.Sets[0]
	require.Equal(t, 7, set.Current, "counts = %+v, want the newest EphemeralRunnerSet's 7/3/4/1", set)
	require.Equal(t, 3, set.Pending, "counts = %+v, want the newest EphemeralRunnerSet's 7/3/4/1", set)
	require.Equal(t, 4, set.Running, "counts = %+v, want the newest EphemeralRunnerSet's 7/3/4/1", set)
	require.Equal(t, 1, set.Failed, "counts = %+v, want the newest EphemeralRunnerSet's 7/3/4/1", set)
}

func TestCountsFallBackToScaleSetStatusWithoutAnEphemeralRunnerSet(t *testing.T) {
	t.Parallel()

	snap := BuildSnapshot(SnapshotInput{
		Now: testNow,
		Sets: []*arcapi.AutoscalingRunnerSet{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu"},
			Status:     arcapi.AutoscalingRunnerSetStatus{CurrentRunners: 3, RunningEphemeralRunners: 3},
		}},
	})
	require.Equal(t, 3, snap.Sets[0].Current, "want the scale set's own 3 when no runner set is visible")
}

// TestRunnerJoinsPodByName covers the 1:1 name join and its namespace scoping.
func TestRunnerJoinsPodByName(t *testing.T) {
	t.Parallel()

	er := &arcapi.EphemeralRunner{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu-abc", UID: "er-1"},
		Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, RunnerID: 12},
	}
	pod := readyPod("arc", "ubuntu-abc")
	pod.Spec.NodeName = "node-a"
	pod.Spec.Containers = []corev1.Container{{
		Name:  arcapi.RunnerContainerName,
		Image: "ghcr.io/actions/runner:2.320.0",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		},
	}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         arcapi.RunnerContainerName,
		Image:        "ghcr.io/actions/runner:2.320.0",
		RestartCount: 2,
	}}
	// Same name, wrong namespace: must not join.
	decoy := readyPod("other", "ubuntu-abc")
	decoy.Spec.NodeName = "wrong-node"

	snap := BuildSnapshot(SnapshotInput{
		Now:        testNow,
		Runners:    []*arcapi.EphemeralRunner{er},
		RunnerPods: []*corev1.Pod{decoy, pod},
		Usage:      map[string]fleet.Usage{"arc/ubuntu-abc": {CPUCores: 1.5, MemBytes: 1 << 30, At: testNow}},
	})

	r := snap.Runners[0]
	assert.Equal(t, "node-a", r.Node, "Node")
	assert.Equal(t, int32(2), r.Restarts, "Restarts")
	assert.Equal(t, "ghcr.io/actions/runner:2.320.0", r.Image, "Image")
	assert.InDelta(t, 0.5, r.CPU.Request, 1e-9, "CPU.Request")
	assert.InDelta(t, 1.5, r.CPU.Used, 1e-9, "usage not joined: %+v", r.CPU)
	assert.True(t, r.CPU.HasUsage(), "usage not joined: %+v", r.CPU)
	assert.Equal(t, 12, r.RunnerID, "RunnerID")
}

func TestRunnerWithoutUsageSampleHasNoTimestamp(t *testing.T) {
	t.Parallel()

	snap := BuildSnapshot(SnapshotInput{
		Now: testNow,
		Runners: []*arcapi.EphemeralRunner{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1", UID: "u1"},
			Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, RunnerID: 1},
		}},
		RunnerPods: []*corev1.Pod{readyPod("arc", "r1")},
	})

	// A never-scraped runner must be distinguishable from one using nothing,
	// or the fleet's usage total silently understates itself.
	require.False(t, snap.Runners[0].CPU.HasUsage(), "CPU.HasUsage() is true for a runner metrics-server never saw")
}

func TestJobFieldsAndAttribution(t *testing.T) {
	t.Parallel()

	arsObj := &arcapi.AutoscalingRunnerSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu", UID: "ars-1"},
		Spec:       arcapi.AutoscalingRunnerSetSpec{GitHubConfigURL: "https://github.com/acme-corp"},
	}
	ers := &arcapi.EphemeralRunnerSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "arc",
			Name:            "ubuntu-xyz",
			OwnerReferences: []metav1.OwnerReference{ownerRef("AutoscalingRunnerSet", "ubuntu", "ars-1")},
		},
	}
	er := &arcapi.EphemeralRunner{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "arc",
			Name:            "ubuntu-xyz-runner-1",
			UID:             "er-1",
			OwnerReferences: []metav1.OwnerReference{ownerRef("EphemeralRunnerSet", "ubuntu-xyz", "ers-1")},
		},
		Status: arcapi.EphemeralRunnerStatus{
			Phase:             arcapi.EphemeralRunnerPhaseRunning,
			JobID:             "job-1",
			JobRepositoryName: "acme-corp/api",
			JobWorkflowRef:    "acme-corp/api/.github/workflows/ci.yml@refs/heads/main",
			JobDisplayName:    "build (ubuntu-latest)",
			WorkflowRunID:     998877,
			JobRequestID:      42,
			RunnerID:          5,
		},
	}

	snap := BuildSnapshot(SnapshotInput{
		Now:           testNow,
		Sets:          []*arcapi.AutoscalingRunnerSet{arsObj},
		EphemeralSets: []*arcapi.EphemeralRunnerSet{ers},
		Runners:       []*arcapi.EphemeralRunner{er},
		RunnerPods:    []*corev1.Pod{readyPod("arc", "ubuntu-xyz-runner-1")},
	})

	assert.Equal(t, "acme-corp", snap.Org, "Org should be derived from githubConfigUrl")
	r := snap.Runners[0]
	assert.Equal(t, "ubuntu", r.SetName, "SetName should come via the owner chain")
	assert.Equal(t, "ci.yml", r.Job.Workflow, "Job.Workflow")
	assert.Equal(t, "acme-corp/api", r.Job.Repository, "job fields = %+v", r.Job)
	assert.Equal(t, "build (ubuntu-latest)", r.Job.Name, "job fields = %+v", r.Job)
	assert.Equal(t, int64(998877), r.Job.RunID, "job fields = %+v", r.Job)
	assert.Equal(t, int64(42), r.Job.RequestID, "job fields = %+v", r.Job)
	assert.Equal(t, fleet.StateBusy, r.State, "State")
}

func TestConfiguredOrgOverridesConfigURL(t *testing.T) {
	t.Parallel()

	snap := BuildSnapshot(SnapshotInput{
		Now: testNow,
		Org: "override",
		Sets: []*arcapi.AutoscalingRunnerSet{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu"},
			Spec:       arcapi.AutoscalingRunnerSetSpec{GitHubConfigURL: "https://github.com/acme-corp"},
		}},
	})
	require.Equal(t, "override", snap.Org, "want the configured override")
}

func TestFailureReason(t *testing.T) {
	t.Parallel()

	base := func() *arcapi.EphemeralRunner {
		return &arcapi.EphemeralRunner{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "arc",
				Name:              "r1",
				CreationTimestamp: metav1.Time{Time: testNow.Add(-time.Minute)},
			},
			Status: arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, RunnerID: 3},
		}
	}

	tests := []struct {
		name string
		er   func() *arcapi.EphemeralRunner
		pod  *corev1.Pod
		want string
	}{
		{
			name: "healthy runner has no reason",
			er:   base,
			pod:  readyPod("arc", "r1"),
			want: "",
		},
		{
			name: "image pull failure",
			er:   base,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name:  arcapi.RunnerContainerName,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
				}}},
			},
			want: "ImagePullBackOff",
		},
		{
			name: "normal container creation is not a failure",
			er:   base,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name:  arcapi.RunnerContainerName,
					State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
				}}},
			},
			want: "",
		},
		{
			name: "OOMKilled is read off the previous termination",
			er:   base,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name: arcapi.RunnerContainerName,
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason:     "OOMKilled",
						ExitCode:   137,
						FinishedAt: metav1.Time{Time: testNow.Add(-30 * time.Second)},
					}},
				}}},
			},
			want: "OOMKilled",
		},
		{
			name: "a bare Error reason yields the exit code instead",
			er:   base,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name: arcapi.RunnerContainerName,
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason:   "Error",
						ExitCode: 1,
					}},
				}}},
			},
			want: "exit code 1",
		},
		{
			name: "a clean exit is not a failure",
			er:   base,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1"},
				Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
					Name: arcapi.RunnerContainerName,
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason:   "Completed",
						ExitCode: 0,
					}},
				}}},
			},
			want: "",
		},
		{
			name: "the controller's own reason and message are joined",
			er: func() *arcapi.EphemeralRunner {
				er := base()
				er.Status.Reason = "TooManyPodFailures"
				er.Status.Message = "pod has failed to start more than 5 times"
				return er
			},
			pod:  nil,
			want: "TooManyPodFailures: pod has failed to start more than 5 times",
		},
		{
			name: "a runner past the grace period with no runner id never registered",
			er: func() *arcapi.EphemeralRunner {
				er := base()
				er.Status.RunnerID = 0
				er.CreationTimestamp = metav1.Time{Time: testNow.Add(-registrationGrace - time.Minute)}
				return er
			},
			pod:  readyPod("arc", "r1"),
			want: "never registered",
		},
		{
			name: "a young runner with no runner id is still starting up",
			er: func() *arcapi.EphemeralRunner {
				er := base()
				er.Status.RunnerID = 0
				er.CreationTimestamp = metav1.Time{Time: testNow.Add(-10 * time.Second)}
				return er
			},
			pod:  readyPod("arc", "r1"),
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := failureReason(tc.er(), tc.pod, testNow)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestListenerHealthCorrelatesAcrossNamespaces(t *testing.T) {
	t.Parallel()

	sets := []*arcapi.AutoscalingRunnerSet{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "arc-runners", Name: "ubuntu"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "arc-runners", Name: "windows"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "arc-runners", Name: "macos"}},
	}
	listeners := []*arcapi.AutoscalingListener{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc-systems", Name: "ubuntu-listener"},
			Spec: arcapi.AutoscalingListenerSpec{
				AutoscalingRunnerSetNamespace: "arc-runners",
				AutoscalingRunnerSetName:      "ubuntu",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc-systems", Name: "windows-listener"},
			Spec: arcapi.AutoscalingListenerSpec{
				AutoscalingRunnerSetNamespace: "arc-runners",
				AutoscalingRunnerSetName:      "windows",
			},
		},
	}
	// Only the ubuntu listener's pod is up; the windows listener has no pod.
	ctrlPods := []*corev1.Pod{readyPod("arc-systems", "ubuntu-listener")}

	snap := BuildSnapshot(SnapshotInput{
		Now:            testNow,
		Sets:           sets,
		Listeners:      listeners,
		ControllerPods: ctrlPods,
	})

	byName := map[string]fleet.RunnerSet{}
	for _, s := range snap.Sets {
		byName[s.Name] = s
	}
	assert.True(t, byName["ubuntu"].ListenerKnown, "ubuntu listener = %+v, want known and healthy", byName["ubuntu"])
	assert.True(t, byName["ubuntu"].ListenerHealthy, "ubuntu listener = %+v, want known and healthy", byName["ubuntu"])
	assert.True(t, byName["windows"].ListenerKnown, "windows listener = %+v, want known and unhealthy", byName["windows"])
	assert.False(t, byName["windows"].ListenerHealthy, "windows listener = %+v, want known and unhealthy", byName["windows"])
	assert.False(t, byName["macos"].ListenerKnown, "macos listener should be unknown, got %+v", byName["macos"])
	assert.Equal(t, 1, snap.ListenersReady, "listeners = %d/%d, want 1/2", snap.ListenersReady, snap.ListenersTotal)
	assert.Equal(t, 2, snap.ListenersTotal, "listeners = %d/%d, want 1/2", snap.ListenersReady, snap.ListenersTotal)
}

func TestListenerHealthFallsBackToPodLabels(t *testing.T) {
	t.Parallel()

	// No AutoscalingListener resources at all — CRD missing or unreadable.
	pod := readyPod("arc-systems", "ubuntu-listener")
	pod.Labels = map[string]string{
		arcapi.LabelScaleSetName:      "ubuntu",
		arcapi.LabelScaleSetNamespace: "arc-runners",
	}

	snap := BuildSnapshot(SnapshotInput{
		Now:            testNow,
		Sets:           []*arcapi.AutoscalingRunnerSet{{ObjectMeta: metav1.ObjectMeta{Namespace: "arc-runners", Name: "ubuntu"}}},
		ControllerPods: []*corev1.Pod{pod},
	})

	require.True(t, snap.Sets[0].ListenerKnown, "listener = %+v, want known and healthy from pod labels", snap.Sets[0])
	require.True(t, snap.Sets[0].ListenerHealthy, "listener = %+v, want known and healthy from pod labels", snap.Sets[0])
}

func TestQueueDepthUnknownWhenUnreported(t *testing.T) {
	t.Parallel()

	sets := []*arcapi.AutoscalingRunnerSet{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "windows"}},
	}

	t.Run("reported sets carry a value, unreported ones stay unknown", func(t *testing.T) {
		t.Parallel()
		snap := BuildSnapshot(SnapshotInput{
			Now:        testNow,
			Sets:       sets,
			Queue:      map[string]int{"ubuntu": 0},
			QueueKnown: true,
		})
		byName := map[string]fleet.RunnerSet{}
		for _, s := range snap.Sets {
			byName[s.Name] = s
		}
		assert.True(t, byName["ubuntu"].QueuedKnown, "ubuntu = %+v, want a known depth of 0", byName["ubuntu"])
		assert.Zero(t, byName["ubuntu"].Queued, "ubuntu = %+v, want a known depth of 0", byName["ubuntu"])
		assert.False(t, byName["windows"].QueuedKnown, "windows queue should be unknown, got %+v", byName["windows"])
	})

	t.Run("nothing is known when the scraper is not reporting", func(t *testing.T) {
		t.Parallel()
		snap := BuildSnapshot(SnapshotInput{
			Now:        testNow,
			Sets:       sets,
			Queue:      map[string]int{"ubuntu": 4},
			QueueKnown: false,
		})
		for _, s := range snap.Sets {
			assert.False(t, s.QueuedKnown, "%s reported a known queue depth with QueueKnown=false", s.Name)
		}
	})
}

func TestJobStartTrackedAndEvicted(t *testing.T) {
	t.Parallel()

	tracker := NewJobStartTracker()
	er := &arcapi.EphemeralRunner{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1", UID: "er-1"},
		Status: arcapi.EphemeralRunnerStatus{
			Phase: arcapi.EphemeralRunnerPhaseRunning,
			JobID: "job-1",
		},
	}
	pod := readyPod("arc", "r1")

	first := BuildSnapshot(SnapshotInput{
		Now:        testNow,
		Runners:    []*arcapi.EphemeralRunner{er},
		RunnerPods: []*corev1.Pod{pod},
		JobStarts:  tracker,
	})
	require.WithinDuration(t, testNow, first.Runners[0].Job.StartedAt, 0, "StartedAt should be the first observation")

	// Five minutes later the same job must still report the original start,
	// or the runner detail page resets its timer on every refresh.
	later := BuildSnapshot(SnapshotInput{
		Now:        testNow.Add(5 * time.Minute),
		Runners:    []*arcapi.EphemeralRunner{er},
		RunnerPods: []*corev1.Pod{pod},
		JobStarts:  tracker,
	})
	require.WithinDuration(t, testNow, later.Runners[0].Job.StartedAt, 0, "StartedAt should be the sticky original")
	require.Equal(t, 5*time.Minute, later.Runners[0].JobAge(testNow.Add(5*time.Minute)), "JobAge")

	// The runner disappears: its entry must go with it or the map leaks for the
	// life of the process.
	BuildSnapshot(SnapshotInput{Now: testNow.Add(10 * time.Minute), JobStarts: tracker})
	require.Zero(t, tracker.Len(), "tracker retained entries after the runner vanished")
}

// TestJobStartsSurviveADegradedRunnerInformer pins the difference between
// "no runners exist" and "we cannot currently see the runners". A watch error,
// a re-LIST or an RBAC blip empties the EphemeralRunner input, and evicting on
// that resets every job's age to its pod start time.
func TestJobStartsSurviveADegradedRunnerInformer(t *testing.T) {
	t.Parallel()

	tracker := NewJobStartTracker()
	er := &arcapi.EphemeralRunner{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1", UID: "er-1"},
		Status: arcapi.EphemeralRunnerStatus{
			Phase: arcapi.EphemeralRunnerPhaseRunning,
			JobID: "job-1",
		},
	}
	// A second runner, busy now and finished by the time the watch recovers.
	finished := &arcapi.EphemeralRunner{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r2", UID: "er-2"},
		Status: arcapi.EphemeralRunnerStatus{
			Phase: arcapi.EphemeralRunnerPhaseRunning,
			JobID: "job-2",
		},
	}
	pod := readyPod("arc", "r1")
	pod2 := readyPod("arc", "r2")

	BuildSnapshot(SnapshotInput{
		Now:        testNow,
		Runners:    []*arcapi.EphemeralRunner{er, finished},
		RunnerPods: []*corev1.Pod{pod, pod2},
		JobStarts:  tracker,
	})
	require.Equal(t, 2, tracker.Len(), "the busy runners were not tracked")

	// The watch breaks: the informer reports nothing at all.
	BuildSnapshot(SnapshotInput{
		Now:             testNow.Add(time.Minute),
		RunnerPods:      []*corev1.Pod{pod, pod2},
		RunnersDegraded: true,
		JobStarts:       tracker,
	})
	require.Equal(t, 2, tracker.Len(), "a degraded runner informer evicted every tracked job start")

	// The watch recovers: r1 is still on the same job, r2 finished and was
	// deleted while we could not see it. Two things have to hold at once — r1
	// still reports its original start rather than its pod's, which would show a
	// long job as freshly restarted, and the sweep that was skipped while
	// degraded resumes now that the list can be trusted again.
	back := BuildSnapshot(SnapshotInput{
		Now:        testNow.Add(2 * time.Minute),
		Runners:    []*arcapi.EphemeralRunner{er},
		RunnerPods: []*corev1.Pod{pod},
		JobStarts:  tracker,
	})
	require.WithinDuration(t, testNow, back.Runners[0].Job.StartedAt, 0,
		"StartedAt is not the original first sighting after the informer blipped")
	require.Equal(t, 1, tracker.Len(),
		"the sweep did not resume once the informer was trusted again")

	// A trusted informer reporting no runners is genuine: the entry has to go,
	// or the map leaks for the life of the process.
	BuildSnapshot(SnapshotInput{
		Now:       testNow.Add(3 * time.Minute),
		JobStarts: tracker,
	})
	require.Zero(t, tracker.Len(), "tracker retained entries after a synced informer reported no runners")
}

// TestJobStartEvictionIgnoresTheAggregateARCVerdict pins that the sweep is
// gated on the EphemeralRunner informer alone, not on the arc-crds source.
//
// arc-crds is an aggregate: probeARCCRDs reports it unavailable when ANY of the
// four ARC resources is absent or denied, while still handing back the others
// as usable and running their informers. It is also probed exactly once, at
// boot, with nothing to re-probe it — so one missing autoscalinglisteners RBAC
// rule would pin the tracker for the life of the process while the
// EphemeralRunner informer works perfectly.
func TestJobStartEvictionIgnoresTheAggregateARCVerdict(t *testing.T) {
	t.Parallel()

	tracker := NewJobStartTracker()
	er := &arcapi.EphemeralRunner{
		ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1", UID: "er-1"},
		Status: arcapi.EphemeralRunnerStatus{
			Phase: arcapi.EphemeralRunnerPhaseRunning,
			JobID: "job-1",
		},
	}
	// One denied resource out of four: the ephemeralrunners informer is fine.
	partial := []fleet.Source{{
		Name:      fleet.SourceARCCRDs,
		Available: false,
		Reason:    "missing RBAC: list autoscalinglisteners.actions.github.com in arc-systems",
		CheckedAt: testNow,
	}}

	BuildSnapshot(SnapshotInput{
		Now:        testNow,
		Runners:    []*arcapi.EphemeralRunner{er},
		RunnerPods: []*corev1.Pod{readyPod("arc", "r1")},
		Sources:    partial,
		JobStarts:  tracker,
	})
	require.Equal(t, 1, tracker.Len(), "the busy runner was not tracked")

	// The runner is genuinely gone and the informer that reports runners never
	// broke, so the entry has to go with it.
	BuildSnapshot(SnapshotInput{
		Now:       testNow.Add(time.Minute),
		Sources:   partial,
		JobStarts: tracker,
	})
	require.Zero(t, tracker.Len(),
		"an unrelated denied ARC resource stopped the sweep; the tracker grows for the life of the process")
}

func TestJobStartFallsBackToPodStartTime(t *testing.T) {
	t.Parallel()

	// No tracker: this is the "arc-ui restarted while the job was running" case.
	podStart := testNow.Add(-12 * time.Minute)
	pod := readyPod("arc", "r1")
	pod.Status.StartTime = &metav1.Time{Time: podStart}

	snap := BuildSnapshot(SnapshotInput{
		Now: testNow,
		Runners: []*arcapi.EphemeralRunner{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "r1", UID: "er-1"},
			Status:     arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhaseRunning, JobID: "job-1"},
		}},
		RunnerPods: []*corev1.Pod{pod},
	})

	require.WithinDuration(t, podStart, snap.Runners[0].Job.StartedAt, 0, "StartedAt should be the pod start")
}

func TestJobStartTrackerForgetsFinishedRunners(t *testing.T) {
	t.Parallel()

	tracker := NewJobStartTracker()
	require.WithinDuration(t, testNow, tracker.Observe("uid-1", true, testNow), 0, "first Observe")
	require.WithinDuration(t, testNow, tracker.Observe("uid-1", true, testNow.Add(time.Minute)), 0,
		"second Observe should be the sticky first one")
	got := tracker.Observe("uid-1", false, testNow.Add(2*time.Minute))
	require.True(t, got.IsZero(), "Observe with no job = %v, want zero", got)
	require.Zero(t, tracker.Len(), "tracker kept entries for a runner with no job")
	// A nil tracker must be usable, so BuildSnapshot works untracked.
	var nilTracker *JobStartTracker
	got = nilTracker.Observe("uid-2", true, testNow)
	require.True(t, got.IsZero(), "nil tracker Observe = %v, want zero", got)
	nilTracker.Retain(nil)
}

func TestImageTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		image string
		want  string
	}{
		{name: "plain tag", image: "ghcr.io/actions/gha-runner-scale-set-controller:0.14.2", want: "0.14.2"},
		{name: "registry port is not a tag", image: "registry:5000/actions/controller", want: ""},
		{name: "registry port with tag", image: "registry:5000/actions/controller:1.2.3", want: "1.2.3"},
		{name: "digest pins no readable version", image: "ghcr.io/actions/controller@sha256:abcdef", want: ""},
		{name: "no tag at all", image: "controller", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, imageTag(tc.image), "imageTag(%q)", tc.image)
		})
	}
}

func TestRunnerResourcesFallBackToTheScaleSetTemplate(t *testing.T) {
	t.Parallel()

	// A runner whose pod does not exist yet still has to show what it will ask
	// for, or the capacity view undercounts every pending runner.
	snap := BuildSnapshot(SnapshotInput{
		Now: testNow,
		Sets: []*arcapi.AutoscalingRunnerSet{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "arc", Name: "ubuntu", UID: "ars-1"},
			Spec: arcapi.AutoscalingRunnerSetSpec{
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  arcapi.RunnerContainerName,
					Image: "ghcr.io/actions/runner:latest",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("250m"),
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				}}}},
			},
		}},
		EphemeralSets: []*arcapi.EphemeralRunnerSet{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       "arc",
				Name:            "ubuntu-1",
				OwnerReferences: []metav1.OwnerReference{ownerRef("AutoscalingRunnerSet", "ubuntu", "ars-1")},
			},
		}},
		Runners: []*arcapi.EphemeralRunner{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       "arc",
				Name:            "ubuntu-1-runner-a",
				UID:             "er-1",
				OwnerReferences: []metav1.OwnerReference{ownerRef("EphemeralRunnerSet", "ubuntu-1", "ers-1")},
			},
			Status: arcapi.EphemeralRunnerStatus{Phase: arcapi.EphemeralRunnerPhasePending},
		}},
	})

	set := snap.Sets[0]
	require.InDelta(t, 0.25, set.CPURequest, 1e-9,
		"set requests = %v cores / %v bytes, want 0.25 / 1Gi", set.CPURequest, set.MemRequest)
	require.InDelta(t, 1<<30, set.MemRequest, 1e-9,
		"set requests = %v cores / %v bytes, want 0.25 / 1Gi", set.CPURequest, set.MemRequest)
	r := snap.Runners[0]
	assert.InDelta(t, 0.25, r.CPU.Request, 1e-9, "runner CPU.Request should be the template's 0.25")
	assert.Equal(t, "ghcr.io/actions/runner:latest", r.Image, "runner Image should be the template's")
	assert.Equal(t, fleet.StatePending, r.State, "State")
}

func TestSetNameFallsBackToLabel(t *testing.T) {
	t.Parallel()

	// No owner chain visible (the EphemeralRunnerSet informer is degraded), so
	// the label is all we have.
	snap := BuildSnapshot(SnapshotInput{
		Now: testNow,
		Runners: []*arcapi.EphemeralRunner{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "arc",
				Name:      "r1",
				UID:       "er-1",
				Labels:    map[string]string{arcapi.LabelScaleSetName: "ubuntu"},
			},
		}},
	})
	require.Equal(t, "ubuntu", snap.Runners[0].SetName, "SetName should come from the label")
}
