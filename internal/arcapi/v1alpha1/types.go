// Package v1alpha1 holds a minimal, hand-vendored copy of the Actions Runner
// Controller custom resource types for group actions.github.com.
//
// These are deliberately NOT imported from
// github.com/actions/actions-runner-controller. That module is effectively
// unversioned for Go consumers: every release since October 2023 is tagged
// `gha-runner-scale-set-X.Y.Z`, which the module proxy cannot see, so a plain
// `go get` silently resolves to v0.27.6 and drags in a k8s.io generation from
// 2023. Worse, importing the real API package compiles the entire Azure Key
// Vault SDK, because VaultConfig.Type is typed `vault.VaultType`. The payoff
// for all that would be nil — the repo ships no generated clientset, only
// controller-gen deepcopy.
//
// So we copy the ~20 structs we actually render, with json tags taken verbatim
// from release tag gha-runner-scale-set-0.14.2 (commit 9bb16ae). Decoding via
// runtime.DefaultUnstructuredConverter ignores fields we did not declare, so
// ARC can grow its API without breaking us.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ---------------------------------------------------------------------------
// AutoscalingRunnerSet — the user-facing scale set. One per runner pool.
// ---------------------------------------------------------------------------

// AutoscalingRunnerSet is the top-level scale set resource.
type AutoscalingRunnerSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AutoscalingRunnerSetSpec   `json:"spec,omitempty"`
	Status AutoscalingRunnerSetStatus `json:"status,omitempty"`
}

// AutoscalingRunnerSetSpec is the declared shape of a scale set.
type AutoscalingRunnerSetSpec struct {
	GitHubConfigURL      string   `json:"githubConfigUrl,omitempty"`
	GitHubConfigSecret   string   `json:"githubConfigSecret,omitempty"`
	RunnerGroup          string   `json:"runnerGroup,omitempty"`
	RunnerScaleSetName   string   `json:"runnerScaleSetName,omitempty"`
	RunnerScaleSetLabels []string `json:"runnerScaleSetLabels,omitempty"`

	// Template carries the runner pod, and with it the image, resource
	// requests and limits, and node selector the dashboard displays.
	Template corev1.PodTemplateSpec `json:"template,omitempty"`

	// MaxRunners is a pointer because nil means *unbounded*, not zero. The
	// controller resolves nil to math.MaxInt32 when creating the listener, so
	// an unbounded set reports a max of 2147483647 — render "unlimited"
	// instead of a capacity line.
	MaxRunners *int `json:"maxRunners,omitempty"`
	MinRunners *int `json:"minRunners,omitempty"`
}

// AutoscalingRunnerSetStatus mirrors counts up from the EphemeralRunnerSet.
//
// Treat these as a display hint only and prefer the EphemeralRunnerSet's own
// status: upstream is actively trimming this struct down to Phase alone, and
// the CRD's printer columns already reference fields that no longer exist.
type AutoscalingRunnerSetStatus struct {
	CurrentRunners          int    `json:"currentRunners"`
	Phase                   string `json:"phase"`
	PendingEphemeralRunners int    `json:"pendingEphemeralRunners"`
	RunningEphemeralRunners int    `json:"runningEphemeralRunners"`
	FailedEphemeralRunners  int    `json:"failedEphemeralRunners"`

	// State is the pre-0.14.0 spelling of Phase. Declared so a dashboard
	// pointed at an older controller still shows something.
	State string `json:"state,omitempty"`
}

// AutoscalingRunnerSetPhase values.
const (
	AutoscalingRunnerSetPhasePending  = "Pending"
	AutoscalingRunnerSetPhaseRunning  = "Running"
	AutoscalingRunnerSetPhaseOutdated = "Outdated"
)

// EffectivePhase prefers the modern Phase field and falls back to the legacy
// State spelling used before gha-runner-scale-set 0.14.0.
func (s AutoscalingRunnerSetStatus) EffectivePhase() string {
	if s.Phase != "" {
		return s.Phase
	}
	return s.State
}

// MaxRunnersValue reports the configured ceiling. ok is false when the set is
// unbounded, either because maxRunners is unset or because the controller has
// already expanded nil to MaxInt32.
func (s AutoscalingRunnerSetSpec) MaxRunnersValue() (value int, ok bool) {
	if s.MaxRunners == nil || *s.MaxRunners >= UnboundedRunners {
		return 0, false
	}
	return *s.MaxRunners, true
}

// MinRunnersValue reports the configured floor, defaulting to zero.
func (s AutoscalingRunnerSetSpec) MinRunnersValue() int {
	if s.MinRunners == nil {
		return 0
	}
	return *s.MinRunners
}

// UnboundedRunners is what the controller substitutes for a nil maxRunners
// when it creates the AutoscalingListener (math.MaxInt32).
const UnboundedRunners = 2147483647

// RunnerContainer returns the runner container from the pod template, which is
// where image and resource requests live.
func (s AutoscalingRunnerSetSpec) RunnerContainer() (corev1.Container, bool) {
	for _, c := range s.Template.Spec.Containers {
		if c.Name == RunnerContainerName {
			return c, true
		}
	}
	// Some scale sets name the container differently; the first one is a
	// better guess than nothing.
	if len(s.Template.Spec.Containers) > 0 {
		return s.Template.Spec.Containers[0], true
	}
	return corev1.Container{}, false
}

// ---------------------------------------------------------------------------
// EphemeralRunnerSet — the generation actually being scaled. Owns the runners.
// ---------------------------------------------------------------------------

// EphemeralRunnerSet is one generation of a scale set's runners. A rollout can
// leave more than one alive at a time.
type EphemeralRunnerSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EphemeralRunnerSetSpec   `json:"spec,omitempty"`
	Status EphemeralRunnerSetStatus `json:"status,omitempty"`
}

// EphemeralRunnerSetSpec is what the listener asked the controller for.
type EphemeralRunnerSetSpec struct {
	Replicas int `json:"replicas,omitempty"`
	// PatchID uses a capital ID in its json tag, unlike every other field in
	// this API.
	PatchID int `json:"patchID"`
}

// EphemeralRunnerSetStatus is the authoritative source of runner counts.
//
// CurrentReplicas is pending+running only: finished, deleting and outdated
// runners are tracked internally but never published, so this can read lower
// than the number of EphemeralRunner objects that exist.
type EphemeralRunnerSetStatus struct {
	CurrentReplicas         int    `json:"currentReplicas"`
	PendingEphemeralRunners int    `json:"pendingEphemeralRunners"`
	RunningEphemeralRunners int    `json:"runningEphemeralRunners"`
	FailedEphemeralRunners  int    `json:"failedEphemeralRunners"`
	Phase                   string `json:"phase"`
}

// ---------------------------------------------------------------------------
// EphemeralRunner — one runner, one pod, one job.
// ---------------------------------------------------------------------------

// EphemeralRunner is a single runner. Its pod always shares its name.
type EphemeralRunner struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EphemeralRunnerSpec   `json:"spec,omitempty"`
	Status EphemeralRunnerStatus `json:"status,omitempty"`
}

// EphemeralRunnerSpec embeds a pod template inline, so the pod spec is reached
// through Spec.Spec.
type EphemeralRunnerSpec struct {
	GitHubConfigURL  string `json:"githubConfigUrl,omitempty"`
	RunnerScaleSetID int    `json:"runnerScaleSetId,omitempty"`

	corev1.PodTemplateSpec `json:",inline"`
}

// EphemeralRunnerStatus carries both runner lifecycle and current job.
type EphemeralRunnerStatus struct {
	Ready   bool   `json:"ready"`
	Phase   string `json:"phase,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`

	RunnerID   int    `json:"runnerId,omitempty"`
	RunnerName string `json:"runnerName,omitempty"`

	Failures map[string]metav1.Time `json:"failures,omitempty"`

	JobRequestID      int64  `json:"jobRequestId,omitempty"`
	JobID             string `json:"jobId,omitempty"`
	JobRepositoryName string `json:"jobRepositoryName,omitempty"`
	JobWorkflowRef    string `json:"jobWorkflowRef,omitempty"`
	WorkflowRunID     int64  `json:"workflowRunId,omitempty"`
	JobDisplayName    string `json:"jobDisplayName,omitempty"`
}

// EphemeralRunnerPhase values. The phase is a superset of PodPhase, so it
// describes the pod, not the job.
const (
	EphemeralRunnerPhasePending   = "Pending"
	EphemeralRunnerPhaseRunning   = "Running"
	EphemeralRunnerPhaseSucceeded = "Succeeded"
	EphemeralRunnerPhaseFailed    = "Failed"
	EphemeralRunnerPhaseOutdated  = "Outdated"
)

// HasJob reports whether a job is currently executing.
//
// This is the only correct busy test. Phase == "Running" means the *pod* is
// running, which is equally true of an idle runner waiting for work.
func (r *EphemeralRunner) HasJob() bool { return r.Status.JobID != "" }

// IsDone reports whether the runner has reached a terminal phase.
func (r *EphemeralRunner) IsDone() bool {
	switch r.Status.Phase {
	case EphemeralRunnerPhaseSucceeded, EphemeralRunnerPhaseFailed, EphemeralRunnerPhaseOutdated:
		return true
	}
	return false
}

// LastFailure returns the most recent recorded failure time, or the zero time.
func (s EphemeralRunnerStatus) LastFailure() metav1.Time {
	var latest metav1.Time
	for _, ts := range s.Failures {
		if ts.After(latest.Time) {
			latest = ts
		}
	}
	return latest
}

// ---------------------------------------------------------------------------
// AutoscalingListener — the GitHub-side poller. Health lives on its pod.
// ---------------------------------------------------------------------------

// AutoscalingListener polls GitHub for job assignments.
//
// It lives in the *controller's* namespace, not the runner namespace, and has
// no ownerReference back to its AutoscalingRunnerSet — correlate through
// Spec.AutoscalingRunnerSetNamespace and Spec.AutoscalingRunnerSetName.
type AutoscalingListener struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AutoscalingListenerSpec `json:"spec,omitempty"`
	// Status is deliberately omitted: AutoscalingListenerStatus is an empty
	// struct upstream, so listener health must come from the listener pod.
}

// AutoscalingListenerSpec ties a listener back to its scale set.
type AutoscalingListenerSpec struct {
	GitHubConfigURL               string `json:"githubConfigUrl,omitempty"`
	RunnerScaleSetID              int    `json:"runnerScaleSetId,omitempty"`
	AutoscalingRunnerSetNamespace string `json:"autoscalingRunnerSetNamespace,omitempty"`
	AutoscalingRunnerSetName      string `json:"autoscalingRunnerSetName,omitempty"`
	EphemeralRunnerSetName        string `json:"ephemeralRunnerSetName,omitempty"`
	MaxRunners                    int    `json:"maxRunners,omitempty"`
	MinRunners                    int    `json:"minRunners,omitempty"`
	Image                         string `json:"image,omitempty"`
}
