// Package v1alpha1 is a hand-vendored subset of the Actions Runner Controller
// types for group actions.github.com, with json tags copied from the release
// pinned in arc-version.
//
// ARC's Go module is not imported: its releases are tagged
// gha-runner-scale-set-X.Y.Z, so `go get` resolves to v0.27.6 from 2023 and pulls
// in the Azure SDK. Every status field is optional and no runner counts are read
// from status: 0.15.0 removed them (actions/actions-runner-controller#4557).
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type AutoscalingRunnerSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AutoscalingRunnerSetSpec   `json:"spec,omitempty"`
	Status AutoscalingRunnerSetStatus `json:"status,omitempty"`
}

type AutoscalingRunnerSetSpec struct {
	GitHubConfigURL      string   `json:"githubConfigUrl,omitempty"`
	GitHubConfigSecret   string   `json:"githubConfigSecret,omitempty"`
	RunnerGroup          string   `json:"runnerGroup,omitempty"`
	RunnerScaleSetName   string   `json:"runnerScaleSetName,omitempty"`
	RunnerScaleSetLabels []string `json:"runnerScaleSetLabels,omitempty"`

	Template corev1.PodTemplateSpec `json:"template,omitempty"`

	// nil means unbounded; the controller hands the listener MaxInt32 in that case.
	MaxRunners *int `json:"maxRunners,omitempty"`
	MinRunners *int `json:"minRunners,omitempty"`
}

type AutoscalingRunnerSetStatus struct {
	Phase string `json:"phase,omitempty"`
}

const (
	AutoscalingRunnerSetPhasePending  = "Pending"
	AutoscalingRunnerSetPhaseRunning  = "Running"
	AutoscalingRunnerSetPhaseOutdated = "Outdated"
)

// MaxRunnersValue reports the configured ceiling; ok is false for an unbounded set.
func (s AutoscalingRunnerSetSpec) MaxRunnersValue() (value int, ok bool) {
	if s.MaxRunners == nil || *s.MaxRunners >= UnboundedRunners {
		return 0, false
	}
	return *s.MaxRunners, true
}

func (s AutoscalingRunnerSetSpec) MinRunnersValue() int {
	if s.MinRunners == nil {
		return 0
	}
	return *s.MinRunners
}

const UnboundedRunners = 2147483647

func (s AutoscalingRunnerSetSpec) RunnerContainer() (corev1.Container, bool) {
	return RunnerContainer(s.Template.Spec.Containers)
}

// RunnerContainer picks the container named "runner", or the first one for sets
// that rename it.
func RunnerContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == RunnerContainerName {
			return c, true
		}
	}
	if len(containers) > 0 {
		return containers[0], true
	}
	return corev1.Container{}, false
}

// ContainerMode infers the scale set chart's containerMode from the template it
// rendered, since the mode itself is not part of the API.
func (s AutoscalingRunnerSetSpec) ContainerMode() string {
	spec := s.Template.Spec
	for _, c := range append(spec.InitContainers, spec.Containers...) {
		if c.Name == "dind" || c.Name == "init-dind-externals" {
			return "dind"
		}
		for _, e := range c.Env {
			if e.Name == "ACTIONS_RUNNER_CONTAINER_HOOKS" {
				return "kubernetes"
			}
		}
	}
	return "default"
}

type EphemeralRunnerSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EphemeralRunnerSetSpec   `json:"spec,omitempty"`
	Status EphemeralRunnerSetStatus `json:"status,omitempty"`
}

type EphemeralRunnerSetSpec struct {
	Replicas int `json:"replicas,omitempty"`
	PatchID  int `json:"patchID,omitempty"`
}

type EphemeralRunnerSetStatus struct {
	Phase string `json:"phase,omitempty"`
}

type EphemeralRunner struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EphemeralRunnerSpec   `json:"spec,omitempty"`
	Status EphemeralRunnerStatus `json:"status,omitempty"`
}

type EphemeralRunnerSpec struct {
	GitHubConfigURL  string `json:"githubConfigUrl,omitempty"`
	RunnerScaleSetID int    `json:"runnerScaleSetId,omitempty"`

	corev1.PodTemplateSpec `json:",inline"`
}

type EphemeralRunnerStatus struct {
	Ready   bool   `json:"ready,omitempty"`
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

// EphemeralRunner phases describe the pod, not the job: an idle runner is Running.
const (
	EphemeralRunnerPhasePending   = "Pending"
	EphemeralRunnerPhaseRunning   = "Running"
	EphemeralRunnerPhaseSucceeded = "Succeeded"
	EphemeralRunnerPhaseFailed    = "Failed"
	EphemeralRunnerPhaseOutdated  = "Outdated"
)

// HasJob is the only correct busy test; Phase == Running is true of idle runners too.
func (r *EphemeralRunner) HasJob() bool { return r.Status.JobID != "" }

func (r *EphemeralRunner) IsDone() bool {
	switch r.Status.Phase {
	case EphemeralRunnerPhaseSucceeded, EphemeralRunnerPhaseFailed, EphemeralRunnerPhaseOutdated:
		return true
	}
	return false
}

func (s EphemeralRunnerStatus) LastFailure() metav1.Time {
	var latest metav1.Time
	for _, ts := range s.Failures {
		if ts.After(latest.Time) {
			latest = ts
		}
	}
	return latest
}

// AutoscalingListener lives in the controller namespace with no ownerReference to
// its scale set; correlate through the spec's namespace and name. Its status is
// an empty struct upstream.
type AutoscalingListener struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec AutoscalingListenerSpec `json:"spec,omitempty"`
}

type AutoscalingListenerSpec struct {
	// Phase appeared in 0.15.0 and is empty on 0.14.x.
	Phase                         string `json:"phase,omitempty"`
	GitHubConfigURL               string `json:"githubConfigUrl,omitempty"`
	RunnerScaleSetID              int    `json:"runnerScaleSetId,omitempty"`
	AutoscalingRunnerSetNamespace string `json:"autoscalingRunnerSetNamespace,omitempty"`
	AutoscalingRunnerSetName      string `json:"autoscalingRunnerSetName,omitempty"`
	EphemeralRunnerSetName        string `json:"ephemeralRunnerSetName,omitempty"`
	MaxRunners                    int    `json:"maxRunners,omitempty"`
	MinRunners                    int    `json:"minRunners,omitempty"`
	Image                         string `json:"image,omitempty"`
}

const (
	AutoscalingListenerPhaseRunning = "Running"
	AutoscalingListenerPhaseStopped = "Stopped"
)
