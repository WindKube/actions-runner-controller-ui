package v1alpha1

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "actions.github.com"
	Version = "v1alpha1"
)

var GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

var (
	AutoscalingRunnerSetGVR = GroupVersion.WithResource("autoscalingrunnersets")
	EphemeralRunnerSetGVR   = GroupVersion.WithResource("ephemeralrunnersets")
	EphemeralRunnerGVR      = GroupVersion.WithResource("ephemeralrunners")
	AutoscalingListenerGVR  = GroupVersion.WithResource("autoscalinglisteners")
)

func AllGVRs() []schema.GroupVersionResource {
	return []schema.GroupVersionResource{
		AutoscalingRunnerSetGVR,
		EphemeralRunnerSetGVR,
		EphemeralRunnerGVR,
		AutoscalingListenerGVR,
	}
}

const (
	LabelScaleSetName      = "actions.github.com/scale-set-name"
	LabelScaleSetNamespace = "actions.github.com/scale-set-namespace"
	LabelEphemeralRunner   = "actions-ephemeral-runner"
	// LabelVersion carries the gha-runner-scale-set chart version on every
	// AutoscalingRunnerSet.
	LabelVersion = "app.kubernetes.io/version"

	// AnnotationRunnerScaleSetID has no group prefix upstream; its absence means
	// the scale set has not registered with GitHub yet.
	AnnotationRunnerScaleSetID = "runner-scale-set-id"

	RunnerContainerName = "runner"
)

// OrgFromConfigURL extracts the owner from https://host/<org>[/<repo>] or
// https://host/enterprises/<enterprise>. The org labels are useless for this
// because ARC truncates them to 63 characters.
func OrgFromConfigURL(raw string) string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(raw), "/")
	if _, after, found := strings.Cut(trimmed, "://"); found {
		trimmed = after
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 {
		return ""
	}
	if parts[1] == "enterprises" && len(parts) > 2 {
		return parts[2]
	}
	return parts[1]
}

// WorkflowFileFromRef turns "owner/repo/.github/workflows/ci.yml@refs/heads/main"
// into "ci.yml".
func WorkflowFileFromRef(ref string) string {
	path, _, _ := strings.Cut(ref, "@")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		path = path[i+1:]
	}
	return path
}
