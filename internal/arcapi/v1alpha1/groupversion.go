package v1alpha1

import (
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Group and Version of the modern ARC API.
//
// Note that the legacy summerwind API is a trap: its Go directory is
// apis/actions.summerwind.net/v1alpha1 but its actual Kubernetes group is
// actions.summerwind.dev. Only the modern group matches its directory name.
const (
	Group   = "actions.github.com"
	Version = "v1alpha1"
)

// GroupVersion of every resource in this package.
var GroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// Resource plurals, all namespaced.
var (
	AutoscalingRunnerSetGVR = GroupVersion.WithResource("autoscalingrunnersets")
	EphemeralRunnerSetGVR   = GroupVersion.WithResource("ephemeralrunnersets")
	EphemeralRunnerGVR      = GroupVersion.WithResource("ephemeralrunners")
	AutoscalingListenerGVR  = GroupVersion.WithResource("autoscalinglisteners")
)

// AllGVRs is the set the dashboard watches, in dependency order.
func AllGVRs() []schema.GroupVersionResource {
	return []schema.GroupVersionResource{
		AutoscalingRunnerSetGVR,
		EphemeralRunnerSetGVR,
		EphemeralRunnerGVR,
		AutoscalingListenerGVR,
	}
}

// Labels and annotations ARC applies. Taken from the controller source at
// gha-runner-scale-set-0.14.2.
const (
	// LabelScaleSetName and LabelScaleSetNamespace appear together on every object
	// belonging to one scale set — EphemeralRunnerSet, EphemeralRunner, Pod,
	// listener, ServiceAccount and Role alike.
	LabelScaleSetName      = "actions.github.com/scale-set-name"
	LabelScaleSetNamespace = "actions.github.com/scale-set-namespace"

	// LabelEphemeralRunner is set to "True" on every runner pod, cluster-wide.
	LabelEphemeralRunner = "actions-ephemeral-runner"

	// AnnotationRunnerScaleSetID is GitHub's numeric id for the scale set.
	// Note the missing group prefix — this key does not follow the
	// actions.github.com/ convention and could in principle collide. Its
	// absence means the scale set has not registered with GitHub yet.
	AnnotationRunnerScaleSetID = "runner-scale-set-id"

	// RunnerContainerName is the container inside a runner pod that carries
	// the image and resource requests worth showing.
	RunnerContainerName = "runner"
)

// OrgFromConfigURL extracts the GitHub owner from a githubConfigUrl.
//
// The URL is one of https://github.com/<org>, .../<org>/<repo>, or
// .../enterprises/<enterprise>. The org labels can't be used for this because
// ARC trims them to 63 characters.
func OrgFromConfigURL(raw string) string {
	trimmed := strings.TrimSuffix(strings.TrimSpace(raw), "/")
	if _, after, found := strings.Cut(trimmed, "://"); found {
		trimmed = after
	}
	// parts[0] is the host, so the path segments start at parts[1].
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 {
		return ""
	}
	if parts[1] == "enterprises" && len(parts) > 2 {
		return parts[2]
	}
	return parts[1]
}

// WorkflowFileFromRef extracts the workflow filename from a jobWorkflowRef,
// which looks like "owner/repo/.github/workflows/ci.yml@refs/heads/main".
func WorkflowFileFromRef(ref string) string {
	path, _, _ := strings.Cut(ref, "@")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		path = path[i+1:]
	}
	return path
}
