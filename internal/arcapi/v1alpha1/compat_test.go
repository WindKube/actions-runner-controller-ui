package v1alpha1

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// fixtureVersions are the ARC releases the decode tests run against: the pin and
// the newest release of the previous minor.
var fixtureVersions = []string{"0.15.1", "0.14.2"}

func loadFixtures(t *testing.T, version string) map[string]*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", version+".yaml"))
	require.NoError(t, err)

	out := map[string]*unstructured.Unstructured{}
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var obj map[string]any
		require.NoError(t, yaml.Unmarshal([]byte(doc), &obj))
		u := &unstructured.Unstructured{Object: obj}
		out[u.GetKind()] = u
	}
	return out
}

func decode[T any](t *testing.T, u *unstructured.Unstructured) T {
	t.Helper()
	require.NotNil(t, u)
	var out T
	require.NoError(t, runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &out))
	return out
}

func TestFixturesDecode(t *testing.T) {
	t.Parallel()
	for _, v := range fixtureVersions {
		t.Run(v, func(t *testing.T) {
			t.Parallel()
			objs := loadFixtures(t, v)

			ars := decode[AutoscalingRunnerSet](t, objs["AutoscalingRunnerSet"])
			assert.Equal(t, v, ars.Labels[LabelVersion])
			assert.Equal(t, "https://github.com/WindKube", ars.Spec.GitHubConfigURL)
			assert.Equal(t, "Running", ars.Status.Phase)
			if major, minor, _ := majorMinor(v); fmt.Sprintf("%d.%d", major, minor) == SupportedMinors()[0] {
				assert.True(t, Supported(ars.Labels[LabelVersion]), "the pinned minor's fixture must be supported")
			}
			maxRunners, ok := ars.Spec.MaxRunnersValue()
			assert.True(t, ok)
			assert.Equal(t, 10, maxRunners)
			assert.Equal(t, "dind", ars.Spec.ContainerMode())

			ers := decode[EphemeralRunnerSet](t, objs["EphemeralRunnerSet"])
			assert.Equal(t, 2, ers.Spec.Replicas)

			er := decode[EphemeralRunner](t, objs["EphemeralRunner"])
			assert.True(t, er.HasJob())
			assert.Equal(t, "WindKube/web-api", er.Status.JobRepositoryName)
			assert.Equal(t, int64(9876543210), er.Status.WorkflowRunID)
			assert.Equal(t, "ci.yml", WorkflowFileFromRef(er.Status.JobWorkflowRef))
			assert.Equal(t, "runner", er.Spec.Spec.Containers[0].Name)

			l := decode[AutoscalingListener](t, objs["AutoscalingListener"])
			assert.Equal(t, "arc-runners", l.Spec.AutoscalingRunnerSetNamespace)
			assert.Equal(t, "linux-x64", l.Spec.AutoscalingRunnerSetName)
		})
	}
}

func TestListenerPhaseOnlyExistsFrom015(t *testing.T) {
	t.Parallel()
	l15 := decode[AutoscalingListener](t, loadFixtures(t, "0.15.1")["AutoscalingListener"])
	l14 := decode[AutoscalingListener](t, loadFixtures(t, "0.14.2")["AutoscalingListener"])
	assert.Equal(t, AutoscalingListenerPhaseRunning, l15.Spec.Phase)
	assert.Empty(t, l14.Spec.Phase)
}

// TestTypesMatchCRDSchema checks every json field we decode against the CRDs of
// the pinned release. CI downloads them into ARC_CRD_DIR (the arc-compat job);
// without it the test is skipped.
func TestTypesMatchCRDSchema(t *testing.T) {
	dir := os.Getenv("ARC_CRD_DIR")
	if dir == "" {
		t.Skip("ARC_CRD_DIR not set; the arc-compat CI job provides it")
	}

	kinds := map[string]any{
		"actions.github.com_autoscalingrunnersets.yaml": AutoscalingRunnerSet{},
		"actions.github.com_ephemeralrunnersets.yaml":   EphemeralRunnerSet{},
		"actions.github.com_ephemeralrunners.yaml":      EphemeralRunner{},
		"actions.github.com_autoscalinglisteners.yaml":  AutoscalingListener{},
	}
	for file, typ := range kinds {
		t.Run(file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(dir, file))
			require.NoError(t, err)
			var crd struct {
				Spec struct {
					Versions []struct {
						Name   string `json:"name"`
						Schema struct {
							OpenAPIV3Schema map[string]any `json:"openAPIV3Schema"`
						} `json:"schema"`
					} `json:"versions"`
				} `json:"spec"`
			}
			require.NoError(t, yaml.Unmarshal(raw, &crd))

			var schema map[string]any
			for _, v := range crd.Spec.Versions {
				if v.Name == Version {
					schema = v.Schema.OpenAPIV3Schema
				}
			}
			require.NotNil(t, schema, "no %s schema in %s", Version, file)

			for _, missing := range missingFields(reflect.TypeOf(typ), schema, "") {
				t.Errorf("%s: field %s is not in the CRD schema", file, missing)
			}
		})
	}
}

const ownPkg = "arc-ui/internal/arcapi/v1alpha1"

// missingFields walks our struct and reports json paths absent from the schema.
// Foreign types (corev1, metav1) are checked by name only; their shape is
// Kubernetes', not ARC's.
func missingFields(t reflect.Type, schema map[string]any, prefix string) []string {
	props, _ := schema["properties"].(map[string]any)
	var missing []string
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" || !f.IsExported() {
			continue
		}
		if name == "" && strings.Contains(opts, "inline") {
			missing = append(missing, missingFields(f.Type, schema, prefix)...)
			continue
		}
		path := prefix + "." + name
		sub, ok := props[name].(map[string]any)
		if !ok {
			missing = append(missing, path)
			continue
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Map {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct && ft.PkgPath() == ownPkg {
			missing = append(missing, missingFields(ft, sub, path)...)
		}
	}
	return missing
}

func TestContainerModeTellsTheKubernetesHooksApart(t *testing.T) {
	t.Parallel()
	mode := func(hook string) string {
		var s AutoscalingRunnerSetSpec
		s.Template.Spec.Containers = []corev1.Container{{
			Name: RunnerContainerName,
			Env:  []corev1.EnvVar{{Name: "ACTIONS_RUNNER_CONTAINER_HOOKS", Value: hook}},
		}}
		return s.ContainerMode()
	}
	assert.Equal(t, "kubernetes", mode("/home/runner/k8s/index.js"))
	assert.Equal(t, "kubernetes-novolume", mode("/home/runner/k8s-novolume/index.js"))
	assert.Equal(t, "default", AutoscalingRunnerSetSpec{}.ContainerMode())
}

// Everything here is derived from the pin, so bumping arc-version never needs
// a test edit.
func TestSupportedMinorsFollowThePin(t *testing.T) {
	t.Parallel()
	major, minor, ok := majorMinor(PinnedVersion())
	require.True(t, ok, "arc-version %q must be a version", PinnedVersion())
	require.Positive(t, minor, "the previous-minor rule needs a minor above zero")

	assert.Equal(t, []string{fmt.Sprintf("%d.%d", major, minor), fmt.Sprintf("%d.%d", major, minor-1)}, SupportedMinors())

	for v, want := range map[string]bool{
		PinnedVersion():                         true,
		fmt.Sprintf("%d.%d.0", major, minor):    true,
		fmt.Sprintf("v%d.%d.7", major, minor-1): true,
		fmt.Sprintf("%d.%d.0", major, minor+1):  false,
		fmt.Sprintf("%d.%d.0", major+1, minor):  false,
		"":                                      false, "latest": false,
	} {
		assert.Equal(t, want, Supported(v), v)
	}
	if minor >= 2 {
		assert.False(t, Supported(fmt.Sprintf("%d.%d.0", major, minor-2)))
	}
}
