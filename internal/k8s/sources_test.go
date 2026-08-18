package k8s

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"
)

// stubMapper answers only KindFor; the probe uses nothing else, and embedding
// the interface makes any accidental extra call an obvious panic.
type stubMapper struct {
	meta.RESTMapper
	known map[schema.GroupVersionResource]bool
}

func (m stubMapper) KindFor(gvr schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	if m.known[gvr] {
		return gvr.GroupVersion().WithKind("Stub"), nil
	}
	return schema.GroupVersionKind{}, &meta.NoResourceMatchError{PartialResource: gvr}
}

// stubDiscovery answers only what the probes ask for.
type stubDiscovery struct {
	discovery.DiscoveryInterface
	versionErr   error
	resourcesErr error
}

func (d stubDiscovery) ServerVersion() (*version.Info, error) {
	if d.versionErr != nil {
		return nil, d.versionErr
	}
	return &version.Info{GitVersion: "v1.33.0"}, nil
}

func (d stubDiscovery) ServerResourcesForGroupVersion(string) (*metav1.APIResourceList, error) {
	if d.resourcesErr != nil {
		return nil, d.resourcesErr
	}
	return &metav1.APIResourceList{}, nil
}

// allowingClientset answers SelfSubjectAccessReviews with allow, except for the
// (resource, verb) pairs listed in deny.
func allowingClientset(deny map[string]string) *fake.Clientset {
	kube := fake.NewClientset()
	kube.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review, ok := action.(k8stesting.CreateAction).GetObject().(*authzv1.SelfSubjectAccessReview)
		if !ok {
			return false, nil, nil
		}
		attrs := review.Spec.ResourceAttributes
		review.Status.Allowed = deny[attrs.Resource] != attrs.Verb
		return true, review, nil
	})
	return kube
}

func allGVRsKnown() map[schema.GroupVersionResource]bool {
	known := map[schema.GroupVersionResource]bool{}
	for _, gvr := range arcapi.AllGVRs() {
		known[gvr] = true
	}
	return known
}

func TestProbeARCCRDs(t *testing.T) {
	t.Parallel()

	scope := namespaceScope{watch: []string{"arc-runners"}, controller: "arc-systems"}

	t.Run("all installed and permitted", func(t *testing.T) {
		t.Parallel()
		src, usable := probeARCCRDs(context.Background(), allowingClientset(nil), stubMapper{known: allGVRsKnown()}, scope, testNow)
		require.True(t, src.Available, "source = %+v, want available", src)
		require.Len(t, usable, len(arcapi.AllGVRs()), "want all GVRs usable")
	})

	t.Run("CRDs absent gates every informer", func(t *testing.T) {
		t.Parallel()
		src, usable := probeARCCRDs(context.Background(), allowingClientset(nil), stubMapper{}, scope, testNow)
		require.False(t, src.Available, "source = %+v, want unavailable", src)
		assert.Contains(t, src.Reason, "not installed", "reason should say the CRDs are not installed")
		// This is the whole point: an informer created here would retry a 404
		// forever and hang WaitForCacheSync.
		require.Empty(t, usable, "want no usable resources")
	})

	t.Run("a denial on one resource does not disable its prefix sibling", func(t *testing.T) {
		t.Parallel()
		// "ephemeralrunners" is a prefix of "ephemeralrunnersets": matching
		// resource names by substring would take out both.
		kube := allowingClientset(map[string]string{"ephemeralrunnersets": "watch"})
		src, usable := probeARCCRDs(context.Background(), kube, stubMapper{known: allGVRsKnown()}, scope, testNow)

		require.False(t, src.Available, "source = %+v, want unavailable", src)
		assert.False(t, usable[arcapi.EphemeralRunnerSetGVR],
			"ephemeralrunnersets stayed usable despite a denied watch")
		assert.True(t, usable[arcapi.EphemeralRunnerGVR],
			"ephemeralrunners was disabled by its prefix sibling's denial")
		assert.True(t, usable[arcapi.AutoscalingRunnerSetGVR], "unrelated resource was disabled")
		assert.True(t, usable[arcapi.AutoscalingListenerGVR], "unrelated resource was disabled")
	})
}

func TestProbeMetrics(t *testing.T) {
	t.Parallel()

	scope := namespaceScope{watch: []string{"arc-runners"}, controller: "arc-systems"}

	t.Run("present and permitted", func(t *testing.T) {
		t.Parallel()
		src := probeMetrics(context.Background(), allowingClientset(nil), stubDiscovery{}, scope, testNow)
		assert.True(t, src.Available, "source = %+v, want available", src)
	})

	t.Run("not installed", func(t *testing.T) {
		t.Parallel()
		notFound := apierrors.NewNotFound(schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, "")
		src := probeMetrics(context.Background(), allowingClientset(nil), stubDiscovery{resourcesErr: notFound}, scope, testNow)
		require.False(t, src.Available, "source = %+v, want unavailable", src)
		assert.Contains(t, src.Reason, "not installed")
	})

	t.Run("installed but not permitted", func(t *testing.T) {
		t.Parallel()
		kube := allowingClientset(map[string]string{"pods": "list"})
		src := probeMetrics(context.Background(), kube, stubDiscovery{}, scope, testNow)
		require.False(t, src.Available, "source = %+v, want unavailable", src)
		assert.Contains(t, src.Reason, "missing RBAC", "want an RBAC explanation")
	})
}

func TestProbeKubernetesReportsUnreachableDistinctly(t *testing.T) {
	t.Parallel()

	scope := namespaceScope{watch: []string{"arc-runners"}, controller: "arc-systems"}

	reachable := probeKubernetes(context.Background(), allowingClientset(nil), stubDiscovery{}, scope, testNow)
	require.True(t, reachable.Available, "source = %+v, want available", reachable)

	// Denied RBAC must NOT read as unreachable: one is a degraded dashboard,
	// the other aborts boot.
	denied := probeKubernetes(context.Background(), allowingClientset(map[string]string{"pods": "watch"}), stubDiscovery{}, scope, testNow)
	require.False(t, denied.Available, "source = %+v, want unavailable", denied)
	require.False(t, isUnreachable(denied.Reason),
		"an RBAC denial was classified as unreachable: %q", denied.Reason)

	down := probeKubernetes(context.Background(), allowingClientset(nil), stubDiscovery{versionErr: context.DeadlineExceeded}, scope, testNow)
	require.True(t, isUnreachable(down.Reason),
		"a dead API server was not classified as unreachable: %q", down.Reason)
}

// TestSelfSubjectAccessReviewErrorsAreNotDenials guards the failure mode where
// a hiccup in the review API would blank the dashboard for a reason that has
// nothing to do with RBAC.
func TestSelfSubjectAccessReviewErrorsAreNotDenials(t *testing.T) {
	t.Parallel()

	kube := fake.NewClientset()
	kube.PrependReactor("create", "selfsubjectaccessreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("try again")
	})

	src := probeKubernetes(context.Background(), kube, stubDiscovery{}, namespaceScope{watch: []string{"arc"}, controller: "arc-systems"}, testNow)
	require.True(t, src.Available, "source = %+v, want available: a failed review is not a denial", src)
	assert.Equal(t, fleet.SourceKubernetes, src.Name)
}
