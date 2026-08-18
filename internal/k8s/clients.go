// Package k8s is the dashboard's read-only Kubernetes access layer: one bundle
// of clients derived from a single rest.Config, a set of informers over the ARC
// custom resources and runner pods, and the builder that turns those caches
// into a fleet.Snapshot.
//
// Two ideas shape everything here.
//
// First, nothing is mandatory. A cluster with no metrics-server, no ARC CRDs
// installed and RBAC that grants us half of what we ask for must still let the
// process boot and serve. Every such gap becomes a fleet.Source marked
// unavailable, never an error at startup and never a page of plausible zeros.
//
// Second, the interesting logic must be testable without a cluster. The ARC
// semantics that are easy to get subtly wrong — busy is a job id and not a pod
// phase, counts live on the EphemeralRunnerSet, a nil maxRunners is unbounded —
// all live in BuildSnapshot, a pure function over typed objects.
package k8s

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"

	"arc-ui/internal/config"
)

// Clients bundles everything derived from one *rest.Config.
//
// All four clientsets share a single rate limiter — see NewClients for why that
// is not the default and why it matters.
type Clients struct {
	Kube      kubernetes.Interface
	Dynamic   dynamic.Interface
	Metrics   metricsclient.Interface
	Discovery discovery.DiscoveryInterface
	Mapper    meta.RESTMapper
	Config    *rest.Config
}

// NewClients resolves a cluster connection and builds every client from it.
//
// It returns an error only when no connection can be established at all.
// Whether any particular API is actually present or permitted is a question for
// the source probes, not for construction.
func NewClients(cfg config.Config, log zerolog.Logger, userAgent string) (*Clients, error) {
	base, err := resolveRESTConfig(cfg, log)
	if err != nil {
		return nil, err
	}

	// QPS, Burst and UserAgent have to be set BEFORE any client is built: the
	// throttle and the user-agent string are baked into the transport at
	// construction time, so mutating the config afterwards changes nothing.
	base = rest.CopyConfig(base)
	base.QPS = cfg.KubeQPS
	base.Burst = cfg.KubeBurst
	base.UserAgent = userAgent

	// Every clientset built from a config with a nil RateLimiter constructs its
	// OWN token bucket from QPS/Burst. Four clientsets at QPS=50 therefore mean
	// 200 QPS against the API server, which is exactly the kind of surprise that
	// gets a read-only dashboard blamed for a control-plane outage. Handing them
	// one shared limiter makes the configured budget the real budget.
	base.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(cfg.KubeQPS, cfg.KubeBurst)

	kube, err := kubernetes.NewForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	mc, err := metricsclient.NewForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build metrics client: %w", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build discovery client: %w", err)
	}

	// A deferred mapper backed by a memory cache so the CRD presence check costs
	// one discovery round trip at boot rather than one per lookup. It caches
	// negative results too, which is why the check calls Reset first.
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disc))

	log.Info().
		Str("host", base.Host).
		Float64("qps", float64(cfg.KubeQPS)).
		Int("burst", cfg.KubeBurst).
		Msg("kubernetes clients ready")

	return &Clients{
		Kube:      kube,
		Dynamic:   dyn,
		Metrics:   mc,
		Discovery: disc,
		Mapper:    mapper,
		Config:    base,
	}, nil
}

// resolveRESTConfig picks a cluster connection: an explicit API URL wins, then
// in-cluster credentials, then a kubeconfig.
func resolveRESTConfig(cfg config.Config, log zerolog.Logger) (*rest.Config, error) {
	if cfg.KubeAPIURL != "" {
		// Deliberately bare: an explicit URL is how you point the dashboard at
		// `kubectl proxy` or an envtest apiserver, neither of which wants the
		// ambient credentials. Anything needing auth should use a kubeconfig.
		log.Info().Str("host", cfg.KubeAPIURL).Msg("using explicit KUBE_API_URL with no credentials")
		return &rest.Config{Host: cfg.KubeAPIURL}, nil
	}

	inCluster, err := rest.InClusterConfig()
	switch {
	case err == nil:
		log.Info().Msg("using in-cluster credentials")
		return inCluster, nil
	case !errors.Is(err, rest.ErrNotInCluster):
		// A broken service account is a real failure. Only "we are not running
		// in a pod" is a reason to fall through to the kubeconfig.
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}

	// NewNonInteractiveDeferredLoadingClientConfig, not BuildConfigFromFlags:
	// only the former can select a context by name, which is the whole point of
	// ARC_UI_KUBE_CONTEXT.
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = cfg.Kubeconfig
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cfg.KubeContext}

	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	log.Info().Str("context", cfg.KubeContext).Msg("using kubeconfig credentials")
	return rc, nil
}
