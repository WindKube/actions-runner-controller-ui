// Package k8s is the read-only Kubernetes access layer: clients, informers over
// the ARC custom resources and runner pods, and the pure builder that turns
// those caches into a fleet.Snapshot. A missing CRD or RBAC rule becomes an
// unavailable fleet.Source, never a boot failure or a page of zeros.
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

	"arc-ui/internal/config"
)

type Clients struct {
	Kube      kubernetes.Interface
	Dynamic   dynamic.Interface
	Discovery discovery.DiscoveryInterface
	Mapper    meta.RESTMapper
	Config    *rest.Config
}

// NewClients fails only when no connection can be configured at all.
func NewClients(cfg config.Config, log zerolog.Logger, userAgent string) (*Clients, error) {
	base, err := resolveRESTConfig(cfg, log)
	if err != nil {
		return nil, err
	}

	base = rest.CopyConfig(base)
	base.QPS = cfg.KubeQPS
	base.Burst = cfg.KubeBurst
	base.UserAgent = userAgent
	// Each clientset would otherwise build its own token bucket, multiplying
	// the configured QPS by the number of clientsets.
	base.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(cfg.KubeQPS, cfg.KubeBurst)

	kube, err := kubernetes.NewForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(rest.CopyConfig(base))
	if err != nil {
		return nil, fmt.Errorf("build discovery client: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(disc))

	log.Info().
		Str("host", base.Host).
		Float64("qps", float64(cfg.KubeQPS)).
		Int("burst", cfg.KubeBurst).
		Msg("kubernetes clients ready")

	return &Clients{
		Kube:      kube,
		Dynamic:   dyn,
		Discovery: disc,
		Mapper:    mapper,
		Config:    base,
	}, nil
}

// resolveRESTConfig: an explicit API URL wins, then in-cluster, then kubeconfig.
func resolveRESTConfig(cfg config.Config, log zerolog.Logger) (*rest.Config, error) {
	if cfg.KubeAPIURL != "" {
		// Bare on purpose: this is how you point at `kubectl proxy`.
		log.Info().Str("host", cfg.KubeAPIURL).Msg("using explicit KUBE_API_URL with no credentials")
		return &rest.Config{Host: cfg.KubeAPIURL}, nil
	}

	inCluster, err := rest.InClusterConfig()
	switch {
	case err == nil:
		log.Info().Msg("using in-cluster credentials")
		return inCluster, nil
	case !errors.Is(err, rest.ErrNotInCluster):
		return nil, fmt.Errorf("load in-cluster config: %w", err)
	}

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
