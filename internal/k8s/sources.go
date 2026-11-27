package k8s

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	authzv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"
)

// unreachablePrefix marks the one fatal source failure: an API server we cannot
// talk to at all.
const unreachablePrefix = "api server unreachable"

// probeKubernetes checks the API server answers and that we may list and watch
// pods; an informer needs both verbs.
func probeKubernetes(ctx context.Context, kube kubernetes.Interface, disc discovery.DiscoveryInterface, cfg namespaceScope, now time.Time) fleet.Source {
	if _, err := disc.ServerVersion(); err != nil {
		return unavailable(fleet.SourceKubernetes, fmt.Sprintf("%s: %v", unreachablePrefix, err), now)
	}

	checks := make([]accessCheck, 0, 2*len(cfg.watch)+2)
	for _, ns := range append(slices.Clone(cfg.watch), cfg.controller) {
		checks = append(checks,
			accessCheck{namespace: ns, resource: "pods", verb: "list"},
			accessCheck{namespace: ns, resource: "pods", verb: "watch"},
		)
	}

	if denied := deniedChecks(ctx, kube, checks); len(denied) > 0 {
		return unavailable(fleet.SourceKubernetes, "missing RBAC: "+strings.Join(denied, ", "), now)
	}
	return available(fleet.SourceKubernetes, now)
}

// probeARCCRDs reports whether the ARC CRDs are installed and readable, and
// which ones the informers may start on. It must run first: an informer on a
// missing CRD spins on a 404 forever.
func probeARCCRDs(ctx context.Context, kube kubernetes.Interface, mapper meta.RESTMapper, cfg namespaceScope, now time.Time) (fleet.Source, map[schema.GroupVersionResource]bool) {
	usable := make(map[schema.GroupVersionResource]bool, len(arcapi.AllGVRs()))

	// The deferred mapper caches negative results.
	if r, ok := mapper.(interface{ Reset() }); ok {
		r.Reset()
	}

	var notInstalled, unreadable []string
	for _, gvr := range arcapi.AllGVRs() {
		if _, err := mapper.KindFor(gvr); err != nil {
			if meta.IsNoMatchError(err) {
				notInstalled = append(notInstalled, gvr.Resource)
			} else {
				unreadable = append(unreadable, fmt.Sprintf("%s (%v)", gvr.Resource, err))
			}
			continue
		}
		usable[gvr] = true
	}

	if len(usable) == 0 {
		if len(unreadable) > 0 {
			return unavailable(fleet.SourceARCCRDs, "discovery failed: "+strings.Join(unreadable, ", "), now), usable
		}
		return unavailable(fleet.SourceARCCRDs, "actions.github.com CRDs are not installed", now), usable
	}

	// Per resource, so one denial cannot blank the others.
	var denied []string
	for gvr := range usable {
		namespaces := cfg.watch
		if gvr == arcapi.AutoscalingListenerGVR {
			namespaces = []string{cfg.controller}
		}
		checks := make([]accessCheck, 0, 2*len(namespaces))
		for _, ns := range namespaces {
			checks = append(checks,
				accessCheck{namespace: ns, group: gvr.Group, resource: gvr.Resource, verb: "list"},
				accessCheck{namespace: ns, group: gvr.Group, resource: gvr.Resource, verb: "watch"},
			)
		}
		if bad := deniedChecks(ctx, kube, checks); len(bad) > 0 {
			delete(usable, gvr)
			denied = append(denied, bad...)
		}
	}

	if len(denied) > 0 {
		slices.Sort(denied)
		return unavailable(fleet.SourceARCCRDs, "missing RBAC: "+strings.Join(denied, ", "), now), usable
	}
	missing := slices.Concat(notInstalled, unreadable)
	if len(missing) > 0 {
		slices.Sort(missing)
		return unavailable(fleet.SourceARCCRDs, "unavailable: "+strings.Join(missing, ", "), now), usable
	}
	return available(fleet.SourceARCCRDs, now), usable
}

type accessCheck struct {
	namespace string
	group     string
	resource  string
	verb      string
}

func (a accessCheck) String() string {
	res := a.resource
	if a.group != "" {
		res += "." + a.group
	}
	ns := a.namespace
	if ns == "" {
		ns = "*"
	}
	return fmt.Sprintf("%s %s in %s", a.verb, res, ns)
}

// deniedChecks names the denied checks. A review that fails to run is not a
// denial: SelfSubjectAccessReview needs no permissions, so an error means the
// API server is unhappy, not that RBAC said no.
func deniedChecks(ctx context.Context, kube kubernetes.Interface, checks []accessCheck) []string {
	var denied []string
	for _, chk := range checks {
		review := &authzv1.SelfSubjectAccessReview{
			Spec: authzv1.SelfSubjectAccessReviewSpec{
				ResourceAttributes: &authzv1.ResourceAttributes{
					Namespace: chk.namespace,
					Group:     chk.group,
					Resource:  chk.resource,
					Verb:      chk.verb,
				},
			},
		}
		res, err := kube.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			continue
		}
		if !res.Status.Allowed {
			denied = append(denied, chk.String())
		}
	}
	return denied
}

type namespaceScope struct {
	// watch is the runner namespaces, or "" for cluster-wide.
	watch      []string
	controller string
}

func available(name string, now time.Time) fleet.Source {
	return fleet.Source{Name: name, Available: true, CheckedAt: now}
}

func unavailable(name, reason string, now time.Time) fleet.Source {
	return fleet.Source{Name: name, Available: false, Reason: reason, CheckedAt: now}
}

// sourceOrder: the closer to the cluster, the further left.
var sourceOrder = map[string]int{
	fleet.SourceKubernetes:        0,
	fleet.SourceARCCRDs:           1,
	fleet.SourcePrometheus:        2,
	fleet.SourceARCMetrics:        3,
	fleet.SourceControllerMetrics: 4,
	fleet.SourceCAdvisor:          5,
	fleet.SourceKSM:               6,
	fleet.SourceStore:             7,
}

func sortSources(sources []fleet.Source) {
	slices.SortStableFunc(sources, func(a, b fleet.Source) int {
		ra, oka := sourceOrder[a.Name]
		rb, okb := sourceOrder[b.Name]
		return cmp.Or(
			// Unknown sources sort last, not into rank 0.
			cmp.Compare(boolRank(!oka), boolRank(!okb)),
			cmp.Compare(ra, rb),
			cmp.Compare(a.Name, b.Name),
		)
	})
}

func logSource(log zerolog.Logger, s fleet.Source) {
	if s.Available {
		log.Info().Str("source", s.Name).Msg("data source available")
		return
	}
	log.Warn().Str("source", s.Name).Str("reason", s.Reason).Msg("data source unavailable, degrading")
}
