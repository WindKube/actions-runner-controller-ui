package k8s

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
	authzv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"
)

// MetricsGroupVersion is the aggregated API metrics-server serves. It is
// reached through the main API server, not from metrics-server directly.
const MetricsGroupVersion = "metrics.k8s.io/v1beta1"

// unreachablePrefix marks the one source failure that is fatal to boot: an API
// server we cannot talk to at all, as opposed to one that merely refuses us.
const unreachablePrefix = "api server unreachable"

// Three different questions get asked with three different APIs, and mixing
// them up produces confidently wrong answers:
//
//   - "does this aggregated API exist?" is discovery. metrics.k8s.io is an
//     APIService, not a CRD, so the RESTMapper is the wrong instrument.
//   - "is this CRD installed?" is the RESTMapper, because a dynamic informer on
//     a missing resource does not fail, it retries forever.
//   - "am I allowed?" is SelfSubjectAccessReview, which needs no permissions of
//     its own — it is granted to system:authenticated — so it is the one probe
//     that cannot itself be blocked by the RBAC it is checking.

// probeKubernetes checks that the API server answers and that we may list and
// watch the pods every view depends on.
func probeKubernetes(ctx context.Context, kube kubernetes.Interface, disc discovery.DiscoveryInterface, cfg namespaceScope, now time.Time) fleet.Source {
	if _, err := disc.ServerVersion(); err != nil {
		return unavailable(fleet.SourceKubernetes, fmt.Sprintf("%s: %v", unreachablePrefix, err), now)
	}

	checks := make([]accessCheck, 0, 2*len(cfg.watch)+1)
	for _, ns := range cfg.watch {
		checks = append(checks,
			accessCheck{namespace: ns, resource: "pods", verb: "list"},
			accessCheck{namespace: ns, resource: "pods", verb: "watch"},
		)
	}
	checks = append(checks, accessCheck{namespace: cfg.controller, resource: "pods", verb: "list"})

	if denied := deniedChecks(ctx, kube, checks); len(denied) > 0 {
		return unavailable(fleet.SourceKubernetes, "missing RBAC: "+strings.Join(denied, ", "), now)
	}
	return available(fleet.SourceKubernetes, now)
}

// probeARCCRDs reports whether the ARC custom resources are installed and
// readable, and returns the per-resource verdict the informer wiring gates on.
//
// This check has to happen before any informer is created. A dynamic informer
// pointed at an uninstalled CRD does not return an error from ForResource — it
// spins on a 404 forever and hangs WaitForCacheSync until the boot timeout.
func probeARCCRDs(ctx context.Context, kube kubernetes.Interface, mapper meta.RESTMapper, cfg namespaceScope, now time.Time) (fleet.Source, map[schema.GroupVersionResource]bool) {
	usable := make(map[schema.GroupVersionResource]bool, len(arcapi.AllGVRs()))

	// The deferred mapper caches negative discovery results, so a dashboard that
	// started before the CRDs were installed would never notice them.
	if r, ok := mapper.(interface{ Reset() }); ok {
		r.Reset()
	}

	var (
		notInstalled []string
		unreadable   []string
	)
	for _, gvr := range arcapi.AllGVRs() {
		if _, err := mapper.KindFor(gvr); err != nil {
			if meta.IsNoMatchError(err) {
				notInstalled = append(notInstalled, gvr.Resource)
			} else {
				// Discovery itself failed, which is a different problem from a
				// CRD that is genuinely absent and deserves a different message.
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

	// Permissions are checked per resource so a denial on one CRD cannot take
	// the others down with it. Substring-matching resource names would: every
	// ARC plural is a prefix of another ("ephemeralrunners" of
	// "ephemeralrunnersets"), so one denied watch would blank two informers.
	var denied []string
	for gvr := range usable {
		namespaces := cfg.watch
		if gvr == arcapi.AutoscalingListenerGVR {
			// Listeners live with the controller, not with the runners.
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
		sort.Strings(denied)
		return unavailable(fleet.SourceARCCRDs, "missing RBAC: "+strings.Join(denied, ", "), now), usable
	}
	// Built into a fresh slice rather than appended onto notInstalled: append
	// would write through that slice's spare capacity, so the two would alias.
	// Nothing reads notInstalled afterwards today, which is exactly the kind of
	// thing a later edit quietly invalidates.
	missing := make([]string, 0, len(notInstalled)+len(unreadable))
	missing = append(missing, notInstalled...)
	missing = append(missing, unreadable...)
	if len(missing) > 0 {
		sort.Strings(missing)
		return unavailable(fleet.SourceARCCRDs, "unavailable: "+strings.Join(missing, ", "), now), usable
	}
	return available(fleet.SourceARCCRDs, now), usable
}

// probeMetrics reports whether metrics-server's aggregated API is present and
// readable. A cluster without it is entirely normal; usage columns render as
// "—" rather than as zero.
func probeMetrics(ctx context.Context, kube kubernetes.Interface, disc discovery.DiscoveryInterface, cfg namespaceScope, now time.Time) fleet.Source {
	if _, err := disc.ServerResourcesForGroupVersion(MetricsGroupVersion); err != nil {
		if apierrors.IsNotFound(err) {
			return unavailable(fleet.SourceMetrics, "metrics-server is not installed", now)
		}
		// An aggregated API whose backend is down answers 503 through discovery.
		return unavailable(fleet.SourceMetrics, fmt.Sprintf("%s unavailable: %v", MetricsGroupVersion, err), now)
	}

	checks := make([]accessCheck, 0, len(cfg.watch))
	for _, ns := range cfg.watch {
		checks = append(checks, accessCheck{namespace: ns, group: "metrics.k8s.io", resource: "pods", verb: "list"})
	}
	if denied := deniedChecks(ctx, kube, checks); len(denied) > 0 {
		return unavailable(fleet.SourceMetrics, "missing RBAC: "+strings.Join(denied, ", "), now)
	}
	return available(fleet.SourceMetrics, now)
}

// accessCheck is one namespaced resource permission to verify.
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

// deniedChecks returns the human-readable names of the checks that came back
// denied.
//
// A failure to run the review at all is deliberately NOT reported as a denial:
// SelfSubjectAccessReview requires no permissions, so an error means the API
// server is unhappy, and guessing "denied" would blank the dashboard for a
// reason that has nothing to do with RBAC.
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

// namespaceScope is the set of namespaces a probe should ask about.
type namespaceScope struct {
	// watch is the runner namespaces, or a single empty string meaning
	// cluster-wide.
	watch []string
	// controller holds the ARC controller and the listener pods.
	controller string
}

func available(name string, now time.Time) fleet.Source {
	return fleet.Source{Name: name, Available: true, CheckedAt: now}
}

func unavailable(name, reason string, now time.Time) fleet.Source {
	return fleet.Source{Name: name, Available: false, Reason: reason, CheckedAt: now}
}

// sourceOrder is the order the control-plane strip renders sources in: the
// closer to the cluster, the further left.
var sourceOrder = map[string]int{
	fleet.SourceKubernetes: 0,
	fleet.SourceARCCRDs:    1,
	fleet.SourceMetrics:    2,
	fleet.SourceListener:   3,
	fleet.SourceStore:      4,
}

// sortSources gives the strip a stable order regardless of which probe finished
// first or which subsystem pushed an update last.
func sortSources(sources []fleet.Source) {
	sort.SliceStable(sources, func(i, j int) bool {
		ri, oki := sourceOrder[sources[i].Name]
		rj, okj := sourceOrder[sources[j].Name]
		if oki != okj {
			return oki
		}
		if ri != rj {
			return ri < rj
		}
		return sources[i].Name < sources[j].Name
	})
}

// logSource records a probe result once, at a level that matches its severity.
func logSource(log zerolog.Logger, s fleet.Source) {
	if s.Available {
		log.Info().Str("source", s.Name).Msg("data source available")
		return
	}
	log.Warn().Str("source", s.Name).Str("reason", s.Reason).Msg("data source unavailable, degrading")
}
