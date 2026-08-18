// Package metrics polls pod CPU and memory usage from the metrics.k8s.io
// aggregated API and pushes it into the snapshot.
//
// It polls rather than watches on purpose. metrics.k8s.io is served by
// metrics-server out of an in-memory store, not by etcd: there is no
// resourceVersion, no watch verb and no informer to build. The API only ever
// answers "what did I scrape most recently", so a ticker is the whole design.
//
// Everything here is best-effort. metrics-server is an optional add-on, RBAC
// for it is frequently missing, and even on a healthy cluster a pod has no
// metrics for the first ~30 seconds of its life — which for ephemeral runners
// means a meaningful fraction of them live and die without ever being scraped.
// A missing pod is therefore never an error; only a failing List is.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"time"

	arcv1alpha1 "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/fleet"

	"github.com/rs/zerolog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// runnerSelector matches every ARC runner pod in the cluster. The controller
// stamps this label on the pod (not just the EphemeralRunner), so one selector
// keeps the response to runner pods instead of every pod in the namespace.
const runnerSelector = arcv1alpha1.LabelEphemeralRunner + "=True"

// defaultInterval is used when the caller passes a non-positive interval.
// It matches metrics-server's own --metric-resolution default: polling faster
// returns byte-identical data, because there is nothing newer to return.
const defaultInterval = 15 * time.Second

// Sink receives each successful scrape and each health change.
//
// Implementations must be safe to call from the poller's goroutine.
type Sink interface {
	// SetUsage replaces the usage map. Keys are "namespace/podname".
	SetUsage(map[string]fleet.Usage)
	// SetSource reports whether metrics-server is currently usable.
	SetSource(fleet.Source)
}

// Poller scrapes pod metrics on an interval and pushes them to a Sink.
type Poller struct {
	client     metricsclient.Interface
	namespaces []string
	interval   time.Duration
	log        zerolog.Logger
	sink       Sink

	health healthTracker
	// now is swapped in tests; production always uses time.Now.
	now func() time.Time
}

// NewPoller polls pod metrics on interval and pushes results to sink. An empty
// namespace list means every namespace.
func NewPoller(client metricsclient.Interface, namespaces []string, interval time.Duration, log zerolog.Logger, sink Sink) *Poller {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Poller{
		client:     client,
		namespaces: namespaces,
		interval:   interval,
		log:        log.With().Str("component", "metrics-poller").Logger(),
		sink:       sink,
		now:        time.Now,
	}
}

// Run polls until ctx is cancelled. Returns nil on clean shutdown.
//
// The first scrape happens immediately so the dashboard is not blank for a
// whole interval after boot.
func (p *Poller) Run(ctx context.Context) error {
	if p.client == nil {
		// No client means the cluster connection itself failed. Say so once
		// and stop, rather than panicking on every tick.
		p.sink.SetSource(fleet.Source{
			Name:      fleet.SourceMetrics,
			Available: false,
			Reason:    "no Kubernetes client: metrics.k8s.io cannot be reached",
			CheckedAt: p.now(),
		})
		return nil
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	p.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

// tick performs one scrape and reports the outcome. It never returns an error:
// a broken metrics-server degrades the dashboard, it does not stop it.
func (p *Poller) tick(ctx context.Context) {
	// One scrape must not outlive its own interval. Run calls tick
	// synchronously, so a metrics-server that accepts the connection and then
	// never answers blocks the List for the life of the process: no later tick
	// fires, and the dashboard silently stops updating without ever reporting
	// the source as unavailable. With a deadline the hang becomes an ordinary
	// scrape failure, which the code below already knows how to report.
	scrapeCtx, cancel := context.WithTimeout(ctx, p.interval)
	defer cancel()

	usage, err := p.scrape(scrapeCtx)
	if err != nil {
		if ctx.Err() != nil {
			// Deliberately ctx and not scrapeCtx: a scrape that timed out is a
			// metrics-server fault worth reporting, while a cancelled parent is
			// shutdown. Testing the derived context would conflate them.
			//
			// Shutdown, not a metrics-server fault. Leave the last known
			// health alone so the final render does not accuse a healthy
			// cluster of being broken.
			return
		}
		p.health.fail(p.log, err.Error())
		p.sink.SetSource(fleet.Source{
			Name:      fleet.SourceMetrics,
			Available: false,
			Reason:    err.Error(),
			CheckedAt: p.now(),
		})
		if len(usage) == 0 {
			// Nothing at all came back. Keep whatever the sink already holds:
			// every sample carries its own timestamp, so the UI can show the
			// readings as stale, which beats blanking them.
			return
		}
	} else {
		p.health.ok(p.log)
		p.sink.SetSource(fleet.Source{
			Name:      fleet.SourceMetrics,
			Available: true,
			CheckedAt: p.now(),
		})
	}

	// A successful scrape that saw no pods legitimately clears the map:
	// the runners it described are gone.
	p.sink.SetUsage(usage)
}

// scrape lists pod metrics across every configured namespace.
//
// Namespaces are independent, so one failing does not discard the others: the
// partial map is returned alongside the error and the caller publishes both.
func (p *Poller) scrape(ctx context.Context) (map[string]fleet.Usage, error) {
	out := make(map[string]fleet.Usage)

	namespaces := p.namespaces
	if len(namespaces) == 0 {
		namespaces = []string{metav1.NamespaceAll}
	}

	var errs []error
	for _, ns := range namespaces {
		list, err := p.client.MetricsV1beta1().PodMetricses(ns).List(ctx, metav1.ListOptions{
			LabelSelector: runnerSelector,
		})
		if err != nil {
			scope := ns
			if scope == metav1.NamespaceAll {
				scope = "all namespaces"
			}
			errs = append(errs, fmt.Errorf("listing pod metrics in %s: %w", scope, err))
			continue
		}
		for i := range list.Items {
			pm := &list.Items[i]
			out[Key(pm.Namespace, pm.Name)] = usageOf(pm.Containers, pm.Timestamp.Time)
		}
	}

	return out, errors.Join(errs...)
}

// Key is the map key the sink is indexed by: "namespace/podname".
func Key(namespace, pod string) string { return namespace + "/" + pod }

// usageOf sums a pod's containers. PodMetrics carries no pod-level total —
// only the per-container breakdown — so the sum is ours to compute.
//
// CPU goes through MilliValue and not Value: Value rounds up to whole cores,
// which would turn every 250m reading into 1.0 and inflate the entire fleet's
// usage by an order of magnitude. Milli-cores are summed as integers first so
// no per-container rounding accumulates.
func usageOf(containers []metricsv1beta1.ContainerMetrics, at time.Time) fleet.Usage {
	var milliCores, bytes int64
	for _, c := range containers {
		if q, ok := c.Usage[corev1.ResourceCPU]; ok {
			milliCores += q.MilliValue()
		}
		if q, ok := c.Usage[corev1.ResourceMemory]; ok {
			bytes += q.Value()
		}
	}
	return fleet.Usage{
		CPUCores: float64(milliCores) / 1000,
		MemBytes: float64(bytes),
		// The scrape time comes from metrics-server, never from the clock
		// here: the UI renders staleness from it and must not claim a reading
		// is fresher than it is.
		At: at,
	}
}
