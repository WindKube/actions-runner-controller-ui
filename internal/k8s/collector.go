package k8s

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/config"
	"arc-ui/internal/fleet"
)

const (
	// resyncPeriod is zero deliberately. A resync does not re-fetch anything —
	// it replays the local cache through every handler — so a non-zero period
	// buys no fresher data and costs a full re-decode of every object on a
	// timer. Watches already deliver every change.
	resyncPeriod = 0

	// cacheSyncTimeout bounds boot. An informer whose watch is being refused
	// retries forever, so without a deadline the process hangs at startup
	// instead of serving a dashboard that says what is broken.
	cacheSyncTimeout = 60 * time.Second

	// changeDebounce coalesces informer events into one SSE push. A fleet
	// scaling up produces hundreds of events a second and every one of them
	// would otherwise re-render every connected browser.
	changeDebounce = 250 * time.Millisecond

	// watchErrorLogInterval throttles watch-failure logging. The reflector
	// retries continuously, so an unthrottled handler turns one missing RBAC
	// rule into an unbounded log flood.
	watchErrorLogInterval = 30 * time.Second

	// watchFailureTTL is how long a watch error keeps a source marked broken.
	// A reflector whose watch is being refused retries about once a second, so
	// silence for this long means it recovered.
	watchFailureTTL = time.Minute
)

// Collector owns the informers and produces fleet.Snapshot.
//
// It keeps its own typed maps rather than reading through the informer stores
// on demand: decoding unstructured objects is reflection-heavy, and doing it
// once per watch event instead of once per object per HTTP request is the
// difference between a dashboard that scales to a few thousand runners and one
// that does not.
type Collector struct {
	clients *Clients
	cfg     config.Config
	log     zerolog.Logger

	mu            sync.RWMutex
	sets          map[string]*arcapi.AutoscalingRunnerSet
	ephemeralSets map[string]*arcapi.EphemeralRunnerSet
	runners       map[string]*arcapi.EphemeralRunner
	listeners     map[string]*arcapi.AutoscalingListener
	runnerPods    map[string]*corev1.Pod
	ctrlPods      map[string]*corev1.Pod

	usage      map[string]fleet.Usage
	queue      map[string]int
	queueKnown bool
	sources    map[string]fleet.Source
	// watchFailures overlays sources with expiring, informer-reported breakage.
	watchFailures map[string]fleet.Source

	// runnerSynced holds HasSynced for every EphemeralRunner informer, one per
	// watched namespace. Empty means none is running — the CRD is absent or
	// list/watch on it is denied — which is itself a reason to distrust an empty
	// runner list. Written once during Start.
	runnerSynced []cache.InformerSynced
	// runnerWatchFailedAt stamps the last watch error reported by an
	// EphemeralRunner informer, and only that resource: the arc-crds source is
	// an aggregate over four of them, so it cannot answer "can we see runners".
	runnerWatchFailedAt time.Time

	controllerVersion string

	cached fleet.Snapshot
	dirty  bool

	jobStarts *JobStartTracker
	events    *eventCache

	synced atomic.Bool

	subsMu  sync.Mutex
	subs    map[uint64]func()
	nextSub uint64
	notify  chan struct{}

	errLogMu sync.Mutex
	errLogAt map[string]time.Time
}

// NewCollector wires a collector to a set of clients. Nothing talks to the
// cluster until Start.
func NewCollector(clients *Clients, cfg config.Config, log zerolog.Logger) *Collector {
	return &Collector{
		clients:       clients,
		cfg:           cfg,
		log:           log.With().Str("component", "collector").Logger(),
		sets:          map[string]*arcapi.AutoscalingRunnerSet{},
		ephemeralSets: map[string]*arcapi.EphemeralRunnerSet{},
		runners:       map[string]*arcapi.EphemeralRunner{},
		listeners:     map[string]*arcapi.AutoscalingListener{},
		runnerPods:    map[string]*corev1.Pod{},
		ctrlPods:      map[string]*corev1.Pod{},
		usage:         map[string]fleet.Usage{},
		queue:         map[string]int{},
		sources:       map[string]fleet.Source{},
		watchFailures: map[string]fleet.Source{},
		jobStarts:     NewJobStartTracker(),
		events:        newEventCache(eventCacheTTL),
		subs:          map[uint64]func(){},
		notify:        make(chan struct{}, 1),
		errLogAt:      map[string]time.Time{},
		dirty:         true,
	}
}

// Start probes the available data sources, launches the informers and blocks
// until their caches sync or the boot deadline passes.
//
// ctx governs the lifetime of every informer, not just the wait: cancelling it
// shuts the watches down. A missing CRD, an absent metrics-server or denied
// RBAC all return nil and leave a fleet.Source explaining the gap; only a
// cluster we cannot reach at all is an error.
func (c *Collector) Start(ctx context.Context) error {
	now := time.Now()
	scope := c.scope()

	kubeSrc := probeKubernetes(ctx, c.clients.Kube, c.clients.Discovery, scope, now)
	logSource(c.log, kubeSrc)
	c.SetSource(kubeSrc)
	if !kubeSrc.Available && isUnreachable(kubeSrc.Reason) {
		return fmt.Errorf("kubernetes api unusable: %s", kubeSrc.Reason)
	}

	arcSrc, usable := probeARCCRDs(ctx, c.clients.Kube, c.clients.Mapper, scope, now)
	logSource(c.log, arcSrc)
	c.SetSource(arcSrc)

	metricsSrc := probeMetrics(ctx, c.clients.Kube, c.clients.Discovery, scope, now)
	logSource(c.log, metricsSrc)
	c.SetSource(metricsSrc)

	c.setControllerVersion(c.discoverControllerVersion(ctx))

	syncFns := make([]cache.InformerSynced, 0, len(arcapi.AllGVRs())+2)
	syncFns = append(syncFns, c.startCustomResourceInformers(ctx, scope, usable)...)
	syncFns = append(syncFns, c.startPodInformers(ctx, scope)...)

	// Bounded, so a cluster that will never answer produces a dashboard that
	// says so instead of a process that never finishes booting.
	syncCtx, cancel := context.WithTimeout(ctx, cacheSyncTimeout)
	defer cancel()

	if cache.WaitForCacheSync(syncCtx.Done(), syncFns...) {
		c.synced.Store(true)
		c.log.Info().Int("informers", len(syncFns)).Msg("informer caches synced")
	} else {
		c.log.Warn().Dur("timeout", cacheSyncTimeout).Msg("informer caches did not sync; serving degraded")
	}

	go c.runNotifier(ctx)
	c.touch()
	return nil
}

// HasSynced reports informer readiness for the /readyz probe.
func (c *Collector) HasSynced() bool { return c.synced.Load() }

// Snapshot returns the current fleet state. Safe for concurrent use.
//
// The result is rebuilt only when something changed, then handed out with its
// top-level slices copied. The copy is not paranoia: fleet.SortRunners sorts in
// place, so a view that sorts a shared slice would race against every other
// request.
func (c *Collector) Snapshot() fleet.Snapshot {
	c.mu.RLock()
	// A pending watch failure expires on a timer rather than on an event, so
	// while one is outstanding the snapshot cannot be trusted to be current.
	if !c.dirty && len(c.watchFailures) == 0 {
		snap := c.cached
		c.mu.RUnlock()
		return copySnapshot(snap)
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dirty || len(c.watchFailures) > 0 {
		c.cached = BuildSnapshot(c.snapshotInputLocked())
		c.dirty = false
	}
	return copySnapshot(c.cached)
}

// snapshotInputLocked flattens the collector's maps. Caller holds c.mu.
func (c *Collector) snapshotInputLocked() SnapshotInput {
	now := time.Now()
	in := SnapshotInput{
		Now:               now,
		Org:               c.cfg.GitHubOrg,
		ControllerVersion: c.controllerVersion,
		Sets:              lo.Values(c.sets),
		EphemeralSets:     lo.Values(c.ephemeralSets),
		Runners:           lo.Values(c.runners),
		Listeners:         lo.Values(c.listeners),
		RunnerPods:        lo.Values(c.runnerPods),
		ControllerPods:    lo.Values(c.ctrlPods),
		Usage:             c.usage,
		Queue:             c.queue,
		QueueKnown:        c.queueKnown,
		RunnersDegraded:   c.runnersDegradedLocked(now),
		JobStarts:         c.jobStarts,
	}

	// A live watch failure overrides the boot-time probe for that source; an
	// expired one is dropped so the strip goes green again on its own.
	for name, failure := range c.watchFailures {
		if in.Now.Sub(failure.CheckedAt) > watchFailureTTL {
			delete(c.watchFailures, name)
			continue
		}
		in.Sources = append(in.Sources, failure)
	}
	for name, s := range c.sources {
		if _, broken := c.watchFailures[name]; broken {
			continue
		}
		in.Sources = append(in.Sources, s)
	}
	return in
}

// runnersDegradedLocked answers one narrow question: can the EphemeralRunner
// cache be trusted, right now, to hold every runner that exists? Caller holds
// c.mu.
//
// It deliberately does not consult the arc-crds source. That verdict is an
// aggregate over four resources — probeARCCRDs marks it unavailable when any
// one of them is absent or denied while the rest keep working — and it is
// computed exactly once, at boot, with nothing that ever re-probes it. Reading
// it here would let a missing autoscalinglisteners rule pin a "we cannot see
// runners" answer for the life of the process.
//
// Both inputs here are live and self-healing: HasSynced flips true when the
// initial LIST lands, and the watch stamp ages out on the same TTL that clears
// the source overlay.
//
// Calling HasSynced under c.mu is safe because the dependency only runs one way:
// it takes the informer's own locks, and the event handlers that take c.mu are
// invoked from the shared informer's own goroutine rather than inline while
// those locks are held.
func (c *Collector) runnersDegradedLocked(now time.Time) bool {
	// No informer at all: the runner list is empty for a reason that has
	// nothing to do with the fleet being idle. Nothing is ever tracked in that
	// state either, so refusing to sweep costs nothing.
	if len(c.runnerSynced) == 0 {
		return true
	}
	if !c.runnerWatchFailedAt.IsZero() && now.Sub(c.runnerWatchFailedAt) <= watchFailureTTL {
		return true
	}
	// Before the initial LIST completes the cache is partially filled, so an
	// absent runner is unproven rather than gone.
	for _, synced := range c.runnerSynced {
		if !synced() {
			return true
		}
	}
	return false
}

func copySnapshot(s fleet.Snapshot) fleet.Snapshot {
	s.Sets = slices.Clone(s.Sets)
	s.Runners = slices.Clone(s.Runners)
	s.Sources = slices.Clone(s.Sources)
	return s
}

// listenerMetricsPort is the container port ARC names when the controller is
// configured with a listener metrics address. Its absence is how a listener says
// metrics were never enabled, which on a stock install is every listener.
const listenerMetricsPort = "metrics"

// defaultListenerMetricsPath matches the controller chart's default
// metrics.listenerEndpoint.
const defaultListenerMetricsPath = "/metrics"

// ListenerTargets returns one scrape target per listener pod currently serving
// metrics, so the scraper can cover a whole fleet instead of one scale set.
//
// ARC runs one AutoscalingListener pod per AutoscalingRunnerSet and each serves
// only its own scale set's series, so a single configured URL covers exactly one
// set however many there are — and a Service in front of them is worse, because
// a keep-alive connection pins to whichever pod it reached first and then jumps
// to another when it is recycled.
//
// Everything this needs is already cached: the controller namespace is watched
// unfiltered for listener health, and trimPod keeps status.podIP and the
// container ports. Targets are rebuilt on every call rather than remembered,
// because a pod IP is recycled the moment the address is.
func (c *Collector) ListenerTargets() []fleet.ListenerTarget {
	path := c.cfg.ListenerMetricsPath
	if path == "" {
		path = defaultListenerMetricsPath
	}

	c.mu.Lock()
	pods := lo.Values(c.ctrlPods)
	c.mu.Unlock()

	out := make([]fleet.ListenerTarget, 0, len(pods))
	for _, pod := range pods {
		if pod == nil || pod.Status.PodIP == "" || pod.DeletionTimestamp != nil {
			continue
		}
		// A Succeeded or Failed pod can keep a stale address long enough to be
		// scraped, and the answer would be a connection refused reported as a
		// broken listener.
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		// The scale set labels are what separates a listener from the controller
		// manager, which lives in the same namespace and also serves metrics —
		// its own gha_controller_* series, which this parser does not read. They
		// are also the only link that survives across ARC versions; the
		// component label does not, which is why the informer above is
		// unfiltered.
		set := pod.Labels[arcapi.LabelScaleSetName]
		ns := pod.Labels[arcapi.LabelScaleSetNamespace]
		if set == "" || ns == "" {
			continue
		}
		// Runner pods carry those same labels, and land here if runners share
		// the controller namespace. They expose no ports, so the port lookup
		// below already excludes them — this is belt and braces for the day
		// something gives them one.
		if pod.Labels[arcapi.LabelEphemeralRunner] == "True" {
			continue
		}

		port, ok := metricsPortOf(pod)
		if !ok {
			continue
		}

		out = append(out, fleet.ListenerTarget{
			Set:       set,
			Namespace: ns,
			Pod:       pod.Name,
			URL:       fmt.Sprintf("http://%s%s", net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(port))), path),
		})
	}

	// Stable order so the log line a scrape failure produces names the same
	// listener each time, and so tests do not depend on map iteration.
	slices.SortFunc(out, func(a, b fleet.ListenerTarget) int {
		return strings.Compare(a.Pod, b.Pod)
	})
	return out
}

// metricsPortOf finds the listener's metrics port by name.
func metricsPortOf(pod *corev1.Pod) (int32, bool) {
	for _, ctr := range pod.Spec.Containers {
		for _, p := range ctr.Ports {
			if p.Name == listenerMetricsPort {
				return p.ContainerPort, true
			}
		}
	}
	return 0, false
}

// SetUsage injects the latest pod metrics, keyed by "namespace/podname".
func (c *Collector) SetUsage(usage map[string]fleet.Usage) {
	c.mu.Lock()
	c.usage = usage
	c.dirty = true
	c.mu.Unlock()
	c.touch()
}

// SetQueueDepth injects listener-derived queue depth per scale set name.
//
// known is false when the listener's metrics endpoint is not configured or not
// answering, which is the default on a stock ARC install. Rendering "0 queued"
// in that case is worse than rendering nothing.
func (c *Collector) SetQueueDepth(perSet map[string]int, known bool) {
	c.mu.Lock()
	c.queue = perSet
	c.queueKnown = known
	c.dirty = true
	c.mu.Unlock()
	c.touch()
}

// SetSource records the health of an externally-owned source, such as the
// metrics poller or the listener scraper.
//
// Only a change of verdict wakes subscribers. CheckedAt moves on every probe,
// so treating each call as news would make a source that reports the same thing
// on a timer indistinguishable from one that just changed — and worse, a source
// whose failure is itself reported by a subscriber would sustain a loop: the
// history store reports a failed write through here, that wakes the recorder,
// the recorder writes and fails again. The freshest CheckedAt is still stored,
// so the strip keeps its real timestamp; it just rides out on the next change.
func (c *Collector) SetSource(s fleet.Source) {
	if s.CheckedAt.IsZero() {
		s.CheckedAt = time.Now()
	}
	c.mu.Lock()
	prev, seen := c.sources[s.Name]
	changed := !seen || prev.Available != s.Available || prev.Reason != s.Reason
	c.sources[s.Name] = s
	if changed {
		c.dirty = true
	}
	c.mu.Unlock()

	if changed {
		c.touch()
	}
}

// OnChange registers a callback fired, debounced, when the fleet changes.
//
// Callbacks run on the collector's notifier goroutine, so a slow one delays
// every other subscriber. Push to a channel and return.
func (c *Collector) OnChange(fn func()) (cancel func()) {
	c.subsMu.Lock()
	id := c.nextSub
	c.nextSub++
	c.subs[id] = fn
	c.subsMu.Unlock()

	return func() {
		c.subsMu.Lock()
		delete(c.subs, id)
		c.subsMu.Unlock()
	}
}

// touch signals that the fleet changed. Non-blocking: the channel has room for
// one pending notification, which is all a debounce needs.
func (c *Collector) touch() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// runNotifier fires subscribers at most once per debounce window.
func (c *Collector) runNotifier(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.notify:
		}

		timer := time.NewTimer(changeDebounce)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		c.subsMu.Lock()
		fns := make([]func(), 0, len(c.subs))
		for _, fn := range c.subs {
			fns = append(fns, fn)
		}
		c.subsMu.Unlock()

		for _, fn := range fns {
			fn()
		}
	}
}

// --- informers --------------------------------------------------------------

// scope resolves which namespaces to watch. An empty namespace list means the
// whole cluster, which the informer factories spell as the empty string.
func (c *Collector) scope() namespaceScope {
	watch := c.cfg.Namespaces
	if len(watch) == 0 {
		watch = []string{metav1.NamespaceAll}
	}
	return namespaceScope{watch: watch, controller: c.cfg.ControllerNamespace}
}

// startCustomResourceInformers launches one dynamic informer per available ARC
// resource per watched namespace.
//
// Every informer is gated on the RESTMapper verdict computed earlier. This is
// not an optimisation: a dynamic informer over a resource whose CRD is not
// installed never reports an error from ForResource, it just retries the 404
// forever and blocks WaitForCacheSync until the boot deadline.
func (c *Collector) startCustomResourceInformers(ctx context.Context, scope namespaceScope, usable map[schema.GroupVersionResource]bool) []cache.InformerSynced {
	var synced []cache.InformerSynced

	for _, ns := range scope.watch {
		for _, gvr := range arcapi.AllGVRs() {
			if !usable[gvr] {
				continue
			}
			// Listeners live in the controller namespace; watching them in the
			// runner namespaces finds nothing.
			if gvr == arcapi.AutoscalingListenerGVR {
				continue
			}
			started := c.startDynamicInformer(ctx, gvr, ns)
			if gvr == arcapi.EphemeralRunnerGVR {
				// Remembered separately because the job-start sweep needs to
				// know whether THIS cache is complete; see
				// runnersDegradedLocked.
				c.mu.Lock()
				c.runnerSynced = append(c.runnerSynced, started...)
				c.mu.Unlock()
			}
			synced = append(synced, started...)
		}
	}

	if usable[arcapi.AutoscalingListenerGVR] {
		synced = append(synced, c.startDynamicInformer(ctx, arcapi.AutoscalingListenerGVR, scope.controller)...)
	}
	return synced
}

func (c *Collector) startDynamicInformer(ctx context.Context, gvr schema.GroupVersionResource, namespace string) []cache.InformerSynced {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(c.clients.Dynamic, resyncPeriod, namespace, nil)
	informer := factory.ForResource(gvr).Informer()

	// Must be set before Start. Without it a 403 from a missing RBAC rule is an
	// infinite silent retry: no log line, no source marked bad, just a process
	// that hangs at boot and then serves an empty dashboard.
	c.attachWatchErrorHandler(informer, fleet.SourceARCCRDs, gvr.Resource, namespace)

	if err := informer.SetTransform(trimCustomResource(gvr)); err != nil {
		c.log.Error().Err(err).Str("resource", gvr.Resource).Msg("set informer transform")
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.applyCustomResource(gvr, obj, false) },
		UpdateFunc: func(_, obj any) { c.applyCustomResource(gvr, obj, false) },
		DeleteFunc: func(obj any) { c.applyCustomResource(gvr, obj, true) },
	}); err != nil {
		c.log.Error().Err(err).Str("resource", gvr.Resource).Msg("attach event handler")
		return nil
	}

	factory.Start(ctx.Done())
	return []cache.InformerSynced{informer.HasSynced}
}

// startPodInformers launches the typed pod informers: runner pods in the
// watched namespaces, and everything in the controller namespace so listener
// health can be read off the listener pods.
func (c *Collector) startPodInformers(ctx context.Context, scope namespaceScope) []cache.InformerSynced {
	var synced []cache.InformerSynced

	runnerSelector := labels.Set{arcapi.LabelEphemeralRunner: "True"}.String()
	for _, ns := range scope.watch {
		synced = append(synced, c.startPodInformer(ctx, ns, runnerSelector, c.runnerPods, "runner")...)
	}

	// The controller namespace holds a handful of pods and the listeners carry
	// no single reliable label across ARC versions, so it is watched unfiltered.
	return append(synced, c.startPodInformer(ctx, scope.controller, "", c.ctrlPods, "controller")...)
}

// startPodInformer watches one namespace's pods into dst. role names the
// informer in log lines only.
func (c *Collector) startPodInformer(ctx context.Context, namespace, selector string, dst map[string]*corev1.Pod, role string) []cache.InformerSynced {
	opts := []informers.SharedInformerOption{
		informers.WithNamespace(namespace),
		informers.WithTransform(trimPod),
	}
	if selector != "" {
		opts = append(opts, informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector }))
	}

	factory := informers.NewSharedInformerFactoryWithOptions(c.clients.Kube, resyncPeriod, opts...)
	informer := factory.Core().V1().Pods().Informer()
	c.attachWatchErrorHandler(informer, fleet.SourceKubernetes, "pods", namespace)

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.applyPod(dst, obj, false) },
		UpdateFunc: func(_, obj any) { c.applyPod(dst, obj, false) },
		DeleteFunc: func(obj any) { c.applyPod(dst, obj, true) },
	}); err != nil {
		c.log.Error().Err(err).Msgf("attach %s pod handler", role)
		return nil
	}

	factory.Start(ctx.Done())
	return []cache.InformerSynced{informer.HasSynced}
}

// attachWatchErrorHandler makes watch failures visible instead of silent.
func (c *Collector) attachWatchErrorHandler(informer cache.SharedIndexInformer, source, resource, namespace string) {
	key := source + "/" + resource + "/" + namespace
	err := informer.SetWatchErrorHandlerWithContext(func(ctx context.Context, _ *cache.Reflector, werr error) {
		// A cancelled context is a shutdown, not a fault.
		if errors.Is(werr, context.Canceled) || ctx.Err() != nil {
			return
		}
		if c.shouldLogWatchError(key) {
			c.log.Error().Err(werr).
				Str("resource", resource).
				Str("namespace", namespace).
				Msg("watch failed; data source degraded")
		}
		c.noteWatchFailure(source, resource, fmt.Sprintf("watch %s in %s failed: %v",
			resource, displayNamespace(namespace), werr))
	})
	if err != nil {
		c.log.Error().Err(err).Str("resource", resource).Msg("set watch error handler")
	}
}

// noteWatchFailure records a watch error against a data source, and against the
// resource it came from.
//
// Kept apart from the probe results because it has to expire. The reflector
// retries roughly once a second while a watch is broken, so a failure we have
// not seen for a minute has healed — whereas writing it into the probe results
// would leave the control-plane strip red for the rest of the process's life
// after one blip, with nothing able to clear it.
//
// resource is tracked as well as source because the source is coarse: every ARC
// resource reports as arc-crds, so the source alone cannot say whether the
// runner cache in particular is trustworthy.
func (c *Collector) noteWatchFailure(source, resource, reason string) {
	now := time.Now()
	c.mu.Lock()
	c.watchFailures[source] = fleet.Source{
		Name:      source,
		Available: false,
		Reason:    reason,
		CheckedAt: now,
	}
	if resource == arcapi.EphemeralRunnerGVR.Resource {
		c.runnerWatchFailedAt = now
	}
	c.dirty = true
	c.mu.Unlock()
	c.touch()
}

func (c *Collector) shouldLogWatchError(key string) bool {
	c.errLogMu.Lock()
	defer c.errLogMu.Unlock()
	now := time.Now()
	if last, ok := c.errLogAt[key]; ok && now.Sub(last) < watchErrorLogInterval {
		return false
	}
	c.errLogAt[key] = now
	return true
}

func displayNamespace(ns string) string {
	if ns == metav1.NamespaceAll {
		return "all namespaces"
	}
	return ns
}

// trimPod strips the parts of a runner pod nobody renders.
//
// ARC injects a large environment block into every runner container, and
// managedFields on a pod that several controllers have touched is comparable in
// size to the spec. At a few thousand runners those two dominate the process's
// RSS, and neither is ever displayed.
func trimPod(obj any) (any, error) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		trimmed, err := trimPod(tombstone.Obj)
		if err != nil {
			return obj, err
		}
		tombstone.Obj = trimmed
		return tombstone, nil
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	pod.ManagedFields = nil
	pod.Spec.Volumes = nil
	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = nil
		pod.Spec.Containers[i].VolumeMounts = nil
	}
	for i := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].Env = nil
		pod.Spec.InitContainers[i].VolumeMounts = nil
	}
	return pod, nil
}

// trimCustomResource strips what the dynamic caches would otherwise hold and
// nothing would ever read.
//
// EphemeralRunner.spec embeds an entire pod template — ARC's large environment
// block included — and there is one per runner. Nothing in this package touches
// it (image, resources and node all come from the pod or from the scale set's
// template), so at a few thousand runners it is simply the biggest thing in the
// process for no reason. Anything that later needs a field from that spec has
// to stop dropping it here.
func trimCustomResource(gvr schema.GroupVersionResource) cache.TransformFunc {
	return func(obj any) (any, error) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			trimmed, err := trimCustomResource(gvr)(tombstone.Obj)
			if err != nil {
				return obj, err
			}
			tombstone.Obj = trimmed
			return tombstone, nil
		}
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return obj, nil
		}
		unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
		if gvr == arcapi.EphemeralRunnerGVR {
			unstructured.RemoveNestedField(u.Object, "spec")
		}
		return u, nil
	}
}

// --- event handlers ---------------------------------------------------------

// applyCustomResource decodes one unstructured object into its vendored type.
//
// The decode happens here, once per watch event, because
// DefaultUnstructuredConverter is reflection-heavy — running it per object per
// HTTP request is what turns a 3000-runner fleet into a second of CPU per page
// view.
func (c *Collector) applyCustomResource(gvr schema.GroupVersionResource, obj any, remove bool) {
	u, ok := asUnstructured(obj)
	if !ok {
		return
	}
	key := objectKey(u.GetNamespace(), u.GetName())

	c.mu.Lock()
	defer func() {
		c.dirty = true
		c.mu.Unlock()
		c.touch()
	}()

	if remove {
		switch gvr {
		case arcapi.AutoscalingRunnerSetGVR:
			delete(c.sets, key)
		case arcapi.EphemeralRunnerSetGVR:
			delete(c.ephemeralSets, key)
		case arcapi.EphemeralRunnerGVR:
			delete(c.runners, key)
		case arcapi.AutoscalingListenerGVR:
			delete(c.listeners, key)
		}
		return
	}

	switch gvr {
	case arcapi.AutoscalingRunnerSetGVR:
		var out arcapi.AutoscalingRunnerSet
		if c.decode(u, &out, gvr) {
			c.sets[key] = &out
		}
	case arcapi.EphemeralRunnerSetGVR:
		var out arcapi.EphemeralRunnerSet
		if c.decode(u, &out, gvr) {
			c.ephemeralSets[key] = &out
		}
	case arcapi.EphemeralRunnerGVR:
		var out arcapi.EphemeralRunner
		if c.decode(u, &out, gvr) {
			c.runners[key] = &out
		}
	case arcapi.AutoscalingListenerGVR:
		var out arcapi.AutoscalingListener
		if c.decode(u, &out, gvr) {
			c.listeners[key] = &out
		}
	}
}

func (c *Collector) decode(u *unstructured.Unstructured, into any, gvr schema.GroupVersionResource) bool {
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, into); err != nil {
		c.log.Warn().Err(err).
			Str("resource", gvr.Resource).
			Str("object", objectKey(u.GetNamespace(), u.GetName())).
			Msg("decode custom resource")
		return false
	}
	return true
}

// applyPod stores a pod in one of the collector's two pod maps. The map is
// passed rather than reassigned, so it is never replaced after construction.
func (c *Collector) applyPod(dst map[string]*corev1.Pod, obj any, remove bool) {
	pod, ok := asPod(obj)
	if !ok {
		return
	}
	key := objectKey(pod.Namespace, pod.Name)

	c.mu.Lock()
	if remove {
		delete(dst, key)
	} else {
		dst[key] = pod
	}
	c.dirty = true
	c.mu.Unlock()
	c.touch()
}

func asUnstructured(obj any) (*unstructured.Unstructured, bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	return u, ok
}

func asPod(obj any) (*corev1.Pod, bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	return pod, ok
}

// --- controller version -----------------------------------------------------

func (c *Collector) setControllerVersion(v string) {
	if v == "" {
		return
	}
	c.mu.Lock()
	c.controllerVersion = v
	c.dirty = true
	c.mu.Unlock()
}

// discoverControllerVersion reads the ARC controller's image tag, once, at
// boot. It is decoration for the health strip: an empty result is fine and
// never worth failing over.
func (c *Collector) discoverControllerVersion(ctx context.Context) string {
	deployments, err := c.clients.Kube.AppsV1().Deployments(c.cfg.ControllerNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		c.log.Debug().Err(err).Str("namespace", c.cfg.ControllerNamespace).Msg("controller version unavailable")
		return ""
	}
	for _, d := range deployments.Items {
		for _, container := range d.Spec.Template.Spec.Containers {
			if !isControllerImage(container.Name, container.Image) {
				continue
			}
			if tag := imageTag(container.Image); tag != "" {
				return tag
			}
		}
	}
	return ""
}

// isControllerImage recognises the ARC controller container. The Helm chart
// names it "manager"; the image name is the more stable signal.
func isControllerImage(name, image string) bool {
	return strings.Contains(image, "gha-runner-scale-set-controller") ||
		strings.Contains(image, "actions-runner-controller") ||
		name == "manager"
}

// isUnreachable distinguishes "we cannot talk to the API server" from "we can,
// but we are not allowed to see something".
func isUnreachable(reason string) bool {
	return strings.Contains(reason, unreachablePrefix)
}
