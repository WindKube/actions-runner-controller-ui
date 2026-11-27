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

	"github.com/hashicorp/golang-lru/v2/expirable"
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
	// A resync replays the local cache through every handler and fetches nothing.
	resyncPeriod = 0

	// cacheSyncTimeout bounds boot: a refused watch retries forever.
	cacheSyncTimeout = 60 * time.Second

	changeDebounce = 250 * time.Millisecond

	watchErrorLogInterval = 30 * time.Second

	// watchFailureTTL: a refused watch retries about once a second, so silence
	// this long means it recovered.
	watchFailureTTL = time.Minute
)

// Collector owns the informers and produces fleet.Snapshot. It decodes each
// object once per watch event into typed maps, because the unstructured
// converter is too slow to run per request on a large fleet.
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

	usage         map[string]fleet.Usage
	queue         map[string]int
	queueKnown    bool
	sources       map[string]fleet.Source
	watchFailures map[string]fleet.Source

	// runnerSynced and runnerWatchFailedAt answer whether the EphemeralRunner
	// cache in particular is complete; arc-crds aggregates four resources.
	runnerSynced        []cache.InformerSynced
	runnerWatchFailedAt time.Time

	cached fleet.Snapshot
	dirty  bool

	jobStarts *JobStartTracker
	events    *expirable.LRU[string, []fleet.Event]

	synced atomic.Bool

	subsMu  sync.Mutex
	subs    map[uint64]func()
	nextSub uint64
	notify  chan struct{}

	errLogMu sync.Mutex
	errLogAt map[string]time.Time
}

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
		events:        expirable.NewLRU[string, []fleet.Event](eventCacheSize, nil, eventCacheTTL),
		subs:          map[uint64]func(){},
		notify:        make(chan struct{}, 1),
		errLogAt:      map[string]time.Time{},
		dirty:         true,
	}
}

// Start probes the sources, launches the informers and waits for them to sync
// or for the boot deadline. Only an unreachable cluster is an error; missing
// CRDs or RBAC leave a fleet.Source explaining the gap.
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

	syncFns := make([]cache.InformerSynced, 0, len(arcapi.AllGVRs())+2)
	syncFns = append(syncFns, c.startCustomResourceInformers(ctx, scope, usable)...)
	syncFns = append(syncFns, c.startPodInformers(ctx, scope)...)

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

func (c *Collector) HasSynced() bool { return c.synced.Load() }

// Snapshot returns the current fleet state, rebuilt only on change. Top-level
// slices are copied because views sort them in place.
func (c *Collector) Snapshot() fleet.Snapshot {
	c.mu.RLock()
	// A pending watch failure expires on a timer, not an event.
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

func (c *Collector) snapshotInputLocked() SnapshotInput {
	now := time.Now()
	in := SnapshotInput{
		Now:             now,
		Org:             c.cfg.GitHubOrg,
		Sets:            lo.Values(c.sets),
		EphemeralSets:   lo.Values(c.ephemeralSets),
		Runners:         lo.Values(c.runners),
		Listeners:       lo.Values(c.listeners),
		RunnerPods:      lo.Values(c.runnerPods),
		ControllerPods:  lo.Values(c.ctrlPods),
		Usage:           c.usage,
		Queue:           c.queue,
		QueueKnown:      c.queueKnown,
		RunnersDegraded: c.runnersDegradedLocked(now),
		JobStarts:       c.jobStarts,
	}

	// A live watch failure overrides the boot probe until it expires.
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

// runnersDegradedLocked reports whether the EphemeralRunner cache may be
// missing runners. It reads only live, self-healing signals, never the boot-time
// arc-crds probe.
func (c *Collector) runnersDegradedLocked(now time.Time) bool {
	if len(c.runnerSynced) == 0 {
		return true
	}
	if !c.runnerWatchFailedAt.IsZero() && now.Sub(c.runnerWatchFailedAt) <= watchFailureTTL {
		return true
	}
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
	s.ARCVersions = slices.Clone(s.ARCVersions)
	return s
}

// listenerMetricsPort is the port name ARC gives listeners when metrics are on.
const listenerMetricsPort = "metrics"

const defaultListenerMetricsPath = "/metrics"

// ListenerTargets returns one scrape target per running listener pod that
// serves metrics. Each listener serves only its own scale set.
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
		if pod == nil || pod.Status.PodIP == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		// The scale-set labels separate listeners from the controller manager.
		set := pod.Labels[arcapi.LabelScaleSetName]
		ns := pod.Labels[arcapi.LabelScaleSetNamespace]
		if set == "" || ns == "" || pod.Labels[arcapi.LabelEphemeralRunner] == "True" {
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

	slices.SortFunc(out, func(a, b fleet.ListenerTarget) int { return strings.Compare(a.Pod, b.Pod) })
	return out
}

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

// SetUsage injects the latest pod usage, keyed "namespace/podname".
func (c *Collector) SetUsage(usage map[string]fleet.Usage) {
	c.mu.Lock()
	c.usage = usage
	c.dirty = true
	c.mu.Unlock()
	c.touch()
}

// SetQueueDepth injects queue depth per scale set. known is false when there
// is no reading at all, which must not render as zero queued.
func (c *Collector) SetQueueDepth(perSet map[string]int, known bool) {
	c.mu.Lock()
	c.queue = perSet
	c.queueKnown = known
	c.dirty = true
	c.mu.Unlock()
	c.touch()
}

// SetSource records an externally-owned source's health. Only a changed verdict
// wakes subscribers; otherwise a subscriber that reports its own failure here
// would loop forever.
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

// OnChange registers a debounced change callback. Callbacks share one
// goroutine, so they must not block.
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

func (c *Collector) touch() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

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

func (c *Collector) scope() namespaceScope {
	watch := c.cfg.Namespaces
	if len(watch) == 0 {
		watch = []string{metav1.NamespaceAll}
	}
	return namespaceScope{watch: watch, controller: c.cfg.ControllerNamespace}
}

// startCustomResourceInformers starts one informer per usable ARC resource per
// namespace. The RESTMapper gate matters: an informer on a missing CRD retries
// the 404 forever and blocks the cache sync until the deadline.
func (c *Collector) startCustomResourceInformers(ctx context.Context, scope namespaceScope, usable map[schema.GroupVersionResource]bool) []cache.InformerSynced {
	var synced []cache.InformerSynced

	for _, ns := range scope.watch {
		for _, gvr := range arcapi.AllGVRs() {
			if !usable[gvr] || gvr == arcapi.AutoscalingListenerGVR {
				continue
			}
			started := c.startDynamicInformer(ctx, gvr, ns)
			if gvr == arcapi.EphemeralRunnerGVR {
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

	// Without this a 403 is an infinite silent retry.
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

// startPodInformers watches runner pods by label, and the controller namespace
// unfiltered because listener labels differ across ARC versions.
func (c *Collector) startPodInformers(ctx context.Context, scope namespaceScope) []cache.InformerSynced {
	var synced []cache.InformerSynced

	runnerSelector := labels.Set{arcapi.LabelEphemeralRunner: "True"}.String()
	for _, ns := range scope.watch {
		synced = append(synced, c.startPodInformer(ctx, ns, runnerSelector, c.runnerPods, "runner")...)
	}
	return append(synced, c.startPodInformer(ctx, scope.controller, "", c.ctrlPods, "controller")...)
}

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

func (c *Collector) attachWatchErrorHandler(informer cache.SharedIndexInformer, source, resource, namespace string) {
	key := source + "/" + resource + "/" + namespace
	err := informer.SetWatchErrorHandlerWithContext(func(ctx context.Context, _ *cache.Reflector, werr error) {
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

// noteWatchFailure records an expiring watch error, kept apart from the probe
// results so one blip cannot leave the strip red for the life of the process.
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

// trimPod drops the env blocks and managedFields that dominate RSS on a large
// fleet and that nothing renders.
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

// trimCustomResource drops managedFields, and the pod template every
// EphemeralRunner embeds; image and resources come from the pod instead.
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

// isUnreachable separates "cannot talk to the API server" from "not allowed".
func isUnreachable(reason string) bool {
	return strings.Contains(reason, unreachablePrefix)
}
