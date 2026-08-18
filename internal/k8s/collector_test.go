package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	arcapi "arc-ui/internal/arcapi/v1alpha1"
	"arc-ui/internal/config"
	"arc-ui/internal/fleet"
)

func testCollector(kube *fake.Clientset) *Collector {
	return NewCollector(
		&Clients{Kube: kube},
		config.Config{Namespaces: []string{"arc-runners"}, ControllerNamespace: "arc-systems"},
		zerolog.New(io.Discard),
	)
}

// TestTrimPodDropsTheBigFields covers the transform that keeps RSS bounded at a
// few thousand runners.
func TestTrimPodDropsTheBigFields(t *testing.T) {
	t.Parallel()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:     "arc",
			Name:          "r1",
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}},
		},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{Name: "work"}},
			InitContainers: []corev1.Container{{
				Name:         "init",
				Env:          []corev1.EnvVar{{Name: "BIG", Value: "x"}},
				VolumeMounts: []corev1.VolumeMount{{Name: "work"}},
			}},
			Containers: []corev1.Container{{
				Name:         "runner",
				Image:        "ghcr.io/actions/runner:latest",
				Env:          []corev1.EnvVar{{Name: "ACTIONS_RUNNER_INPUT_JITCONFIG", Value: "very long"}},
				VolumeMounts: []corev1.VolumeMount{{Name: "work"}},
			}},
		},
	}

	out, err := trimPod(pod)
	require.NoError(t, err)
	trimmed, ok := out.(*corev1.Pod)
	require.True(t, ok, "trimPod returned %T, want *corev1.Pod", out)
	assert.Nil(t, trimmed.ManagedFields, "managedFields/volumes survived: %+v", trimmed.ObjectMeta)
	assert.Nil(t, trimmed.Spec.Volumes, "managedFields/volumes survived: %+v", trimmed.ObjectMeta)
	assert.Nil(t, trimmed.Spec.Containers[0].Env, "container env/mounts survived")
	assert.Nil(t, trimmed.Spec.Containers[0].VolumeMounts, "container env/mounts survived")
	assert.Nil(t, trimmed.Spec.InitContainers[0].Env, "init container env/mounts survived")
	assert.Nil(t, trimmed.Spec.InitContainers[0].VolumeMounts, "init container env/mounts survived")
	// The fields the dashboard actually renders must be untouched.
	assert.Equal(t, "ghcr.io/actions/runner:latest", trimmed.Spec.Containers[0].Image, "image was trimmed away")
}

func TestTrimPodHandlesTombstonesAndForeignObjects(t *testing.T) {
	t.Parallel()

	tombstone := cache.DeletedFinalStateUnknown{
		Key: "arc/r1",
		Obj: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:          "r1",
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubelet"}},
		}},
	}
	out, err := trimPod(tombstone)
	require.NoError(t, err)
	got, ok := out.(cache.DeletedFinalStateUnknown)
	require.True(t, ok, "trimPod unwrapped the tombstone into %T", out)
	assert.Nil(t, got.Obj.(*corev1.Pod).ManagedFields, "tombstoned pod was not trimmed")

	// A transform must never reject an object it does not recognise, or the
	// informer drops it entirely.
	_, err = trimPod(&corev1.Service{})
	require.NoError(t, err, "trimPod on a non-pod")
}

func TestSnapshotIsIsolatedFromTheCache(t *testing.T) {
	t.Parallel()

	c := testCollector(fake.NewClientset())
	c.SetSource(fleet.Source{Name: fleet.SourceMetrics, Available: false, Reason: "not installed"})

	first := c.Snapshot()
	first.Sources = append(first.Sources, fleet.Source{Name: "injected"})

	second := c.Snapshot()
	for _, s := range second.Sources {
		require.NotEqual(t, "injected", s.Name, "a caller's append leaked into the collector's cached snapshot")
	}
	src, ok := second.Source(fleet.SourceMetrics)
	require.True(t, ok, "metrics source = %+v, want the unavailable one that was set", src)
	require.False(t, src.Available, "metrics source = %+v, want the unavailable one that was set", src)
}

func TestSetQueueDepthAndUsageReachTheSnapshot(t *testing.T) {
	t.Parallel()

	c := testCollector(fake.NewClientset())
	c.SetUsage(map[string]fleet.Usage{"arc/r1": {CPUCores: 2, At: testNow}})
	c.SetQueueDepth(map[string]int{"ubuntu": 6}, true)

	c.mu.Lock()
	in := c.snapshotInputLocked()
	c.mu.Unlock()

	assert.InDelta(t, 2, in.Usage["arc/r1"].CPUCores, 1e-9, "usage did not reach the snapshot input: %+v", in.Usage)
	assert.True(t, in.QueueKnown, "queue did not reach the snapshot input: %+v", in.Queue)
	assert.Equal(t, 6, in.Queue["ubuntu"], "queue did not reach the snapshot input: %+v", in.Queue)
}

func TestOnChangeIsDebouncedAndCancellable(t *testing.T) {
	t.Parallel()

	c := testCollector(fake.NewClientset())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.runNotifier(ctx)

	var calls atomic.Int32
	fired := make(chan struct{}, 16)
	stop := c.OnChange(func() {
		calls.Add(1)
		select {
		case fired <- struct{}{}:
		default:
		}
	})

	// A burst of changes, as a scale-up produces, must collapse into one push.
	for i := 0; i < 50; i++ {
		c.touch()
	}
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		require.Fail(t, "subscriber was never called")
	}
	fires := calls.Load()
	require.LessOrEqual(t, fires, int32(2), "50 changes fired %d callbacks, want them debounced", fires)

	stop()
	before := calls.Load()
	for i := 0; i < 10; i++ {
		c.touch()
	}
	time.Sleep(3 * changeDebounce)
	after := calls.Load()
	require.Equal(t, before, after, "cancelled subscriber still fired: %d -> %d", before, after)
}

func TestEventsForPodFiltersOnUIDAndCaches(t *testing.T) {
	t.Parallel()

	const (
		ns  = "arc-runners"
		pod = "ubuntu-runner-1"
		uid = types.UID("pod-uid-1")
	)
	older := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: "e1"},
		Type:           "Normal",
		Reason:         "Scheduled",
		Message:        "Successfully assigned",
		InvolvedObject: corev1.ObjectReference{Namespace: ns, Name: pod, UID: uid},
		FirstTimestamp: metav1.Time{Time: testNow.Add(-2 * time.Minute)},
	}
	newer := &corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: "e2"},
		Type:           "Warning",
		Reason:         "BackOff",
		Message:        "Back-off pulling image",
		Count:          4,
		InvolvedObject: corev1.ObjectReference{Namespace: ns, Name: pod, UID: uid},
		LastTimestamp:  metav1.Time{Time: testNow.Add(-30 * time.Second)},
	}

	kube := fake.NewClientset(older, newer)
	c := testCollector(kube)

	events, err := c.EventsForPod(context.Background(), ns, pod, uid)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "BackOff", events[0].Reason, "events are not newest-first: %+v", events)
	assert.True(t, events[0].Warning(), "warning event = %+v", events[0])
	assert.Equal(t, int32(4), events[0].Count, "warning event = %+v", events[0])
	// A modern emitter leaves lastTimestamp zero; falling back to the other two
	// stamps is what keeps those events from sorting to the bottom.
	assert.False(t, events[1].At.IsZero(), "firstTimestamp fallback was not applied: %+v", events[1])

	// The uid has to be in the selector: ARC reuses runner pod names across
	// generations, so name alone surfaces a dead pod's events.
	var listActions int
	for _, a := range kube.Actions() {
		la, ok := a.(k8stesting.ListActionImpl)
		if !ok || la.GetResource().Resource != "events" {
			continue
		}
		listActions++
		fieldSelector := la.GetListRestrictions().Fields.String()
		assert.Contains(t, fieldSelector, "involvedObject.uid=pod-uid-1", "field selector does not pin the uid")
		assert.Contains(t, fieldSelector, "involvedObject.name="+pod, "field selector does not pin the name")
	}
	require.Equal(t, 1, listActions, "issued %d list calls, want 1", listActions)

	// The detail page re-renders on every SSE push; the cache is what keeps
	// that from hammering the busiest collection in the cluster.
	_, err = c.EventsForPod(context.Background(), ns, pod, uid)
	require.NoError(t, err, "cached EventsForPod")
	listActions = 0
	for _, a := range kube.Actions() {
		if la, ok := a.(k8stesting.ListActionImpl); ok && la.GetResource().Resource == "events" {
			listActions++
		}
	}
	require.Equal(t, 1, listActions, "second call issued another list: %d total", listActions)
}

// pagedEventServer serves all in pages of the requested Limit, honouring the
// continue token exactly as the API server does: page one is the oldest events,
// because event names embed the emission time and etcd hands back key order.
//
// It reports the largest page it served and how many requests it answered,
// which is what pins the bound the page size actually buys.
type pagedEventServer struct {
	all []corev1.Event

	mu       sync.Mutex
	requests int
	largest  int
}

func (s *pagedEventServer) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := strconv.Atoi(q.Get("limit"))
	if err != nil || limit <= 0 {
		limit = len(s.all)
	}
	from, err := strconv.Atoi(q.Get("continue"))
	if err != nil || from < 0 || from > len(s.all) {
		from = 0
	}
	to := min(from+limit, len(s.all))

	list := &corev1.EventList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "EventList"},
		Items:    s.all[from:to],
	}
	if to < len(s.all) {
		remaining := int64(len(s.all) - to)
		list.Continue = strconv.Itoa(to)
		list.RemainingItemCount = &remaining
	}

	s.mu.Lock()
	s.requests++
	s.largest = max(s.largest, len(list.Items))
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

func (s *pagedEventServer) stats() (requests, largest int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.largest
}

func eventCollector(t *testing.T, handler http.HandlerFunc) *Collector {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	kube, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
	require.NoError(t, err)
	return NewCollector(
		&Clients{Kube: kube},
		config.Config{Namespaces: []string{"arc-runners"}, ControllerNamespace: "arc-systems"},
		zerolog.New(io.Discard),
	)
}

// TestEventsForPodShowsTheNewestEvents covers the difference between paging and
// picking: ListOptions.Limit returns the FIRST page in the API server's key
// order, which for a pod with more history than one page is the OLDEST events.
// Following the continue token is the only thing that makes the newest win.
//
// The fake clientset ignores Limit and Continue entirely, so paging has to be
// reproduced against a real REST client talking to a handler that implements it.
func TestEventsForPodShowsTheNewestEvents(t *testing.T) {
	t.Parallel()

	const (
		ns  = "arc-runners"
		pod = "ubuntu-runner-1"
		uid = types.UID("pod-uid-1")
	)
	// More than one page, so a single Limited list cannot see the newest event.
	total := 2*eventPageSize + 200

	all := make([]corev1.Event, 0, total)
	for i := range total {
		all = append(all, corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("%s.%04d", pod, i)},
			InvolvedObject: corev1.ObjectReference{Namespace: ns, Name: pod, UID: uid},
			Type:           "Warning",
			Reason:         fmt.Sprintf("Event%04d", i),
			LastTimestamp:  metav1.Time{Time: testNow.Add(time.Duration(i-total) * time.Minute)},
		})
	}

	srv := &pagedEventServer{all: all}
	c := eventCollector(t, srv.handle)

	events, err := c.EventsForPod(context.Background(), ns, pod, uid)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	// The panel's whole job is recent activity, so the most recent event has to
	// be in it — and at the top.
	assert.Equal(t, fmt.Sprintf("Event%04d", total-1), events[0].Reason,
		"newest event missing; got the %s..%s window", events[len(events)-1].Reason, events[0].Reason)
	require.LessOrEqual(t, len(events), maxEventsPerPod, "panel got %d events, want it capped", len(events))
	assert.Equal(t, fmt.Sprintf("Event%04d", total-len(events)), events[len(events)-1].Reason,
		"the events shown are not the newest %d", len(events))

	// Each individual response still has to be bounded — that, and nothing
	// else, is what the page size buys.
	requests, largest := srv.stats()
	require.NotZero(t, requests, "handler was never asked for events")
	assert.LessOrEqual(t, largest, eventPageSize, "a single response returned %d events, want it paged", largest)
	assert.Equal(t, 3, requests, "%d events at %d per page should take 3 requests, took %d", total, eventPageSize, requests)
}

// TestEventsForPodStopsPagingAtTheBudget pins the other half of the bound. Now
// that the continue token is followed, a server that keeps handing out tokens —
// a pathological emitter, or a pod whose history simply never ends — must not
// turn one panel render into an unbounded walk over the busiest collection in
// the cluster.
func TestEventsForPodStopsPagingAtTheBudget(t *testing.T) {
	t.Parallel()

	const (
		ns  = "arc-runners"
		pod = "ubuntu-runner-1"
		uid = types.UID("pod-uid-1")
	)

	var requests atomic.Int64
	c := eventCollector(t, func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// Always one more page, and every page newer than the last.
		_ = json.NewEncoder(w).Encode(&corev1.EventList{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "EventList"},
			ListMeta: metav1.ListMeta{Continue: strconv.FormatInt(n, 10)},
			Items: []corev1.Event{{
				ObjectMeta:     metav1.ObjectMeta{Namespace: ns, Name: fmt.Sprintf("%s.%04d", pod, n)},
				InvolvedObject: corev1.ObjectReference{Namespace: ns, Name: pod, UID: uid},
				Reason:         fmt.Sprintf("Event%04d", n),
				LastTimestamp:  metav1.Time{Time: testNow.Add(time.Duration(n) * time.Second)},
			}},
		})
	})

	events, err := c.EventsForPod(context.Background(), ns, pod, uid)
	require.NoError(t, err)
	assert.Equal(t, int64(eventPageBudget), requests.Load(), "paging did not stop at the page budget")
	require.Len(t, events, eventPageBudget, "one event per page, all of them inside maxEventsPerPod")
	assert.Equal(t, fmt.Sprintf("Event%04d", eventPageBudget), events[0].Reason,
		"the newest of the pages actually read is not at the top")
}

func TestConvertEventTakesTheNewestTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		event corev1.Event
		want  time.Time
	}{
		{
			name: "a newer eventTime beats a stale lastTimestamp",
			event: corev1.Event{
				FirstTimestamp: metav1.Time{Time: testNow.Add(-10 * time.Minute)},
				LastTimestamp:  metav1.Time{Time: testNow.Add(-5 * time.Minute)},
				EventTime:      metav1.MicroTime{Time: testNow},
			},
			want: testNow,
		},
		{
			name: "the kubelet's lastTimestamp still wins when it is the newest",
			event: corev1.Event{
				FirstTimestamp: metav1.Time{Time: testNow.Add(-10 * time.Minute)},
				LastTimestamp:  metav1.Time{Time: testNow},
			},
			want: testNow,
		},
		{
			name: "a modern emitter sets only eventTime",
			event: corev1.Event{
				EventTime: metav1.MicroTime{Time: testNow},
			},
			want: testNow,
		},
		{
			name: "firstTimestamp is the last resort",
			event: corev1.Event{
				FirstTimestamp: metav1.Time{Time: testNow},
			},
			want: testNow,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := convertEvent(&tc.event)
			assert.WithinDuration(t, tc.want, got.At, 0, "convertEvent(%+v).At", tc.event)
			assert.Equal(t, int32(1), got.Count, "an uncounted event should read as one occurrence")
		})
	}
}

func TestEventCacheExpires(t *testing.T) {
	t.Parallel()

	c := newEventCache(10 * time.Second)
	c.put("k", []fleet.Event{{Reason: "Started"}}, testNow)

	_, ok := c.get("k", testNow.Add(5*time.Second))
	require.True(t, ok, "entry expired early")
	_, ok = c.get("k", testNow.Add(11*time.Second))
	require.False(t, ok, "entry outlived its ttl")

	// Writing sweeps the expired keys, so the map does not grow forever across
	// the endless stream of ephemeral runner names.
	c.put("other", nil, testNow.Add(time.Minute))
	require.NotContains(t, c.entries, "k", "expired entry was not swept on write")
}

func TestSortSourcesIsStable(t *testing.T) {
	t.Parallel()

	sources := []fleet.Source{
		{Name: fleet.SourceStore},
		{Name: "unknown-source"},
		{Name: fleet.SourceMetrics},
		{Name: fleet.SourceKubernetes},
		{Name: fleet.SourceARCCRDs},
	}
	sortSources(sources)

	want := []string{
		fleet.SourceKubernetes,
		fleet.SourceARCCRDs,
		fleet.SourceMetrics,
		fleet.SourceStore,
		"unknown-source",
	}
	for i, name := range want {
		require.Equal(t, name, sources[i].Name, "sources[%d] is out of order (full: %+v)", i, sources)
	}
}

func TestIsControllerImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		container string
		image     string
		want      bool
	}{
		{name: "modern controller image", container: "manager", image: "ghcr.io/actions/gha-runner-scale-set-controller:0.14.2", want: true},
		{name: "legacy controller image", container: "x", image: "summerwind/actions-runner-controller:v0.27.6", want: true},
		{name: "helm container name alone", container: "manager", image: "ghcr.io/acme/something:1", want: true},
		{name: "unrelated workload", container: "app", image: "nginx:1.27", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isControllerImage(tc.container, tc.image),
				"isControllerImage(%q, %q)", tc.container, tc.image)
		})
	}
}

func TestWatchFailureOverridesTheProbeAndThenExpires(t *testing.T) {
	t.Parallel()

	c := testCollector(fake.NewClientset())
	c.SetSource(fleet.Source{Name: fleet.SourceARCCRDs, Available: true})

	c.noteWatchFailure(fleet.SourceARCCRDs, arcapi.EphemeralRunnerGVR.Resource,
		"watch ephemeralrunners in arc failed: forbidden")

	src, ok := c.Snapshot().Source(fleet.SourceARCCRDs)
	require.True(t, ok, "source = %+v, want the watch failure to win over the probe", src)
	require.False(t, src.Available, "source = %+v, want the watch failure to win over the probe", src)
	assert.Contains(t, src.Reason, "forbidden", "reason should be the watch error")

	// One blip must not paint the strip red for the rest of the process's
	// life: the reflector retries every second, so silence means recovery.
	c.mu.Lock()
	failure := c.watchFailures[fleet.SourceARCCRDs]
	failure.CheckedAt = time.Now().Add(-2 * watchFailureTTL)
	c.watchFailures[fleet.SourceARCCRDs] = failure
	c.mu.Unlock()

	src, ok = c.Snapshot().Source(fleet.SourceARCCRDs)
	require.True(t, ok, "source = %+v, want the probe result back once the failure aged out", src)
	require.True(t, src.Available, "source = %+v, want the probe result back once the failure aged out", src)
	c.mu.Lock()
	remaining := len(c.watchFailures)
	c.mu.Unlock()
	require.Zero(t, remaining, "expired failure was not swept: %d left", remaining)
}

// TestRunnersDegradedTracksTheRunnerInformerOnly pins the signal the job-start
// sweep is gated on. It has to answer "is the EphemeralRunner cache complete
// right now", which the arc-crds source cannot: that verdict covers four
// resources and is probed once, at boot.
func TestRunnersDegradedTracksTheRunnerInformerOnly(t *testing.T) {
	t.Parallel()

	c := testCollector(fake.NewClientset())
	degraded := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.runnersDegradedLocked(time.Now())
	}

	// No informer running at all — CRD absent, or list/watch denied.
	assert.True(t, degraded(), "an empty runner list with no informer behind it was treated as trustworthy")

	var synced atomic.Bool
	c.mu.Lock()
	c.runnerSynced = []cache.InformerSynced{synced.Load}
	c.mu.Unlock()
	assert.True(t, degraded(), "an informer that has not finished its initial LIST was treated as trustworthy")

	synced.Store(true)
	require.False(t, degraded(), "a synced informer with no watch errors is the one state that allows the sweep")

	// A watch error on a different ARC resource marks arc-crds unavailable but
	// says nothing about the runner cache, which is exactly the conflation the
	// aggregate source caused.
	c.noteWatchFailure(fleet.SourceARCCRDs, arcapi.AutoscalingListenerGVR.Resource,
		"watch autoscalinglisteners in arc-systems failed: forbidden")
	src, ok := c.Snapshot().Source(fleet.SourceARCCRDs)
	require.True(t, ok, "the listener watch failure never reached the source strip")
	require.False(t, src.Available, "the listener watch failure never reached the source strip")
	assert.False(t, degraded(), "a listener watch error blinded the runner view")

	// A watch error on the runners themselves does degrade it...
	c.noteWatchFailure(fleet.SourceARCCRDs, arcapi.EphemeralRunnerGVR.Resource,
		"watch ephemeralrunners in arc-runners failed: forbidden")
	assert.True(t, degraded(), "a runner watch error left the runner view trusted")

	// ...and heals on its own, so one blip cannot pin the tracker for the life
	// of the process.
	c.mu.Lock()
	c.runnerWatchFailedAt = time.Now().Add(-2 * watchFailureTTL)
	c.mu.Unlock()
	assert.False(t, degraded(), "an aged-out watch error still blinded the runner view")
}

func TestTrimCustomResource(t *testing.T) {
	t.Parallel()

	newRunner := func() *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": arcapi.GroupVersion.String(),
			"kind":       "EphemeralRunner",
			"metadata": map[string]any{
				"name":          "r1",
				"namespace":     "arc",
				"managedFields": []any{map[string]any{"manager": "controller"}},
			},
			"spec":   map[string]any{"githubConfigUrl": "https://github.com/acme"},
			"status": map[string]any{"jobId": "job-1", "phase": "Running"},
		}}
	}

	out, err := trimCustomResource(arcapi.EphemeralRunnerGVR)(newRunner())
	require.NoError(t, err)
	trimmed := out.(*unstructured.Unstructured)
	assert.NotContains(t, trimmed.Object, "spec", "EphemeralRunner spec survived; it is the largest field in the cache and is never read")
	assert.Nil(t, trimmed.GetManagedFields(), "managedFields survived")
	// The status is the entire point of watching these.
	require.Contains(t, trimmed.Object, "status", "status was trimmed away")

	// A scale set's spec carries the pod template the config panel renders and
	// must survive.
	ars := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "ubuntu", "managedFields": []any{map[string]any{"manager": "helm"}}},
		"spec":     map[string]any{"maxRunners": int64(10)},
	}}
	out, err = trimCustomResource(arcapi.AutoscalingRunnerSetGVR)(ars)
	require.NoError(t, err)
	kept := out.(*unstructured.Unstructured)
	assert.Contains(t, kept.Object, "spec", "AutoscalingRunnerSet spec was trimmed; the config panel renders it")
	assert.Nil(t, kept.GetManagedFields(), "managedFields survived on the scale set")
}

// TestRepeatingAnUnchangedSourceStaysQuiet guards against a feedback loop that
// a broken history store would otherwise sustain on its own. main.go reports a
// failed write by calling SetSource, so if the collector treated every repeat
// as news it would hand the failure straight back to the recorder that caused
// it: SetSource -> dirty -> touch -> notify -> record -> fail -> SetSource,
// one lap per debounce window, for as long as the store stayed broken.
func TestRepeatingAnUnchangedSourceStaysQuiet(t *testing.T) {
	t.Parallel()

	c := testCollector(fake.NewClientset())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.runNotifier(ctx)

	fired := make(chan struct{}, 64)
	stop := c.OnChange(func() {
		select {
		case fired <- struct{}{}:
		default:
		}
	})
	defer stop()

	down := fleet.Source{Name: fleet.SourceStore, Available: false, Reason: "writes failing"}

	// The transition itself is news: the health strip has to go red.
	c.SetSource(down)
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		require.Fail(t, "the first failure was never announced")
	}
	time.Sleep(3 * changeDebounce)
	for len(fired) > 0 {
		<-fired
	}

	// Re-reporting the same verdict is not.
	for i := 0; i < 10; i++ {
		c.SetSource(down)
	}
	time.Sleep(3 * changeDebounce)
	assert.Empty(t, fired, "an unchanged source repeat woke the notifier")

	// A genuine change still has to get through, or the strip would never
	// recover once it had gone red.
	c.SetSource(fleet.Source{Name: fleet.SourceStore, Available: true})
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		require.Fail(t, "recovery was never announced")
	}
}
