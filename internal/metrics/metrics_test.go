package metrics

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"arc-ui/internal/fleet"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

// epsilon is the tolerance for the core and byte comparisons below. The
// conversions under test are exact, so this only absorbs float representation
// noise, never a real rounding bug.
const epsilon = 1e-9

// recorder is a Sink that remembers what the poller pushed.
type recorder struct {
	mu      sync.Mutex
	usage   map[string]fleet.Usage
	usageN  int
	sources []fleet.Source
}

func (r *recorder) SetUsage(u map[string]fleet.Usage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.usage = u
	r.usageN++
}

func (r *recorder) SetSource(s fleet.Source) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources = append(r.sources, s)
}

func (r *recorder) last() (fleet.Source, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sources) == 0 {
		return fleet.Source{}, false
	}
	return r.sources[len(r.sources)-1], true
}

func testLogger() zerolog.Logger { return zerolog.New(io.Discard) }

func podMetrics(ns, name string, at time.Time, containers ...metricsv1beta1.ContainerMetrics) *metricsv1beta1.PodMetrics {
	return &metricsv1beta1.PodMetrics{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"actions-ephemeral-runner": "True"},
		},
		Timestamp:  metav1.NewTime(at),
		Window:     metav1.Duration{Duration: 15 * time.Second},
		Containers: containers,
	}
}

// newClient seeds a fake clientset with pod metrics.
//
// The objects go in through Tracker().Create with an explicit GVR rather than
// NewSimpleClientset(objs...): the tracker's automatic kind-to-resource guess
// turns PodMetrics into "podmetricses", while the generated client lists
// "pods" (metrics.k8s.io serves pod metrics under that name). Seeded the easy
// way, every List comes back empty and the tests pass for the wrong reason.
func newClient(t *testing.T, objs ...*metricsv1beta1.PodMetrics) *metricsfake.Clientset {
	t.Helper()

	client := metricsfake.NewSimpleClientset()
	gvr := metricsv1beta1.SchemeGroupVersion.WithResource("pods")
	for _, o := range objs {
		require.NoError(t, client.Tracker().Create(gvr, o, o.Namespace), "seeding %s/%s", o.Namespace, o.Name)
	}
	return client
}

func container(name, cpu, mem string) metricsv1beta1.ContainerMetrics {
	return metricsv1beta1.ContainerMetrics{
		Name: name,
		Usage: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(mem),
		},
	}
}

// TestUsageCPUConversion is the load-bearing test of this package: Quantity's
// Value() rounds 250m up to a whole core, which would inflate every reading.
func TestUsageCPUConversion(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		cpu       string
		mem       string
		wantCores float64
		wantBytes float64
	}{
		{name: "quarter core does not round up to one", cpu: "250m", mem: "128Mi", wantCores: 0.25, wantBytes: 128 * 1024 * 1024},
		{name: "single milli", cpu: "1m", mem: "1Ki", wantCores: 0.001, wantBytes: 1024},
		{name: "sub milli rounds up to one milli", cpu: "500u", mem: "0", wantCores: 0.001},
		{name: "whole cores", cpu: "2", mem: "1Gi", wantCores: 2, wantBytes: 1024 * 1024 * 1024},
		{name: "nano suffix", cpu: "1500000000n", mem: "0", wantCores: 1.5},
		{name: "zero", cpu: "0", mem: "0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := usageOf([]metricsv1beta1.ContainerMetrics{container("runner", tc.cpu, tc.mem)}, at)
			assert.InDelta(t, tc.wantCores, got.CPUCores, epsilon, "CPUCores")
			assert.InDelta(t, tc.wantBytes, got.MemBytes, epsilon, "MemBytes")
			assert.True(t, got.At.Equal(at), "At = %v, want %v", got.At, at)
		})
	}
}

func TestUsageSumsContainers(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	got := usageOf([]metricsv1beta1.ContainerMetrics{
		container("runner", "250m", "512Mi"),
		container("dind", "1250m", "1Gi"),
		// A sidecar reporting only memory must not break the CPU sum.
		{Name: "sidecar", Usage: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")}},
	}, at)

	assert.InDelta(t, 1.5, got.CPUCores, epsilon, "CPUCores")
	assert.InDelta(t, float64(512+1024+64)*1024*1024, got.MemBytes, epsilon, "MemBytes")
}

// TestUsageTimestampFromScrape guards against using time.Now: the UI renders
// staleness off this field.
func TestUsageTimestampFromScrape(t *testing.T) {
	t.Parallel()

	scraped := time.Now().Add(-90 * time.Second).Truncate(time.Second)
	got := usageOf([]metricsv1beta1.ContainerMetrics{container("runner", "100m", "1Mi")}, scraped)
	// fleet.Resources reads a zero At as "never scraped" and a wall-clock At
	// as "fresh", so this field has to be metrics-server's own timestamp.
	require.True(t, got.At.Equal(scraped), "At = %v, want the scrape time %v", got.At, scraped)
}

func TestPollerScrapeKeysAndSelector(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	client := newClient(t,
		podMetrics("arc-runners", "runner-abc", at, container("runner", "250m", "512Mi")),
		podMetrics("arc-runners", "runner-def", at, container("runner", "2", "1Gi")),
		podMetrics("other-ns", "runner-ghi", at, container("runner", "1", "1Gi")),
	)

	var gotSelector string
	client.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		gotSelector = action.(k8stesting.ListAction).GetListRestrictions().Labels.String()
		return false, nil, nil
	})

	rec := &recorder{}
	p := NewPoller(client, []string{"arc-runners"}, time.Minute, testLogger(), rec)

	usage, err := p.scrape(context.Background())
	require.NoError(t, err, "scrape")
	assert.Equal(t, "actions-ephemeral-runner=True", gotSelector, "label selector")
	require.Len(t, usage, 2, "other-ns must not be listed, got %v", usage)
	require.Contains(t, usage, "arc-runners/runner-abc", "missing key, got %v", usage)
	assert.InDelta(t, 0.25, usage["arc-runners/runner-abc"].CPUCores, epsilon, "CPUCores")
}

func TestPollerAllNamespaces(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	client := newClient(t,
		podMetrics("arc-runners", "runner-abc", at, container("runner", "250m", "512Mi")),
		podMetrics("other-ns", "runner-ghi", at, container("runner", "1", "1Gi")),
	)

	rec := &recorder{}
	p := NewPoller(client, nil, time.Minute, testLogger(), rec)

	usage, err := p.scrape(context.Background())
	require.NoError(t, err, "scrape")
	require.Len(t, usage, 2, "empty namespace list must scan every namespace, got %v", usage)
}

// TestPollerMissingPodIsNotUnhealthy is the "brand new pod has no metrics yet"
// case: a runner with no PodMetrics is simply absent, and the source stays
// available.
func TestPollerMissingPodIsNotUnhealthy(t *testing.T) {
	t.Parallel()

	client := newClient(t) // no PodMetrics at all
	rec := &recorder{}
	p := NewPoller(client, []string{"arc-runners"}, time.Minute, testLogger(), rec)

	p.tick(context.Background())

	src, ok := rec.last()
	require.True(t, ok, "no source reported")
	require.True(t, src.Available, "source marked unavailable for an empty result: %q", src.Reason)
	assert.Equal(t, fleet.SourceMetrics, src.Name, "source name")
	assert.False(t, src.CheckedAt.IsZero(), "CheckedAt is zero")
	assert.Equal(t, 1, rec.usageN, "want one empty usage push, got %d pushes", rec.usageN)
	assert.Empty(t, rec.usage, "want one empty usage push, got %v", rec.usage)
}

// TestPollerPartialListStaysHealthy covers metrics-server having scraped some
// runners but not others: the unscraped ones are absent, and that is healthy.
func TestPollerPartialListStaysHealthy(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	client := newClient(t,
		podMetrics("arc-runners", "runner-scraped", at, container("runner", "250m", "512Mi")),
	)
	rec := &recorder{}
	p := NewPoller(client, []string{"arc-runners"}, time.Minute, testLogger(), rec)

	p.tick(context.Background())

	src, _ := rec.last()
	require.True(t, src.Available, "source unavailable: %q", src.Reason)
	assert.NotContains(t, rec.usage, "arc-runners/runner-never-scraped", "an unscraped runner must be absent, not zero")
	assert.Contains(t, rec.usage, "arc-runners/runner-scraped", "scraped runner missing from usage")
}

func TestPollerListErrorMarksUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantSub string
	}{
		{
			name:    "rbac forbidden",
			err:     apierrors.NewForbidden(schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, "", apierrors.NewForbidden(schema.GroupResource{}, "", nil)),
			wantSub: "forbidden",
		},
		{
			name:    "api not registered",
			err:     apierrors.NewNotFound(schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, ""),
			wantSub: "not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newClient(t)
			client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})

			rec := &recorder{}
			p := NewPoller(client, []string{"arc-runners"}, time.Minute, testLogger(), rec)
			p.tick(context.Background())

			src, ok := rec.last()
			require.True(t, ok, "no source reported")
			require.False(t, src.Available, "source available despite a failing List")
			assert.Contains(t, strings.ToLower(src.Reason), tc.wantSub, "reason = %q, want it to mention %q", src.Reason, tc.wantSub)
			assert.Contains(t, src.Reason, "arc-runners", "reason = %q, want it to name the namespace", src.Reason)
			// A total failure must not blank the previously known usage.
			assert.Zero(t, rec.usageN, "usage pushed %d times on a total failure, want 0", rec.usageN)
		})
	}
}

// TestPollerPartialNamespaceFailure: one namespace failing must not discard
// the readings from the ones that worked.
func TestPollerPartialNamespaceFailure(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	client := newClient(t,
		podMetrics("good-ns", "runner-abc", at, container("runner", "250m", "512Mi")),
	)
	client.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() == "bad-ns" {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "", nil)
		}
		return false, nil, nil
	})

	rec := &recorder{}
	p := NewPoller(client, []string{"good-ns", "bad-ns"}, time.Minute, testLogger(), rec)
	p.tick(context.Background())

	src, _ := rec.last()
	assert.False(t, src.Available, "source should be degraded when one namespace fails")
	require.Equal(t, 1, rec.usageN, "usage pushed %d times, want 1 (partial results are still useful)", rec.usageN)
	assert.Contains(t, rec.usage, "good-ns/runner-abc", "healthy namespace's readings were discarded: %v", rec.usage)
}

func TestPollerRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	client := newClient(t)
	rec := &recorder{}
	p := NewPoller(client, []string{"arc-runners"}, time.Hour, testLogger(), rec)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Wait for the immediate first scrape before cancelling. Asserting on it
	// after an unconditional cancel races Run: if the cancellation is observed
	// first, nothing is ever pushed and the assertion fails for a reason that
	// has nothing to do with what it is testing.
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.usageN > 0
	}, 5*time.Second, time.Millisecond,
		"no scrape before the first tick; the dashboard would be blank for a whole interval")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "Run must return nil on clean shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestNewPollerDefaultsInterval(t *testing.T) {
	t.Parallel()

	for _, in := range []time.Duration{0, -time.Second} {
		p := NewPoller(newClient(t), nil, in, testLogger(), &recorder{})
		assert.Equal(t, defaultInterval, p.interval, "interval for %v = %v, want %v (a non-positive ticker panics)", in, p.interval, defaultInterval)
	}
}

func TestPollerNilClient(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	p := NewPoller(nil, nil, time.Minute, testLogger(), rec)
	require.NoError(t, p.Run(context.Background()), "Run with no client must return nil")
	src, ok := rec.last()
	require.True(t, ok, "no source reported, want an unavailable one")
	require.False(t, src.Available, "want an unavailable source, got %+v", src)
}

func TestKey(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "arc-runners/runner-1", Key("arc-runners", "runner-1"), "Key")
}

func TestHealthTrackerLogsTransitionsOnly(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := zerolog.New(&buf).Level(zerolog.InfoLevel)

	var h healthTracker
	h.fail(log, "boom")
	h.fail(log, "boom")
	h.fail(log, "boom")
	assert.Equal(t, 1, strings.Count(buf.String(), "source unavailable"), "identical failures must be logged once above debug")

	h.fail(log, "different boom")
	assert.Equal(t, 2, strings.Count(buf.String(), "source unavailable"), "a changed failure mode must be logged")

	h.ok(log)
	h.ok(log)
	assert.Equal(t, 1, strings.Count(buf.String(), "source recovered"), "recovery must be logged once")
}
