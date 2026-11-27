package promql

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
	"arc-ui/internal/prom"
	"arc-ui/internal/web"
)

var testQueries = Queries{Schema: prom.Schema{
	ScaleSetLabel:    "name",
	KSMScaleSetLabel: "label_actions_github_com_scale_set_name",
	RunnerNamespaces: []string{"arc-runners"},
}}

// fakeQuerier answers by the first registered metric name a query mentions.
type fakeQuerier struct {
	mu      sync.Mutex
	instant map[string][]prom.Sample
	ranged  map[string][]prom.Series
	fail    string
	seen    []string
}

func (f *fakeQuerier) match(q string) (string, error) {
	f.mu.Lock()
	f.seen = append(f.seen, q)
	f.mu.Unlock()
	if f.fail != "" && strings.Contains(q, f.fail) {
		return "", errors.New("prometheus: connection refused")
	}
	for k := range f.instant {
		if strings.Contains(q, k) {
			return k, nil
		}
	}
	for k := range f.ranged {
		if strings.Contains(q, k) {
			return k, nil
		}
	}
	return "", nil
}

func (f *fakeQuerier) Query(_ context.Context, q string) ([]prom.Sample, error) {
	k, err := f.match(q)
	return f.instant[k], err
}

func (f *fakeQuerier) QueryRange(_ context.Context, q string, _ prom.Range) ([]prom.Series, error) {
	k, err := f.match(q)
	return f.ranged[k], err
}

var t0 = time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

func window() web.Window {
	return web.Window{From: t0, To: t0.Add(4 * time.Minute), Points: 4}
}

func flat(v float64) []prom.Series {
	pts := make([]prom.Point, 0, 5)
	for i := range 5 {
		pts = append(pts, prom.Point{At: t0.Add(time.Duration(i) * time.Minute), Value: v})
	}
	return []prom.Series{{Points: pts}}
}

func TestSetScopeJoinsKubeStateMetricsAndFleetScopeDoesNot(t *testing.T) {
	t.Parallel()

	fleetCPU := testQueries.CPUUsed("", time.Minute)
	setCPU := testQueries.CPUUsed("linux-x64", time.Minute)

	assert.NotContains(t, fleetCPU, "kube_pod_labels", "fleet usage must render without kube-state-metrics")
	assert.Contains(t, setCPU, `label_actions_github_com_scale_set_name="linux-x64"`)
	assert.Contains(t, setCPU, `namespace=~"arc-runners"`)
	assert.Contains(t, setCPU, "[120s]", "a rate window under two minutes misses samples at a 30s scrape")

	assert.Contains(t, testQueries.Busy("linux-x64"), `name="linux-x64"`)
	assert.Contains(t, testQueries.Pending("linux-x64"), `name="linux-x64"`, "controller series name the set in their own label")
	assert.Contains(t, testQueries.Capacity(""), "< 2147483647", "an unbounded set must not become the ceiling")
	assert.Contains(t, testQueries.Completed("", true, time.Minute), `job_result="succeeded"`)
	assert.Contains(t, testQueries.Completed("", false, time.Minute), `job_result!="succeeded"`)
}

func TestJobCountersSpanTwoScrapesAtEveryRange(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	window := regexp.MustCompile(`\[(\d+)s\]\)\) \* (\d+)$`)
	for _, tr := range web.AllRanges() {
		r := rangeOf(web.Window{From: now.Add(-tr.Duration()), To: now, Points: tr.Points()})
		for _, q := range []string{
			testQueries.Completed("", true, r.Step),
			testQueries.Started("linux-x64", r.Step),
			testQueries.AllCompleted("", r.Step),
		} {
			m := window.FindStringSubmatch(q)
			require.Len(t, m, 3, "%s: %s", tr, q)
			secs, _ := strconv.Atoi(m[1])
			assert.GreaterOrEqual(t, secs, 60, "%s: the range must hold two 30s scrapes", tr)
			assert.Equal(t, strconv.FormatInt(int64(r.Step.Seconds()), 10), m[2], "%s: scaled back to one step", tr)
		}
	}
}

func TestScopeLinesEverySeriesUpOnOneAxis(t *testing.T) {
	t.Parallel()

	gappy := []prom.Series{{Points: []prom.Point{{At: t0.Add(2 * time.Minute), Value: 3}}}}
	q := &fakeQuerier{ranged: map[string][]prom.Series{
		"gha_busy_runners":   gappy,
		"gha_idle_runners":   flat(2),
		"gha_max_runners":    flat(10),
		"container_cpu":      flat(1.5),
		"pending_ephemeral":  flat(1),
		"memory_working_set": flat(1 << 30),
	}}
	h := History{Client: q, Queries: testQueries}

	s, err := h.Scope(context.Background(), web.FleetScope, window())
	require.NoError(t, err)

	require.Len(t, s.At, 5)
	assert.Equal(t, []float64{0, 0, 3, 0, 0}, s.Busy, "a gap inside a series reads as zero on the shared axis")
	assert.Equal(t, 2.0, s.Idle[4])
	assert.Nil(t, s.CPURequest, "a metric with no series must stay nil, not draw as zeros")
}

func TestAnyFailedQueryFailsTheChart(t *testing.T) {
	t.Parallel()

	q := &fakeQuerier{ranged: map[string][]prom.Series{"gha_busy_runners": flat(1)}, fail: "gha_max_runners"}
	h := History{Client: q, Queries: testQueries}

	_, err := h.Scope(context.Background(), web.FleetScope, window())
	require.Error(t, err, "a half-drawn stack misstates the fleet")
}

func TestNoSeriesAtAllIsAnEmptyChart(t *testing.T) {
	t.Parallel()

	h := History{Client: &fakeQuerier{}, Queries: testQueries}

	c, err := h.Throughput(context.Background(), web.Set("linux-x64"), window())
	require.NoError(t, err)
	assert.Zero(t, c.Len(), "no completed-jobs series means no chart, not a row of zero bars")
}

func TestThroughputAndChurnSplitByJobResult(t *testing.T) {
	t.Parallel()

	q := &fakeQuerier{ranged: map[string][]prom.Series{
		`job_result="succeeded"`:  flat(4),
		`job_result!="succeeded"`: flat(1),
		"gha_started_jobs_total":  flat(6),
	}}
	h := History{Client: q, Queries: testQueries}

	tp, err := h.Throughput(context.Background(), web.FleetScope, window())
	require.NoError(t, err)
	ok, failed := tp.Sum()
	assert.Equal(t, 20.0, ok)
	assert.Equal(t, 5.0, failed)

	ch, err := h.Churn(context.Background(), web.FleetScope, window())
	require.NoError(t, err)
	started, _ := ch.Sum()
	assert.Equal(t, 30.0, started)
}

func TestReposRankByRunnerTimeAsOwnerSlashRepo(t *testing.T) {
	t.Parallel()

	q := &fakeQuerier{instant: map[string][]prom.Sample{
		"duration_seconds_sum": {
			{Labels: map[string]string{"organization": "WindKube", "repository": "docs"}, Value: 60},
			{Labels: map[string]string{"organization": "WindKube", "repository": "web-api"}, Value: 3600},
			{Labels: map[string]string{"organization": "WindKube", "repository": "idle"}, Value: 0},
			{Labels: map[string]string{"organization": "WindKube"}, Value: 99},
		},
		"duration_seconds_count": {
			{Labels: map[string]string{"organization": "WindKube", "repository": "web-api"}, Value: 11.6},
		},
	}}
	h := History{Client: q, Queries: testQueries}

	got, err := h.Repos(context.Background(), window(), 1)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, web.RepoHistory{Repository: "WindKube/web-api", Seconds: 3600, Jobs: 12}, got[0])
}

func TestRunnerIsOnePodsCAdvisorUsage(t *testing.T) {
	t.Parallel()

	q := &fakeQuerier{ranged: map[string][]prom.Series{
		"container_cpu_usage_seconds_total":  flat(0.25),
		"container_memory_working_set_bytes": flat(512 << 20),
	}}
	h := History{Client: q, Queries: testQueries}

	s, err := h.Runner(context.Background(), "linux-x64-abc", window())
	require.NoError(t, err)
	assert.Equal(t, 0.25, s.CPU[0])
	assert.Equal(t, float64(512<<20), s.Mem[4])
	for _, seen := range q.seen {
		assert.Contains(t, seen, `pod="linux-x64-abc"`)
	}

	_, err = History{Client: &fakeQuerier{fail: "container_"}, Queries: testQueries}.Runner(context.Background(), "x", window())
	require.Error(t, err)
}

func TestRangeIsAlignedSoCallersShareACacheEntry(t *testing.T) {
	t.Parallel()

	w := web.Window{From: t0.Add(17 * time.Second), To: t0.Add(time.Hour + 17*time.Second), Points: 60}
	r := rangeOf(w)
	assert.Equal(t, time.Minute, r.Step)
	assert.Equal(t, t0, r.Start)
}

type recordingSink struct {
	usage map[string]fleet.Usage
	queue map[string]int
	known bool
}

func (s *recordingSink) SetUsage(u map[string]fleet.Usage) { s.usage = u }
func (s *recordingSink) SetQueueDepth(q map[string]int, known bool) {
	s.queue, s.known = q, known
}

func TestLiveTickFeedsUsageAndQueue(t *testing.T) {
	t.Parallel()

	pod := map[string]string{"namespace": "arc-runners", "pod": "linux-x64-abc"}
	q := &fakeQuerier{instant: map[string][]prom.Sample{
		"container_cpu_usage_seconds_total":  {{Labels: pod, Value: 0.5}},
		"container_memory_working_set_bytes": {{Labels: pod, Value: 1 << 30}},
		"gha_assigned_jobs":                  {{Labels: map[string]string{"name": "linux-x64"}, Value: 2.4}},
	}}
	sink := &recordingSink{}
	Live{Client: q, Queries: testQueries, Sink: sink, Log: zerolog.New(io.Discard)}.Tick(context.Background(), t0)

	u := sink.usage["arc-runners/linux-x64-abc"]
	assert.Equal(t, 0.5, u.CPUCores)
	assert.Equal(t, float64(1<<30), u.MemBytes)
	assert.True(t, sink.known)
	assert.Equal(t, 2, sink.queue["linux-x64"])
}

func TestLiveTickClearsReadingsWhenPrometheusIsDown(t *testing.T) {
	t.Parallel()

	q := &fakeQuerier{fail: "container_", instant: map[string][]prom.Sample{}}
	sink := &recordingSink{usage: map[string]fleet.Usage{"stale": {CPUCores: 9}}}
	Live{Client: q, Queries: testQueries, Sink: sink, Log: zerolog.New(io.Discard)}.Tick(context.Background(), t0)

	assert.Empty(t, sink.usage, "stale usage must not outlive an outage")
	assert.False(t, sink.known, "an empty queue answer is unknown, not zero")
}
