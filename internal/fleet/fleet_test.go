package fleet

import (
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var now = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

func runner(name, set string, state State, repo, wf, job string) Runner {
	r := Runner{Name: name, SetName: set, State: state, CreatedAt: now.Add(-time.Hour)}
	if repo != "" {
		r.Job = Job{Repository: repo, Workflow: wf, Name: job, StartedAt: now.Add(-5 * time.Minute)}
	}
	return r
}

func sampleSnapshot() Snapshot {
	return Snapshot{
		At: now,
		Sets: []RunnerSet{
			{Name: "arc-ubuntu", MaxRunners: 30, MinRunners: 4, CPURequest: 2, MemRequest: 4 * GiB, Current: 3},
			{Name: "arc-arm64", MaxRunners: 10, CPURequest: 4, MemRequest: 8 * GiB, Current: 1},
		},
		Runners: []Runner{
			runner("u-1", "arc-ubuntu", StateBusy, "WindKube/web-api", "ci.yml", "unit-tests"),
			runner("u-2", "arc-ubuntu", StateIdle, "", "", ""),
			runner("u-3", "arc-ubuntu", StateBusy, "WindKube/payments", "e2e.yml", "playwright"),
			runner("a-1", "arc-arm64", StatePending, "", "", ""),
		},
	}
}

func TestFilterMatchesAcrossDimensions(t *testing.T) {
	t.Parallel()
	s := sampleSnapshot()

	cases := []struct {
		name string
		f    Filter
		want int
	}{
		{"empty filter matches all", Filter{}, 4},
		{"all sentinel matches all", Filter{Repo: AnyValue, State: AnyValue}, 4},
		{"by repo", Filter{Repo: "WindKube/web-api"}, 1},
		{"by state", Filter{State: string(StateBusy)}, 2},
		{"by set", Filter{Set: "arc-arm64"}, 1},
		{"by workflow", Filter{Workflow: "e2e.yml"}, 1},
		{"by job", Filter{Job: "unit-tests"}, 1},
		{"combined", Filter{Set: "arc-ubuntu", State: string(StateBusy)}, 2},
		{"contradictory", Filter{Set: "arc-arm64", State: string(StateBusy)}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, _ := tc.f.Apply(s)
			assert.Len(t, got, tc.want)
		})
	}
}

func TestFilterActiveIgnoresSentinels(t *testing.T) {
	t.Parallel()

	assert.Zero(t, Filter{Repo: AnyValue, State: ""}.Active(),
		"sentinels should not count as active filters")
	assert.Equal(t, 2, Filter{Repo: "x", State: "busy"}.Active())
}

func TestFilterSummaryWording(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "4 runners · 2 runnersets · no filters",
		Filter{}.Summary(4, 4, 2))
	assert.Equal(t, "1 of 4 runners match 1 filter",
		Filter{Repo: "x"}.Summary(1, 4, 2))
	assert.Equal(t, "1 of 4 runners match 2 filters",
		Filter{Repo: "x", State: "busy"}.Summary(1, 4, 2))
}

func TestSelectsOfferOnlyPresentValues(t *testing.T) {
	t.Parallel()

	byKey := lo.KeyBy(Filter{}.Selects(sampleSnapshot()), func(s Select) string { return s.Key })

	// Two busy runners on two repos, plus the "all repositories" entry.
	assert.Len(t, byKey["repo"].Options, 3)

	// Idle runners contribute no repository, so "" must never become an option.
	for _, o := range byKey["repo"].Options {
		assert.NotEmpty(t, o.Value, "an empty repository leaked into the select")
	}
}

func TestSelectsKeepAStaleSelection(t *testing.T) {
	t.Parallel()

	// A repo present in the URL but absent from the fleet (its runners just
	// finished) must stay selected, or the page silently shows a different
	// filter than the address bar claims.
	sel := Filter{Repo: "WindKube/gone"}.Selects(sampleSnapshot())

	found := lo.ContainsBy(sel[0].Options, func(o Option) bool {
		return o.Value == "WindKube/gone" && o.Selected
	})
	assert.True(t, found, "stale selection was dropped from the options")
}

func TestAggregateCountsAndScalesRequests(t *testing.T) {
	t.Parallel()

	runners, sets := Filter{}.Apply(sampleSnapshot())
	got := Aggregate(runners, sets)

	assert.Equal(t, 4, got.Runners)
	assert.Equal(t, 2, got.Busy)
	assert.Equal(t, 1, got.Idle)
	assert.Equal(t, 1, got.Pending)
	assert.Equal(t, 40, got.Capacity)
	// 3 ubuntu runners at 2 cores + 1 arm runner at 4 cores.
	assert.InDelta(t, 10.0, got.CPU.Request, 0.001)
}

func TestAggregateUnboundedSuppressesCapacity(t *testing.T) {
	t.Parallel()

	got := Aggregate(nil, []RunnerSet{{Name: "a", Unbounded: true}, {Name: "b", MaxRunners: 10}})

	assert.True(t, got.Unbounded, "an unbounded set must mark the total unbounded")
	assert.Equal(t, 10, got.Capacity, "bounded sets should still contribute")
}

func TestAggregateQueuedUnknownWhenListenerSilent(t *testing.T) {
	t.Parallel()

	// No set reports queue depth: the tile must read as unknown, not zero.
	got := Aggregate(nil, []RunnerSet{{Name: "a"}})
	assert.False(t, got.QueuedKnown,
		"queued must be unknown when no listener metrics are available")

	got = Aggregate(nil, []RunnerSet{{Name: "a", Queued: 3, QueuedKnown: true}})
	assert.True(t, got.QueuedKnown)
	assert.Equal(t, 3, got.Queued)
}

func TestAggregateTracksPartialMetricsCoverage(t *testing.T) {
	t.Parallel()

	// Ephemeral runners routinely die before metrics-server first scrapes
	// them; that must be visible rather than silently understating usage.
	got := Aggregate([]Runner{
		{Name: "a", CPU: Resources{Used: 1, At: now}},
		{Name: "b"},
	}, nil)

	assert.Equal(t, 1, got.MetricsCovered)
	assert.False(t, got.MetricsComplete())
}

func TestSortRunnersPutsWorkFirst(t *testing.T) {
	t.Parallel()

	rs := []Runner{
		{Name: "failed", State: StateFailed},
		{Name: "idle", State: StateIdle},
		{Name: "busy-new", State: StateBusy, Job: Job{StartedAt: now.Add(-time.Minute)}},
		{Name: "busy-old", State: StateBusy, Job: Job{StartedAt: now.Add(-time.Hour)}},
		{Name: "pending", State: StatePending},
	}
	SortRunners(rs, now)

	got := lo.Map(rs, func(r Runner, _ int) string { return r.Name })
	assert.Equal(t, []string{"busy-old", "busy-new", "pending", "idle", "failed"}, got)
}

func TestSortSetsByPressurePrioritisesQueue(t *testing.T) {
	t.Parallel()

	sets := []RunnerSet{
		{Name: "saturated", Current: 10, MaxRunners: 10},
		{Name: "queued", Current: 2, MaxRunners: 30, Queued: 5, QueuedKnown: true},
		{Name: "idle", Current: 1, MaxRunners: 30},
	}
	SortSets(sets, SortPressure)

	assert.Equal(t, "queued", sets[0].Name, "queued work should outrank saturation")
	assert.Equal(t, "saturated", sets[1].Name, "saturation should outrank idle")
}

func TestSortSetsUnboundedNeverSaturates(t *testing.T) {
	t.Parallel()

	sets := []RunnerSet{
		{Name: "unbounded", Current: 500, Unbounded: true},
		{Name: "full", Current: 10, MaxRunners: 10},
	}
	SortSets(sets, SortPressure)

	assert.Equal(t, "full", sets[0].Name,
		"an unbounded set is never under capacity pressure")
}

func TestAtCapacityIgnoresUnbounded(t *testing.T) {
	t.Parallel()

	assert.False(t, RunnerSet{Current: 999, Unbounded: true}.AtCapacity(),
		"an unbounded set is never at capacity")
	assert.True(t, RunnerSet{Current: 10, MaxRunners: 10}.AtCapacity(),
		"a set at its ceiling must report at capacity")
}

func TestFormatAge(t *testing.T) {
	t.Parallel()

	cases := map[time.Duration]string{
		0:                           "—",
		-time.Second:                "—",
		45 * time.Second:            "45s",
		90 * time.Second:            "1m 30s",
		2*time.Hour + 3*time.Minute: "2h 3m",
	}
	for in, want := range cases {
		assert.Equal(t, want, FormatAge(in), "FormatAge(%v)", in)
	}
}

func TestByRepositoryRanksBusiestFirst(t *testing.T) {
	t.Parallel()

	got := ByRepository([]Runner{
		runner("a", "s", StateBusy, "WindKube/one", "ci.yml", "test"),
		runner("b", "s", StateBusy, "WindKube/two", "ci.yml", "test"),
		runner("c", "s", StateBusy, "WindKube/two", "ci.yml", "test"),
		runner("d", "s", StateIdle, "", "", ""),
	})

	require.Len(t, got, 2)
	assert.Equal(t, "WindKube/two", got[0].Repository)
	assert.Equal(t, 2, got[0].Runners)
}

func TestFailuresNewestFirstAndCapped(t *testing.T) {
	t.Parallel()

	got := Failures([]Runner{
		{Name: "old", FailureReason: "OOMKilled", FailedAt: now.Add(-time.Hour), State: StateFailed},
		{Name: "new", FailureReason: "ImagePullBackOff", FailedAt: now.Add(-time.Minute), State: StateFailed},
		{Name: "healthy"},
		{Name: "undated", FailureReason: "exit code 1"},
	}, 2)

	require.Len(t, got, 2)
	assert.Equal(t, "new", got[0].Runner, "newest failure should sort first")
}
