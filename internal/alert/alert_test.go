package alert

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
	"arc-ui/internal/prom"
	"arc-ui/internal/store"
)

var t0 = time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

type scripted struct {
	mu    sync.Mutex
	found []Finding
	err   error
}

func (s *scripted) set(found []Finding, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.found, s.err = found, err
}

func (s *scripted) rule(hold time.Duration) Rule {
	return Rule{ID: "r", Title: "rule", Severity: Warning, For: hold, Eval: func(context.Context, time.Time) ([]Finding, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.found, s.err
	}}
}

func TestAFindingFiresOnlyAfterHoldingForItsDuration(t *testing.T) {
	t.Parallel()
	src := &scripted{}
	changes := 0
	e := &Engine{Rules: []Rule{src.rule(15 * time.Minute)}, OnChange: func() { changes++ }, Log: zerolog.Nop()}

	src.set([]Finding{{Set: "linux", Message: "3 queued"}}, nil)
	assert.False(t, e.Evaluate(t.Context(), t0))
	assert.False(t, e.Evaluate(t.Context(), t0.Add(14*time.Minute)))
	assert.Empty(t, e.State().Firing)

	assert.True(t, e.Evaluate(t.Context(), t0.Add(15*time.Minute)))
	s := e.State()
	require.Len(t, s.Firing, 1)
	assert.Equal(t, t0, s.Firing[0].Since)
	assert.Equal(t, "linux", s.Firing[0].Set)
	assert.Equal(t, 1, s.Rules[0].Firing)
	assert.Equal(t, 1, changes)
}

func TestAFindingThatLapsesRestartsItsHold(t *testing.T) {
	t.Parallel()
	src := &scripted{}
	e := &Engine{Rules: []Rule{src.rule(10 * time.Minute)}, Log: zerolog.Nop()}

	src.set([]Finding{{Set: "linux"}}, nil)
	e.Evaluate(t.Context(), t0)
	src.set(nil, nil)
	e.Evaluate(t.Context(), t0.Add(5*time.Minute))
	src.set([]Finding{{Set: "linux"}}, nil)
	e.Evaluate(t.Context(), t0.Add(11*time.Minute))
	assert.Empty(t, e.State().Firing, "the hold restarted at 11m")
}

func TestResolvedAlertsAreKeptThenDropped(t *testing.T) {
	t.Parallel()
	src := &scripted{}
	e := &Engine{Rules: []Rule{src.rule(0)}, Keep: time.Hour, Log: zerolog.Nop()}

	src.set([]Finding{{Set: "linux"}}, nil)
	e.Evaluate(t.Context(), t0)
	src.set(nil, nil)
	assert.True(t, e.Evaluate(t.Context(), t0.Add(time.Minute)))
	s := e.State()
	assert.Empty(t, s.Firing)
	require.Len(t, s.Resolved, 1)
	assert.Equal(t, t0.Add(time.Minute), s.Resolved[0].ResolvedAt)

	e.Evaluate(t.Context(), t0.Add(2*time.Hour))
	assert.Empty(t, e.State().Resolved)
}

func TestAnUnavailableSourceNeitherFiresNorResolves(t *testing.T) {
	t.Parallel()
	src := &scripted{}
	e := &Engine{Rules: []Rule{src.rule(0)}, Log: zerolog.Nop()}

	src.set([]Finding{{Set: "linux"}}, nil)
	e.Evaluate(t.Context(), t0)
	src.set(nil, errors.New("prometheus: connection refused"))
	assert.False(t, e.Evaluate(t.Context(), t0.Add(time.Minute)))

	s := e.State()
	assert.Len(t, s.Firing, 1, "an outage is not a recovery")
	assert.Contains(t, s.Rules[0].Unavailable, "connection refused")
}

type fakeFleet struct{ snap fleet.Snapshot }

func (f fakeFleet) Snapshot() fleet.Snapshot { return f.snap }

func sources(names ...string) []fleet.Source {
	out := make([]fleet.Source, 0, len(names))
	for _, n := range names {
		out = append(out, fleet.Source{Name: n, Available: true})
	}
	return out
}

func eval(t *testing.T, r Rule) []Finding {
	t.Helper()
	f, err := r.Eval(context.Background(), t0)
	require.NoError(t, err)
	return f
}

func TestQueuedAtMaxNeedsBothAQueueAndAFullSet(t *testing.T) {
	t.Parallel()
	snap := fleet.Snapshot{Sources: sources(fleet.SourceARCCRDs), Sets: []fleet.RunnerSet{
		{Name: "full", MaxRunners: 4, Current: 4, Queued: 2, QueuedKnown: true},
		{Name: "room", MaxRunners: 4, Current: 2, Queued: 2, QueuedKnown: true},
		{Name: "idle", MaxRunners: 4, Current: 4, QueuedKnown: true},
		{Name: "unbounded", Unbounded: true, Current: 40, Queued: 9, QueuedKnown: true},
	}}
	got := eval(t, QueuedAtMax(fakeFleet{snap}, 15*time.Minute))
	require.Len(t, got, 1)
	assert.Equal(t, "full", got[0].Set)
	assert.Equal(t, "2 jobs queued with 4/4 runners", got[0].Message)

	snap.Sets = []fleet.RunnerSet{{Name: "x", MaxRunners: 1, Current: 1}}
	_, err := QueuedAtMax(fakeFleet{snap}, time.Minute).Eval(context.Background(), t0)
	require.ErrorContains(t, err, "queue depth unknown")
}

func TestListenerDownIgnoresSetsWhoseListenerIsUnknown(t *testing.T) {
	t.Parallel()
	snap := fleet.Snapshot{Sources: sources(fleet.SourceARCCRDs), Sets: []fleet.RunnerSet{
		{Name: "down", ListenerKnown: true},
		{Name: "up", ListenerKnown: true, ListenerHealthy: true},
		{Name: "unknown"},
	}}
	got := eval(t, ListenerDown(fakeFleet{snap}, 2*time.Minute))
	require.Len(t, got, 1)
	assert.Equal(t, "down", got[0].Set)

	_, err := ListenerDown(fakeFleet{fleet.Snapshot{}}, 0).Eval(context.Background(), t0)
	require.ErrorContains(t, err, "arc-crds unavailable")
}

type fakeFailures struct {
	jobs []store.JobRecord
	err  error
}

func (f fakeFailures) Failures(context.Context, string, time.Time, time.Time, int) ([]store.JobRecord, int64, error) {
	return f.jobs, int64(len(f.jobs)), f.err
}

func TestOOMKilledCountsRunnersFromTheFleetAndTheStoreOnce(t *testing.T) {
	t.Parallel()
	snap := fleet.Snapshot{Runners: []fleet.Runner{
		{Name: "a", SetName: "linux", FailureReason: "OOMKilled", FailedAt: t0.Add(-time.Minute)},
		{Name: "old", SetName: "linux", FailureReason: "OOMKilled", FailedAt: t0.Add(-3 * time.Hour)},
		{Name: "b", SetName: "linux", FailureReason: "exit code 1", FailedAt: t0},
	}}
	db := fakeFailures{jobs: []store.JobRecord{
		{Runner: "a", Set: "linux", FailureReason: "OOMKilled"},
		{Runner: "c", Set: "linux", FailureReason: "OOMKilled"},
		{Runner: "d", Set: "gpu", FailureReason: "Evicted"},
	}}
	got := eval(t, OOMKilled(fakeFleet{snap}, db, time.Hour))
	require.Len(t, got, 1)
	assert.Equal(t, "linux", got[0].Set)
	assert.Equal(t, "2 runners OOMKilled in the last 1h", got[0].Message)

	_, err := OOMKilled(fakeFleet{snap}, fakeFailures{err: errors.New("database is locked")}, time.Hour).Eval(context.Background(), t0)
	require.Error(t, err)
}

type fakeQuerier struct {
	rows []prom.Sample
	err  error
	got  string
}

func (f *fakeQuerier) Query(_ context.Context, q string) ([]prom.Sample, error) {
	f.got = q
	return f.rows, f.err
}

func TestFailedRisingComparesTheIncreaseWithTheThreshold(t *testing.T) {
	t.Parallel()
	snap := fleet.Snapshot{Sources: sources(fleet.SourceControllerMetrics)}
	q := &fakeQuerier{rows: []prom.Sample{
		{Labels: map[string]string{"name": "linux"}, Value: 4},
		{Labels: map[string]string{"name": "gpu"}, Value: 2},
	}}
	got := eval(t, FailedRising(fakeFleet{snap}, q, "QUERY", 3, 15*time.Minute))
	require.Len(t, got, 1)
	assert.Equal(t, "linux", got[0].Set)
	assert.Equal(t, "QUERY", q.got)

	_, err := FailedRising(fakeFleet{fleet.Snapshot{}}, q, "QUERY", 3, time.Minute).Eval(context.Background(), t0)
	require.ErrorContains(t, err, "controller-metrics unavailable", "no controller metrics is not the same as no failures")
}

func TestStartupP95SkipsSetsWithNoJobs(t *testing.T) {
	t.Parallel()
	snap := fleet.Snapshot{Sources: sources(fleet.SourceARCMetrics)}
	q := &fakeQuerier{rows: []prom.Sample{
		{Labels: map[string]string{"name": "slow"}, Value: 192},
		{Labels: map[string]string{"name": "fast"}, Value: 20},
		{Labels: map[string]string{"name": "quiet"}, Value: math.NaN()},
	}}
	got := eval(t, StartupP95(fakeFleet{snap}, q, "QUERY", "name", 2*time.Minute, 30*time.Minute))
	require.Len(t, got, 1)
	assert.Equal(t, "slow", got[0].Set)
	assert.Equal(t, "p95 startup 3m12s over the last 30m", got[0].Message)
}

func TestShortDurations(t *testing.T) {
	t.Parallel()
	for d, want := range map[time.Duration]string{
		30 * time.Second: "30s", 15 * time.Minute: "15m", time.Hour: "1h",
		90 * time.Minute: "1h30m", 192 * time.Second: "3m12s", 0: "0s",
	} {
		assert.Equal(t, want, short(d))
	}
}
