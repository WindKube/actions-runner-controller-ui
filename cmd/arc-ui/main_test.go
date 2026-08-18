package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
)

// recorderProbe stands in for the store and the hub, and records the exact
// interleaving of the two so the tests can assert on ordering rather than on
// timing.
type recorderProbe struct {
	mu sync.Mutex
	// steps is the observed sequence, e.g. "record 1s" then "broadcast 1s".
	steps []string
	// inFlight counts concurrent record calls; overlapped is latched the
	// moment two of them are ever in flight at once.
	inFlight   int
	overlapped bool
	// hold, if set, blocks inside record until it is closed. Only the first
	// record call waits on it, which is what puts a slow store write in flight
	// while the next change arrives.
	hold chan struct{}
	held bool
	// err is returned by every record call.
	err error
}

func (p *recorderProbe) recordFn(_ context.Context, snap fleet.Snapshot) error {
	p.mu.Lock()
	p.inFlight++
	if p.inFlight > 1 {
		p.overlapped = true
	}
	hold := p.hold
	if p.held {
		hold = nil
	}
	p.held = true
	p.mu.Unlock()

	if hold != nil {
		<-hold
	}

	p.mu.Lock()
	p.steps = append(p.steps, "record "+label(snap))
	p.inFlight--
	err := p.err
	p.mu.Unlock()
	return err
}

func (p *recorderProbe) broadcastFn(at time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.steps = append(p.steps, "broadcast "+at.UTC().Format("15:04:05.000"))
}

func (p *recorderProbe) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.steps...)
}

func (p *recorderProbe) waitForSteps(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if steps := p.snapshot(); len(steps) >= n {
			return steps
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only saw %v after 2s, wanted %d steps", p.snapshot(), n)
	return nil
}

func label(snap fleet.Snapshot) string {
	return snap.At.UTC().Format("15:04:05.000")
}

// snapAt is a snapshot that carries nothing but its observation time, which is
// all the recorder ever looks at.
func snapAt(base time.Time, offset time.Duration) fleet.Snapshot {
	return fleet.Snapshot{At: base.Add(offset)}
}

func startRecorder(t *testing.T, p *recorderProbe, onResult func(error)) *snapshotRecorder {
	t.Helper()

	if onResult == nil {
		// The recorder reports every outcome, so a nil here is a write that
		// worked and is exactly what these tests expect to see.
		onResult = func(err error) {
			if err != nil {
				t.Errorf("unexpected record error: %v", err)
			}
		}
	}
	rec := newSnapshotRecorder(p.recordFn, p.broadcastFn, onResult)

	ctx, cancel := context.WithCancel(context.Background())
	rec.start(ctx)
	t.Cleanup(func() {
		cancel()
		select {
		case <-rec.stopped:
		case <-time.After(2 * time.Second):
			t.Error("recorder did not stop when its context was cancelled")
		}
	})
	return rec
}

func TestSnapshotRecorderAppliesSnapshotsInOrder(t *testing.T) {
	t.Parallel()

	// The store diffs each snapshot against the previous one it saw, so two
	// recordings in flight at once let the older snapshot land last and become
	// "previous" for the newer one: the CPU/memory integration interval goes
	// zero or negative and churn is diffed against a future fleet. A store
	// write only has to outlast the collector's 250ms debounce window for the
	// second change to arrive mid-write, which is exactly what happens during
	// the churn bursts that make writes slow in the first place.
	p := &recorderProbe{hold: make(chan struct{})}
	rec := startRecorder(t, p, nil)

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	first, second := snapAt(base, 0), snapAt(base, time.Second)

	rec.enqueue(first)

	// Wait until the first write is actually in flight before delivering the
	// second change, so this reproduces the overlap rather than hoping for it.
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.inFlight == 1
	}, 2*time.Second, time.Millisecond, "first record never started")

	rec.enqueue(second)
	// Give a racing goroutine every chance to finish the second write ahead of
	// the first; the fix makes this wait uneventful.
	time.Sleep(50 * time.Millisecond)
	close(p.hold)

	steps := p.waitForSteps(t, 4)

	assert.Equal(t, []string{
		"record " + label(first),
		"broadcast " + label(first),
		"record " + label(second),
		"broadcast " + label(second),
	}, steps, "snapshots must be recorded in observation order, each broadcast after its write")

	p.mu.Lock()
	defer p.mu.Unlock()
	assert.False(t, p.overlapped, "two recordings were in flight at once")
}

func TestSnapshotRecorderCoalescesChangesQueuedBehindAWrite(t *testing.T) {
	t.Parallel()

	// Skipping a snapshot that was superseded before it was ever written is
	// deliberate: the newer one describes the same fleet, and replaying the
	// stale one would only make the store diff against state it has already
	// moved past.
	p := &recorderProbe{hold: make(chan struct{})}
	rec := startRecorder(t, p, nil)

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	first, stale, newest := snapAt(base, 0), snapAt(base, time.Second), snapAt(base, 2*time.Second)

	rec.enqueue(first)
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.inFlight == 1
	}, 2*time.Second, time.Millisecond, "first record never started")

	rec.enqueue(stale)
	rec.enqueue(newest)
	close(p.hold)

	steps := p.waitForSteps(t, 4)

	assert.Equal(t, []string{
		"record " + label(first),
		"broadcast " + label(first),
		"record " + label(newest),
		"broadcast " + label(newest),
	}, steps, "the superseded snapshot should have been dropped, not recorded")
}

func TestSnapshotRecorderEnqueueNeverBlocksTheCollector(t *testing.T) {
	t.Parallel()

	// enqueue runs on the collector's notifier goroutine, where a slow
	// subscriber delays every other one. It must return even with no worker
	// draining it — that is the whole reason the original code detached a
	// goroutine. The undrained recorder reports itself once through onError,
	// which is swallowed here; TestSnapshotRecorderReportsSnapshotsEnqueuedWithNoWorker
	// is where that report is asserted on.
	p := &recorderProbe{}
	rec := newSnapshotRecorder(p.recordFn, p.broadcastFn, func(error) {})

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100 {
			rec.enqueue(snapAt(base, time.Duration(i)*time.Second))
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue blocked with no worker draining it")
	}
}

func TestSnapshotRecorderKeepsTheNewerOfTwoPendingSnapshots(t *testing.T) {
	t.Parallel()

	// Nothing the recorder owns serialises its callers, so two enqueues can
	// hand over out of order. Resolving that by "whoever wrote r.pending last"
	// lets the older snapshot reach the store after the newer one — the exact
	// out-of-order application the worker exists to prevent.
	//
	// No worker here on purpose: this asserts on what enqueue parks, so nothing
	// may drain it mid-test.
	p := &recorderProbe{}
	rec := newSnapshotRecorder(p.recordFn, p.broadcastFn, func(error) {})

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	newest, stale := snapAt(base, time.Second), snapAt(base, 0)

	rec.enqueue(newest)
	rec.enqueue(stale)

	snap, ok := rec.take()
	require.True(t, ok, "enqueue parked nothing")
	assert.Equal(t, label(newest), label(snap), "the older snapshot overwrote the newer pending one")
	assert.Empty(t, p.snapshot(), "no worker was started, so nothing can have been recorded")
}

func TestSnapshotRecorderReportsSnapshotsEnqueuedWithNoWorker(t *testing.T) {
	t.Parallel()

	// enqueue neither blocks nor fails, so a recorder whose worker was never
	// started swallows every snapshot: no history rows, no live updates, no log
	// line. Reporting it is what turns that wiring mistake into something
	// somebody notices — onError flips the store source to unavailable, which
	// surfaces in the health strip.
	p := &recorderProbe{}

	var (
		mu       sync.Mutex
		reported []error
	)
	rec := newSnapshotRecorder(p.recordFn, p.broadcastFn, func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err)
	})

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rec.enqueue(snapAt(base, 0))
	rec.enqueue(snapAt(base, time.Second))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, reported, 1, "an undrained recorder must report itself, exactly once")
	assert.ErrorIs(t, reported[0], errRecorderNotStarted)
}

func TestSnapshotRecorderStaysQuietWhenItsWorkerIsRunning(t *testing.T) {
	t.Parallel()

	// The counterpart to the test above: the orphan report must not fire just
	// because the worker goroutine has not been scheduled yet, which is why
	// start latches the flag synchronously instead of run latching it.
	p := &recorderProbe{}
	rec := startRecorder(t, p, nil)

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rec.enqueue(snapAt(base, 0))

	p.waitForSteps(t, 2)
}

func TestStoreSourceNamesTheConditionThatOccurred(t *testing.T) {
	t.Parallel()

	// The health strip renders "store: <reason>", so the reason has to be the
	// condition that actually happened. An orphaned recorder never attempted a
	// write, and reporting failing writes for it names a cause that did not
	// occur.
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

	write := storeSource(errors.New("disk full"), now)
	assert.Equal(t, fleet.Source{
		Name: fleet.SourceStore, Available: false,
		Reason: "writes failing", CheckedAt: now,
	}, write)

	orphan := storeSource(fmt.Errorf("recording: %w", errRecorderNotStarted), now)
	assert.Equal(t, fleet.Source{
		Name: fleet.SourceStore, Available: false,
		Reason: "snapshots are not being recorded", CheckedAt: now,
	}, orphan, "an orphaned recorder must not be reported as a failing write")
}

func TestSnapshotRecorderBroadcastsEvenWhenTheWriteFails(t *testing.T) {
	t.Parallel()

	// A failing store must not freeze the dashboard: the fleet still changed,
	// so browsers are still told to re-render. The failure is reported once,
	// through onError, which is what flips the store source to unavailable.
	p := &recorderProbe{err: errors.New("disk full")}

	var (
		mu       sync.Mutex
		reported []string
	)
	rec := startRecorder(t, p, func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err.Error())
	})

	base := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	rec.enqueue(snapAt(base, 0))

	steps := p.waitForSteps(t, 2)
	assert.Equal(t, []string{"record " + label(snapAt(base, 0)), "broadcast " + label(snapAt(base, 0))}, steps)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"disk full"}, reported, "the write failure should be reported exactly once")
}

// TestRecorderReportsRecoveryAfterAFailedWrite pins that a store which starts
// working again is reported as working again. Nothing else in the process ever
// revisits that verdict, so a recorder that only spoke up on failure would
// leave the health strip accusing the store for the life of the process.
func TestRecorderReportsRecoveryAfterAFailedWrite(t *testing.T) {
	t.Parallel()

	var failing atomic.Bool
	failing.Store(true)

	results := make(chan error, 8)
	r := newSnapshotRecorder(
		func(context.Context, fleet.Snapshot) error {
			if failing.Load() {
				return errors.New("disk full")
			}
			return nil
		},
		func(time.Time) {},
		func(err error) { results <- err },
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.start(ctx)

	r.enqueue(fleet.Snapshot{At: time.Now()})
	select {
	case err := <-results:
		require.Error(t, err, "the failed write was not reported")
	case <-time.After(5 * time.Second):
		require.Fail(t, "the failed write was never reported")
	}

	failing.Store(false)
	r.enqueue(fleet.Snapshot{At: time.Now().Add(time.Second)})
	select {
	case err := <-results:
		assert.NoError(t, err, "a write that succeeded must be reported as recovery")
	case <-time.After(5 * time.Second):
		require.Fail(t, "a write that succeeded was never reported")
	}
}
