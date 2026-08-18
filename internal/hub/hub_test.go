package hub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var at = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

func TestBroadcastReachesEverySubscriber(t *testing.T) {
	t.Parallel()
	h := New()

	a, cancelA := h.Subscribe()
	b, cancelB := h.Subscribe()
	defer cancelA()
	defer cancelB()

	h.Broadcast(at)

	for name, ch := range map[string]<-chan Tick{"a": a, "b": b} {
		select {
		case tick := <-ch:
			assert.Equal(t, uint64(1), tick.Seq, "%s sequence", name)
			assert.True(t, tick.At.Equal(at), "%s timestamp", name)
		case <-time.After(time.Second):
			t.Errorf("%s received nothing", name)
		}
	}
}

func TestCancelUnsubscribesAndIsIdempotent(t *testing.T) {
	t.Parallel()
	h := New()

	ch, cancel := h.Subscribe()
	require.Equal(t, 1, h.Subscribers())

	cancel()
	assert.Zero(t, h.Subscribers(), "cancel should unsubscribe")

	_, open := <-ch
	assert.False(t, open, "channel should be closed after cancel")

	// A deferred cancel after an explicit one must not panic on a double close.
	assert.NotPanics(t, cancel)
}

func TestSlowSubscriberIsSkippedNotBlocking(t *testing.T) {
	t.Parallel()
	h := New()

	_, cancel := h.Subscribe() // never drained
	defer cancel()

	// Far more ticks than the channel buffer. If Broadcast blocked on a full
	// buffer, one stalled browser tab would freeze every other client.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			h.Broadcast(at)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Broadcast blocked on a slow subscriber")
	}
}

func TestSeqIncreasesMonotonically(t *testing.T) {
	t.Parallel()
	h := New()

	ch, cancel := h.Subscribe()
	defer cancel()

	h.Broadcast(at)
	h.Broadcast(at)
	h.Broadcast(at)

	var last uint64
	for i := 0; i < 3; i++ {
		tick := <-ch
		require.Greater(t, tick.Seq, last, "sequence went backwards")
		last = tick.Seq
	}
}

func TestLatestLetsANewStreamRenderImmediately(t *testing.T) {
	t.Parallel()
	h := New()

	assert.Zero(t, h.Latest().Seq, "a fresh hub has no ticks")

	h.Broadcast(at)
	got := h.Latest()
	assert.Equal(t, uint64(1), got.Seq)
	assert.True(t, got.At.Equal(at))
}

func TestConcurrentSubscribeAndBroadcast(t *testing.T) {
	t.Parallel()
	h := New()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := h.Subscribe()
			defer cancel()
			h.Broadcast(at)
			select {
			case <-ch:
			case <-time.After(time.Second):
			}
		}()
	}
	wg.Wait()

	assert.Zero(t, h.Subscribers(), "subscribers leaked")
}
