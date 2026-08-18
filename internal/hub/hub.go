// Package hub fans fleet-change notifications out to connected browsers.
//
// It deliberately carries no payload beyond a sequence number and a timestamp.
// Each SSE stream re-renders from the current snapshot using its own filter
// state, so pushing the data through the hub would mean either broadcasting
// one client's filtered view to everyone or serialising the whole fleet per
// subscriber. A bare "something changed" tick is both smaller and correct.
package hub

import (
	"sync"
	"time"
)

// Tick announces that the fleet has changed.
type Tick struct {
	// Seq increases monotonically, letting a client detect dropped ticks.
	Seq uint64
	// At is when the change was observed. The UI shows the age of the most
	// recent tick as its live-connection indicator, so this must be the
	// observation time rather than the delivery time.
	At time.Time
}

// Hub broadcasts ticks to every active subscriber.
type Hub struct {
	mu     sync.RWMutex
	subs   map[chan Tick]struct{}
	seq    uint64
	latest Tick
}

// New returns an empty hub.
func New() *Hub {
	return &Hub{subs: make(map[chan Tick]struct{})}
}

// Subscribe returns a channel of ticks and a cancel func that unsubscribes and
// closes the channel. Cancel is idempotent and must always be called.
func (h *Hub) Subscribe() (<-chan Tick, func()) {
	// Buffered so a subscriber that is mid-render does not immediately start
	// dropping ticks; see Broadcast for what happens when the buffer fills.
	ch := make(chan Tick, 8)

	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, ch)
			h.mu.Unlock()
			close(ch)
		})
	}
}

// Broadcast notifies every subscriber that the fleet changed.
//
// A subscriber whose buffer is full is skipped rather than blocked. Dropping a
// tick is harmless here precisely because ticks carry no data: the next one the
// client receives will make it re-render from the current snapshot, which is
// the same state it would have reached by processing both.
//
// The lock is held across the sends, not just the bookkeeping. Copying the
// subscriber set and sending after unlocking would race with a concurrent
// cancel: that cancel closes the channel, and the in-flight send then panics
// on a closed channel. Holding the lock is safe here only because every send
// below is non-blocking, so this never sleeps while holding it.
func (h *Hub) Broadcast(at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seq++
	tick := Tick{Seq: h.seq, At: at}
	h.latest = tick

	for ch := range h.subs {
		select {
		case ch <- tick:
		default: // slow subscriber; it will catch up on the next tick
		}
	}
}

// Latest returns the most recent tick, so a stream that has just opened can
// render the live indicator without waiting for the next change.
func (h *Hub) Latest() Tick {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.latest
}

// Subscribers reports the current subscriber count, exposed for the health
// strip and for tests.
func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
