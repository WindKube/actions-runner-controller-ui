// Package hub fans fleet-change notifications out to connected browsers.
//
// A tick carries no payload beyond a sequence number and a timestamp: each SSE
// stream re-renders from the current snapshot using its own filter state.
package hub

import (
	"sync"
	"time"
)

// Tick announces that the fleet has changed.
type Tick struct {
	// Seq increases monotonically, letting a client detect dropped ticks.
	Seq uint64
	// At must be the observation time rather than the delivery time: the UI
	// renders its age as the live-connection indicator.
	At time.Time
}

// Hub broadcasts ticks to every active subscriber.
type Hub struct {
	mu   sync.RWMutex
	subs map[chan Tick]struct{}
	seq  uint64
}

func New() *Hub {
	return &Hub{subs: make(map[chan Tick]struct{})}
}

// Subscribe returns a channel of ticks and a cancel func that unsubscribes and
// closes the channel. Cancel is idempotent and must always be called.
func (h *Hub) Subscribe() (<-chan Tick, func()) {
	// Buffered so a subscriber that is mid-render does not immediately start
	// dropping ticks.
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

// Broadcast notifies every subscriber that the fleet changed. A subscriber
// whose buffer is full is skipped rather than blocked: ticks carry no data, so
// the next one it receives leaves it in the state it would have reached by
// processing both.
//
// The lock is held across the sends, not just the bookkeeping. Sending after
// unlocking would race with a concurrent cancel closing the channel, panicking
// the in-flight send. That is safe only because every send below is
// non-blocking, so this never sleeps while holding the lock.
func (h *Hub) Broadcast(at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.seq++
	tick := Tick{Seq: h.seq, At: at}

	for ch := range h.subs {
		select {
		case ch <- tick:
		default:
		}
	}
}

// Subscribers reports the current subscriber count. Nothing renders it; it is
// here because the leak it would show — a cancelled stream whose channel is
// still in the map — is otherwise invisible to a test.
func (h *Hub) Subscribers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
