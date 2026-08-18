package k8s

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"

	"arc-ui/internal/fleet"
)

const (
	// eventCacheTTL is how long one pod's events are reused. The runner detail
	// page re-renders on every SSE push; without this, a page left open would
	// issue an uncached list against the busiest collection in the cluster
	// several times a second.
	eventCacheTTL = 10 * time.Second

	// maxEventsPerPod caps what the panel is handed. A pod in CrashLoopBackOff
	// accumulates events indefinitely and the panel shows a handful.
	maxEventsPerPod = 50

	// eventPageSize bounds ONE API response, and nothing else.
	//
	// ListOptions.Limit is PAGINATION, not selection: the API server answers
	// with the first page in its own key order, and event names embed the
	// emission time, so page one is the OLDEST events. A single limited list
	// therefore cannot see the newest event for any pod with more history than
	// the limit, and no amount of local sorting recovers what was never sent —
	// which is why EventsForPod follows list.Continue instead of reading one
	// page. This number only keeps a single response (and the memory it decodes
	// into) bounded against the busiest collection in the cluster.
	eventPageSize = 500

	// eventPageBudget caps how many pages one uncached call will follow, so a
	// pathological emitter cannot turn opening a runner page into an unbounded
	// walk over its event history. The budget is counted in PAGES, not events:
	// Limit is a maximum, so eventPageSize * eventPageBudget is a ceiling on
	// what the loop can read and never a floor. See EventsForPod.
	eventPageBudget = 20
)

// EventsForPod fetches recent events for one pod, on demand and cached ~10s.
//
// Three deliberate choices:
//
// The core/v1 Event API, not events.k8s.io/v1 — the involvedObject.* field
// selectors the API server indexes only exist on the legacy type, and without
// them this becomes a full list of every event in the namespace.
//
// The uid is part of the selector, not just the name. ARC derives runner pod
// names from the scale set, so names DO repeat across generations; filtering on
// name alone shows a live runner the death throes of a pod that was deleted an
// hour ago.
//
// No informer. Events are the highest-churn objects in any cluster — watching
// them cluster-wide costs more than the rest of this dashboard combined, to
// serve one panel almost nobody has open.
//
// An empty result is normal: the API server's default --event-ttl is one hour
// (see EventRetention), so anything older has been garbage collected. Callers
// should render "no recent events", not "nothing ever happened".
//
// What is guaranteed: the continue token is followed (until it is empty or the
// page budget below runs out) and the running set is re-sorted as each page
// arrives, so the result is the newest maxEventsPerPod events of everything
// read, newest first. A single limited list returns the OLDEST page instead,
// which is the whole reason for the loop.
//
// The remaining bound: paging stops after eventPageBudget pages, and that is a
// bound on pages, not on this pod's events. ListOptions.Limit documents that a
// limited list "may return fewer than the requested amount of items (up to zero
// items) in the event all requested objects are filtered out", which is what a
// field selector over a namespace-wide event stream does, so the pages read
// cover at most eventPageSize * eventPageBudget of this pod's events and may
// cover far fewer. Whatever the budget bought, the result is the newest of it;
// anything older than that window is invisible, and the log says so. Cost per
// uncached call is at most eventPageBudget requests of eventPageSize items,
// decoded one page at a time and drawn from the rate limiter every client here
// shares; the ~10s cache absorbs repeats.
func (c *Collector) EventsForPod(ctx context.Context, namespace, name string, uid types.UID) ([]fleet.Event, error) {
	key := fmt.Sprintf("%s/%s/%s", namespace, name, uid)
	if cached, ok := c.events.get(key, time.Now()); ok {
		return cached, nil
	}

	selector := fields.Set{"involvedObject.name": name}
	if uid != "" {
		selector["involvedObject.uid"] = string(uid)
	}
	opts := metav1.ListOptions{
		FieldSelector: selector.AsSelector().String(),
		Limit:         eventPageSize,
	}

	events := make([]fleet.Event, 0, maxEventsPerPod+eventPageSize)
	for page := 1; ; page++ {
		list, err := c.clients.Kube.CoreV1().Events(namespace).List(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("list events for pod %s/%s: %w", namespace, name, err)
		}
		for i := range list.Items {
			events = append(events, convertEvent(&list.Items[i]))
		}
		// Sorted and trimmed per page rather than once at the end: the newest
		// still win across every page read, while the slice never holds more
		// than one page plus the panel's worth. A field selector may also return
		// a short — even empty — page with a continue token, so the loop keys
		// off the token and never off how much a page contained.
		events = newestEvents(events)
		if list.Continue == "" {
			break
		}
		if page >= eventPageBudget {
			c.log.Warn().
				Str("pod", objectKey(namespace, name)).
				Int("pages", page).
				Int("page_size", eventPageSize).
				Msg("event paging stopped at the page budget; showing an older window")
			break
		}
		opts.Continue = list.Continue
	}

	c.events.put(key, events, time.Now())
	// Cloned for the same reason eventCache.get clones: the caller may sort or
	// truncate what it gets back. Returning `events` here would hand out the
	// cache's own backing array on the miss path, so the first caller could
	// reorder the entry every later cache hit then serves.
	return slices.Clone(events), nil
}

// newestEvents orders events newest first and keeps at most maxEventsPerPod.
//
// Undated events (which a malformed emitter can produce) sort last rather than
// to the top of the panel. The sort is stable so events sharing a timestamp keep
// the order the API server listed them in.
func newestEvents(events []fleet.Event) []fleet.Event {
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].At.IsZero() != events[j].At.IsZero() {
			return !events[i].At.IsZero()
		}
		return events[i].At.After(events[j].At)
	})
	if len(events) > maxEventsPerPod {
		events = events[:maxEventsPerPod]
	}
	return events
}

// convertEvent picks the most recent of the three timestamps an Event may
// carry. Modern emitters set eventTime and leave the legacy fields zero, while
// the kubelet still sets lastTimestamp, so checking only one loses half of them.
//
// The genuine maximum, not the first non-zero in some fixed order: an emitter
// that writes both — a fresh eventTime beside a lastTimestamp it stopped
// updating — would otherwise be stamped with the stale one and sort into the
// past, which is precisely backwards for a panel ordered by recency.
func convertEvent(e *corev1.Event) fleet.Event {
	at := newestTime(e.LastTimestamp.Time, e.EventTime.Time, e.FirstTimestamp.Time)
	count := e.Count
	if count == 0 {
		count = 1
	}
	return fleet.Event{
		Type:    e.Type,
		Reason:  e.Reason,
		Message: e.Message,
		At:      at,
		Count:   count,
	}
}

// newestTime returns the latest of the given times. Unset fields are the zero
// time, which loses every comparison, so they drop out without a special case.
func newestTime(times ...time.Time) time.Time {
	var out time.Time
	for _, t := range times {
		if t.After(out) {
			out = t
		}
	}
	return out
}

// eventCache is a tiny TTL cache keyed by namespace/name/uid.
type eventCache struct {
	ttl time.Duration

	mu      sync.Mutex
	entries map[string]eventCacheEntry
}

type eventCacheEntry struct {
	at     time.Time
	events []fleet.Event
}

func newEventCache(ttl time.Duration) *eventCache {
	return &eventCache{ttl: ttl, entries: map[string]eventCacheEntry{}}
}

func (c *eventCache) get(key string, now time.Time) ([]fleet.Event, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || now.Sub(entry.at) > c.ttl {
		return nil, false
	}
	// Copied because the caller may sort or truncate it.
	return slices.Clone(entry.events), true
}

func (c *eventCache) put(key string, events []fleet.Event, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Runner pods are ephemeral, so keys are never reused and the map would
	// grow forever. Expired entries are swept on write, which is bounded by how
	// often anyone actually opens a runner detail page.
	for k, entry := range c.entries {
		if now.Sub(entry.at) > c.ttl {
			delete(c.entries, k)
		}
	}
	c.entries[key] = eventCacheEntry{at: now, events: events}
}
