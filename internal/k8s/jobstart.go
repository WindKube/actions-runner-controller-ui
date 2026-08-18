package k8s

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// JobStartTracker remembers when each runner was first seen holding a job.
//
// ARC records no job start time anywhere: status.jobId simply appears on the
// EphemeralRunner and the controller never stamps a timestamp beside it. The
// only way to show "running for 4m" is therefore to watch for the transition
// ourselves.
//
// Keyed by UID rather than by name on purpose. ARC derives runner pod names
// from the scale set, and those names ARE reused across generations, so a
// name-keyed map would hand a fresh runner the start time of a dead one and
// render a job that has been going for hours.
type JobStartTracker struct {
	mu     sync.Mutex
	starts map[types.UID]time.Time
}

// NewJobStartTracker returns an empty tracker.
func NewJobStartTracker() *JobStartTracker {
	return &JobStartTracker{starts: map[types.UID]time.Time{}}
}

// Observe records the first sighting of a job on a runner and returns it.
//
// A runner without a job forgets any earlier sighting: ephemeral runners run
// exactly one job, so a cleared jobId means this runner is finished and its
// entry is dead weight. A nil tracker returns the zero time, which lets
// BuildSnapshot run untracked in tests.
func (t *JobStartTracker) Observe(uid types.UID, hasJob bool, now time.Time) time.Time {
	if t == nil || uid == "" {
		return time.Time{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if !hasJob {
		delete(t.starts, uid)
		return time.Time{}
	}
	if at, ok := t.starts[uid]; ok {
		return at
	}
	t.starts[uid] = now
	return now
}

// Retain drops every entry whose runner no longer exists.
//
// Without this the map grows for the life of the process: a busy fleet churns
// through thousands of ephemeral runners an hour, and a runner deleted while
// still holding a job never gets the cleared-jobId observation that would have
// removed it.
func (t *JobStartTracker) Retain(live map[types.UID]struct{}) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for uid := range t.starts {
		if _, ok := live[uid]; !ok {
			delete(t.starts, uid)
		}
	}
}

// Len reports how many runners are being tracked. Used by tests and by the
// process's own memory reporting.
func (t *JobStartTracker) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.starts)
}
