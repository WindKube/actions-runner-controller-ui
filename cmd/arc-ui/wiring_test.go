package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
	"arc-ui/internal/prom"
	"arc-ui/internal/promql"
	"arc-ui/internal/store"
)

func TestLocalAddrMakesAWildcardBindDialable(t *testing.T) {
	t.Parallel()

	// The healthcheck runs inside the container and dials what the server was
	// told to bind. A wildcard bind is not an address, so probing it verbatim
	// fails on exactly the deployment the distroless healthcheck exists for.
	for _, tc := range []struct{ name, addr, want string }{
		{"ipv4 wildcard", "0.0.0.0:8080", "127.0.0.1:8080"},
		{"ipv6 wildcard", "[::]:8080", "127.0.0.1:8080"},
		{"port only", ":8080", "127.0.0.1:8080"},
		{"a real host is left alone", "10.1.2.3:9000", "10.1.2.3:9000"},
		{"loopback is already dialable", "127.0.0.1:8080", "127.0.0.1:8080"},
		{"nonsense is returned unchanged", "not-an-address", "not-an-address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, localAddr(tc.addr))
		})
	}
}

func TestHealthcheckReportsWhatTheServerSaid(t *testing.T) {
	t.Parallel()

	var status int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/healthz", r.URL.Path, "the probe must hit the health endpoint")
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	probe := func(t *testing.T) error {
		t.Helper()
		cmd := newHealthcheckCmd()
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(&bytes.Buffer{})
		cmd.SetArgs([]string{"--addr", srv.Listener.Addr().String(), "--timeout", "5s"})
		return cmd.ExecuteContext(t.Context())
	}

	status = http.StatusOK
	assert.NoError(t, probe(t), "a healthy server must exit zero")

	status = http.StatusServiceUnavailable
	assert.ErrorContains(t, probe(t), "status 503", "an unhealthy server must name the status it gave")
}

func TestHealthcheckFailsWhenNothingIsListening(t *testing.T) {
	t.Parallel()

	// A dead server is the case the container probe exists to catch, and it
	// arrives as a dial error rather than a status.
	srv := httptest.NewServer(http.NotFoundHandler())
	addr := srv.Listener.Addr().String()
	srv.Close()

	cmd := newHealthcheckCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"--addr", addr, "--timeout", "2s"})
	assert.ErrorContains(t, cmd.ExecuteContext(t.Context()), "healthcheck http://"+addr+"/healthz")
}

func TestVersionCommandPrintsTheStampedVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	root := newRootCmd()
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"version"})

	require.NoError(t, root.ExecuteContext(t.Context()), "version must not fail")
	assert.Equal(t, version+"\n", out.String())
}

type fakeJobStore struct {
	mu     sync.Mutex
	closed []store.JobRecord
	usage  map[int][2]float64
	stored chan int
}

func (f *fakeJobStore) Observe(context.Context, fleet.Snapshot) ([]store.JobRecord, error) {
	return f.closed, nil
}

func (f *fakeJobStore) SetUsage(_ context.Context, id int, cpu, mem float64) error {
	f.mu.Lock()
	f.usage[id] = [2]float64{cpu, mem}
	f.mu.Unlock()
	f.stored <- id
	return nil
}

// laggingQuerier has no samples for r1 until its third read, and never for r2.
type laggingQuerier struct {
	mu    sync.Mutex
	reads int
	at    []time.Time
}

func (l *laggingQuerier) QueryAt(_ context.Context, q string, at time.Time) ([]prom.Sample, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.at = append(l.at, at)
	if !strings.Contains(q, `pod="r1"`) {
		return nil, nil
	}
	l.reads++
	if l.reads <= 4 {
		return nil, nil
	}
	if strings.Contains(q, "cpu") {
		return []prom.Sample{{Value: 42}}, nil
	}
	return []prom.Sample{{Value: 1 << 30}}, nil
}

func TestJobObserverRetriesUntilPrometheusHasTheJob(t *testing.T) {
	t.Parallel()

	finished := time.Now().Add(-10 * time.Minute)
	db := &fakeJobStore{
		closed: []store.JobRecord{
			{ID: 7, Runner: "r1", StartedAt: finished.Add(-time.Minute), FinishedAt: finished},
			{ID: 8, Runner: "r2", StartedAt: finished.Add(-time.Minute), FinishedAt: finished},
		},
		usage:  map[int][2]float64{},
		stored: make(chan int, 2),
	}
	q := &laggingQuerier{}
	delays := []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	observe := jobObserver(t.Context(), db, q, promql.Queries{}, time.Second, delays, zerolog.Nop())
	require.NoError(t, observe(t.Context(), fleet.Snapshot{At: time.Now()}))

	select {
	case id := <-db.stored:
		assert.Equal(t, 7, id)
	case <-time.After(5 * time.Second):
		require.Fail(t, "usage was never stored")
	}
	db.mu.Lock()
	assert.Equal(t, [2]float64{42, 1 << 30}, db.usage[7])
	db.mu.Unlock()

	select {
	case id := <-db.stored:
		assert.Failf(t, "an empty answer was stored", "job %d", id)
	case <-time.After(50 * time.Millisecond):
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, at := range q.at {
		assert.True(t, at.After(finished), "usage is read at the job's end, not now: %s", at)
		assert.True(t, at.Before(finished.Add(2*time.Minute)))
	}
}

func TestSuperviseReportsOnlyUnexpectedStops(t *testing.T) {
	t.Parallel()

	logged := func(t *testing.T, err error) string {
		t.Helper()
		var buf bytes.Buffer
		supervise(t.Context(), zerolog.New(&buf), "collector", func(context.Context) error { return err })
		return buf.String()
	}

	assert.Empty(t, logged(t, nil), "a loop that returned cleanly is not news")
	assert.Empty(t, logged(t, context.Canceled), "cancellation is how shutdown works, not a fault")
	assert.Contains(t, logged(t, errors.New("informer died")), "collector stopped",
		"a real failure has to name the loop that stopped")
}
