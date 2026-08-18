package api

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/config"
	"arc-ui/internal/fleet"
	"arc-ui/internal/hub"
	"arc-ui/internal/web"
)

func testConfig(addr string) config.Config {
	return config.Config{
		HTTPAddr: addr,
		// Both short: the drain semantics are what is under test, not the wait.
		PreStopDelay:    50 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		ScrapeInterval:  15 * time.Second,
	}
}

func testServer(t *testing.T, ready map[string]Checker) (*Server, string) {
	t.Helper()

	// Bind first so the test knows the port without racing the goroutine.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	assets, err := web.NewAssets()
	require.NoError(t, err)

	handler := &web.Handler{
		Builder: &web.Builder{
			Fleet:   web.SnapshotFunc(func() fleet.Snapshot { return fleet.Snapshot{At: time.Now()} }),
			History: web.NoHistory{},
			Version: "test",
			CSS:     assets.CSS(),
			JS:      assets.JS(),
		},
		Hub:       hub.New(),
		Log:       zerolog.Nop(),
		Heartbeat: time.Hour,
		Streams:   web.NewStreamRegistry(),
	}

	srv := New(Options{
		Config:  testConfig(addr),
		Log:     zerolog.Nop(),
		Handler: handler,
		Assets:  assets,
		Ready:   ready,
	})

	go func() {
		assert.NoError(t, srv.Start(), "serve")
	}()

	base := "http://" + addr
	waitFor(t, base+"/healthz")
	return srv, base
}

func waitFor(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx // bounded by the loop deadline
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("server never came up at %s", url)
}

func TestShutdownReturnsPromptlyWithAStreamOpen(t *testing.T) {
	t.Parallel()

	srv, base := testServer(t, nil)

	// An SSE handler does not return until its stream ends, and http.Shutdown
	// waits for active handlers. Without the registry closing streams from the
	// on-shutdown hook, this would block for the full ShutdownTimeout — which
	// on a real deployment means every rolling update stalls for its duration.
	resp, err := http.Get(base + "/stream") //nolint:noctx // closed by shutdown
	require.NoError(t, err)
	defer resp.Body.Close()

	// Make sure the stream is actually established before shutting down.
	buf := make([]byte, 1)
	_, err = resp.Body.Read(buf)
	require.NoError(t, err, "stream never produced data")

	start := time.Now()
	require.NoError(t, srv.Shutdown(context.Background()), "shutdown")

	// PreStopDelay is 50ms here; anything near ShutdownTimeout means the
	// stream was not closed and Shutdown waited it out.
	assert.Less(t, time.Since(start), 2*time.Second,
		"the open stream was not closed")
}

func TestReadyzReportsFailingDependencies(t *testing.T) {
	t.Parallel()

	srv, base := testServer(t, map[string]Checker{
		"informers": func(context.Context) error { return errors.New("still syncing") },
		"store":     func(context.Context) error { return nil },
	})
	defer func() { _ = srv.Shutdown(context.Background()) }()

	resp, err := http.Get(base + "/readyz") //nolint:noctx // test client
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
		"want 503 while a dependency is failing")

	body, _ := io.ReadAll(resp.Body)
	// The reason has to reach the body: "readyz is red" without a cause means
	// somebody has to go read logs to learn what every probe already knows.
	assert.Contains(t, string(body), "still syncing",
		"readyz body should name the failing check")
}

func TestLivezNeverDependsOnTheCluster(t *testing.T) {
	t.Parallel()

	// Every readiness check fails, yet liveness must stay green. Tying liveness
	// to the API server means a control-plane blip restarts the pod and forces
	// a full informer re-LIST, making recovery strictly slower than doing
	// nothing.
	srv, base := testServer(t, map[string]Checker{
		"informers": func(context.Context) error { return errors.New("down") },
		"store":     func(context.Context) error { return errors.New("down") },
	})
	defer func() { _ = srv.Shutdown(context.Background()) }()

	for _, path := range []string{"/healthz", "/livez"} {
		resp, err := http.Get(base + path) //nolint:noctx // test client
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode,
			"%s must stay green regardless of dependencies", path)
	}
}

func TestReadinessIsWithdrawnBeforeTheListenerCloses(t *testing.T) {
	t.Parallel()

	srv, base := testServer(t, nil)

	// Shutdown holds the process open for PreStopDelay after flipping
	// readiness, so a load balancer has a window to observe the failing probe
	// and stop routing. Poll during that window.
	done := make(chan error, 1)
	go func() { done <- srv.Shutdown(context.Background()) }()

	var sawDraining bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/readyz") //nolint:noctx // bounded loop
		if err != nil {
			break // listener closed; the drain window has passed
		}
		code := resp.StatusCode
		resp.Body.Close()
		if code == http.StatusServiceUnavailable {
			sawDraining = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	require.NoError(t, <-done, "shutdown")
	assert.True(t, sawDraining, "readiness never went red before the listener closed")
}

func TestAssetsAreServedUnderTheHashedPath(t *testing.T) {
	t.Parallel()

	srv, base := testServer(t, nil)
	defer func() { _ = srv.Shutdown(context.Background()) }()

	assets, err := web.NewAssets()
	require.NoError(t, err)

	resp, err := http.Get(base + assets.JS()) //nolint:noctx // test client
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode, "for %s", assets.JS())
	assert.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "text/javascript"),
		"Content-Type = %q", resp.Header.Get("Content-Type"))
}
