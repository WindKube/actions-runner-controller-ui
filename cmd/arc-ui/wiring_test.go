package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/config"
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

func TestRetentionFromCarriesEveryWindow(t *testing.T) {
	t.Parallel()

	// Six fields copied one by one, which is exactly the shape a mistake hides
	// in: every duration here is distinct, so a crossed pair cannot pass.
	ret := retentionFrom(config.Config{
		RetentionRunnerRaw:  1 * time.Minute,
		RetentionScopeRaw:   2 * time.Minute,
		RetentionScope1m:    3 * time.Minute,
		RetentionScope5m:    4 * time.Minute,
		RetentionScope1h:    5 * time.Minute,
		RetentionJobSamples: 6 * time.Minute,
	})

	assert.Equal(t, 1*time.Minute, ret.RunnerRaw, "runner raw")
	assert.Equal(t, 2*time.Minute, ret.ScopeRaw, "scope raw")
	assert.Equal(t, 3*time.Minute, ret.Scope1m, "1m")
	assert.Equal(t, 4*time.Minute, ret.Scope5m, "5m")
	assert.Equal(t, 5*time.Minute, ret.Scope1h, "1h")
	assert.Equal(t, 6*time.Minute, ret.JobSamples, "job samples")
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
