package prom

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
)

// fakeProm answers queries from a table keyed by the PromQL text.
type fakeProm struct {
	mu      sync.Mutex
	answers map[string]string
	hits    map[string]int
	lastReq *http.Request
	status  int
}

func newFake(answers map[string]string) (*fakeProm, *httptest.Server) {
	f := &fakeProm{answers: answers, hits: map[string]int{}, status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		q := r.Form.Get("query")
		f.mu.Lock()
		f.hits[r.URL.Path+"|"+q]++
		f.lastReq = r.Clone(r.Context())
		body, ok := f.answers[q]
		status := f.status
		f.mu.Unlock()
		if !ok {
			body = emptyVector
			if r.URL.Path == "/api/v1/query_range" {
				body = `{"status":"success","data":{"resultType":"matrix","result":[]}}`
			}
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	return f, srv
}

func (f *fakeProm) hitsFor(path, q string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[path+"|"+q]
}

const emptyVector = `{"status":"success","data":{"resultType":"vector","result":[]}}`

func vector(value string) string {
	return `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"name":"linux"},"value":[1700000000.5,"` + value + `"]}]}}`
}

func newClient(t *testing.T, url string, mod func(*Config)) *Client {
	t.Helper()
	cfg := Config{URL: url, CacheTTL: time.Minute}
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	require.NoError(t, err)
	return c
}

func TestQueryParsesAVector(t *testing.T) {
	t.Parallel()
	_, srv := newFake(map[string]string{"up": vector("3")})
	defer srv.Close()

	rows, err := newClient(t, srv.URL, nil).Query(t.Context(), "up")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "linux", rows[0].Labels["name"])
	assert.InDelta(t, 3.0, rows[0].Value, 0)
}

func TestQueryRangeParsesAMatrixAndAlignsTheWindow(t *testing.T) {
	t.Parallel()
	f, srv := newFake(map[string]string{
		"rate(x[1m])": `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"pod":"a"},"values":[[60,"1"],[120,"2.5"]]}]}}`,
	})
	defer srv.Close()

	start := time.Unix(61, 0)
	series, err := newClient(t, srv.URL, nil).QueryRange(t.Context(), "rate(x[1m])", Range{Start: start, End: start.Add(time.Minute), Step: time.Minute})
	require.NoError(t, err)
	require.Len(t, series, 1)
	require.Len(t, series[0].Points, 2)
	assert.Equal(t, time.Unix(120, 0).UTC(), series[0].Points[1].At)
	assert.InDelta(t, 2.5, series[0].Points[1].Value, 0)

	assert.Equal(t, "60", f.lastReq.Form.Get("start"), "start is aligned down to the step")
	assert.Equal(t, "120", f.lastReq.Form.Get("end"))
	assert.Equal(t, "60", f.lastReq.Form.Get("step"))
}

func TestInvalidRangeIsRejectedLocally(t *testing.T) {
	t.Parallel()
	c := newClient(t, "http://127.0.0.1:1", nil)
	_, err := c.QueryRange(t.Context(), "x", Range{Start: time.Unix(100, 0), End: time.Unix(50, 0), Step: time.Second})
	require.Error(t, err)
}

func TestAPIErrorsSurfaceTheirMessage(t *testing.T) {
	t.Parallel()
	f, srv := newFake(map[string]string{"bad(": `{"status":"error","errorType":"bad_data","error":"parse error"}`})
	defer srv.Close()
	f.status = http.StatusBadRequest

	_, err := newClient(t, srv.URL, nil).Query(t.Context(), "bad(")
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "bad_data", apiErr.Type)
	assert.Contains(t, err.Error(), "parse error")
}

func TestNonJSONBodyIsAnError(t *testing.T) {
	t.Parallel()
	f, srv := newFake(map[string]string{"x": "<html>bad gateway</html>"})
	defer srv.Close()
	f.status = http.StatusBadGateway

	_, err := newClient(t, srv.URL, nil).Query(t.Context(), "x")
	require.ErrorContains(t, err, "bad gateway")
}

func TestWrongResultTypeIsAnError(t *testing.T) {
	t.Parallel()
	_, srv := newFake(map[string]string{"x": `{"status":"success","data":{"resultType":"scalar","result":[1,"1"]}}`})
	defer srv.Close()
	_, err := newClient(t, srv.URL, nil).Query(t.Context(), "x")
	require.ErrorContains(t, err, "scalar")
}

func TestAuthAndHeaders(t *testing.T) {
	t.Parallel()

	t.Run("bearer token and tenant header", func(t *testing.T) {
		t.Parallel()
		f, srv := newFake(map[string]string{"up": vector("1")})
		defer srv.Close()
		c := newClient(t, srv.URL, func(c *Config) {
			c.BearerToken = "s3cret"
			c.Headers = map[string]string{"X-Scope-OrgID": "team-a"}
		})
		_, err := c.Query(t.Context(), "up")
		require.NoError(t, err)
		assert.Equal(t, "Bearer s3cret", f.lastReq.Header.Get("Authorization"))
		assert.Equal(t, "team-a", f.lastReq.Header.Get("X-Scope-OrgID"))
	})

	t.Run("bearer token file is re-read", func(t *testing.T) {
		t.Parallel()
		f, srv := newFake(map[string]string{"up": vector("1"), "up2": vector("1")})
		defer srv.Close()
		path := filepath.Join(t.TempDir(), "token")
		require.NoError(t, os.WriteFile(path, []byte("one\n"), 0o600))
		c := newClient(t, srv.URL, func(c *Config) { c.BearerTokenFile = path })

		_, err := c.Query(t.Context(), "up")
		require.NoError(t, err)
		assert.Equal(t, "Bearer one", f.lastReq.Header.Get("Authorization"))

		require.NoError(t, os.WriteFile(path, []byte("two"), 0o600))
		_, err = c.Query(t.Context(), "up2")
		require.NoError(t, err)
		assert.Equal(t, "Bearer two", f.lastReq.Header.Get("Authorization"))
	})

	t.Run("basic auth", func(t *testing.T) {
		t.Parallel()
		f, srv := newFake(map[string]string{"up": vector("1")})
		defer srv.Close()
		c := newClient(t, srv.URL, func(c *Config) { c.Username, c.Password = "u", "p" })
		_, err := c.Query(t.Context(), "up")
		require.NoError(t, err)
		u, p, ok := f.lastReq.BasicAuth()
		require.True(t, ok)
		assert.Equal(t, "u", u)
		assert.Equal(t, "p", p)
	})
}

func TestNewRejectsBadConfig(t *testing.T) {
	t.Parallel()
	_, err := New(Config{URL: "prometheus:9090"})
	require.Error(t, err)

	_, err = New(Config{URL: "http://p", CAFile: filepath.Join(t.TempDir(), "missing.pem")})
	require.Error(t, err)

	bad := filepath.Join(t.TempDir(), "bad.pem")
	require.NoError(t, os.WriteFile(bad, []byte("not pem"), 0o600))
	_, err = New(Config{URL: "http://p", CAFile: bad})
	require.ErrorContains(t, err, "no PEM")
}

func TestCacheSharesAnswersAndCollapsesConcurrentQueries(t *testing.T) {
	t.Parallel()
	f, srv := newFake(map[string]string{"up": vector("1")})
	defer srv.Close()
	c := newClient(t, srv.URL, nil)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, err := c.Query(t.Context(), "up")
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	_, err := c.Query(t.Context(), "up")
	require.NoError(t, err)
	assert.Equal(t, 1, f.hitsFor("/api/v1/query", "up"))
}

func TestErrorsAreNotCached(t *testing.T) {
	t.Parallel()
	f, srv := newFake(map[string]string{"up": `{"status":"error","error":"overloaded"}`})
	defer srv.Close()
	c := newClient(t, srv.URL, nil)

	_, err := c.Query(t.Context(), "up")
	require.Error(t, err)
	f.mu.Lock()
	f.answers["up"] = vector("1")
	f.mu.Unlock()
	_, err = c.Query(t.Context(), "up")
	require.NoError(t, err)
}

func TestCacheDisabled(t *testing.T) {
	t.Parallel()
	f, srv := newFake(map[string]string{"up": vector("1")})
	defer srv.Close()
	c := newClient(t, srv.URL, func(c *Config) { c.CacheTTL = 0 })
	for range 3 {
		_, err := c.Query(t.Context(), "up")
		require.NoError(t, err)
	}
	assert.Equal(t, 3, f.hitsFor("/api/v1/query", "up"))
}

func TestObserveSeesEveryRequest(t *testing.T) {
	t.Parallel()
	_, srv := newFake(map[string]string{"up": vector("1")})
	defer srv.Close()
	c := newClient(t, srv.URL, nil)
	var calls atomic.Int32
	var lastErr atomic.Value
	c.Observe(func(endpoint string, _ time.Duration, err error) {
		calls.Add(1)
		if err != nil {
			lastErr.Store(err)
		}
		assert.Equal(t, "/api/v1/query", endpoint)
	})
	_, err := c.Query(t.Context(), "up")
	require.NoError(t, err)
	assert.Equal(t, int32(1), calls.Load())
	assert.Nil(t, lastErr.Load())
}

func TestUnreachableIsATransportError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := newClient(t, url, nil).Query(t.Context(), "up")
	require.Error(t, err)
	var apiErr *APIError
	assert.False(t, errors.As(err, &apiErr))
}

type sinkFunc func(fleet.Source)

func (f sinkFunc) SetSource(s fleet.Source) { f(s) }

var testSchema = Schema{
	ScaleSetLabel:    "actions_github_com_scale_set_name",
	KSMScaleSetLabel: "label_actions_github_com_scale_set_name",
	RunnerNamespaces: []string{"arc-runners", "arc.gpu"},
}

func TestNamespaceMatcherQuotesRegexMeta(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `namespace=~"arc-runners|arc\\.gpu"`, testSchema.NamespaceMatcher())
	assert.Equal(t, `namespace!=""`, Schema{}.NamespaceMatcher())
}

func probeBy(srcs []fleet.Source) map[string]fleet.Source {
	out := map[string]fleet.Source{}
	for _, s := range srcs {
		out[s.Name] = s
	}
	return out
}

func TestProbeReportsEachSeriesGroup(t *testing.T) {
	t.Parallel()
	checks := testSchema.checks()
	answers := map[string]string{"vector(1)": vector("1")}
	for _, chk := range checks {
		answers[chk.query] = vector("4")
	}
	// kube_pod_labels exists but without the scale-set label: the allowlist is missing.
	delete(answers, checks[3].query)
	answers["count(kube_pod_labels)"] = vector("12")
	// No listener series at all.
	delete(answers, checks[0].query)

	_, srv := newFake(answers)
	defer srv.Close()

	got := probeBy(NewProber(newClient(t, srv.URL, nil), testSchema, time.Minute, nil, zerolog.Nop()).Probe(t.Context()))
	assert.True(t, got[fleet.SourcePrometheus].Available)
	assert.True(t, got[fleet.SourceControllerMetrics].Available)
	assert.True(t, got[fleet.SourceCAdvisor].Available)
	assert.False(t, got[fleet.SourceARCMetrics].Available)
	assert.Contains(t, got[fleet.SourceARCMetrics].Reason, "no gha_* listener series")
	assert.False(t, got[fleet.SourceKSM].Available)
	assert.Contains(t, got[fleet.SourceKSM].Reason, "--metric-labels-allowlist")
}

func TestProbeDistinguishesAMissingLabelFromMissingSeries(t *testing.T) {
	t.Parallel()
	_, srv := newFake(map[string]string{
		"vector(1)":                vector("1"),
		"count(gha_assigned_jobs)": vector("2"),
	})
	defer srv.Close()
	got := probeBy(NewProber(newClient(t, srv.URL, nil), testSchema, time.Minute, nil, zerolog.Nop()).Probe(t.Context()))
	assert.Contains(t, got[fleet.SourceARCMetrics].Reason, "carry no actions_github_com_scale_set_name label")
	assert.Contains(t, got[fleet.SourceKSM].Reason, "no kube-state-metrics series")
}

func TestProbeWithPrometheusDownMarksEverythingUnavailable(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	got := NewProber(newClient(t, url, nil), testSchema, time.Minute, nil, zerolog.Nop()).Probe(t.Context())
	require.Len(t, got, 5)
	for _, s := range got {
		assert.False(t, s.Available, s.Name)
	}
	assert.True(t, strings.HasPrefix(probeBy(got)[fleet.SourcePrometheus].Reason, "unreachable"))
}

func TestProberRunReportsToTheSinkUntilCancelled(t *testing.T) {
	t.Parallel()
	_, srv := newFake(map[string]string{"vector(1)": vector("1")})
	defer srv.Close()

	var mu sync.Mutex
	seen := map[string]bool{}
	ctx, cancel := context.WithCancel(t.Context())
	p := NewProber(newClient(t, srv.URL, nil), testSchema, time.Hour, sinkFunc(func(s fleet.Source) {
		mu.Lock()
		seen[s.Name] = true
		if len(seen) == 5 {
			cancel()
		}
		mu.Unlock()
	}), zerolog.Nop())

	err := p.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, seen, 5)
}
