package appmetrics

import (
	"errors"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/alert"
	"arc-ui/internal/prom"
)

func TestMetricsExposeQueriesStreamsAndAlerts(t *testing.T) {
	t.Parallel()

	state := alert.State{Rules: []alert.RuleStatus{
		{ID: "listener-down", Firing: 2},
		{ID: "startup-p95", Unavailable: "arc-metrics unavailable"},
	}}
	m := New(func() int { return 3 }, func() alert.State { return state })
	m.ObservePrometheus("/api/v1/query_range", 120*time.Millisecond, nil)
	m.ObservePrometheus("/api/v1/query", time.Second, &prom.APIError{Status: 422, Msg: "bad"})
	m.ObservePrometheus("/api/v1/query", time.Second, errors.New("connection refused"))

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	out := string(body)

	assert.Contains(t, out, `arc_ui_prometheus_query_duration_seconds_count{endpoint="query_range"} 1`)
	assert.Contains(t, out, `arc_ui_prometheus_query_errors_total{endpoint="query",kind="api"} 1`)
	assert.Contains(t, out, `arc_ui_prometheus_query_errors_total{endpoint="query",kind="transport"} 1`)
	assert.Contains(t, out, `arc_ui_sse_clients 3`)
	assert.Contains(t, out, `arc_ui_alerts_firing{rule="listener-down"} 2`)
	assert.Contains(t, out, `arc_ui_alert_rule_unavailable{rule="startup-p95"} 1`)
}
