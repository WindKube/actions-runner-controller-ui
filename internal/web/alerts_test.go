package web

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/alert"
	"arc-ui/internal/hub"
)

type fixedAlerts alert.State

func (f fixedAlerts) State() alert.State { return alert.State(f) }

func alertState() fixedAlerts {
	return fixedAlerts{
		Evaluated: now.Add(-5 * time.Second),
		Firing: []alert.Alert{
			{Rule: "listener-down", Title: "listener down", Severity: alert.Critical, Set: "arc-ubuntu",
				Message: "listener is not running", Since: now.Add(-4 * time.Minute), FiredAt: now.Add(-2 * time.Minute)},
			{Rule: "queued-at-max", Title: "jobs queued at max runners", Severity: alert.Warning, Set: "arc-arm64",
				Message: "3 jobs queued with 4/4 runners", Since: now.Add(-2 * time.Hour), FiredAt: now.Add(-time.Hour)},
		},
		Resolved: []alert.Alert{
			{Rule: "oom-killed", Title: "runners OOMKilled", Set: "arc-ubuntu", Message: "1 runners OOMKilled in the last 1h",
				Since: now.Add(-3 * time.Hour), FiredAt: now.Add(-3 * time.Hour), ResolvedAt: now.Add(-20 * time.Minute)},
		},
		Rules: []alert.RuleStatus{
			{ID: "listener-down", Title: "listener down", Severity: alert.Critical, Threshold: "not running for 2m", Firing: 1},
			{ID: "startup-p95", Title: "slow job startup", Threshold: "p95 > 2m over 30m", Unavailable: "arc-metrics unavailable: no gha_* listener series"},
			{ID: "oom-killed", Title: "runners OOMKilled", Threshold: "any in the last 1h"},
		},
	}
}

func alertBuilder() *Builder {
	b := testBuilder()
	b.Alerts = alertState()
	return b
}

func TestAlertsPageListsFiringRulesAndResolved(t *testing.T) {
	t.Parallel()
	v := alertBuilder().AlertsPage(context.Background(), Signals{}, now)
	html := renderComponent(t, v.Page, AlertsPage(v))

	assert.Contains(t, html, "listener down")
	assert.Contains(t, html, `href="/runnersets/arc-ubuntu"`)
	assert.Contains(t, html, "for 4m 0s")
	assert.Contains(t, html, "resolved 20m ago")
	assert.Contains(t, html, "arc-metrics unavailable", "a rule that cannot run says so instead of reading as ok")
	assert.Contains(t, html, ">unavailable<")
	assert.Contains(t, html, ">1 firing<")
}

func TestAlertsPageWithoutAnEngineSaysSo(t *testing.T) {
	t.Parallel()
	v := testBuilder().AlertsPage(context.Background(), Signals{}, now)
	assert.Contains(t, renderComponent(t, v.Page, AlertsPage(v)), "alerting is not configured")
}

func TestARefiredAlertGetsAFreshDismissSignal(t *testing.T) {
	t.Parallel()
	first := alert.Alert{Rule: "listener-down", Set: "arc-ubuntu", FiredAt: now.Add(-50 * time.Minute)}
	again := first
	again.FiredAt = now.Add(-time.Minute)
	assert.NotEqual(t, alertRow(first, now).Signal, alertRow(again, now).Signal,
		"a dismissed toast must not hide the next firing of the same rule and set")
	assert.Equal(t, alertRow(again, now).Signal, alertRow(again, now.Add(time.Minute)).Signal,
		"the same firing keeps its signal across refreshes")
}

func TestEveryPageCountsFiringAlertsAndToastsOnlyRecentOnes(t *testing.T) {
	t.Parallel()
	html := renderOverviewWith(t, alertBuilder())

	_, tabs, ok := strings.Cut(html, `id="tabs"`)
	require.True(t, ok)
	tabs, _, _ = strings.Cut(tabs, "</nav>")
	assert.Contains(t, tabs, ">2</span>", "the alerts tab carries the firing count")

	_, toasts, ok := strings.Cut(html, `id="toasts"`)
	require.True(t, ok)
	assert.Contains(t, toasts, "listener is not running", "fired 2m ago: toasted")
	assert.NotContains(t, toasts, "3 jobs queued", "fired an hour ago: listed, not toasted")
	assert.Contains(t, toasts, "__ifmissing", "dismissal survives a re-render")
}

func TestStreamsPatchTheAlertChrome(t *testing.T) {
	t.Parallel()
	handler := testHandler(hub.New())
	handler.Builder = alertBuilder()
	srv := httptest.NewServer(http.HandlerFunc(handler.StreamJobs))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	markup := readElementsUntilSignal(t, bufio.NewReader(resp.Body))
	assert.Contains(t, markup, `id="tabs"`)
	assert.Contains(t, markup, `id="toasts"`)
	assert.Contains(t, markup, `id="health"`)
}
