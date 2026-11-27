package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Load reads the process environment, so nothing here may run in parallel.

func withPrometheus(t *testing.T) {
	t.Helper()
	t.Setenv("ARC_UI_PROMETHEUS_URL", "http://prometheus:9090")
}

func TestLoadRejects(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"negative prestop delay", map[string]string{"ARC_UI_PRESTOP_DELAY": "-1s"}, "ARC_UI_PRESTOP_DELAY"},
		{"zero shutdown timeout", map[string]string{"ARC_UI_SHUTDOWN_TIMEOUT": "0s"}, "ARC_UI_SHUTDOWN_TIMEOUT"},
		{"negative shutdown timeout", map[string]string{"ARC_UI_SHUTDOWN_TIMEOUT": "-20s"}, "ARC_UI_SHUTDOWN_TIMEOUT"},
		{"tiny job retention", map[string]string{"ARC_UI_JOB_RETENTION": "10m"}, "ARC_UI_JOB_RETENTION"},
		{"relative grafana url", map[string]string{"ARC_UI_GRAFANA_URL": "grafana:3000"}, "ARC_UI_GRAFANA_URL"},
		{"empty github url", map[string]string{"ARC_UI_GITHUB_URL": "ftp://github.com"}, "ARC_UI_GITHUB_URL"},
		{"relative prometheus url", map[string]string{"ARC_UI_PROMETHEUS_URL": "prometheus:9090"}, "ARC_UI_PROMETHEUS_URL"},
		{"two token inputs", map[string]string{"ARC_UI_PROMETHEUS_BEARER_TOKEN": "a", "ARC_UI_PROMETHEUS_BEARER_TOKEN_FILE": "/t"}, "BEARER_TOKEN"},
		{"zero prometheus timeout", map[string]string{"ARC_UI_PROMETHEUS_TIMEOUT": "0s"}, "ARC_UI_PROMETHEUS_TIMEOUT"},
		{"sub-second refresh", map[string]string{"ARC_UI_REFRESH_INTERVAL": "100ms"}, "ARC_UI_REFRESH_INTERVAL"},
		{"bad log format", map[string]string{"ARC_UI_LOG_FORMAT": "xml"}, "ARC_UI_LOG_FORMAT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withPrometheus(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, _, err := Load()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr, "the error must name what to fix")
		})
	}
}

func TestLoadRequiresPrometheus(t *testing.T) {
	_, _, err := Load()
	require.ErrorContains(t, err, "ARC_UI_PROMETHEUS_URL is required")
}

// Zero means "do not wait", which is right for a local run; a zero drain
// budget is not.
func TestLoadAcceptsZeroPreStopDelay(t *testing.T) {
	withPrometheus(t)
	t.Setenv("ARC_UI_PRESTOP_DELAY", "0s")

	cfg, _, err := Load()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.PreStopDelay)
}

func TestLoadDefaultsAreSelfConsistent(t *testing.T) {
	withPrometheus(t)
	cfg, _, err := Load()

	require.NoError(t, err, "the built-in defaults must satisfy Load's own checks")
	assert.Positive(t, cfg.ShutdownTimeout)
	assert.NotEmpty(t, cfg.DBPath)
	assert.Positive(t, cfg.KubeQPS)
	assert.Positive(t, cfg.KubeBurst)
	assert.Equal(t, 15*time.Second, cfg.RefreshInterval)
	assert.Equal(t, "name", cfg.PrometheusScaleSetLabel)
	assert.Equal(t, "label_actions_github_com_scale_set_name", cfg.PrometheusKSMScaleSetLabel)
}

func TestAllNamespaces(t *testing.T) {
	withPrometheus(t)
	t.Setenv("ARC_UI_NAMESPACES", " ")

	cfg, _, err := Load()
	require.NoError(t, err)
	assert.Empty(t, cfg.Namespaces, "a blank ARC_UI_NAMESPACES means all namespaces")
}

func TestLoadPrometheusHeaders(t *testing.T) {
	t.Setenv("ARC_UI_PROMETHEUS_URL", "https://mimir.example.com/prometheus")
	t.Setenv("ARC_UI_PROMETHEUS_HEADERS", "X-Scope-OrgID=team-a,X-Extra=1")

	cfg, _, err := Load()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"X-Scope-OrgID": "team-a", "X-Extra": "1"}, cfg.PrometheusHeaders)
}
