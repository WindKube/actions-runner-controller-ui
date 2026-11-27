// Package config loads and validates the process configuration from the
// environment, as one flat struct.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/samber/lo"
)

type Config struct {
	HTTPAddr  string `env:"ARC_UI_HTTP_ADDR" envDefault:"0.0.0.0:8080"`
	LogLevel  string `env:"ARC_UI_LOG_LEVEL" envDefault:"info"`
	LogFormat string `env:"ARC_UI_LOG_FORMAT" envDefault:"json"`

	SentryDSN         string  `env:"ARC_UI_SENTRY_DSN"`
	SentryEnvironment string  `env:"ARC_UI_SENTRY_ENVIRONMENT" envDefault:"production"`
	SentrySampleRate  float64 `env:"ARC_UI_SENTRY_SAMPLE_RATE" envDefault:"1.0"`

	// KubeAPIURL overrides the API server address; empty means in-cluster,
	// then kubeconfig.
	KubeAPIURL  string  `env:"KUBE_API_URL"`
	Kubeconfig  string  `env:"ARC_UI_KUBECONFIG"`
	KubeContext string  `env:"ARC_UI_KUBE_CONTEXT"`
	KubeQPS     float32 `env:"ARC_UI_KUBE_QPS" envDefault:"50"`
	KubeBurst   int     `env:"ARC_UI_KUBE_BURST" envDefault:"100"`

	// Namespaces holding AutoscalingRunnerSets and runners; empty means all.
	Namespaces          []string `env:"ARC_UI_NAMESPACES" envSeparator:"," envDefault:"arc-runners"`
	ControllerNamespace string   `env:"ARC_UI_CONTROLLER_NAMESPACE" envDefault:"arc-systems"`

	PrometheusURL                string            `env:"ARC_UI_PROMETHEUS_URL"`
	PrometheusBearerToken        string            `env:"ARC_UI_PROMETHEUS_BEARER_TOKEN"`
	PrometheusBearerTokenFile    string            `env:"ARC_UI_PROMETHEUS_BEARER_TOKEN_FILE"`
	PrometheusUsername           string            `env:"ARC_UI_PROMETHEUS_USERNAME"`
	PrometheusPassword           string            `env:"ARC_UI_PROMETHEUS_PASSWORD"`
	PrometheusHeaders            map[string]string `env:"ARC_UI_PROMETHEUS_HEADERS" envSeparator:"," envKeyValSeparator:"="`
	PrometheusCAFile             string            `env:"ARC_UI_PROMETHEUS_CA_FILE"`
	PrometheusInsecureSkipVerify bool              `env:"ARC_UI_PROMETHEUS_INSECURE_SKIP_VERIFY"`
	PrometheusTimeout            time.Duration     `env:"ARC_UI_PROMETHEUS_TIMEOUT" envDefault:"10s"`
	PrometheusScaleSetLabel      string            `env:"ARC_UI_PROMETHEUS_SCALE_SET_LABEL" envDefault:"name"`
	PrometheusKSMScaleSetLabel   string            `env:"ARC_UI_PROMETHEUS_KSM_SCALE_SET_LABEL" envDefault:"label_actions_github_com_scale_set_name"`
	PrometheusProbeInterval      time.Duration     `env:"ARC_UI_PROMETHEUS_PROBE_INTERVAL" envDefault:"1m"`

	// RefreshInterval is how often the UI re-queries Prometheus, and how long a
	// query answer is shared between browsers.
	RefreshInterval time.Duration `env:"ARC_UI_REFRESH_INTERVAL" envDefault:"15s"`

	DBPath       string        `env:"ARC_UI_DB_PATH" envDefault:"/data/arc-ui.db"`
	JobRetention time.Duration `env:"ARC_UI_JOB_RETENTION" envDefault:"720h"`

	// GitHubOrg labels the breadcrumb; empty derives it from githubConfigUrl.
	GitHubOrg string `env:"ARC_UI_GITHUB_ORG"`
	GitHubURL string `env:"ARC_UI_GITHUB_URL" envDefault:"https://github.com"`

	GrafanaURL        string `env:"ARC_UI_GRAFANA_URL"`
	GrafanaDatasource string `env:"ARC_UI_GRAFANA_DATASOURCE"`

	AlertQueuedAtMaxFor      time.Duration `env:"ARC_UI_ALERT_QUEUED_AT_MAX_FOR" envDefault:"15m"`
	AlertListenerDownFor     time.Duration `env:"ARC_UI_ALERT_LISTENER_DOWN_FOR" envDefault:"2m"`
	AlertOOMWindow           time.Duration `env:"ARC_UI_ALERT_OOM_WINDOW" envDefault:"1h"`
	AlertFailedRunnersRise   int           `env:"ARC_UI_ALERT_FAILED_RUNNERS_RISE" envDefault:"3"`
	AlertFailedRunnersWindow time.Duration `env:"ARC_UI_ALERT_FAILED_RUNNERS_WINDOW" envDefault:"15m"`
	AlertStartupP95          time.Duration `env:"ARC_UI_ALERT_STARTUP_P95" envDefault:"2m"`
	AlertStartupWindow       time.Duration `env:"ARC_UI_ALERT_STARTUP_WINDOW" envDefault:"30m"`

	ShutdownTimeout time.Duration `env:"ARC_UI_SHUTDOWN_TIMEOUT" envDefault:"20s"`
	// PreStopDelay keeps serving after readiness flips; it must cover two
	// readiness periods or clients see 502s.
	PreStopDelay time.Duration `env:"ARC_UI_PRESTOP_DELAY" envDefault:"8s"`
}

// Warning is a non-fatal configuration problem for the caller to log.
type Warning string

// Load parses and validates the environment. A non-nil error means the process
// must not start.
func Load() (Config, []Warning, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, nil, fmt.Errorf("parse env: %w", err)
	}

	var warns []Warning

	if cfg.LogFormat != "json" && cfg.LogFormat != "console" {
		return Config{}, warns, fmt.Errorf("ARC_UI_LOG_FORMAT=%q must be json or console", cfg.LogFormat)
	}
	if cfg.DBPath == "" {
		return Config{}, warns, errors.New("ARC_UI_DB_PATH must not be empty")
	}
	if cfg.JobRetention < time.Hour {
		return Config{}, warns, fmt.Errorf("ARC_UI_JOB_RETENTION=%s must be at least 1h", cfg.JobRetention)
	}
	if err := absoluteURL("ARC_UI_GITHUB_URL", cfg.GitHubURL); err != nil {
		return Config{}, warns, err
	}
	if cfg.GrafanaURL != "" {
		if err := absoluteURL("ARC_UI_GRAFANA_URL", cfg.GrafanaURL); err != nil {
			return Config{}, warns, err
		}
	}
	if err := validateAlerts(cfg); err != nil {
		return Config{}, warns, err
	}
	if cfg.KubeQPS <= 0 || cfg.KubeBurst <= 0 {
		return Config{}, warns, errors.New("ARC_UI_KUBE_QPS and ARC_UI_KUBE_BURST must be positive")
	}

	if cfg.PrometheusURL == "" {
		return Config{}, warns, errors.New("ARC_UI_PROMETHEUS_URL is required: every chart and usage figure is a Prometheus query")
	}
	if err := absoluteURL("ARC_UI_PROMETHEUS_URL", cfg.PrometheusURL); err != nil {
		return Config{}, warns, err
	}
	if cfg.PrometheusBearerToken != "" && cfg.PrometheusBearerTokenFile != "" {
		return Config{}, warns, errors.New("set ARC_UI_PROMETHEUS_BEARER_TOKEN or ARC_UI_PROMETHEUS_BEARER_TOKEN_FILE, not both")
	}
	if cfg.PrometheusTimeout <= 0 || cfg.RefreshInterval < time.Second || cfg.PrometheusProbeInterval < time.Second {
		return Config{}, warns, errors.New("ARC_UI_PROMETHEUS_TIMEOUT must be positive and ARC_UI_REFRESH_INTERVAL and ARC_UI_PROMETHEUS_PROBE_INTERVAL at least 1s")
	}

	// Negative durations are accepted by time.After and fire at once, which
	// would look like a clean shutdown while skipping the drain.
	if cfg.PreStopDelay < 0 {
		return Config{}, warns, fmt.Errorf("ARC_UI_PRESTOP_DELAY=%s must not be negative", cfg.PreStopDelay)
	}
	if cfg.ShutdownTimeout <= 0 {
		return Config{}, warns, fmt.Errorf("ARC_UI_SHUTDOWN_TIMEOUT=%s must be positive", cfg.ShutdownTimeout)
	}

	cfg.Namespaces = normalizeNamespaces(cfg.Namespaces)

	return cfg, warns, nil
}

func absoluteURL(name, v string) error {
	if u, err := url.Parse(v); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%s=%q must be an absolute http(s) URL", name, v)
	}
	return nil
}

func validateAlerts(cfg Config) error {
	for name, d := range map[string]time.Duration{
		"ARC_UI_ALERT_QUEUED_AT_MAX_FOR": cfg.AlertQueuedAtMaxFor,
		"ARC_UI_ALERT_LISTENER_DOWN_FOR": cfg.AlertListenerDownFor,
	} {
		if d < 0 {
			return fmt.Errorf("%s=%s must not be negative", name, d)
		}
	}
	for name, d := range map[string]time.Duration{
		"ARC_UI_ALERT_OOM_WINDOW":            cfg.AlertOOMWindow,
		"ARC_UI_ALERT_FAILED_RUNNERS_WINDOW": cfg.AlertFailedRunnersWindow,
		"ARC_UI_ALERT_STARTUP_WINDOW":        cfg.AlertStartupWindow,
	} {
		if d < time.Minute {
			return fmt.Errorf("%s=%s must be at least 1m", name, d)
		}
	}
	if cfg.AlertFailedRunnersRise < 1 {
		return fmt.Errorf("ARC_UI_ALERT_FAILED_RUNNERS_RISE=%d must be at least 1", cfg.AlertFailedRunnersRise)
	}
	if cfg.AlertStartupP95 <= 0 {
		return fmt.Errorf("ARC_UI_ALERT_STARTUP_P95=%s must be positive", cfg.AlertStartupP95)
	}
	return nil
}

// normalizeNamespaces drops blanks and duplicates; a single empty entry means
// all namespaces and survives as an empty slice.
func normalizeNamespaces(in []string) []string {
	trimmed := lo.Map(in, func(ns string, _ int) string { return strings.TrimSpace(ns) })
	return lo.Uniq(lo.Compact(trimmed))
}
