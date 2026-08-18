// Package config loads and validates the process configuration from the
// environment. Everything is a single flat struct so the full surface is
// visible in one place; nested prefixes buy nothing at this size.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/samber/lo"
)

// Config is the complete runtime configuration. Every field is settable from
// the environment; only the defaults below are assumed.
type Config struct {
	HTTPAddr  string `env:"ARC_UI_HTTP_ADDR" envDefault:"0.0.0.0:8080"`
	LogLevel  string `env:"ARC_UI_LOG_LEVEL" envDefault:"info"`
	LogFormat string `env:"ARC_UI_LOG_FORMAT" envDefault:"json"`

	SentryDSN         string  `env:"ARC_UI_SENTRY_DSN"`
	SentryEnvironment string  `env:"ARC_UI_SENTRY_ENVIRONMENT" envDefault:"production"`
	SentrySampleRate  float64 `env:"ARC_UI_SENTRY_SAMPLE_RATE" envDefault:"1.0"`

	// KubeAPIURL overrides the API server address from the kubeconfig. Empty
	// means "in-cluster if possible, else kubeconfig".
	//
	// metrics.k8s.io is an aggregated API served *through* the API server, so
	// this one URL covers custom resources, pods, events and pod metrics
	// alike. MetricsServerURL is a deprecated alias kept because earlier
	// compose files set it; Load() folds it in and warns.
	KubeAPIURL       string `env:"KUBE_API_URL"`
	MetricsServerURL string `env:"METRICS_SERVER_URL"`

	Kubeconfig  string  `env:"ARC_UI_KUBECONFIG"`
	KubeContext string  `env:"ARC_UI_KUBE_CONTEXT"`
	KubeQPS     float32 `env:"ARC_UI_KUBE_QPS" envDefault:"50"`
	KubeBurst   int     `env:"ARC_UI_KUBE_BURST" envDefault:"100"`

	// Namespaces holding AutoscalingRunnerSets and their runners. Empty means
	// all namespaces.
	Namespaces []string `env:"ARC_UI_NAMESPACES" envSeparator:"," envDefault:"arc-runners"`
	// ControllerNamespace holds the ARC controller and the AutoscalingListener
	// pods, which live apart from the runners and carry the listener health.
	ControllerNamespace string `env:"ARC_UI_CONTROLLER_NAMESPACE" envDefault:"arc-systems"`

	// ScrapeInterval is how often pod metrics are polled. metrics-server's
	// own default resolution is 15s, so polling faster returns byte-identical
	// data and only burns API server quota.
	ScrapeInterval time.Duration `env:"ARC_UI_SCRAPE_INTERVAL" envDefault:"15s"`
	// ListenerMetricsURL is the ARC listener's Prometheus endpoint. Optional:
	// ARC ships with metrics disabled, and the dashboard degrades without it.
	ListenerMetricsURL string `env:"ARC_UI_LISTENER_METRICS_URL"`

	DBPath string `env:"ARC_UI_DB_PATH" envDefault:"/data/arc-ui.db"`

	// Retention windows per storage tier. Runner-level samples are kept only
	// as long as the runner detail view actually renders them.
	RetentionRunnerRaw time.Duration `env:"ARC_UI_RETENTION_RUNNER_RAW" envDefault:"15m"`
	RetentionScopeRaw  time.Duration `env:"ARC_UI_RETENTION_SCOPE_RAW" envDefault:"6h"`
	RetentionScope1m   time.Duration `env:"ARC_UI_RETENTION_SCOPE_1M" envDefault:"168h"`
	RetentionScope5m   time.Duration `env:"ARC_UI_RETENTION_SCOPE_5M" envDefault:"720h"`
	RetentionScope1h   time.Duration `env:"ARC_UI_RETENTION_SCOPE_1H" envDefault:"9600h"`

	// GitHubOrg labels the breadcrumb. Empty means "derive it from the
	// AutoscalingRunnerSet's githubConfigUrl".
	GitHubOrg string `env:"ARC_UI_GITHUB_ORG"`

	ShutdownTimeout time.Duration `env:"ARC_UI_SHUTDOWN_TIMEOUT" envDefault:"20s"`
	// PreStopDelay keeps the process serving after readiness flips, so load
	// balancers stop sending traffic before the listener goes away. Must be at
	// least twice the readiness probe period or clients see 502s.
	PreStopDelay time.Duration `env:"ARC_UI_PRESTOP_DELAY" envDefault:"8s"`
}

// Warning is a non-fatal configuration problem. Load returns these instead of
// logging directly so the caller controls where they go.
type Warning string

// Load parses the environment and validates it. The returned warnings are
// advisory; a non-nil error means the process must not start.
func Load() (Config, []Warning, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, nil, fmt.Errorf("parse env: %w", err)
	}

	var warns []Warning

	if cfg.KubeAPIURL == "" && cfg.MetricsServerURL != "" {
		cfg.KubeAPIURL = cfg.MetricsServerURL
		warns = append(warns, "METRICS_SERVER_URL is deprecated and was used as KUBE_API_URL; "+
			"metrics.k8s.io is an aggregated API reached through the Kubernetes API server, "+
			"not from metrics-server directly")
	}

	if cfg.LogFormat != "json" && cfg.LogFormat != "console" {
		return Config{}, warns, fmt.Errorf("ARC_UI_LOG_FORMAT=%q must be json or console", cfg.LogFormat)
	}
	if cfg.ScrapeInterval < time.Second {
		return Config{}, warns, fmt.Errorf("ARC_UI_SCRAPE_INTERVAL=%s must be at least 1s", cfg.ScrapeInterval)
	}
	if cfg.ScrapeInterval < 15*time.Second {
		warns = append(warns, Warning(fmt.Sprintf(
			"ARC_UI_SCRAPE_INTERVAL=%s is below metrics-server's default 15s resolution; "+
				"extra polls return identical data", cfg.ScrapeInterval)))
	}
	if cfg.DBPath == "" {
		return Config{}, warns, fmt.Errorf("ARC_UI_DB_PATH must not be empty")
	}
	if cfg.KubeQPS <= 0 || cfg.KubeBurst <= 0 {
		return Config{}, warns, fmt.Errorf("ARC_UI_KUBE_QPS and ARC_UI_KUBE_BURST must be positive")
	}

	cfg.Namespaces = normalizeNamespaces(cfg.Namespaces)

	return cfg, warns, nil
}

// normalizeNamespaces drops blanks and de-duplicates. A single empty entry is
// the documented way to say "all namespaces", and survives as an empty slice.
func normalizeNamespaces(in []string) []string {
	trimmed := lo.Map(in, func(ns string, _ int) string { return strings.TrimSpace(ns) })
	return lo.Uniq(lo.Compact(trimmed))
}

// AllNamespaces reports whether the dashboard watches the whole cluster.
func (c Config) AllNamespaces() bool { return len(c.Namespaces) == 0 }

// Hostname is used to label the store and log lines. Failure is not
// interesting enough to propagate.
func Hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
