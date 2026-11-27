// Package appmetrics is arc-ui's own /metrics: how its Prometheus queries are
// doing, how many browsers are streaming, and which alerts are firing.
package appmetrics

import (
	"errors"
	"net/http"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"arc-ui/internal/alert"
	"arc-ui/internal/prom"
)

type Metrics struct {
	registry      *prometheus.Registry
	queryDuration *prometheus.HistogramVec
	queryErrors   *prometheus.CounterVec
}

// New registers the metrics. sseClients and alerts are read at scrape time.
func New(sseClients func() int, alerts func() alert.State) *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		queryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "arc_ui_prometheus_query_duration_seconds",
			Help:    "Duration of queries arc-ui sends to Prometheus, by API endpoint.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"endpoint"}),
		queryErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "arc_ui_prometheus_query_errors_total",
			Help: "Queries to Prometheus that failed, by endpoint and kind (api or transport).",
		}, []string{"endpoint", "kind"}),
	}
	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.queryDuration,
		m.queryErrors,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "arc_ui_sse_clients",
			Help: "Browsers holding a live stream open.",
		}, func() float64 { return float64(sseClients()) }),
		alertCollector{state: alerts},
	)
	return m
}

// ObservePrometheus matches prom.Client.Observe.
func (m *Metrics) ObservePrometheus(endpoint string, d time.Duration, err error) {
	name := path.Base(endpoint)
	m.queryDuration.WithLabelValues(name).Observe(d.Seconds())
	if err == nil {
		return
	}
	kind := "transport"
	if _, ok := errors.AsType[*prom.APIError](err); ok {
		kind = "api"
	}
	m.queryErrors.WithLabelValues(name, kind).Inc()
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

var (
	firingDesc = prometheus.NewDesc("arc_ui_alerts_firing",
		"In-app alerts firing, by rule.", []string{"rule"}, nil)
	unavailableDesc = prometheus.NewDesc("arc_ui_alert_rule_unavailable",
		"1 when an alert rule could not be evaluated because its source is down.", []string{"rule"}, nil)
)

type alertCollector struct{ state func() alert.State }

func (c alertCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- firingDesc
	ch <- unavailableDesc
}

func (c alertCollector) Collect(ch chan<- prometheus.Metric) {
	for _, r := range c.state().Rules {
		ch <- prometheus.MustNewConstMetric(firingDesc, prometheus.GaugeValue, float64(r.Firing), r.ID)
		down := 0.0
		if r.Unavailable != "" {
			down = 1
		}
		ch <- prometheus.MustNewConstMetric(unavailableDesc, prometheus.GaugeValue, down, r.ID)
	}
}
