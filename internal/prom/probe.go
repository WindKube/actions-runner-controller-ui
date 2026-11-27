package prom

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"arc-ui/internal/fleet"
)

// Schema names the labels the app filters on. They depend on how the
// operator's scrape config relabels pod labels, so they are configurable.
type Schema struct {
	// ScaleSetLabel is the scale set name on gha_* listener series, mapped
	// from the listener pod's actions.github.com/scale-set-name label.
	ScaleSetLabel string
	// KSMScaleSetLabel is the same pod label as kube-state-metrics exports it
	// on kube_pod_labels.
	KSMScaleSetLabel string
	// RunnerNamespaces scopes cAdvisor and kube-state-metrics series; empty
	// means every namespace.
	RunnerNamespaces []string
}

// NamespaceMatcher is a PromQL label matcher for the runner namespaces.
func (s Schema) NamespaceMatcher() string {
	if len(s.RunnerNamespaces) == 0 {
		return `namespace!=""`
	}
	quoted := make([]string, len(s.RunnerNamespaces))
	for i, ns := range s.RunnerNamespaces {
		quoted[i] = regexp.QuoteMeta(ns)
	}
	return fmt.Sprintf(`namespace=~%q`, strings.Join(quoted, "|"))
}

// check is one expected group of series. query must return a single sample
// counting them; an empty result means none exist.
type check struct {
	source string
	query  string
	// diagnose runs when query is empty, to tell "metric absent" from "label
	// missing"; it returns the reason to show.
	diagnose func(ctx context.Context, c *Client) string
}

func (s Schema) checks() []check {
	ns := s.NamespaceMatcher()
	return []check{
		{
			source: fleet.SourceARCMetrics,
			query:  fmt.Sprintf(`count(gha_assigned_jobs{%s!=""})`, s.ScaleSetLabel),
			diagnose: func(ctx context.Context, c *Client) string {
				if !exists(ctx, c, `count(gha_assigned_jobs)`) {
					return "no gha_* listener series; enable listener metrics and scrape them (podMonitors.arc.enabled)"
				}
				return fmt.Sprintf("gha_* series carry no %s label; map the actions.github.com/scale-set-name pod label in the scrape config", s.ScaleSetLabel)
			},
		},
		{
			source: fleet.SourceControllerMetrics,
			query:  `count(gha_controller_running_listeners)`,
			diagnose: func(context.Context, *Client) string {
				return "no gha_controller_* series; scrape the ARC controller's metrics port"
			},
		},
		{
			source: fleet.SourceCAdvisor,
			query:  fmt.Sprintf(`count(container_cpu_usage_seconds_total{%s,container!="",container!="POD"})`, ns),
			diagnose: func(context.Context, *Client) string {
				return "no cAdvisor container series for the runner namespaces; scrape the kubelet /metrics/cadvisor endpoint"
			},
		},
		{
			source: fleet.SourceKSM,
			query:  fmt.Sprintf(`count(kube_pod_labels{%s,%s!=""})`, ns, s.KSMScaleSetLabel),
			diagnose: func(ctx context.Context, c *Client) string {
				if !exists(ctx, c, `count(kube_pod_labels)`) {
					return "no kube-state-metrics series; runner requests, limits and restarts are unavailable"
				}
				return fmt.Sprintf("kube_pod_labels has no %s; run kube-state-metrics with --metric-labels-allowlist=pods=[actions.github.com/scale-set-name]", s.KSMScaleSetLabel)
			},
		},
	}
}

func exists(ctx context.Context, c *Client, query string) bool {
	rows, err := c.Query(ctx, query)
	return err == nil && len(rows) > 0 && rows[0].Value > 0
}

// SourceSink receives probe verdicts; the collector implements it.
type SourceSink interface {
	SetSource(fleet.Source)
}

// Prober checks, on an interval, that Prometheus answers and that the series
// the app reads exist, and reports each group as a data source.
type Prober struct {
	client   *Client
	schema   Schema
	interval time.Duration
	sink     SourceSink
	log      zerolog.Logger
}

func NewProber(client *Client, schema Schema, interval time.Duration, sink SourceSink, log zerolog.Logger) *Prober {
	return &Prober{client: client, schema: schema, interval: interval, sink: sink, log: log.With().Str("component", "prometheus-probe").Logger()}
}

func (p *Prober) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		for _, s := range p.Probe(ctx) {
			p.sink.SetSource(s)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Probe returns one source for Prometheus itself and one per series group.
// When Prometheus is down every group is reported unavailable with that cause.
func (p *Prober) Probe(ctx context.Context) []fleet.Source {
	now := time.Now()
	checks := p.schema.checks()
	out := make([]fleet.Source, 0, len(checks)+1)

	if _, err := p.client.Query(ctx, "vector(1)"); err != nil {
		reason := "unreachable: " + err.Error()
		out = append(out, fleet.Source{Name: fleet.SourcePrometheus, Reason: reason, CheckedAt: now})
		for _, chk := range checks {
			out = append(out, fleet.Source{Name: chk.source, Reason: "prometheus unreachable", CheckedAt: now})
		}
		return out
	}
	out = append(out, fleet.Source{Name: fleet.SourcePrometheus, Available: true, CheckedAt: now})

	for _, chk := range checks {
		src := fleet.Source{Name: chk.source, CheckedAt: now}
		rows, err := p.client.Query(ctx, chk.query)
		switch {
		case err != nil:
			src.Reason = err.Error()
		case len(rows) == 0 || rows[0].Value == 0:
			src.Reason = chk.diagnose(ctx, p.client)
		default:
			src.Available = true
		}
		out = append(out, src)
	}
	return out
}
