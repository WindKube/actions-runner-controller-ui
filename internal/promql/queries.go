// Package promql turns the fleet questions the views ask into PromQL against
// the ARC listener and controller metrics, cAdvisor and kube-state-metrics.
package promql

import (
	"fmt"
	"strconv"
	"time"

	"arc-ui/internal/prom"
)

// Queries renders PromQL for one label schema.
type Queries struct {
	Schema prom.Schema
}

// containers matches real containers in the runner namespaces; cAdvisor also
// exports a pod-level aggregate with container="" and the pause container.
func (q Queries) containers() string {
	return q.Schema.NamespaceMatcher() + `,container!="",container!="POD"`
}

// listener selects gha_* series for one scale set, or every set when set is "".
func (q Queries) listener(set string) string {
	if set == "" {
		return fmt.Sprintf(`%s!=""`, q.Schema.ScaleSetLabel)
	}
	return fmt.Sprintf(`%s=%s`, q.Schema.ScaleSetLabel, strconv.Quote(set))
}

// controller selects gha_controller_* series, which carry the scale set in
// their own name label.
func (q Queries) controller(set string) string {
	if set == "" {
		return ""
	}
	return fmt.Sprintf(`name=%s`, strconv.Quote(set))
}

// podsOf is a 1-valued (namespace, pod) vector of the set's runner pods, used
// to attribute per-pod series to a scale set.
func (q Queries) podsOf(set string) string {
	match := fmt.Sprintf(`%s!=""`, q.Schema.KSMScaleSetLabel)
	if set != "" {
		match = fmt.Sprintf(`%s=%s`, q.Schema.KSMScaleSetLabel, strconv.Quote(set))
	}
	return fmt.Sprintf(`max by (namespace, pod) (kube_pod_labels{%s,%s})`, q.Schema.NamespaceMatcher(), match)
}

// inSet restricts a per-pod expression to one set's pods. The fleet scope skips
// the join so usage still renders without kube-state-metrics.
func (q Queries) inSet(expr, set string) string {
	if set == "" {
		return expr
	}
	return fmt.Sprintf(`(%s) * on (namespace, pod) group_left () %s`, expr, q.podsOf(set))
}

// rateWindow is at least two minutes so a 30s scrape always has samples.
func rateWindow(step time.Duration) string {
	return promDuration(max(step, 2*time.Minute))
}

func promDuration(d time.Duration) string {
	return strconv.FormatInt(int64(d.Seconds()), 10) + "s"
}

func (q Queries) Busy(set string) string {
	return fmt.Sprintf(`sum(gha_busy_runners{%s})`, q.listener(set))
}

func (q Queries) Idle(set string) string {
	return fmt.Sprintf(`sum(gha_idle_runners{%s})`, q.listener(set))
}

func (q Queries) Pending(set string) string {
	return fmt.Sprintf(`sum(gha_controller_pending_ephemeral_runners{%s})`, q.controller(set))
}

// Capacity sums the bounded ceilings; an unbounded set reports MaxInt32.
func (q Queries) Capacity(set string) string {
	return fmt.Sprintf(`sum(gha_max_runners{%s} < 2147483647)`, q.listener(set))
}

func (q Queries) CPUUsed(set string, step time.Duration) string {
	return fmt.Sprintf(`sum(%s)`, q.inSet(fmt.Sprintf(`sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{%s}[%s]))`,
		q.containers(), rateWindow(step)), set))
}

func (q Queries) MemUsed(set string) string {
	return fmt.Sprintf(`sum(%s)`, q.inSet(fmt.Sprintf(`sum by (namespace, pod) (container_memory_working_set_bytes{%s})`,
		q.containers()), set))
}

func (q Queries) Requested(resource, set string) string {
	return fmt.Sprintf(`sum(%s)`, q.inSet(fmt.Sprintf(`sum by (namespace, pod) (kube_pod_container_resource_requests{%s,resource=%q,container!=""})`,
		q.Schema.NamespaceMatcher(), resource), set))
}

// perStep is a counter's increase per step. increase over [step] is empty when
// the step is shorter than two scrapes, so the rate is taken over rateWindow
// and scaled back to the step.
func perStep(selector string, step time.Duration) string {
	return fmt.Sprintf(`sum(rate(%s[%s])) * %s`, selector, rateWindow(step), strconv.FormatInt(int64(step.Seconds()), 10))
}

// Completed counts jobs finished per step; ok selects job_result="succeeded"
// or everything else.
func (q Queries) Completed(set string, ok bool, step time.Duration) string {
	op := "!="
	if ok {
		op = "="
	}
	return perStep(fmt.Sprintf(`gha_completed_jobs_total{%s,job_result%s"succeeded"}`, q.listener(set), op), step)
}

func (q Queries) Started(set string, step time.Duration) string {
	return perStep(fmt.Sprintf(`gha_started_jobs_total{%s}`, q.listener(set)), step)
}

func (q Queries) AllCompleted(set string, step time.Duration) string {
	return perStep(fmt.Sprintf(`gha_completed_jobs_total{%s}`, q.listener(set)), step)
}

// RepoSeconds and RepoJobs rank repositories by runner time over a window.
func (q Queries) RepoSeconds(window time.Duration) string {
	return fmt.Sprintf(`sum by (organization, repository) (increase(gha_job_execution_duration_seconds_sum{%s}[%s]))`, q.listener(""), promDuration(window))
}

func (q Queries) RepoJobs(window time.Duration) string {
	return fmt.Sprintf(`sum by (organization, repository) (increase(gha_job_execution_duration_seconds_count{%s}[%s]))`, q.listener(""), promDuration(window))
}

// PodCPU and PodMem are the live per-pod usage for the runner tables.
func (q Queries) PodCPU() string {
	return fmt.Sprintf(`sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{%s}[2m]))`, q.containers())
}

func (q Queries) PodMem() string {
	return fmt.Sprintf(`sum by (namespace, pod) (container_memory_working_set_bytes{%s})`, q.containers())
}

func (q Queries) RunnerCPU(pod string, step time.Duration) string {
	return fmt.Sprintf(`sum(rate(container_cpu_usage_seconds_total{%s,pod=%s}[%s]))`, q.containers(), strconv.Quote(pod), rateWindow(step))
}

func (q Queries) RunnerMem(pod string) string {
	return fmt.Sprintf(`sum(container_memory_working_set_bytes{%s,pod=%s})`, q.containers(), strconv.Quote(pod))
}

func (q Queries) JobCPUSeconds(pod string, window time.Duration) string {
	return fmt.Sprintf(`sum(increase(container_cpu_usage_seconds_total{%s,pod=%s}[%s]))`, q.containers(), strconv.Quote(pod), promDuration(window))
}

func (q Queries) JobMemPeak(pod string, window time.Duration) string {
	return fmt.Sprintf(`max(max_over_time(container_memory_working_set_bytes{%s,pod=%s}[%s]))`, q.containers(), strconv.Quote(pod), promDuration(window))
}

// FailedRunnersIncrease is the growth of each set's failed EphemeralRunners
// over the window, by the controller's name label.
func (q Queries) FailedRunnersIncrease(window time.Duration) string {
	return fmt.Sprintf(`sum by (name) (delta(gha_controller_failed_ephemeral_runners[%s]))`, promDuration(window))
}

// StartupP95 is each set's 95th percentile job startup latency in seconds.
func (q Queries) StartupP95(window time.Duration) string {
	return fmt.Sprintf(`histogram_quantile(0.95, sum by (le, %s) (rate(gha_job_startup_duration_seconds_bucket{%s}[%s])))`,
		q.Schema.ScaleSetLabel, q.listener(""), promDuration(window))
}

// Queue is jobs assigned to a set but not yet running, per set.
func (q Queries) Queue() string {
	l := q.Schema.ScaleSetLabel
	return fmt.Sprintf(`clamp_min(sum by (%[1]s) (gha_assigned_jobs{%[2]s}) - sum by (%[1]s) (gha_running_jobs{%[2]s}), 0)`, l, q.listener(""))
}
