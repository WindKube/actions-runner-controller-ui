package listener

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"arc-ui/internal/fleet"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixture is a realistic slice of an ARC listener's /metrics output: HELP and
// TYPE lines, several label sets per family, a counter with a _total suffix,
// and a histogram we must ignore.
const fixture = `# HELP gha_assigned_jobs Number of jobs assigned to the runner scale set.
# TYPE gha_assigned_jobs gauge
gha_assigned_jobs{enterprise="",name="arc-linux-x64",namespace="arc-runners",organization="acme",repository="",runner_group_name="default"} 12
gha_assigned_jobs{enterprise="",name="arc-linux-arm64",namespace="arc-runners",organization="acme",repository="",runner_group_name="default"} 3
# HELP gha_running_jobs Number of jobs running.
# TYPE gha_running_jobs gauge
gha_running_jobs{enterprise="",name="arc-linux-x64",namespace="arc-runners",organization="acme",repository="",runner_group_name="default"} 8
gha_running_jobs{enterprise="",name="arc-linux-arm64",namespace="arc-runners",organization="acme",repository="",runner_group_name="default"} 5
# HELP gha_registered_runners Number of runners registered with GitHub.
# TYPE gha_registered_runners gauge
gha_registered_runners{name="arc-linux-x64",namespace="arc-runners"} 20
# HELP gha_busy_runners Number of runners running a job.
# TYPE gha_busy_runners gauge
gha_busy_runners{name="arc-linux-x64",namespace="arc-runners"} 8
# HELP gha_idle_runners Number of runners not running a job.
# TYPE gha_idle_runners gauge
gha_idle_runners{name="arc-linux-x64",namespace="arc-runners"} 12
# HELP gha_desired_runners Number of runners desired by the listener.
# TYPE gha_desired_runners gauge
gha_desired_runners{name="arc-linux-x64",namespace="arc-runners"} 20
# HELP gha_min_runners Minimum number of runners.
# TYPE gha_min_runners gauge
gha_min_runners{name="arc-linux-x64",namespace="arc-runners"} 2
# HELP gha_max_runners Maximum number of runners.
# TYPE gha_max_runners gauge
gha_max_runners{name="arc-linux-x64",namespace="arc-runners"} 50
# HELP gha_started_jobs_total Total number of jobs started.
# TYPE gha_started_jobs_total counter
gha_started_jobs_total{job_workflow_ref="acme/api/.github/workflows/ci.yml@refs/heads/main",name="arc-linux-x64",namespace="arc-runners"} 941
gha_started_jobs_total{job_workflow_ref="acme/web/.github/workflows/ci.yml@refs/heads/main",name="arc-linux-x64",namespace="arc-runners"} 59
# HELP gha_completed_jobs_total Total number of jobs completed.
# TYPE gha_completed_jobs_total counter
gha_completed_jobs_total{job_result="succeeded",name="arc-linux-x64",namespace="arc-runners"} 900
gha_completed_jobs_total{job_result="failed",name="arc-linux-x64",namespace="arc-runners"} 32
# HELP gha_job_execution_duration_seconds Histogram of job execution times.
# TYPE gha_job_execution_duration_seconds histogram
gha_job_execution_duration_seconds_bucket{name="arc-linux-x64",le="60"} 120
gha_job_execution_duration_seconds_bucket{name="arc-linux-x64",le="+Inf"} 400
gha_job_execution_duration_seconds_sum{name="arc-linux-x64"} 51234.5
gha_job_execution_duration_seconds_count{name="arc-linux-x64"} 400
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 41
`

func testLogger() zerolog.Logger { return zerolog.New(io.Discard) }

func TestParse(t *testing.T) {
	t.Parallel()

	m, err := Parse(strings.NewReader(fixture))
	require.NoError(t, err, "Parse")

	tests := []struct {
		name string
		got  map[string]float64
		want map[string]float64
	}{
		{
			name: "assigned jobs keyed per set",
			got:  m.AssignedJobs,
			want: map[string]float64{"arc-linux-x64": 12, "arc-linux-arm64": 3},
		},
		{
			name: "running jobs keyed per set",
			got:  m.RunningJobs,
			want: map[string]float64{"arc-linux-x64": 8, "arc-linux-arm64": 5},
		},
		{name: "registered runners", got: m.RegisteredRunners, want: map[string]float64{"arc-linux-x64": 20}},
		{name: "busy runners", got: m.BusyRunners, want: map[string]float64{"arc-linux-x64": 8}},
		{name: "idle runners", got: m.IdleRunners, want: map[string]float64{"arc-linux-x64": 12}},
		{name: "desired runners", got: m.DesiredRunners, want: map[string]float64{"arc-linux-x64": 20}},
		{name: "min runners", got: m.MinRunners, want: map[string]float64{"arc-linux-x64": 2}},
		{name: "max runners", got: m.MaxRunners, want: map[string]float64{"arc-linux-x64": 50}},
		{
			// The counter carries a _total suffix and is split across
			// workflows; the per-set view is the sum.
			name: "started jobs counter summed across workflows",
			got:  m.StartedJobsTotal,
			want: map[string]float64{"arc-linux-x64": 1000},
		},
		{
			name: "completed jobs counter summed across results",
			got:  m.CompletedJobsTotal,
			want: map[string]float64{"arc-linux-x64": 932},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Len(t, tc.got, len(tc.want), "got %v, want %v", tc.got, tc.want)
			for k, want := range tc.want {
				assert.Contains(t, tc.got, k, "[%s] is absent, want %v", k, want)
				assert.Equal(t, want, tc.got[k], "[%s]", k)
			}
		})
	}
}

func TestParseIgnoresUnknownAndAbsent(t *testing.T) {
	t.Parallel()

	const input = `# HELP some_other_thing Nothing to do with ARC.
# TYPE some_other_thing gauge
some_other_thing{name="arc-linux-x64"} 7
# TYPE gha_assigned_jobs gauge
gha_assigned_jobs{name="arc-linux-x64"} 4
`
	m, err := Parse(strings.NewReader(input))
	require.NoError(t, err, "Parse")
	assert.Equal(t, float64(4), m.AssignedJobs["arc-linux-x64"], "AssignedJobs")
	// Families the listener never exposed stay nil, not zero-valued maps: the
	// dashboard must be able to tell "not reported" from "reported as zero".
	assert.Nil(t, m.MaxRunners, "MaxRunners must stay nil for an unexposed family")
	assert.Nil(t, m.RunningJobs, "RunningJobs must stay nil for an unexposed family")
}

func TestParseScaleSetNameFallbackLabel(t *testing.T) {
	t.Parallel()

	const input = `# TYPE gha_assigned_jobs gauge
gha_assigned_jobs{runner_scale_set_name="legacy-set",namespace="arc-runners"} 6
gha_assigned_jobs{name="modern-set",runner_scale_set_name="ignored",namespace="arc-runners"} 2
gha_assigned_jobs{namespace="arc-runners"} 99
`
	m, err := Parse(strings.NewReader(input))
	require.NoError(t, err, "Parse")
	want := map[string]float64{"legacy-set": 6, "modern-set": 2}
	require.Len(t, m.AssignedJobs, len(want), "AssignedJobs = %v, want %v (an unkeyable series must be dropped)", m.AssignedJobs, want)
	for k, v := range want {
		assert.Equal(t, v, m.AssignedJobs[k], "[%s]", k)
	}
}

func TestParseMalformed(t *testing.T) {
	t.Parallel()

	_, err := Parse(strings.NewReader("gha_assigned_jobs{name=\"broken\" 12\n"))
	require.Error(t, err, "Parse accepted malformed exposition")
}

func TestQueueDepth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		m    Metrics
		want map[string]int
	}{
		{
			name: "assigned minus running",
			m: Metrics{
				AssignedJobs: map[string]float64{"a": 12},
				RunningJobs:  map[string]float64{"a": 8},
			},
			want: map[string]int{"a": 4},
		},
		{
			// The two gauges are sampled independently, so running can
			// transiently exceed assigned. A negative queue is nonsense.
			name: "running exceeds assigned floors at zero",
			m: Metrics{
				AssignedJobs: map[string]float64{"a": 3},
				RunningJobs:  map[string]float64{"a": 5},
			},
			want: map[string]int{"a": 0},
		},
		{
			name: "no running gauge means everything is queued",
			m:    Metrics{AssignedJobs: map[string]float64{"a": 7}},
			want: map[string]int{"a": 7},
		},
		{
			name: "running with no assigned reports a known zero",
			m:    Metrics{RunningJobs: map[string]float64{"a": 4}},
			want: map[string]int{"a": 0},
		},
		{
			name: "multiple sets are independent",
			m: Metrics{
				AssignedJobs: map[string]float64{"a": 12, "b": 1, "c": 0},
				RunningJobs:  map[string]float64{"a": 8, "b": 4},
			},
			want: map[string]int{"a": 4, "b": 0, "c": 0},
		},
		{name: "empty metrics", m: Metrics{}, want: map[string]int{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.m.QueueDepth()
			require.Len(t, got, len(tc.want), "got %v, want %v", got, tc.want)
			for k, want := range tc.want {
				assert.Contains(t, got, k, "[%s] is absent, want %d", k, want)
				assert.Equal(t, want, got[k], "[%s]", k)
			}
		})
	}
}

// TestQueueDepthNonFiniteGauge pins the guard in floorDepth. "+Inf", "-Inf" and
// "NaN" are all legal Prometheus exposition and the scrape URL is operator
// supplied, so a non-finite gauge is reachable input. It must never reach the
// float-to-int conversion: that conversion is implementation-defined once the
// value does not fit in an int, and on this toolchain +Inf lands on
// math.MaxInt64 — a 19-digit "queued jobs" on the dashboard.
func TestQueueDepthNonFiniteGauge(t *testing.T) {
	t.Parallel()

	inf, negInf, nan := math.Inf(1), math.Inf(-1), math.NaN()

	tests := []struct {
		name string
		m    Metrics
	}{
		{name: "assigned +Inf", m: Metrics{AssignedJobs: map[string]float64{"a": inf}}},
		{name: "assigned -Inf", m: Metrics{AssignedJobs: map[string]float64{"a": negInf}}},
		{name: "assigned NaN", m: Metrics{AssignedJobs: map[string]float64{"a": nan}}},
		{
			name: "running -Inf makes the difference +Inf",
			m: Metrics{
				AssignedJobs: map[string]float64{"a": 1},
				RunningJobs:  map[string]float64{"a": negInf},
			},
		},
		{
			name: "both +Inf make the difference NaN",
			m: Metrics{
				AssignedJobs: map[string]float64{"a": inf},
				RunningJobs:  map[string]float64{"a": inf},
			},
		},
		{
			// Finite, but the same out-of-range conversion.
			name: "finite value far past int range",
			m:    Metrics{AssignedJobs: map[string]float64{"a": 1e300}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.m.QueueDepth()
			require.Contains(t, got, "a", "got %v", got)
			assert.Equal(t, 0, got["a"], "a non-finite gauge must report no queue, got %v", got)
		})
	}
}

// TestParseNonFiniteGaugeEndToEnd runs the same defect through the real text
// parser, which accepts +Inf verbatim.
func TestParseNonFiniteGaugeEndToEnd(t *testing.T) {
	t.Parallel()

	const input = `# TYPE gha_assigned_jobs gauge
gha_assigned_jobs{name="a",namespace="arc-runners"} +Inf
`
	m, err := Parse(strings.NewReader(input))
	require.NoError(t, err, "Parse")
	assert.Equal(t, 0, m.QueueDepth()["a"], "queue depth for a +Inf gauge")
}

// collisions is exposition where one scale set name is used in two namespaces —
// legal, since ARC_UI_NAMESPACES may span several — carrying both the ceiling
// gauges (per-set configuration, not quantities) and a counter that upstream
// splits by workflow.
const collisions = `# TYPE gha_max_runners gauge
gha_max_runners{name="shared",namespace="team-b"} 50
gha_max_runners{name="shared",namespace="team-a"} 30
# TYPE gha_min_runners gauge
gha_min_runners{name="shared",namespace="team-b"} 4
gha_min_runners{name="shared",namespace="team-a"} 1
# TYPE gha_desired_runners gauge
gha_desired_runners{name="shared",namespace="team-b"} 7
gha_desired_runners{name="shared",namespace="team-a"} 2
# TYPE gha_started_jobs_total counter
gha_started_jobs_total{job_workflow_ref="acme/api/.github/workflows/ci.yml@refs/heads/main",name="shared",namespace="team-a"} 10
gha_started_jobs_total{job_workflow_ref="acme/web/.github/workflows/ci.yml@refs/heads/main",name="shared",namespace="team-a"} 5
`

// TestParseDoesNotSumCeilingsAcrossNamespaces: min/max/desired are ceilings, so
// two same-named scale sets in different namespaces have two independent limits
// and their sum is a number that exists nowhere. Counters stay summed.
func TestParseDoesNotSumCeilingsAcrossNamespaces(t *testing.T) {
	t.Parallel()

	m, err := Parse(strings.NewReader(collisions))
	require.NoError(t, err, "Parse")

	// The winner is the alphabetically first namespace, so the answer does not
	// depend on the order the listener happened to emit the series in.
	assert.Equal(t, float64(30), m.MaxRunners["shared"], "MaxRunners = %v, want team-a's 30 (never 80)", m.MaxRunners)
	assert.Equal(t, float64(1), m.MinRunners["shared"], "MinRunners = %v, want team-a's 1 (never 5)", m.MinRunners)
	assert.Equal(t, float64(2), m.DesiredRunners["shared"], "DesiredRunners = %v, want team-a's 2 (never 9)", m.DesiredRunners)
	assert.Equal(t, float64(15), m.StartedJobsTotal["shared"], "StartedJobsTotal = %v, want the per-workflow total", m.StartedJobsTotal)
}

// TestParseReportsScaleSetNameCollision: silently dropping one namespace's
// ceiling would be its own trap, so the operator has to hear about it. parse
// reports rather than logs, so one deployment fact is one report however many
// series and families carry it.
func TestParseReportsScaleSetNameCollision(t *testing.T) {
	t.Parallel()

	_, cols, err := parse(strings.NewReader(collisions))
	require.NoError(t, err, "parse")

	require.Len(t, cols, 1, "one name in two namespaces is one collision, got %+v", cols)
	assert.Equal(t, "shared", cols[0].set, "collision set")
	assert.Equal(t, []string{"team-a", "team-b"}, cols[0].namespaces, "collision namespaces")
}

// TestParseSingleNamespaceIsUnaffected guards the common install: one namespace
// per scale set must behave exactly as before, including families that legally
// repeat a name across the labels we ignore.
func TestParseSingleNamespaceIsUnaffected(t *testing.T) {
	t.Parallel()

	m, cols, err := parse(strings.NewReader(fixture))
	require.NoError(t, err, "parse")

	assert.Equal(t, float64(50), m.MaxRunners["arc-linux-x64"], "MaxRunners")
	assert.Equal(t, float64(2), m.MinRunners["arc-linux-x64"], "MinRunners")
	assert.Equal(t, float64(20), m.DesiredRunners["arc-linux-x64"], "DesiredRunners")
	assert.Empty(t, cols, "a single-namespace scrape must not report a collision")
}

// ceilingFamilies are the families collect reads with perScaleSet aggregation:
// the ones where two same-named scale sets cannot be added up.
var ceilingFamilies = []string{metricMaxRunners, metricMinRunners, metricDesiredRunners}

// oneNameManyNamespaces builds exposition where a single scale set name appears
// under n namespaces in every ceiling family — the shape an endpoint that is not
// a listener, or a listener watching a great many namespaces, can push all the
// way to the body cap.
func oneNameManyNamespaces(n int) string {
	var b strings.Builder
	for _, family := range ceilingFamilies {
		fmt.Fprintf(&b, "# TYPE %s gauge\n", family)
		for i := range n {
			fmt.Fprintf(&b, "%s{name=\"shared\",namespace=\"team-%04d\"} %d\n", family, i, i)
		}
	}
	return b.String()
}

// manyNamesTwoNamespaces builds exposition where n distinct scale set names each
// collide across the same two namespaces.
func manyNamesTwoNamespaces(n int) string {
	var b strings.Builder
	for _, family := range ceilingFamilies {
		fmt.Fprintf(&b, "# TYPE %s gauge\n", family)
		for i := range n {
			fmt.Fprintf(&b, "%s{name=\"set-%04d\",namespace=\"team-a\"} %d\n", family, i, i)
			fmt.Fprintf(&b, "%s{name=\"set-%04d\",namespace=\"team-b\"} %d\n", family, i, i)
		}
	}
	return b.String()
}

// warnLines counts warnings in a zerolog JSON stream.
func warnLines(s string) int { return strings.Count(s, `"level":"warn"`) }

// TestScraperCollisionWarningIsBoundedAndDeduped pins the log-volume budget for
// a collision. The scrape body is untrusted and unbounded up to maxBodyBytes,
// and the scraper re-reads it every interval forever, so a warning per offending
// series per family per tick is both a log flood and — in the two-namespace case
// an operator actually meets — three identical lines every 15s for a deployment
// fact that never changes. One tick says it once; an unchanged tick says nothing.
func TestScraperCollisionWarningIsBoundedAndDeduped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want []string // substrings the single warning must carry
	}{
		{name: "two namespaces", body: collisions, want: []string{"shared", "team-a", "team-b"}},
		{name: "one name in 500 namespaces", body: oneNameManyNamespaces(500), want: []string{"shared"}},
		{name: "500 names in two namespaces", body: manyNamesTwoNamespaces(500), want: []string{"set-0000"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			var buf strings.Builder
			s := NewScraper(srv.URL, time.Hour, zerolog.New(&buf).Level(zerolog.InfoLevel), &recorder{})

			s.tick(context.Background())
			assert.Equal(t, 1, warnLines(buf.String()), "first scrape must warn exactly once, got:\n%s", firstLines(buf.String(), 5))
			for _, want := range tc.want {
				assert.Contains(t, buf.String(), want, "the warning does not name %q", want)
			}
			// A single line naming every offender is the same flood with fewer
			// newlines, so the whole tick's output has a byte budget too.
			assert.Less(t, buf.Len(), 2048, "one tick logged %d bytes about one deployment mistake", buf.Len())

			before := buf.Len()
			s.tick(context.Background())
			assert.Equal(t, before, buf.Len(), "an unchanged collision logged again above debug:\n%s", buf.String()[before:])
		})
	}
}

// TestCollisionWarningTruncatesOversizedLabelValues: the warning caps how many
// names and namespaces it lists, but their *length* comes straight from the
// scrape body, which is operator-supplied and bounded only by maxBodyBytes. One
// colliding name carrying megabyte label values is one megabyte-long warn line,
// and dedup does not help — the first line is already too big to log.
func TestCollisionWarningTruncatesOversizedLabelValues(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("a", 1<<20)
	body := "# TYPE gha_max_runners gauge\n" +
		fmt.Sprintf("gha_max_runners{name=%q,namespace=%q} 30\n", huge+"-set", huge+"-team-a") +
		fmt.Sprintf("gha_max_runners{name=%q,namespace=%q} 50\n", huge+"-set", huge+"-team-b")

	_, cols, err := parse(strings.NewReader(body))
	require.NoError(t, err, "parse")
	require.Len(t, cols, 1, "one name in two namespaces is one collision, got %d", len(cols))

	var buf strings.Builder
	var c collisionTracker
	c.observe(zerolog.New(&buf).Level(zerolog.InfoLevel), cols)

	assert.Less(t, buf.Len(), 1024, "one collision carrying 1 MiB label values logged %d bytes on one line", buf.Len())
	assert.Contains(t, buf.String(), "…", "an oversized label value must be marked as cut short")
}

// TestDescribeCollisionsNamesTheUnlabelledSeries: "" is a real member of a
// collision — a series that carried no namespace label — and rendering it as an
// empty element gives "shared in , team-a", a bare leading comma an operator
// cannot decode.
func TestDescribeCollisionsNamesTheUnlabelledSeries(t *testing.T) {
	t.Parallel()

	got := describeCollisions([]collision{{set: "shared", namespaces: []string{"", "team-a"}}})
	require.Len(t, got, 1, "one collision is one entry, got %q", got)
	assert.Contains(t, got[0], "no namespace label", "the unlabelled member is not named: %q", got[0])
	assert.Contains(t, got[0], "team-a", "entry: %q", got[0])
}

// firstLines trims a log dump so a failure message stays readable.
func firstLines(s string, n int) string {
	lines := strings.SplitN(s, "\n", n+1)
	if len(lines) > n {
		lines = append(lines[:n], "...")
	}
	return strings.Join(lines, "\n")
}

// TestParsePrefersLabelledNamespace: labelValue cannot tell "no namespace label"
// from `namespace=""`, and both arrive as "". Sorting "" first would let a
// series that says nothing about where it came from evict one that does, and
// attribute the surviving value to namespace "".
func TestParsePrefersLabelledNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "labelled series first",
			body: "# TYPE gha_max_runners gauge\n" +
				`gha_max_runners{name="shared",namespace="team-a"} 30` + "\n" +
				`gha_max_runners{name="shared"} 50` + "\n",
		},
		{
			name: "unlabelled series first",
			body: "# TYPE gha_max_runners gauge\n" +
				`gha_max_runners{name="shared"} 50` + "\n" +
				`gha_max_runners{name="shared",namespace="team-a"} 30` + "\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, err := Parse(strings.NewReader(tc.body))
			require.NoError(t, err, "Parse")
			assert.Equal(t, float64(30), m.MaxRunners["shared"], "MaxRunners = %v, want team-a's 30", m.MaxRunners)
		})
	}
}

// TestParseCeilingWinnerIsOrderIndependent pins the property the perScaleSet
// tie-break exists for: which namespace's ceiling survives must not depend on
// the order the listener happened to emit its series in. Both orders are
// exercised on purpose — with only one, "alphabetically first wins" and "last
// series wins" are indistinguishable and the tie-break could be deleted.
func TestParseCeilingWinnerIsOrderIndependent(t *testing.T) {
	t.Parallel()

	body := func(first, second string) string {
		var b strings.Builder
		for _, family := range ceilingFamilies {
			fmt.Fprintf(&b, "# TYPE %s gauge\n", family)
			fmt.Fprintf(&b, "%s{name=\"shared\",namespace=\"%s\"} 30\n", family, first)
			fmt.Fprintf(&b, "%s{name=\"shared\",namespace=\"%s\"} 50\n", family, second)
		}
		return b.String()
	}

	tests := []struct {
		name string
		body string
		want float64 // team-a's value, whichever order it was emitted in
	}{
		{name: "team-a emitted first", body: body("team-a", "team-b"), want: 30},
		{name: "team-a emitted last", body: body("team-b", "team-a"), want: 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m, err := Parse(strings.NewReader(tc.body))
			require.NoError(t, err, "Parse")
			for _, got := range []map[string]float64{m.MaxRunners, m.MinRunners, m.DesiredRunners} {
				assert.Equal(t, tc.want, got["shared"], "want team-a's value regardless of emission order, got %v", got)
			}
		})
	}
}

// TestParseUnlabelledCollisionIsUndetectable documents the hole collision
// detection has by construction: the namespace label is the only thing telling
// two same-named scale sets apart, so two that carry none fold into one entry
// with no warning and an emission-order winner.
func TestParseUnlabelledCollisionIsUndetectable(t *testing.T) {
	t.Parallel()

	const body = `# TYPE gha_max_runners gauge
gha_max_runners{name="shared"} 30
gha_max_runners{name="shared"} 50
`
	m, cols, err := parse(strings.NewReader(body))
	require.NoError(t, err, "parse")
	assert.Empty(t, cols, "an unlabelled collision cannot be detected, so it must not be reported")
	assert.Equal(t, float64(30), m.MaxRunners["shared"], "the first series wins; there is nothing else to choose on")
}

func TestQueueDepthFromFixture(t *testing.T) {
	t.Parallel()

	m, err := Parse(strings.NewReader(fixture))
	require.NoError(t, err, "Parse")
	got := m.QueueDepth()
	assert.Equal(t, 4, got["arc-linux-x64"], "arc-linux-x64 depth")
	assert.Zero(t, got["arc-linux-arm64"], "arc-linux-arm64 depth must be 0 (5 running > 3 assigned)")
}

// recorder is a Sink that remembers what the scraper pushed.
type recorder struct {
	mu      sync.Mutex
	depth   map[string]int
	known   bool
	depthN  int
	sources []fleet.Source
}

func (r *recorder) SetQueueDepth(perSet map[string]int, known bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.depth, r.known = perSet, known
	r.depthN++
}

func (r *recorder) SetSource(s fleet.Source) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources = append(r.sources, s)
}

func (r *recorder) last() (fleet.Source, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sources) == 0 {
		return fleet.Source{}, false
	}
	return r.sources[len(r.sources)-1], true
}

// TestScraperEmptyURLShortCircuits covers the default ARC install, where the
// listener's metrics are simply not exposed.
func TestScraperEmptyURLShortCircuits(t *testing.T) {
	t.Parallel()

	rec := &recorder{}
	s := NewScraper("", time.Millisecond, testLogger(), rec)

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	select {
	case err := <-done:
		require.NoError(t, err, "Run with no URL must return nil")
	case <-time.After(5 * time.Second):
		t.Fatal("Run with no URL did not return; it must not busy-loop")
	}

	assert.False(t, rec.known, "queue depth reported as known with no endpoint configured")
	assert.Equal(t, 1, rec.depthN, "SetQueueDepth call count")
	src, ok := rec.last()
	require.True(t, ok, "no source reported")
	assert.False(t, src.Available, "source reported available with no endpoint configured")
	assert.Equal(t, fleet.SourceListener, src.Name, "source name")
	// The reason has to tell an operator how to turn the metrics on.
	for _, want := range []string{"metrics", "chart", "ARC_UI_LISTENER_METRICS_URL"} {
		assert.Contains(t, src.Reason, want, "reason does not mention %q", want)
	}
	assert.False(t, src.CheckedAt.IsZero(), "CheckedAt is zero")
}

func TestScraperSuccess(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, fixture)
	}))
	defer srv.Close()

	rec := &recorder{}
	s := NewScraper(srv.URL, time.Hour, testLogger(), rec)
	s.tick(context.Background())

	require.True(t, rec.known, "known = false after a successful scrape")
	assert.Equal(t, 4, rec.depth["arc-linux-x64"], "depth = %v, want arc-linux-x64=4", rec.depth)
	src, _ := rec.last()
	assert.True(t, src.Available, "source unavailable after a successful scrape: %q", src.Reason)
}

// TestScraperLogsScaleSetNameCollision proves the scraper hands parse's
// collision report to its own logger: a collision noticed only inside Parse
// would never reach an operator.
func TestScraperLogsScaleSetNameCollision(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, collisions)
	}))
	defer srv.Close()

	var buf strings.Builder
	s := NewScraper(srv.URL, time.Hour, zerolog.New(&buf).Level(zerolog.InfoLevel), &recorder{})
	s.tick(context.Background())

	// The name and both namespaces, but no metric family: one name in two
	// namespaces is one deployment mistake, not one per ceiling gauge.
	for _, want := range []string{"shared", "team-a", "team-b"} {
		assert.Contains(t, buf.String(), want, "collision warning does not mention %q: %s", want, buf.String())
	}
}

// TestScraperUsesPrivateTransport pins the scraper to its own connection pool.
//
// A nil Transport means http.DefaultTransport, and that global is shared with
// every other HTTP client in the process — including httptest, whose
// Server.Close() calls CloseIdleConnections() on http.DefaultTransport directly
// (net/http/httptest/server.go). Sharing it means one parallel test closing its
// server can tear down a connection another test's scrape is still using, and
// that scrape then fails with "http: CloseIdleConnections called" instead of
// reporting the status it was asserting on. TestScraperFailureModes below
// flaked on exactly that, roughly once in twenty runs of the suite.
// TestScraperRedactsCredentialsFromFailures proves that neither the userinfo
// nor a token query parameter in ARC_UI_LISTENER_METRICS_URL reaches
// fleet.Source.Reason, which the dashboard renders and the log records.
//
// Redacting only what this package formats is not enough on its own: a
// *url.Error carries the request URL verbatim, and net/http redacts the
// password inside it but not the query string. Both halves are checked here.
func TestScraperRedactsCredentialsFromFailures(t *testing.T) {
	t.Parallel()

	const (
		password = "s3cr3t"
		token    = "deadbeef"
	)

	t.Run("dial failure", func(t *testing.T) {
		t.Parallel()

		rec := &recorder{}
		// Port 1 has nothing listening, so this fails while dialling and the
		// error arrives wrapped in a *url.Error.
		s := NewScraper("http://user:"+password+"@127.0.0.1:1/metrics?token="+token,
			time.Hour, testLogger(), rec)
		s.tick(context.Background())

		src, ok := rec.last()
		require.True(t, ok, "no source reported")
		require.False(t, src.Available)
		assert.NotContains(t, src.Reason, password, "the password reached Source.Reason")
		assert.NotContains(t, src.Reason, token, "the token reached Source.Reason")
		assert.Contains(t, src.Reason, "127.0.0.1:1", "the endpoint is no longer identifiable")
	})

	t.Run("non-200 response", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		}))
		defer srv.Close()

		u, err := url.Parse(srv.URL)
		require.NoError(t, err)
		u.User = url.UserPassword("user", password)
		u.RawQuery = "token=" + token

		rec := &recorder{}
		s := NewScraper(u.String(), time.Hour, testLogger(), rec)
		s.tick(context.Background())

		src, ok := rec.last()
		require.True(t, ok, "no source reported")
		require.False(t, src.Available)
		assert.NotContains(t, src.Reason, password, "the password reached Source.Reason")
		assert.NotContains(t, src.Reason, token, "the token reached Source.Reason")
		assert.Contains(t, src.Reason, "401", "the status is what an operator needs to see")
	})

	t.Run("fragment", func(t *testing.T) {
		t.Parallel()

		rec := &recorder{}
		// A fragment is never sent to the server, which is exactly why it is
		// easy to forget: it is still part of the string an operator pasted
		// into ARC_UI_LISTENER_METRICS_URL, and safeURL formats that string
		// into Source.Reason for the dashboard to render.
		s := NewScraper("http://127.0.0.1:1/metrics#token="+token,
			time.Hour, testLogger(), rec)
		s.tick(context.Background())

		src, ok := rec.last()
		require.True(t, ok, "no source reported")
		require.False(t, src.Available)
		assert.NotContains(t, src.Reason, token, "the fragment reached Source.Reason")
		assert.Contains(t, src.Reason, "127.0.0.1:1", "the endpoint is no longer identifiable")
	})
}

// TestScraperBodyLimit pins the behaviour at the maxBodyBytes boundary.
//
// io.LimitReader reports EOF at the cap, which a Prometheus text parser cannot
// tell apart from the real end of the exposition. A body that overruns the cap
// on a line boundary therefore parses cleanly as its own prefix, and the
// scraper publishes the truncated result as known — a dashboard confidently
// showing a queue depth that is missing every series past the 8MiB mark. The
// scrape has to fail instead.
func TestScraperBodyLimit(t *testing.T) {
	t.Parallel()

	// A comment line the parser skips, sized so a whole number of them lands
	// exactly on the cap. That puts the truncation point on a line boundary,
	// which is the case that parses cleanly rather than erroring by luck.
	const padLine = 64
	padding := strings.Repeat("#"+strings.Repeat("p", padLine-2)+"\n", maxBodyBytes/padLine)
	require.Len(t, padding, maxBodyBytes, "padding must sit exactly on the cap")

	serve := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = io.WriteString(w, body)
		}))
	}

	t.Run("over the limit is an error, not a truncated success", func(t *testing.T) {
		t.Parallel()

		// The real metrics sit past the cap, so a reader that stops at the cap
		// sees only the padding: valid, parseable, and empty.
		srv := serve(padding + fixture)
		defer srv.Close()

		rec := &recorder{}
		s := NewScraper(srv.URL, time.Hour, testLogger(), rec)
		s.tick(context.Background())

		src, ok := rec.last()
		require.True(t, ok, "no source reported")
		assert.False(t, src.Available,
			"an over-long body was published as a good scrape: %q", src.Reason)
		assert.False(t, rec.known, "truncated metrics were published as known")
	})

	t.Run("exactly at the limit still succeeds", func(t *testing.T) {
		t.Parallel()

		// One byte under the cap would be a weaker test: the point is that the
		// limit is inclusive, so a body of exactly maxBodyBytes is fine.
		pad := maxBodyBytes - len(fixture)
		require.Positive(t, pad, "fixture must be smaller than the cap")
		body := fixture + "#" + strings.Repeat("p", pad-2) + "\n"
		require.Len(t, body, maxBodyBytes, "body must sit exactly on the cap")

		srv := serve(body)
		defer srv.Close()

		rec := &recorder{}
		s := NewScraper(srv.URL, time.Hour, testLogger(), rec)
		s.tick(context.Background())

		src, _ := rec.last()
		assert.True(t, src.Available,
			"a body exactly at the cap was rejected: %q", src.Reason)
		assert.Equal(t, 4, rec.depth["arc-linux-x64"],
			"depth = %v, want arc-linux-x64=4", rec.depth)
	})
}

func TestScraperUsesPrivateTransport(t *testing.T) {
	t.Parallel()

	s := NewScraper("http://127.0.0.1:1", time.Hour, testLogger(), &recorder{})

	require.NotNil(t, s.client.Transport,
		"Transport is nil, which means the process-global http.DefaultTransport")
	assert.NotSame(t, http.DefaultTransport, s.client.Transport,
		"scraper shares the process-global connection pool")
}

func TestScraperFailureModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantSub string
	}{
		{
			name:    "non-200",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) },
			wantSub: "503",
		},
		{
			name: "unparseable body",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "<html>not prometheus at all</html>\n")
			},
			wantSub: "parsing prometheus exposition",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tc.handler)
			defer srv.Close()

			rec := &recorder{}
			s := NewScraper(srv.URL, time.Hour, testLogger(), rec)
			s.tick(context.Background())

			src, ok := rec.last()
			require.True(t, ok, "no source reported")
			require.False(t, src.Available, "source available after a failed scrape")
			assert.Contains(t, src.Reason, tc.wantSub, "want the reason to mention %q", tc.wantSub)
			// A stale queue depth is worse than none: it has no timestamp on
			// screen, so the reader cannot tell it is old.
			assert.False(t, rec.known, "queue depth still reported as known after a failed scrape")
		})
	}
}

func TestScraperUnreachableEndpoint(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now

	rec := &recorder{}
	s := NewScraper(url, time.Hour, testLogger(), rec)
	s.tick(context.Background())

	src, ok := rec.last()
	require.True(t, ok, "no source reported for a dead endpoint")
	require.False(t, src.Available, "want an unavailable source for a dead endpoint, got %+v", src)
	assert.Contains(t, src.Reason, url, "want the reason to name the URL")
}

func TestScraperRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, fixture)
	}))
	defer srv.Close()

	rec := &recorder{}
	s := NewScraper(srv.URL, time.Hour, testLogger(), rec)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Let the immediate first scrape land, then shut down.
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.depthN > 0
	}, 5*time.Second, time.Millisecond, "no scrape before the first tick; queue depth would be blank for a whole interval")
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "Run must return nil on clean shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestScraperCancelledContextKeepsHealth: a scrape that fails only because the
// process is shutting down must not accuse the listener of being down.
func TestScraperCancelledContextKeepsHealth(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, fixture)
	}))
	defer srv.Close()

	rec := &recorder{}
	s := NewScraper(srv.URL, time.Hour, testLogger(), rec)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.tick(ctx)

	_, ok := rec.last()
	assert.False(t, ok, "a shutdown-cancelled scrape reported a source change")
}

func TestNewScraperDefaultsInterval(t *testing.T) {
	t.Parallel()

	for _, in := range []time.Duration{0, -time.Second} {
		s := NewScraper("http://example.invalid/metrics", in, testLogger(), &recorder{})
		assert.Equal(t, defaultInterval, s.interval, "interval for %v (a non-positive ticker panics)", in)
	}
}

// TestScraperFlappingEndpointDoesNotRepeatCollisionWarning: a failed scrape says
// nothing about how the cluster is deployed, so a failure between two identical
// scrapes must not turn into a fresh announcement of the same collision.
func TestScraperFlappingEndpointDoesNotRepeatCollisionWarning(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	up := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if !up {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, collisions)
	}))
	defer srv.Close()

	setUp := func(v bool) {
		mu.Lock()
		defer mu.Unlock()
		up = v
	}

	var buf strings.Builder
	s := NewScraper(srv.URL, time.Hour, zerolog.New(&buf).Level(zerolog.InfoLevel), &recorder{})
	s.tick(context.Background())
	setUp(false)
	s.tick(context.Background())
	setUp(true)
	s.tick(context.Background())

	assert.Equal(t, 1, strings.Count(buf.String(), "more than one namespace"),
		"the collision was announced again after a failed scrape:\n%s", buf.String())
}

// TestCollisionTrackerLogsStateChangesOnly pins the tracker's whole state
// machine, including the two silences: a deployment with no collisions must not
// announce that, and one that has been fixed must say so exactly once.
func TestCollisionTrackerLogsStateChangesOnly(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := zerolog.New(&buf).Level(zerolog.InfoLevel)
	shared := []collision{{set: "shared", namespaces: []string{"team-a", "team-b"}}}

	var c collisionTracker
	c.observe(log, nil)
	assert.Empty(t, buf.String(), "the common case — no collisions — must say nothing at all")

	c.observe(log, shared)
	c.observe(log, shared)
	assert.Equal(t, 1, strings.Count(buf.String(), "more than one namespace"),
		"an unchanged collision must be announced once, got:\n%s", buf.String())

	c.observe(log, []collision{{set: "shared", namespaces: []string{"team-a", "team-c"}}})
	assert.Equal(t, 2, strings.Count(buf.String(), "more than one namespace"),
		"a third namespace joining is news, got:\n%s", buf.String())

	c.observe(log, nil)
	c.observe(log, nil)
	assert.Equal(t, 1, strings.Count(buf.String(), "collisions resolved"),
		"a fixed deployment must report once, got:\n%s", buf.String())
}

func TestHealthTrackerLogsTransitionsOnly(t *testing.T) {
	t.Parallel()

	var buf strings.Builder
	log := zerolog.New(&buf).Level(zerolog.InfoLevel)

	var h healthTracker
	h.fail(log, "boom")
	h.fail(log, "boom")
	assert.Equal(t, 1, strings.Count(buf.String(), "source unavailable"), "identical failures must log once above debug")
	h.ok(log)
	h.ok(log)
	assert.Equal(t, 1, strings.Count(buf.String(), "source recovered"), "recovery must log once")
}
