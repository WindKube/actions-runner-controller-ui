package web

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-h/templ"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/fleet"
)

// TestWritePreview renders all three views to standalone HTML files.
//
// It exists because the charts are server-computed SVG: their geometry is
// decided in Go, so the only way to see whether a chart is right is to look at
// one. Set ARC_UI_PREVIEW_DIR to a directory and run:
//
//	go test ./internal/web -run TestWritePreview
//
// The CSS is inlined so the files open straight from disk with no server.
func TestWritePreview(t *testing.T) {
	dir := os.Getenv("ARC_UI_PREVIEW_DIR")
	if dir == "" {
		t.Skip("set ARC_UI_PREVIEW_DIR to render preview HTML")
	}

	css, err := staticFS.ReadFile("static/app.css")
	require.NoError(t, err, "read app.css (run `task gen:css` first)")

	b := &Builder{
		Fleet:    SnapshotFunc(previewSnapshot),
		History:  previewHistory{},
		Version:  "preview",
		Interval: 15 * time.Second,
		// Only needs to be a stable string for write() to match on — the link is
		// swapped for an inline <style> there. JS is deliberately left unset: the
		// layout omits the module script entirely when Page.JS is empty, and a
		// preview file has no server to stream from. An empty src= would resolve
		// to the page itself and make the browser parse this HTML as a module.
		CSS: "/static/app.css",
	}
	ctx := context.Background()
	sig := Signals{}.Normalize()

	overview := b.Overview(ctx, sig, previewNow)
	overview.Stream = "/stream"
	write(t, dir, "overview.html", css, overview.Page, OverviewPage(overview))

	set, ok := b.Set(ctx, "arc-ubuntu-2xl", sig, previewNow)
	require.True(t, ok, "preview set missing")
	set.Stream = "/stream/runnersets/arc-ubuntu-2xl"
	write(t, dir, "runnerset.html", css, set.Page, SetPage(set))

	runner, ok := b.Runner(ctx, "arc-ubuntu-2xl-r7k2p", sig, previewNow, previewEvents())
	require.True(t, ok, "preview runner missing")
	runner.Stream = "/stream/runners/arc-ubuntu-2xl-r7k2p"
	write(t, dir, "runner.html", css, runner.Page, RunnerPage(runner))

	workflows := b.Workflows(ctx, sig, previewNow)
	workflows.Stream = "/stream/workflows"
	write(t, dir, "workflows.html", css, workflows.Page, WorkflowsPage(workflows))

	jobs := b.Jobs(ctx, sig, previewNow)
	jobs.Stream = "/stream/jobs"
	write(t, dir, "jobs.html", css, jobs.Page, JobsPage(jobs))

	job, ok := b.Job(ctx, previewJobID, sig, previewNow)
	require.True(t, ok, "preview job missing")
	job.Stream = fmt.Sprintf("/stream/jobs/%d", previewJobID)
	write(t, dir, "job.html", css, job.Page, JobPage(job))
}

// previewJobID is the job the detail preview renders — the one with a full
// series behind it, so the chart has something to draw.
const previewJobID = 1

// previewJobs is a window of job history: a mix of outcomes, two repositories,
// one job still running, and one that ARC reported no run id for.
func previewJobs() []Job {
	mins := func(n int) time.Time { return previewNow.Add(-time.Duration(n) * time.Minute) }
	return []Job{
		{
			ID: previewJobID, Runner: "arc-ubuntu-2xl-r7k2p", Set: "arc-ubuntu-2xl",
			Repository: "WindKube/platform", Workflow: "ci.yml", Name: "build (ubuntu-2xl)",
			RunID: 4_182_993, StartedAt: mins(42), Succeeded: false,
			CPUSeconds: 2_940, MemGiBSecs: 5_880,
			// The detail preview is rendered from this row. Memory is
			// burstable and CPU has no limit, which is the ARC default and
			// gives the chart one line on one axis and two on the other.
			CPURequest: 2, MemRequest: 3 * fleet.GiB, MemLimit: 4 * fleet.GiB,
		},
		{
			ID: 2, Runner: "arc-ubuntu-2xl-h9z1w", Set: "arc-ubuntu-2xl",
			Repository: "WindKube/platform", Workflow: "ci.yml", Name: "test (integration)",
			RunID: 4_182_993, StartedAt: mins(41), FinishedAt: mins(12), Succeeded: true,
			CPUSeconds: 3_180, MemGiBSecs: 7_420,
		},
		{
			ID: 3, Runner: "arc-arm64-graviton-b2k7d", Set: "arc-arm64-graviton",
			Repository: "WindKube/actions-runner-controller-ui", Workflow: "release.yml",
			Name: "publish", RunID: 4_182_871, StartedAt: mins(96), FinishedAt: mins(74),
			Succeeded: false, CPUSeconds: 1_104, MemGiBSecs: 2_030,
		},
		{
			ID: 4, Runner: "arc-ubuntu-2xl-x7p3r", Set: "arc-ubuntu-2xl",
			Repository: "WindKube/platform", Workflow: "nightly.yml", Name: "e2e",
			StartedAt: mins(188), FinishedAt: mins(41), Succeeded: true,
			CPUSeconds: 26_400, MemGiBSecs: 61_200,
		},
	}
}

// Jobs is the window's job history, narrowed the way the store would narrow
// it. The filtering is deliberately real rather than ignored: the preview is
// how the filter bar's own rendering gets checked.
func (previewHistory) Jobs(_ context.Context, f JobFilter, _ Window) (JobList, error) {
	var out []Job
	for _, j := range previewJobs() {
		if f.Repository != "" && j.Repository != f.Repository {
			continue
		}
		if f.Workflow != "" && j.Workflow != f.Workflow {
			continue
		}
		out = append(out, j)
	}
	return JobList{Jobs: out, Total: 4_312}, nil
}

// Workflows rolls the same jobs up the way the store's GROUP BY would.
func (previewHistory) Workflows(_ context.Context, _ JobFilter, _ Window) (WorkflowList, error) {
	mins := func(n int) time.Time { return previewNow.Add(-time.Duration(n) * time.Minute) }
	return WorkflowList{
		Runs: []WorkflowRun{
			{
				Repository: "WindKube/platform", Workflow: "ci.yml", RunID: 4_182_993,
				Jobs: 12, Running: 1, Failed: 0, StartedAt: mins(42),
				CPUSeconds: 18_240, MemGiBSecs: 41_100,
			},
			{
				Repository: "WindKube/actions-runner-controller-ui", Workflow: "release.yml",
				RunID: 4_182_871, Jobs: 3, Failed: 1, StartedAt: mins(96), FinishedAt: mins(74),
				CPUSeconds: 3_312, MemGiBSecs: 6_090,
			},
			{
				Repository: "WindKube/platform", Workflow: "nightly.yml",
				Jobs: 5, StartedAt: mins(188), FinishedAt: mins(41),
				CPUSeconds: 44_800, MemGiBSecs: 98_300,
			},
		},
		Total: 187,
	}, nil
}

// Job returns one of the fixtures above by id.
func (h previewHistory) Job(_ context.Context, id int) (Job, bool, error) {
	for _, j := range previewJobs() {
		if j.ID == id {
			return j, true, nil
		}
	}
	return Job{}, false, nil
}

// JobSeries is a job ramping up and then holding, which is the shape a build
// actually makes and the one that shows whether the chart's peak scaling is
// right.
func (previewHistory) JobSeries(_ context.Context, id int, _ Window) (JobSeries, error) {
	if id != previewJobID {
		return JobSeries{}, nil
	}
	const n = 42
	out := JobSeries{
		At:  make([]time.Time, 0, n),
		CPU: make([]float64, 0, n),
		Mem: make([]float64, 0, n),
	}
	for i := range n {
		at := previewNow.Add(-time.Duration(n-i) * time.Minute)
		ramp := math.Min(1, float64(i)/8)
		wobble := 0.18 * math.Sin(float64(i)/2.3)
		out.At = append(out.At, at)
		out.CPU = append(out.CPU, 1.15*ramp+wobble)
		out.Mem = append(out.Mem, (2.4*ramp+0.4*wobble)*fleet.GiB)
	}
	return out, nil
}

// Facets are the dropdown options, which on a real store come from what the
// window contains rather than from the whole table.
func (previewHistory) Facets(context.Context, Window) (JobFacets, error) {
	return JobFacets{
		Repositories: []string{"WindKube/actions-runner-controller-ui", "WindKube/platform"},
		Workflows:    []string{"ci.yml", "nightly.yml", "release.yml"},
		Sets:         []string{"arc-arm64-graviton", "arc-ubuntu-2xl"},
	}, nil
}

func write(t *testing.T, dir, name string, css []byte, p Page, body templ.Component) {
	t.Helper()

	var sb strings.Builder
	require.NoError(t, Document(p, body).Render(context.Background(), &sb), "render %s", name)

	// Swap the stylesheet link for the real thing so the file is self-contained.
	// The module script needs no handling here: the layout omits it when Page.JS
	// is empty, which is why the preview Builder leaves it unset.
	html := strings.Replace(sb.String(),
		`<link rel="stylesheet" href="/static/app.css">`,
		"<style>"+string(css)+"</style>", 1)

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(html), 0o600), "write %s", path)
	t.Logf("wrote %s (%d bytes)", path, len(html))
}

var previewNow = time.Date(2026, 8, 17, 14, 30, 0, 0, time.UTC)

// previewSnapshot is a fleet under real load: several sets, one saturated, one
// unbounded, a mix of runner states, and partial metrics coverage.
func previewSnapshot() fleet.Snapshot {
	sets := []fleet.RunnerSet{
		{
			Name: "arc-ubuntu-2xl", Namespace: "arc-runners",
			MinRunners: 4, MaxRunners: 30, Current: 18,
			RunnerGroup: "default", Phase: "Running",
			Image:           "ghcr.io/actions/actions-runner:2.328.0",
			RunnerLabels:    []string{"self-hosted", "linux", "x64", "ubuntu-2xl"},
			GitHubConfigURL: "https://github.com/WindKube",
			ScaleSetID:      "17",
			CPURequest:      4, MemRequest: 8 * fleet.GiB,
			CPULimit: 8, MemLimit: 16 * fleet.GiB,
			Queued: 6, QueuedKnown: true,
			ListenerKnown: true, ListenerHealthy: true,
		},
		{
			Name: "arc-ubuntu-small", Namespace: "arc-runners",
			MinRunners: 2, MaxRunners: 12, Current: 12,
			RunnerGroup: "default", Phase: "Running",
			Image:           "ghcr.io/actions/actions-runner:2.328.0",
			RunnerLabels:    []string{"self-hosted", "linux", "x64"},
			GitHubConfigURL: "https://github.com/WindKube",
			ScaleSetID:      "18",
			CPURequest:      1, MemRequest: 2 * fleet.GiB,
			Queued: 11, QueuedKnown: true,
			ListenerKnown: true, ListenerHealthy: true,
		},
		{
			Name: "arc-arm64", Namespace: "arc-runners",
			MinRunners: 0, Unbounded: true, Current: 3,
			RunnerGroup: "arm", Phase: "Running",
			Image:           "ghcr.io/actions/actions-runner:2.328.0-arm64",
			RunnerLabels:    []string{"self-hosted", "linux", "arm64"},
			GitHubConfigURL: "https://github.com/WindKube",
			ScaleSetID:      "22",
			CPURequest:      2, MemRequest: 4 * fleet.GiB,
			Queued: 0, QueuedKnown: true,
			ListenerKnown: true, ListenerHealthy: false,
		},
	}

	type spec struct {
		set, repo, wf, job string
		state              fleet.State
		cpu, mem           float64
		scraped            bool
		ageMin, jobMin     int
		fail               string
	}
	specs := []spec{
		{"arc-ubuntu-2xl", "WindKube/web-api", "ci.yml", "rspec (3/8)", fleet.StateBusy, 3.4, 6.1, true, 42, 19, ""},
		{"arc-ubuntu-2xl", "WindKube/web-api", "ci.yml", "rspec (5/8)", fleet.StateBusy, 3.9, 6.8, true, 38, 16, ""},
		{"arc-ubuntu-2xl", "WindKube/payments", "e2e.yml", "playwright", fleet.StateBusy, 2.1, 7.4, true, 31, 12, ""},
		{"arc-ubuntu-2xl", "WindKube/web-api", "ci.yml", "lint", fleet.StateBusy, 0.7, 1.9, true, 22, 8, ""},
		{"arc-ubuntu-2xl", "WindKube/mobile", "build.yml", "android", fleet.StateBusy, 3.7, 7.9, true, 17, 6, ""},
		{"arc-ubuntu-2xl", "", "", "", fleet.StateIdle, 0.05, 0.4, true, 14, 0, ""},
		{"arc-ubuntu-2xl", "", "", "", fleet.StateIdle, 0.04, 0.4, true, 11, 0, ""},
		{"arc-ubuntu-2xl", "", "", "", fleet.StatePending, 0, 0, false, 1, 0, ""},
		{"arc-ubuntu-2xl", "", "", "", fleet.StatePending, 0, 0, false, 0, 0, ""},
		{"arc-ubuntu-small", "WindKube/docs", "publish.yml", "build", fleet.StateBusy, 0.8, 1.4, true, 25, 9, ""},
		{"arc-ubuntu-small", "WindKube/web-api", "nightly.yml", "smoke", fleet.StateBusy, 0.6, 1.1, true, 20, 7, ""},
		{"arc-ubuntu-small", "WindKube/payments", "ci.yml", "unit", fleet.StateBusy, 0.9, 1.7, true, 15, 5, ""},
		{"arc-ubuntu-small", "", "", "", fleet.StateTerminating, 0, 0, false, 33, 0, ""},
		{"arc-ubuntu-small", "", "", "", fleet.StateFailed, 0, 0, false, 4, 0, "OOMKilled"},
		{"arc-arm64", "WindKube/mobile", "build.yml", "ios-sim", fleet.StateBusy, 1.6, 3.2, true, 12, 4, ""},
		{"arc-arm64", "", "", "", fleet.StateIdle, 0.03, 0.3, true, 9, 0, ""},
		{"arc-arm64", "", "", "", fleet.StateFailed, 0, 0, false, 2, 0, "ImagePullBackOff"},
	}

	runners := make([]fleet.Runner, 0, len(specs))
	names := []string{"r7k2p", "m4x9q", "b8n3v", "t2w6z", "k9j5h", "d3f7l", "q6r1c", "z8y4m",
		"n5p2g", "v7t9s", "h4k6b", "w2m8d", "j9c3x", "f6q1n", "s3v7r", "y8b5t", "g1n4w"}
	for i, s := range specs {
		r := fleet.Runner{
			Name: s.set + "-" + names[i%len(names)], Namespace: "arc-runners", SetName: s.set,
			State:     s.state,
			CreatedAt: previewNow.Add(-time.Duration(s.ageMin) * time.Minute),
			Node:      []string{"ip-10-0-1-42", "ip-10-0-2-17", "ip-10-0-3-88"}[i%3],
			Image:     "ghcr.io/actions/actions-runner:2.328.0",
			PodPhase:  "Running", PodUID: "uid-" + names[i%len(names)],
			FailureReason: s.fail,
		}
		if s.fail != "" {
			r.FailedAt = previewNow.Add(-time.Duration(s.ageMin) * time.Minute)
			r.PodPhase = "Failed"
		}
		if s.repo != "" {
			r.Job = fleet.Job{
				Repository: s.repo, Workflow: s.wf, Name: s.job,
				RunID:     int64(48200 + i*7),
				StartedAt: previewNow.Add(-time.Duration(s.jobMin) * time.Minute),
			}
		}
		for _, set := range sets {
			if set.Name == s.set {
				r.CPU.Request, r.Mem.Request = set.CPURequest, set.MemRequest
				r.CPU.Limit, r.Mem.Limit = set.CPULimit, set.MemLimit
			}
		}
		if s.scraped {
			r.CPU.Used, r.CPU.At = s.cpu, previewNow.Add(-8*time.Second)
			r.Mem.Used, r.Mem.At = s.mem*fleet.GiB, previewNow.Add(-8*time.Second)
		}
		runners = append(runners, r)
	}

	return fleet.Snapshot{
		At: previewNow.Add(-3 * time.Second), Org: "WindKube",
		Sets: sets, Runners: runners,
		ControllerVersion: "0.14.2",
		ListenersReady:    2, ListenersTotal: 3,
		Sources: []fleet.Source{
			{Name: fleet.SourceKubernetes, Available: true, CheckedAt: previewNow},
			{Name: fleet.SourceARCCRDs, Available: true, CheckedAt: previewNow},
			{Name: fleet.SourceMetrics, Available: true, CheckedAt: previewNow},
			{Name: fleet.SourceListener, Available: true, CheckedAt: previewNow},
			{Name: fleet.SourceStore, Available: true, CheckedAt: previewNow},
		},
	}
}

func previewEvents() []fleet.Event {
	return []fleet.Event{
		{Type: "Normal", Reason: "Scheduled", At: previewNow.Add(-42 * time.Minute), Count: 1,
			Message: "Successfully assigned arc-runners/arc-ubuntu-2xl-r7k2p to ip-10-0-1-42"},
		{Type: "Normal", Reason: "Pulled", At: previewNow.Add(-41 * time.Minute), Count: 1,
			Message: `Container image "ghcr.io/actions/actions-runner:2.328.0" already present on machine`},
		{Type: "Normal", Reason: "Started", At: previewNow.Add(-41 * time.Minute), Count: 1,
			Message: "Started container runner"},
		{Type: "Warning", Reason: "Unhealthy", At: previewNow.Add(-6 * time.Minute), Count: 2,
			Message: "Readiness probe failed: dial tcp 10.0.1.42:8080: connect: connection refused"},
	}
}

// previewHistory synthesises plausible series so every chart has something to
// draw. The shapes are deliberately uneven — a flat sine would hide exactly the
// alignment bugs this preview exists to catch.
type previewHistory struct{ NoHistory }

// Failures is a lane with more in the window than fits on it, which is the
// state the "+N more" footer exists for.
func (previewHistory) Failures(_ context.Context, _ Scope, _ Window, limit int) (FailureWindow, error) {
	rows := []fleet.Failure{
		{Runner: "arc-ubuntu-2xl-q4m8t", Set: "arc-ubuntu-2xl", Reason: "ImagePullBackOff", At: previewNow.Add(-4 * time.Minute), Severe: true},
		{Runner: "arc-ubuntu-2xl-h9z1w", Set: "arc-ubuntu-2xl", Reason: "OOMKilled", At: previewNow.Add(-17 * time.Minute), Severe: true},
		{Runner: "arc-arm64-graviton-b2k7d", Set: "arc-arm64-graviton", Reason: "Evicted", At: previewNow.Add(-38 * time.Minute), Severe: true},
		{Runner: "arc-ubuntu-2xl-x7p3r", Set: "arc-ubuntu-2xl", Reason: "never registered", At: previewNow.Add(-52 * time.Minute), Severe: false},
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return FailureWindow{Failures: rows, Total: 23}, nil
}

// Stats is a plausible mid-life store: a few weeks of history on a volume that
// is nowhere near full, so the footer renders every row with realistic widths.
func (previewHistory) Stats(context.Context) (StoreStats, error) {
	return StoreStats{
		Enabled:     true,
		Path:        "/data/arc-ui.db",
		SizeBytes:   47 * 1024 * 1024,
		Samples:     3_214_887,
		Jobs:        18_402,
		JobSamples:  742_118,
		Phases:      91_755,
		ChurnEvents: 36_804,
		Failures:    1_142,
		Rows:        3_362_990,
		Oldest:      previewNow.Add(-21 * 24 * time.Hour),
	}, nil
}

func (previewHistory) Scope(_ context.Context, scope Scope, w Window) (ScopeSeries, error) {
	n := w.Points
	if n <= 0 {
		n = 60
	}
	scale := 1.0
	capacity := 42.0
	if scope != FleetScope {
		scale, capacity = 0.45, 30
	}

	s := ScopeSeries{At: make([]time.Time, n)}
	step := w.To.Sub(w.From) / time.Duration(n)
	for i := 0; i < n; i++ {
		f := float64(i) / float64(n-1)
		s.At[i] = w.From.Add(time.Duration(i) * step)

		busy := math.Max(0, (9+8*math.Sin(f*5.2)+4*math.Sin(f*13))*scale)
		idle := math.Max(0, (4+2.5*math.Cos(f*7.7))*scale)
		pending := math.Max(0, (2.5*math.Sin(f*11+1))*scale)

		s.Busy = append(s.Busy, math.Round(busy))
		s.Idle = append(s.Idle, math.Round(idle))
		s.Pending = append(s.Pending, math.Round(pending))
		s.Capacity = append(s.Capacity, capacity)

		total := busy + idle
		s.CPUUsed = append(s.CPUUsed, total*2.1+3*math.Sin(f*9))
		s.CPURequest = append(s.CPURequest, total*4)
		s.MemUsed = append(s.MemUsed, (total*4.4+2*math.Cos(f*6))*fleet.GiB)
		s.MemRequest = append(s.MemRequest, total*8*fleet.GiB)
	}
	return s, nil
}

func (previewHistory) Runner(_ context.Context, _ string, w Window) (RunnerSeries, error) {
	n := 48
	s := RunnerSeries{At: make([]time.Time, n)}
	step := w.To.Sub(w.From) / time.Duration(n)
	for i := 0; i < n; i++ {
		f := float64(i) / float64(n-1)
		s.At[i] = w.From.Add(time.Duration(i) * step)
		s.CPU = append(s.CPU, 2.6+1.2*math.Sin(f*14)+0.4*math.Sin(f*31))
		s.Mem = append(s.Mem, (5.2+1.4*math.Sin(f*6))*fleet.GiB)
	}
	s.Peak.CPU, s.Peak.Mem = PeakOf(s.CPU), PeakOf(s.Mem)
	return s, nil
}

func (previewHistory) Throughput(_ context.Context, _ Scope, w Window) (Counts, error) {
	return previewCounts(w, 24, 9, 1.6), nil
}

func (previewHistory) Churn(_ context.Context, _ Scope, w Window) (Counts, error) {
	return previewCounts(w, 30, 7, 6.4), nil
}

func previewCounts(w Window, n int, amp, freq float64) Counts {
	c := Counts{At: make([]time.Time, n)}
	step := w.To.Sub(w.From) / time.Duration(n)
	for i := 0; i < n; i++ {
		f := float64(i) / float64(n-1)
		c.At[i] = w.From.Add(time.Duration(i) * step)
		c.Up = append(c.Up, math.Round(amp*(0.55+0.45*math.Sin(f*freq))))
		c.Down = append(c.Down, math.Round(amp*0.25*(0.6+0.4*math.Cos(f*freq*1.3))))
	}
	return c
}

func (previewHistory) Repos(_ context.Context, _ Window, limit int) ([]RepoHistory, error) {
	all := []RepoHistory{
		{Repository: "WindKube/web-api", Jobs: 412, CPUSeconds: 184320, MemGiBSecs: 372000},
		{Repository: "WindKube/payments", Jobs: 208, CPUSeconds: 96400, MemGiBSecs: 201400},
		{Repository: "WindKube/mobile", Jobs: 96, CPUSeconds: 71200, MemGiBSecs: 156800},
		{Repository: "WindKube/docs", Jobs: 143, CPUSeconds: 22800, MemGiBSecs: 41200},
		{Repository: "WindKube/infra", Jobs: 61, CPUSeconds: 14100, MemGiBSecs: 29900},
	}
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}
