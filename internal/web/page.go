package web

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/samber/lo"

	"arc-ui/internal/chart"
	"arc-ui/internal/fleet"
)

// Chart geometry. Every chart is computed against a 1000-unit wide coordinate
// grid. The number is arbitrary: the SVGs are w-full with
// preserveAspectRatio="none", so the browser stretches the grid to whatever the
// panel is and 1000 only sets the resolution coordinates are computed at.
//
// The width travels with the geometry: each chart view model carries the Width its
// coordinates were computed against and the template draws its viewBox from that,
// so the two cannot drift.
//
// For the two charts with y-axis labels — area and line — the height constant does
// double duty and is load-bearing: it is also passed as renderedPx to chart.Grid,
// and to LabelTopPx for the area chart's capacity marker, so those pixel offsets
// are right only while the SVG really is that many CSS pixels tall. Change
// historyH or lineH and the matching height class in charts.templ has to change
// with it; TestLabelledChartsRenderAtTheHeightTheirLabelsAssume guards the pair.
// barH and churnH have no labels beside them and no such tie.
//
// One trap for whoever edits these comments: the CSS build scans this package's
// .go and .templ files as plain text, so anything in a comment that parses as a
// Tailwind class is emitted as a real rule into app.css.
const (
	chartW = 1000.0

	historyH   = 200.0
	historyTop = 8.0

	lineH = 120.0

	barH   = 120.0
	barGap = 5.0

	churnH      = 140.0
	churnCentre = 70.0
)

// SVG paint values. These duplicate the Tailwind token palette because SVG
// presentation attributes are not Tailwind's to style: the geometry functions
// emit fill and stroke strings directly.
const (
	paintBusy    = "rgba(63,185,80,0.30)"
	paintIdle    = "rgba(68,147,248,0.26)"
	paintPending = "rgba(210,153,34,0.30)"

	strokeBusy    = "#3fb950"
	strokeIdle    = "#4493f8"
	strokePending = "#d29922"
	strokeDanger  = "#f85149"
	strokeCPU     = "#ab7df8"
	strokeMem     = "#39c5cf"
	strokeMuted   = "#9198a1"

	fillCPU  = "rgba(171,125,248,0.16)"
	fillMem  = "rgba(57,197,207,0.16)"
	fillUtil = "rgba(68,147,248,0.16)"
)

// Snapshotter supplies the current fleet state. The collector satisfies it;
// tests hand in a literal.
type Snapshotter interface {
	Snapshot() fleet.Snapshot
}

// SnapshotFunc adapts a function to Snapshotter.
type SnapshotFunc func() fleet.Snapshot

func (f SnapshotFunc) Snapshot() fleet.Snapshot { return f() }

// Builder assembles view models. It is the only place that knows how to turn
// a snapshot plus a history query into something a template can render, which
// is what keeps the templates free of logic and the handlers free of layout.
type Builder struct {
	Fleet   Snapshotter
	History History
	Version string
	// Interval is the configured scrape interval, shown in the health strip so
	// an operator can tell a stale chart from a slow one.
	Interval time.Duration

	// CSS and JS are the content-hashed asset URLs. They are carried on the
	// page rather than read from a package-level variable so a test can render
	// the whole document without an embedded filesystem.
	CSS string
	JS  string
}

// Page is the chrome every view shares.
type Page struct {
	Title   string
	Org     string
	Version string
	Signals Signals
	Now     time.Time
	Range   TimeRange

	// Lag is how long ago the last collection completed. The design's pulsing
	// dot and "last event N.Ns" read this, which makes the indicator an honest
	// staleness readout rather than a decoration.
	Lag   time.Duration
	Stale bool

	Sources  []SourceRow
	Warnings []string
	Crumbs   []Crumb

	// Tab is which of the top-level views is showing, so the tab strip can
	// mark it. The zero value is the fleet overview, which is what a page
	// built before the strip existed should light up.
	Tab Tab

	// Stream is the SSE endpoint this page's controls reopen. Each view has
	// its own, so a filter change on a detail page refreshes that page rather
	// than the fleet overview.
	Stream string

	CSS string
	JS  string
}

// Tab names one of the dashboard's top-level views.
type Tab string

// The three tabs. TabFleet is the zero value so a page that sets none is the
// overview.
const (
	TabFleet     Tab = ""
	TabWorkflows Tab = "workflows"
	TabJobs      Tab = "jobs"
)

// TabLink is one entry in the top-level tab strip.
type TabLink struct {
	Tab   Tab
	Label string
	Href  string
}

// AllTabs lists the tabs in strip order.
//
// The hrefs carry no filter state on purpose. Switching tab is a change of
// subject — from what the fleet is doing now to what it did — and the fleet
// tab's filters are over runner state, which the history tabs have no column
// for. Carrying them across would silently empty the table someone just
// clicked into.
func AllTabs() []TabLink {
	return []TabLink{
		{Tab: TabFleet, Label: "fleet", Href: "/"},
		{Tab: TabWorkflows, Label: "workflows", Href: "/workflows"},
		{Tab: TabJobs, Label: "jobs", Href: "/jobs"},
	}
}

// Crumb is one breadcrumb segment. A crumb with no Href is the current page.
type Crumb struct {
	Label string
	Href  string
}

// SourceRow is one entry in the "control plane & collection" strip.
type SourceRow struct {
	Label string
	Value string
	Tone  Tone
}

// Overview is everything the main dashboard renders.
type Overview struct {
	Page

	Selects []fleet.Select
	Summary string
	Active  int

	Tiles  []Tile
	Totals fleet.Totals

	History    AreaChart
	Util       LineChart
	CPU        LineChart
	Mem        LineChart
	Throughput BarChart
	Churn      ChurnChart

	Sets     []SetRow
	Runners  []fleet.Runner
	Repos    []RepoBar
	Failures FailureLane

	// Store is the history store's own footprint, reported at the foot of the
	// page. Nothing else on the dashboard is about the dashboard itself.
	Store StorePanel

	SetSort fleet.SetSort
}

// FailureLane is the recent-failures panel.
type FailureLane struct {
	Items []fleet.Failure
	// More is how many failures the window holds beyond the ones listed. It is
	// zero on a lane assembled without the store, which knows of no window.
	More int
	// Window names the range the lane covers, e.g. "last 6 hours". Empty means
	// the lane is the live snapshot rather than a window of history.
	Window string
}

// StorePanel is the SQLite footer's view model.
type StorePanel struct {
	// Enabled is false when there is no history store, in which case Rows is
	// empty and the panel explains itself rather than reporting zeros.
	Enabled bool
	Rows    []KV
}

// SetDetail is the per-RunnerSet view.
type SetDetail struct {
	Page

	Set     fleet.RunnerSet
	Totals  fleet.Totals
	Tiles   []Tile
	Config  []KV
	History AreaChart
	CPU     LineChart
	Mem     LineChart
	Churn   ChurnChart
	Runners []fleet.Runner
}

// RunnerDetail is the per-runner view.
type RunnerDetail struct {
	Page

	Runner fleet.Runner
	Set    fleet.RunnerSet
	Facts  []KV
	CPU    LineChart
	Mem    LineChart
	Events []fleet.Event

	// SetJobs is recent job activity across the whole RunnerSet rather than
	// this runner. An ARC ephemeral runner executes exactly one job and then
	// terminates, so a per-runner job history would always be one row.
	SetJobs []JobRow
}

// JobRow is one line of the RunnerSet job activity panel.
type JobRow struct {
	Runner     string
	Repository string
	Workflow   string
	Job        string
	State      fleet.State
	Age        string
	Current    bool
}

// KV is one label/value row in a config or facts panel.
type KV struct {
	Label string
	Value string
	Mono  bool
	Tone  Tone
}

// Tile is one KPI in the summary strip.
type Tile struct {
	Label string
	Value string
	Sub   string
	Tone  Tone
	// Bar, when Total is positive, draws a micro-bar under the value.
	Bar   float64
	Total float64
}

// SetRow is one line of the runnersets table, with its capacity bar
// pre-segmented.
type SetRow struct {
	Set     fleet.RunnerSet
	Totals  fleet.Totals
	Tone    Tone
	Badge   string
	Bounds  string
	Busy    string
	Idle    string
	Pending string
	Queued  string
}

// RepoBar is one row of the per-repository consumption panel.
type RepoBar struct {
	Repository string
	Value      float64
	Max        float64
	Label      string
	Detail     string
}

// AreaChart is a stacked area chart with an optional dashed ceiling.
type AreaChart struct {
	// Width is the viewBox width every coordinate in this struct was computed
	// against, and the width the template draws its viewBox with. It is zero on
	// an empty chart, which renders a placeholder rather than an SVG.
	Width float64

	Areas []chart.Area
	Grid  []chart.GridLine
	Ticks []string

	// Cap is the dashed max-capacity line, absent for unbounded sets.
	Cap      string
	CapLabel string
	CapTopPx string

	Legend []LegendItem
	Empty  bool
}

// LegendItem is one swatch and label beneath a chart.
type LegendItem struct {
	Label string
	Tone  Tone
	Value string
}

// LineChart is a single series with optional flat reference lines.
type LineChart struct {
	// Width is the geometry's viewBox width; see AreaChart.Width.
	Width float64

	Title  string
	Line   chart.Line
	Refs   []RefLine
	Grid   []chart.GridLine
	Ticks  []string
	Legend []LegendItem
	Empty  bool
}

// RefLine is a horizontal request or limit marker.
type RefLine struct {
	Points string
	Stroke string
	Label  string
}

// BarChart is a two-segment stacked column chart.
type BarChart struct {
	// Width is the geometry's viewBox width; see AreaChart.Width.
	Width float64

	Bars   []chart.StackedBar
	Ticks  []string
	Legend []LegendItem
	Empty  bool
}

// ChurnChart is a column chart mirrored about a centre line.
type ChurnChart struct {
	// Width is the geometry's viewBox width; see AreaChart.Width. The mirror
	// line spans it, so a churn chart drawn at any other width would have its
	// bars hanging off the end of the line they are mirrored about.
	Width float64

	Bars   []chart.DivergingBar
	Centre string
	Ticks  []string
	Legend []LegendItem
	Empty  bool
}

// page builds the shared chrome.
func (b *Builder) page(title string, s fleet.Snapshot, sig Signals, now time.Time, crumbs []Crumb) Page {
	var lag time.Duration
	if !s.At.IsZero() {
		lag = max(0, now.Sub(s.At))
	}

	return Page{
		Title:   title,
		Org:     s.Org,
		Version: b.Version,
		Signals: sig,
		Now:     now,
		Range:   ParseRange(sig.Range),
		Lag:     lag,
		// Two missed collections is the point at which a chart is showing
		// something the cluster has moved on from.
		Stale:   s.At.IsZero() || lag > 2*b.interval(),
		Sources: sourceRows(s, b.interval()),
		Crumbs:  crumbs,
		CSS:     b.CSS,
		JS:      b.JS,
	}
}

func (b *Builder) interval() time.Duration {
	if b.Interval <= 0 {
		return 15 * time.Second
	}
	return b.Interval
}

// Overview builds the main dashboard.
//
// History failures are absorbed into empty panels rather than returned: the live
// half of this page comes from the informer cache and is still worth serving when
// SQLite is unhappy.
func (b *Builder) Overview(ctx context.Context, sig Signals, now time.Time) Overview {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()
	rng := ParseRange(sig.Range)
	win := rng.Window(now)

	filter := sig.Filter()
	runners, sets := filter.Apply(snap)
	fleet.SortRunners(runners, now)

	setSort := fleet.ParseSetSort(sig.Sort)
	fleet.SortSets(sets, setSort)

	totals := fleet.Aggregate(runners, sets)

	// A set filter scopes the history to that set; anything else still asks
	// for the fleet series, because the store has no per-repository history.
	scope := FleetScope
	if filter.Set != "" && filter.Set != fleet.AnyValue {
		scope = Set(filter.Set)
	}

	series, _ := b.History.Scope(ctx, scope, win)
	throughput, _ := b.History.Throughput(ctx, scope, win)
	churn, _ := b.History.Churn(ctx, scope, win)
	repos, _ := b.History.Repos(ctx, win, 8)
	stats, _ := b.History.Stats(ctx)

	// A store that cannot answer leaves the live snapshot, which still knows what is
	// broken this instant. An empty lane would claim a healthy fleet at the exact
	// moment the dashboard has least to go on.
	lane := FailureLane{Items: fleet.Failures(runners, failureLaneRows)}
	if stored, err := b.History.Failures(ctx, scope, win, failureLaneRows); err == nil {
		lane = mergeFailures(stored, fleet.Failures(runners, failureLaneRows), failureLaneRows, rng.Label())
	}

	page := b.page("Fleet", snap, sig, now, []Crumb{{Label: snap.Org}, {Label: "fleet"}})
	page.Warnings = warnings(snap, totals)

	return Overview{
		Page:       page,
		Selects:    filter.Selects(snap),
		Summary:    filter.Summary(len(runners), len(snap.Runners), len(snap.Sets)),
		Active:     filter.Active(),
		Tiles:      overviewTiles(totals),
		Totals:     totals,
		History:    areaChart(series, rng),
		Util:       utilChart(series, rng),
		CPU:        cpuChart(series, rng),
		Mem:        memChart(series, rng),
		Throughput: throughputChart(throughput, rng),
		Churn:      churnChart(churn, rng),
		Sets:       setRows(fleet.GroupBySet(runners, sets)),
		Runners:    runners,
		Repos:      repoBars(repos, runners),
		Failures:   lane,
		Store:      storePanel(stats, now),
		SetSort:    setSort,
	}
}

// Set builds the RunnerSet detail view. It reports false when the named set is
// not in the current snapshot, which the handler turns into a 404.
func (b *Builder) Set(ctx context.Context, name string, sig Signals, now time.Time) (SetDetail, bool) {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()

	set, ok := snap.Set(name)
	if !ok {
		return SetDetail{}, false
	}

	rng := ParseRange(sig.Range)
	win := rng.Window(now)

	runners := lo.Filter(snap.Runners, func(r fleet.Runner, _ int) bool { return r.SetName == name })
	fleet.SortRunners(runners, now)
	totals := fleet.Aggregate(runners, []fleet.RunnerSet{set})

	series, _ := b.History.Scope(ctx, Set(name), win)
	churn, _ := b.History.Churn(ctx, Set(name), win)

	page := b.page(name, snap, sig, now, []Crumb{
		{Label: snap.Org},
		{Label: "fleet", Href: "/"},
		{Label: name},
	})

	return SetDetail{
		Page:    page,
		Set:     set,
		Totals:  totals,
		Tiles:   setTiles(set, totals),
		Config:  setConfig(set),
		History: areaChart(series, rng),
		CPU:     cpuChart(series, rng),
		Mem:     memChart(series, rng),
		Churn:   churnChart(churn, rng),
		Runners: runners,
	}, true
}

// Runner builds the runner detail view.
func (b *Builder) Runner(ctx context.Context, name string, sig Signals, now time.Time, events []fleet.Event) (RunnerDetail, bool) {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()

	r, ok := snap.Runner(name)
	if !ok {
		return RunnerDetail{}, false
	}
	set, _ := snap.Set(r.SetName)

	// Runner charts are always the short window: raw per-runner samples are
	// retained for minutes, so honouring a 30d range here would render a line
	// with five points at the far right and nothing else.
	win := Window{From: now.Add(-4 * time.Minute), To: now, Points: 48}
	raw, _ := b.History.Runner(ctx, name, win)

	page := b.page(name, snap, sig, now, []Crumb{
		{Label: snap.Org},
		{Label: "fleet", Href: "/"},
		{Label: r.SetName, Href: "/runnersets/" + r.SetName},
		{Label: name},
	})

	return RunnerDetail{
		Page:    page,
		Runner:  r,
		Set:     set,
		Facts:   runnerFacts(r, now),
		CPU:     runnerLine("CPU", raw.At, raw.CPU, r.CPU.Request, r.CPU.Limit, strokeCPU, fillCPU, fleet.FormatCores),
		Mem:     runnerLine("Memory", raw.At, raw.Mem, r.Mem.Request, r.Mem.Limit, strokeMem, fillMem, fleet.FormatGiB),
		Events:  events,
		SetJobs: setJobs(snap, r, now),
	}, true
}

func overviewTiles(t fleet.Totals) []Tile {
	queued := Tile{Label: "queued", Value: "—", Sub: "listener metrics off", Tone: ToneMuted}
	if t.QueuedKnown {
		queued = Tile{Label: "queued", Value: fmt.Sprint(t.Queued), Sub: "jobs waiting", Tone: ToneAttention}
		if t.Queued == 0 {
			queued.Sub, queued.Tone = "nothing waiting", ToneMuted
		}
	}

	cpu := Tile{Label: "cpu", Value: "—", Sub: "no samples", Tone: ToneMuted}
	if t.CPU.HasUsage() {
		cpu = Tile{
			Label: "cpu",
			Value: fleet.FormatCores(t.CPU.Used),
			Sub:   "of " + fleet.FormatCores(t.CPU.Request) + " requested",
			Tone:  ToneCPU,
			Bar:   t.CPU.Used, Total: t.CPU.Request,
		}
	}

	mem := Tile{Label: "memory", Value: "—", Sub: "no samples", Tone: ToneMuted}
	if t.Mem.HasUsage() {
		mem = Tile{
			Label: "memory",
			Value: fleet.FormatGiB(t.Mem.Used),
			Sub:   "of " + fleet.FormatGiB(t.Mem.Request) + " requested",
			Tone:  ToneMemory,
			Bar:   t.Mem.Used, Total: t.Mem.Request,
		}
	}

	return []Tile{
		{
			Label: "runners", Value: fmt.Sprint(t.Runners), Sub: CapacityLabel(t), Tone: ToneAccent,
			Bar: float64(t.Runners), Total: float64(t.Capacity),
		},
		{Label: "busy", Value: fmt.Sprint(t.Busy), Sub: fmt.Sprintf("%d idle · %d pending", t.Idle, t.Pending), Tone: ToneSuccess},
		queued,
		{
			Label: "utilization", Value: Pct(t.Utilization()), Sub: "busy of running", Tone: ToneSuccess,
			Bar: t.Utilization(), Total: 1,
		},
		cpu,
		mem,
	}
}

func setTiles(s fleet.RunnerSet, t fleet.Totals) []Tile {
	queued := Tile{Label: "queued", Value: "—", Sub: "listener metrics off", Tone: ToneMuted}
	if s.QueuedKnown {
		queued = Tile{Label: "queued", Value: fmt.Sprint(s.Queued), Sub: "jobs waiting", Tone: ToneAttention}
	}
	return []Tile{
		{
			Label: "runners", Value: fmt.Sprint(s.Current), Sub: "bounds " + Bounds(s), Tone: ToneAccent,
			Bar: float64(s.Current), Total: s.CapacityDenominator(),
		},
		{Label: "busy", Value: fmt.Sprint(t.Busy), Sub: fmt.Sprintf("%d idle · %d pending", t.Idle, t.Pending), Tone: ToneSuccess},
		queued,
		{Label: "failed", Value: fmt.Sprint(t.Failed), Sub: "in this set", Tone: failTone(t.Failed)},
	}
}

func failTone(n int) Tone {
	if n > 0 {
		return ToneDanger
	}
	return ToneMuted
}

func areaChart(s ScopeSeries, r TimeRange) AreaChart {
	if s.Empty() {
		return AreaChart{Empty: true, Ticks: r.Ticks(), Legend: stateLegend(s)}
	}

	peak := s.Peak()
	c := AreaChart{
		Width: chartW,
		Areas: chart.Stacked(s.Stacked(), []chart.Band{
			{Fill: paintBusy, Stroke: strokeBusy},
			{Fill: paintIdle, Stroke: strokeIdle},
			{Fill: paintPending, Stroke: strokePending},
		}, chartW, historyH, historyTop, peak),
		Grid:   chart.Grid(4, chartW, historyH, historyTop, peak, historyH, func(v float64) string { return fmt.Sprintf("%.0f", v) }),
		Ticks:  r.Ticks(),
		Legend: stateLegend(s),
	}

	if capacity, ok := s.CapacityLine(); ok {
		y := historyH - (capacity/peak)*(historyH-historyTop)
		c.Cap = fmt.Sprintf("0,%.1f %.1f,%.1f", y, chartW, y)
		c.CapLabel = fmt.Sprintf("max %.0f", capacity)
		c.CapTopPx = chart.LabelTopPx(y, historyH, historyH)
	}
	return c
}

func stateLegend(s ScopeSeries) []LegendItem {
	last := func(v []float64) string {
		if len(v) == 0 {
			return "—"
		}
		return fmt.Sprintf("%.0f", v[len(v)-1])
	}
	return []LegendItem{
		{Label: "busy", Tone: ToneSuccess, Value: last(s.Busy)},
		{Label: "idle", Tone: ToneAccent, Value: last(s.Idle)},
		{Label: "pending", Tone: ToneAttention, Value: last(s.Pending)},
	}
}

func utilChart(s ScopeSeries, r TimeRange) LineChart {
	if s.Empty() {
		return LineChart{Title: "utilization", Empty: true, Ticks: r.Ticks()}
	}
	util := s.Utilization()
	return LineChart{
		Width: chartW,
		Title: "utilization",
		Line:  chart.Plot(util, chartW, lineH, 1, fillUtil, strokeIdle),
		Grid:  chart.Grid(2, chartW, lineH, 6, 1, lineH, Pct),
		Ticks: r.Ticks(),
		Legend: []LegendItem{
			{Label: "busy of running", Tone: ToneAccent, Value: Pct(lastOf(util))},
		},
	}
}

func cpuChart(s ScopeSeries, r TimeRange) LineChart {
	return resourceChart("cpu", s.CPUUsed, s.CPURequest, r, strokeCPU, fillCPU, ToneCPU,
		fleet.FormatCores)
}

func memChart(s ScopeSeries, r TimeRange) LineChart {
	return resourceChart("memory", s.MemUsed, s.MemRequest, r, strokeMem, fillMem, ToneMemory,
		fleet.FormatGiB)
}

// resourceChart draws observed usage against the requested level. Both series
// share one maximum: if the request line were scaled independently, a fleet using
// a tenth of what it reserves would look fully utilised.
func resourceChart(title string, used, request []float64, r TimeRange, stroke, fill string, tone Tone, format func(float64) string) LineChart {
	if len(used) == 0 && len(request) == 0 {
		return LineChart{Title: title, Empty: true, Ticks: r.Ticks()}
	}

	peak := PeakOf(used, request)
	c := LineChart{
		Width: chartW,
		Title: title,
		Line:  chart.Plot(used, chartW, lineH, peak, fill, stroke),
		Grid:  chart.Grid(2, chartW, lineH, 6, peak, lineH, format),
		Ticks: r.Ticks(),
		Legend: []LegendItem{
			{Label: "used", Tone: tone, Value: format(lastOf(used))},
		},
	}

	// Any positive sample in the window earns the line, not just the newest
	// one: a fleet that has scaled to nothing since midnight still reserved
	// what it reserved, and that is the only thing left on the panel to read.
	if anyPositive(request) {
		c.Refs = append(c.Refs, RefLine{
			// The whole series rather than its newest value flattened across the window.
			// Requests move — sets get rescaled and the runner count they are multiplied by
			// changes every scrape — so a flat line backdates today's reservation over an hour
			// that never saw it.
			Points: chart.Plot(request, chartW, lineH, peak, "", strokeMuted).Points,
			Stroke: strokeMuted,
			Label:  "requested " + format(lastOf(request)),
		})
		c.Legend = append(c.Legend, LegendItem{
			Label: "requested", Tone: ToneMuted, Value: format(lastOf(request)),
		})
	}
	return c
}

func throughputChart(c Counts, r TimeRange) BarChart {
	if c.Len() == 0 {
		return BarChart{Empty: true, Ticks: r.Ticks()}
	}
	ok, failed := c.Sum()
	return BarChart{
		Width: chartW,
		Bars:  chart.Bars(c.Up, c.Down, c.Len(), chartW, barH, c.Peak(), barGap),
		Ticks: r.Ticks(),
		Legend: []LegendItem{
			{Label: "completed", Tone: ToneSuccess, Value: fmt.Sprintf("%.0f", ok)},
			{Label: "failed", Tone: ToneDanger, Value: fmt.Sprintf("%.0f", failed)},
		},
	}
}

func churnChart(c Counts, r TimeRange) ChurnChart {
	if c.Len() == 0 {
		return ChurnChart{Empty: true, Ticks: r.Ticks(), Centre: fmt.Sprintf("%.0f", churnCentre)}
	}
	created, destroyed := c.Sum()
	return ChurnChart{
		Width:  chartW,
		Bars:   chart.Diverging(c.Up, c.Down, c.Len(), chartW, churnH, churnCentre, c.Peak(), barGap),
		Centre: fmt.Sprintf("%.0f", churnCentre),
		Ticks:  r.Ticks(),
		Legend: []LegendItem{
			{Label: "created", Tone: ToneSuccess, Value: fmt.Sprintf("%.0f", created)},
			{Label: "destroyed", Tone: ToneMuted, Value: fmt.Sprintf("%.0f", destroyed)},
		},
	}
}

// runnerLine draws one raw per-runner series with its request and limit.
func runnerLine(title string, at []time.Time, vals []float64, request, limit float64, stroke, fill string, format func(float64) string) LineChart {
	if len(vals) == 0 {
		return LineChart{Title: title, Empty: true, Ticks: []string{"-4m", "-3m", "-2m", "-1m", "now"}}
	}

	peak := PeakOf(vals, []float64{request, limit})
	c := LineChart{
		Width: chartW,
		Title: title,
		Line:  chart.Plot(vals, chartW, lineH, peak, fill, stroke),
		Grid:  chart.Grid(2, chartW, lineH, 6, peak, lineH, format),
		Ticks: []string{"-4m", "-3m", "-2m", "-1m", "now"},
		Legend: []LegendItem{
			{Label: "used", Tone: ToneCPU, Value: format(lastOf(vals))},
		},
	}
	if request > 0 {
		c.Refs = append(c.Refs, RefLine{
			Points: chart.FlatLine(request, chartW, lineH, peak),
			Stroke: strokeMuted,
		})
	}
	if limit > 0 {
		c.Refs = append(c.Refs, RefLine{
			Points: chart.FlatLine(limit, chartW, lineH, peak),
			Stroke: strokeDanger,
			Label:  "limit " + format(limit),
		})
	}
	return c
}

// anyPositive reports whether a series ever rose above zero in the window.
//
// PeakOf cannot answer this: it floors its result at 1 so callers can divide by
// it, which makes an all-zero series indistinguishable from one that peaked at one.
func anyPositive(v []float64) bool {
	for _, x := range v {
		if x > 0 {
			return true
		}
	}
	return false
}

func lastOf(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1]
}

func setRows(groups []fleet.SetTotals) []SetRow {
	out := make([]SetRow, 0, len(groups))
	for _, g := range groups {
		denom := g.Set.CapacityDenominator()
		row := SetRow{
			Set:     g.Set,
			Totals:  g.Totals,
			Tone:    SetTone(g.Set),
			Badge:   SetBadge(g.Set),
			Bounds:  Bounds(g.Set),
			Busy:    chart.Percent(float64(g.Totals.Busy), denom),
			Idle:    chart.Percent(float64(g.Totals.Idle), denom),
			Pending: chart.Percent(float64(g.Totals.Pending), denom),
		}
		if g.Set.QueuedKnown {
			row.Queued = chart.Percent(float64(g.Set.Queued), denom)
		}
		out = append(out, row)
	}
	return out
}

// repoBars prefers historical consumption and falls back to what is running
// right now, because for the first minutes after a restart the store is empty
// and an operator staring at a saturated fleet still needs the answer.
func repoBars(history []RepoHistory, runners []fleet.Runner) []RepoBar {
	if len(history) > 0 {
		peak := lo.MaxBy(history, func(a, b RepoHistory) bool { return a.CPUSeconds > b.CPUSeconds })
		return lo.Map(history, func(h RepoHistory, _ int) RepoBar {
			return RepoBar{
				Repository: h.Repository,
				Value:      h.CPUSeconds,
				Max:        peak.CPUSeconds,
				Label:      fmt.Sprintf("%.0f core-s", h.CPUSeconds),
				Detail:     fmt.Sprintf("%d jobs", h.Jobs),
			}
		})
	}

	live := fleet.ByRepository(runners)
	peak := lo.MaxBy(live, func(a, b fleet.RepoUsage) bool { return a.Runners > b.Runners })
	return lo.Map(live, func(u fleet.RepoUsage, _ int) RepoBar {
		return RepoBar{
			Repository: u.Repository,
			Value:      float64(u.Runners),
			Max:        float64(peak.Runners),
			Label:      fmt.Sprintf("%d now", u.Runners),
			Detail:     fleet.FormatCores(u.CPUCores) + " cores",
		}
	})
}

func setConfig(s fleet.RunnerSet) []KV {
	return []KV{
		{Label: "namespace", Value: Dash(s.Namespace), Mono: true},
		{Label: "runner group", Value: Dash(s.RunnerGroup)},
		{Label: "bounds", Value: Bounds(s), Mono: true},
		{Label: "image", Value: Dash(s.Image), Mono: true},
		{Label: "labels", Value: fleet.FormatLabels(s.RunnerLabels), Mono: true},
		{Label: "cpu request", Value: fleet.FormatCores(s.CPURequest), Mono: true},
		{Label: "cpu limit", Value: limitOrNone(s.CPULimit, fleet.FormatCores), Mono: true},
		{Label: "memory request", Value: fleet.FormatGiB(s.MemRequest), Mono: true},
		{Label: "memory limit", Value: limitOrNone(s.MemLimit, fleet.FormatGiB), Mono: true},
		{Label: "scale set id", Value: Dash(s.ScaleSetID), Mono: true},
		{Label: "phase", Value: Dash(s.Phase)},
		{Label: "config url", Value: Dash(s.GitHubConfigURL), Mono: true},
	}
}

// limitOrNone distinguishes an unset limit from a zero one. A runner pod with
// no CPU limit is the common ARC configuration and must not read as "0".
func limitOrNone(v float64, format func(float64) string) string {
	if v <= 0 {
		return "none"
	}
	return format(v)
}

func runnerFacts(r fleet.Runner, now time.Time) []KV {
	facts := []KV{
		{Label: "state", Value: string(r.State), Tone: StateTone(r.State)},
		{Label: "runnerset", Value: Dash(r.SetName), Mono: true},
		{Label: "namespace", Value: Dash(r.Namespace), Mono: true},
		{Label: "node", Value: Dash(r.Node), Mono: true},
		{Label: "age", Value: fleet.FormatAge(r.Age(now)), Mono: true},
		{Label: "pod phase", Value: Dash(r.PodPhase)},
		{Label: "restarts", Value: fmt.Sprint(r.Restarts), Mono: true},
		{Label: "image", Value: Dash(r.Image), Mono: true},
	}
	if r.Job.Present() {
		facts = append(facts,
			KV{Label: "repository", Value: Dash(r.Job.Repository), Mono: true},
			KV{Label: "workflow", Value: Dash(r.Job.Workflow), Mono: true},
			KV{Label: "job", Value: Dash(r.Job.Name)},
			KV{Label: "run", Value: runLabel(r.Job.RunID), Mono: true},
			KV{Label: "running for", Value: fleet.FormatAge(r.JobAge(now)), Mono: true},
		)
	}
	if r.FailureReason != "" {
		facts = append(facts, KV{Label: "failure", Value: r.FailureReason, Tone: ToneDanger, Mono: true})
	}
	return facts
}

func runLabel(id int64) string {
	if id <= 0 {
		return "—"
	}
	return fmt.Sprintf("#%d", id)
}

// setJobs lists what every runner in this runner's set is working on.
func setJobs(s fleet.Snapshot, current fleet.Runner, now time.Time) []JobRow {
	out := make([]JobRow, 0, 8)
	for _, r := range s.Runners {
		if r.SetName != current.SetName || !r.Job.Present() {
			continue
		}
		out = append(out, JobRow{
			Runner:     r.Name,
			Repository: r.Job.Repository,
			Workflow:   r.Job.Workflow,
			Job:        r.Job.Name,
			State:      r.State,
			Age:        fleet.FormatAge(r.JobAge(now)),
			Current:    r.Name == current.Name,
		})
	}
	return out
}

func sourceRows(s fleet.Snapshot, interval time.Duration) []SourceRow {
	strip := []struct{ name, label string }{
		{fleet.SourceKubernetes, "kubernetes"},
		{fleet.SourceARCCRDs, "arc crds"},
		{fleet.SourceMetrics, "metrics-server"},
		{fleet.SourceListener, "listener metrics"},
		{fleet.SourceStore, "history store"},
	}

	rows := make([]SourceRow, 0, len(strip)+3)
	for _, want := range strip {
		src, ok := s.Source(want.name)
		if !ok {
			rows = append(rows, SourceRow{Label: want.label, Value: "not checked", Tone: ToneMuted})
			continue
		}
		rows = append(rows, SourceRow{
			Label: want.label,
			Value: SourceValue(src, "ok"),
			Tone:  SourceTone(src),
		})
	}

	rows = append(rows,
		SourceRow{Label: "scrape interval", Value: interval.String(), Tone: ToneMuted},
		SourceRow{Label: "listeners", Value: listenerValue(s), Tone: listenerTone(s)},
	)
	if s.ControllerVersion != "" {
		rows = append(rows, SourceRow{Label: "controller", Value: s.ControllerVersion, Tone: ToneMuted})
	}
	return rows
}

func listenerValue(s fleet.Snapshot) string {
	if s.ListenersTotal == 0 {
		return "none"
	}
	return fmt.Sprintf("%d/%d ready", s.ListenersReady, s.ListenersTotal)
}

func listenerTone(s fleet.Snapshot) Tone {
	switch {
	case s.ListenersTotal == 0:
		return ToneMuted
	case s.ListenersReady < s.ListenersTotal:
		return ToneDanger
	}
	return ToneSuccess
}

// failureLaneRows is how many failures the lane lists. The rest of the window
// is reported as a count, because the panel sits in a column beside two others
// and an unbounded list would push them off the page.
const failureLaneRows = 6

// mergeFailures combines the persisted page with any live failure the store has not
// caught up with.
//
// Both sources are needed. The store is authoritative for the window — it is the
// only one that remembers runners ARC has deleted — but the recorder writes from
// the same snapshot the page renders from, so a failure can be on screen a tick
// before it is in the database, and a store that has stopped accepting writes is
// exactly when the lane matters most.
//
// A live failure absent from the page is treated as unpersisted and counted into
// the total: it cannot have been pushed off by newer rows, because live failures
// are the newest thing there is.
func mergeFailures(stored FailureWindow, live []fleet.Failure, limit int, window string) FailureLane {
	seen := make(map[fleet.Failure]struct{}, len(stored.Failures))
	key := func(f fleet.Failure) fleet.Failure {
		// The stored row carries the first observation's timestamp and the live one
		// carries the current reading, so the timestamp cannot be part of the identity.
		// Runner and reason are what the store is keyed on too.
		return fleet.Failure{Runner: f.Runner, Reason: f.Reason}
	}

	items := make([]fleet.Failure, 0, len(stored.Failures)+len(live))
	for _, f := range stored.Failures {
		seen[key(f)] = struct{}{}
		items = append(items, f)
	}

	total := stored.Total
	for _, f := range live {
		if _, dup := seen[key(f)]; dup {
			continue
		}
		seen[key(f)] = struct{}{}
		items = append(items, f)
		total++
	}

	fleet.SortFailures(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}

	return FailureLane{Items: items, More: max(0, total-len(items)), Window: window}
}

// storePanel describes the history store's own footprint.
//
// Every count is a row the retention sweep is responsible for eventually
// deleting, so the panel doubles as the readout for whether retention is
// keeping up: rows climbing while the oldest sample stays put is the shape of a
// compactor that has stopped running.
func storePanel(s StoreStats, now time.Time) StorePanel {
	if !s.Enabled {
		return StorePanel{}
	}

	oldest := "—"
	if !s.Oldest.IsZero() {
		oldest = fleet.FormatRelative(s.Oldest, now)
	}

	return StorePanel{
		Enabled: true,
		Rows: []KV{
			{Label: "on disk", Value: fleet.FormatBytes(s.SizeBytes), Mono: true},
			{Label: "rows", Value: Thousands(s.Rows), Mono: true},
			{Label: "samples", Value: Thousands(s.Samples), Mono: true},
			{Label: "jobs", Value: Thousands(s.Jobs), Mono: true},
			{Label: "churn events", Value: Thousands(s.ChurnEvents), Mono: true},
			{Label: "failures", Value: Thousands(s.Failures), Mono: true},
			{Label: "oldest sample", Value: oldest, Mono: true},
			{Label: "file", Value: Dash(s.Path), Mono: true},
		},
	}
}

// warnings surfaces conditions that make the numbers on screen less than they
// appear. Each one explains why a panel might be empty or understated, which
// is the difference between a dashboard an operator trusts and one they learn
// to ignore.
func warnings(s fleet.Snapshot, t fleet.Totals) []string {
	var out []string

	if !t.MetricsComplete() && t.Runners > 0 {
		out = append(out, fmt.Sprintf(
			"resource usage covers %d of %d runners — the rest have not been scraped yet",
			t.MetricsCovered, t.Runners))
	}
	if !t.QueuedKnown && len(s.Sets) > 0 {
		out = append(out, "queue depth is unavailable: ARC listener metrics are disabled")
	}
	// Any source carrying a reason is worth a line, available or not: a partial
	// answer is a source that is working for some of the fleet and silent for
	// the rest, and silence is what the warnings exist to break.
	for _, src := range s.Sources {
		if src.Reason != "" {
			out = append(out, src.Name+": "+src.Reason)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Workflows and Jobs
// ---------------------------------------------------------------------------

// historyRows is how many rows the two history tables render. The window's
// true total is reported beside them, so the cap bounds rendering cost without
// hiding how much work the fleet actually did.
const historyRows = 100

// JobsView is the Jobs tab.
type JobsView struct {
	Page

	Selects []fleet.Select
	Search  string
	Rows    []JobListRow
	Total   int64
	Summary string

	// Enabled is false when there is no history store. These two tabs are
	// entirely store-backed, so that is the difference between "nothing ran"
	// and "nothing is being recorded" — and rendering an empty table for the
	// second would be the dashboard lying about an idle fleet.
	Enabled bool
}

// WorkflowsView is the Workflows tab.
type WorkflowsView struct {
	Page

	Selects []fleet.Select
	Search  string
	Rows    []WorkflowListRow
	Total   int64
	Summary string
	Enabled bool
}

// JobView is the per-job detail page.
type JobView struct {
	Page

	Job   Job
	Tiles []Tile
	Facts []KV
	CPU   LineChart
	Mem   LineChart

	// Buckets is how many usage samples the chart was drawn from. Zero means
	// none were recorded, which the panel explains rather than drawing a flat
	// line at zero.
	Buckets int
}

// JobListRow is one line of the Jobs table, with its display values resolved.
type JobListRow struct {
	Job      Job
	Tone     Tone
	Badge    string
	Duration string
	Started  string
	CPU      string
	Mem      string
	Href     string
}

// WorkflowListRow is one line of the Workflows table.
type WorkflowListRow struct {
	Run      WorkflowRun
	Tone     Tone
	Badge    string
	Duration string
	Started  string
	CPU      string
	Mem      string
	// Href opens the Jobs tab narrowed to this run.
	Href string
}

// Jobs builds the Jobs tab.
func (b *Builder) Jobs(ctx context.Context, sig Signals, now time.Time) JobsView {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()
	rng := ParseRange(sig.Range)
	win := rng.Window(now)

	filter := sig.JobFilter()
	filter.Limit = historyRows

	page := b.page("Jobs", snap, sig, now, []Crumb{{Label: snap.Org}, {Label: "jobs"}})
	page.Tab = TabJobs

	stats, _ := b.History.Stats(ctx)
	view := JobsView{
		Page:    page,
		Search:  sig.Q,
		Enabled: stats.Enabled,
		Selects: jobSelects(ctx, b.History, sig, win, true),
	}
	if !view.Enabled {
		return view
	}

	jobs, err := b.History.Jobs(ctx, filter, win)
	if err != nil {
		return view
	}

	view.Total = jobs.Total
	view.Rows = make([]JobListRow, 0, len(jobs.Jobs))
	for _, j := range jobs.Jobs {
		view.Rows = append(view.Rows, jobListRow(j, now))
	}
	view.Summary = listSummary(len(view.Rows), jobs.Total, "job", rng)
	return view
}

// Workflows builds the Workflows tab.
func (b *Builder) Workflows(ctx context.Context, sig Signals, now time.Time) WorkflowsView {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()
	rng := ParseRange(sig.Range)
	win := rng.Window(now)

	filter := sig.JobFilter()
	filter.Limit = historyRows

	page := b.page("Workflows", snap, sig, now, []Crumb{{Label: snap.Org}, {Label: "workflows"}})
	page.Tab = TabWorkflows

	stats, _ := b.History.Stats(ctx)
	view := WorkflowsView{
		Page:    page,
		Search:  sig.Q,
		Enabled: stats.Enabled,
		Selects: jobSelects(ctx, b.History, sig, win, false),
	}
	if !view.Enabled {
		return view
	}

	runs, err := b.History.Workflows(ctx, filter, win)
	if err != nil {
		return view
	}

	view.Total = runs.Total
	view.Rows = make([]WorkflowListRow, 0, len(runs.Runs))
	for _, r := range runs.Runs {
		view.Rows = append(view.Rows, workflowListRow(r, now))
	}
	view.Summary = listSummary(len(view.Rows), runs.Total, "workflow run", rng)
	return view
}

// Job builds the per-job detail view. It reports false when no such job is
// recorded, which the handler turns into a 404.
func (b *Builder) Job(ctx context.Context, id int, sig Signals, now time.Time) (JobView, bool) {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()

	j, ok, err := b.History.Job(ctx, id)
	if err != nil || !ok {
		return JobView{}, false
	}

	page := b.page(j.Name, snap, sig, now, []Crumb{
		{Label: snap.Org},
		{Label: "jobs", Href: "/jobs"},
		{Label: jobCrumb(j)},
	})
	page.Tab = TabJobs

	// The window is the job's own life, not the range picker's: a job is a
	// bounded thing and its chart should show all of it, whatever window the
	// operator was looking at when they clicked through. Points is capped the
	// same way every other chart is, so a six-hour job renders as a readable
	// line rather than three thousand coordinates.
	end := j.FinishedAt
	if end.IsZero() {
		end = now
	}
	win := Window{From: j.StartedAt, To: end.Add(time.Second), Points: 90}
	series, _ := b.History.JobSeries(ctx, id, win)

	view := JobView{
		Page:    page,
		Job:     j,
		Tiles:   jobTiles(j, now),
		Facts:   jobFacts(j, now),
		Buckets: series.Len(),
	}

	ticks := jobTicks(j.Duration(now))
	view.CPU = jobLine("cpu", series.CPU, ticks,
		j.CPURequest, j.CPULimit, strokeCPU, fillCPU, ToneCPU, fleet.FormatCores)
	view.Mem = jobLine("memory", series.Mem, ticks,
		j.MemRequest, j.MemLimit, strokeMem, fillMem, ToneMemory, fleet.FormatGiB)
	return view, true
}

// jobSelects builds the filter dropdowns from what the window actually
// contains, so a dimension never offers a value that would match nothing.
//
// withOutcome is false on the Workflows tab: a run is a set of jobs that can
// have ended several different ways at once, and an "ok/failed" dropdown over
// it would claim a precision the row does not have.
func jobSelects(ctx context.Context, h History, sig Signals, win Window, withOutcome bool) []fleet.Select {
	facets, _ := h.Facets(ctx, win)

	out := []fleet.Select{
		jobSelect("repo", "repo", sig.Repo, facets.Repositories, "all repositories"),
		jobSelect("workflow", "workflow", sig.Workflow, facets.Workflows, "all workflows"),
		jobSelect("set", "runnerset", sig.Set, facets.Sets, "all runnersets"),
	}
	if withOutcome {
		outcomes := make([]string, 0, len(AllOutcomes()))
		for _, o := range AllOutcomes() {
			outcomes = append(outcomes, string(o))
		}
		out = append(out, jobSelect("outcome", "outcome", sig.Outcome, outcomes, "any outcome"))
	}
	return out
}

// jobSelect builds one dropdown. It mirrors fleet.Filter.Selects so the two
// filter bars are the same control with the same semantics, including keeping
// a selected value that the window no longer contains rather than silently
// jumping to a different filter than the URL says.
func jobSelect(key, label, current string, values []string, anyLabel string) fleet.Select {
	filtering := current != "" && current != fleet.AnyValue

	options := make([]fleet.Option, 0, len(values)+2)
	options = append(options, fleet.Option{
		Value: fleet.AnyValue, Label: anyLabel, Selected: !filtering,
	})

	found := false
	for _, v := range values {
		if v == current {
			found = true
		}
		options = append(options, fleet.Option{Value: v, Label: v, Selected: v == current})
	}
	if filtering && !found {
		options = append(options, fleet.Option{
			Value: current, Label: current + " (no matches)", Selected: true,
		})
	}

	return fleet.Select{
		Key: key, Label: label, Value: current, Options: options, Filtering: filtering,
	}
}

// listSummary is the line at the right of a history filter bar.
func listSummary(shown int, total int64, noun string, r TimeRange) string {
	plural := noun + "s"
	if total == 1 {
		plural = noun
	}
	if int64(shown) < total {
		return fmt.Sprintf("%d of %s %s · %s", shown, Thousands(total), plural, r.Label())
	}
	return fmt.Sprintf("%s %s · %s", Thousands(total), plural, r.Label())
}

// jobListRow resolves one job's display values.
func jobListRow(j Job, now time.Time) JobListRow {
	return JobListRow{
		Job:      j,
		Tone:     JobTone(j),
		Badge:    JobBadge(j),
		Duration: fleet.FormatAge(j.Duration(now)),
		Started:  fleet.FormatRelative(j.StartedAt, now),
		CPU:      CoreSeconds(j.CPUSeconds),
		Mem:      GiBSeconds(j.MemGiBSecs),
		Href:     fmt.Sprintf("/jobs/%d", j.ID),
	}
}

// workflowListRow resolves one run's display values.
func workflowListRow(w WorkflowRun, now time.Time) WorkflowListRow {
	row := WorkflowListRow{
		Run:      w,
		Tone:     RunTone(w),
		Badge:    RunBadge(w),
		Duration: fleet.FormatAge(w.Duration(now)),
		Started:  fleet.FormatRelative(w.StartedAt, now),
		CPU:      CoreSeconds(w.CPUSeconds),
		Mem:      GiBSeconds(w.MemGiBSecs),
	}

	// A run ARC reported no id for cannot be addressed as a run, so the row
	// links to its workflow's jobs instead of to a filter that would match
	// every such job in the window.
	q := url.Values{}
	q.Set("repo", w.Repository)
	q.Set("workflow", w.Workflow)
	if w.RunID > 0 {
		q.Set("run", strconv.FormatInt(w.RunID, 10))
	}
	row.Href = "/jobs?" + q.Encode()
	return row
}

// JobTone colours a job by how it ended.
func JobTone(j Job) Tone {
	switch {
	case j.Running():
		return ToneAccent
	case j.Succeeded:
		return ToneSuccess
	}
	return ToneDanger
}

// JobBadge labels a job by how it ended.
func JobBadge(j Job) string {
	switch {
	case j.Running():
		return "running"
	case j.Succeeded:
		return "ok"
	}
	return "failed"
}

// RunTone colours a workflow run. A run with any failure is a failed run: the
// green of "some of it worked" is the one thing nobody needs to know.
func RunTone(w WorkflowRun) Tone {
	switch {
	case w.Failed > 0:
		return ToneDanger
	case w.Running > 0:
		return ToneAccent
	}
	return ToneSuccess
}

// RunBadge labels a workflow run.
func RunBadge(w WorkflowRun) string {
	switch {
	case w.Failed > 0:
		return fmt.Sprintf("%d failed", w.Failed)
	case w.Running > 0:
		return "running"
	}
	return "ok"
}

// RunLabel renders a workflow run number, spelling out the case where ARC
// reported none rather than printing "#0".
func RunLabel(id int64) string {
	if id <= 0 {
		return "no run id"
	}
	return fmt.Sprintf("#%d", id)
}

// CoreSeconds renders integrated CPU cost, promoting to core-hours once
// core-seconds stops being a number anyone can read at a glance.
func CoreSeconds(v float64) string {
	if v <= 0 {
		return "—"
	}
	if v < 3600 {
		return fmt.Sprintf("%.0f core-s", v)
	}
	return fmt.Sprintf("%.1f core-h", v/3600)
}

// GiBSeconds renders integrated memory cost on the same scale.
func GiBSeconds(v float64) string {
	if v <= 0 {
		return "—"
	}
	if v < 3600 {
		return fmt.Sprintf("%.0f GiB-s", v)
	}
	return fmt.Sprintf("%.1f GiB-h", v/3600)
}

// jobCrumb is the breadcrumb label for one job.
func jobCrumb(j Job) string {
	if j.Workflow == "" {
		return Dash(j.Name)
	}
	return j.Workflow + " · " + Dash(j.Name)
}

// jobTiles is the KPI strip on the job detail page.
//
// The averages are derived from the integrated cost rather than from the
// samples, because they are the only figures that survive for a job whose
// samples have been swept: cost is a column on the job row, the series is not.
func jobTiles(j Job, now time.Time) []Tile {
	d := j.Duration(now)
	secs := d.Seconds()

	cpu := Tile{Label: "avg cpu", Value: "—", Sub: "no cost recorded", Tone: ToneMuted}
	if secs > 0 && j.CPUSeconds > 0 {
		cpu = Tile{
			Label: "avg cpu", Value: fleet.FormatCores(j.CPUSeconds / secs),
			Sub: "cores over " + fleet.FormatAge(d), Tone: ToneCPU,
		}
	}

	mem := Tile{Label: "avg memory", Value: "—", Sub: "no cost recorded", Tone: ToneMuted}
	if secs > 0 && j.MemGiBSecs > 0 {
		mem = Tile{
			Label: "avg memory", Value: fleet.FormatGiB(j.MemGiBSecs / secs * fleet.GiB),
			Sub: "over " + fleet.FormatAge(d), Tone: ToneMemory,
		}
	}

	return []Tile{
		{Label: "outcome", Value: JobBadge(j), Sub: Dash(j.Repository), Tone: JobTone(j)},
		{Label: "duration", Value: fleet.FormatAge(d), Sub: "since " + fleet.FormatRelative(j.StartedAt, now), Tone: ToneAccent},
		cpu,
		mem,
		{Label: "cpu cost", Value: CoreSeconds(j.CPUSeconds), Sub: "integrated", Tone: ToneCPU},
		{Label: "memory cost", Value: GiBSeconds(j.MemGiBSecs), Sub: "integrated", Tone: ToneMemory},
	}
}

// jobFacts is the detail page's label/value panel.
func jobFacts(j Job, now time.Time) []KV {
	facts := []KV{
		{Label: "repository", Value: Dash(j.Repository), Mono: true},
		{Label: "workflow", Value: Dash(j.Workflow), Mono: true},
		{Label: "job", Value: Dash(j.Name)},
		{Label: "run", Value: RunLabel(j.RunID), Mono: true},
		{Label: "runner", Value: Dash(j.Runner), Mono: true},
		{Label: "runnerset", Value: Dash(j.Set), Mono: true},
		{Label: "started", Value: fleet.FormatRelative(j.StartedAt, now), Mono: true},
	}
	if j.Running() {
		return append(facts, KV{Label: "finished", Value: "still running", Tone: ToneAccent})
	}
	return append(facts,
		KV{Label: "finished", Value: fleet.FormatRelative(j.FinishedAt, now), Mono: true},
		KV{Label: "outcome", Value: JobBadge(j), Tone: JobTone(j)},
	)
}

// jobTicks labels a job chart's x-axis in elapsed time rather than in clock
// time, because a job is read as "what happened during it", not "what the
// cluster was doing at 14:05".
func jobTicks(d time.Duration) []string {
	const n = 5
	out := make([]string, 0, n)
	for i := range n {
		at := time.Duration(float64(d) * float64(i) / float64(n-1))
		if i == 0 {
			out = append(out, "start")
			continue
		}
		out = append(out, fleet.FormatAge(at))
	}
	return out
}

// jobLine draws one of the job's two usage series against what its runner
// reserved.
//
// The reference lines come off the job row rather than the live pod, which no
// longer exists for a job that finished last week. A zero is not drawn: it
// means the reservation was never observed, and an undrawn line is honest
// where a line at zero would not be.
//
// The numbers go in the legend rather than beside the rules: LineChartView
// draws a RefLine as a dashed polyline and nothing else, so an unlabelled one
// is an anonymous line across the chart.
func jobLine(
	title string, vals []float64, ticks []string,
	request, limit float64, stroke, fill string, tone Tone, format func(float64) string,
) LineChart {
	if len(vals) == 0 {
		return LineChart{Title: title, Empty: true, Ticks: ticks}
	}
	peak := PeakOf(vals, []float64{request, limit})
	c := LineChart{
		Width: chartW,
		Title: title,
		Line:  chart.Plot(vals, chartW, lineH, peak, fill, stroke),
		Grid:  chart.Grid(2, chartW, lineH, 6, peak, lineH, format),
		Ticks: ticks,
		Legend: []LegendItem{
			{Label: "peak", Tone: tone, Value: format(PeakOf(vals))},
			{Label: "last", Tone: tone, Value: format(lastOf(vals))},
		},
	}
	if request > 0 {
		c.Refs = append(c.Refs, RefLine{
			Points: chart.FlatLine(request, chartW, lineH, peak),
			Stroke: strokeMuted,
			Label:  "request " + format(request),
		})
		c.Legend = append(c.Legend, LegendItem{Label: "request", Tone: ToneMuted, Value: format(request)})
	}
	if limit > 0 {
		c.Refs = append(c.Refs, RefLine{
			Points: chart.FlatLine(limit, chartW, lineH, peak),
			Stroke: strokeDanger,
		})
		c.Legend = append(c.Legend, LegendItem{Label: "limit", Tone: ToneDanger, Value: format(limit)})
	}
	return c
}
