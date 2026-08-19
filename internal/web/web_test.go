package web

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-h/templ"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"arc-ui/internal/chart"
	"arc-ui/internal/fleet"
	"arc-ui/internal/hub"
)

var now = time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

func sampleSnapshot() fleet.Snapshot {
	return fleet.Snapshot{
		At:  now,
		Org: "WindKube",
		Sets: []fleet.RunnerSet{
			{
				Name: "arc-ubuntu", Namespace: "arc-runners",
				MinRunners: 2, MaxRunners: 30,
				Image:      "ghcr.io/actions/runner:2.320.0",
				Current:    3,
				CPURequest: 2, MemRequest: 4 * fleet.GiB,
			},
			{
				Name: "arc-arm64", Namespace: "arc-runners",
				MinRunners: 0, Unbounded: true,
				Current:    1,
				CPURequest: 4, MemRequest: 8 * fleet.GiB,
			},
		},
		Runners: []fleet.Runner{
			{
				Name: "arc-ubuntu-abc", Namespace: "arc-runners", SetName: "arc-ubuntu",
				State: fleet.StateBusy, Node: "ip-10-0-1-5", PodUID: "uid-1",
				CreatedAt: now.Add(-20 * time.Minute),
				Job: fleet.Job{
					Repository: "WindKube/web-api", Workflow: "ci.yml", Name: "unit-tests",
					RunID: 4471, StartedAt: now.Add(-6 * time.Minute),
				},
				CPU: fleet.Resources{Used: 1.4, Request: 2, At: now},
				Mem: fleet.Resources{Used: 3 * fleet.GiB, Request: 4 * fleet.GiB, At: now},
			},
			{
				// Never scraped: usage must render as an em dash, not zero.
				Name: "arc-ubuntu-def", Namespace: "arc-runners", SetName: "arc-ubuntu",
				State: fleet.StateIdle, CreatedAt: now.Add(-3 * time.Minute),
				CPU: fleet.Resources{Request: 2},
				Mem: fleet.Resources{Request: 4 * fleet.GiB},
			},
			{
				Name: "arc-arm64-xyz", Namespace: "arc-runners", SetName: "arc-arm64",
				State: fleet.StateFailed, CreatedAt: now.Add(-time.Minute),
				FailureReason: "ImagePullBackOff", FailedAt: now.Add(-30 * time.Second),
			},
		},
		Sources: []fleet.Source{
			{Name: fleet.SourceKubernetes, Available: true, CheckedAt: now},
			{Name: fleet.SourceARCCRDs, Available: true, CheckedAt: now},
			{Name: fleet.SourceMetrics, Available: false, Reason: "metrics-server not installed", CheckedAt: now},
		},
	}
}

func testBuilder() *Builder {
	return &Builder{
		Fleet:    SnapshotFunc(sampleSnapshot),
		History:  NoHistory{},
		Version:  "test",
		Interval: 15 * time.Second,
		CSS:      "/static/app.css",
		JS:       "/static/datastar.js",
	}
}

// renderOverview renders the unfiltered fleet overview as a complete document.
func renderOverview(t *testing.T) string {
	t.Helper()
	return renderOverviewWith(t, testBuilder())
}

// renderOverviewWith renders the overview from a caller-supplied builder, for
// the tests that need a history other than NoHistory.
func renderOverviewWith(t *testing.T, b *Builder) string {
	t.Helper()

	o := b.Overview(context.Background(), Signals{}, now)
	o.Stream = "/stream"

	var sb strings.Builder
	require.NoError(t, Document(o.Page, OverviewPage(o)).Render(context.Background(), &sb), "render")
	return sb.String()
}

// statsHistory is NoHistory with a store-statistics answer, so the footer can
// be rendered without a database anywhere in sight.
type statsHistory struct {
	NoHistory
	stats StoreStats
}

func (h statsHistory) Stats(context.Context) (StoreStats, error) { return h.stats, nil }

func TestOverviewRendersEveryPatchTarget(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	// Every id the stream patches must exist in the initial document, or the
	// first push lands nowhere and that panel silently stops updating.
	for _, id := range []string{
		"filterbar", "tiles", "history", "utilization", "resources",
		"throughput", "churn", "runnersets", "runners", "repos",
		"failures", "store", "health", "live-indicator",
	} {
		assert.Contains(t, html, `id="`+id+`"`, "missing patch target id=%q", id)
	}
}

func TestUnscrapedRunnerRendersDashNotZero(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	// A runner metrics-server has never seen must not claim to use 0.0 cores;
	// that understates the fleet and looks like a healthy idle runner.
	assert.NotContains(t, html, ">0.0<", "an unscraped runner rendered as 0.0 rather than an em dash")
	assert.Contains(t, html, "—", "expected an em dash for the unscraped runner")
}

func TestUnboundedSetRendersInfinityNotMaxInt(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	assert.NotContains(t, html, "2147483647", "the controller's internal MaxInt32 leaked into the page")
	assert.Contains(t, html, "0–∞", "an unbounded set should render its ceiling as ∞")
}

func TestDegradedSourceIsNamedNotHidden(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	// The whole point of the health strip: an empty resource chart must be
	// explained, not left looking like an idle fleet.
	assert.Contains(t, html, "metrics-server not installed", "an unavailable source must be named in the health strip")
}

func TestEmptyHistoryRendersAnExplanation(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	assert.Contains(t, html, "no history yet", "an empty history chart should say why it is empty")
	assert.NotContains(t, html, "<polygon points=\"\"", "an empty chart emitted a degenerate polygon")
}

// A grid rule has to span the width its geometry was computed for. The rules
// live inside the chart's SVG, so a template that ends them at a width of its
// own draws rules that stop short of — or run past — the plot the labels
// belong to the moment a chart is built on any other viewBox.
func TestGridLinesSpanTheWidthTheyWereBuiltFor(t *testing.T) {
	t.Parallel()

	lines := chart.Grid(2, 400, lineH, 6, 10, lineH, func(float64) string { return "" })

	var sb strings.Builder
	require.NoError(t, gridLines(lines).Render(context.Background(), &sb), "render")

	assert.Contains(t, sb.String(), `x2="400.0"`,
		"grid rules should span the 400-unit width they were built for, got %s", sb.String())
}

// A capacity bar's four segments are each computed against the same
// denominator but independently of one another, and are never normalised
// against each other, so they can sum past 100% — the fixture below reaches
// 191%. A flex child is shrinkable by default, and a browser resolving an
// overfull flex line rescales *all* of them proportionally — at which point no
// segment is its true fraction of the ceiling any more and the bar lies exactly
// when the fleet is under pressure.
func TestCapacityBarSegmentsDoNotRescaleWhenOverfull(t *testing.T) {
	t.Parallel()

	rows := setRows([]fleet.SetTotals{{
		Set: fleet.RunnerSet{
			Name: "arc-ubuntu-small", MinRunners: 2, MaxRunners: 12, Current: 12,
			Queued: 11, QueuedKnown: true,
		},
		Totals: fleet.Totals{Busy: 9, Idle: 2, Pending: 1},
	}})
	require.Len(t, rows, 1, "want one row")
	row := rows[0]
	// 75 + 16.7 + 8.3 + 91.7 = 191.7% of the ceiling.
	require.Equal(t, []string{"75.0", "16.7", "8.3", "91.7"},
		[]string{row.Busy, row.Idle, row.Pending, row.Queued}, "fixture is not overfull")

	var sb strings.Builder
	require.NoError(t, CapacityBar(row).Render(context.Background(), &sb), "render")

	segments := 0
	for _, div := range strings.Split(sb.String(), "<div ")[1:] {
		if !strings.Contains(div, `style="width:`) {
			continue
		}
		segments++
		assert.Contains(t, div, "shrink-0",
			"a width-carrying segment can still be shrunk, so an overfull bar rescales every segment: %s", div)
	}
	// Without this the test passes by inspecting nothing at all — move the
	// widths off the style attribute and every assertion above stops running.
	assert.Equal(t, 4, segments, "want busy, idle, pending and queued inspected, got %d segments carrying a width", segments)
}

// A chart's viewBox is the coordinate system its geometry was computed in. The
// geometry functions are handed a width; a template that supplies its own
// instead agrees with them only by coincidence, and every x coordinate on the
// chart is misread the moment the two differ.
func TestChartViewBoxFollowsTheGeometryWidth(t *testing.T) {
	t.Parallel()

	const w = 400.0
	grid := chart.Grid(2, w, lineH, 6, 10, lineH, func(float64) string { return "" })

	for _, tc := range []struct {
		name string
		view templ.Component
		want string
	}{
		{"area", AreaChartView(AreaChart{Width: w, Grid: grid}), `viewBox="0 0 400 200"`},
		{"line", LineChartView(LineChart{Width: w, Grid: grid}), `viewBox="0 0 400 120"`},
		{"bar", BarChartView(BarChart{
			Width: w,
			Bars:  chart.Bars([]float64{3}, []float64{1}, 1, w, barH, 4, barGap),
		}), `viewBox="0 0 400 120"`},
		{"churn", ChurnChartView(ChurnChart{
			Width:  w,
			Centre: "70.0",
			Bars:   chart.Diverging([]float64{3}, []float64{1}, 1, w, churnH, churnCentre, 4, barGap),
		}), `viewBox="0 0 400 140"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var sb strings.Builder
			require.NoError(t, tc.view.Render(context.Background(), &sb), "render")
			assert.Contains(t, sb.String(), tc.want,
				"chart geometry built %.0f units wide should be drawn in a %.0f-unit viewBox", w, w)
		})
	}
}

// The area and line charts carry their y-axis labels in the HTML layer, at
// pixel offsets the builder computes by handing chart.Grid the height constant
// as the rendered height. That is only the right answer while the SVG really is
// that many CSS pixels tall, so the constant and the template's height class
// have to be checked against each other.
func TestLabelledChartsRenderAtTheHeightTheirLabelsAssume(t *testing.T) {
	t.Parallel()

	grid := chart.Grid(2, 400, lineH, 6, 10, lineH, func(float64) string { return "" })

	for _, tc := range []struct {
		name   string
		view   templ.Component
		height float64
	}{
		{"area", AreaChartView(AreaChart{Width: 400, Grid: grid}), historyH},
		{"line", LineChartView(LineChart{Width: 400, Grid: grid}), lineH},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var sb strings.Builder
			require.NoError(t, tc.view.Render(context.Background(), &sb), "render")

			// Assembled rather than written out: a literal Tailwind class in a
			// scanned .go file is emitted as a real rule into app.css.
			want := "h-" + fmt.Sprintf("[%.0fpx]", tc.height)
			assert.Contains(t, sb.String(), want,
				"grid labels are positioned as if the SVG were %.0f pixels tall, so it has to be", tc.height)
		})
	}
}

// The churn chart's mirror line is part of that chart's geometry: the bars are
// laid out across the width the builder chose, so the line they are mirrored
// about has to reach the same place.
func TestChurnCentreLineSpansTheGeometryWidth(t *testing.T) {
	t.Parallel()

	var sb strings.Builder
	require.NoError(t, ChurnChartView(ChurnChart{
		Width:  400,
		Centre: "70.0",
		Bars:   chart.Diverging([]float64{3}, []float64{1}, 1, 400, churnH, churnCentre, 4, barGap),
	}).Render(context.Background(), &sb), "render")

	assert.Contains(t, sb.String(), `x2="400"`,
		"the centre line should end where the bars do, got %s", sb.String())
}

func TestFilterNarrowsTheRenderedFleet(t *testing.T) {
	t.Parallel()

	b := testBuilder()
	all := b.Overview(context.Background(), Signals{}, now)
	require.Len(t, all.Runners, 3, "fixture changed")

	one := b.Overview(context.Background(), Signals{Set: "arc-arm64"}, now)
	assert.Len(t, one.Runners, 1, "want 1 runner for the arm64 set")
	assert.Contains(t, one.Summary, "1 of 3 runners", "summary should report the narrowing")
}

func TestSignalsSurviveAQueryRoundTrip(t *testing.T) {
	t.Parallel()

	in := Signals{Repo: "WindKube/web-api", State: "busy", Range: "6h", Sort: "name"}.Normalize()
	out := SignalsFromQuery(in.Query())

	assert.Equal(t, in, out, "round trip changed the signals")
}

func TestQueryOmitsDefaults(t *testing.T) {
	t.Parallel()

	// A shared link for the unfiltered default view should be a bare "/".
	assert.Empty(t, (Signals{}).Normalize().Query().Encode(), "defaults leaked into the URL")
}

func TestRunnerDetailUsesAFixedShortWindow(t *testing.T) {
	t.Parallel()

	// The runner charts must ignore the page range: raw per-runner samples are
	// retained for minutes, so honouring "30d" would render four points at the
	// far right of an empty chart.
	var got Window
	b := testBuilder()
	b.History = windowSpy{&got}

	_, ok := b.Runner(context.Background(), "arc-ubuntu-abc", Signals{Range: "30d"}, now, nil)
	require.True(t, ok, "runner not found")
	assert.Equal(t, 4*time.Minute, got.To.Sub(got.From), "want a 4m runner window regardless of the page range")
}

// windowSpy records the window a runner query was made with.
type windowSpy struct{ got *Window }

func (s windowSpy) Scope(context.Context, Scope, Window) (ScopeSeries, error) {
	return ScopeSeries{}, nil
}
func (s windowSpy) Runner(_ context.Context, _ string, w Window) (RunnerSeries, error) {
	*s.got = w
	return RunnerSeries{}, nil
}
func (s windowSpy) Throughput(context.Context, Scope, Window) (Counts, error) { return Counts{}, nil }
func (s windowSpy) Churn(context.Context, Scope, Window) (Counts, error)      { return Counts{}, nil }
func (s windowSpy) Repos(context.Context, Window, int) ([]RepoHistory, error) { return nil, nil }
func (s windowSpy) Stats(context.Context) (StoreStats, error)                 { return StoreStats{}, nil }
func (s windowSpy) Failures(context.Context, Scope, Window, int) (FailureWindow, error) {
	return FailureWindow{}, nil
}

func TestMissingRunnerIsNotFoundNotAnError(t *testing.T) {
	t.Parallel()

	_, ok := testBuilder().Runner(context.Background(), "gone", Signals{}, now, nil)
	assert.False(t, ok, "a runner that has finished should report not found")

	_, ok = testBuilder().Set(context.Background(), "gone", Signals{}, now)
	assert.False(t, ok, "an absent set should report not found")
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

func testHandler(h *hub.Hub) *Handler {
	return &Handler{
		Builder:   testBuilder(),
		Hub:       h,
		Log:       zerolog.Nop(),
		Heartbeat: time.Hour, // long, so tests drive the stream through the hub
		Streams:   NewStreamRegistry(),
	}
}

// readUntilSignal consumes SSE lines until the sequence-signal frame arrives,
// reporting how many element patches preceded it. Counting frames rather than
// waiting for a fixed number is deliberate: the initial paint also carries the
// address-bar script, so the exact count is an implementation detail no test
// should be pinned to.
func readUntilSignal(t *testing.T, r *bufio.Reader) (elements int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		line, err := r.ReadString('\n')
		require.NoError(t, err, "reading stream")
		switch {
		case strings.HasPrefix(line, "event: datastar-patch-elements"):
			elements++
		case strings.HasPrefix(line, "event: datastar-patch-signals"):
			return elements
		}
	}
	t.Fatal("no signal frame arrived before the deadline")
	return 0
}

func TestStreamEmitsDatastarFraming(t *testing.T) {
	t.Parallel()

	h := testHandler(hub.New())
	srv := httptest.NewServer(http.HandlerFunc(h.StreamOverview))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"), "Content-Type")
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"), "Cache-Control")

	// v1 has exactly two event types; anything else means the SDK contract
	// moved and the browser would ignore the frames entirely.
	elements := readUntilSignal(t, bufio.NewReader(resp.Body))
	assert.NotZero(t, elements, "the initial paint sent no element patches")
}

func TestStreamSkipsUnchangedRegions(t *testing.T) {
	t.Parallel()

	h := hub.New()
	handler := testHandler(h)
	srv := httptest.NewServer(http.HandlerFunc(handler.StreamOverview))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	readUntilSignal(t, reader) // drain the initial paint

	// Nothing about the fleet changed, so the next tick must carry only the
	// sequence signal. Re-sending a full page of identical markup every
	// interval is the cost this diffing exists to avoid.
	h.Broadcast(now)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		require.NoError(t, err, "reading second tick")
		if strings.HasPrefix(line, "event: datastar-patch-elements") {
			t.Fatal("an unchanged region was re-sent")
		}
		if strings.HasPrefix(line, "event: datastar-patch-signals") {
			return // only the signal, as intended
		}
	}
	t.Fatal("no second tick arrived")
}

func TestStreamRegistryClosesOpenStreams(t *testing.T) {
	t.Parallel()

	handler := testHandler(hub.New())
	srv := httptest.NewServer(http.HandlerFunc(handler.StreamOverview))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	deadline := time.Now().Add(5 * time.Second)
	for handler.Streams.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, 1, handler.Streams.Len(), "want 1 registered stream")

	handler.Streams.CloseAll()

	// The body must end rather than hanging: this is what stops Shutdown from
	// blocking for its full timeout on every dashboard someone left open.
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("closing the registry did not end the stream")
	}
}

func TestStreamRegistryRefusesAfterClose(t *testing.T) {
	t.Parallel()

	reg := NewStreamRegistry()
	reg.CloseAll()

	ctx, release := reg.Add(context.Background())
	defer release()

	// A stream that opens mid-shutdown must not be served: the server is
	// trying to drain, and admitting it would extend the drain indefinitely.
	assert.Error(t, ctx.Err(), "a stream opened after CloseAll should start already cancelled")
	assert.Zero(t, reg.Len(), "want no registered streams after close")
}

// tickOffset parses a tick label into how far before "now" it sits.
// time.ParseDuration has no unit for days, so those are handled separately.
func tickOffset(t *testing.T, label string) time.Duration {
	t.Helper()

	if label == "now" {
		return 0
	}
	body := strings.TrimPrefix(label, "-")
	if days, ok := strings.CutSuffix(body, "d"); ok {
		n, err := strconv.Atoi(days)
		require.NoError(t, err, "tick %q", label)
		return time.Duration(n) * 24 * time.Hour
	}
	d, err := time.ParseDuration(body)
	require.NoError(t, err, "tick %q", label)
	return d
}

// TestTicksAreEvenlySpaced treats the label strip as a correctness property
// rather than a snapshot.
//
// Ticks lays the labels out with flex justify-between, so they land at equal
// spacing across the chart however many there are. A set whose labels are not
// equally spaced in TIME therefore prints a label underneath a position that
// means something else, and the axis lies without looking wrong. Three ranges
// used to do exactly that: 6h skipped -2h, 24h ended on two 3h steps after
// three 6h ones, and 7d mixed 2d and 1d steps.
func TestTicksAreEvenlySpaced(t *testing.T) {
	t.Parallel()

	for _, r := range AllRanges() {
		labels := r.Ticks()

		require.GreaterOrEqual(t, len(labels), 3, "%s: too few labels to space", r)
		assert.Equal(t, "now", labels[len(labels)-1], "%s: the strip must end at now", r)
		assert.Equal(t, r.Duration(), tickOffset(t, labels[0]),
			"%s: the strip must start at the window edge", r)

		want := r.Duration() / time.Duration(len(labels)-1)
		for i := 1; i < len(labels); i++ {
			gap := tickOffset(t, labels[i-1]) - tickOffset(t, labels[i])
			assert.Equal(t, want, gap,
				"%s: %q to %q is %s, but %d labels across %s means every gap is %s",
				r, labels[i-1], labels[i], gap, len(labels), r.Duration(), want)
		}
	}
}

func TestAssetsAreContentHashedAndImmutable(t *testing.T) {
	t.Parallel()

	assets, err := NewAssets()
	require.NoError(t, err)

	url := assets.JS()
	require.NotEqual(t, AssetPrefix+"datastar.js", url, "the asset URL was not content-hashed")

	rec := httptest.NewRecorder()
	assets.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, strings.TrimPrefix(url, AssetPrefix[:len(AssetPrefix)-1]), nil))

	require.Equal(t, http.StatusOK, rec.Code, "status for %s", url)
	// embed.FS files have a zero modtime, so ServeContent emits no
	// Last-Modified and no ETag of its own; the hashed name plus this header
	// is the entire caching strategy.
	assert.Contains(t, rec.Header().Get("Cache-Control"), "immutable", "want immutable for a hashed asset")
}

// TestAssetsConditionalRequestKeepsCachingHeaders pins what a 304 must carry.
//
// A 304 answering If-None-Match has to repeat the ETag and Cache-Control the
// 200 would have carried, or the browser cannot extend the freshness lifetime
// and revalidates the hashed asset on every load — which defeats the whole
// content-hash-plus-immutable strategy. An earlier hand-rolled branch here
// wrote the 304 before those headers were set; http.ServeContent now answers
// the conditional itself, after they are in place.
func TestAssetsConditionalRequestKeepsCachingHeaders(t *testing.T) {
	t.Parallel()

	assets, err := NewAssets()
	require.NoError(t, err)

	path := strings.TrimPrefix(assets.JS(), AssetPrefix[:len(AssetPrefix)-1])

	first := httptest.NewRecorder()
	assets.ServeHTTP(first, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, first.Code)

	etag := first.Header().Get("ETag")
	require.NotEmpty(t, etag, "no ETag on the 200, so there is nothing to revalidate with")

	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("If-None-Match", etag)
	second := httptest.NewRecorder()
	assets.ServeHTTP(second, req)

	require.Equal(t, http.StatusNotModified, second.Code, "a matching ETag must revalidate")
	assert.Equal(t, etag, second.Header().Get("ETag"), "the 304 dropped the ETag")
	assert.Contains(t, second.Header().Get("Cache-Control"), "immutable",
		"the 304 dropped Cache-Control")
}

// The requested-resources line is drawn from a series the store has kept all
// along, yet resourceChart drew only its newest value, flat across the whole
// window. A fleet that grew from 2 cores to 6 an instant ago then claimed to
// have reserved 6 cores for the entire hour — a reservation that never
// happened, on the one panel whose whole job is comparing usage against what
// was actually reserved at the time.
func TestRequestLineFollowsHistoryNotOnlyItsNewestValue(t *testing.T) {
	t.Parallel()

	used := []float64{1, 1, 1, 4}
	request := []float64{2, 2, 2, 6}

	c := resourceChart("cpu", used, request, Range1h, strokeCPU, fillCPU, ToneCPU, fleet.FormatCores)

	require.Len(t, c.Refs, 1, "want the requested reference line")
	ys := polylineYs(t, c.Refs[0].Points)
	require.Len(t, ys, len(request), "want one point per sample, got %v", ys)
	// SVG y grows downward, so the smaller early request sits lower on the plot
	// than the larger closing one.
	assert.Greater(t, ys[0], ys[3],
		"the early 2-core request must sit below the closing 6-core one, got %v", ys)
}

// polylineYs pulls the y coordinate out of each "x,y" pair of a points string.
func polylineYs(t *testing.T, points string) []float64 {
	t.Helper()

	fields := strings.Fields(points)
	out := make([]float64, 0, len(fields))
	for _, pair := range fields {
		_, y, ok := strings.Cut(pair, ",")
		require.True(t, ok, "malformed point %q in %q", pair, points)
		v, err := strconv.ParseFloat(y, 64)
		require.NoError(t, err, "parse y of %q", pair)
		out = append(out, v)
	}
	return out
}

// A fleet scaled to nothing right now still reserved resources for the rest of
// the window, and that history is the only thing on the panel worth looking at.
// Gating the line on its newest value alone blanks it every night.
func TestRequestLineSurvivesAFleetThatIsIdleRightNow(t *testing.T) {
	t.Parallel()

	used := []float64{1, 1, 1, 0}
	request := []float64{2, 2, 2, 0}

	c := resourceChart("cpu", used, request, Range1h, strokeCPU, fillCPU, ToneCPU, fleet.FormatCores)

	require.Len(t, c.Refs, 1, "want the requested line kept for the history in the window")
	assert.Len(t, polylineYs(t, c.Refs[0].Points), len(request),
		"want one point per sample even though the newest is zero")
}

// Row counts in the store footer run to seven and eight digits. Unseparated,
// 1234567 and 12345678 are the same shape at a glance, which is the one thing a
// capacity readout must not be.
func TestThousandsSeparatesLongCounts(t *testing.T) {
	t.Parallel()

	cases := map[int64]string{
		0:         "0",
		7:         "7",
		999:       "999",
		1000:      "1,000",
		12345:     "12,345",
		1234567:   "1,234,567",
		123456789: "123,456,789",
	}
	for in, want := range cases {
		assert.Equal(t, want, Thousands(in), "Thousands(%d)", in)
	}
}

// The SQLite file is the one moving part of this dashboard nobody else
// monitors: it grows on a PVC sized once at install time, and the first symptom
// of a full volume is the history quietly stopping. The footer is the only
// place its size is ever reported.
func TestStoreFooterReportsSizeAndRowCounts(t *testing.T) {
	t.Parallel()

	b := testBuilder()
	b.History = statsHistory{stats: StoreStats{
		Enabled:     true,
		Path:        "/data/arc-ui.db",
		SizeBytes:   12 * 1024 * 1024,
		Samples:     1234567,
		Jobs:        42,
		Phases:      7,
		ChurnEvents: 99,
		Rows:        1234715,
		Oldest:      now.Add(-72 * time.Hour),
	}}

	html := renderOverviewWith(t, b)

	assert.Contains(t, html, `id="store"`, "the footer must be a patchable region")
	assert.Contains(t, html, "12.0 MiB", "want the size on disk")
	assert.Contains(t, html, "1,234,567", "want the sample count, separated")
	assert.Contains(t, html, "/data/arc-ui.db", "want the file the numbers describe")
}

// Running without a writable volume is a supported configuration, and the
// footer is then reporting on a store that does not exist. Zeros would read as
// an empty database rather than an absent one.
func TestStoreFooterSaysSoWhenHistoryIsDisabled(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	assert.Contains(t, html, "history store disabled",
		"an absent store must be named, not rendered as zero rows")
	assert.NotContains(t, html, "0 B", "an absent store must not report a size")
}

// failureHistory answers the lane from a fixture.
type failureHistory struct {
	NoHistory
	window FailureWindow
	err    error
}

func (h failureHistory) Failures(context.Context, Scope, Window, int) (FailureWindow, error) {
	return h.window, h.err
}

// The store is the lane's source of truth, and its whole point is rows whose
// runners no longer exist.
func TestFailureLaneRendersHistoryAndTheWindowFooter(t *testing.T) {
	t.Parallel()

	b := testBuilder()
	b.History = failureHistory{window: FailureWindow{
		Failures: []fleet.Failure{
			{Runner: "deleted-1", Set: "arc-ubuntu", Reason: "OOMKilled", At: now.Add(-20 * time.Minute), Severe: true},
			{Runner: "arc-arm64-xyz", Set: "arc-arm64", Reason: "ImagePullBackOff", At: now.Add(-30 * time.Second), Severe: true},
		},
		Total: 41,
	}}

	html := renderOverviewWith(t, b)

	assert.Contains(t, html, "deleted-1",
		"a failure whose runner ARC has already deleted must still be listed")
	assert.Contains(t, html, "39 more", "want the rest of the window counted in the footer")
}

// When the store cannot answer, the live snapshot is all there is — and it still
// knows what is broken right now. An empty lane would claim a healthy fleet.
func TestFailureLaneFallsBackToLiveWhenTheStoreQueryFails(t *testing.T) {
	t.Parallel()

	b := testBuilder()
	b.History = failureHistory{err: errors.New("database is locked")}

	html := renderOverviewWith(t, b)

	assert.Contains(t, html, "arc-arm64-xyz", "want the live failure when history is unavailable")
	assert.NotContains(t, html, "more in this window",
		"a fallback lane knows nothing about a window total and must not claim one")
}

// The recorder writes on the same snapshot the page renders from, so a brand new
// failure can be on screen a tick before it is in the database — and a store
// that has stopped accepting writes is exactly when the lane matters most.
func TestMergeFailuresAddsLiveFailuresTheStoreHasNotCaughtUpWith(t *testing.T) {
	t.Parallel()

	stored := FailureWindow{
		Failures: []fleet.Failure{
			{Runner: "old", Reason: "Evicted", At: now.Add(-time.Hour), Severe: true},
		},
		Total: 1,
	}
	live := []fleet.Failure{
		{Runner: "brand-new", Reason: "ImagePullBackOff", At: now.Add(-time.Second), Severe: true},
	}

	got := mergeFailures(stored, live, 6, "last hour")

	require.Len(t, got.Items, 2, "want both the persisted and the unpersisted failure")
	assert.Equal(t, "brand-new", got.Items[0].Runner, "want newest first across both sources")
	assert.Zero(t, got.More, "two of two shown leaves nothing more to count")
}

func TestMergeFailuresDoesNotListAStoredFailureTwice(t *testing.T) {
	t.Parallel()

	failure := fleet.Failure{Runner: "runner-x", Reason: "OOMKilled", At: now.Add(-time.Minute), Severe: true}
	stored := FailureWindow{Failures: []fleet.Failure{failure}, Total: 1}

	got := mergeFailures(stored, []fleet.Failure{failure}, 6, "last hour")

	assert.Len(t, got.Items, 1, "the same failure came back from both sources and was listed twice")
	assert.Zero(t, got.More, "more")
}

func TestMergeFailuresCapsThePageWithoutCappingTheCount(t *testing.T) {
	t.Parallel()

	stored := FailureWindow{
		Failures: []fleet.Failure{
			{Runner: "a", Reason: "Evicted", At: now.Add(-time.Minute)},
			{Runner: "b", Reason: "Evicted", At: now.Add(-2 * time.Minute)},
		},
		Total: 41,
	}

	got := mergeFailures(stored, nil, 1, "last 6 hours")

	require.Len(t, got.Items, 1, "want the page capped")
	assert.Equal(t, "a", got.Items[0].Runner, "want the newest kept")
	assert.Equal(t, 40, got.More, "want the window's remainder, not the page's")
	assert.Equal(t, "last 6 hours", got.Window, "the lane has to say which window it covers")
}
