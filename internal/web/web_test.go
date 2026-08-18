package web

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

	o := testBuilder().Overview(context.Background(), Signals{}, now)
	o.Stream = "/stream"

	var sb strings.Builder
	require.NoError(t, Document(o.Page, OverviewPage(o)).Render(context.Background(), &sb), "render")
	return sb.String()
}

func TestOverviewRendersEveryPatchTarget(t *testing.T) {
	t.Parallel()

	html := renderOverview(t)

	// Every id the stream patches must exist in the initial document, or the
	// first push lands nowhere and that panel silently stops updating.
	for _, id := range []string{
		"filterbar", "tiles", "history", "utilization", "resources",
		"throughput", "churn", "runnersets", "runners", "repos",
		"failures", "health", "live-indicator",
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
