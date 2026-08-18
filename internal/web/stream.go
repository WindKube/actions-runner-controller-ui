package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/a-h/templ"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"github.com/starfederation/datastar-go/datastar"

	"arc-ui/internal/fleet"
	"arc-ui/internal/hub"
)

// EventSource fetches recent Kubernetes events for one runner pod.
//
// It is separate from the snapshot because events are the highest-churn object
// in a cluster and must never be held in an informer cache; they are fetched
// on demand, for one pod, only when someone opens its detail page.
type EventSource interface {
	Events(ctx context.Context, r fleet.Runner) ([]fleet.Event, error)
}

// Handler serves the dashboard.
type Handler struct {
	Builder *Builder
	Hub     *hub.Hub
	Log     zerolog.Logger
	Events  EventSource

	// Heartbeat is how often a stream re-renders even when nothing has
	// changed. Without it a quiet fleet is indistinguishable from a dead
	// connection, because the browser's staleness counter only resets when a
	// patch arrives.
	Heartbeat time.Duration

	// Streams tracks open streams so shutdown can close them deliberately
	// rather than waiting for each to time out.
	Streams *StreamRegistry
}

func (h *Handler) now() time.Time { return time.Now() }

func (h *Handler) heartbeat() time.Duration {
	if h.Heartbeat > 0 {
		return h.Heartbeat
	}
	return 15 * time.Second
}

// ---------------------------------------------------------------------------
// Page handlers
// ---------------------------------------------------------------------------

// Index serves the fleet overview as a complete document.
func (h *Handler) Index(w http.ResponseWriter, r *http.Request) {
	sig := SignalsFromQuery(r.URL.Query())
	o := h.Builder.Overview(r.Context(), sig, h.now())
	o.Stream = "/stream"
	h.render(w, r, o.Page, OverviewPage(o))
}

// SetDetail serves one RunnerSet.
func (h *Handler) SetDetail(w http.ResponseWriter, r *http.Request, name string) {
	sig := SignalsFromQuery(r.URL.Query())
	d, ok := h.Builder.Set(r.Context(), name, sig, h.now())
	if !ok {
		h.notFound(w, r, sig, "runnerset", name)
		return
	}
	d.Stream = "/stream/runnersets/" + name
	h.render(w, r, d.Page, SetPage(d))
}

// RunnerDetail serves one runner.
func (h *Handler) RunnerDetail(w http.ResponseWriter, r *http.Request, name string) {
	sig := SignalsFromQuery(r.URL.Query())
	d, ok := h.Builder.Runner(r.Context(), name, sig, h.now(), h.events(r.Context(), name))
	if !ok {
		h.notFound(w, r, sig, "runner", name)
		return
	}
	d.Stream = "/stream/runners/" + name
	h.render(w, r, d.Page, RunnerPage(d))
}

// events fetches a runner's pod events, tolerating both a missing source and a
// failed lookup: an empty events panel is a much better outcome than a 500 on
// the page that explains why a runner failed.
func (h *Handler) events(ctx context.Context, name string) []fleet.Event {
	if h.Events == nil {
		return nil
	}
	r, ok := h.Builder.Fleet.Snapshot().Runner(name)
	if !ok {
		return nil
	}
	evs, err := h.Events.Events(ctx, r)
	if err != nil {
		h.Log.Warn().Err(err).Str("runner", name).Msg("could not fetch pod events")
		return nil
	}
	return evs
}

func (h *Handler) notFound(w http.ResponseWriter, r *http.Request, sig Signals, kind, name string) {
	o := h.Builder.Overview(r.Context(), sig, h.now())
	o.Title = name
	o.Stream = "/stream"
	o.Crumbs = []Crumb{{Label: o.Org}, {Label: "fleet", Href: "/"}, {Label: name}}
	w.WriteHeader(http.StatusNotFound)
	h.render(w, r, o.Page, NotFound(o.Page, kind, name))
}

func (h *Handler) render(w http.ResponseWriter, r *http.Request, p Page, body templ.Component) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := Document(p, body).Render(r.Context(), w); err != nil {
		// The status line is already written by this point, so there is no
		// meaningful recovery — log it and let the client see a truncated page.
		h.Log.Error().Err(err).Str("path", r.URL.Path).Msg("render failed")
	}
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

// regionSet is a page's patchable regions, keyed by DOM id.
type regionSet map[string]templ.Component

// StreamOverview keeps the fleet overview live.
func (h *Handler) StreamOverview(w http.ResponseWriter, r *http.Request) {
	h.stream(w, r, "/stream", func(ctx context.Context, sig Signals, now time.Time) (regionSet, bool) {
		o := h.Builder.Overview(ctx, sig, now)
		o.Stream = "/stream"
		return regionSet{
			"filterbar":   FilterBar(o),
			"tiles":       Tiles(o),
			"history":     HistoryPanel(o),
			"utilization": UtilPanel(o),
			"resources":   ResourcePanel(o),
			"throughput":  ThroughputPanel(o),
			"churn":       ChurnPanel(o),
			"runnersets":  SetsPanel(o),
			"runners":     RunnersPanel(o),
			"repos":       ReposPanel(o),
			"failures":    FailuresPanel(o),
			"health":      HealthStrip(o.Page),
		}, true
	})
}

// StreamSet keeps one RunnerSet detail view live.
func (h *Handler) StreamSet(w http.ResponseWriter, r *http.Request, name string) {
	url := "/stream/runnersets/" + name
	h.stream(w, r, url, func(ctx context.Context, sig Signals, now time.Time) (regionSet, bool) {
		d, ok := h.Builder.Set(ctx, name, sig, now)
		if !ok {
			return nil, false
		}
		d.Stream = url
		return regionSet{
			"tiles":     SetTiles(d),
			"history":   SetHistory(d),
			"resources": SetResources(d),
			"config":    SetConfig(d),
			"churn":     SetChurn(d),
			"runners":   SetRunners(d),
			"health":    HealthStrip(d.Page),
		}, true
	})
}

// StreamRunner keeps one runner detail view live.
func (h *Handler) StreamRunner(w http.ResponseWriter, r *http.Request, name string) {
	url := "/stream/runners/" + name
	h.stream(w, r, url, func(ctx context.Context, sig Signals, now time.Time) (regionSet, bool) {
		d, ok := h.Builder.Runner(ctx, name, sig, now, h.events(ctx, name))
		if !ok {
			return nil, false
		}
		d.Stream = url
		return regionSet{
			"resources": RunnerResources(d),
			"events":    RunnerEvents(d),
			"setjobs":   SetJobsPanel(d),
			"facts":     RunnerFacts(d),
			"health":    HealthStrip(d.Page),
		}, true
	})
}

// stream is the shared loop behind every live view.
//
// It re-renders on each fleet change and on a heartbeat, patches only the
// regions whose markup actually differs from what this client last received,
// and bumps a sequence signal so the browser can time its own staleness.
func (h *Handler) stream(
	w http.ResponseWriter,
	r *http.Request,
	streamURL string,
	render func(ctx context.Context, sig Signals, now time.Time) (regionSet, bool),
) {
	// Read the signals before anything is written: on a GET they arrive as a
	// JSON query parameter, and NewSSE commits the response headers.
	var sig Signals
	if err := datastar.ReadSignals(r, &sig); err != nil {
		h.Log.Debug().Err(err).Msg("stream opened without usable signals; falling back to the query string")
		sig = SignalsFromQuery(r.URL.Query())
	}
	sig = sig.Normalize()

	// A server-wide WriteTimeout would otherwise cut every stream at the
	// deadline. Clearing it per-request means ordinary handlers keep their
	// timeout and only streams opt out.
	if err := http.NewResponseController(w).SetWriteDeadline(time.Time{}); err != nil {
		h.Log.Debug().Err(err).Msg("could not clear the write deadline for this stream")
	}

	ctx := r.Context()
	if h.Streams != nil {
		var release func()
		ctx, release = h.Streams.Add(ctx)
		defer release()
	}

	sse := datastar.NewSSE(w, r, datastar.WithContext(ctx))

	ticks, unsubscribe := h.Hub.Subscribe()
	defer unsubscribe()

	// Reflect the active filters in the address bar so the view can be shared
	// or reloaded. Done once per stream, since the filters are fixed for its
	// lifetime — a change reopens the stream.
	h.replaceURL(sse, r, sig)

	sent := make(map[string]string, 16)
	heartbeat := time.NewTicker(h.heartbeat())
	defer heartbeat.Stop()

	var seq uint64
	for {
		regions, ok := render(ctx, sig, h.now())
		if !ok {
			// The runner or set vanished mid-stream. On an ephemeral fleet that
			// is the normal end of a runner's life, not an error.
			h.Log.Debug().Str("stream", streamURL).Msg("stream target no longer exists")
			return
		}

		for id, component := range regions {
			if err := h.patch(ctx, sse, sent, id, component); err != nil {
				h.logStreamEnd(streamURL, err)
				return
			}
		}

		seq++
		if err := sse.PatchSignals([]byte(`{"_seq":` + strconv.FormatUint(seq, 10) + `}`)); err != nil {
			h.logStreamEnd(streamURL, err)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticks:
		case <-heartbeat.C:
		}
	}
}

// patch renders one region and sends it only when it differs from what this
// client last received.
//
// The comparison is per-connection rather than global because two browsers
// with different filters legitimately see different markup for the same id.
// Most ticks change only the tiles and a couple of table rows, so this turns a
// full page of markup per interval into a few hundred bytes.
func (h *Handler) patch(ctx context.Context, sse *datastar.ServerSentEventGenerator, sent map[string]string, id string, c templ.Component) error {
	var buf bytes.Buffer
	if err := c.Render(ctx, &buf); err != nil {
		return fmt.Errorf("render %s: %w", id, err)
	}
	html := buf.String()
	if sent[id] == html {
		return nil
	}
	if err := sse.PatchElements(html); err != nil {
		return err
	}
	sent[id] = html
	return nil
}

// replaceURL rewrites the address bar to match the active filters.
func (h *Handler) replaceURL(sse *datastar.ServerSentEventGenerator, r *http.Request, sig Signals) {
	path := "/"
	if u, err := url.Parse(r.Header.Get("Referer")); err == nil && u.Path != "" {
		path = u.Path
	}
	if q := sig.Query().Encode(); q != "" {
		path += "?" + q
	}
	// JSON-quoting the path is what keeps a crafted filter value from breaking
	// out of the string literal and into the surrounding script.
	quoted, err := json.Marshal(path)
	if err != nil {
		return
	}
	if err := sse.ExecuteScript("history.replaceState(null, '', "+string(quoted)+")",
		datastar.WithExecuteScriptAutoRemove(true)); err != nil {
		h.Log.Debug().Err(err).Msg("could not sync the address bar")
	}
}

// logStreamEnd records why a stream ended, distinguishing a browser that
// navigated away from a genuine failure. Closed connections are the normal
// case and must not be logged as errors.
func (h *Handler) logStreamEnd(streamURL string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		h.Log.Debug().Str("stream", streamURL).Msg("client disconnected")
		return
	}
	h.Log.Warn().Err(err).Str("stream", streamURL).Msg("stream ended")
}

// ---------------------------------------------------------------------------
// Stream registry
// ---------------------------------------------------------------------------

// StreamRegistry tracks open SSE streams so shutdown can end them promptly.
//
// http.Server.Shutdown waits for active connections, and an SSE stream is
// active until its handler returns — so without this, shutting down waits the
// full timeout for every open dashboard. Cancelling the server's BaseContext
// would also work but is indiscriminate: it would abort ordinary in-flight
// requests too, which should be allowed to finish.
type StreamRegistry struct {
	mu     sync.Mutex
	cancel map[int64]context.CancelFunc
	next   int64
	closed bool
}

// NewStreamRegistry returns an empty registry.
func NewStreamRegistry() *StreamRegistry {
	return &StreamRegistry{cancel: make(map[int64]context.CancelFunc)}
}

// Add registers a stream and returns a derived context plus a release func
// that must always be called.
func (s *StreamRegistry) Add(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		// Shutdown has already begun; hand back a context that is already
		// cancelled so the stream ends immediately rather than serving a
		// connection the server is trying to drain.
		cancel()
		return ctx, func() {}
	}
	s.next++
	id := s.next
	s.cancel[id] = cancel
	s.mu.Unlock()

	return ctx, func() {
		s.mu.Lock()
		delete(s.cancel, id)
		s.mu.Unlock()
		cancel()
	}
}

// CloseAll ends every open stream. It is safe to call more than once.
func (s *StreamRegistry) CloseAll() {
	s.mu.Lock()
	s.closed = true
	cancels := lo.Values(s.cancel)
	clear(s.cancel)
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

// Len reports how many streams are open.
func (s *StreamRegistry) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.cancel)
}
