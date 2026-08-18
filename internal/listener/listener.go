// Package listener scrapes the ARC listener's Prometheus endpoint for the one
// number Kubernetes cannot tell us: how many jobs GitHub has assigned to a
// scale set that have no runner yet.
//
// Queue depth exists only on GitHub's side of the connection. The cluster sees
// runners, never the backlog waiting for them, so without this scrape the
// dashboard can say "40 runners busy" but not "and 120 jobs waiting".
//
// The catch is that these metrics are off by default. The `metrics:` block in
// the gha-runner-scale-set-controller chart values is commented out and no
// Service is created for the listener, so on a stock install there is nothing
// to scrape. That is the normal case, not a failure: an empty URL makes Run
// return immediately after recording why the number is missing.
package listener

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"arc-ui/internal/fleet"

	"github.com/rs/zerolog"
)

// defaultInterval is used when the caller passes a non-positive interval.
const defaultInterval = 15 * time.Second

// scrapeTimeout bounds one HTTP request. The listener serves from memory, so
// anything slower than this is a network or scheduling problem, and a poller
// that blocks forever is worse than one that reports a timeout.
const scrapeTimeout = 10 * time.Second

// maxBodyBytes caps what we will read from the endpoint. The URL is operator
// supplied and may point at something that is not a listener at all; a
// dashboard must not be OOM-killed by a misconfiguration.
const maxBodyBytes = 8 << 20

// disabledReason explains an empty URL in the terms an operator needs to act
// on. It names the chart values because "not configured" alone sends people
// hunting through the dashboard's own settings, where the problem is not.
const disabledReason = "listener metrics endpoint not configured; ARC ships them disabled — " +
	"uncomment the metrics: block in the gha-runner-scale-set-controller chart values, " +
	"expose the listener's metrics port with a Service, then set ARC_UI_LISTENER_METRICS_URL"

// Sink receives each scrape result and each health change.
type Sink interface {
	// SetQueueDepth replaces per-set queue depth. known is false whenever the
	// numbers are not trustworthy, and the sink must then render nothing
	// rather than zero.
	SetQueueDepth(perSet map[string]int, known bool)
	// SetSource reports whether the listener endpoint is currently usable.
	SetSource(fleet.Source)
}

// Scraper polls one ARC listener metrics endpoint on an interval.
type Scraper struct {
	url      string
	interval time.Duration
	log      zerolog.Logger
	sink     Sink
	client   *http.Client

	health     healthTracker
	collisions collisionTracker
	now        func() time.Time
}

// scrapeTransport returns a connection pool private to one Scraper.
//
// Leaving http.Client.Transport nil would mean http.DefaultTransport, which is
// shared with every other HTTP client in the process. That is a correctness
// problem in tests before it is a tidiness one: httptest.Server.Close() calls
// CloseIdleConnections() on http.DefaultTransport directly
// (net/http/httptest/server.go), so one parallel test closing its server tears
// down connections another test's scrape is still using — the scrape then fails
// with "http: CloseIdleConnections called" rather than the status it was
// meant to observe.
//
// Clone() rather than a bare &http.Transport{}: a zero Transport silently drops
// ProxyFromEnvironment (so HTTP_PROXY/NO_PROXY would stop being honoured for
// in-cluster scrapes), the dial timeouts, HTTP/2, and idle-connection reaping.
// Cloning changes which pool the connections live in and nothing else.
func scrapeTransport() http.RoundTripper {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		return t.Clone()
	}
	return &http.Transport{}
}

// NewScraper returns a Scraper for the given endpoint. An empty url is valid
// and means "ARC's listener metrics are disabled", which is the default.
func NewScraper(url string, interval time.Duration, log zerolog.Logger, sink Sink) *Scraper {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Scraper{
		url:      url,
		interval: interval,
		log:      log.With().Str("component", "listener-scraper").Logger(),
		sink:     sink,
		client:   &http.Client{Timeout: scrapeTimeout, Transport: scrapeTransport()},
		now:      time.Now,
	}
}

// Run scrapes until ctx is cancelled. Returns nil on clean shutdown.
//
// With no URL configured it records the reason and returns immediately: there
// is nothing to poll, and spinning a ticker to fail forever would only bury
// the real message under repetition.
func (s *Scraper) Run(ctx context.Context) error {
	if s.url == "" {
		s.sink.SetQueueDepth(nil, false)
		s.sink.SetSource(fleet.Source{
			Name:      fleet.SourceListener,
			Available: false,
			Reason:    disabledReason,
			CheckedAt: s.now(),
		})
		s.log.Info().Msg("listener metrics disabled; queue depth will not be shown")
		return nil
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// tick performs one scrape and publishes the outcome. Like the metrics poller
// it never returns an error: a missing listener degrades the view, it does not
// stop the process.
func (s *Scraper) tick(ctx context.Context) {
	m, cols, err := s.scrape(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // shutdown, not a listener fault
		}
		// The collision tracker is deliberately left alone: a scrape that
		// failed says nothing about how the cluster is deployed, and treating
		// it as "collisions gone" would make a flapping endpoint re-announce
		// the same ones on every recovery.
		s.health.fail(s.log, err.Error())
		// Unlike pod usage, a stale queue depth is actively misleading: the
		// backlog it describes may have drained minutes ago and there is no
		// timestamp on screen to hint otherwise. Drop back to "unknown".
		s.sink.SetQueueDepth(nil, false)
		s.sink.SetSource(fleet.Source{
			Name:      fleet.SourceListener,
			Available: false,
			Reason:    err.Error(),
			CheckedAt: s.now(),
		})
		return
	}

	s.health.ok(s.log)
	s.collisions.observe(s.log, cols)
	s.sink.SetQueueDepth(m.QueueDepth(), true)
	s.sink.SetSource(fleet.Source{
		Name:      fleet.SourceListener,
		Available: true,
		CheckedAt: s.now(),
	})
}

// safeURL is the configured endpoint with its credentials and query string
// removed, for use in error text.
//
// Every error scrape returns is copied into fleet.Source.Reason by tick, which
// the dashboard renders and the log records. ARC_UI_LISTENER_METRICS_URL is
// operator-supplied and may legitimately carry userinfo or a token query
// parameter, so neither may appear there. The userinfo is replaced rather than
// dropped: an operator reading "credentials were sent and it still 401'd" is
// better served than one who cannot tell whether any were sent at all.
func (s *Scraper) safeURL() string {
	u, err := url.Parse(s.url)
	if err != nil {
		// Unparseable, so it cannot be redacted field by field, and echoing it
		// raw is the thing being avoided.
		return "the configured listener metrics URL"
	}
	u.RawQuery = ""
	if u.User != nil {
		u.User = url.User("redacted")
	}
	return u.String()
}

// withoutURL strips net/url's URL-bearing wrapper from err.
//
// Redacting the URL this package formats itself is only half the job: a
// *url.Error carries the request URL verbatim, and net/http redacts only the
// password inside it — a token in the query string survives untouched, and %w
// would carry the whole thing into Source.Reason. Unwrapping keeps the useful
// half ("connect: connection refused") and drops the half that leaks, since the
// caller supplies its own redacted URL alongside.
func withoutURL(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) && uerr.Err != nil {
		return uerr.Err
	}
	return err
}

// scrape fetches and parses the endpoint once, returning the scale set name
// collisions the body contained along with the metrics.
func (s *Scraper) scrape(ctx context.Context) (Metrics, []collision, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("building request for %s: %w", s.safeURL(), withoutURL(err))
	}
	// The listener serves classic Prometheus text; ask for it explicitly so a
	// proxy in front of it does not negotiate something we cannot parse.
	req.Header.Set("Accept", "text/plain;version=0.0.4,*/*;q=0.1")

	resp, err := s.client.Do(req)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("scraping %s: %w", s.safeURL(), withoutURL(err))
	}
	defer func() {
		// Drain before closing so the connection can be reused across ticks.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return Metrics{}, nil, fmt.Errorf("scraping %s: unexpected status %s", s.safeURL(), resp.Status)
	}

	// parse, not Parse: a scale set name reused across namespaces is something
	// only an operator can fix, so the report has to reach the log — via the
	// tracker, because this runs every interval forever.
	m, cols, err := parse(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("scraping %s: %w", s.safeURL(), withoutURL(err))
	}
	return m, cols, nil
}
