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
	"fmt"
	"io"
	"net/http"
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
		client:   &http.Client{Timeout: scrapeTimeout},
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

// scrape fetches and parses the endpoint once, returning the scale set name
// collisions the body contained along with the metrics.
func (s *Scraper) scrape(ctx context.Context) (Metrics, []collision, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("building request for %s: %w", s.url, err)
	}
	// The listener serves classic Prometheus text; ask for it explicitly so a
	// proxy in front of it does not negotiate something we cannot parse.
	req.Header.Set("Accept", "text/plain;version=0.0.4,*/*;q=0.1")

	resp, err := s.client.Do(req)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("scraping %s: %w", s.url, err)
	}
	defer func() {
		// Drain before closing so the connection can be reused across ticks.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return Metrics{}, nil, fmt.Errorf("scraping %s: unexpected status %s", s.url, resp.Status)
	}

	// parse, not Parse: a scale set name reused across namespaces is something
	// only an operator can fix, so the report has to reach the log — via the
	// tracker, because this runs every interval forever.
	m, cols, err := parse(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("scraping %s: %w", s.url, err)
	}
	return m, cols, nil
}
