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
// to scrape. That is the normal case, not a failure: nothing to scrape is
// recorded as the reason the number is missing.
//
// There is also never just one endpoint. ARC runs one AutoscalingListener pod
// per scale set and each serves only its own scale set's series, so a fleet's
// queue depth exists only as the union of every listener's answer — which is why
// this scraper takes a Discoverer rather than a URL, and why a partial answer is
// published rather than discarded. A single configured URL is still supported for
// the case where something in front of the listeners aggregates them, such as a
// Prometheus /federate endpoint.
package listener

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
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

// maxConcurrentScrapes bounds how many listeners are scraped at once.
//
// One round covers a normal fleet, so a tick costs about one scrapeTimeout
// rather than one per listener. Ticks never overlap — Run calls tick
// synchronously — so on a very large fleet of dead listeners the effect of the
// cap is a slower cadence, not a pile-up of goroutines.
const maxConcurrentScrapes = 16

// maxNamedFailures caps how many failing listeners one reason names. The rest
// are counted: an operator needs the shape of the failure and one example, and a
// reason listing ninety pods is a reason nobody reads.
const maxNamedFailures = 3

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

// Discoverer supplies the endpoints to scrape, re-resolved on every tick.
//
// The collector implements it from the pod cache it already keeps for listener
// health. Re-resolving matters: pod IPs are recycled, listeners are recreated
// whenever a scale set changes, and a target list remembered at boot would
// slowly become a list of addresses belonging to someone else.
type Discoverer interface {
	ListenerTargets() []fleet.ListenerTarget
}

// fixedTargets is a Discoverer over an operator-configured endpoint list.
type fixedTargets []fleet.ListenerTarget

// ListenerTargets returns the fixed list.
func (f fixedTargets) ListenerTargets() []fleet.ListenerTarget { return f }

// Scraper polls ARC listener metrics endpoints on an interval.
type Scraper struct {
	targets Discoverer
	// configured is true when the endpoints came from configuration rather than
	// from discovery. The two differ in what "nothing to scrape" means: a
	// configured empty endpoint will never become non-empty, while discovery
	// finding no listener today finds one the moment someone enables metrics.
	configured bool

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

// NewScraper returns a Scraper for one operator-configured endpoint, which is
// either a single listener or something aggregating several of them. An empty
// url is valid and means "there is nothing configured to scrape".
func NewScraper(url string, interval time.Duration, log zerolog.Logger, sink Sink) *Scraper {
	var fixed fixedTargets
	if url != "" {
		// No Pod: a configured endpoint is named in messages by its redacted
		// URL, because that is the string the operator can go and fix.
		fixed = fixedTargets{{URL: url}}
	}
	s := NewDiscoveringScraper(fixed, interval, log, sink)
	s.configured = true
	return s
}

// NewDiscoveringScraper returns a Scraper that rebuilds its target list from d
// on every tick.
func NewDiscoveringScraper(d Discoverer, interval time.Duration, log zerolog.Logger, sink Sink) *Scraper {
	if interval <= 0 {
		interval = defaultInterval
	}
	return &Scraper{
		targets:  d,
		interval: interval,
		log:      log.With().Str("component", "listener-scraper").Logger(),
		sink:     sink,
		client:   &http.Client{Timeout: scrapeTimeout, Transport: scrapeTransport()},
		now:      time.Now,
	}
}

// Run scrapes until ctx is cancelled. Returns nil on clean shutdown.
//
// A configured-but-empty endpoint returns immediately: there is nothing to poll
// and nothing that will ever make there be, so spinning a ticker to fail forever
// would only bury the real message under repetition. Discovery does keep
// ticking, because an install with no listener metrics today has them the moment
// someone uncomments the chart's metrics block.
func (s *Scraper) Run(ctx context.Context) error {
	if s.configured && len(s.targets.ListenerTargets()) == 0 {
		s.reportDisabled()
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

// tick scrapes every current target and publishes one outcome. Like the metrics
// poller it never returns an error: a missing listener degrades the view, it
// does not stop the process.
func (s *Scraper) tick(ctx context.Context) {
	list := s.targets.ListenerTargets()
	if len(list) == 0 {
		// Discovery found nothing, which on a stock install is the truth rather
		// than a fault: no listener exposes a metrics port because ARC ships
		// them disabled. The reason says how to change that.
		s.health.fail(s.log, disabledReason)
		s.reportDisabled()
		return
	}

	merged, index, failures := s.scrapeAll(ctx, list)
	if ctx.Err() != nil {
		return // shutdown, not a listener fault
	}

	if len(failures) == len(list) {
		// The collision tracker is deliberately left alone: a scrape that
		// failed says nothing about how the cluster is deployed, and treating
		// it as "collisions gone" would make a flapping endpoint re-announce
		// the same ones on every recovery.
		reason := allFailedReason(failures)
		s.health.fail(s.log, reason)
		// Unlike pod usage, a stale queue depth is actively misleading: the
		// backlog it describes may have drained minutes ago and there is no
		// timestamp on screen to hint otherwise. Drop back to "unknown".
		s.sink.SetQueueDepth(nil, false)
		s.sink.SetSource(fleet.Source{
			Name:      fleet.SourceListener,
			Available: false,
			Reason:    reason,
			CheckedAt: s.now(),
		})
		return
	}

	// Collisions are computed once over every body, because with one listener
	// per scale set a name claimed by two namespaces is now spread across two
	// responses and invisible inside either.
	s.collisions.observe(s.log, index.collisions())

	// A partial answer is published, not discarded. The sets that answered have
	// real queue depth and the ones that did not are simply absent from the map,
	// which the dashboard already renders as unknown rather than zero.
	reason := ""
	if len(failures) > 0 {
		reason = partialReason(len(list)-len(failures), len(list), failures)
		s.health.degrade(s.log, reason)
	} else {
		s.health.ok(s.log)
	}

	s.sink.SetQueueDepth(merged.QueueDepth(), true)
	s.sink.SetSource(fleet.Source{
		Name:      fleet.SourceListener,
		Available: true,
		Reason:    reason,
		CheckedAt: s.now(),
	})
}

// reportDisabled publishes "there is nothing serving listener metrics".
func (s *Scraper) reportDisabled() {
	s.sink.SetQueueDepth(nil, false)
	s.sink.SetSource(fleet.Source{
		Name:      fleet.SourceListener,
		Available: false,
		Reason:    disabledReason,
		CheckedAt: s.now(),
	})
}

// failure is one target that did not answer.
type failure struct {
	target fleet.ListenerTarget
	err    error
}

// String names the listener and what it said, which is the pair an operator
// needs to go and look at it.
func (f failure) String() string {
	return targetName(f.target) + ": " + f.err.Error()
}

// targetName identifies a target in a message: the pod for a discovered
// listener, the redacted URL for a configured endpoint.
func targetName(t fleet.ListenerTarget) string {
	if t.Pod != "" {
		return t.Pod
	}
	return safeURL(t.URL)
}

// scrapeAll scrapes every target concurrently and folds the answers together.
func (s *Scraper) scrapeAll(
	ctx context.Context, list []fleet.ListenerTarget,
) (Metrics, namespaceIndex, []failure) {
	type result struct {
		m     Metrics
		index namespaceIndex
		err   error
	}
	results := make([]result, len(list))

	var wg sync.WaitGroup
	slots := make(chan struct{}, min(maxConcurrentScrapes, len(list)))
	for i, target := range list {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()

			m, index, err := s.scrape(ctx, target.URL)
			results[i] = result{m: m, index: index, err: err}
		}()
	}
	wg.Wait()

	// Folded in target order rather than completion order, so a fleet with two
	// same-named scale sets reports the same ceiling on every tick instead of
	// whichever listener happened to answer first.
	var (
		merged   Metrics
		index    = namespaceIndex{}
		failures []failure
	)
	for i, r := range results {
		if r.err != nil {
			failures = append(failures, failure{target: list[i], err: r.err})
			continue
		}
		merged.fold(r.m)
		index.union(r.index)
	}
	return merged, index, failures
}

// partialReason describes a fleet whose listeners are only partly reachable.
func partialReason(ok, total int, failures []failure) string {
	return fmt.Sprintf("%d of %d listeners scraped; %s", ok, total, namedFailures(failures))
}

// allFailedReason describes a fleet where nothing answered.
//
// A single endpoint reports its own error verbatim: it is the only thing that
// failed, and "no listener answered" in front of it would be ceremony around a
// message that already says everything.
func allFailedReason(failures []failure) string {
	if len(failures) == 1 {
		return failures[0].err.Error()
	}
	return fmt.Sprintf("no listener answered (%d of %d failed): %s",
		len(failures), len(failures), namedFailures(failures))
}

// namedFailures lists at most maxNamedFailures failures and counts the rest.
func namedFailures(failures []failure) string {
	named := failures
	if len(named) > maxNamedFailures {
		named = named[:maxNamedFailures]
	}

	parts := make([]string, 0, len(named))
	for _, f := range named {
		parts = append(parts, f.String())
	}
	out := strings.Join(parts, "; ")
	if rest := len(failures) - len(named); rest > 0 {
		out += fmt.Sprintf(" (+%d more)", rest)
	}
	return out
}

// safeURL is an endpoint with its credentials, query string and fragment
// removed, for use in error text.
//
// Every error scrape returns is copied into fleet.Source.Reason by tick, which
// the dashboard renders and the log records. ARC_UI_LISTENER_METRICS_URL is
// operator-supplied and may legitimately carry userinfo or a token query
// parameter, so neither may appear there. The userinfo is replaced rather than
// dropped: an operator reading "credentials were sent and it still 401'd" is
// better served than one who cannot tell whether any were sent at all.
//
// The fragment is stripped for the same reason and is the easiest of the three
// to overlook, because it is the one part of a URL that never reaches the
// server. That makes it invisible in a packet capture and in the listener's own
// logs — but it is still in the string the operator configured, and this
// function's whole job is to make that string safe to display.
func safeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		// Unparseable, so it cannot be redacted field by field, and echoing it
		// raw is the thing being avoided.
		return "the configured listener metrics URL"
	}
	u.RawQuery = ""
	// Both: String writes the fragment only when Fragment is set, but leaving
	// RawFragment behind as a stale encoding hint for a fragment that no longer
	// exists is a trap for the next reader.
	u.Fragment = ""
	u.RawFragment = ""
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

// scrape fetches and parses one endpoint, returning the namespace index the body
// produced along with the metrics.
func (s *Scraper) scrape(ctx context.Context, endpoint string) (Metrics, namespaceIndex, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("building request for %s: %w", safeURL(endpoint), withoutURL(err))
	}
	// The listener serves classic Prometheus text; ask for it explicitly so a
	// proxy in front of it does not negotiate something we cannot parse.
	req.Header.Set("Accept", "text/plain;version=0.0.4,*/*;q=0.1")

	resp, err := s.client.Do(req)
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("scraping %s: %w", safeURL(endpoint), withoutURL(err))
	}
	defer func() {
		// Drain before closing so the connection can be reused across ticks.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return Metrics{}, nil, fmt.Errorf("scraping %s: unexpected status %s", safeURL(endpoint), resp.Status)
	}

	// One byte more than we will accept, so that running the reader dry is
	// evidence the body overran rather than ended. io.LimitReader alone cannot
	// express this: it reports EOF at the cap, which the parser reads as the
	// end of the exposition. A body that overruns on a line boundary then
	// parses cleanly as its own prefix and we publish it as known — a queue
	// depth missing every series past the cap, presented as fact.
	body := &io.LimitedReader{R: resp.Body, N: maxBodyBytes + 1}

	// parse, not Parse: a scale set name reused across namespaces is something
	// only an operator can fix, so the report has to reach the log — via the
	// tracker, because this runs every interval forever.
	m, index, err := parse(body)

	// Checked before err: an oversized body usually fails the parse too, on
	// whatever half-line it was cut at. "unexpected token" sends an operator
	// hunting for a malformed exposition; the size is the actual fault and the
	// only one of the two they can act on.
	if body.N == 0 {
		return Metrics{}, nil, fmt.Errorf(
			"scraping %s: response exceeds the %d byte limit", safeURL(endpoint), maxBodyBytes)
	}
	if err != nil {
		return Metrics{}, nil, fmt.Errorf("scraping %s: %w", safeURL(endpoint), withoutURL(err))
	}
	return m, index, nil
}
