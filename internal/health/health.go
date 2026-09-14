// Package health turns a poller's per-tick outcome into log lines a human can
// live with.
//
// Every source in this dashboard is scraped on a timer and every one of them is
// optional, so the same failure recurs every interval until an operator fixes
// it. A poller running every 15s against a cluster with no metrics-server would
// emit ~5,700 identical warnings a day, which trains everyone to ignore the
// log. Transitions are loud; repeats are debug-only.
package health

import "github.com/rs/zerolog"

// state is tri-state on purpose: "not yet known" must log the first outcome,
// whatever it is, and only then start suppressing repeats.
type state int

const (
	unknown state = iota
	up
	down
)

// Tracker records the outcome of one source's polls. The zero value is unusable;
// construct it with New so the log lines name what is being done.
type Tracker struct {
	// verb names the action in the debug lines, e.g. "poll" or "scrape".
	verb string

	state  state
	reason string
}

// New returns a tracker whose debug lines read "<verb> succeeded" and
// "<verb> still failing".
func New(verb string) Tracker { return Tracker{verb: verb} }

// OK records a successful tick, logging only the recovery.
func (h *Tracker) OK(log zerolog.Logger) {
	switch h.state {
	case up:
		log.Debug().Msg(h.verb + " succeeded")
		return
	case down:
		log.Info().Str("previous_error", h.reason).Msg("source recovered")
	case unknown:
		log.Info().Msg("source available")
	}
	h.state = up
	h.reason = ""
}

// Fail records a tick where nothing answered. The same error repeating is not
// news; a different error is, because it usually means the failure mode changed
// (RBAC denied, then connection refused, then unparseable body).
func (h *Tracker) Fail(log zerolog.Logger, reason string) {
	h.down(log, "source unavailable", reason)
}

// Degrade records a tick where some targets answered and some did not.
//
// It shares Fail's state machine, so a fleet that loses one listener says so
// once rather than every interval — but not its wording: a fleet with nineteen
// healthy listeners is not an unavailable source, and logging it as one is how
// an operator learns to distrust the line.
func (h *Tracker) Degrade(log zerolog.Logger, reason string) {
	h.down(log, "source degraded", reason)
}

func (h *Tracker) down(log zerolog.Logger, msg, reason string) {
	if h.state == down && h.reason == reason {
		log.Debug().Str("error", reason).Msg(h.verb + " still failing")
		return
	}
	log.Warn().Str("error", reason).Msg(msg)
	h.state = down
	h.reason = reason
}
