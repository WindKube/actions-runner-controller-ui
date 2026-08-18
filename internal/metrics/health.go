package metrics

import "github.com/rs/zerolog"

// healthState is tri-state on purpose: "not yet known" must log the first
// outcome, whatever it is, and only then start suppressing repeats.
type healthState int

const (
	healthUnknown healthState = iota
	healthUp
	healthDown
)

// healthTracker turns a per-tick outcome into log lines a human can live with.
//
// A poller running every 15s against a cluster with no metrics-server would
// otherwise emit ~5,700 identical warnings a day, which trains everyone to
// ignore the log. Transitions are loud; repeats are debug-only.
type healthTracker struct {
	state  healthState
	reason string
}

// ok records a successful poll, logging only the recovery.
func (h *healthTracker) ok(log zerolog.Logger) {
	switch h.state {
	case healthUp:
		log.Debug().Msg("poll succeeded")
		return
	case healthDown:
		log.Info().Str("previous_error", h.reason).Msg("source recovered")
	case healthUnknown:
		log.Info().Msg("source available")
	}
	h.state = healthUp
	h.reason = ""
}

// fail records a failed poll. The same error repeating is not news; a
// different error is, because it usually means the failure mode changed
// (RBAC denied, then connection refused, then timeout).
func (h *healthTracker) fail(log zerolog.Logger, reason string) {
	if h.state == healthDown && h.reason == reason {
		log.Debug().Str("error", reason).Msg("poll still failing")
		return
	}
	log.Warn().Str("error", reason).Msg("source unavailable")
	h.state = healthDown
	h.reason = reason
}
