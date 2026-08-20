package listener

import (
	"fmt"
	"hash/fnv"
	"strings"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"github.com/samber/lo"
)

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
// A scraper pointed at a listener that never came up would otherwise emit an
// identical warning every interval forever, which trains everyone to ignore
// the log. Transitions are loud; repeats are debug-only.
type healthTracker struct {
	state  healthState
	reason string
}

// ok records a successful scrape, logging only the recovery.
func (h *healthTracker) ok(log zerolog.Logger) {
	switch h.state {
	case healthUp:
		log.Debug().Msg("scrape succeeded")
		return
	case healthDown:
		log.Info().Str("previous_error", h.reason).Msg("source recovered")
	case healthUnknown:
		log.Info().Msg("source available")
	}
	h.state = healthUp
	h.reason = ""
}

// fail records a scrape where nothing answered. The same error repeating is not
// news; a different error is, because it usually means the failure mode changed
// (connection refused, then 404, then unparseable body).
func (h *healthTracker) fail(log zerolog.Logger, reason string) {
	h.down(log, "source unavailable", reason)
}

// degrade records a scrape where some listeners answered and some did not.
//
// It shares fail's state machine, so a fleet that loses one listener says so
// once rather than every interval — but not its wording: a fleet with nineteen
// healthy listeners is not an unavailable source, and logging it as one is how
// an operator learns to distrust the line.
func (h *healthTracker) degrade(log zerolog.Logger, reason string) {
	h.down(log, "source degraded", reason)
}

func (h *healthTracker) down(log zerolog.Logger, msg, reason string) {
	if h.state == healthDown && h.reason == reason {
		log.Debug().Str("error", reason).Msg("scrape still failing")
		return
	}
	log.Warn().Str("error", reason).Msg(msg)
	h.state = healthDown
	h.reason = reason
}

// maxNamedCollisions caps how many colliding scale set names one warning lists.
// The scrape body is operator-supplied and bounded only by maxBodyBytes, which
// leaves room for tens of thousands of distinct colliding names, and a warning
// nobody can read through is the same failure as a warning nobody sees. The
// colliding_names field always carries the exact count; only naming is cut short.
const maxNamedCollisions = 8

// maxNamedNamespaces caps how many namespaces each named collision lists, for
// the same reason: two is the case an operator meets, and the interesting part
// of a hundred is that there are a hundred.
const maxNamedNamespaces = 4

// maxLabelBytes caps each label value a warning interpolates. Capping how many
// names and namespaces are listed bounds their count but not their length: the
// values come from the scrape body, which is operator-supplied and bounded only
// by maxBodyBytes, so one colliding name carrying megabyte label values is
// otherwise one megabyte-long warn line.
//
// 64 bytes clears a Kubernetes namespace name, which is a DNS label and so at
// most 63 characters; a scale set name may legitimately be longer and is then
// logged cut short.
const maxLabelBytes = 64

// unlabelledNamespace names the member of a collision that stands for a series
// carrying no namespace label. Rendering that member as the empty string it
// actually is produces "shared in , team-a", a bare leading comma nothing on
// screen explains.
const unlabelledNamespace = "(no namespace label)"

// collisionTracker reports scale set name collisions on healthTracker's terms:
// a state is announced once, and repeats of it are debug-only.
//
// A collision is a property of how the exposition is labelled, not of this
// scrape — the same labels next tick say it again, and every tick after that
// until someone changes them. Repeating it each tick trains everyone
// to ignore the log, which is exactly what health.go's doctrine exists to
// prevent, and here the repetition is also sized by an untrusted body.
//
// The zero value means "no collisions", so the clean scrape — the overwhelmingly
// common case — says nothing at all, and the first collision is the first line.
type collisionTracker struct {
	// reported fingerprints the last state announced; 0 means "none".
	reported uint64
}

// observe reports cols, and must be given them sorted — namespaceIndex.collisions
// does that. An unsorted report would fingerprint differently every tick and
// defeat the whole point.
func (c *collisionTracker) observe(log zerolog.Logger, cols []collision) {
	fp := fingerprint(cols)
	if fp == c.reported {
		if len(cols) > 0 {
			log.Debug().Int("colliding_names", len(cols)).Msg("scale set name collisions unchanged")
		}
		return
	}
	c.reported = fp

	if len(cols) == 0 {
		log.Info().Msg("scale set name collisions resolved")
		return
	}
	log.Warn().
		Int("colliding_names", len(cols)).
		Strs("scale_sets", describeCollisions(cols)).
		Msg("scale set name seen under more than one namespace label value in the min, max and " +
			"desired runner gauges (a series carrying no namespace label counts as one); " +
			"at most one series' value is reported per gauge")
}

// describeCollisions renders a bounded, human-readable slice of a report: at
// most maxNamedCollisions names, each with at most maxNamedNamespaces of its
// namespaces and a count of the ones left out. Every part that comes from the
// scrape body is either cut down by truncateLabel or replaced by a fixed
// stand-in, so a body carrying megabyte label values does not produce a
// megabyte log line.
func describeCollisions(cols []collision) []string {
	named := lo.Slice(cols, 0, maxNamedCollisions)
	out := make([]string, 0, len(named))
	for _, c := range named {
		shown := lo.Map(lo.Slice(c.namespaces, 0, maxNamedNamespaces), renderNamespace)
		entry := truncateLabel(c.set) + " in " + strings.Join(shown, ", ")
		if extra := len(c.namespaces) - maxNamedNamespaces; extra > 0 {
			entry += fmt.Sprintf(" and %d more", extra)
		}
		out = append(out, entry)
	}
	return out
}

// renderNamespace names one member of a collision, spelling out the one that
// carried no namespace label rather than rendering it as an empty element.
func renderNamespace(ns string, _ int) string {
	if ns == "" {
		return unlabelledNamespace
	}
	return truncateLabel(ns)
}

// truncateLabel cuts a value longer than maxLabelBytes down to that many bytes,
// plus an ellipsis marking that it did. The cut backs up to a rune-start byte,
// so it never leaves half a multi-byte character behind.
func truncateLabel(v string) string {
	if len(v) <= maxLabelBytes {
		return v
	}
	cut := maxLabelBytes
	for cut > 0 && !utf8.RuneStart(v[cut]) {
		cut--
	}
	return v[:cut] + "…"
}

// fingerprint reduces a collision report to something a tracker can hold on to
// between ticks. It hashes rather than keeps a copy because the report is
// derived from an untrusted body and can be megabytes wide, while the tracker
// lives as long as the process.
//
// 0 is reserved for "no collisions" so that a zero-valued tracker already means
// clean; the |1 keeps a real report from ever landing on it. Names are separated
// by a byte rather than length-prefixed, so a contrived pair of label values can
// in principle hash alike — the cost of that is one warning suppressed, which
// does not earn framing machinery.
func fingerprint(cols []collision) uint64 {
	if len(cols) == 0 {
		return 0
	}
	h := fnv.New64a()
	for _, c := range cols {
		_, _ = h.Write([]byte(c.set))
		for _, ns := range c.namespaces {
			_, _ = h.Write([]byte{0x1f})
			_, _ = h.Write([]byte(ns))
		}
		_, _ = h.Write([]byte{0x1e})
	}
	return h.Sum64() | 1
}
