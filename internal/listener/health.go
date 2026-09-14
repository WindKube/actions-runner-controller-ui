package listener

import (
	"strings"
	"unicode/utf8"

	"github.com/rs/zerolog"
)

// maxNamedCollisions caps how many colliding scale set names one warning lists.
// The scrape body is operator-supplied and bounded only by maxKeptBytes, which
// leaves room for tens of thousands of distinct colliding names. The
// colliding_names field always carries the exact count; only naming is cut short.
const maxNamedCollisions = 8

// maxNamedNamespaces caps how many namespaces each named collision lists, for
// the same reason: two is the case an operator meets, and the interesting part
// of a hundred is that there are a hundred.
const maxNamedNamespaces = 4

// maxLabelBytes caps each label value a warning interpolates. Capping how many
// names and namespaces are listed bounds their count but not their length, so one
// colliding name carrying megabyte label values is otherwise a megabyte-long warn
// line.
//
// 64 bytes clears a Kubernetes namespace name, which is a DNS label and so at most
// 63 characters; a scale set name may legitimately be longer and is then cut short.
// The cut backs up to a rune boundary so it never leaves half a character behind.
const maxLabelBytes = 64

// unlabelledNamespace names the member of a collision that stands for a series
// carrying no namespace label. Rendering it as the empty string it actually is
// produces "shared in , team-a", a bare leading comma nothing on screen explains.
const unlabelledNamespace = "(no namespace label)"

// collisionTracker reports scale set name collisions on health.Tracker's terms: a
// state is announced once, and repeats of it are debug-only.
//
// A collision is a property of how the exposition is labelled, not of this scrape,
// so it recurs every tick until someone changes the labels. The zero value means
// "nothing reported yet", so a clean scrape says nothing at all.
type collisionTracker struct {
	// reported is the last rendered report; empty means "no collisions".
	reported string
}

// observe reports cols, which namespaceIndex.collisions returns already sorted
// and bounded. An unsorted report would render differently every tick and
// defeat the whole point.
func (c *collisionTracker) observe(log zerolog.Logger, count int, cols []string) {
	report := strings.Join(cols, "; ")
	if report == c.reported {
		if count > 0 {
			log.Debug().Int("colliding_names", count).Msg("scale set name collisions unchanged")
		}
		return
	}
	c.reported = report

	if count == 0 {
		log.Info().Msg("scale set name collisions resolved")
		return
	}
	log.Warn().
		Int("colliding_names", count).
		Strs("scale_sets", cols).
		Msg("scale set name seen under more than one namespace label value in the min, max and " +
			"desired runner gauges (a series carrying no namespace label counts as one); " +
			"at most one series' value is reported per gauge")
}

// truncateLabel cuts a value longer than maxLabelBytes down to that many bytes,
// plus an ellipsis marking that it did.
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
