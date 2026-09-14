package listener

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

// maxKeptBytes caps the exposition we keep, not the response we read.
//
// Those are very different numbers. A listener on a busy scale set serves
// megabytes of series nothing here reads — the job histograms carry a bucket
// per repository, workflow, job name and result, and the Go runtime families
// come along for free — so a cap on the response rejects an endpoint for bytes
// that were going to be discarded, which is what used to happen and why a large
// enough fleet simply had no queue depth.
//
// Memory is what has to stay bounded, and only the kept bytes cost any: the
// exposition parser has no streaming mode and materialises whatever it is
// handed into map[string]*dto.MetricFamily, roughly an order of magnitude
// larger than the text it came from, with maxConcurrentScrapes of them in
// flight. Response size is bounded by scrapeTimeout instead — a body that never
// ends costs bandwidth for as long as that allows, and no memory at all.
const maxKeptBytes = 8 << 20

// filterBufBytes is the line buffer. A longer line is read in pieces and
// classified on the first one, which is the only piece a metric name can be in.
const filterBufBytes = 64 << 10

// errKeptTooLarge reports a body whose collected series alone overran the cap.
// It says which series overran rather than "the response", because an operator
// told their response is too large will go looking at its size — which is no
// longer the thing that was too big. The endpoint is not named here: scrape
// supplies the redacted URL when it wraps this.
var errKeptTooLarge = fmt.Errorf("the series this dashboard reads exceed the %d byte limit", maxKeptBytes)

// wantedFamilies is every family name filterExposition passes through: the ones
// collect looks up, plus the bare spelling of each counter, because which of the
// two a parser hands back has changed between prometheus/common releases.
var wantedFamilies = func() map[string]struct{} {
	out := make(map[string]struct{}, len(collectedFamilies)+2)
	for _, name := range collectedFamilies {
		out[name] = struct{}{}
		if bare, isTotal := strings.CutSuffix(name, "_total"); isTotal {
			out[bare] = struct{}{}
		}
	}
	return out
}()

// filterExposition reads a Prometheus text exposition and returns only the lines
// belonging to the families this package collects.
//
// A line it cannot confidently read as a sample or as metadata is kept, not
// dropped. An endpoint that is not a listener at all — an HTML error page, a
// tarball, an exposition in a format this parser does not speak — has to keep
// failing as the parse error it is, rather than filtering down to nothing and
// publishing an empty answer as fact. That also puts junk back under a cap: it
// counts against the budget like anything else we keep, which is what still
// bounds a response that is no longer bounded itself.
func filterExposition(r io.Reader) ([]byte, error) {
	br := bufio.NewReaderSize(r, filterBufBytes)

	var (
		out bytes.Buffer
		// partial is set when the previous chunk ended mid-line, so the
		// classification made on its first piece carries over to the rest.
		partial bool
		keep    bool
	)
	for {
		chunk, err := br.ReadSlice('\n')
		if len(chunk) > 0 {
			if !partial {
				keep = keepLine(chunk)
			}
			if keep {
				if out.Len()+len(chunk) > maxKeptBytes {
					return nil, errKeptTooLarge
				}
				// ReadSlice hands back the reader's own buffer, valid only
				// until the next read, so this copy is the point.
				_, _ = out.Write(chunk)
			}
		}

		switch {
		case err == nil:
			partial = false
		case errors.Is(err, bufio.ErrBufferFull):
			partial = true
		case errors.Is(err, io.EOF):
			return out.Bytes(), nil
		default:
			return nil, err
		}
	}
}

// keepLine reports whether one exposition line is worth handing to the parser.
func keepLine(line []byte) bool {
	s := bytes.TrimLeft(line, " \t")
	switch {
	case len(s) == 0, s[0] == '\n', s[0] == '\r':
		return false
	case s[0] == '#':
		return keepComment(s)
	}

	name := familyName(s)
	if name == nil {
		return true
	}
	_, ok := wantedFamilies[string(name)]
	return ok
}

// keepComment keeps the HELP and TYPE lines of families we collect and drops
// every other comment, including plain ones.
//
// Metadata travels with its samples in both directions: a TYPE line kept for a
// family whose samples were dropped declares a family the parser then reports
// as present and empty, and samples kept without their TYPE line arrive untyped,
// which is a value collect no longer reads as a gauge.
func keepComment(line []byte) bool {
	// The leading hashes are trimmed rather than skipped as one token because
	// the parser accepts "#HELP" as readily as "# HELP", and metadata we failed
	// to recognise is metadata dropped from a family we kept.
	fields := bytes.Fields(bytes.TrimLeft(line, "#"))
	if len(fields) < 2 {
		return false
	}
	switch string(fields[0]) {
	case "HELP", "TYPE":
		_, ok := wantedFamilies[string(fields[1])]
		return ok
	}
	return false
}

// familyName returns the metric name a sample line starts with, or nil when the
// line does not start with one — which keepLine resolves in the parser's favour
// rather than dropping the line.
//
// Only the classic name production is recognised. A UTF-8 name is quoted inside
// the braces (`{"job.duration_seconds",le="1"}`), so a line starting with `{`
// reports no name and is passed through: ARC's listener does not emit them, and
// guessing at a name we would then have to unquote is how a filter silently eats
// series it did not understand.
func familyName(line []byte) []byte {
	end := bytes.IndexAny(line, "{ \t")
	if end <= 0 {
		return nil
	}

	name := line[:end]
	for i, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_', c == ':':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return nil
		}
	}
	return name
}
