// Package web renders the dashboard and keeps it live.
//
// Three things distinguish it from a conventional Go web layer.
//
// Every chart is server-computed SVG. internal/chart turns numbers into
// polygon and polyline point strings, and the templates interpolate them
// directly, so there is no client-side charting library and no JSON API behind
// the charts.
//
// Updates are pushed, not polled. One SSE stream per page carries that page's
// filter state; when the fleet changes, the server re-renders the affected
// regions and patches them by DOM id. Only the regions whose markup actually
// changed are sent, so a quiet tick costs a few dozen bytes.
//
// The only client-side dependency is Datastar, which is why the interaction
// model is expressed as data-* attributes on server-rendered HTML rather than
// as a component tree. Pages render complete on the server: deep links, back
// and forward, and a no-JavaScript reader all work before any script runs.
package web

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/url"
	"strconv"
	"strings"
	"time"

	"arc-ui/internal/chart"
	"arc-ui/internal/fleet"
)

// Signals are the Datastar signals the browser holds and sends back with every
// request. The json tags are the signal names.
//
// Names are single lowercase words on purpose. HTML lowercases attribute
// names, and Datastar's default "camel" case conversion turns a kebab-cased
// attribute key into a camelCase signal — so a signal called "fRepo" would
// have to be written `data-bind:f-repo`. Flat lowercase sidesteps the whole
// mapping.
type Signals struct {
	Repo     string `json:"repo"`
	Workflow string `json:"workflow"`
	Job      string `json:"job"`
	Set      string `json:"set"`
	State    string `json:"state"`
	Range    string `json:"range"`
	Sort     string `json:"sort"`
}

// Filter converts signals into a domain filter.
func (s Signals) Filter() fleet.Filter {
	return fleet.Filter{
		Repo:     s.Repo,
		Workflow: s.Workflow,
		Job:      s.Job,
		Set:      s.Set,
		State:    s.State,
	}
}

// Normalize fills in defaults for anything the browser did not send, so a
// first page load and a signal round-trip produce identical output.
func (s Signals) Normalize() Signals {
	blankToAny := func(v string) string {
		if v == "" {
			return fleet.AnyValue
		}
		return v
	}
	s.Repo = blankToAny(s.Repo)
	s.Workflow = blankToAny(s.Workflow)
	s.Job = blankToAny(s.Job)
	s.Set = blankToAny(s.Set)
	s.State = blankToAny(s.State)
	s.Range = string(ParseRange(s.Range))
	s.Sort = string(fleet.ParseSetSort(s.Sort))
	return s
}

// SignalsFromQuery builds signals from ordinary URL query parameters, which is
// how a plain page load (or a shared deep link) carries filter state before
// any JavaScript has run.
func SignalsFromQuery(q url.Values) Signals {
	return Signals{
		Repo:     q.Get("repo"),
		Workflow: q.Get("workflow"),
		Job:      q.Get("job"),
		Set:      q.Get("set"),
		State:    q.Get("state"),
		Range:    q.Get("range"),
		Sort:     q.Get("sort"),
	}.Normalize()
}

// Query renders signals back into query parameters, omitting defaults so a
// shared URL stays short and readable.
func (s Signals) Query() url.Values {
	q := url.Values{}
	add := func(k, v string) {
		if v != "" && v != fleet.AnyValue {
			q.Set(k, v)
		}
	}
	add("repo", s.Repo)
	add("workflow", s.Workflow)
	add("job", s.Job)
	add("set", s.Set)
	add("state", s.State)
	if s.Range != string(DefaultRange) {
		add("range", s.Range)
	}
	if s.Sort != string(fleet.SortPressure) {
		add("sort", s.Sort)
	}
	return q
}

// JSON renders the signals as the object literal the page seeds Datastar with.
// Errors are impossible for a flat struct of strings, so the value is returned
// bare rather than forcing every template to handle one.
func (s Signals) JSON() string {
	b, err := json.Marshal(s.Normalize())
	if err != nil {
		return "{}"
	}
	return string(b)
}

// StreamCall is the Datastar action every control invokes.
//
// The retry options are not decoration. Datastar defaults to giving up after
// ten reconnection attempts — roughly three minutes — after which the page
// silently stops updating while still displaying plausible numbers. A wall
// dashboard must reconnect for as long as it is open, so the count is
// effectively unlimited and the backoff is capped at ten seconds.
func StreamCall(streamURL string) string {
	return fmt.Sprintf("@get('%s', {retryMaxCount: 1e9, retryMaxWait: 10000})",
		template.JSEscapeString(streamURL))
}

// SetAndStream is the click handler for a control that sets one signal and
// reloads. Re-invoking the stream aborts the previous one: Datastar keys
// in-flight requests by method and URL, so the new stream replaces the old
// rather than accumulating connections.
func SetAndStream(streamURL, signal, value string) string {
	return fmt.Sprintf("$%s = '%s'; %s", signal, template.JSEscapeString(value), StreamCall(streamURL))
}

// ClearFilters resets every filter signal and reloads.
func ClearFilters(streamURL string) string {
	var b strings.Builder
	for _, s := range []string{"repo", "workflow", "job", "set", "state"} {
		fmt.Fprintf(&b, "$%s = '%s'; ", s, fleet.AnyValue)
	}
	b.WriteString(StreamCall(streamURL))
	return b.String()
}

// TimeRange is one of the six windows the range selector offers.
type TimeRange string

// The ranges the design offers, and the default.
const (
	Range15m TimeRange = "15m"
	Range1h  TimeRange = "1h"
	Range6h  TimeRange = "6h"
	Range24h TimeRange = "24h"
	Range7d  TimeRange = "7d"
	Range30d TimeRange = "30d"

	DefaultRange = Range1h
)

// AllRanges lists the ranges in selector order.
func AllRanges() []TimeRange {
	return []TimeRange{Range15m, Range1h, Range6h, Range24h, Range7d, Range30d}
}

// ParseRange maps a signal value to a range, defaulting to one hour.
func ParseRange(v string) TimeRange {
	for _, r := range AllRanges() {
		if string(r) == v {
			return r
		}
	}
	return DefaultRange
}

// Duration is how far back the range reaches.
func (r TimeRange) Duration() time.Duration {
	switch r {
	case Range15m:
		return 15 * time.Minute
	case Range6h:
		return 6 * time.Hour
	case Range24h:
		return 24 * time.Hour
	case Range7d:
		return 7 * 24 * time.Hour
	case Range30d:
		return 30 * 24 * time.Hour
	default:
		return time.Hour
	}
}

// Label is the prose form used in chart headers.
func (r TimeRange) Label() string {
	switch r {
	case Range15m:
		return "last 15 minutes"
	case Range6h:
		return "last 6 hours"
	case Range24h:
		return "last 24 hours"
	case Range7d:
		return "last 7 days"
	case Range30d:
		return "last 30 days"
	default:
		return "last hour"
	}
}

// Points is how many samples to plot: enough to show the shape of the window,
// few enough that each live update re-renders a compact polygon rather than
// thousands of coordinates.
func (r TimeRange) Points() int {
	switch r {
	case Range15m, Range1h:
		return 60
	case Range6h:
		return 72
	case Range24h:
		return 96
	case Range7d:
		return 84
	default:
		return 90
	}
}

// Window converts a range into the query window handed to the history store.
func (r TimeRange) Window(now time.Time) Window {
	return Window{From: now.Add(-r.Duration()), To: now, Points: r.Points()}
}

// Ticks are the x-axis labels beneath a chart.
//
// The Ticks component lays these out with flex justify-between, so whatever is
// returned lands at equal spacing across the full width of the chart. That
// makes the step size a correctness property rather than a matter of taste: a
// set whose labels are not equally spaced in TIME puts a label under a position
// that means something else, and the axis silently lies.
//
// The count therefore varies by range, chosen so the step is a round unit —
// six labels means five gaps, and a 6h window does not divide into five whole
// hours. Six works for 15m (3m), 1h (12m) and 30d (6d); 6h and 24h want seven,
// and 7d wants eight.
func (r TimeRange) Ticks() []string {
	switch r {
	case Range15m:
		return []string{"-15m", "-12m", "-9m", "-6m", "-3m", "now"}
	case Range6h:
		return []string{"-6h", "-5h", "-4h", "-3h", "-2h", "-1h", "now"}
	case Range24h:
		return []string{"-24h", "-20h", "-16h", "-12h", "-8h", "-4h", "now"}
	case Range7d:
		return []string{"-7d", "-6d", "-5d", "-4d", "-3d", "-2d", "-1d", "now"}
	case Range30d:
		return []string{"-30d", "-24d", "-18d", "-12d", "-6d", "now"}
	default:
		return []string{"-60m", "-48m", "-36m", "-24m", "-12m", "now"}
	}
}

// ---------------------------------------------------------------------------
// Presentation vocabulary
//
// The mock expressed these as inline style strings built by stateVisual(),
// pillStyle() and dotStyle(). Here they become Tailwind class sets over the
// Primer token layer, so the palette lives in one place.
// ---------------------------------------------------------------------------

// Tone is a semantic colour role.
type Tone string

// The tones the dashboard uses.
const (
	ToneSuccess   Tone = "success"
	ToneAccent    Tone = "accent"
	ToneAttention Tone = "attention"
	ToneDanger    Tone = "danger"
	ToneDone      Tone = "done"
	ToneMuted     Tone = "muted"
	ToneCPU       Tone = "cpu"
	ToneMemory    Tone = "memory"
)

// StateTone maps a runner state to its colour role.
func StateTone(s fleet.State) Tone {
	switch s {
	case fleet.StateBusy:
		return ToneSuccess
	case fleet.StateIdle:
		return ToneAccent
	case fleet.StatePending:
		return ToneAttention
	case fleet.StateFailed:
		return ToneDanger
	case fleet.StateTerminating:
		return ToneDone
	}
	return ToneMuted
}

// Text is the foreground class for a tone.
func (t Tone) Text() string {
	switch t {
	case ToneSuccess:
		return "text-success"
	case ToneAccent:
		return "text-accent"
	case ToneAttention:
		return "text-attention"
	case ToneDanger:
		return "text-danger"
	case ToneDone:
		return "text-done"
	case ToneCPU:
		return "text-done"
	case ToneMemory:
		return "text-cyan"
	}
	return "text-fg-subtle"
}

// Dot is the class set for the small status dot beside a name.
func (t Tone) Dot() string {
	switch t {
	case ToneSuccess:
		return "bg-success"
	case ToneAccent:
		return "bg-accent"
	case ToneAttention:
		return "bg-attention"
	case ToneDanger:
		return "bg-danger"
	case ToneDone:
		return "bg-done"
	case ToneCPU:
		return "bg-done"
	case ToneMemory:
		return "bg-cyan"
	}
	return "bg-fg-subtle"
}

// Pill is the class set for an uppercase status badge.
func (t Tone) Pill() string {
	base := "inline-flex items-center h-[18px] px-[7px] rounded-full text-[10.5px] font-semibold tracking-[0.2px] uppercase font-mono w-fit border "
	switch t {
	case ToneSuccess:
		return base + "text-success bg-success/12 border-success/35"
	case ToneAccent:
		return base + "text-accent bg-accent/12 border-accent/30"
	case ToneAttention:
		return base + "text-attention bg-attention/12 border-attention/35"
	case ToneDanger:
		return base + "text-danger bg-danger/12 border-danger/35"
	case ToneDone:
		return base + "text-done bg-done/12 border-done/35"
	}
	return base + "text-fg-subtle bg-fg-subtle/10 border-default"
}

// Bar is the fill class for a micro-bar.
func (t Tone) Bar() string {
	switch t {
	case ToneCPU:
		return "bg-done"
	case ToneMemory:
		return "bg-cyan"
	case ToneSuccess:
		return "bg-success"
	case ToneAccent:
		return "bg-accent"
	}
	return "bg-fg-subtle"
}

// SetTone picks the colour for a runner set's scaling badge.
func SetTone(s fleet.RunnerSet) Tone {
	switch {
	case s.AtCapacity():
		return ToneDanger
	case s.QueuedKnown && s.Queued > 0:
		return ToneAttention
	case s.ScaledToZero():
		return ToneMuted
	default:
		return ToneSuccess
	}
}

// SetBadge is the label on a runner set's scaling badge.
func SetBadge(s fleet.RunnerSet) string {
	switch {
	case s.AtCapacity():
		return "at max"
	case s.ScaledToZero():
		return "scaled to 0"
	default:
		return "ok"
	}
}

// Bounds renders a set's min–max, spelling out an absent ceiling rather than
// printing the MaxInt32 the controller substitutes internally.
func Bounds(s fleet.RunnerSet) string {
	if s.Unbounded {
		return fmt.Sprintf("%d–∞", s.MinRunners)
	}
	return fmt.Sprintf("%d–%d", s.MinRunners, s.MaxRunners)
}

// CapacityLabel is the denominator in the "Runners" tile.
func CapacityLabel(t fleet.Totals) string {
	if t.Unbounded {
		if t.Capacity > 0 {
			return fmt.Sprintf("of %d+ max", t.Capacity)
		}
		return "unlimited"
	}
	return fmt.Sprintf("of %d max", t.Capacity)
}

// Dash renders an absent value the way the design does.
func Dash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

// UsageCores renders observed CPU, distinguishing "not scraped" from zero.
func UsageCores(r fleet.Resources) string {
	if !r.HasUsage() {
		return "—"
	}
	return fleet.FormatCores(r.Used)
}

// UsageGiB renders observed memory, distinguishing "not scraped" from zero.
func UsageGiB(r fleet.Resources) string {
	if !r.HasUsage() {
		return "—"
	}
	return fleet.FormatGiB(r.Used)
}

// Thousands groups a count with comma separators, e.g. "1,234,567".
//
// The store footer reports row counts that reach eight digits. Unseparated,
// 1234567 and 12345678 are the same shape at a glance, and telling those two
// apart is the entire job of a capacity readout.
func Thousands(n int64) string {
	digits := strconv.FormatInt(n, 10)

	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}

	var sb strings.Builder
	for i, d := range digits {
		// A separator goes before every digit whose distance from the end is a
		// multiple of three, except at the very start.
		if i > 0 && (len(digits)-i)%3 == 0 {
			sb.WriteByte(',')
		}
		sb.WriteRune(d)
	}
	return sign + sb.String()
}

// Pct renders a fraction as a whole percentage.
func Pct(f float64) string { return fmt.Sprintf("%.0f%%", f*100) }

// BarWidth is the inline width style for a micro-bar fill.
func BarWidth(used, total float64) string {
	return "width:" + chart.Percent(used, total) + "%"
}

// JobSummary renders the "workflow · job #run" cell.
func JobSummary(r fleet.Runner) string {
	if !r.Job.Present() {
		switch r.State {
		case fleet.StatePending:
			return "registering"
		case fleet.StateFailed:
			return Dash(r.FailureReason)
		case fleet.StateTerminating:
			return "terminating"
		}
		return "awaiting job"
	}
	s := r.Job.Workflow
	if s != "" && r.Job.Name != "" {
		s += " · "
	}
	s += r.Job.Name
	if r.Job.RunID > 0 {
		s += fmt.Sprintf(" #%d", r.Job.RunID)
	}
	return Dash(s)
}

// SourceTone colours a data source in the control-plane strip.
func SourceTone(s fleet.Source) Tone {
	switch {
	case s.Available && s.Reason != "":
		// Degraded: neither the green that says "nothing to look at" nor the red
		// that says "this is down", because it is genuinely in between.
		return ToneAttention
	case s.Available:
		return ToneSuccess
	}
	return ToneDanger
}

// SourceValue is the text shown for a data source, favouring the failure
// reason when there is one — an operator needs to know *why* a panel is empty.
func SourceValue(s fleet.Source, okText string) string {
	// A reason on an available source means it answered only partly — a fleet
	// whose listeners are half reachable, say. Reporting that as okText hides
	// the half that is missing, and the panels it feeds give no other sign.
	if s.Reason != "" {
		return s.Reason
	}
	if s.Available {
		return okText
	}
	return "unavailable"
}
