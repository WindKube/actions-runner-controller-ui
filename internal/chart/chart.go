// Package chart turns numbers into SVG geometry strings.
//
// Every chart in this dashboard is server-rendered inline SVG: there is no
// client-side charting library, so these functions produce the exact
// `points="..."` attributes that the templates interpolate. That keeps live
// updates cheap — a new sample means recomputing one polygon and pushing a
// couple of kilobytes of markup down an already-open stream.
//
// All functions are pure and total: degenerate inputs (no samples, a zero
// maximum, a single point) yield empty or flat geometry rather than NaN, so a
// cluster with no data renders an empty chart instead of corrupt markup.
package chart

import (
	"slices"
	"strconv"
	"strings"
)

// Band describes one stacked layer of an area chart.
type Band struct {
	Fill   string
	Stroke string
}

// Area is a closed polygon covering one band of a stacked chart.
type Area struct {
	Points string
	Fill   string
	Stroke string
}

// Line is an open polyline plus the closed polygon beneath it.
type Line struct {
	Points string
	Area   string
	Fill   string
	Stroke string
}

// StackedBar is one column of a two-part stacked bar chart, the lower segment
// sitting on the baseline and the upper stacked directly on top of it.
type StackedBar struct {
	X, W           string
	LowerY, LowerH string
	UpperY, UpperH string
}

// DivergingBar is one column of a chart mirrored about a centre line.
type DivergingBar struct {
	X, W         string
	UpY, UpH     string
	DownY, DownH string
}

// Stacked builds cumulative area polygons for a stacked chart.
//
// values[b][i] is band b's value at sample i; every band must have the same
// length. top is the padding reserved above the plot so the topmost band never
// touches the frame. Bands are returned in reverse order so that the first
// band in the input paints last and therefore sits visually on top.
func Stacked(values [][]float64, bands []Band, w, h, top, max float64) []Area {
	n := sampleCount(values)
	if n == 0 || max <= 0 || len(bands) == 0 {
		return nil
	}

	x := xScale(n, w)
	y := func(v float64) string { return f1(h - (v/max)*(h-top)) }

	base := make([]float64, n)
	out := make([]Area, 0, len(values))

	for b, series := range values {
		if b >= len(bands) {
			break
		}
		// Walk the upper edge left-to-right, then back along the previous
		// band's edge to close the polygon.
		var sb strings.Builder
		upper := make([]float64, n)
		for i := range n {
			upper[i] = base[i] + at(series, i)
			writePoint(&sb, x(i), y(upper[i]))
		}
		for i := n - 1; i >= 0; i-- {
			writePoint(&sb, x(i), y(base[i]))
		}
		copy(base, upper)
		// #nosec G602 -- b < len(bands) is enforced by the break at the top of this loop
		out = append(out, Area{Points: sb.String(), Fill: bands[b].Fill, Stroke: bands[b].Stroke})
	}

	slices.Reverse(out)
	return out
}

// Plot builds a line and the filled area beneath it. The series is compressed
// by 18 units — zero sits 6 above the bottom edge, a full-scale value 12 below
// the top — so the stroke stays clear of the viewBox edges and a value at the
// maximum still shows its cap.
func Plot(vals []float64, w, h, max float64, fill, stroke string) Line {
	if len(vals) == 0 || max <= 0 {
		return Line{Fill: fill, Stroke: stroke}
	}

	x := xScale(len(vals), w)
	var sb strings.Builder
	for i, v := range vals {
		writePoint(&sb, x(i), plotY(v, h, max))
	}
	line := sb.String()

	baseline := f1(0) + "," + f1(h)
	return Line{
		Points: line,
		Area:   baseline + " " + line + " " + f1(w) + "," + f1(h),
		Fill:   fill,
		Stroke: stroke,
	}
}

// FlatLine is a horizontal reference line — a resource request or limit —
// drawn on the same scale as Plot so the two line up exactly.
func FlatLine(v, w, h, max float64) string {
	if max <= 0 {
		return ""
	}
	y := plotY(v, h, max)
	return f1(0) + "," + y + " " + f1(w) + "," + y
}

// Bars lays out evenly spaced two-segment stacked columns across the width.
// Columns are positioned on a fixed pitch so that a partially filled chart
// still aligns with its x-axis labels.
func Bars(lower, upper []float64, count int, w, h, max, gap float64) []StackedBar {
	if count <= 0 || max <= 0 {
		return nil
	}
	pitch, barW := columns(count, w, gap)

	out := make([]StackedBar, 0, count)
	for i := range count {
		lo := (at(lower, i) / max) * h
		up := (at(upper, i) / max) * h
		out = append(out, StackedBar{
			X:      f0(float64(i)*pitch + gap/2),
			W:      f0(barW),
			LowerY: f1(h - lo),
			LowerH: f1(lo),
			UpperY: f1(h - lo - up),
			UpperH: f1(up),
		})
	}
	return out
}

// Diverging lays out columns mirrored about a centre line: up values grow
// upward from centre, down values downward.
func Diverging(up, down []float64, count int, w, h, centre, max, gap float64) []DivergingBar {
	if count <= 0 || max <= 0 {
		return nil
	}
	pitch, barW := columns(count, w, gap)
	// Each half gets whichever span is smaller, so neither can overflow.
	span := min(centre, h-centre)

	out := make([]DivergingBar, 0, count)
	for i := range count {
		u := (at(up, i) / max) * span
		d := (at(down, i) / max) * span
		out = append(out, DivergingBar{
			X:     f0(float64(i)*pitch + gap/2),
			W:     f0(barW),
			UpY:   f1(centre - u),
			UpH:   f1(u),
			DownY: f1(centre),
			DownH: f1(d),
		})
	}
	return out
}

// GridLine is a horizontal rule with its right-margin value label.
type GridLine struct {
	Y string
	// X2 is where the rule ends. Rules start at x=0 and span the full plot
	// width, and carrying that width here is what keeps the template from
	// having to know the viewBox the geometry was computed against.
	X2    string
	Label string
	// TopPx positions the label in CSS pixels, since the label lives in the
	// HTML layer beside the SVG rather than inside it.
	TopPx string
}

// Grid spaces count+1 rules from zero to max across a chart w wide whose plot
// area is inset by top, and computes each label's pixel offset for the given
// rendered height. label formats the value at each rule.
func Grid(count int, w, h, top, max, renderedPx float64, label func(float64) string) []GridLine {
	if count <= 0 || max <= 0 {
		return nil
	}
	step := max / float64(count)
	x2 := f1(w)
	out := make([]GridLine, 0, count+1)
	for i := 0; i <= count; i++ {
		v := step * float64(i)
		y := h - (v/max)*(h-top)
		out = append(out, GridLine{
			Y:     f1(y),
			X2:    x2,
			Label: label(v),
			TopPx: f1(y * renderedPx / h),
		})
	}
	return out
}

// LabelTopPx converts a viewBox y coordinate to a pixel offset for a label
// rendered alongside the SVG rather than within it.
func LabelTopPx(y, viewBoxH, renderedPx float64) string {
	if viewBoxH <= 0 {
		return "0.0"
	}
	return f1(y * renderedPx / viewBoxH)
}

// Percent clamps a ratio to 0..100 and renders it for a CSS width.
func Percent(used, total float64) string {
	if total <= 0 {
		return "0"
	}
	return f1(min(max(used/total*100, 0), 100))
}

// plotY maps a value onto the inset plot area shared by Plot and FlatLine.
func plotY(v, h, max float64) string {
	return f1(h - 6 - (v/max)*(h-18))
}

// xScale returns an index-to-x mapping. A single sample is pinned to the left
// edge rather than dividing by zero.
func xScale(n int, w float64) func(int) string {
	if n <= 1 {
		return func(int) string { return "0.0" }
	}
	den := float64(n - 1)
	return func(i int) string { return f1(float64(i) * w / den) }
}

// columns spaces count bars evenly across w. A gap at least as wide as the
// pitch would leave no bar to draw, so it is dropped rather than clamped.
func columns(count int, w, gap float64) (pitch, barW float64) {
	pitch = w / float64(count)
	barW = pitch - gap
	if barW <= 0 {
		barW = pitch
	}
	return pitch, barW
}

// writePoint appends "x,y" to sb, space-separated from whatever is already
// there, so callers never have to track which point is first.
func writePoint(sb *strings.Builder, x, y string) {
	if sb.Len() > 0 {
		sb.WriteByte(' ')
	}
	sb.WriteString(x)
	sb.WriteByte(',')
	sb.WriteString(y)
}

func sampleCount(values [][]float64) int {
	n := 0
	for _, s := range values {
		n = max(n, len(s))
	}
	return n
}

// at reads a series defensively so a short band does not panic a whole page.
func at(s []float64, i int) float64 {
	if i < 0 || i >= len(s) {
		return 0
	}
	return s[i]
}

func f1(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }
func f0(v float64) string { return strconv.FormatFloat(v, 'f', 0, 64) }
