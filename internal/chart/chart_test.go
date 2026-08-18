package chart

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStackedAccumulatesAndReverses(t *testing.T) {
	t.Parallel()

	bands := []Band{{Fill: "busyF", Stroke: "busyS"}, {Fill: "idleF", Stroke: "idleS"}}
	got := Stacked([][]float64{{1, 1}, {1, 1}}, bands, 100, 100, 0, 4)

	require.Len(t, got, 2)
	// Reversed: the last input band comes back first so the first input band
	// paints last and sits on top.
	assert.Equal(t, "idleF", got[0].Fill, "bands not reversed")
	assert.Equal(t, "busyF", got[1].Fill, "bands not reversed")

	// busy occupies 0..1 of a max of 4 on a height of 100: y goes 100 -> 75.
	assert.Equal(t, "0.0,75.0 100.0,75.0 100.0,100.0 0.0,100.0", got[1].Points, "busy points")
	// idle stacks on top of busy: 1..2 of 4, so y goes 50 back down to 75.
	assert.Equal(t, "0.0,50.0 100.0,50.0 100.0,75.0 0.0,75.0", got[0].Points, "idle points")
}

func TestStackedRespectsTopInset(t *testing.T) {
	t.Parallel()

	// A band at full scale must stop at the inset, not at the frame.
	got := Stacked([][]float64{{10}}, []Band{{}}, 100, 100, 12, 10)
	require.Len(t, got, 1)
	assert.True(t, strings.HasPrefix(got[0].Points, "0.0,12.0"),
		"full-scale band should reach the 12px inset, got %q", got[0].Points)
}

func TestPlotAndFlatLineShareAScale(t *testing.T) {
	t.Parallel()

	// A request line at value v must land on the same y as a sample of value
	// v, or every "used vs requested" chart is subtly misaligned.
	line := Plot([]float64{5, 5}, 300, 82, 10, "f", "s")
	flat := FlatLine(5, 300, 82, 10)

	sampleY := strings.Split(strings.Fields(line.Points)[0], ",")[1]
	flatY := strings.Split(strings.Fields(flat)[0], ",")[1]
	assert.Equal(t, flatY, sampleY, "scales diverge")
}

func TestPlotClosesAreaAcrossFullWidth(t *testing.T) {
	t.Parallel()

	got := Plot([]float64{1, 2}, 300, 100, 4, "f", "s")
	assert.True(t, strings.HasPrefix(got.Area, "0.0,100.0 "),
		"area must start at the baseline, got %q", got.Area)
	assert.True(t, strings.HasSuffix(got.Area, " 300.0,100.0"),
		"area must close at the far baseline, got %q", got.Area)
}

func TestDivergingMirrorsAboutCentre(t *testing.T) {
	t.Parallel()

	got := Diverging([]float64{10}, []float64{10}, 1, 100, 110, 55, 10, 4)
	require.Len(t, got, 1)
	assert.Equal(t, got[0].DownH, got[0].UpH, "equal values must give equal heights")
	assert.Equal(t, "0.0", got[0].UpY, "full-scale bars should span centre to edge")
	assert.Equal(t, "55.0", got[0].DownY, "full-scale bars should span centre to edge")
}

func TestBarsStackUpperOnLower(t *testing.T) {
	t.Parallel()

	got := Bars([]float64{5}, []float64{5}, 1, 100, 100, 10, 4)
	require.Len(t, got, 1)
	assert.Equal(t, "50.0", got[0].LowerY, "upper must sit directly on lower")
	assert.Equal(t, "0.0", got[0].UpperY, "upper must sit directly on lower")
}

// Degenerate inputs are the common case on a fresh install: no history yet, a
// scale set with no runners, a cluster with metrics-server missing. None of
// them may produce NaN, which would render as literal "NaN" in the markup.
func TestDegenerateInputsProduceNoNaN(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"stacked no samples": join(Stacked(nil, []Band{{}}, 100, 100, 0, 10)),
		"stacked zero max":   join(Stacked([][]float64{{1}}, []Band{{}}, 100, 100, 0, 0)),
		"plot no samples":    Plot(nil, 100, 100, 10, "", "").Points,
		"plot zero max":      Plot([]float64{1}, 100, 100, 0, "", "").Points,
		"plot single sample": Plot([]float64{1}, 100, 100, 10, "", "").Points,
		"flatline zero max":  FlatLine(1, 100, 100, 0),
		"percent zero total": Percent(1, 0),
		"label zero viewbox": LabelTopPx(1, 0, 10),
	}
	for name, got := range cases {
		assert.NotContains(t, got, "NaN", name)
		assert.NotContains(t, got, "Inf", name)
	}
}

func TestSingleSamplePinsToOrigin(t *testing.T) {
	t.Parallel()

	got := Plot([]float64{5}, 300, 100, 10, "", "")
	assert.True(t, strings.HasPrefix(got.Points, "0.0,"),
		"single sample should pin to x=0, got %q", got.Points)
}

func TestPercentClamps(t *testing.T) {
	t.Parallel()

	// Usage above request is normal and must not overflow its container bar.
	assert.Equal(t, "100.0", Percent(30, 10), "over-request usage")
	assert.Equal(t, "0.0", Percent(-1, 10), "negative usage")
}

func TestGridSpansZeroToMax(t *testing.T) {
	t.Parallel()

	got := Grid(4, 1000, 230, 12, 40, 212, f0)
	require.Len(t, got, 5)
	assert.Equal(t, "0", got[0].Label, "grid should run 0..max")
	assert.Equal(t, "40", got[4].Label, "grid should run 0..max")
	assert.Equal(t, "230.0", got[0].Y, "grid should span baseline to inset")
	assert.Equal(t, "12.0", got[4].Y, "grid should span baseline to inset")
}

func TestGridRulesSpanTheGivenWidth(t *testing.T) {
	t.Parallel()

	// The rule has to end where the plot does, whatever width the caller draws
	// on — a rule that stops short leaves its label pointing at nothing.
	got := Grid(2, 640, 120, 6, 10, 120, f0)
	require.Len(t, got, 3)
	for i, g := range got {
		assert.Equal(t, "640.0", g.X2, "rule %d should span the chart width", i)
	}
}

func join(areas []Area) string {
	var sb strings.Builder
	for _, a := range areas {
		sb.WriteString(a.Points)
	}
	return sb.String()
}
