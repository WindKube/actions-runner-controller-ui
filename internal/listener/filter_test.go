package listener

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterExposition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "a collected family keeps its metadata and every sample",
			in: "# HELP gha_assigned_jobs Number of jobs assigned.\n" +
				"# TYPE gha_assigned_jobs gauge\n" +
				"gha_assigned_jobs{name=\"a\"} 12\n" +
				"gha_assigned_jobs{name=\"b\"} 3\n",
			want: "# HELP gha_assigned_jobs Number of jobs assigned.\n" +
				"# TYPE gha_assigned_jobs gauge\n" +
				"gha_assigned_jobs{name=\"a\"} 12\n" +
				"gha_assigned_jobs{name=\"b\"} 3\n",
		},
		{
			name: "the histograms go, and take their _bucket, _sum and _count series with them",
			in: "# HELP gha_job_execution_duration_seconds Histogram of job execution times.\n" +
				"# TYPE gha_job_execution_duration_seconds histogram\n" +
				"gha_job_execution_duration_seconds_bucket{name=\"a\",le=\"60\"} 120\n" +
				"gha_job_execution_duration_seconds_sum{name=\"a\"} 51234.5\n" +
				"gha_job_execution_duration_seconds_count{name=\"a\"} 400\n" +
				"gha_idle_runners{name=\"a\"} 12\n",
			want: "gha_idle_runners{name=\"a\"} 12\n",
		},
		{
			name: "the Go runtime families go too",
			in: "go_goroutines 41\n" +
				"go_memstats_alloc_bytes 1.234e+06\n" +
				"process_cpu_seconds_total 12.5\n" +
				"gha_busy_runners{name=\"a\"} 8\n",
			want: "gha_busy_runners{name=\"a\"} 8\n",
		},
		{
			name: "plain comments and blank lines carry nothing worth keeping",
			in:   "# a comment\n\n   \n#\n####\ngha_running_jobs{name=\"a\"} 1\n",
			want: "gha_running_jobs{name=\"a\"} 1\n",
		},
		{
			name: "a counter is recognised under both spellings",
			in: "gha_started_jobs_total{name=\"a\"} 941\n" +
				"gha_completed_jobs{name=\"a\"} 900\n",
			want: "gha_started_jobs_total{name=\"a\"} 941\n" +
				"gha_completed_jobs{name=\"a\"} 900\n",
		},
		{
			name: "metadata written without the customary space still travels with its family",
			in:   "#TYPE gha_max_runners gauge\n#TYPE go_goroutines gauge\ngha_max_runners{name=\"a\"} 50\n",
			want: "#TYPE gha_max_runners gauge\ngha_max_runners{name=\"a\"} 50\n",
		},
		{
			name: "leading whitespace does not hide which family a line belongs to",
			in:   "  gha_min_runners{name=\"a\"} 2\n\tgha_job_startup_duration_seconds_count{name=\"a\"} 9\n",
			want: "  gha_min_runners{name=\"a\"} 2\n",
		},
		{
			name: "a body that is not an exposition at all reaches the parser unchanged",
			in:   "<html>not prometheus at all</html>\n",
			want: "<html>not prometheus at all</html>\n",
		},
		{
			name: "a quoted UTF-8 name is passed through rather than guessed at",
			in:   "{\"job.duration_seconds\",name=\"a\"} 1\ngo_goroutines 41\n",
			want: "{\"job.duration_seconds\",name=\"a\"} 1\n",
		},
		{
			name: "a line the format does not explain is left for the parser to reject",
			in:   "gha_assigned_jobs{name=\"broken\" 12\nnot_a_sample_line\n",
			want: "gha_assigned_jobs{name=\"broken\" 12\nnot_a_sample_line\n",
		},
		{
			name: "the last line survives without a trailing newline",
			in:   "go_goroutines 41\ngha_desired_runners{name=\"a\"} 20",
			want: "gha_desired_runners{name=\"a\"} 20",
		},
		{
			name: "an empty body filters to an empty body",
			in:   "",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := filterExposition(strings.NewReader(tc.in))
			require.NoError(t, err, "filterExposition")
			assert.Equal(t, tc.want, string(got))
		})
	}
}

// TestFilterPreservesParse is the property the whole filter rests on: what the
// parser makes of a listener's body must not change because the body was
// filtered first.
func TestFilterPreservesParse(t *testing.T) {
	t.Parallel()

	want, _, err := parse(strings.NewReader(fixture))
	require.NoError(t, err, "parse of the raw fixture")

	kept, err := filterExposition(strings.NewReader(fixture))
	require.NoError(t, err, "filterExposition")

	got, _, err := parse(strings.NewReader(string(kept)))
	require.NoError(t, err, "parse of the filtered fixture")

	assert.Equal(t, want, got, "filtering changed what the fixture parses to")
}

// TestFilterCoversEveryCollectedFamily guards the one way this fix can rot: a
// metric family added to the parser but not to the filter is dropped on the way
// in, and reads on the dashboard as a listener that never exposed it.
func TestFilterCoversEveryCollectedFamily(t *testing.T) {
	t.Parallel()

	// Metrics has exactly one map per collected family, so a family added to
	// one and not the other shows up here rather than in production.
	assert.Equal(t, len(collectedFamilies), reflect.TypeOf(Metrics{}).NumField(),
		"collectedFamilies and Metrics disagree about how many families are collected")

	for _, name := range collectedFamilies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			body := fmt.Sprintf("# HELP %s Some help.\n# TYPE %s gauge\n%s{name=\"a\"} 1\n", name, name, name)
			got, err := filterExposition(strings.NewReader(body))
			require.NoError(t, err, "filterExposition")
			assert.Equal(t, body, string(got), "a collected family was filtered out")
		})
	}
}

// TestFilterLongLines covers a line longer than the read buffer, which arrives
// in pieces. The metric name is only in the first piece, so the rest of the line
// has to inherit its verdict — a body full of megabyte-labelled histogram series
// is otherwise kept from the second piece onwards.
func TestFilterLongLines(t *testing.T) {
	t.Parallel()

	huge := strings.Repeat("x", 3*filterBufBytes)

	dropped := fmt.Sprintf("gha_job_execution_duration_seconds_bucket{job_name=\"%s\",le=\"60\"} 1\n", huge)
	kept := fmt.Sprintf("gha_assigned_jobs{name=\"a\",repository=\"%s\"} 7\n", huge)

	got, err := filterExposition(strings.NewReader(dropped + kept))
	require.NoError(t, err, "filterExposition")
	assert.Equal(t, kept, string(got), "a line spanning several reads was not classified as one line")

	m, _, err := parse(strings.NewReader(string(got)))
	require.NoError(t, err, "parse")
	assert.Equal(t, map[string]float64{"a": 7}, m.AssignedJobs)
}

// TestFilterBudget pins the cap to what is kept rather than what is read: a
// listener serving far more than the cap is scraped fine as long as the series
// this package collects fit, and junk is still bounded because a line the filter
// cannot explain is kept.
func TestFilterBudget(t *testing.T) {
	t.Parallel()

	t.Run("a body many times the cap is fine when what we keep is small", func(t *testing.T) {
		t.Parallel()

		got, err := filterExposition(strings.NewReader(ignoredPadding(4*maxKeptBytes) + fixture))
		require.NoError(t, err, "filterExposition rejected a body it was meant to filter down")

		m, _, err := parse(strings.NewReader(string(got)))
		require.NoError(t, err, "parse")
		assert.Equal(t, map[string]int{"arc-linux-x64": 4, "arc-linux-arm64": 0}, m.QueueDepth())
	})

	t.Run("kept series over the cap are an error, not a truncated success", func(t *testing.T) {
		t.Parallel()

		_, err := filterExposition(strings.NewReader(keptPadding(maxKeptBytes + 1)))
		require.ErrorIs(t, err, errKeptTooLarge)
	})

	t.Run("exactly at the cap still succeeds", func(t *testing.T) {
		t.Parallel()

		body := keptPadding(maxKeptBytes)
		require.Len(t, body, maxKeptBytes, "padding must sit exactly on the cap")

		got, err := filterExposition(strings.NewReader(body))
		require.NoError(t, err, "a body exactly at the cap was rejected")
		assert.Len(t, got, maxKeptBytes)
	})

	t.Run("junk is bounded, because junk is kept", func(t *testing.T) {
		t.Parallel()

		_, err := filterExposition(strings.NewReader(strings.Repeat("<html>not prometheus</html>\n", maxKeptBytes/16)))
		require.ErrorIs(t, err, errKeptTooLarge)
	})
}

func TestFilterReadError(t *testing.T) {
	t.Parallel()

	want := errors.New("connection reset by peer")
	_, err := filterExposition(io.MultiReader(
		strings.NewReader("gha_assigned_jobs{name=\"a\"} 1\n"),
		errReader{want},
	))
	require.ErrorIs(t, err, want, "a body that failed mid-read was reported as a short one")
}

// errReader fails every read, standing in for a connection dropped mid-body.
type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// ignoredPadding builds n bytes of exposition the dashboard does not read: the
// job histogram, which is what a real listener's body is mostly made of.
func ignoredPadding(n int) string {
	var b strings.Builder
	b.Grow(n + 256)
	b.WriteString("# HELP gha_job_execution_duration_seconds Histogram of job execution times.\n")
	b.WriteString("# TYPE gha_job_execution_duration_seconds histogram\n")
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, "gha_job_execution_duration_seconds_bucket{name=\"arc-linux-x64\","+
			"repository=\"acme/service-%d\",job_name=\"build\",le=\"60\"} %d\n", i, i)
	}
	return b.String()
}

// keptPadding builds exactly n bytes of series the dashboard does read. Each
// line names its own scale set, so the result is as expensive to parse as it is
// to hold — which is what the cap is there to bound.
func keptPadding(n int) string {
	line := func(set string) string { return fmt.Sprintf("gha_idle_runners{name=%q} 1\n", set) }
	overhead := len(line(""))

	var b strings.Builder
	b.Grow(n)
	// Stop while there is still room for a whole line, so the last one can be
	// stretched to whatever is left and the body lands exactly on n.
	for i := 0; n-b.Len() >= 2*overhead; i++ {
		b.WriteString(line(fmt.Sprintf("pad-%08d", i)))
	}
	b.WriteString(line(strings.Repeat("z", n-b.Len()-overhead)))
	return b.String()
}
