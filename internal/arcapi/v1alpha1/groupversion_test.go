package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOrgFromConfigURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"org", "https://github.com/WindKube", "WindKube"},
		{"org trailing slash", "https://github.com/WindKube/", "WindKube"},
		{"repo scoped", "https://github.com/WindKube/web-api", "WindKube"},
		{"enterprise", "https://github.com/enterprises/acme", "acme"},
		{"ghes", "https://ghe.example.com/WindKube", "WindKube"},
		{"no scheme", "github.com/WindKube", "WindKube"},
		{"empty", "", ""},
		{"host only", "https://github.com", ""},
		{"whitespace", "  https://github.com/WindKube  ", "WindKube"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, OrgFromConfigURL(tc.in), "OrgFromConfigURL(%q)", tc.in)
		})
	}
}

func TestWorkflowFileFromRef(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"branch ref", "WindKube/web-api/.github/workflows/ci.yml@refs/heads/main", "ci.yml"},
		{"tag ref", "WindKube/web-api/.github/workflows/release.yml@refs/tags/v1.2.3", "release.yml"},
		{"no ref suffix", "WindKube/web-api/.github/workflows/e2e.yml", "e2e.yml"},
		{"bare filename", "ci.yml", "ci.yml"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, WorkflowFileFromRef(tc.in), "WorkflowFileFromRef(%q)", tc.in)
		})
	}
}

func TestMaxRunnersValueTreatsNilAndMaxIntAsUnbounded(t *testing.T) {
	t.Parallel()

	// A nil maxRunners means unbounded, and the controller expands it to
	// MaxInt32 before the listener ever sees it. Both must read as unlimited
	// or the dashboard draws a capacity line at two billion.
	_, ok := (AutoscalingRunnerSetSpec{}).MaxRunnersValue()
	assert.False(t, ok, "nil maxRunners should report unbounded")

	unbounded := UnboundedRunners
	_, ok = (AutoscalingRunnerSetSpec{MaxRunners: &unbounded}).MaxRunnersValue()
	assert.False(t, ok, "MaxInt32 maxRunners should report unbounded")

	thirty := 30
	got, ok := (AutoscalingRunnerSetSpec{MaxRunners: &thirty}).MaxRunnersValue()
	require.True(t, ok)
	assert.Equal(t, 30, got)
}

func TestHasJobIsTheBusyTest(t *testing.T) {
	t.Parallel()

	// A runner whose pod is Running but which holds no job is idle, not busy.
	idle := &EphemeralRunner{Status: EphemeralRunnerStatus{Phase: EphemeralRunnerPhaseRunning}}
	assert.False(t, idle.HasJob(), "a Running runner with no jobId must not count as busy")

	busy := &EphemeralRunner{Status: EphemeralRunnerStatus{
		Phase: EphemeralRunnerPhaseRunning,
		JobID: "abc-123",
	}}
	assert.True(t, busy.HasJob(), "a runner holding a jobId must count as busy")
}

func TestEffectivePhaseFallsBackToLegacyState(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "Running", (AutoscalingRunnerSetStatus{Phase: "Running"}).EffectivePhase())
	// Controllers older than 0.14.0 populate state, not phase.
	assert.Equal(t, "Running", (AutoscalingRunnerSetStatus{State: "Running"}).EffectivePhase(),
		"legacy state ignored")
}
