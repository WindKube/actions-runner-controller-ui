package config

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Load reads the process environment, so nothing here may call t.Parallel() —
// t.Setenv forbids it, and two cases racing on the same variable would be
// unreproducible anyway.

// TestLoadRejectsUnusableShutdownDurations covers the two values that are accepted
// by the duration parser but meaningless to the shutdown sequence.
//
// Neither time.After nor context.WithTimeout rejects a negative duration; both fire
// at once. Unvalidated, ARC_UI_PRESTOP_DELAY=-1s skips the window that keeps the
// pod serving, and ARC_UI_SHUTDOWN_TIMEOUT=0s cuts in-flight requests while the
// store is still checkpointing. Both then look like a clean shutdown in the logs.
func TestLoadRejectsUnusableShutdownDurations(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{
			name:    "negative prestop delay",
			key:     "ARC_UI_PRESTOP_DELAY",
			value:   "-1s",
			wantErr: "ARC_UI_PRESTOP_DELAY",
		},
		{
			name:    "zero shutdown timeout",
			key:     "ARC_UI_SHUTDOWN_TIMEOUT",
			value:   "0s",
			wantErr: "ARC_UI_SHUTDOWN_TIMEOUT",
		},
		{
			name:    "negative shutdown timeout",
			key:     "ARC_UI_SHUTDOWN_TIMEOUT",
			value:   "-20s",
			wantErr: "ARC_UI_SHUTDOWN_TIMEOUT",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)

			_, _, err := Load()

			require.Error(t, err, "%s=%s must not start the process", tc.key, tc.value)
			assert.Contains(t, err.Error(), tc.wantErr,
				"the error must name the variable the operator has to fix")
		})
	}
}

// TestLoadAcceptsZeroPreStopDelay pins the asymmetry between the two checks.
// Zero is a legitimate pre-stop delay — it means "do not wait", which is right
// for a local run with no proxy in front — whereas a zero drain budget is not.
func TestLoadAcceptsZeroPreStopDelay(t *testing.T) {
	t.Setenv("ARC_UI_PRESTOP_DELAY", "0s")

	cfg, _, err := Load()

	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), cfg.PreStopDelay)
}

// TestLoadDefaultsAreSelfConsistent guards the envDefault tags themselves: a
// default that cannot pass Load's own validation would make the binary refuse
// to start with no configuration at all.
func TestLoadDefaultsAreSelfConsistent(t *testing.T) {
	cfg, _, err := Load()

	require.NoError(t, err, "the built-in defaults must satisfy Load's own checks")
	assert.Positive(t, cfg.ShutdownTimeout, "default ARC_UI_SHUTDOWN_TIMEOUT")
	assert.GreaterOrEqual(t, cfg.PreStopDelay, time.Duration(0), "default ARC_UI_PRESTOP_DELAY")
	assert.NotEmpty(t, cfg.DBPath, "default ARC_UI_DB_PATH")
	assert.Positive(t, cfg.KubeQPS, "default ARC_UI_KUBE_QPS")
	assert.Positive(t, cfg.KubeBurst, "default ARC_UI_KUBE_BURST")
	assert.Positive(t, cfg.JobSampleResolution, "default ARC_UI_JOB_SAMPLE_RESOLUTION")
	// Per-job usage defaults to the same window as the job rows it belongs to,
	// so a job listed in the table always has a chart that can be drawn.
	assert.Equal(t, cfg.RetentionScope5m, cfg.RetentionJobSamples,
		"job samples should default to the job observations' own window")
}

// TestAllNamespaces documents the empty-slice sentinel, which normalizeNamespaces
// is careful to preserve: a single blank entry is the documented way to say
// "watch everything", and it must not collapse into a namespace literally named "".
func TestAllNamespaces(t *testing.T) {
	t.Setenv("ARC_UI_NAMESPACES", " ")

	cfg, _, err := Load()

	require.NoError(t, err)
	assert.Empty(t, cfg.Namespaces, "a blank ARC_UI_NAMESPACES means all namespaces")
}

// TestLoadRejectsRelativeListenerMetricsPath ensures the path is absolute,
// preventing relative paths like "metrics" from producing malformed URLs when
// the collector constructs listener endpoints from pod IPs.
func TestLoadRejectsRelativeListenerMetricsPath(t *testing.T) {
	t.Setenv("ARC_UI_LISTENER_METRICS_PATH", "metrics")

	_, _, err := Load()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ARC_UI_LISTENER_METRICS_PATH")
	assert.Contains(t, err.Error(), "must begin with /")
}

// The bucket width is divided by to floor a timestamp. Unlike the retention
// windows, zero is not a "keep forever" escape hatch here — it is a panic.
func TestLoadRejectsAnUnusableJobSampleResolution(t *testing.T) {
	for _, v := range []string{"0s", "-1m", "500ms"} {
		t.Run(v, func(t *testing.T) {
			t.Setenv("ARC_UI_JOB_SAMPLE_RESOLUTION", v)

			_, _, err := Load()
			require.Error(t, err, "ARC_UI_JOB_SAMPLE_RESOLUTION=%s should be rejected", v)
			assert.Contains(t, err.Error(), "ARC_UI_JOB_SAMPLE_RESOLUTION")
		})
	}
}

// A bucket finer than the scrape interval buys nothing and costs rows, so it
// is worth a word rather than a silent acceptance.
func TestLoadWarnsWhenJobSamplesAreFinerThanTheScrape(t *testing.T) {
	t.Setenv("ARC_UI_SCRAPE_INTERVAL", "30s")
	t.Setenv("ARC_UI_JOB_SAMPLE_RESOLUTION", "5s")

	_, warns, err := Load()
	require.NoError(t, err, "a fine bucket is legal, just wasteful")

	var found bool
	for _, w := range warns {
		if strings.Contains(string(w), "ARC_UI_JOB_SAMPLE_RESOLUTION") {
			found = true
		}
	}
	assert.True(t, found, "expected a warning, got %v", warns)
}
