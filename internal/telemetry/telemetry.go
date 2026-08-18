// Package telemetry wires error tracking. Sentry is optional: an empty DSN
// disables it and every entry point stays valid, so callers never have to
// branch on whether telemetry is configured.
package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"

	"arc-ui/internal/config"
)

// FlushTimeout bounds how long shutdown waits for Sentry to drain.
const FlushTimeout = 2 * time.Second

// Setup initialises error tracking and returns a shutdown func. The shutdown
// func is never nil, even when Setup returns an error, so callers can always
// defer it unconditionally.
func Setup(cfg config.Config, version string) (func(context.Context) error, error) {
	if cfg.SentryDSN == "" {
		return noop, nil
	}

	// The SDK treats a zero sample rate as "unset" and silently promotes it to
	// 1.0. Treat a non-positive rate as the operator asking for Sentry to be
	// off, which is what they almost certainly meant.
	if cfg.SentrySampleRate <= 0 {
		return noop, nil
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:         cfg.SentryDSN,
		Environment: cfg.SentryEnvironment,
		Release:     "arc-ui@" + version,
		SampleRate:  cfg.SentrySampleRate,
	})
	if err != nil {
		return noop, fmt.Errorf("init sentry: %w", err)
	}

	return func(context.Context) error {
		sentry.Flush(FlushTimeout)
		return nil
	}, nil
}

func noop(context.Context) error { return nil }

// CaptureError reports an error raised outside an HTTP request, where there is
// no per-request hub to inherit. Safe to call when Sentry is disabled.
func CaptureError(err error, tags map[string]string) {
	if err == nil {
		return
	}
	hub := sentry.CurrentHub().Clone()
	hub.ConfigureScope(func(scope *sentry.Scope) {
		for k, v := range tags {
			scope.SetTag(k, v)
		}
	})
	hub.CaptureException(err)
}
