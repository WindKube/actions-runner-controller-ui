// Command arc-ui serves the read-only Actions Runner Controller web UI.
//
// SCAFFOLDING: the real UI is not here yet. This exists so the CI, release and
// container pipelines have something genuine to lint, test, cross-compile and
// run — an image whose entrypoint exits immediately proves nothing about the
// build. When the UI lands, only this file's body changes; the module path,
// binary name and internal/version contract stay put.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/WindKube/actions-runner-controller-ui/internal/version"
)

const (
	// shutdownGrace bounds how long in-flight requests get to finish after a
	// SIGTERM. Kubernetes sends SIGTERM then SIGKILLs after
	// terminationGracePeriodSeconds (30s by default), so stay well under it.
	shutdownGrace = 10 * time.Second

	// readHeaderTimeout caps how long a client may take to send its request
	// headers. Without it a peer can hold a connection — and its goroutine —
	// open indefinitely, which is both a slowloris vector and a gosec G112
	// finding.
	readHeaderTimeout = 10 * time.Second
)

// main does nothing but choose an exit code. os.Exit skips the entire defer
// stack, so it has to sit in a frame that owns no defers — every bit of setup
// and teardown belongs in cli, one level down.
func main() {
	os.Exit(cli())
}

// cli parses flags, wires up signals and logging, and returns the process exit
// code.
func cli() int {
	addr := flag.String("addr", ":8080", "host:port the HTTP server listens on")
	showVersion := flag.Bool("version", false, "print the build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.Current())

		return 0
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Signals are wired here rather than in run so that run's lifecycle is
	// driven purely by its context — which is what makes it substitutable in a
	// test.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *addr, logger); err != nil {
		logger.Error("server stopped", "error", err)

		return 1
	}

	return 0
}

// run owns the server lifecycle: it blocks until ctx is cancelled or the
// listener fails, then drains in-flight requests before returning.
func run(ctx context.Context, addr string, logger *slog.Logger) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           newHandler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	// Buffered, so the goroutine can always finish even if run has already
	// returned down the ctx.Done path and nobody is left reading.
	listenErr := make(chan error, 1)

	go func() {
		logger.Info("listening", "addr", addr, "version", version.Version)
		listenErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-listenErr:
		// Shutdown makes ListenAndServe return ErrServerClosed; that is a clean
		// stop, not a failure.
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("listen on %s: %w", addr, err)

	case <-ctx.Done():
		logger.Info("shutdown signal received, draining", "grace", shutdownGrace)

		// WithoutCancel, not Background: ctx is already cancelled, so deriving
		// the drain deadline from it directly would abort the drain instantly.
		// This keeps any values on ctx while dropping its cancellation.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("drain: %w", err)
		}

		return nil
	}
}

// newHandler builds the route table. Both routes are deliberately trivial and
// dependency-free: they are a container smoke test, not the application.
func newHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		// Nothing useful to do with a write error to an already-broken client.
		_, _ = fmt.Fprintln(w, "ok")
	})

	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintln(w, version.Current())
	})

	return mux
}
