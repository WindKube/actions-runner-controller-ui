// Package api is the HTTP surface: routing, health probes and the server
// lifecycle.
//
// It owns no dashboard logic. Rendering lives in internal/web, which is
// framework-agnostic and exposes plain net/http handlers; this package's job
// is to mount them, add the probes, and shut everything down in the right
// order.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"arc-ui/internal/config"
	"arc-ui/internal/web"
)

// Checker reports whether one dependency is usable. /readyz is assembled from
// these; liveness deliberately is not.
type Checker func(ctx context.Context) error

// Server owns the HTTP listener.
type Server struct {
	cfg     config.Config
	log     zerolog.Logger
	http    *http.Server
	streams *web.StreamRegistry

	// ready is flipped false at the start of shutdown so load balancers stop
	// routing to this pod before the listener closes.
	ready atomic.Bool

	// readyChecks are the dependencies /readyz aggregates.
	readyChecks map[string]Checker
}

// Options configures a Server.
//
// There is deliberately no Version here. Nothing this package serves reports
// one — /healthz is a bare "ok" and /readyz carries only the dependency
// verdicts — and the version the dashboard displays comes from
// web.Builder.Version. A field here would be a second place to set it that
// silently changes nothing.
type Options struct {
	Config  config.Config
	Log     zerolog.Logger
	Handler *web.Handler
	Assets  *web.Assets

	// Ready are the dependency checks /readyz aggregates, keyed by the name
	// reported in the response body.
	Ready map[string]Checker
}

// New builds the server and its routes.
func New(opts Options) *Server {
	s := &Server{
		cfg:         opts.Config,
		log:         opts.Log,
		streams:     opts.Handler.Streams,
		readyChecks: opts.Ready,
	}
	s.ready.Store(true)

	engine := s.engine(opts)

	s.http = &http.Server{
		Addr:              opts.Config.HTTPAddr,
		Handler:           engine,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// WriteTimeout is deliberately zero. Any non-zero value is a hard
		// deadline on the whole response, which for an SSE stream means the
		// connection is severed mid-dashboard on a timer. Individual handlers
		// bound their own work through the request context instead.
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	// Runs during Shutdown, in its own goroutine, before Shutdown returns.
	// Without it, Shutdown waits for every open stream to end on its own —
	// which for a dashboard left open on a wall display is never.
	s.http.RegisterOnShutdown(func() {
		if s.streams != nil {
			s.log.Info().Int("streams", s.streams.Len()).Msg("closing live streams")
			s.streams.CloseAll()
		}
	})

	return s
}

func (s *Server) engine(opts Options) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	engine := gin.New()
	engine.Use(recovery(s.log), accessLog(s.log))
	engine.RedirectTrailingSlash = true

	h := opts.Handler

	engine.GET("/healthz", s.healthz)
	engine.GET("/livez", s.healthz)
	engine.GET("/readyz", s.readyz)

	engine.GET("/", gin.WrapF(h.Index))
	engine.GET("/runnersets/:name", param(h.SetDetail))
	engine.GET("/runners/:name", param(h.RunnerDetail))

	engine.GET("/stream", gin.WrapF(h.StreamOverview))
	engine.GET("/stream/runnersets/:name", param(h.StreamSet))
	engine.GET("/stream/runners/:name", param(h.StreamRunner))

	engine.Any(web.AssetPrefix+"*filepath", gin.WrapH(
		http.StripPrefix(web.AssetPrefix, opts.Assets)))

	engine.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "not found")
	})

	return engine
}

// param adapts a handler that takes a path parameter to gin.
func param(fn func(http.ResponseWriter, *http.Request, string)) gin.HandlerFunc {
	return func(c *gin.Context) {
		fn(c.Writer, c.Request, c.Param("name"))
	}
}

// Start begins serving and returns once the listener is closed.
func (s *Server) Start() error {
	s.log.Info().Str("addr", s.cfg.HTTPAddr).Msg("http server listening")
	if err := s.http.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

// Shutdown drains the server.
//
// The order matters and is the whole reason this is not two lines. Readiness is
// withdrawn first and the process keeps serving for PreStopDelay, so that load
// balancers observe the failing probe and stop sending new requests before the
// listener disappears. Only then does Shutdown run, which stops accepting,
// fires the on-shutdown hook that closes every live stream, and waits for the
// remaining in-flight requests.
func (s *Server) Shutdown(ctx context.Context) error {
	s.ready.Store(false)
	s.log.Info().Dur("drain", s.cfg.PreStopDelay).Msg("readiness withdrawn; draining")

	select {
	case <-time.After(s.cfg.PreStopDelay):
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.ShutdownTimeout)
	defer cancel()

	if err := s.http.Shutdown(shutdownCtx); err != nil {
		// A timeout here means something is still holding a connection open.
		// Close is the blunt instrument that guarantees the process can exit
		// rather than being killed by the orchestrator's grace period.
		s.log.Warn().Err(err).Msg("graceful shutdown timed out; closing connections")
		if closeErr := s.http.Close(); closeErr != nil {
			return fmt.Errorf("close http server: %w", closeErr)
		}
		return nil
	}

	s.log.Info().Msg("http server stopped")
	return nil
}

// healthz is process-local and never touches the API server.
//
// Liveness must not depend on the cluster. Pointing it at readiness means an
// API server blip restarts the pod, which forces every informer to re-LIST from
// scratch — turning a transient upstream problem into a slower recovery than
// doing nothing at all.
func (s *Server) healthz(c *gin.Context) {
	c.String(http.StatusOK, "ok")
}

// readyz aggregates the dependency checks.
func (s *Server) readyz(c *gin.Context) {
	if !s.ready.Load() {
		c.String(http.StatusServiceUnavailable, "draining")
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	status := make(map[string]string, len(s.readyChecks))
	code := http.StatusOK
	for name, check := range s.readyChecks {
		if err := check(ctx); err != nil {
			status[name] = err.Error()
			code = http.StatusServiceUnavailable
			continue
		}
		status[name] = "ok"
	}

	c.JSON(code, gin.H{"ready": code == http.StatusOK, "checks": status})
}
