package api

import (
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"arc-ui/internal/telemetry"
)

// recovery turns a panic in a handler into a 500 and a Sentry event, rather
// than taking the whole process down with it.
func recovery(log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}

			// A broken pipe is the client hanging up, not a bug. It reaches
			// here as a panic from the write, and reporting it would fill
			// Sentry with noise every time someone closes a dashboard tab.
			if isBrokenPipe(rec) {
				log.Debug().Str("path", c.Request.URL.Path).Msg("client hung up mid-response")
				c.Abort()
				return
			}

			err, ok := rec.(error)
			if !ok {
				err = errors.New("panic in handler")
			}
			log.Error().Err(err).
				Str("path", c.Request.URL.Path).
				Interface("panic", rec).
				Msg("recovered from panic")
			telemetry.CaptureError(err, map[string]string{"path": c.Request.URL.Path})

			if !c.Writer.Written() {
				c.String(http.StatusInternalServerError, "internal error")
			}
			c.Abort()
		}()

		c.Next()
	}
}

func isBrokenPipe(rec any) bool {
	ne, ok := rec.(*net.OpError)
	if !ok {
		return false
	}
	var se *os.SyscallError
	if !errors.As(ne.Err, &se) {
		return false
	}
	msg := strings.ToLower(se.Error())
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset by peer")
}

// accessLog records one line per request.
//
// SSE streams get a line when they open as well as when they close, because a
// stream that stays open for an hour would otherwise produce no log line at all
// until it ended — exactly backwards from what you want when debugging why a
// dashboard is not updating. The closing line carries how long it was open.
func accessLog(log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		start := time.Now()

		if isStream(path) {
			log.Info().Str("path", path).Str("ip", c.ClientIP()).Msg("stream opened")
			c.Next()
			log.Info().Str("path", path).Dur("open", time.Since(start)).Msg("stream closed")
			return
		}

		c.Next()

		status := c.Writer.Status()
		event := log.Debug()
		switch {
		case status >= http.StatusInternalServerError:
			event = log.Error()
		case status >= http.StatusBadRequest:
			event = log.Warn()
		}
		event.
			Str("method", c.Request.Method).
			Str("path", path).
			Int("status", status).
			Dur("took", time.Since(start)).
			Msg("request")
	}
}

func isStream(path string) bool {
	return path == "/stream" || strings.HasPrefix(path, "/stream/")
}
