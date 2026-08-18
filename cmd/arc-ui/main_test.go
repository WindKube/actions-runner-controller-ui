package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/WindKube/actions-runner-controller-ui/internal/version"
)

func TestHandlerRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "healthz reports ok",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
			wantBody:   "ok\n",
		},
		{
			name:       "version reports the uninjected build identity",
			method:     http.MethodGet,
			path:       "/version",
			wantStatus: http.StatusOK,
			wantBody:   "arc-ui dev (commit unknown, built unknown, " + goVersionLine() + ")\n",
		},
		{
			name:       "unknown path is a 404",
			method:     http.MethodGet,
			path:       "/nope",
			wantStatus: http.StatusNotFound,
		},
		{
			// ServeMux's method-prefixed patterns ("GET /healthz") reject other
			// verbs themselves; assert that rather than assume it.
			name:       "wrong method is rejected",
			method:     http.MethodPost,
			path:       "/healthz",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	handler := newHandler()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantBody != "" && rec.Body.String() != tt.wantBody {
				t.Errorf("body = %q, want %q", rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// The graceful-shutdown path is the part most likely to regress into a hang, and
// a hang in a Kubernetes deployment means a 30s SIGKILL wait on every rollout.
func TestRunShutsDownOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	done := make(chan error, 1)
	go func() {
		// Port 0 lets the kernel pick a free port, so parallel tests cannot
		// collide on a hardcoded one.
		done <- run(ctx, "127.0.0.1:0", logger)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run() = %v, want nil on a clean shutdown", err)
		}
	case <-time.After(shutdownGrace + 5*time.Second):
		t.Fatal("run() did not return after its context was cancelled")
	}
}

func TestRunReportsAnUnusableAddress(t *testing.T) {
	t.Parallel()

	// Occupy a port, then hand run the same one so ListenAndServe fails
	// immediately. This is the branch that must surface an error rather than
	// silently log and keep the process alive.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not reserve a port: %v", err)
	}
	defer occupied.Close()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	if err := run(context.Background(), occupied.Addr().String(), logger); err == nil {
		t.Error("run() = nil, want an error when the address is already in use")
	}
}

// goVersionLine mirrors what version.Current derives from the running toolchain,
// so the /version assertion does not hardcode a Go release or a GOARCH.
func goVersionLine() string {
	info := version.Current()

	return info.GoVersion + ", " + info.Platform
}
