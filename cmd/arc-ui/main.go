// Command arc-ui serves a read-only dashboard for GitHub Actions Runner
// Controller fleets.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"

	"arc-ui/internal/api"
	"arc-ui/internal/config"
	"arc-ui/internal/fleet"
	"arc-ui/internal/history"
	"arc-ui/internal/hub"
	"arc-ui/internal/k8s"
	"arc-ui/internal/listener"
	"arc-ui/internal/logging"
	"arc-ui/internal/metrics"
	"arc-ui/internal/store"
	"arc-ui/internal/telemetry"
	"arc-ui/internal/web"
)

// version is stamped at build time with -ldflags -X main.version=…
var version = "dev"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "arc-ui",
		Short:         "Read-only dashboard for GitHub Actions Runner Controller",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newServeCmd(), newVersionCmd(), newHealthcheckCmd())
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version and exit",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), version)
			return err
		},
	}
}

// newHealthcheckCmd exists because the runtime image is distroless: there is no
// shell and no curl, so a container healthcheck has to be the binary itself.
func newHealthcheckCmd() *cobra.Command {
	var addr string
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Probe a running arc-ui and exit non-zero if it is unhealthy",
		RunE: func(cmd *cobra.Command, _ []string) error {
			target := addr
			if target == "" {
				cfg, _, err := config.Load()
				if err != nil {
					return err
				}
				target = cfg.HTTPAddr
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			url := "http://" + localAddr(target) + "/healthz"
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("healthcheck %s: %w", url, err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("healthcheck %s: status %d", url, resp.StatusCode)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "", "address to probe (default: ARC_UI_HTTP_ADDR)")
	cmd.Flags().DurationVar(&timeout, "timeout", 3*time.Second, "probe timeout")
	return cmd
}

// localAddr rewrites a wildcard bind address into something dialable. The
// server listens on 0.0.0.0:8080, which is not an address you can connect to.
func localAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func newServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run the dashboard",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context())
		},
	}
}

// run wires every component together and serves until interrupted. The wiring
// is linear; splitting it into helpers that each take half the object graph
// reads worse than one ordered sequence.
//
//nolint:gocyclo // linear composition root; see above
func run(parent context.Context) error {
	cfg, warnings, err := config.Load()
	if err != nil {
		return err
	}

	log := logging.New(cfg.LogLevel, cfg.LogFormat)
	for _, w := range warnings {
		log.Warn().Msg(string(w))
	}
	log.Info().Str("version", version).Msg("arc-ui starting")

	shutdownTelemetry, err := telemetry.Setup(cfg, version)
	if err != nil {
		return err
	}
	// Detached from the signal context on purpose: this runs after that context
	// has been cancelled, and a flush bound to it would abort instantly — losing
	// exactly the errors that explain why the process is shutting down.
	//
	//nolint:contextcheck // deliberately detached; see above
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTelemetry(ctx); err != nil {
			log.Warn().Err(err).Msg("telemetry shutdown")
		}
	}()

	// SIGTERM cancels this; every long-running component hangs off it.
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- history store -----------------------------------------------------

	db, err := store.Open(ctx, cfg.DBPath, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Warn().Err(err).Msg("close store")
		}
	}()

	// --- cluster -----------------------------------------------------------

	clients, err := k8s.NewClients(cfg, log, "arc-ui/"+version)
	if err != nil {
		return err
	}

	collector := k8s.NewCollector(clients, cfg, log)
	if err := collector.Start(ctx); err != nil {
		return err
	}

	// --- pollers -----------------------------------------------------------

	events := hub.New()

	poller := metrics.NewPoller(clients.Metrics, cfg.Namespaces, cfg.ScrapeInterval, log, collector)
	go supervise(ctx, log, "metrics poller", poller.Run)

	scraper := listener.NewScraper(cfg.ListenerMetricsURL, cfg.ScrapeInterval, log, collector)
	go supervise(ctx, log, "listener scraper", scraper.Run)

	// Fleet changes are handed to the recorder, which persists each snapshot for
	// the history charts and then ticks the hub so connected browsers re-render.
	// What is guaranteed is ordering, not that every change gets both: the
	// worker applies snapshots one at a time in the order this callback observed
	// them, so the store never diffs a snapshot against a newer one, but a change
	// superseded while a write is in flight is dropped whole — neither persisted
	// nor broadcast — because the snapshot that replaced it is about to be
	// recorded and ticked in its place. See snapshotRecorder for why that trade
	// is the right one.
	//
	// The tick is sent after the write returns. That ordering is all it is: it
	// announces that a newer snapshot exists, not that any particular chart now
	// shows it. The tick carries no fleet data — each stream re-renders from
	// the collector's current snapshot, which may already be newer than
	// anything written — and it goes out even when the write failed,
	// deliberately, so a broken store does not freeze the dashboard. Whether a
	// history chart contains the sample depends on the tier its range resolves
	// to: every tier coarser than raw only gains it after compaction has rolled
	// the completed buckets up into it.
	//
	// The snapshot is taken here rather than in the worker: this runs the
	// instant the debounce fires, so its timestamp is when the change was
	// observed rather than whenever the write queue got to it, which is what
	// both the store's integration interval and the hub's liveness indicator
	// mean by "at". Everything after the hand-off is the worker's problem,
	// which is what keeps the slow half — the write — off the collector's
	// notifier goroutine this callback runs on.
	recorder := newSnapshotRecorder(
		db.RecordSnapshot,
		events.Broadcast,
		func(err error) {
			if err != nil {
				log.Warn().Err(err).Msg("record snapshot")
			}
			collector.SetSource(storeSource(err, time.Now()))
		},
	)
	recorder.start(ctx)

	cancelWatch := collector.OnChange(func() { recorder.enqueue(collector.Snapshot()) })
	defer cancelWatch()

	go runCompactor(ctx, db, retentionFrom(cfg), log)

	// --- web ---------------------------------------------------------------

	assets, err := web.NewAssets()
	if err != nil {
		return err
	}

	handler := &web.Handler{
		Builder: &web.Builder{
			Fleet:    collector,
			History:  history.New(db),
			Version:  version,
			Interval: cfg.ScrapeInterval,
			CSS:      assets.CSS(),
			JS:       assets.JS(),
		},
		Hub:       events,
		Log:       log,
		Events:    eventSource{collector},
		Heartbeat: cfg.ScrapeInterval,
		Streams:   web.NewStreamRegistry(),
	}

	server := api.New(api.Options{
		Config:  cfg,
		Log:     log,
		Handler: handler,
		Assets:  assets,
		Ready: map[string]api.Checker{
			// HasSynced, never WaitForCacheSync: the latter blocks, and a probe
			// that blocks reads to the kubelet as a probe that failed.
			"informers": func(context.Context) error {
				if !collector.HasSynced() {
					return errors.New("informer caches still syncing")
				}
				return nil
			},
			"store": db.Ping,
		},
	})

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Start() }()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		log.Info().Msg("signal received; shutting down")
	}

	// Detached from ctx, which is already cancelled: the drain has to outlive
	// the signal that started it.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(parent),
		cfg.PreStopDelay+cfg.ShutdownTimeout+5*time.Second)
	defer cancel()

	return server.Shutdown(shutdownCtx)
}

// storeSource turns a write outcome into the health-strip verdict. A nil error
// is a write that worked, and is what lets the row go green again.
//
// The two failures both land on the store row, since that is the only source
// the history store has. The reason has to tell them apart: an orphaned
// recorder drops its snapshots before any write is attempted, so calling that a
// failing write blames the store for something it never did.
func storeSource(err error, now time.Time) fleet.Source {
	if err == nil {
		return fleet.Source{Name: fleet.SourceStore, Available: true, CheckedAt: now}
	}
	reason := "writes failing"
	if errors.Is(err, errRecorderNotStarted) {
		reason = "snapshots are not being recorded"
	}
	return fleet.Source{
		Name: fleet.SourceStore, Available: false,
		Reason: reason, CheckedAt: now,
	}
}

// snapshotRecorder persists fleet snapshots and wakes connected browsers, one
// snapshot at a time and always in the order the changes were observed.
//
// Both halves of that are load-bearing. enqueue runs on the collector's
// notifier goroutine, where a slow subscriber delays every other one, so it
// must not block. But the obvious way to achieve that — detach a goroutine per
// change — is wrong: Store.RecordSnapshot diffs every snapshot against the
// previous one it saw, so with two writes in flight "previous" becomes
// whichever goroutine won the store's mutex rather than whichever snapshot is
// chronologically older. The CPU/memory integration interval then comes out
// zero or negative, and churn and phase transitions are diffed against a
// future fleet, inventing runners that were never created or terminated. A
// write only has to outlast the collector's debounce window for that to
// happen, and the churn bursts that make writes slow are exactly the ones that
// fire changes fastest.
//
// So enqueue parks the snapshot and returns; a single worker records them in
// turn. Only the newest pending snapshot survives — one superseded before it
// was ever written describes a fleet the store is about to be told about
// anyway, and replaying it would only diff against state already moved past.
type snapshotRecorder struct {
	record    func(context.Context, fleet.Snapshot) error
	broadcast func(time.Time)
	// onResult reports the outcome of every write attempt, nil included. A
	// store that recovers has to be able to say so: nothing else in the process
	// revisits that verdict, so reporting only failures would leave the health
	// strip blaming the store until restart.
	onResult func(error)

	// started is latched by start, synchronously, before the worker goroutine
	// is launched. It distinguishes "nobody ever asked for a worker" — a wiring
	// mistake enqueue must not swallow — from "the worker goroutine has not
	// been scheduled yet", which is ordinary and must stay silent. Latching it
	// inside run instead would collapse the two.
	started  atomic.Bool
	orphaned sync.Once
	// stopped closes when the worker returns, so a caller (today only the
	// tests) can tell a cancelled recorder has finished the write it was in
	// the middle of.
	stopped chan struct{}

	mu      sync.Mutex
	pending *fleet.Snapshot
	// wake carries no data; the snapshot itself lives under mu. Capacity one,
	// so a burst of changes during a single write collapses into one wakeup.
	wake chan struct{}
}

// errRecorderNotStarted reports a recorder nobody is draining.
var errRecorderNotStarted = errors.New("snapshot recorder worker was never started; snapshots are being dropped")

func newSnapshotRecorder(
	record func(context.Context, fleet.Snapshot) error,
	broadcast func(time.Time),
	onResult func(error),
) *snapshotRecorder {
	return &snapshotRecorder{
		record:    record,
		broadcast: broadcast,
		onResult:  onResult,
		stopped:   make(chan struct{}),
		wake:      make(chan struct{}, 1),
	}
}

// start launches the worker. Callers must use it rather than starting run
// themselves: enqueue neither blocks nor fails, so a recorder whose worker was
// never started drops every snapshot in silence — no history rows, no live
// updates, no error. start latches that a worker was asked for, and enqueue
// reports the recorders where one never was.
//
// It must be called at most once; run closes stopped on its way out.
func (r *snapshotRecorder) start(ctx context.Context) {
	r.started.Store(true)
	go r.run(ctx)
}

// enqueue makes snap the pending snapshot and returns without blocking, even
// when no worker is draining.
func (r *snapshotRecorder) enqueue(snap fleet.Snapshot) {
	if !r.started.Load() {
		// Once is enough to be loud: the condition is permanent — start is
		// never called late — and the source flip it triggers leaves the
		// history panel showing the store as unavailable until restart.
		r.orphaned.Do(func() { r.onResult(errRecorderNotStarted) })
	}

	r.mu.Lock()
	// Keep whichever snapshot is newer, not whichever call arrived last.
	// Nothing the recorder owns serialises its callers, so two of them can
	// hand over out of order, and letting the older one win here would put it
	// in front of the newer one on the way to the store — reintroducing the
	// out-of-order application the worker exists to prevent.
	//
	// It covers exactly one interleaving: both snapshots pending at the same
	// time, the older arriving second. It is not general ordering between
	// callers — an older snapshot handed over after the worker has already
	// claimed the newer one finds pending empty, and is recorded behind it.
	// Today there is a single caller (the collector's notifier goroutine, which
	// never hands over a snapshot older than the one before it), so the
	// comparison never rejects anything.
	if r.pending == nil || !snap.At.Before(r.pending.At) {
		r.pending = &snap
	}
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// take claims the pending snapshot, if there still is one.
func (r *snapshotRecorder) take() (fleet.Snapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.pending == nil {
		return fleet.Snapshot{}, false
	}
	snap := *r.pending
	r.pending = nil
	return snap, true
}

// run records pending snapshots until ctx ends, then closes stopped. Enter it
// through start, which is what tells enqueue somebody is draining.
//
// Each broadcast follows its own write, and goes out whether that write
// succeeded or failed — the fleet changed either way, and a failing store must
// not freeze the dashboard. The ordering is the whole of it: a tick says a
// newer snapshot exists, not that a chart fetched after it shows that
// snapshot's sample.
func (r *snapshotRecorder) run(ctx context.Context) {
	defer close(r.stopped)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		}

		// A wakeup can outlive its snapshot: enqueue publishes the snapshot
		// before signalling, so a second enqueue can leave a spare token behind
		// one that has already been claimed.
		snap, ok := r.take()
		if !ok {
			continue
		}

		if err := r.record(ctx, snap); ctx.Err() == nil {
			r.onResult(err)
		}
		r.broadcast(snap.At)
	}
}

// supervise runs a long-lived loop and reports why it stopped. Cancellation is
// the expected exit at shutdown, so it is not worth an error line.
func supervise(ctx context.Context, log zerolog.Logger, name string, run func(context.Context) error) {
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error().Err(err).Msg(name + " stopped")
	}
}

// compactInterval is how often roll-ups and retention run.
//
// It is a minute rather than something leisurely because the store answers a
// range query from exactly one tier — the coarsest that resolves the requested
// bucket width — and the default one-hour view resolves to the one-minute
// tier. Compacting every ten minutes would leave that chart visibly missing its
// last ten minutes. Each pass is incremental, so running it often is cheap.
const compactInterval = time.Minute

// runCompactor rolls samples up and applies retention until ctx ends.
func runCompactor(ctx context.Context, db *store.Store, ret store.Retention, log zerolog.Logger) {
	ticker := time.NewTicker(compactInterval)
	defer ticker.Stop()

	for {
		if err := db.Compact(ctx, time.Now(), ret); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("compaction failed; retrying next tick")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func retentionFrom(cfg config.Config) store.Retention {
	return store.Retention{
		RunnerRaw: cfg.RetentionRunnerRaw,
		ScopeRaw:  cfg.RetentionScopeRaw,
		Scope1m:   cfg.RetentionScope1m,
		Scope5m:   cfg.RetentionScope5m,
		Scope1h:   cfg.RetentionScope1h,
	}
}

// eventSource adapts the collector's pod-event lookup to what the views need.
//
// The pod UID is the point: it pins the field selector to this exact pod, so a
// recycled runner name cannot surface a dead pod's events as if they were this
// runner's.
type eventSource struct{ c *k8s.Collector }

func (e eventSource) Events(ctx context.Context, r fleet.Runner) ([]fleet.Event, error) {
	return e.c.EventsForPod(ctx, r.Namespace, r.Name, types.UID(r.PodUID))
}
