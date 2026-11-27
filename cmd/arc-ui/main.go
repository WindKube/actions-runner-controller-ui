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
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"

	"arc-ui/internal/alert"
	"arc-ui/internal/api"
	"arc-ui/internal/appmetrics"
	"arc-ui/internal/config"
	"arc-ui/internal/fleet"
	"arc-ui/internal/history"
	"arc-ui/internal/hub"
	"arc-ui/internal/k8s"
	"arc-ui/internal/logging"
	"arc-ui/internal/prom"
	"arc-ui/internal/promql"
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

// run wires every component together and serves until interrupted. The wiring is
// linear; splitting it into helpers that each take half the object graph reads
// worse than one ordered sequence.
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

	db, err := store.Open(ctx, cfg.DBPath, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Warn().Err(err).Msg("close store")
		}
	}()

	clients, err := k8s.NewClients(cfg, log, "arc-ui/"+version)
	if err != nil {
		return err
	}

	collector := k8s.NewCollector(clients, cfg, log)
	if err := collector.Start(ctx); err != nil {
		return err
	}

	events := hub.New()

	promClient, err := prom.New(promConfig(cfg))
	if err != nil {
		return err
	}
	queries := promql.Queries{Schema: promSchema(cfg)}
	streams := web.NewStreamRegistry()
	alerts := &alert.Engine{
		Rules:    alertRules(cfg, collector, db, promClient, queries),
		Interval: cfg.RefreshInterval,
		OnChange: func() { events.Broadcast(time.Now()) },
		Log:      log,
	}
	metrics := appmetrics.New(streams.Len, alerts.State)
	promClient.Observe(metrics.ObservePrometheus)

	prober := prom.NewProber(promClient, queries.Schema, cfg.PrometheusProbeInterval, collector, log)
	go supervise(ctx, log, "prometheus probe", prober.Run)

	live := promql.Live{Client: promClient, Queries: queries, Interval: cfg.RefreshInterval, Sink: collector, Log: log}
	go supervise(ctx, log, "live prometheus readings", live.Run)

	// Each fleet change is written to the job store, then ticks the hub so
	// browsers re-render. A change superseded while a write is in flight is
	// dropped for its successor; the tick goes out even when the write failed,
	// so a broken store never freezes the dashboard.
	recorder := startSnapshotRecorder(ctx,
		jobObserver(ctx, db, promClient, queries, cfg.PrometheusTimeout, usageDelays, log),
		events.Broadcast,
		func(err error) {
			if err != nil {
				log.Warn().Err(err).Msg("record snapshot")
			}
			collector.SetSource(storeSource(err, time.Now()))
		},
	)

	cancelWatch := collector.OnChange(func() { recorder.enqueue(collector.Snapshot()) })
	defer cancelWatch()

	go runPruner(ctx, db, cfg.JobRetention, log)
	go supervise(ctx, log, "alerts", alerts.Run)

	assets, err := web.NewAssets()
	if err != nil {
		return err
	}

	handler := &web.Handler{
		Builder: &web.Builder{
			Fleet:    collector,
			History:  promql.History{Rest: history.New(db), Client: promClient, Queries: queries},
			Version:  version,
			Interval: cfg.RefreshInterval,

			GitHubURL:         cfg.GitHubURL,
			GrafanaURL:        cfg.GrafanaURL,
			GrafanaDatasource: cfg.GrafanaDatasource,
			Alerts:            alerts,
			CSS:               assets.CSS(),
			JS:                assets.JS(),
		},
		Hub:       events,
		Log:       log,
		Events:    eventSource{collector},
		Heartbeat: cfg.RefreshInterval,
		Streams:   streams,
	}

	server := api.New(api.Options{
		Config:  cfg,
		Log:     log,
		Handler: handler,
		Assets:  assets,
		Metrics: metrics.Handler(),
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

	shutdownErr := server.Shutdown(shutdownCtx)

	// Join the recorder before returning. Its worker holds the store, and db.Close is
	// deferred further up this function — deferred calls run after this return, so
	// without the join a RecordSnapshot still in flight can write to a closed store.
	select {
	case <-recorder.stopped:
	case <-shutdownCtx.Done():
		log.Warn().Msg("snapshot recorder did not stop before the shutdown deadline")
	}

	return shutdownErr
}

// storeSource turns a write outcome into the health-strip verdict. A nil error is a
// write that worked, and is what lets the row go green again.
func storeSource(err error, now time.Time) fleet.Source {
	if err == nil {
		return fleet.Source{Name: fleet.SourceStore, Available: true, CheckedAt: now}
	}
	return fleet.Source{
		Name: fleet.SourceStore, Available: false,
		Reason: "writes failing", CheckedAt: now,
	}
}

// snapshotRecorder persists fleet snapshots and wakes connected browsers, one
// snapshot at a time and always in the order the changes were observed.
//
// Both halves are load-bearing. enqueue runs on the collector's notifier
// goroutine, where a slow subscriber delays every other one, so it must not block.
// But detaching a goroutine per change is wrong: Store.RecordSnapshot diffs every
// snapshot against the previous one it saw, so with two writes in flight
// "previous" becomes whichever goroutine won the store's mutex rather than
// whichever snapshot is older. The integration interval then comes out zero or
// negative, and churn is diffed against a future fleet, inventing runners that
// were never created. A write only has to outlast the debounce window for that to
// happen, and the churn bursts that make writes slow fire changes fastest.
//
// So enqueue parks the snapshot and returns; a single worker records them in turn.
// Only the newest pending snapshot survives.
type snapshotRecorder struct {
	record    func(context.Context, fleet.Snapshot) error
	broadcast func(time.Time)
	// onResult reports the outcome of every write attempt, nil included. A store that
	// recovers has to be able to say so: nothing else revisits that verdict, so
	// reporting only failures would leave the health strip blaming it until restart.
	onResult func(error)

	// stopped closes when the worker returns, so a caller can tell a cancelled
	// recorder has finished the write it was in the middle of.
	stopped chan struct{}

	mu      sync.Mutex
	pending *fleet.Snapshot
	// wake carries no data; the snapshot itself lives under mu. Capacity one,
	// so a burst of changes during a single write collapses into one wakeup.
	wake chan struct{}
}

// startSnapshotRecorder builds a recorder and launches its worker.
//
// Construction and start are one call because enqueue neither blocks nor fails: a
// recorder whose worker was never started would drop every snapshot in silence.
// It must be called at most once per recorder; run closes stopped on its way out.
func startSnapshotRecorder(
	ctx context.Context,
	record func(context.Context, fleet.Snapshot) error,
	broadcast func(time.Time),
	onResult func(error),
) *snapshotRecorder {
	r := &snapshotRecorder{
		record:    record,
		broadcast: broadcast,
		onResult:  onResult,
		stopped:   make(chan struct{}),
		wake:      make(chan struct{}, 1),
	}
	go r.run(ctx)
	return r
}

// enqueue makes snap the pending snapshot and returns without blocking.
func (r *snapshotRecorder) enqueue(snap fleet.Snapshot) {
	r.mu.Lock()
	// Keep whichever snapshot is newer, not whichever call arrived last. Nothing the
	// recorder owns serialises its callers, so two of them can hand over out of order,
	// and letting the older one win here would put it in front of the newer one on the
	// way to the store.
	//
	// It covers exactly one interleaving: both snapshots pending at the same time, the
	// older arriving second. Today there is a single caller, so the comparison never
	// rejects anything.
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

// run records pending snapshots until ctx ends, then closes stopped.
//
// Each broadcast follows its own write, and goes out whether that write succeeded
// or failed — the fleet changed either way, and a failing store must not freeze
// the dashboard.
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

// jobStore is what the observer writes to.
type jobStore interface {
	Observe(ctx context.Context, snap fleet.Snapshot) ([]store.JobRecord, error)
	SetUsage(ctx context.Context, id int, cpuSeconds, memPeak float64) error
}

// jobObserver records each snapshot's jobs, then reads closed jobs' usage from
// cAdvisor off the recorder's path so a slow Prometheus never delays a render.
// usageDelays are the waits before each read of a closed job's usage. The
// first gives remote-write and ingestion a few scrapes to catch up.
var usageDelays = []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute}

func jobObserver(ctx context.Context, db jobStore, c promql.InstantQuerier, q promql.Queries, timeout time.Duration, delays []time.Duration, log zerolog.Logger) func(context.Context, fleet.Snapshot) error {
	return func(octx context.Context, snap fleet.Snapshot) error {
		closed, err := db.Observe(octx, snap)
		if err != nil || len(closed) == 0 {
			return err
		}
		go func() {
			pending := closed
			for _, d := range delays {
				select {
				case <-ctx.Done():
					return
				case <-time.After(d):
				}
				pending = readUsage(ctx, db, c, q, timeout, pending, log)
				if len(pending) == 0 {
					return
				}
			}
			for _, j := range pending {
				log.Debug().Str("job", j.JobID).Msg("job usage unavailable; leaving it unset")
			}
		}()
		return nil
	}
}

// readUsage stores the usage of each job Prometheus can answer for and returns
// the rest.
func readUsage(ctx context.Context, db jobStore, c promql.InstantQuerier, q promql.Queries, timeout time.Duration, jobs []store.JobRecord, log zerolog.Logger) []store.JobRecord {
	var retry []store.JobRecord
	for _, j := range jobs {
		qctx, cancel := context.WithTimeout(ctx, timeout)
		cpu, mem, err := promql.JobUsage(qctx, c, q, j.Runner, j.StartedAt, j.FinishedAt)
		cancel()
		if err != nil {
			retry = append(retry, j)
			continue
		}
		if err := db.SetUsage(ctx, j.ID, cpu, mem); err != nil {
			log.Warn().Err(err).Str("job", j.JobID).Msg("store job usage")
		}
	}
	return retry
}

func alertRules(cfg config.Config, f alert.Snapshotter, db alert.FailureStore, q alert.Querier, queries promql.Queries) []alert.Rule {
	return []alert.Rule{
		alert.QueuedAtMax(f, cfg.AlertQueuedAtMaxFor),
		alert.ListenerDown(f, cfg.AlertListenerDownFor),
		alert.OOMKilled(f, db, cfg.AlertOOMWindow),
		alert.FailedRising(f, q, queries.FailedRunnersIncrease(cfg.AlertFailedRunnersWindow), cfg.AlertFailedRunnersRise, cfg.AlertFailedRunnersWindow),
		alert.StartupP95(f, q, queries.StartupP95(cfg.AlertStartupWindow), queries.Schema.ScaleSetLabel, cfg.AlertStartupP95, cfg.AlertStartupWindow),
	}
}

// runPruner applies job retention hourly until ctx ends.
func runPruner(ctx context.Context, db *store.Store, retention time.Duration, log zerolog.Logger) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		if n, err := db.Prune(ctx, time.Now().Add(-retention)); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("job retention failed; retrying next hour")
		} else if n > 0 {
			log.Info().Int("jobs", n).Msg("pruned jobs past retention")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func promConfig(cfg config.Config) prom.Config {
	return prom.Config{
		URL:                cfg.PrometheusURL,
		BearerToken:        cfg.PrometheusBearerToken,
		BearerTokenFile:    cfg.PrometheusBearerTokenFile,
		Username:           cfg.PrometheusUsername,
		Password:           cfg.PrometheusPassword,
		Headers:            cfg.PrometheusHeaders,
		CAFile:             cfg.PrometheusCAFile,
		InsecureSkipVerify: cfg.PrometheusInsecureSkipVerify,
		Timeout:            cfg.PrometheusTimeout,
		CacheTTL:           cfg.RefreshInterval,
	}
}

func promSchema(cfg config.Config) prom.Schema {
	return prom.Schema{
		ScaleSetLabel:    cfg.PrometheusScaleSetLabel,
		KSMScaleSetLabel: cfg.PrometheusKSMScaleSetLabel,
		RunnerNamespaces: cfg.Namespaces,
	}
}

// eventSource adapts the collector's pod-event lookup to what the views need.
//
// The pod UID is the point: it pins the field selector to this exact pod, so a
// recycled runner name cannot surface a dead pod's events as this runner's.
type eventSource struct{ c *k8s.Collector }

func (e eventSource) Events(ctx context.Context, r fleet.Runner) ([]fleet.Event, error) {
	return e.c.EventsForPod(ctx, r.Namespace, r.Name, types.UID(r.PodUID))
}
