// Package store is the dashboard's historian.
//
// metrics-server keeps no history at all — it holds roughly the last two
// scrapes and nothing else — and ARC keeps none either. Every "over time"
// chart in this dashboard exists only because this package samples the fleet
// and writes it down. That is the whole reason it exists.
//
// The hard constraint is volume. A 200-runner fleet sampled every five
// seconds for thirty days is on the order of two hundred million rows, which
// is not a thing to put in an embedded SQLite file on a PVC. So the store is
// tiered: per-runner samples live for minutes because the only view that
// reads them renders the last four; fleet and per-set samples are rolled up
// into progressively coarser buckets as they age, because a chart that draws
// ninety points from a thirty-day window cannot tell the difference.
//
// Everything here is best-effort. The dashboard boots and serves with the
// store broken, reporting it as a fleet.Source rather than failing a page, so
// no method in this package is on a critical path.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/rs/zerolog"
	_ "modernc.org/sqlite" // pure-Go driver, registered as "sqlite"

	"arc-ui/internal/fleet"
	"arc-ui/internal/store/ent"
)

// Scope names what a series is aggregated over.
type Scope string

// The three aggregation scopes. Fleet rows carry an empty scope ID; set and
// runner rows carry the set or runner name.
const (
	ScopeFleet  Scope = "fleet"
	ScopeSet    Scope = "set"
	ScopeRunner Scope = "runner"
)

// Tier names a storage resolution. Coarser tiers are derived from finer ones
// by compaction and outlive them.
type Tier string

// The four resolution tiers.
const (
	// TierRaw is one row per scrape. It is the only tier per-runner samples
	// ever reach, because the runner detail view renders four minutes.
	TierRaw Tier = "raw"
	Tier1m  Tier = "m1"
	Tier5m  Tier = "m5"
	Tier1h  Tier = "h1"
)

// Resolution is the bucket width of a tier. TierRaw has none: its width is
// whatever the scrape interval happens to be, which the store never assumes.
func (t Tier) Resolution() time.Duration {
	switch t {
	case Tier1m:
		return time.Minute
	case Tier5m:
		return 5 * time.Minute
	case Tier1h:
		return time.Hour
	default:
		return 0
	}
}

// rollupTiers is the compaction chain, finest first. Each entry names the tier
// it is derived from, so compaction is a fold over this slice.
var rollupTiers = []struct {
	from, to Tier
}{
	{TierRaw, Tier1m},
	{Tier1m, Tier5m},
	{Tier5m, Tier1h},
}

// Metric names one stored series.
type Metric string

// The stored metrics. Counts are whole numbers stored as floats because
// averaging them across a rollup bucket produces fractions, and rounding at
// write time would make a chart of a set that is half-busy look like a
// staircase.
const (
	MetricRunners Metric = "runners"
	MetricBusy    Metric = "busy"
	MetricIdle    Metric = "idle"
	MetricPending Metric = "pending"
	MetricFailed  Metric = "failed"
	// MetricQueued is only written when the ARC listener's metrics are
	// actually reachable. ARC ships with them disabled, and a stored zero
	// would be indistinguishable from "nothing is queued".
	MetricQueued Metric = "queued"
	// MetricCapacity is summed maxRunners, and is deliberately absent for an
	// unbounded set so the dashed max line is omitted rather than drawn at
	// zero. It is a series rather than a constant because people rescale sets
	// during the day and history must not be retroactively rewritten.
	MetricCapacity   Metric = "capacity"
	MetricCPUUsed    Metric = "cpu_used"
	MetricCPURequest Metric = "cpu_request"
	MetricMemUsed    Metric = "mem_used"
	MetricMemRequest Metric = "mem_request"
)

// Churn event kinds.
const (
	churnCreated    = "created"
	churnTerminated = "terminated"
)

// maxIntegrationGap bounds how long a gap between two snapshots may be before
// the store refuses to integrate CPU usage across it. Without this, a process
// that was paused for six hours would credit the job it was watching with six
// hours of its last observed CPU reading.
const maxIntegrationGap = 5 * time.Minute

// Point is one value at one instant. For a bucketed series the instant is the
// start of the bucket, not its middle: a chart drawn from bucket starts is
// honest about the fact that it has no idea what happened inside one.
type Point struct {
	At    time.Time
	Value float64
}

// Range describes a query window and the number of points wanted.
//
// Points is a request rather than a promise. The store buckets [From, To) into
// roughly that many buckets and returns the ones it has data for, so a store
// that has only been running ten minutes yields a short series rather than one
// padded out with invented zeros.
type Range struct {
	From, To time.Time
	Points   int
}

// RepoTotal is one repository's integrated consumption over a window.
type RepoTotal struct {
	Repository string
	Jobs       int
	// CPUSeconds and MemByteSeconds are integrals of the metrics-server
	// samples taken while a job was assigned, and they bound the truth from
	// neither side. A job short enough to die between two scrapes contributes
	// nothing at all, while a job that took over a persistent runner is
	// credited with the whole interval that straddles the handover, including
	// the part its predecessor ran. RecordSnapshot explains why the handover
	// is billed forwards.
	CPUSeconds     float64
	MemByteSeconds float64
}

// JobRecord is one observed workflow job.
type JobRecord struct {
	Runner     string
	Set        string
	Repository string
	Workflow   string
	Job        string
	RunID      int64
	StartedAt  time.Time
	// FinishedAt is zero while the job is still running.
	FinishedAt time.Time
	Succeeded  bool
	CPUSeconds float64
}

// Running reports whether the job had not finished when it was last observed.
func (j JobRecord) Running() bool { return j.FinishedAt.IsZero() }

// Phase is one contiguous stretch a runner spent in one fleet.State.
type Phase struct {
	Runner string
	Set    string
	Phase  string
	// StartedAt and EndedAt are both scrape-resolution observations, not exact
	// transitions: the true boundary lies somewhere in the interval before the
	// scrape that first saw the change.
	StartedAt time.Time
	EndedAt   time.Time
}

// Duration is how long the phase lasted, or zero if it is still open.
func (p Phase) Duration() time.Duration {
	if p.StartedAt.IsZero() || p.EndedAt.IsZero() || !p.EndedAt.After(p.StartedAt) {
		return 0
	}
	return p.EndedAt.Sub(p.StartedAt)
}

// Retention holds the per-tier windows. It mirrors the corresponding fields of
// config.Config; the store takes it as an argument rather than reading config
// so it can be compacted with a different policy in a test.
//
// A zero duration means "keep forever" for that tier, which is a deliberate
// escape hatch and not a default anyone should ship: the raw tiers are the
// ones that grow without bound.
type Retention struct {
	RunnerRaw, ScopeRaw, Scope1m, Scope5m, Scope1h time.Duration
}

// Stats reports what the store is costing, for the health strip.
type Stats struct {
	Path      string
	SizeBytes int64

	Samples     int64
	Jobs        int64
	Phases      int64
	ChurnEvents int64
	Failures    int64
	Rows        int64

	// Oldest is the timestamp of the oldest surviving sample in any tier,
	// which is the honest answer to "how far back can I look".
	Oldest time.Time
}

// FailureRecord is one persisted runner failure.
type FailureRecord struct {
	Runner string
	Set    string
	Reason string
	// Severe distinguishes a runner that will never work from one that merely
	// exited non-zero.
	Severe bool
	// At is when the failure was observed, not when it was recorded.
	At time.Time
}

// Store is the SQLite-backed history of the fleet.
type Store struct {
	path   string
	db     *sql.DB
	client *ent.Client
	log    zerolog.Logger

	// mu guards the diff state below. RecordSnapshot is the only writer and is
	// expected to be called from a single sampler goroutine, but the lock
	// costs nothing and makes an accidental second caller safe rather than
	// silently corrupting the churn counts.
	mu     sync.Mutex
	prev   map[string]*runnerState
	prevAt time.Time
}

// runnerState is what the store remembers about a runner between snapshots.
// It exists because a Snapshot is a photograph: nothing in it says a runner is
// new, or that a job just finished. Those are differences, and differences
// need the previous frame.
//
// Integrated cost is deliberately not here. It accumulates in the database, a
// scrape's increment at a time, so that it survives the process rather than
// living and dying with it.
type runnerState struct {
	set   string
	state fleet.State
	job   fleet.Job
	// phaseAt is when the runner entered its current state, as observed.
	phaseAt time.Time
}

// Open opens (creating it if necessary) the SQLite database at path and
// applies the schema.
//
// The connection is deliberately capped at one. SQLite serialises writers
// anyway, and a pool of readers competing with the sampler's writes just turns
// lock contention into SQLITE_BUSY errors that the busy_timeout then has to
// absorb.
func Open(ctx context.Context, path string, log zerolog.Logger) (*Store, error) {
	if path == "" {
		return nil, errors.New("open store: path is empty")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create store directory %s: %w", dir, err)
		}
	}

	// modernc.org/sqlite registers itself as "sqlite"; ent's dialect.SQLite is
	// the string "sqlite3", which is mattn's name. They are opened separately
	// and joined below rather than aliased, because handing ent a
	// caller-constructed driver value loses the driver package's own
	// registrations.
	//
	// foreign_keys(1) is mandatory, not hygiene: ent's SQLite migrator refuses
	// to run without it, and the error it prints names mattn's `_fk=1`
	// spelling, which does nothing here and sends you looking in the wrong
	// place entirely.
	dsn := path + "?" + strings.Join([]string{
		"_pragma=foreign_keys(1)",
		"_pragma=journal_mode(WAL)",
		"_pragma=busy_timeout(5000)",
		"_pragma=synchronous(NORMAL)",
	}, "&")

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to sqlite %s: %w", path, err)
	}

	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.SQLite, db)))
	if err := migrate(ctx, client); err != nil {
		_ = client.Close()
		return nil, err
	}

	log.Info().Str("path", path).Msg("history store open")

	return &Store{
		path:   path,
		db:     db,
		client: client,
		log:    log.With().Str("component", "store").Logger(),
		prev:   map[string]*runnerState{},
	}, nil
}

// migrateMu serialises schema creation across every Store in the process.
//
// This is not paranoia about SQLite. ent's Atlas-backed migrator decorates the
// *package-level* table descriptors generated into ent/migrate as it runs, so
// two clients calling Schema.Create at the same time write and read the same
// globals — a real data race that `go test -race` reports from inside ent
// itself. Production opens exactly one store and would never notice; the test
// suite opens a dozen in parallel and does.
var migrateMu sync.Mutex

// migrate applies the schema under that lock.
func migrate(ctx context.Context, client *ent.Client) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()
	if err := client.Schema.Create(ctx); err != nil {
		return fmt.Errorf("migrate store schema: %w", err)
	}
	return nil
}

// Close releases the database. It is safe to call on a partially-constructed
// store.
func (s *Store) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	return nil
}

// Ping reports whether the database is reachable. It backs /readyz.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping store: %w", err)
	}
	return nil
}

// Stats reports on-disk size and row counts.
//
// The size includes the write-ahead log and shared-memory files, because those
// are real bytes on the volume and a WAL that has not been checkpointed can be
// a large fraction of the total. Missing sidecar files are not an error; they
// simply do not exist until SQLite creates them.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Path: s.path}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		fi, err := os.Stat(s.path + suffix)
		if err != nil {
			continue
		}
		st.SizeBytes += fi.Size()
	}

	counts := []struct {
		table string
		into  *int64
	}{
		{"samples", &st.Samples},
		{"job_observations", &st.Jobs},
		{"phase_transitions", &st.Phases},
		{"churn_events", &st.ChurnEvents},
		{"runner_failures", &st.Failures},
	}
	for _, c := range counts {
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table).Scan(c.into); err != nil {
			return st, fmt.Errorf("count %s: %w", c.table, err)
		}
		st.Rows += *c.into
	}

	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MIN(ts) FROM samples").Scan(&oldest); err != nil {
		return st, fmt.Errorf("oldest sample: %w", err)
	}
	if oldest.Valid {
		st.Oldest = time.Unix(oldest.Int64, 0).UTC()
	}
	return st, nil
}

// fromUnix converts stored Unix seconds back to a time, mapping the 0 sentinel
// (still running, not yet ended) to the zero time rather than to 1970.
func fromUnix(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}

// floorTo rounds a unix timestamp down to a bucket boundary. Timestamps are
// positive in every realistic case, so plain integer division is correct.
func floorTo(ts, bucket int64) int64 {
	if bucket <= 1 {
		return ts
	}
	return (ts / bucket) * bucket
}
