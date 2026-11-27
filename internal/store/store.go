// Package store keeps one row per workflow job, written by observing
// EphemeralRunners. Every time series lives in Prometheus; this is only the
// job identity Prometheus cannot hold (job id, run id, workflow ref) and the
// runner outcome.
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
	_ "modernc.org/sqlite" // registered as "sqlite"

	"arc-ui/internal/store/ent"
)

// Outcome is how the runner that carried a job ended. It is not the job's
// conclusion on GitHub, which the cluster never sees.
type Outcome string

const (
	OutcomeAny       Outcome = ""
	OutcomeRunning   Outcome = "running"
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
)

// JobRecord is one stored job.
type JobRecord struct {
	ID            int
	JobID         string
	RunID         int64
	Repository    string
	WorkflowRef   string
	Workflow      string
	Name          string
	Runner        string
	Namespace     string
	Set           string
	StartedAt     time.Time
	FinishedAt    time.Time
	Outcome       Outcome
	FailureReason string

	CPUSeconds   float64
	MemPeakBytes float64
	CPURequest   float64
	CPULimit     float64
	MemRequest   float64
	MemLimit     float64
}

func (j JobRecord) Running() bool { return j.FinishedAt.IsZero() }

// JobFilter narrows a listing; the zero value matches everything.
type JobFilter struct {
	Repository string
	Workflow   string
	Set        string
	RunID      int64
	Outcome    Outcome
	Search     string
	Limit      int
}

// WorkflowRun aggregates the jobs stored for one run.
type WorkflowRun struct {
	Repository string
	Workflow   string
	RunID      int64
	Jobs       int
	Running    int
	Failed     int
	StartedAt  time.Time
	FinishedAt time.Time

	CPUSeconds   float64
	MemPeakBytes float64
}

type JobFacets struct {
	Repositories []string
	Workflows    []string
	Sets         []string
}

// Stats is what the store costs on disk.
type Stats struct {
	Path      string
	SizeBytes int64
	Jobs      int64
	Oldest    time.Time
}

type Store struct {
	path   string
	db     *sql.DB
	client *ent.Client
	log    zerolog.Logger
}

// legacyTables are the time-series tables older versions wrote; dropping them
// gives the volume back on upgrade.
var legacyTables = []string{"samples", "churn_events", "runner_failures", "job_samples", "job_observations"}

// Open opens or creates the database at path. One connection: SQLite
// serialises writers anyway, and a pool only trades waiting for SQLITE_BUSY.
func Open(ctx context.Context, path string, log zerolog.Logger) (*Store, error) {
	if path == "" {
		return nil, errors.New("open store: path is empty")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create store directory %s: %w", dir, err)
		}
	}

	// ent's SQLite migrator refuses to run without foreign_keys(1).
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

	for _, t := range legacyTables {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+t); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("drop legacy table %s: %w", t, err)
		}
	}

	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.SQLite, db)))
	if err := migrate(ctx, client); err != nil {
		_ = client.Close()
		return nil, err
	}

	log.Info().Str("path", path).Msg("job store open")
	return &Store{path: path, db: db, client: client, log: log.With().Str("component", "store").Logger()}, nil
}

// migrateMu: ent's migrator mutates package-level table descriptors, which
// races when tests open several stores in parallel.
var migrateMu sync.Mutex

func migrate(ctx context.Context, client *ent.Client) error {
	migrateMu.Lock()
	defer migrateMu.Unlock()
	if err := client.Schema.Create(ctx); err != nil {
		return fmt.Errorf("migrate store schema: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping store: %w", err)
	}
	return nil
}

// Stats counts the WAL and shm files too: they are real bytes on the volume.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Path: s.path}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if fi, err := os.Stat(s.path + suffix); err == nil {
			st.SizeBytes += fi.Size()
		}
	}
	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(started_at) FROM jobs`).Scan(&st.Jobs, &oldest); err != nil {
		return st, fmt.Errorf("count jobs: %w", err)
	}
	st.Oldest = fromUnix(oldest.Int64)
	return st, nil
}

// fromUnix maps the 0 sentinel to the zero time rather than 1970.
func fromUnix(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.Unix(v, 0).UTC()
}
