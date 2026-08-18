package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	entsql "entgo.io/ent/dialect/sql"

	"arc-ui/internal/store/ent/jobobservation"
	"arc-ui/internal/store/ent/phasetransition"
)

// defaultPoints is what a Range with no Points asks for: enough to fill the
// widest chart in the design without pretending to more resolution than a
// few-hundred-pixel sparkline can show.
const defaultPoints = 60

// maxBuckets caps the dense grids Churn and Throughput build, so a caller that
// passes a decade-long range with a million points cannot allocate the process
// to death.
const maxBuckets = 10000

// window resolves a Range into the half-open [from, to) second range and the
// bucket width to group by. ok is false when the range is empty or inverted,
// which every caller turns into an empty result rather than an error: a chart
// with nothing to draw is a rendering state, not a failure.
func (r Range) window() (from, to, bucket int64, ok bool) {
	if r.From.IsZero() || r.To.IsZero() || !r.To.After(r.From) {
		return 0, 0, 0, false
	}
	points := r.Points
	if points <= 0 {
		points = defaultPoints
	}
	from, to = r.From.Unix(), r.To.Unix()
	bucket = int64(math.Ceil(float64(to-from) / float64(points)))
	if bucket < 1 {
		bucket = 1
	}
	return from, to, bucket, true
}

// tierFor picks the coarsest tier whose native resolution still fits inside
// the requested bucket width.
//
// This is what keeps a thirty-day query off the raw table. A thirty-day window
// rendered as ninety points has seven-hour buckets, so the hourly tier is
// exact enough and is four orders of magnitude smaller to scan; a fifteen
// minute window has fifteen-second buckets, finer than any rollup, so it must
// read raw.
func tierFor(bucket int64) Tier {
	best := TierRaw
	for _, t := range []Tier{Tier1m, Tier5m, Tier1h} {
		if int64(t.Resolution().Seconds()) <= bucket {
			best = t
		}
	}
	return best
}

// Series returns bucketed values for the given metrics over the range,
// choosing the coarsest tier that still resolves the requested bucket width.
//
// The result has an entry for every requested metric, empty when the store has
// nothing. Within a series only buckets that actually contain samples appear:
// gaps are gaps, not zeros. A gauge padded with zeros would draw a fleet
// dropping to nothing every time the sampler missed a tick.
func (s *Store) Series(ctx context.Context, scope Scope, scopeID string, metrics []Metric, r Range) (map[Metric][]Point, error) {
	out := make(map[Metric][]Point, len(metrics))
	if len(metrics) == 0 {
		return out, nil
	}
	for _, m := range metrics {
		out[m] = nil
	}
	from, to, bucket, ok := r.window()
	if !ok {
		return out, nil
	}

	// The fleet is a single series, so its scope ID is always empty however
	// the caller spelled it.
	if scope == ScopeFleet {
		scopeID = ""
	}
	// Runner samples are never rolled up — they are retained for minutes, and
	// there is no coarser tier to fall back to.
	tier := TierRaw
	if scope != ScopeRunner {
		tier = tierFor(bucket)
	}

	args := []any{bucket, bucket, string(scope), scopeID, string(tier), from, to}
	holders := make([]string, len(metrics))
	for i, m := range metrics {
		holders[i] = "?"
		args = append(args, string(m))
	}

	// The only thing concatenated is a run of "?" placeholders sized to the
	// metric list; every metric name goes in as a bound argument below.
	// #nosec G202 -- placeholders only, never interpolated values
	q := `SELECT metric, (ts / ?) * ? AS bts, AVG(value)
	      FROM samples
	      WHERE scope = ? AND scope_id = ? AND tier = ? AND ts >= ? AND ts < ?
	        AND metric IN (` + strings.Join(holders, ",") + `)
	      GROUP BY metric, bts
	      ORDER BY bts`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return out, fmt.Errorf("query series %s/%s: %w", scope, scopeID, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			metric string
			bts    int64
			value  float64
		)
		if err := rows.Scan(&metric, &bts, &value); err != nil {
			return out, fmt.Errorf("scan series row: %w", err)
		}
		m := Metric(metric)
		out[m] = append(out[m], Point{At: time.Unix(bts, 0).UTC(), Value: value})
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("read series rows: %w", err)
	}
	return out, nil
}

// grid builds the dense bucket starts covering [from, to).
//
// Counts get a dense grid where gauges do not, and the asymmetry is deliberate:
// "no jobs finished in this minute" is a measurement worth drawing as zero,
// whereas "no CPU sample in this minute" is an absence.
func grid(from, to, bucket int64) []int64 {
	start := floorTo(from, bucket)
	n := int((to - start + bucket - 1) / bucket)
	if n < 0 {
		return nil
	}
	n = min(n, maxBuckets)
	out := make([]int64, n)
	for i := range out {
		out[i] = start + int64(i)*bucket
	}
	return out
}

// densePoints is an all-zero series on the given grid.
func densePoints(buckets []int64) []Point {
	out := make([]Point, len(buckets))
	for i, b := range buckets {
		out[i] = Point{At: time.Unix(b, 0).UTC()}
	}
	return out
}

// countSeries runs a bucketed COUNT(*) grouped by one discriminator column and
// projects it onto a dense grid. It backs both Churn and Throughput, which are
// the same query over different tables.
func (s *Store) countSeries(ctx context.Context, q string, args []any, buckets []int64) (map[string][]Point, error) {
	index := make(map[int64]int, len(buckets))
	for i, b := range buckets {
		index[b] = i
	}
	series := map[string][]Point{}
	ensure := func(key string) []Point {
		if p, ok := series[key]; ok {
			return p
		}
		p := densePoints(buckets)
		series[key] = p
		return p
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			key   string
			bts   int64
			count float64
		)
		if err := rows.Scan(&key, &bts, &count); err != nil {
			return nil, fmt.Errorf("scan count row: %w", err)
		}
		p := ensure(key)
		if i, ok := index[bts]; ok {
			p[i].Value += count
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read count rows: %w", err)
	}
	return series, nil
}

// Churn returns created and terminated counts bucketed over the range. An
// empty setName covers the whole fleet.
//
// Both slices always have the same length and the same bucket starts, so a
// caller can zip them without checking. On an empty store they are empty, not
// nil-of-different-lengths.
func (s *Store) Churn(ctx context.Context, setName string, r Range) (created, terminated []Point, err error) {
	from, to, bucket, ok := r.window()
	if !ok {
		return nil, nil, nil
	}
	buckets := grid(from, to, bucket)
	if len(buckets) == 0 {
		return nil, nil, nil
	}

	q := `SELECT kind, (ts / ?) * ? AS bts, COUNT(*)
	      FROM churn_events
	      WHERE ts >= ? AND ts < ?`
	args := []any{bucket, bucket, from, to}
	if setName != "" {
		q += " AND set_name = ?"
		args = append(args, setName)
	}
	q += " GROUP BY kind, bts ORDER BY bts"

	series, err := s.countSeries(ctx, q, args, buckets)
	if err != nil {
		return nil, nil, fmt.Errorf("churn for %q: %w", setName, err)
	}
	return denseOr(series[churnCreated], buckets), denseOr(series[churnTerminated], buckets), nil
}

// Throughput returns completed and failed job counts bucketed over the range,
// keyed on when each job finished. An empty setName covers the whole fleet.
//
// Jobs still running are excluded rather than counted in the newest bucket:
// they have not finished, and putting them anywhere would make the newest
// bucket lie until the next refresh moved them.
func (s *Store) Throughput(ctx context.Context, setName string, r Range) (ok, failed []Point, err error) {
	from, to, bucket, valid := r.window()
	if !valid {
		return nil, nil, nil
	}
	buckets := grid(from, to, bucket)
	if len(buckets) == 0 {
		return nil, nil, nil
	}

	// The success flag is projected to text in SQL rather than scanned as a
	// bool, because SQLite stores it as 0/1 and the grouping key has to be one
	// comparable type for both this query and Churn's.
	q := `SELECT CASE WHEN succeeded THEN 'ok' ELSE 'failed' END, (finished_at / ?) * ? AS bts, COUNT(*)
	      FROM job_observations
	      WHERE finished_at > 0 AND finished_at >= ? AND finished_at < ?`
	args := []any{bucket, bucket, from, to}
	if setName != "" {
		q += " AND set_name = ?"
		args = append(args, setName)
	}
	q += " GROUP BY 1, bts ORDER BY bts"

	series, err := s.countSeries(ctx, q, args, buckets)
	if err != nil {
		return nil, nil, fmt.Errorf("throughput for %q: %w", setName, err)
	}
	return denseOr(series["ok"], buckets), denseOr(series["failed"], buckets), nil
}

// denseOr returns p, or an all-zero series on the same grid when the query
// produced no rows for that key at all.
func denseOr(p []Point, buckets []int64) []Point {
	if p != nil {
		return p
	}
	return densePoints(buckets)
}

// RepoConsumption totals jobs and integrated cost per repository over the
// range, busiest first.
//
// A job counts when it overlapped the window, not when it started, so a
// six-hour build is not invisible in the last hour's chart. That does mean a
// long job's whole cost is attributed to any window it touches; the
// alternative is storing per-bucket cost for every job, which is exactly the
// row explosion this package is built to avoid.
func (s *Store) RepoConsumption(ctx context.Context, r Range) ([]RepoTotal, error) {
	from, to, _, ok := r.window()
	if !ok {
		return nil, nil
	}

	const q = `SELECT repository, COUNT(*), SUM(cpu_seconds), SUM(mem_byte_seconds)
	           FROM job_observations
	           WHERE repository <> ''
	             AND started_at < ?
	             AND (finished_at = 0 OR finished_at >= ?)
	           GROUP BY repository`

	rows, err := s.db.QueryContext(ctx, q, to, from)
	if err != nil {
		return nil, fmt.Errorf("query repo consumption: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []RepoTotal
	for rows.Next() {
		var (
			rt      RepoTotal
			cpu     sql.NullFloat64
			memSecs sql.NullFloat64
		)
		if err := rows.Scan(&rt.Repository, &rt.Jobs, &cpu, &memSecs); err != nil {
			return nil, fmt.Errorf("scan repo consumption row: %w", err)
		}
		rt.CPUSeconds, rt.MemByteSeconds = cpu.Float64, memSecs.Float64
		out = append(out, rt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read repo consumption rows: %w", err)
	}

	// Cost first, job count as the tie-break: a repository burning cores
	// matters more than one running many trivial jobs, and that is the
	// question the panel is asked. Name last so the order is deterministic.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CPUSeconds != out[j].CPUSeconds {
			return out[i].CPUSeconds > out[j].CPUSeconds
		}
		if out[i].Jobs != out[j].Jobs {
			return out[i].Jobs > out[j].Jobs
		}
		return out[i].Repository < out[j].Repository
	})
	return out, nil
}

// JobsForSet returns recent job observations for a runner set, newest first.
//
// The runner-detail "job history" panel is scoped to the set rather than to
// the runner on purpose: an ARC ephemeral runner executes exactly one job and
// is then destroyed, so a per-runner history would always be one row long.
func (s *Store) JobsForSet(ctx context.Context, setName string, limit int) ([]JobRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	q := s.client.JobObservation.Query().
		Order(jobobservation.ByStartedAt(entsql.OrderDesc()), jobobservation.ByID(entsql.OrderDesc())).
		Limit(limit)
	if setName != "" {
		q = q.Where(jobobservation.SetNameEQ(setName))
	}

	rows, err := q.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("query jobs for set %q: %w", setName, err)
	}
	out := make([]JobRecord, 0, len(rows))
	for _, j := range rows {
		out = append(out, JobRecord{
			Runner:     j.RunnerName,
			Set:        j.SetName,
			Repository: j.Repository,
			Workflow:   j.Workflow,
			Job:        j.JobName,
			RunID:      j.RunID,
			StartedAt:  fromUnix(j.StartedAt),
			FinishedAt: fromUnix(j.FinishedAt),
			Succeeded:  j.Succeeded,
			CPUSeconds: j.CPUSeconds,
		})
	}
	return out, nil
}

// PhasesForRunner returns the lifecycle phases observed for one runner,
// oldest first, which is the order the lifecycle bar lays them out in.
func (s *Store) PhasesForRunner(ctx context.Context, runnerName string) ([]Phase, error) {
	rows, err := s.client.PhaseTransition.Query().
		Where(phasetransition.RunnerNameEQ(runnerName)).
		Order(phasetransition.ByStartedAt(), phasetransition.ByID()).
		All(ctx)
	if err != nil {
		return nil, fmt.Errorf("query phases for runner %q: %w", runnerName, err)
	}
	out := make([]Phase, 0, len(rows))
	for _, p := range rows {
		out = append(out, Phase{
			Runner:    p.RunnerName,
			Set:       p.SetName,
			Phase:     p.Phase,
			StartedAt: fromUnix(p.StartedAt),
			EndedAt:   fromUnix(p.EndedAt),
		})
	}
	return out, nil
}
