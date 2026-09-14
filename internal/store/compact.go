package store

import (
	"context"
	"fmt"
	"time"

	entsql "entgo.io/ent/dialect/sql"

	"arc-ui/internal/store/ent/churnevent"
	"arc-ui/internal/store/ent/jobobservation"
	"arc-ui/internal/store/ent/jobsample"
	"arc-ui/internal/store/ent/predicate"
	"arc-ui/internal/store/ent/runnerfailure"
	"arc-ui/internal/store/ent/sample"
)

// Compact rolls raw samples up into coarser tiers and applies retention.
//
// Run it periodically. It is idempotent — running it twice with the same
// `now` leaves exactly the same rows — and safe to interrupt, because every
// step is a single statement and rollups always run before the deletes that
// consume their sources. A crash between the two leaves duplicate data, never
// missing data, and the next run converges.
//
// The idempotency rests on one detail that is easy to get wrong: the boundary
// at which a source tier is trimmed is floored to the *target* bucket width,
// and the rollup reads from the same floored boundary. Without that
// alignment, a bucket straddling the retention edge would be re-averaged from
// whichever of its source rows happened to survive the previous sweep, and
// history would quietly drift every time compaction ran.
//
// `now` is a parameter rather than time.Now() so a test can compact a
// fabricated timeline, and so two calls in the same tick agree.
func (s *Store) Compact(ctx context.Context, now time.Time, ret Retention) error {
	start := time.Now()

	sourceRetention := map[Tier]time.Duration{
		TierRaw: ret.ScopeRaw,
		Tier1m:  ret.Scope1m,
		Tier5m:  ret.Scope5m,
	}

	var rolled int64
	for _, step := range rollupTiers {
		n, err := s.rollup(ctx, now, step.from, step.to, sourceRetention[step.from])
		if err != nil {
			return err
		}
		rolled += n
	}

	deleted, err := s.applyRetention(ctx, now, ret)
	if err != nil {
		return err
	}

	s.log.Debug().
		Int64("rolled_up", rolled).
		Int64("deleted", deleted).
		Dur("took", time.Since(start)).
		Msg("compacted history store")
	return nil
}

// rollup derives the `to` tier from the `from` tier for every bucket that is
// complete and still within the source tier's retention.
//
// Only whole buckets are derived: the bucket containing `now` is left alone,
// so a rollup never publishes a partial average that a later run would
// silently correct.
//
// Averaging is the right fold here because every stored metric is a gauge.
// Counts — churn, job throughput — deliberately live in their own tables so
// they are never averaged by accident. Rolling m1 into m5 does average
// averages, which is only exactly the true mean when the underlying buckets
// have equal sample counts; at scrape resolution they effectively do, and the
// error is far below what a ninety-pixel chart can render.
func (s *Store) rollup(ctx context.Context, now time.Time, from, to Tier, srcRetention time.Duration) (int64, error) {
	bucket := int64(to.Resolution().Seconds())
	if bucket <= 0 {
		return 0, fmt.Errorf("rollup %s to %s: target tier has no resolution", from, to)
	}
	keepFrom := rollupFloor(now, srcRetention, bucket)
	upTo := floorTo(now.Unix(), bucket)
	if upTo <= keepFrom {
		return 0, nil
	}

	// The subquery is not cosmetic. SQLite's parser cannot always tell where a
	// SELECT ends and an upsert clause begins when INSERT ... SELECT is
	// followed by ON CONFLICT; wrapping the aggregate in a derived table and
	// adding the documented `WHERE true` removes the ambiguity entirely.
	//
	// Runner-scoped rows are excluded: they are raw-only by design and are
	// dropped after fifteen minutes, so rolling them up would create exactly
	// the per-runner history the tiering exists to avoid.
	const q = `
INSERT INTO samples (scope, scope_id, metric, tier, ts, value)
SELECT scope, scope_id, metric, tier, bts, value FROM (
    SELECT scope, scope_id, metric, ? AS tier, (ts / ?) * ? AS bts, AVG(value) AS value
    FROM samples
    WHERE tier = ? AND scope <> ? AND ts >= ? AND ts < ?
    GROUP BY scope, scope_id, metric, bts
)
WHERE true
ON CONFLICT (scope, scope_id, metric, tier, ts) DO UPDATE SET value = excluded.value`

	res, err := s.db.ExecContext(ctx, q,
		string(to), bucket, bucket,
		string(from), string(ScopeRunner), keepFrom, upTo)
	if err != nil {
		return 0, fmt.Errorf("roll %s up into %s: %w", from, to, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		// The count is this function's whole return value, and a zero here is
		// indistinguishable from "there was nothing to roll up" — so the only
		// honest answer to "I cannot count what I wrote" is an error. The
		// retention sweep below takes the opposite view for the same call and
		// is right to: there the count is a running total for one log line,
		// and losing it must not abandon the sweeps that have not run yet.
		return 0, fmt.Errorf("count rows rolled from %s into %s: %w", from, to, err)
	}
	return n, nil
}

// rollupFloor is the oldest source timestamp a rollup reads, and — critically
// — the exact cutoff the matching retention delete uses. A zero retention
// means "keep forever", so the rollup has to start at the beginning of time.
func rollupFloor(now time.Time, retention time.Duration, bucket int64) int64 {
	if retention <= 0 {
		return 0
	}
	return floorTo(now.Add(-retention).Unix(), bucket)
}

// maxJobRuntime is how long past the retention window an unfinished job
// observation is still believed rather than swept.
//
// A row is only closed by the completion the store observes, so a runner that
// went away while the process was down leaves one open forever: without a
// second cutoff those rows are the one thing in the database that never
// expires. Five days is GitHub's own execution-time ceiling for a job on a
// self-hosted runner, so a row still open that far past the window is not a
// long build, it is a lost completion signal.
const maxJobRuntime = 5 * 24 * time.Hour

// applyRetention deletes everything past its window.
//
// The derived tables have no retention knob of their own, so they borrow one:
// jobs, churn and failures follow the 5-minute tier, which is the longest
// window the throughput, consumption and failure panels offer.
func (s *Store) applyRetention(ctx context.Context, now time.Time, ret Retention) (int64, error) {
	type sweep struct {
		what      string
		retention time.Duration
		// del deletes everything older than cutoff and reports how many rows
		// went. Each is a closure rather than a SQL string so the predicates
		// are the same generated ones the rest of the package writes through.
		del func(ctx context.Context, cutoff int64) (int, error)
		// cutoff is the boundary this sweep deletes below. The tiers that feed
		// a rollup align it to the TARGET bucket width, matching what the
		// rollup reads from, so a bucket straddling the edge is never
		// re-averaged from whichever of its source rows survived. The rest are
		// exact.
		cutoff int64
	}

	sweeps := []sweep{
		{
			// Per-runner raw samples. Nothing is derived from them, so the
			// cutoff needs no bucket alignment and is exact.
			what:      "runner raw samples",
			retention: ret.RunnerRaw,
			cutoff:    unixCutoff(now, ret.RunnerRaw),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.Sample.Delete().Where(
					sample.TierEQ(string(TierRaw)),
					sample.ScopeEQ(string(ScopeRunner)),
					sample.TsLT(cutoff),
				).Exec(ctx)
			},
		},
		{
			what:      "scope raw samples",
			retention: ret.ScopeRaw,
			cutoff:    rollupFloor(now, ret.ScopeRaw, int64(Tier1m.Resolution().Seconds())),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.Sample.Delete().Where(
					sample.TierEQ(string(TierRaw)),
					sample.ScopeNEQ(string(ScopeRunner)),
					sample.TsLT(cutoff),
				).Exec(ctx)
			},
		},
		{
			what:      "1m samples",
			retention: ret.Scope1m,
			cutoff:    rollupFloor(now, ret.Scope1m, int64(Tier5m.Resolution().Seconds())),
			del:       s.deleteTier(Tier1m),
		},
		{
			what:      "5m samples",
			retention: ret.Scope5m,
			cutoff:    rollupFloor(now, ret.Scope5m, int64(Tier1h.Resolution().Seconds())),
			del:       s.deleteTier(Tier5m),
		},
		{
			// The coarsest tier feeds nothing, so it too gets an exact cutoff.
			what:      "1h samples",
			retention: ret.Scope1h,
			cutoff:    unixCutoff(now, ret.Scope1h),
			del:       s.deleteTier(Tier1h),
		},
		{
			// Per-job usage, on its own window. This is the knob that lets an
			// operator keep a month of job rows while paying for a week of the
			// samples behind them, which is the only table here whose size is
			// driven by how BUSY the fleet is rather than how big it is.
			//
			// Sweeping on the sample's own timestamp means a job that straddles
			// the boundary keeps the part of its series inside the window and
			// loses the part outside it. That is bounded by maxJobRuntime and
			// is the honest reading of "keep N of usage history"; the
			// alternative, holding every sample of any job with one foot in the
			// window, makes the window a lower bound rather than a limit.
			what:      "job samples",
			retention: ret.JobSamples,
			cutoff:    unixCutoff(now, ret.JobSamples),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.JobSample.Delete().Where(jobsample.TsLT(cutoff)).Exec(ctx)
			},
		},
		{
			// Samples whose job is about to go. These two run BEFORE the job
			// sweeps below and select on the same cutoffs, so a sample can
			// never outlive the row that gives it meaning — an orphan here
			// would be unreachable and immortal, since nothing else in this
			// list would ever match it again.
			what:      "job samples of expiring finished jobs",
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, ret.Scope5m),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.JobSample.Delete().Where(samplesOfJobs(
					jobobservation.FinishedAtGT(0),
					jobobservation.FinishedAtLT(cutoff),
				)).Exec(ctx)
			},
		},
		{
			what:      "job samples of expiring abandoned jobs",
			retention: ret.Scope5m,
			// Same two-step subtraction as the abandoned-job sweep below, and
			// for the same overflow reason.
			cutoff: now.Add(-ret.Scope5m).Add(-maxJobRuntime).Unix(),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.JobSample.Delete().Where(samplesOfJobs(
					jobobservation.FinishedAtLTE(0),
					jobobservation.StartedAtLT(cutoff),
				)).Exec(ctx)
			},
		},
		{
			// Finished jobs age from their completion, not from their start. A
			// row becomes history the moment it stops changing, and sweeping on
			// started_at would delete a week-long build that finished a minute
			// ago — along with the throughput bucket it belongs in.
			what:      "finished job observations",
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, ret.Scope5m),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.JobObservation.Delete().Where(
					jobobservation.FinishedAtGT(0),
					jobobservation.FinishedAtLT(cutoff),
				).Exec(ctx)
			},
		},
		{
			// Unfinished jobs have no completion to age from, so they are held
			// for the window plus a job's maximum lifetime. Anything still open
			// beyond that will never be closed and would otherwise be immortal.
			//
			// The predicate is `<= 0`, not `= 0`, so that it and the sweep above
			// partition the table exhaustively. Only the 0 sentinel is ever
			// written, but a row that somehow carried a negative finished_at
			// would match neither `> 0` nor `= 0` and would then be the one row
			// in this database that nothing ever expires.
			what:      "abandoned job observations",
			retention: ret.Scope5m,
			// Two subtractions rather than one sum of two windows: adding them
			// first can overflow time.Duration's 292-year range and land the
			// cutoff in the future, where the sweep deletes rows written
			// seconds ago. time.Time.Add clamps, so stepping back twice cannot.
			cutoff: now.Add(-ret.Scope5m).Add(-maxJobRuntime).Unix(),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.JobObservation.Delete().Where(
					jobobservation.FinishedAtLTE(0),
					jobobservation.StartedAtLT(cutoff),
				).Exec(ctx)
			},
		},
		{
			what:      "churn events",
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, ret.Scope5m),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.ChurnEvent.Delete().Where(churnevent.TsLT(cutoff)).Exec(ctx)
			},
		},
		{
			// Without a sweep this would be the one table in the database that
			// grows forever: nothing else deletes it, and a scale set with a bad
			// image writes a row per runner it burns through.
			what:      "runner failures",
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, ret.Scope5m),
			del: func(ctx context.Context, cutoff int64) (int, error) {
				return s.client.RunnerFailure.Delete().Where(runnerfailure.TsLT(cutoff)).Exec(ctx)
			},
		},
	}

	var total int64
	for _, sw := range sweeps {
		if sw.retention <= 0 {
			continue
		}
		n, err := sw.del(ctx, sw.cutoff)
		if err != nil {
			return total, fmt.Errorf("expire %s: %w", sw.what, err)
		}
		total += int64(n)
	}
	return total, nil
}

// samplesOfJobs matches every sample belonging to a job the given predicates
// select. JobSample.job_id is a plain column rather than an ent edge — the
// samples are written and read by id and never traversed — so the IN subquery
// is built here instead of coming from a generated HasJobWith.
func samplesOfJobs(where ...predicate.JobObservation) predicate.JobSample {
	return predicate.JobSample(func(s *entsql.Selector) {
		jobs := entsql.Select(jobobservation.FieldID).From(entsql.Table(jobobservation.Table))
		for _, p := range where {
			p(jobs)
		}
		s.Where(entsql.In(s.C(jobsample.FieldJobID), jobs))
	})
}

// deleteTier sweeps one whole sample tier, which is the shape three of the
// sweeps above share.
func (s *Store) deleteTier(tier Tier) func(context.Context, int64) (int, error) {
	return func(ctx context.Context, cutoff int64) (int, error) {
		return s.client.Sample.Delete().
			Where(sample.TierEQ(string(tier)), sample.TsLT(cutoff)).
			Exec(ctx)
	}
}

// unixCutoff is the plain, unaligned retention boundary.
func unixCutoff(now time.Time, retention time.Duration) int64 {
	return now.Add(-retention).Unix()
}
