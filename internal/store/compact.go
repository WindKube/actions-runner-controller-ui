package store

import (
	"context"
	"fmt"
	"math"
	"time"
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
// jobs and churn follow the 5-minute tier (the longest window the throughput
// and consumption panels offer), and phases follow the 1-minute tier, because
// the lifecycle bar is a minutes-to-hours view and nobody scrolls a runner's
// phase history back a month.
func (s *Store) applyRetention(ctx context.Context, now time.Time, ret Retention) (int64, error) {
	type sweep struct {
		what      string
		q         string
		args      []any
		retention time.Duration
		cutoff    int64
	}

	sweeps := []sweep{
		{
			// Per-runner raw samples. Nothing is derived from them, so the
			// cutoff needs no bucket alignment and is exact.
			what:      "runner raw samples",
			q:         `DELETE FROM samples WHERE tier = ? AND scope = ? AND ts < ?`,
			args:      []any{string(TierRaw), string(ScopeRunner)},
			retention: ret.RunnerRaw,
			cutoff:    unixCutoff(now, ret.RunnerRaw),
		},
		{
			what:      "scope raw samples",
			q:         `DELETE FROM samples WHERE tier = ? AND scope <> ? AND ts < ?`,
			args:      []any{string(TierRaw), string(ScopeRunner)},
			retention: ret.ScopeRaw,
			cutoff:    rollupFloor(now, ret.ScopeRaw, int64(Tier1m.Resolution().Seconds())),
		},
		{
			what:      "1m samples",
			q:         `DELETE FROM samples WHERE tier = ? AND ts < ?`,
			args:      []any{string(Tier1m)},
			retention: ret.Scope1m,
			cutoff:    rollupFloor(now, ret.Scope1m, int64(Tier5m.Resolution().Seconds())),
		},
		{
			what:      "5m samples",
			q:         `DELETE FROM samples WHERE tier = ? AND ts < ?`,
			args:      []any{string(Tier5m)},
			retention: ret.Scope5m,
			cutoff:    rollupFloor(now, ret.Scope5m, int64(Tier1h.Resolution().Seconds())),
		},
		{
			// The coarsest tier feeds nothing, so it too gets an exact cutoff.
			what:      "1h samples",
			q:         `DELETE FROM samples WHERE tier = ? AND ts < ?`,
			args:      []any{string(Tier1h)},
			retention: ret.Scope1h,
			cutoff:    unixCutoff(now, ret.Scope1h),
		},
		{
			// Finished jobs age from their completion, not from their start. A
			// row becomes history the moment it stops changing, and sweeping on
			// started_at would delete a week-long build that finished a minute
			// ago — along with the throughput bucket it belongs in.
			what:      "finished job observations",
			q:         `DELETE FROM job_observations WHERE finished_at > 0 AND finished_at < ?`,
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, ret.Scope5m),
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
			q:         `DELETE FROM job_observations WHERE finished_at <= 0 AND started_at < ?`,
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, addWindows(ret.Scope5m, maxJobRuntime)),
		},
		{
			what:      "churn events",
			q:         `DELETE FROM churn_events WHERE ts < ?`,
			retention: ret.Scope5m,
			cutoff:    unixCutoff(now, ret.Scope5m),
		},
		{
			what:      "phase transitions",
			q:         `DELETE FROM phase_transitions WHERE started_at < ?`,
			retention: ret.Scope1m,
			cutoff:    unixCutoff(now, ret.Scope1m),
		},
	}

	var total int64
	for _, sw := range sweeps {
		if sw.retention <= 0 {
			continue
		}
		res, err := s.db.ExecContext(ctx, sw.q, append(sw.args, sw.cutoff)...)
		if err != nil {
			return total, fmt.Errorf("expire %s: %w", sw.what, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			total += n
		}
	}
	return total, nil
}

// unixCutoff is the plain, unaligned retention boundary.
func unixCutoff(now time.Time, retention time.Duration) int64 {
	return now.Add(-retention).Unix()
}

// addWindows adds two retention windows, saturating instead of wrapping.
//
// time.Duration is int64 nanoseconds and so tops out at about 292 years. A
// window configured within maxJobRuntime of that ceiling wraps NEGATIVE when
// the two are added, and unixCutoff turns a negative window into a boundary in
// the *future* — a sweep that then deletes every row it looks at, including
// ones written seconds ago. The `retention <= 0` guard in the loop cannot
// catch it, because what it tests is the configured window, which is still
// perfectly positive.
//
// Saturating is safe in the other direction: subtracting the maximum duration
// from any plausible `now` is an ordinary instant in the eighteenth century —
// no second overflow, and time.Time.Add clamps rather than wrapping in any
// case — so the cutoff predates every row and deletes nothing, which is what a
// retention window of three centuries asked for.
func addWindows(a, b time.Duration) time.Duration {
	sum := a + b
	if a > 0 && b > 0 && sum < 0 {
		return math.MaxInt64
	}
	return sum
}
