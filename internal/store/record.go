package store

import (
	"context"
	"fmt"
	"time"

	entsql "entgo.io/ent/dialect/sql"

	"arc-ui/internal/fleet"
	"arc-ui/internal/store/ent"
	"arc-ui/internal/store/ent/churnevent"
	"arc-ui/internal/store/ent/jobobservation"
	"arc-ui/internal/store/ent/phasetransition"
	"arc-ui/internal/store/ent/sample"
)

// metricValue is one metric of one scope, staged before the bulk insert.
type metricValue struct {
	metric Metric
	value  float64
}

// RecordSnapshot persists one observation of the fleet.
//
// It writes four things: scope samples for the fleet and each set, raw samples
// for each runner, and the churn, job and phase rows implied by the difference
// from the previous snapshot. The differences are the interesting part — a
// Snapshot alone cannot say that a runner is new or that a job just finished,
// so the store keeps the previous frame in memory and compares.
//
// Every write is an upsert keyed on natural identity rather than on a
// surrogate ID, so replaying the same snapshot, or restarting the process and
// re-observing runners it already knew about, converges instead of doubling
// the counts. The one exception is a job's integrated cost, which accumulates
// in the database rather than being overwritten. Recording the same snapshot
// twice adds nothing to it, because the increment is integrated over the gap
// between the two snapshots' own timestamps and that gap is zero for the same
// frame. A snapshot that carries no timestamp is stamped with time.Now()
// below, so re-recording one of those is a second observation rather than a
// replay: the gap is real and so is the increment.
func (s *Store) RecordSnapshot(ctx context.Context, snap fleet.Snapshot) error {
	at := snap.At
	if at.IsZero() {
		at = time.Now()
	}
	ts := at.Unix()

	s.mu.Lock()
	defer s.mu.Unlock()

	// dt is the interval CPU usage is integrated over. It is deliberately
	// dropped when the gap is implausible: a process paused for hours would
	// otherwise credit whatever job it was last watching with hours of its
	// final CPU reading.
	dt := 0.0
	if !s.prevAt.IsZero() && at.After(s.prevAt) {
		if gap := at.Sub(s.prevAt); gap <= maxIntegrationGap {
			dt = gap.Seconds()
		}
	}
	firstSnapshot := s.prevAt.IsZero()

	var samples []*ent.SampleCreate
	addScope := func(scope Scope, id string, mvs []metricValue) {
		for _, mv := range mvs {
			samples = append(samples, s.client.Sample.Create().
				SetTs(ts).
				SetScope(string(scope)).
				SetScopeID(id).
				SetTier(string(TierRaw)).
				SetMetric(string(mv.metric)).
				SetValue(mv.value))
		}
	}

	addScope(ScopeFleet, "", scopeMetrics(fleet.Aggregate(snap.Runners, snap.Sets)))
	for _, st := range fleet.GroupBySet(snap.Runners, snap.Sets) {
		addScope(ScopeSet, st.Set.Name, scopeMetrics(st.Totals))
	}

	var (
		jobs     []*ent.JobObservationCreate
		phases   []*ent.PhaseTransitionCreate
		churn    []*ent.ChurnEventCreate
		switched []jobClose
		cur      = make(map[string]*runnerState, len(snap.Runners))
	)

	for _, r := range snap.Runners {
		// The runner name is the identity every row below is written under, so
		// a snapshot that lists one runner twice is recorded once. Skipping is
		// not tidiness: the job upsert ADDS its increment to the stored row, so
		// a second copy conflicts with the row its own statement just inserted
		// and bills the interval again.
		if _, seen := cur[r.Name]; seen {
			continue
		}

		addScope(ScopeRunner, r.Name, runnerMetrics(r))

		next := &runnerState{set: r.SetName, state: r.State, job: r.Job}

		// cpuDelta and memDelta are this interval's consumption: the usage this
		// scrape reports, integrated over dt. They are credited to whatever the
		// runner is doing *now*: the job row written below, or nothing at all
		// when it is idle.
		//
		// That rule is what decides the handover. When a persistent runner has
		// moved from one job to the next, the interval genuinely spans both,
		// and nothing here can place the boundary inside it: usage is measured
		// per pod and the pod ran both jobs, and the reading carries
		// metrics-server's own timestamp, which this code never consults. So the
		// increment is credited to exactly one of the two jobs, never split and
		// never to both, and the one it goes to is the successor — the row this
		// scrape is already writing. The price is that at each handover the
		// successor is credited for a stretch it only partly ran, at most that
		// one interval, and the job the runner left gets nothing for it.
		// Billing it backwards would be the same error with its sign flipped.
		// Either way the error crosses repositories whenever the two jobs
		// belong to different ones, which on a persistent runner is routine.
		//
		// A runner that finishes a job and goes idle drops that interval
		// instead, since there is no job row left to bill it to. So a job's
		// cost is neither a lower nor an upper bound on what it consumed — see
		// RepoTotal.
		//
		// Both stay zero for a runner this snapshot is seeing for the first
		// time — including every runner in the first snapshot after a restart —
		// because there is no earlier observation of it to integrate from.
		var cpuDelta, memDelta float64

		if prev, known := s.prev[r.Name]; known {
			cpuDelta, memDelta = r.CPU.Used*dt, r.Mem.Used*dt
			next.phaseAt = prev.phaseAt
			if prev.state != r.State {
				// A new phase starts at the scrape that first saw the change,
				// which is the earliest instant the store can honestly claim.
				next.phaseAt = at
			}
			if prev.job.Present() && !sameJob(prev.job, r.Job) {
				// A persistent runner moving off its job. Close the old one
				// here; the ephemeral case, where the runner disappears with
				// the job, is handled below. Only the outcome needs writing:
				// the old job's row already holds every increment observed
				// while the runner was on it.
				switched = append(switched, jobClose{
					runner: r.Name,
					runID:  prev.job.RunID,
					job:    prev.job.Name,
					ok:     prev.state != fleet.StateFailed,
				})
			}
		} else {
			next.phaseAt = at
			// Announce the runner's creation at its real creation time rather
			// than at this scrape, so the churn chart is right even on the
			// first snapshot after a restart, where every existing runner
			// looks new.
			createdAt := r.CreatedAt
			if createdAt.IsZero() {
				createdAt = at
			}
			churn = append(churn, s.client.ChurnEvent.Create().
				SetRunnerName(r.Name).
				SetSetName(r.SetName).
				SetKind(churnCreated).
				SetTs(createdAt.Unix()))
		}

		if next.phaseAt.IsZero() {
			next.phaseAt = at
		}
		phases = append(phases, s.client.PhaseTransition.Create().
			SetRunnerName(r.Name).
			SetSetName(r.SetName).
			SetPhase(string(r.State)).
			SetStartedAt(next.phaseAt.Unix()).
			SetEndedAt(ts))

		if r.Job.Present() {
			started := r.Job.StartedAt
			if started.IsZero() {
				started = r.CreatedAt
			}
			if started.IsZero() {
				started = at
			}
			// The cost columns carry this scrape's increment, not a running
			// total: the upsert adds them to whatever the row already holds.
			jobs = append(jobs, s.client.JobObservation.Create().
				SetRunnerName(r.Name).
				SetSetName(r.SetName).
				SetRepository(r.Job.Repository).
				SetWorkflow(r.Job.Workflow).
				SetJobName(r.Job.Name).
				SetRunID(r.Job.RunID).
				SetStartedAt(started.Unix()).
				SetCPUSeconds(cpuDelta).
				SetMemByteSeconds(memDelta))
		}

		cur[r.Name] = next
	}

	// Runners that were there last time and are not there now. ARC deletes an
	// ephemeral runner as soon as its job completes, so disappearance is the
	// only completion signal the dashboard ever gets.
	var goneOK, goneFailed []string
	for name, prev := range s.prev {
		if _, still := cur[name]; still {
			continue
		}
		churn = append(churn, s.client.ChurnEvent.Create().
			SetRunnerName(name).
			SetSetName(prev.set).
			SetKind(churnTerminated).
			SetTs(ts))
		if !prev.job.Present() {
			continue
		}
		// A runner that vanished while its last observed state was Failed
		// failed; anything else is treated as success. This is a heuristic and
		// it is the best one available without the GitHub API: ARC tears down
		// a healthy runner the instant its job ends, so "gone" and "done" are
		// the same event.
		if prev.state == fleet.StateFailed {
			goneFailed = append(goneFailed, name)
		} else {
			goneOK = append(goneOK, name)
		}
	}

	if firstSnapshot && len(snap.Runners) > 0 {
		s.log.Debug().Int("runners", len(snap.Runners)).
			Msg("first snapshot: re-announcing existing runners, duplicates dropped by the churn unique index")
	}

	tx, err := s.client.Tx(ctx)
	if err != nil {
		return fmt.Errorf("begin snapshot transaction: %w", err)
	}
	w := snapshotWrite{
		samples:    samples,
		jobs:       jobs,
		phases:     phases,
		churn:      churn,
		ts:         ts,
		goneOK:     goneOK,
		goneFailed: goneFailed,
		switched:   switched,
	}
	if err := w.exec(ctx, tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback: %v)", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}

	s.prev = cur
	s.prevAt = at
	return nil
}

// jobClose finalises one job that ended without its runner disappearing.
//
// It carries the outcome and nothing else. Cost needs no flush here: every
// scrape adds its own increment to the job's row as it goes, so by the time
// the runner is seen on something else the row is already complete — and the
// interval straddling the handover was deliberately billed to the job that
// replaced this one.
type jobClose struct {
	runner string
	runID  int64
	job    string
	ok     bool
}

// snapshotWrite is everything one snapshot changes, staged so it can be
// applied in a single transaction. A crash mid-tick then leaves no
// half-recorded frame — half a frame would show up as phantom churn.
type snapshotWrite struct {
	samples    []*ent.SampleCreate
	jobs       []*ent.JobObservationCreate
	phases     []*ent.PhaseTransitionCreate
	churn      []*ent.ChurnEventCreate
	ts         int64
	goneOK     []string
	goneFailed []string
	switched   []jobClose
}

func (w snapshotWrite) exec(ctx context.Context, tx *ent.Tx) error {
	if len(w.samples) > 0 {
		err := tx.Sample.CreateBulk(w.samples...).
			OnConflict(entsql.ConflictColumns(
				sample.FieldScope, sample.FieldScopeID, sample.FieldMetric, sample.FieldTier, sample.FieldTs,
			)).
			UpdateNewValues().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("write samples: %w", err)
		}
	}

	if len(w.jobs) > 0 {
		// started_at must keep the first observation, and finished_at/succeeded
		// belong to the completion path below — letting the per-scrape upsert
		// touch them would resurrect finished jobs.
		//
		// The cost columns are added to rather than overwritten, because the
		// incoming row carries one scrape's increment. Accumulating in the
		// database instead of in memory is what makes the total survive a
		// restart: a fresh process contributes a zero increment for its first
		// scrape of a job it has never seen, where overwriting from its empty
		// accumulator would reset that job's total to zero and lose every
		// core-second recorded before the restart.
		err := tx.JobObservation.CreateBulk(w.jobs...).
			OnConflict(entsql.ConflictColumns(
				jobobservation.FieldRunnerName, jobobservation.FieldRunID, jobobservation.FieldJobName,
			)).
			Update(func(u *ent.JobObservationUpsert) {
				u.UpdateSetName()
				u.UpdateRepository()
				u.UpdateWorkflow()
				addExcluded(u, jobobservation.FieldCPUSeconds)
				addExcluded(u, jobobservation.FieldMemByteSeconds)
			}).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("write job observations: %w", err)
		}
	}

	if len(w.phases) > 0 {
		// ended_at is pushed forward on every scrape the phase survives, so a
		// runner that disappears already has a closed final phase and no
		// separate close pass is needed.
		err := tx.PhaseTransition.CreateBulk(w.phases...).
			OnConflict(entsql.ConflictColumns(
				phasetransition.FieldRunnerName, phasetransition.FieldPhase, phasetransition.FieldStartedAt,
			)).
			Update(func(u *ent.PhaseTransitionUpsert) {
				u.UpdateSetName()
				u.UpdateEndedAt()
			}).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("write phase transitions: %w", err)
		}
	}

	if len(w.churn) > 0 {
		err := tx.ChurnEvent.CreateBulk(w.churn...).
			OnConflict(entsql.ConflictColumns(churnevent.FieldRunnerName, churnevent.FieldKind)).
			Ignore().
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("write churn events: %w", err)
		}
	}

	// Two statements rather than one per runner, because the only thing that
	// varies within a tick's departures is the success flag.
	for _, g := range []struct {
		names []string
		ok    bool
	}{{w.goneOK, true}, {w.goneFailed, false}} {
		if len(g.names) == 0 {
			continue
		}
		_, err := tx.JobObservation.Update().
			Where(
				jobobservation.RunnerNameIn(g.names...),
				jobobservation.FinishedAtEQ(0),
			).
			SetFinishedAt(w.ts).
			SetSucceeded(g.ok).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("finish job observations: %w", err)
		}
	}

	for _, c := range w.switched {
		// Outcome only — the cost columns are the running upsert's business and
		// are already correct. finished_at = 0 in the predicate keeps a runner
		// that flapped back onto a job it had already left from re-closing it
		// with a second, later completion time.
		_, err := tx.JobObservation.Update().
			Where(
				jobobservation.RunnerNameEQ(c.runner),
				jobobservation.RunIDEQ(c.runID),
				jobobservation.JobNameEQ(c.job),
				jobobservation.FinishedAtEQ(0),
			).
			SetFinishedAt(w.ts).
			SetSucceeded(c.ok).
			Save(ctx)
		if err != nil {
			return fmt.Errorf("finish job %s on %s: %w", c.job, c.runner, err)
		}
	}
	return nil
}

// addExcluded resolves a conflicting insert by ADDING the incoming value to
// the stored one, where ent's generated Update<Field> would replace it.
//
// The bare column name on the right-hand side of a DO UPDATE SET is the row
// already in the table; `excluded` is the row that lost the conflict. Both are
// per-row, which is the whole point: a bulk upsert carries a different
// increment for every runner in the snapshot and shares one conflict clause
// between them, so the increment has to come from the statement's own values
// rather than from a literal bound once.
func addExcluded(u *ent.JobObservationUpsert, column string) {
	u.Set(column, entsql.ExprFunc(func(b *entsql.Builder) {
		b.Ident(column).WriteString(" + excluded.").Ident(column)
	}))
}

// sameJob compares the identity the job table is keyed on.
func sameJob(a, b fleet.Job) bool { return a.RunID == b.RunID && a.Name == b.Name }

// scopeMetrics turns a fleet aggregate into the samples worth storing.
//
// Absent dimensions are omitted rather than written as zero. That is the
// single most important rule in this file: a stored zero is indistinguishable
// from a measurement, and the two cases it would erase — listener metrics
// disabled, and metrics-server never having scraped a short-lived runner —
// are both routine.
func scopeMetrics(t fleet.Totals) []metricValue {
	out := []metricValue{
		{MetricRunners, float64(t.Runners)},
		{MetricBusy, float64(t.Busy)},
		{MetricIdle, float64(t.Idle)},
		{MetricPending, float64(t.Pending)},
		{MetricFailed, float64(t.Failed)},
	}
	if t.QueuedKnown {
		out = append(out, metricValue{MetricQueued, float64(t.Queued)})
	}
	if !t.Unbounded && t.Capacity > 0 {
		out = append(out, metricValue{MetricCapacity, float64(t.Capacity)})
	}
	if t.CPU.Request > 0 {
		out = append(out, metricValue{MetricCPURequest, t.CPU.Request})
	}
	if t.Mem.Request > 0 {
		out = append(out, metricValue{MetricMemRequest, t.Mem.Request})
	}
	if t.CPU.HasUsage() {
		out = append(out, metricValue{MetricCPUUsed, t.CPU.Used})
	}
	if t.Mem.HasUsage() {
		out = append(out, metricValue{MetricMemUsed, t.Mem.Used})
	}
	return out
}

// runnerMetrics is the per-runner series, which is only usage. Requests are
// not stored per runner: they come from the set's pod template and are already
// on the live snapshot, so persisting them per runner per scrape would be the
// single largest table in the database in exchange for nothing.
func runnerMetrics(r fleet.Runner) []metricValue {
	var out []metricValue
	if r.CPU.HasUsage() {
		out = append(out, metricValue{MetricCPUUsed, r.CPU.Used})
	}
	if r.Mem.HasUsage() {
		out = append(out, metricValue{MetricMemUsed, r.Mem.Used})
	}
	return out
}
