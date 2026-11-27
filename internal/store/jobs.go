package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"arc-ui/internal/fleet"
	"arc-ui/internal/store/ent"
	"arc-ui/internal/store/ent/job"
)

const defaultJobRows = 100

// Observe upserts a row for every job a runner holds and closes the open rows
// whose runner has finished or gone. It returns the rows it closed.
//
// Rows are only closed for a vanished runner when the snapshot is complete;
// a half-synced informer cache would otherwise end every running job. The
// first complete snapshot after boot is the sweep for rows left open by the
// previous process, which end at when they were last seen.
func (s *Store) Observe(ctx context.Context, snap fleet.Snapshot) ([]JobRecord, error) {
	now := snap.At
	if now.IsZero() {
		return nil, nil
	}
	ts := now.Unix()
	complete := snapshotComplete(snap)

	tx, err := s.client.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin observe: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	runners := make(map[string]fleet.Runner, len(snap.Runners))
	for _, r := range snap.Runners {
		runners[r.Namespace+"/"+r.Name] = r
		if r.Job.ID == "" {
			continue
		}
		start := r.Job.StartedAt
		if start.IsZero() {
			start = now
		}
		err := tx.Job.Create().
			SetJobID(r.Job.ID).
			SetWorkflowRunID(r.Job.RunID).
			SetRepository(r.Job.Repository).
			SetWorkflowRef(r.Job.WorkflowRef).
			SetWorkflow(r.Job.Workflow).
			SetName(r.Job.Name).
			SetRunner(r.Name).
			SetNamespace(r.Namespace).
			SetScaleSet(r.SetName).
			SetStartedAt(start.Unix()).
			SetLastSeenAt(ts).
			SetCPURequest(r.CPU.Request).
			SetCPULimit(r.CPU.Limit).
			SetMemRequest(r.Mem.Request).
			SetMemLimit(r.Mem.Limit).
			OnConflictColumns(job.FieldJobID).
			Update(func(u *ent.JobUpsert) {
				u.UpdateWorkflowRunID()
				u.UpdateRepository()
				u.UpdateWorkflowRef()
				u.UpdateWorkflow()
				u.UpdateName()
				u.UpdateRunner()
				u.UpdateNamespace()
				u.UpdateScaleSet()
				u.UpdateLastSeenAt()
				u.UpdateCPURequest()
				u.UpdateCPULimit()
				u.UpdateMemRequest()
				u.UpdateMemLimit()
			}).
			Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("upsert job %s: %w", r.Job.ID, err)
		}
	}

	open, err := tx.Job.Query().Where(job.FinishedAt(0)).All(ctx)
	if err != nil {
		return nil, fmt.Errorf("list open jobs: %w", err)
	}
	var closed []JobRecord
	for _, j := range open {
		r, present := runners[j.Namespace+"/"+j.Runner]
		holds := present && r.Job.ID == j.JobID
		end := ts
		switch {
		case holds && !podDone(r):
			continue
		case !present && !complete:
			continue
		case !holds && j.LastSeenAt > 0:
			end = j.LastSeenAt
		}
		outcome, reason := OutcomeSucceeded, ""
		if present && (r.FailureReason != "" || r.PodPhase == "Failed") {
			outcome, reason = OutcomeFailed, r.FailureReason
		}
		end = max(end, j.StartedAt)
		row, err := j.Update().SetFinishedAt(end).SetOutcome(string(outcome)).SetFailureReason(reason).Save(ctx)
		if err != nil {
			return nil, fmt.Errorf("close job %s: %w", j.JobID, err)
		}
		closed = append(closed, recordOf(row))
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit observe: %w", err)
	}
	return closed, nil
}

func podDone(r fleet.Runner) bool { return r.PodPhase == "Succeeded" || r.PodPhase == "Failed" }

func snapshotComplete(s fleet.Snapshot) bool {
	src, ok := s.Source(fleet.SourceARCCRDs)
	return ok && src.Available
}

// SetUsage stores a closed job's CPU core-seconds and peak memory.
func (s *Store) SetUsage(ctx context.Context, id int, cpuSeconds, memPeak float64) error {
	if err := s.client.Job.UpdateOneID(id).SetCPUSeconds(cpuSeconds).SetMemPeakBytes(memPeak).Exec(ctx); err != nil {
		return fmt.Errorf("set job %d usage: %w", id, err)
	}
	return nil
}

// Prune deletes jobs that finished before cutoff.
func (s *Store) Prune(ctx context.Context, cutoff time.Time) (int, error) {
	n, err := s.client.Job.Delete().Where(job.FinishedAtGT(0), job.FinishedAtLT(cutoff.Unix())).Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("prune jobs: %w", err)
	}
	return n, nil
}

func recordOf(j *ent.Job) JobRecord {
	out := JobRecord{
		ID: j.ID, JobID: j.JobID, RunID: j.WorkflowRunID,
		Repository: j.Repository, WorkflowRef: j.WorkflowRef, Workflow: j.Workflow, Name: j.Name,
		Runner: j.Runner, Namespace: j.Namespace, Set: j.ScaleSet,
		StartedAt: fromUnix(j.StartedAt), FinishedAt: fromUnix(j.FinishedAt),
		Outcome: Outcome(j.Outcome), FailureReason: j.FailureReason,
		CPUSeconds: j.CPUSeconds, MemPeakBytes: j.MemPeakBytes,
		CPURequest: j.CPURequest, CPULimit: j.CPULimit, MemRequest: j.MemRequest, MemLimit: j.MemLimit,
	}
	if out.Running() {
		out.Outcome = OutcomeRunning
	}
	return out
}

func (s *Store) Job(ctx context.Context, id int) (JobRecord, bool, error) {
	j, err := s.client.Job.Get(ctx, id)
	switch {
	case ent.IsNotFound(err):
		return JobRecord{}, false, nil
	case err != nil:
		return JobRecord{}, false, fmt.Errorf("get job %d: %w", id, err)
	}
	return recordOf(j), true, nil
}

// jobWindow is overlap, not start: a long job stuck through the window is the
// one most worth listing.
const jobWindow = `started_at < ? AND (finished_at = 0 OR finished_at >= ?)`

const likeEscape = `\`

func escapeLike(s string) string {
	s = strings.ReplaceAll(s, likeEscape, likeEscape+likeEscape)
	s = strings.ReplaceAll(s, "%", likeEscape+"%")
	return strings.ReplaceAll(s, "_", likeEscape+"_")
}

// jobWhere binds every user value as an argument.
func jobWhere(f JobFilter, from, to time.Time) (string, []any) {
	clauses := []string{jobWindow}
	args := []any{to.Unix(), from.Unix()}
	add := func(clause string, a ...any) {
		clauses = append(clauses, clause)
		args = append(args, a...)
	}
	if f.Repository != "" {
		add("repository = ?", f.Repository)
	}
	if f.Workflow != "" {
		add("workflow = ?", f.Workflow)
	}
	if f.Set != "" {
		add("scale_set = ?", f.Set)
	}
	if f.RunID > 0 {
		add("workflow_run_id = ?", f.RunID)
	}
	switch f.Outcome {
	case OutcomeRunning:
		add("finished_at = 0")
	case OutcomeSucceeded, OutcomeFailed:
		add("finished_at > 0 AND outcome = ?", string(f.Outcome))
	case OutcomeAny:
	}
	if q := strings.TrimSpace(f.Search); q != "" {
		p := "%" + escapeLike(strings.ToLower(q)) + "%"
		esc := ` ESCAPE '` + likeEscape + `'`
		add(`(LOWER(workflow) LIKE ?`+esc+` OR LOWER(name) LIKE ?`+esc+` OR LOWER(repository) LIKE ?`+esc+`)`, p, p, p)
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func limitOf(f JobFilter) int {
	if f.Limit <= 0 {
		return defaultJobRows
	}
	return f.Limit
}

// Jobs lists jobs overlapping [from, to), newest first, with the window total.
func (s *Store) Jobs(ctx context.Context, f JobFilter, from, to time.Time) ([]JobRecord, int64, error) {
	where, args := jobWhere(f, from, to)

	var total int64
	// #nosec G202 -- only this file's own clause text is concatenated
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs`+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	ids := make([]int, 0, limitOf(f))
	// #nosec G202 -- only this file's own clause text is concatenated
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM jobs`+where+` ORDER BY started_at DESC, id DESC LIMIT ?`, append(args, limitOf(f))...)
	if err != nil {
		return nil, 0, fmt.Errorf("query jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, 0, fmt.Errorf("scan job id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read job ids: %w", err)
	}

	found, err := s.client.Job.Query().Where(job.IDIn(ids...)).All(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("load jobs: %w", err)
	}
	byID := make(map[int]*ent.Job, len(found))
	for _, j := range found {
		byID[j.ID] = j
	}
	out := make([]JobRecord, 0, len(ids))
	for _, id := range ids {
		if j, ok := byID[id]; ok {
			out = append(out, recordOf(j))
		}
	}
	return out, total, nil
}

// WorkflowRuns groups jobs by (repository, workflow, run id), newest first.
func (s *Store) WorkflowRuns(ctx context.Context, f JobFilter, from, to time.Time) ([]WorkflowRun, int64, error) {
	where, args := jobWhere(f, from, to)
	const grouping = ` GROUP BY repository, workflow, workflow_run_id`

	var total int64
	// #nosec G202 -- only this file's own clause text is concatenated
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT 1 FROM jobs`+where+grouping+`)`, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count workflow runs: %w", err)
	}

	// #nosec G202 -- only this file's own clause text is concatenated
	q := `SELECT repository, workflow, workflow_run_id, COUNT(*),
	             SUM(CASE WHEN finished_at = 0 THEN 1 ELSE 0 END),
	             SUM(CASE WHEN finished_at > 0 AND outcome = 'failed' THEN 1 ELSE 0 END),
	             MIN(started_at), MAX(finished_at), SUM(cpu_seconds), MAX(mem_peak_bytes)
	      FROM jobs` + where + grouping + ` ORDER BY MIN(started_at) DESC, workflow_run_id DESC LIMIT ?`
	rows, err := s.db.QueryContext(ctx, q, append(args, limitOf(f))...)
	if err != nil {
		return nil, 0, fmt.Errorf("query workflow runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []WorkflowRun
	for rows.Next() {
		var (
			w                 WorkflowRun
			started, finished int64
			cpu, mem          sql.NullFloat64
		)
		if err := rows.Scan(&w.Repository, &w.Workflow, &w.RunID, &w.Jobs, &w.Running, &w.Failed,
			&started, &finished, &cpu, &mem); err != nil {
			return nil, 0, fmt.Errorf("scan workflow run: %w", err)
		}
		w.StartedAt = fromUnix(started)
		if w.Running == 0 {
			w.FinishedAt = fromUnix(finished)
		}
		w.CPUSeconds, w.MemPeakBytes = cpu.Float64, mem.Float64
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read workflow runs: %w", err)
	}
	return out, total, nil
}

// Failures lists jobs whose runner failed in [from, to), newest first.
func (s *Store) Failures(ctx context.Context, set string, from, to time.Time, limit int) ([]JobRecord, int64, error) {
	q := s.client.Job.Query().Where(
		job.OutcomeEQ(string(OutcomeFailed)),
		job.FinishedAtGTE(from.Unix()),
		job.FinishedAtLT(to.Unix()),
	)
	if set != "" {
		q = q.Where(job.ScaleSetEQ(set))
	}
	total, err := q.Clone().Count(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("count failures: %w", err)
	}
	rows, err := q.Order(ent.Desc(job.FieldFinishedAt), ent.Desc(job.FieldID)).Limit(max(limit, 1)).All(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("query failures: %w", err)
	}
	out := make([]JobRecord, 0, len(rows))
	for _, j := range rows {
		out = append(out, recordOf(j))
	}
	return out, int64(total), nil
}

// Facets returns the distinct values the window holds, for filter dropdowns.
func (s *Store) Facets(ctx context.Context, from, to time.Time) (JobFacets, error) {
	var out JobFacets
	for _, c := range []struct {
		column string
		into   *[]string
	}{
		{"repository", &out.Repositories},
		{"workflow", &out.Workflows},
		{"scale_set", &out.Sets},
	} {
		// #nosec G202 -- column names come from the literal list above
		rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT `+c.column+` FROM jobs WHERE `+jobWindow+` AND `+c.column+` != '' ORDER BY 1`,
			to.Unix(), from.Unix())
		if err != nil {
			return out, fmt.Errorf("facet %s: %w", c.column, err)
		}
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				_ = rows.Close()
				return out, fmt.Errorf("scan facet %s: %w", c.column, err)
			}
			*c.into = append(*c.into, v)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return out, fmt.Errorf("read facet %s: %w", c.column, err)
		}
	}
	return out, nil
}
