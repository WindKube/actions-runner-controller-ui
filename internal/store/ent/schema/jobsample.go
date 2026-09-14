package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// JobSample is one bucket of resource usage observed while a job was running.
//
// It exists because the job detail view has to answer "what did this job cost"
// for a job that finished weeks ago, and the per-runner samples that carry the
// numbers are deleted after fifteen minutes. Those samples are not merely
// retained longer here: they are written only while a runner actually holds a
// job, which drops every idle, pending and terminating runner-minute the raw
// tier would otherwise keep for a month.
//
// Unlike Sample this is two metrics per row rather than one metric per row.
// The pair is always written together and always read together, so splitting
// them would double the row count and the index for nothing.
type JobSample struct {
	ent.Schema
}

// Fields of the JobSample.
func (JobSample) Fields() []ent.Field {
	return []ent.Field{
		// The job_observations row this belongs to. A surrogate key rather
		// than the job's natural (runner, run, name) triple, which would
		// duplicate some sixty bytes of text onto every sample row.
		field.Int("job_id"),
		// Unix seconds, floored to the start of the bucket. Flooring is what
		// makes the write idempotent: every scrape inside one bucket lands on
		// the same key and averages into the row already there.
		field.Int64("ts"),
		field.Float("cpu_cores").Default(0),
		field.Float("mem_bytes").Default(0),
		// How many scrapes were averaged into this bucket. Carried so that a
		// later scrape can fold itself into the running mean without re-reading
		// the samples that formed it: mean' = mean + (x-mean)/(n+1).
		field.Int("samples").Default(0),
	}
}

// Indexes of the JobSample.
func (JobSample) Indexes() []ent.Index {
	return []ent.Index{
		// One row per job per bucket, and the conflict target the per-scrape
		// update folds into. It doubles as the range scan the detail chart
		// makes, which always reads one job's buckets in time order.
		index.Fields("job_id", "ts").Unique(),
	}
}
