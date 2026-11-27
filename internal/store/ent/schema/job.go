package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Job is one workflow job as its EphemeralRunner reported it. The outcome is
// the runner's, not GitHub's: the cluster never sees the job's conclusion.
type Job struct {
	ent.Schema
}

func (Job) Fields() []ent.Field {
	return []ent.Field{
		field.String("job_id").Unique().Immutable(),
		field.Int64("workflow_run_id").Default(0),
		field.String("repository").Default(""),
		field.String("workflow_ref").Default(""),
		field.String("workflow").Default(""),
		field.String("name").Default(""),
		field.String("runner").Default(""),
		field.String("namespace").Default(""),
		field.String("scale_set").Default(""),
		field.Int64("started_at"),
		// Zero while the job is open; an index on a sentinel is cheaper than NULL.
		field.Int64("finished_at").Default(0),
		// A row left open across a restart is closed at this, not at boot time.
		field.Int64("last_seen_at").Default(0),
		field.String("outcome").Default(""),
		field.String("failure_reason").Default(""),
		field.Float("cpu_seconds").Default(0),
		field.Float("mem_peak_bytes").Default(0),
		field.Float("cpu_request").Default(0),
		field.Float("cpu_limit").Default(0),
		field.Float("mem_request").Default(0),
		field.Float("mem_limit").Default(0),
	}
}

func (Job) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("started_at"),
		index.Fields("finished_at"),
		index.Fields("repository", "started_at"),
		index.Fields("workflow_run_id"),
	}
}
