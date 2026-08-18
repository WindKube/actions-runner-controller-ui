package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// JobObservation is one workflow job as the dashboard watched it run.
//
// It is an *observation*, not a record from GitHub: the dashboard never talks
// to the GitHub API. Everything here is inferred from the runner that carried
// the job, which is why cpu_seconds exists at all — it is integrated from the
// metrics-server samples taken while the job was assigned, and is the only
// route to "which repository is actually consuming the fleet" over time.
type JobObservation struct {
	ent.Schema
}

// Fields of the JobObservation.
func (JobObservation) Fields() []ent.Field {
	return []ent.Field{
		field.String("runner_name"),
		field.String("set_name").Default(""),
		field.String("repository").Default(""),
		field.String("workflow").Default(""),
		field.String("job_name").Default(""),
		field.Int64("run_id").Default(0),
		field.Int64("started_at"),
		// Zero means "still running". A nullable column would be more
		// expressive but would break the "still running" half of every query
		// that also filters on a window, and NULL in an index costs more than
		// the sentinel does.
		field.Int64("finished_at").Default(0),
		field.Bool("succeeded").Default(false),
		// Integrated over the life of the job, in core-seconds and
		// byte-seconds. Both are lower bounds: a runner that dies before
		// metrics-server first scrapes it contributes nothing.
		field.Float("cpu_seconds").Default(0),
		field.Float("mem_byte_seconds").Default(0),
	}
}

// Indexes of the JobObservation.
func (JobObservation) Indexes() []ent.Index {
	return []ent.Index{
		// An ARC ephemeral runner executes exactly one job, but a persistent
		// runner does not, so the identity is the runner plus the job it ran
		// rather than the runner alone. This is the conflict target that lets
		// every scrape upsert the running job's accumulated cost.
		index.Fields("runner_name", "run_id", "job_name").Unique(),
		index.Fields("set_name", "started_at"),
		index.Fields("repository", "started_at"),
		index.Fields("finished_at"),
	}
}
