package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// RunnerFailure is one runner that broke, and why.
//
// It exists because the reason lives on an object ARC deletes. The failure lane
// was derived from the live snapshot, so it emptied seconds after the
// EphemeralRunner went away: an operator arriving a minute after a crash-looping
// scale set saw a healthy fleet and no evidence anything had happened. The row
// outlives the runner it describes.
type RunnerFailure struct {
	ent.Schema
}

// Fields of the RunnerFailure.
func (RunnerFailure) Fields() []ent.Field {
	return []ent.Field{
		field.String("runner_name"),
		field.String("set_name").Default(""),
		// The human-facing cause: a container waiting reason
		// ("ImagePullBackOff"), a termination reason ("OOMKilled"), the
		// EphemeralRunner's own status, or "never registered".
		field.String("reason"),
		// Severe separates a runner that will never work from one that merely
		// exited non-zero. The lane colours the two differently, and only this
		// one is worth waking up for.
		field.Bool("severe").Default(false),
		// When the failure was observed — the pod's or the runner's own
		// timestamp, not the scrape that noticed it.
		field.Int64("ts"),
	}
}

// Indexes of the RunnerFailure.
func (RunnerFailure) Indexes() []ent.Index {
	return []ent.Index{
		// An ephemeral runner's name is unique and a reason does not come and go
		// under it, so this pair is the natural key. It is what makes recording
		// idempotent across the many scrapes that see the same broken runner,
		// and across a restart that re-announces every one of them: without it
		// a runner stuck in ImagePullBackOff for ten minutes would bury every
		// other failure in the window under forty copies of itself.
		//
		// A runner whose reason genuinely changes — waiting, then killed — keeps
		// a row for each, because those are two different facts.
		index.Fields("runner_name", "reason").Unique(),
		index.Fields("set_name", "ts"),
		index.Fields("ts"),
	}
}
