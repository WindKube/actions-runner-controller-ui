package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// ChurnEvent is a runner appearing or disappearing.
//
// Churn is the signal that tells an operator whether a scale set is doing
// useful work or thrashing: an ephemeral runner is created and destroyed for
// every single job, so the created/terminated rate *is* the job rate, and a
// created rate far above the terminated rate means pods are piling up.
type ChurnEvent struct {
	ent.Schema
}

// Fields of the ChurnEvent.
func (ChurnEvent) Fields() []ent.Field {
	return []ent.Field{
		field.String("runner_name"),
		field.String("set_name").Default(""),
		// "created" or "terminated".
		field.String("kind"),
		field.Int64("ts"),
	}
}

// Indexes of the ChurnEvent.
func (ChurnEvent) Indexes() []ent.Index {
	return []ent.Index{
		// A runner is created once and terminated once, so this pair is the
		// natural key. It is what makes churn recording idempotent across a
		// process restart: the store re-announces every runner it can see on
		// its first scrape, and the duplicates are dropped here rather than
		// doubling the chart.
		index.Fields("runner_name", "kind").Unique(),
		index.Fields("set_name", "ts"),
		index.Fields("ts"),
	}
}
