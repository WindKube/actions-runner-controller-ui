package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// PhaseTransition is one contiguous stretch a runner spent in one state.
//
// The phase vocabulary is fleet.State (pending/idle/busy/terminating/failed)
// rather than ARC's pod phases. ARC's EphemeralRunner phase cannot tell an
// idle runner from a busy one — both report a Running pod — and the whole
// point of the lifecycle bar on the runner detail page is that distinction.
type PhaseTransition struct {
	ent.Schema
}

// Fields of the PhaseTransition.
func (PhaseTransition) Fields() []ent.Field {
	return []ent.Field{
		field.String("runner_name"),
		field.String("set_name").Default(""),
		field.String("phase"),
		field.Int64("started_at"),
		// The last instant the runner was observed in this phase, not the
		// instant it left. The dashboard only sees the fleet at scrape
		// resolution, so a phase's true end is somewhere in the scrape after
		// the one recorded here; claiming otherwise would be a lie of
		// precision. It is refreshed on every scrape while the phase holds, so
		// a runner that vanishes still has a closed final phase.
		field.Int64("ended_at").Default(0),
	}
}

// Indexes of the PhaseTransition.
func (PhaseTransition) Indexes() []ent.Index {
	return []ent.Index{
		// A runner can re-enter a phase (idle, busy, idle), so started_at is
		// part of the identity. Conflict target for the per-scrape refresh.
		index.Fields("runner_name", "phase", "started_at").Unique(),
		index.Fields("runner_name", "started_at"),
		index.Fields("started_at"),
	}
}
