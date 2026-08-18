// Package schema declares the ent entities backing the history store.
//
// Four tables, one job each: Sample holds every numeric time series, and the
// other three hold the discrete events the charts count rather than average.
// Keeping counts out of Sample matters, because rolling a gauge up to a
// coarser tier means averaging it while rolling a count up means summing it,
// and a single table would have to carry that distinction per row.
//
// Every timestamp in this schema is an int64 of Unix seconds rather than a
// time.Time. Three reasons: bucketing a range query is integer division on
// that column and SQL cannot do that to a datetime without a string round
// trip; the index entries are half the size; and there is no dialect-specific
// question about how a time is encoded on disk. Sub-second precision is not
// lost in practice because the scrape interval floor is one second.
package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Sample is one numeric observation of one metric, for one scope, at one
// instant, in one resolution tier.
//
// scope/scope_id/metric/tier are plain strings rather than ent enums. The
// vocabularies are already exported from the store package as its public API
// (store.Scope, store.Metric, store.Tier); generating a parallel set of ent
// enum types would duplicate them and force a conversion at every call site
// for validation that the store's own constructors already provide.
type Sample struct {
	ent.Schema
}

// Fields of the Sample.
func (Sample) Fields() []ent.Field {
	return []ent.Field{
		// Unix seconds. For a rolled-up row this is the *start* of the bucket,
		// which is what makes rollup idempotent: re-deriving a bucket lands on
		// the same key and updates instead of inserting a duplicate.
		field.Int64("ts"),
		field.String("scope"),
		// Empty for the fleet scope, the set name for a set, the runner name
		// for a runner. Empty-string rather than NULL keeps the unique index
		// usable: in SQLite every NULL is distinct, so a nullable column in a
		// unique index would silently permit duplicate fleet rows.
		field.String("scope_id").Default(""),
		field.String("tier"),
		field.String("metric"),
		field.Float("value"),
	}
}

// Indexes of the Sample.
func (Sample) Indexes() []ent.Index {
	return []ent.Index{
		// The exact shape of every range query, and the conflict target for
		// both snapshot writes and rollups.
		index.Fields("scope", "scope_id", "metric", "tier", "ts").Unique(),
		// Compaction and retention sweep by tier and age, ignoring scope.
		index.Fields("tier", "ts"),
	}
}
