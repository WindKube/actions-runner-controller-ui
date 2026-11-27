package web

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/url"
	"time"

	"arc-ui/internal/alert"
	"arc-ui/internal/fleet"
)

type AlertSource interface {
	State() alert.State
}

// toastWindow is how long after firing an alert still pops up as a toast on
// a newly opened page; after that it is only in the list.
const toastWindow = 10 * time.Minute

type AlertRow struct {
	Alert  alert.Alert
	Tone   Tone
	SetURL string
	Age    string
	// Signal is the local Datastar signal that hides the alert's toast once
	// dismissed.
	Signal string
	Toast  bool
}

type RuleRow struct {
	Rule  alert.RuleStatus
	Tone  Tone
	State string
}

type AlertsView struct {
	Page
	Enabled   bool
	Evaluated string
	Firing    []AlertRow
	Resolved  []AlertRow
	Rules     []RuleRow
}

func severityTone(s alert.Severity) Tone {
	if s == alert.Critical {
		return ToneDanger
	}
	return ToneAttention
}

func alertRow(a alert.Alert, now time.Time) AlertRow {
	h := fnv.New64a()
	_, _ = h.Write([]byte(a.Key()))
	row := AlertRow{
		Alert:  a,
		Tone:   severityTone(a.Severity),
		Signal: fmt.Sprintf("_alert%x", h.Sum64()),
		Toast:  a.ResolvedAt.IsZero() && now.Sub(a.FiredAt) < toastWindow,
	}
	if a.Set != "" {
		row.SetURL = "/runnersets/" + url.PathEscape(a.Set)
	}
	if a.ResolvedAt.IsZero() {
		row.Age = "for " + fleet.FormatAge(now.Sub(a.Since))
	} else {
		row.Age = "resolved " + fleet.FormatRelative(a.ResolvedAt, now)
		row.Tone = ToneMuted
	}
	return row
}

func (b *Builder) firingAlerts(now time.Time) []AlertRow {
	if b.Alerts == nil {
		return nil
	}
	firing := b.Alerts.State().Firing
	out := make([]AlertRow, 0, len(firing))
	for _, a := range firing {
		out = append(out, alertRow(a, now))
	}
	return out
}

func (b *Builder) AlertsPage(_ context.Context, sig Signals, now time.Time) AlertsView {
	sig = sig.Normalize()
	snap := b.Fleet.Snapshot()
	page := b.page("Alerts", snap, sig, now, []Crumb{{Label: snap.Org}, {Label: "alerts"}})
	page.Tab = TabAlerts

	v := AlertsView{Page: page, Enabled: b.Alerts != nil, Firing: page.Alerts}
	if !v.Enabled {
		return v
	}
	s := b.Alerts.State()
	v.Evaluated = fleet.FormatRelative(s.Evaluated, now)
	for _, a := range s.Resolved {
		v.Resolved = append(v.Resolved, alertRow(a, now))
	}
	for _, r := range s.Rules {
		row := RuleRow{Rule: r, Tone: ToneSuccess, State: "ok"}
		switch {
		case r.Unavailable != "":
			row.Tone, row.State = ToneMuted, "unavailable"
		case r.Firing > 0:
			row.Tone, row.State = severityTone(r.Severity), fmt.Sprintf("%d firing", r.Firing)
		}
		v.Rules = append(v.Rules, row)
	}
	return v
}
