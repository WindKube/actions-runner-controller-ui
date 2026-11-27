package promql

import (
	"context"
	"math"
	"time"

	"github.com/rs/zerolog"

	"arc-ui/internal/fleet"
)

// Sink receives live readings; the collector implements it.
type Sink interface {
	SetUsage(map[string]fleet.Usage)
	SetQueueDepth(perSet map[string]int, known bool)
}

// Live refreshes per-pod usage and per-set queue depth for the live tables.
type Live struct {
	Client   Querier
	Queries  Queries
	Interval time.Duration
	Sink     Sink
	Log      zerolog.Logger
}

func (l Live) Run(ctx context.Context) error {
	ticker := time.NewTicker(l.Interval)
	defer ticker.Stop()
	for {
		l.Tick(ctx, time.Now())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Tick runs one refresh. A failed query clears its readings rather than
// leaving stale ones on screen: no reading renders "—", never zero.
func (l Live) Tick(ctx context.Context, now time.Time) {
	usage := map[string]fleet.Usage{}
	cpu, cpuErr := l.Client.Query(ctx, l.Queries.PodCPU())
	mem, memErr := l.Client.Query(ctx, l.Queries.PodMem())
	if cpuErr != nil || memErr != nil {
		l.Log.Debug().AnErr("cpu", cpuErr).AnErr("mem", memErr).Msg("pod usage unavailable")
	} else {
		for _, s := range cpu {
			key := s.Labels["namespace"] + "/" + s.Labels["pod"]
			u := usage[key]
			u.CPUCores, u.At = s.Value, now
			usage[key] = u
		}
		for _, s := range mem {
			key := s.Labels["namespace"] + "/" + s.Labels["pod"]
			u := usage[key]
			u.MemBytes, u.At = s.Value, now
			usage[key] = u
		}
	}
	l.Sink.SetUsage(usage)

	rows, err := l.Client.Query(ctx, l.Queries.Queue())
	if err != nil {
		l.Log.Debug().Err(err).Msg("queue depth unavailable")
		l.Sink.SetQueueDepth(nil, false)
		return
	}
	queue := make(map[string]int, len(rows))
	for _, s := range rows {
		if set := s.Labels[l.Queries.Schema.ScaleSetLabel]; set != "" {
			queue[set] = int(math.Round(s.Value))
		}
	}
	l.Sink.SetQueueDepth(queue, len(queue) > 0)
}
