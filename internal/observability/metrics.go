package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	ElectionsTotal *prometheus.CounterVec
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		ElectionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "elections_total",
				Help:      "Total number of Raft elections started.",
			},
			[]string{"node_id"},
		),
	}

	registerer.MustRegister(metrics.ElectionsTotal)

	return metrics
}
