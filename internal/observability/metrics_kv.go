package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initKV() {
	m.KVOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "kv",
			Name:      "operations_total",
			Help:      "Total number of KV operations.",
		},
		[]string{"operation"},
	)

	m.KVOperationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "kv",
			Name:      "operation_errors_total",
			Help:      "Total number of failed KV operations.",
		},
		[]string{"operation"},
	)
}
