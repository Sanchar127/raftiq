package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initRPC() {
	m.RPCRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "rpc",
			Name:      "requests_total",
			Help:      "Total number of RPC requests.",
		},
		[]string{"method"},
	)

	m.RPCErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "rpc",
			Name:      "errors_total",
			Help:      "Total number of RPC errors.",
		},
		[]string{"method"},
	)

	m.RPCDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "raftiq",
			Subsystem: "rpc",
			Name:      "duration_seconds",
			Help:      "RPC request duration.",
		},
		[]string{"method"},
	)
}

// The transport package defines RPCMetrics with the same three methods.
// We intentionally do not import transport here; Go interfaces are satisfied
// structurally.

func (m *Metrics) IncRPCRequest(method string) {
	m.RPCRequestsTotal.
		WithLabelValues(method).
		Inc()
}

func (m *Metrics) IncRPCError(method string) {
	m.RPCErrorsTotal.
		WithLabelValues(method).
		Inc()
}

func (m *Metrics) ObserveRPCDuration(
	method string,
	duration time.Duration,
) {
	m.RPCDuration.
		WithLabelValues(method).
		Observe(duration.Seconds())
}
