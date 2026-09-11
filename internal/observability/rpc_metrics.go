package observability

import (
	"time"
)

type RPCMetrics struct {
	metrics *Metrics
}

func NewRPCMetrics(metrics *Metrics) *RPCMetrics {
	if metrics == nil {
		panic("metrics must not be nil")
	}

	return &RPCMetrics{
		metrics: metrics,
	}
}

func (m *RPCMetrics) IncRPCRequest(method string) {
	m.metrics.RPCRequestsTotal.WithLabelValues(method).Inc()
}

func (m *RPCMetrics) IncRPCError(method string) {
	m.metrics.RPCErrorsTotal.WithLabelValues(method).Inc()
}

func (m *RPCMetrics) ObserveRPCDuration(
	method string,
	duration time.Duration,
) {
	m.metrics.RPCDuration.
		WithLabelValues(method).
		Observe(duration.Seconds())
}
