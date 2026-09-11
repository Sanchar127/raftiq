package observability

import (
	"github.com/sanchar127/raftiq/internal/kv"
)

type KVMetrics struct {
	metrics *Metrics
}

func NewKVMetrics(metrics *Metrics) *KVMetrics {
	if metrics == nil {
		panic("metrics must not be nil")
	}

	return &KVMetrics{
		metrics: metrics,
	}
}

func (m *KVMetrics) IncOperation(operation string) {
	m.metrics.KVOperationsTotal.WithLabelValues(operation).Inc()
}

func (m *KVMetrics) IncOperationError(operation string) {
	m.metrics.KVOperationErrors.WithLabelValues(operation).Inc()
}

var _ kv.KVMetrics = (*KVMetrics)(nil)
