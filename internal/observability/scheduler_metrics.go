package observability

import "github.com/sanchar127/raftiq/internal/scheduler"

// SchedulerMetrics adapts scheduler metrics to Prometheus metrics.
type SchedulerMetrics struct {
	nodeID  string
	metrics *Metrics
}

// NewSchedulerMetrics creates a Prometheus-backed scheduler metrics adapter.
func NewSchedulerMetrics(
	nodeID string,
	metrics *Metrics,
) *SchedulerMetrics {
	if nodeID == "" {
		panic("nodeID must not be empty")
	}

	if metrics == nil {
		panic("metrics must not be nil")
	}

	return &SchedulerMetrics{
		nodeID:  nodeID,
		metrics: metrics,
	}
}

func (m *SchedulerMetrics) IncScheduledJobs() {
	m.metrics.ScheduledJobsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *SchedulerMetrics) IncLeaseAcquisitions() {
	m.metrics.LeaseAcquisitionsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *SchedulerMetrics) IncLeaseLosses() {
	m.metrics.LeaseLossesTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

// Compile-time interface check.
var _ scheduler.SchedulerMetrics = (*SchedulerMetrics)(nil)
