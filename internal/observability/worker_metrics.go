package observability

import "github.com/sanchar127/raftiq/internal/worker"

// WorkerMetrics adapts worker metrics to Prometheus metrics.
type WorkerMetrics struct {
	nodeID  string
	metrics *Metrics
}

// NewWorkerMetrics creates a Prometheus-backed worker metrics adapter.
func NewWorkerMetrics(
	nodeID string,
	metrics *Metrics,
) *WorkerMetrics {
	if nodeID == "" {
		panic("nodeID must not be empty")
	}

	if metrics == nil {
		panic("metrics must not be nil")
	}

	return &WorkerMetrics{
		nodeID:  nodeID,
		metrics: metrics,
	}
}

func (m *WorkerMetrics) IncExecutedJobs() {
	m.metrics.ExecutedJobsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *WorkerMetrics) IncJobExecutionFailures() {
	m.metrics.JobExecutionFailures.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *WorkerMetrics) IncLeaseLosses() {
	m.metrics.LeaseLossesTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

// Compile-time interface check.
var _ worker.WorkerMetrics = (*WorkerMetrics)(nil)
