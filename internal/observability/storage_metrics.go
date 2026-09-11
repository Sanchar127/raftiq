package observability

import (
	"time"

	"github.com/sanchar127/raftiq/internal/storage"
)

// StorageMetrics adapts Prometheus metrics to the storage.StorageMetrics
// interface without coupling the storage package to Prometheus.
type StorageMetrics struct {
	metrics *Metrics
	nodeID  string
}

// NewStorageMetrics creates a Prometheus-backed storage metrics adapter.
func NewStorageMetrics(nodeID string, metrics *Metrics) *StorageMetrics {
	if metrics == nil {
		panic("metrics must not be nil")
	}

	if nodeID == "" {
		panic("nodeID must not be empty")
	}

	return &StorageMetrics{
		metrics: metrics,
		nodeID:  nodeID,
	}
}

func (m *StorageMetrics) IncOperation(operation string) {
	m.metrics.StorageOperationsTotal.
		WithLabelValues(operation).
		Inc()
}

func (m *StorageMetrics) IncOperationError(operation string) {
	m.metrics.StorageOperationErrors.
		WithLabelValues(operation).
		Inc()
}

func (m *StorageMetrics) ObserveOperationDuration(
	operation string,
	duration time.Duration,
) {
	m.metrics.StorageOperationDuration.
		WithLabelValues(operation).
		Observe(duration.Seconds())
}

func (m *StorageMetrics) IncSync() {
	m.metrics.StorageSyncTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *StorageMetrics) IncSyncError() {
	m.metrics.StorageSyncErrors.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *StorageMetrics) ObserveSyncDuration(duration time.Duration) {
	m.metrics.StorageSyncDuration.
		WithLabelValues(m.nodeID).
		Observe(duration.Seconds())
}

var _ storage.StorageMetrics = (*StorageMetrics)(nil)
