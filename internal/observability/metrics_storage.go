package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initStorage() {
	m.StorageOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "storage",
			Name:      "operations_total",
			Help:      "Total number of storage operations.",
		},
		[]string{"operation"},
	)

	m.StorageOperationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "storage",
			Name:      "operation_errors_total",
			Help:      "Total number of failed storage operations.",
		},
		[]string{"operation"},
	)

	m.StorageOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "raftiq",
			Subsystem: "storage",
			Name:      "operation_duration_seconds",
			Help:      "Storage operation duration.",
		},
		[]string{"operation"},
	)

	m.StorageSyncTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "storage",
			Name:      "sync_total",
			Help:      "Total number of storage synchronization operations.",
		},
		[]string{"node_id"},
	)

	m.StorageSyncErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "storage",
			Name:      "sync_errors_total",
			Help:      "Total number of failed storage synchronization operations.",
		},
		[]string{"node_id"},
	)

	m.StorageSyncDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "raftiq",
			Subsystem: "storage",
			Name:      "sync_duration_seconds",
			Help:      "Time spent syncing storage to stable storage.",
		},
		[]string{"node_id"},
	)
}

func (m *Metrics) IncOperation(operation string) {
	m.StorageOperationsTotal.
		WithLabelValues(operation).
		Inc()
}

func (m *Metrics) IncOperationError(operation string) {
	m.StorageOperationErrors.
		WithLabelValues(operation).
		Inc()
}

func (m *Metrics) ObserveOperationDuration(
	operation string,
	duration time.Duration,
) {
	m.StorageOperationDuration.
		WithLabelValues(operation).
		Observe(duration.Seconds())
}

func (m *Metrics) IncSync() {
	m.StorageSyncTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *Metrics) IncSyncError() {
	m.StorageSyncErrors.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *Metrics) ObserveSyncDuration(duration time.Duration) {
	m.StorageSyncDuration.
		WithLabelValues(m.nodeID).
		Observe(duration.Seconds())
}
