package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initRaftSnapshot() {
	m.SnapshotsCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "snapshots_created_total",
			Help:      "Total number of snapshots created.",
		},
		[]string{"node_id"},
	)

	m.SnapshotsInstalledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "snapshots_installed_total",
			Help:      "Total number of snapshots installed.",
		},
		[]string{"node_id"},
	)

	// Initialize the node's time series without incrementing the counters.
	// This ensures Prometheus exposes zero-valued metrics before the first
	// snapshot event occurs.
	m.SnapshotsCreatedTotal.WithLabelValues(m.nodeID)
	m.SnapshotsInstalledTotal.WithLabelValues(m.nodeID)
}

func (m *Metrics) IncSnapshotsCreated() {
	m.SnapshotsCreatedTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *Metrics) IncSnapshotsInstalled() {
	m.SnapshotsInstalledTotal.
		WithLabelValues(m.nodeID).
		Inc()
}
