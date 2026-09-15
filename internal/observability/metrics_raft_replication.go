package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/sanchar127/raftiq/internal/raft"
)

func (m *Metrics) initRaftReplication() {
	m.AppendEntriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "append_entries_total",
			Help:      "Total number of AppendEntries RPCs processed.",
		},
		[]string{"node_id", "peer_id", "result"},
	)

	m.AppendEntriesFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "append_entries_failures_total",
			Help:      "Total number of failed AppendEntries operations.",
		},
		[]string{"node_id", "peer_id"},
	)

	m.AppendEntriesDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "append_entries_duration_seconds",
			Help:      "Duration of AppendEntries operations.",
		},
		[]string{"node_id", "peer_id"},
	)
}

func (m *Metrics) IncAppendEntries(
	peerID raft.NodeID,
	result string,
) {
	m.AppendEntriesTotal.
		WithLabelValues(
			m.nodeID,
			string(peerID),
			result,
		).
		Inc()
}

func (m *Metrics) IncAppendEntriesFailures(peerID raft.NodeID) {
	m.AppendEntriesFailures.
		WithLabelValues(
			m.nodeID,
			string(peerID),
		).
		Inc()
}

func (m *Metrics) ObserveAppendEntriesDuration(
	peerID raft.NodeID,
	duration time.Duration,
) {
	m.AppendEntriesDuration.
		WithLabelValues(
			m.nodeID,
			string(peerID),
		).
		Observe(duration.Seconds())
}
