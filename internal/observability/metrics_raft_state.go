package observability

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sanchar127/raftiq/internal/raft"
)

func (m *Metrics) initRaftState() {
	m.CurrentTerm = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "current_term",
			Help:      "Current Raft term.",
		},
		[]string{"node_id"},
	)

	m.Role = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "role",
			Help:      "Current Raft role. Exactly one role is set to 1.",
		},
		[]string{"node_id", "role"},
	)

	m.CommitIndex = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "commit_index",
			Help:      "Highest Raft log index known to be committed.",
		},
		[]string{"node_id"},
	)

	m.LastApplied = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "last_applied",
			Help:      "Highest Raft log index applied to the state machine.",
		},
		[]string{"node_id"},
	)

	m.LastLogIndex = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "last_log_index",
			Help:      "Highest Raft log index currently stored.",
		},
		[]string{"node_id"},
	)

	m.LogSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "log_size",
			Help:      "Number of entries currently retained in the Raft log.",
		},
		[]string{"node_id"},
	)
}

func (m *Metrics) SetCurrentTerm(term raft.Term) {
	m.CurrentTerm.
		WithLabelValues(m.nodeID).
		Set(float64(term))
}

func (m *Metrics) SetRole(role raft.Role) {
	roles := []raft.Role{
		raft.Follower,
		raft.Candidate,
		raft.Leader,
	}

	for _, candidate := range roles {
		value := float64(0)

		if candidate == role {
			value = 1
		}

		m.Role.
			WithLabelValues(m.nodeID, roleLabel(candidate)).
			Set(value)
	}
}

func (m *Metrics) SetCommitIndex(index raft.LogIndex) {
	m.CommitIndex.
		WithLabelValues(m.nodeID).
		Set(float64(index))
}

func (m *Metrics) SetLastApplied(index raft.LogIndex) {
	m.LastApplied.
		WithLabelValues(m.nodeID).
		Set(float64(index))
}

func (m *Metrics) SetLastLogIndex(index raft.LogIndex) {
	m.LastLogIndex.
		WithLabelValues(m.nodeID).
		Set(float64(index))
}

func (m *Metrics) SetLogSize(size int) {
	m.LogSize.
		WithLabelValues(m.nodeID).
		Set(float64(size))
}
