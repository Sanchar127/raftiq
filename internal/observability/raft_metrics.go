package observability

import (
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

// RaftMetrics adapts the Raft metrics interface to Prometheus metrics.
type RaftMetrics struct {
	nodeID  string
	metrics *Metrics
}

// NewRaftMetrics creates a Prometheus-backed Raft metrics adapter.
func NewRaftMetrics(nodeID string, metrics *Metrics) *RaftMetrics {
	if nodeID == "" {
		panic("nodeID must not be empty")
	}

	if metrics == nil {
		panic("metrics must not be nil")
	}

	return &RaftMetrics{
		nodeID:  nodeID,
		metrics: metrics,
	}
}

func (m *RaftMetrics) SetCurrentTerm(term raft.Term) {
	m.metrics.CurrentTerm.
		WithLabelValues(m.nodeID).
		Set(float64(term))
}

func (m *RaftMetrics) SetRole(role raft.Role) {
	for _, currentRole := range []raft.Role{
		raft.Follower,
		raft.Candidate,
		raft.Leader,
	} {
		value := float64(0)

		if currentRole == role {
			value = 1
		}

		m.metrics.Role.
			WithLabelValues(m.nodeID, roleLabel(currentRole)).
			Set(value)
	}
}

func (m *RaftMetrics) SetCommitIndex(index raft.LogIndex) {
	m.metrics.CommitIndex.
		WithLabelValues(m.nodeID).
		Set(float64(index))
}

func (m *RaftMetrics) SetLastApplied(index raft.LogIndex) {
	m.metrics.LastApplied.
		WithLabelValues(m.nodeID).
		Set(float64(index))
}

func (m *RaftMetrics) SetLastLogIndex(index raft.LogIndex) {
	m.metrics.LastLogIndex.
		WithLabelValues(m.nodeID).
		Set(float64(index))
}

func (m *RaftMetrics) SetLogSize(size int) {
	m.metrics.LogSize.
		WithLabelValues(m.nodeID).
		Set(float64(size))
}

func (m *RaftMetrics) IncElections() {
	m.metrics.ElectionsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *RaftMetrics) ObserveElectionDuration(
	duration time.Duration,
	result string,
) {
	m.metrics.ElectionDuration.
		WithLabelValues(m.nodeID, result).
		Observe(duration.Seconds())
}

func (m *RaftMetrics) IncLeaderChanges() {
	m.metrics.LeaderChangesTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *RaftMetrics) IncVoteRequests() {
	m.metrics.VoteRequestsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *RaftMetrics) IncVotesGranted() {
	m.metrics.VotesGrantedTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *RaftMetrics) IncAppendEntries(
	peerID raft.NodeID,
	result string,
) {
	m.metrics.AppendEntriesTotal.
		WithLabelValues(m.nodeID, string(peerID), result).
		Inc()
}

func (m *RaftMetrics) IncAppendEntriesFailures(peerID raft.NodeID) {
	m.metrics.AppendEntriesFailures.
		WithLabelValues(m.nodeID, string(peerID)).
		Inc()
}

func (m *RaftMetrics) ObserveAppendEntriesDuration(
	peerID raft.NodeID,
	duration time.Duration,
) {
	m.metrics.AppendEntriesDuration.
		WithLabelValues(m.nodeID, string(peerID)).
		Observe(duration.Seconds())
}

func (m *RaftMetrics) IncSnapshotsCreated() {
	m.metrics.SnapshotsCreatedTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *RaftMetrics) IncSnapshotsInstalled() {
	m.metrics.SnapshotsInstalledTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func roleLabel(role raft.Role) string {
	switch role {
	case raft.Follower:
		return "follower"
	case raft.Candidate:
		return "candidate"
	case raft.Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Compile-time interface check.
var _ raft.Metrics = (*RaftMetrics)(nil)
