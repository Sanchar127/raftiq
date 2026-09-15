package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initRaftElection() {
	m.ElectionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "elections_total",
			Help:      "Total number of Raft elections started.",
		},
		[]string{"node_id"},
	)

	m.ElectionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "election_duration_seconds",
			Help:      "Time taken for Raft elections to complete.",
		},
		[]string{"node_id", "result"},
	)

	m.LeaderChangesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "leader_changes_total",
			Help:      "Total number of Raft leadership changes.",
		},
		[]string{"node_id"},
	)

	m.VoteRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "vote_requests_total",
			Help:      "Total number of RequestVote RPCs processed.",
		},
		[]string{"node_id"},
	)

	m.VotesGrantedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "raft",
			Name:      "votes_granted_total",
			Help:      "Total number of votes granted.",
		},
		[]string{"node_id"},
	)
}

func (m *Metrics) IncElections() {
	m.ElectionsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *Metrics) ObserveElectionDuration(
	duration time.Duration,
	result string,
) {
	m.ElectionDuration.
		WithLabelValues(m.nodeID, result).
		Observe(duration.Seconds())
}

func (m *Metrics) IncLeaderChanges() {
	m.LeaderChangesTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *Metrics) IncVoteRequests() {
	m.VoteRequestsTotal.
		WithLabelValues(m.nodeID).
		Inc()
}

func (m *Metrics) IncVotesGranted() {
	m.VotesGrantedTotal.
		WithLabelValues(m.nodeID).
		Inc()
}
