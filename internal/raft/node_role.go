package raft

import "fmt"

func (n *RaftNode) becomeFollower(term Term) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.becomeFollowerLocked(term)
}

func (n *RaftNode) becomeFollowerLocked(term Term) error {
	n.state.Role = Follower
	n.state.Persistent.CurrentTerm = term
	n.state.Persistent.VotedFor = ""
	n.state.LeaderID = ""
	n.electionElapsed = 0

	if err := n.persistStateLocked(); err != nil {
		return fmt.Errorf(
			"persist follower transition: %w",
			err,
		)
	}

	n.updateStateMetricsLocked()

	return nil
}

func (n *RaftNode) stepDownForStorageFailureLocked() {
	n.state.Role = Follower
	n.state.LeaderID = ""
	n.storageWriteBlocked = true
	n.electionElapsed = 0

	n.metrics.SetRole(Follower)
}

func (n *RaftNode) initializeReplicationStateLocked(
	peerID NodeID,
) {
	nextIndex := n.log.LastIndex() + 1

	n.state.Leader.NextIndex[peerID] = nextIndex
	n.state.Leader.MatchIndex[peerID] = 0
}

func (n *RaftNode) initializeNewPeerReplicationStateLocked(
	peerID NodeID,
) {
	n.state.Leader.NextIndex[peerID] = 1
	n.state.Leader.MatchIndex[peerID] = 0
}

func (n *RaftNode) becomeLeaderLocked() {
	n.state.Role = Leader
	n.state.LeaderID = n.id
	n.electionElapsed = 0
	n.heartbeatElapsed = 0

	n.finishElectionLocked("won")
	n.metrics.IncLeaderChanges()
	n.updateStateMetricsLocked()

	for _, peerID := range n.peerIDs {
		n.initializeReplicationStateLocked(peerID)
	}
}

func (n *RaftNode) becomeLeader() {
	logger := n.getLogger()

	n.mu.Lock()

	n.becomeLeaderLocked()

	term := n.state.Persistent.CurrentTerm
	peerCount := len(n.peerIDs)

	n.mu.Unlock()

	logger.Info(
		"raft node became leader",
		"term", term,
		"peer_count", peerCount,
	)
}

func majority(clusterSize int) int {
	return clusterSize/2 + 1
}
