package raft

import "sync"

type RaftNode struct {
	mu sync.RWMutex

	id    NodeID
	state State
	log   *Log
}

func NewRaftNode(id NodeID) *RaftNode {
	return &RaftNode{
		id: id,
		state: State{
			Persistent: PersistentState{
				CurrentTerm: 0,
				VotedFor:    "",
			},
			Volatile: VolatileState{
				CommitIndex: 0,
				LastApplied: 0,
			},
			Leader: LeaderState{
				NextIndex:  make(map[NodeID]LogIndex),
				MatchIndex: make(map[NodeID]LogIndex),
			},
			Role:     Follower,
			LeaderID: "",
		},
		log: NewLog(),
	}
}

func (n *RaftNode) ID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id
}

func (n *RaftNode) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.state
}

func (n *RaftNode) Log() *Log {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.log
}

func (n *RaftNode) becomeFollower(term Term) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Follower
	n.state.Persistent.CurrentTerm = term
	n.state.Persistent.VotedFor = ""
	n.state.LeaderID = ""
}

func (n *RaftNode) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Candidate
	n.state.Persistent.CurrentTerm++
	n.state.Persistent.VotedFor = n.id
	n.state.LeaderID = ""
}

func (n *RaftNode) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Leader
	n.state.LeaderID = n.id
}
