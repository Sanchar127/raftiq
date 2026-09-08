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
