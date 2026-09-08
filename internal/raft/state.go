package raft

type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
}

type VolatileState struct {
	CommitIndex LogIndex
	LastApplied LogIndex
}

type LeaderState struct {
	NextIndex  map[NodeID]LogIndex
	MatchIndex map[NodeID]LogIndex
}

type State struct {
	Persistent PersistentState
	Volatile   VolatileState
	Leader     LeaderState
	Role       Role
	LeaderID   NodeID
}
