package raft

import "github.com/sanchar127/raftiq/internal/model"

type PersistentState = model.PersistentState

type VolatileState struct {
	CommitIndex LogIndex
	LastApplied LogIndex
}

type LeaderState struct {
	NextIndex  map[NodeID]LogIndex
	MatchIndex map[NodeID]LogIndex
}

type ElectionState struct {
	VotesReceived map[NodeID]struct{}
}

type State struct {
	Persistent PersistentState
	Volatile   VolatileState
	Leader     LeaderState
	Election   ElectionState
	Role       Role
	LeaderID   NodeID
}
