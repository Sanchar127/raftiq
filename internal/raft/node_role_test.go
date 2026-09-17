package raft

import (
	"errors"
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
)

func TestBecomeLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected leader role, got %v", state.Role)
	}

	if state.LeaderID != "node-1" {
		t.Fatalf(
			"expected leader ID node-1, got %q",
			state.LeaderID,
		)
	}
}

func TestBecomeCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected candidate role, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-1" {
		t.Fatalf(
			"expected vote for node-1, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestBecomeFollower(t *testing.T) {
	node := NewRaftNode("node-1")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	if err := node.becomeFollower(2); err != nil {
		t.Fatalf("become follower: %v", err)
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term 2, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected leader ID to be cleared, got %q",
			state.LeaderID,
		)
	}
}

func TestHigherTermVoteReplyMakesCandidateFollower(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B"},
		},
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	currentTerm := node.State().Persistent.CurrentTerm

	node.recordVote("B", currentTerm, true)

	reply := RequestVoteReply{
		Term:        currentTerm + 1,
		VoterID:     "C",
		VoteGranted: false,
	}

	node.handleVoteReply(currentTerm, reply)
}

func TestLeaderDoesNotStartElectionOnTimeout(t *testing.T) {
	node := NewRaftNode("A")
	node.SetPeers([]Peer{})

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.tryBecomeLeader()

	if node.State().Role != Leader {
		t.Fatal("expected node to become Leader")
	}

	node.onElectionTimeout()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader to remain Leader, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term to remain 1, got %d", state.Persistent.CurrentTerm)
	}
}

func TestBecomeLeaderInitializesReplicationState(t *testing.T) {
	node := NewRaftNode("A")

	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")

	node.SetPeers([]Peer{peerB, peerC})

	if err := node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := node.Log().Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("two"),
	}); err != nil {
		t.Fatal(err)
	}

	node.becomeLeader()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected leader, got %v", state.Role)
	}

	if state.Leader.NextIndex["B"] != 3 {
		t.Fatalf("expected B NextIndex=3, got %d", state.Leader.NextIndex["B"])
	}

	if state.Leader.NextIndex["C"] != 3 {
		t.Fatalf("expected C NextIndex=3, got %d", state.Leader.NextIndex["C"])
	}

	if state.Leader.MatchIndex["B"] != 0 {
		t.Fatalf("expected B MatchIndex=0, got %d", state.Leader.MatchIndex["B"])
	}

	if state.Leader.MatchIndex["C"] != 0 {
		t.Fatalf("expected C MatchIndex=0, got %d", state.Leader.MatchIndex["C"])
	}
}

func TestInitializeReplicationStateLocked(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := node.Log().Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("two"),
	}); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.initializeReplicationStateLocked("D")
	node.mu.Unlock()

	state := node.State()

	if state.Leader.NextIndex["D"] != 3 {
		t.Fatalf(
			"expected D NextIndex=3, got %d",
			state.Leader.NextIndex["D"],
		)
	}

	if state.Leader.MatchIndex["D"] != 0 {
		t.Fatalf(
			"expected D MatchIndex=0, got %d",
			state.Leader.MatchIndex["D"],
		)
	}
}

func TestInitializeNewPeerReplicationStateLocked(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.log.Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("entry-1"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := node.log.Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("entry-2"),
	}); err != nil {
		t.Fatal(err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.initializeNewPeerReplicationStateLocked("D")
	node.mu.Unlock()

	node.mu.RLock()
	nextIndex := node.state.Leader.NextIndex["D"]
	matchIndex := node.state.Leader.MatchIndex["D"]
	node.mu.RUnlock()

	if nextIndex != 1 {
		t.Fatalf(
			"expected new peer D NextIndex=1, got %d",
			nextIndex,
		)
	}

	if matchIndex != 0 {
		t.Fatalf(
			"expected new peer D MatchIndex=0, got %d",
			matchIndex,
		)
	}
}

func TestBecomeFollowerSaveStateFailureDoesNotChangeMemory(t *testing.T) {
	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "node-1",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	memoryStorage := storage.NewMemoryStorage()

	if err := memoryStorage.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	stateStorage := &failingStateStorage{
		MemoryStorage: memoryStorage,
		saveStateErr:  errors.New("save state failed"),
	}

	node, err := NewRaftNodeWithStorage("node-1", stateStorage)
	if err != nil {
		t.Fatalf("create raft node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.LeaderID = "node-1"
	node.mu.Unlock()

	err = node.becomeFollower(3)
	if err == nil {
		t.Fatal("expected becomeFollower to fail")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term to remain 2 after SaveState failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-1" {
		t.Fatalf(
			"expected vote to remain node-1 after SaveState failure, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader after SaveState failure, got %v",
			state.Role,
		)
	}

	if state.LeaderID != "node-1" {
		t.Fatalf(
			"expected leader ID to remain node-1 after SaveState failure, got %q",
			state.LeaderID,
		)
	}
}

func TestBecomeFollowerSyncFailureDoesNotChangeMemory(t *testing.T) {
	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "node-1",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	memoryStorage := storage.NewMemoryStorage()

	if err := memoryStorage.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	stateStorage := &failingStateStorage{
		MemoryStorage: memoryStorage,
		syncErr:       errors.New("sync failed"),
	}

	node, err := NewRaftNodeWithStorage("node-1", stateStorage)
	if err != nil {
		t.Fatalf("create raft node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.LeaderID = "node-1"
	node.mu.Unlock()

	err = node.becomeFollower(3)
	if err == nil {
		t.Fatal("expected becomeFollower to fail")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term to remain 2 after Sync failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-1" {
		t.Fatalf(
			"expected vote to remain node-1 after Sync failure, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader after Sync failure, got %v",
			state.Role,
		)
	}

	if state.LeaderID != "node-1" {
		t.Fatalf(
			"expected leader ID to remain node-1 after Sync failure, got %q",
			state.LeaderID,
		)
	}
}
