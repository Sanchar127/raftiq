package raft

import "testing"

func TestNewRaftNode(t *testing.T) {
	node := NewRaftNode("node-1")

	if node == nil {
		t.Fatal("expected node, got nil")
	}

	if node.ID() != "node-1" {
		t.Fatalf("expected node ID node-1, got %q", node.ID())
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 0 {
		t.Fatalf(
			"expected current term 0, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf("expected no vote, got %q", state.Persistent.VotedFor)
	}

	if state.Volatile.CommitIndex != 0 {
		t.Fatalf(
			"expected commit index 0, got %d",
			state.Volatile.CommitIndex,
		)
	}

	if state.Volatile.LastApplied != 0 {
		t.Fatalf(
			"expected last applied 0, got %d",
			state.Volatile.LastApplied,
		)
	}

	if node.Log() == nil {
		t.Fatal("expected log, got nil")
	}

	if node.Log().LastIndex() != 0 {
		t.Fatalf(
			"expected empty log with last index 0, got %d",
			node.Log().LastIndex(),
		)
	}
}

func TestBecomeLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	node.becomeCandidate()
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

	node.becomeCandidate()

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

	node.becomeCandidate()
	node.becomeLeader()

	node.becomeFollower(2)

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

func TestRequestVoteGrantsVote(t *testing.T) {
	node := NewRaftNode("node-1")

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if reply.Term != 1 {
		t.Fatalf("expected term 1, got %d", reply.Term)
	}
}

func TestRequestVoteRejectsSecondCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	first := node.RequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "node-2",
	})

	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := node.RequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "node-3",
	})

	if second.VoteGranted {
		t.Fatal("expected second vote to be rejected")
	}
}

func TestRequestVoteRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("node-1")

	node.becomeCandidate()

	reply := node.RequestVote(RequestVoteArgs{
		Term:        0,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected older-term vote to be rejected")
	}

	if reply.Term != 1 {
		t.Fatalf("expected current term 1, got %d", reply.Term)
	}
}

func TestRequestVoteUpdatesHigherTerm(t *testing.T) {
	node := NewRaftNode("node-1")

	node.becomeCandidate()

	reply := node.RequestVote(RequestVoteArgs{
		Term:        2,
		CandidateID: "node-2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term 2, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}
}
