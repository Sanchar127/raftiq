package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"

	"github.com/stretchr/testify/require"
)

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
func TestRequestVoteGrantsVote(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"node-1",
		"node-2",
	}
	node.mu.Unlock()

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

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"node-1",
		"node-2",
		"node-3",
	}
	node.mu.Unlock()

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

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

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

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"node-1",
		"node-2",
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

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
		t.Fatalf(
			"expected follower role, got %v",
			state.Role,
		)
	}
}

func TestCandidateVotesForItself(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if state.Persistent.VotedFor != "A" {
		t.Fatalf("expected A to vote for itself, got %q", state.Persistent.VotedFor)
	}

	if _, ok := state.Election.VotesReceived["A"]; !ok {
		t.Fatal("expected candidate's own vote to be recorded")
	}
}

func TestRecordVote(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"A",
				"B",
			},
		},
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	recorded := node.recordVote("B", term, true)

	if !recorded {
		t.Fatal("expected vote to be recorded")
	}

	state := node.State()

	if _, ok := state.Election.VotesReceived["B"]; !ok {
		t.Fatal("expected B's vote to be recorded")
	}
}

func TestDuplicateVoteIsIgnored(t *testing.T) {
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

	term := node.State().Persistent.CurrentTerm

	if !node.recordVote("B", term, true) {
		t.Fatal("expected first vote to be recorded")
	}

	if node.recordVote("B", term, true) {
		t.Fatal("expected duplicate vote to be ignored")
	}
}

func TestVoteFromOldElectionIsIgnored(t *testing.T) {
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

	if node.recordVote("B", currentTerm-1, true) {
		t.Fatal("expected vote from old term to be ignored")
	}
}

func TestCandidateBecomesLeaderAfterMajority(t *testing.T) {
	node := NewRaftNode("A")

	node.SetPeers([]Peer{
		NewRaftNode("B"),
		NewRaftNode("C"),
	})

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	if node.tryBecomeLeader() {
		t.Fatal("candidate should not become leader with only one vote")
	}

	node.recordVote("B", term, true)

	if !node.tryBecomeLeader() {
		t.Fatal("candidate should become leader after majority")
	}

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}
}

func TestThreeNodeElection(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership A: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership B: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership C: %v", err)
	}

	nodeA.runElection()

	stateA := nodeA.State()

	if stateA.Role != Leader {
		t.Fatalf("expected A to become Leader, got %v", stateA.Role)
	}

	if stateA.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", stateA.LeaderID)
	}

	if stateA.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term 1, got %d",
			stateA.Persistent.CurrentTerm,
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

func TestStaleElectionVoteReplyIsIgnored(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"A",
				"B",
			},
		},
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	oldTerm := node.State().Persistent.CurrentTerm

	_, err = node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	currentTerm := node.State().Persistent.CurrentTerm

	if currentTerm != oldTerm+1 {
		t.Fatalf(
			"expected current term %d, got %d",
			oldTerm+1,
			currentTerm,
		)
	}

	reply := RequestVoteReply{
		Term:        oldTerm,
		VoterID:     "B",
		VoteGranted: true,
	}

	node.handleVoteReply(oldTerm, reply)

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if len(state.Election.VotesReceived) != 1 {
		t.Fatalf(
			"expected only self vote, got %d votes",
			len(state.Election.VotesReceived),
		)
	}

	if _, ok := state.Election.VotesReceived["B"]; ok {
		t.Fatal("stale vote from B should not have been counted")
	}
}

func TestSplitVoteProducesNoLeader(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node A membership: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node B membership: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node C membership: %v", err)
	}

	if _, err := nodeA.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if _, err := nodeB.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if _, err := nodeC.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if nodeA.tryBecomeLeader() {
		t.Fatal("A should not become leader with only its own vote")
	}

	if nodeB.tryBecomeLeader() {
		t.Fatal("B should not become leader with only its own vote")
	}

	if nodeC.tryBecomeLeader() {
		t.Fatal("C should not become leader with only its own vote")
	}

	if nodeA.State().Role != Candidate {
		t.Fatal("A should remain Candidate")
	}

	if nodeB.State().Role != Candidate {
		t.Fatal("B should remain Candidate")
	}

	if nodeC.State().Role != Candidate {
		t.Fatal("C should remain Candidate")
	}
}

func TestTickStartsElection(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(3)

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if node.Tick() {
		t.Fatal("expected no election after first tick")
	}

	if node.Tick() {
		t.Fatal("expected no election after second tick")
	}

	if !node.Tick() {
		t.Fatal("expected election timeout after third tick")
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term 1, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "A" {
		t.Fatalf(
			"expected A to vote for itself, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestSetElectionTimeout(t *testing.T) {
	node := NewRaftNode("A")

	node.SetElectionTimeout(10)

	node.mu.RLock()
	timeout := node.electionTimeout
	node.mu.RUnlock()

	if timeout != 10 {
		t.Fatalf("expected election timeout 10, got %d", timeout)
	}
}

func TestTickTriggersElectionTimeout(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(3)

	if node.Tick() {
		t.Fatal("expected no election after first tick")
	}

	if node.Tick() {
		t.Fatal("expected no election after second tick")
	}

	if !node.Tick() {
		t.Fatal("expected election after third tick")
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed < node.electionTimeout {
		t.Fatalf(
			"expected election elapsed to reach timeout %d, got %d",
			node.electionTimeout,
			elapsed,
		)
	}
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

func TestElectionTimeoutStartsElection(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(3)

	if err := node.SetTransport(
		&grantingTransport{},
		[]NodeID{"B", "C"},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if node.Tick() {
		t.Fatal("expected no timeout on first tick")
	}

	if node.Tick() {
		t.Fatal("expected no timeout on second tick")
	}

	if !node.Tick() {
		t.Fatal("expected timeout on third tick")
	}

	node.onElectionTimeout()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term 1, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "A" {
		t.Fatalf("expected self-vote for A, got %q", state.Persistent.VotedFor)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected LeaderID A, got %q", state.LeaderID)
	}
}

func TestRequestVoteResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(10)

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"A",
		"B",
	}
	node.mu.Unlock()

	node.Tick()
	node.Tick()

	node.mu.RLock()
	elapsedBefore := node.electionElapsed
	node.mu.RUnlock()

	if elapsedBefore != 2 {
		t.Fatalf("expected elapsed time 2, got %d", elapsedBefore)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	node.mu.RLock()
	elapsedAfter := node.electionElapsed
	node.mu.RUnlock()

	if elapsedAfter != 0 {
		t.Fatalf(
			"expected election timer to reset to 0, got %d",
			elapsedAfter,
		)
	}
}

func TestAppendEntriesResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("B")
	node.SetElectionTimeout(10)

	node.Tick()
	node.Tick()

	node.mu.RLock()
	elapsedBefore := node.electionElapsed
	node.mu.RUnlock()

	if elapsedBefore != 2 {
		t.Fatalf("expected elapsed time 2, got %d", elapsedBefore)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "A",
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected Follower, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}

	node.mu.RLock()
	elapsedAfter := node.electionElapsed
	node.mu.RUnlock()

	if elapsedAfter != 0 {
		t.Fatalf("expected election timer to reset to 0, got %d", elapsedAfter)
	}
}

func TestAppendEntriesRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("B")
	node.SetElectionTimeout(10)

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	node.Tick()
	node.Tick()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 5
	node.state.Role = Follower
	node.state.LeaderID = "A"
	node.electionElapsed = 2
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     4,
		LeaderID: "C",
	})

	if reply.Success {
		t.Fatal("expected stale AppendEntries to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.LeaderID != "A" {
		t.Fatalf(
			"expected leader A to remain unchanged, got %q",
			state.LeaderID,
		)
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 2 {
		t.Fatalf(
			"expected election timer to remain 2, got %d",
			elapsed,
		)
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

func TestAdvanceCommitIndex(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")

	leader.SetPeers([]Peer{peerB, peerC})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 2

	if err := leader.log.Append(LogEntry{
		Index: 1,
		Term:  2,
		Data:  []byte("one"),
	}); err != nil {
		leader.mu.Unlock()
		t.Fatal(err)
	}

	leader.state.Leader.MatchIndex["B"] = 1
	leader.state.Leader.MatchIndex["C"] = 0

	leader.mu.Unlock()

	leader.advanceCommitIndex()

	state := leader.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected commit index 1, got %d",
			state.Volatile.CommitIndex,
		)
	}
}

func TestAppendEntriesAppliesCommittedEntry(t *testing.T) {
	node := NewRaftNode("node-1")

	entry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("command"),
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		Entries:      []LogEntry{entry},
		LeaderCommit: 1,
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	select {
	case applied := <-node.ApplyCh():
		if applied.Index != 1 {
			t.Fatalf(
				"expected applied index 1, got %d",
				applied.Index,
			)
		}

		if string(applied.Data) != "command" {
			t.Fatalf(
				"expected applied data command, got %q",
				applied.Data,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("expected committed entry to be applied")
	}

	state := node.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected commit index 1, got %d",
			state.Volatile.CommitIndex,
		)
	}

	if state.Volatile.LastApplied != 1 {
		t.Fatalf(
			"expected last applied 1, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestProposeAsLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	index, err := node.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose() returned error: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	entry, ok := node.Log().Get(index)
	if !ok {
		t.Fatalf("expected proposed entry at index %d", index)
	}

	if string(entry.Data) != "hello" {
		t.Fatalf("expected data %q, got %q", "hello", string(entry.Data))
	}

	if entry.Term != 1 {
		t.Fatalf("expected term 1, got %d", entry.Term)
	}
}

func TestLeaderBacktracksNextIndexOnReplicationFailure(t *testing.T) {
	leader := NewRaftNode("leader")
	follower := NewRaftNode("follower")

	leader.SetPeers([]Peer{follower})
	follower.SetPeers([]Peer{leader})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := follower.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap follower membership: %v", err)
	}

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	if err := leader.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("two"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 3,
		Term:  2,
		Data:  []byte("three"),
	}); err != nil {
		t.Fatal(err)
	}

	leader.mu.Lock()
	leader.state.Leader.NextIndex[follower.ID()] = 4
	leader.mu.Unlock()

	args, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("expected AppendEntries arguments to be built")
	}

	reply := follower.AppendEntries(args)

	if reply.Success {
		t.Fatal("expected replication to fail")
	}

	leader.handleAppendEntriesReply(follower.ID(), args, reply)

	leader.mu.RLock()
	nextIndex := leader.state.Leader.NextIndex[follower.ID()]
	leader.mu.RUnlock()

	if nextIndex != 3 {
		t.Fatalf("expected NextIndex 3 after failure, got %d", nextIndex)
	}

	retryArgs, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("expected retry AppendEntries arguments to be built")
	}

	if retryArgs.PrevLogIndex != 2 {
		t.Fatalf(
			"expected retry PrevLogIndex 2, got %d",
			retryArgs.PrevLogIndex,
		)
	}

	if retryArgs.PrevLogTerm != 1 {
		t.Fatalf(
			"expected retry PrevLogTerm 1, got %d",
			retryArgs.PrevLogTerm,
		)
	}

	if len(retryArgs.Entries) != 1 {
		t.Fatalf(
			"expected retry to contain 1 entry, got %d",
			len(retryArgs.Entries),
		)
	}

	if retryArgs.Entries[0].Index != 3 {
		t.Fatalf(
			"expected retry entry index 3, got %d",
			retryArgs.Entries[0].Index,
		)
	}
}

func TestProposeReplicatesAndCommits(t *testing.T) {
	leader := NewRaftNode("leader")
	follower1 := NewRaftNode("follower-1")
	follower2 := NewRaftNode("follower-2")

	leader.SetPeers([]Peer{follower1, follower2})
	follower1.SetPeers([]Peer{leader, follower2})
	follower2.SetPeers([]Peer{leader, follower1})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := follower1.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap follower-1 membership: %v", err)
	}

	if err := follower2.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap follower-2 membership: %v", err)
	}

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	index, err := leader.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose() returned error: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected proposed index 1, got %d", index)
	}

	for _, follower := range []*RaftNode{follower1, follower2} {
		entry, ok := follower.Log().Get(1)
		if !ok {
			t.Fatalf("follower %s does not have entry 1", follower.ID())
		}

		if string(entry.Data) != "hello" {
			t.Fatalf(
				"follower %s has data %q, expected %q",
				follower.ID(),
				string(entry.Data),
				"hello",
			)
		}
	}

	state := leader.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected leader CommitIndex 1, got %d",
			state.Volatile.CommitIndex,
		)
	}

	select {
	case entry := <-leader.ApplyCh():
		if entry.Index != 1 {
			t.Fatalf(
				"expected applied index 1, got %d",
				entry.Index,
			)
		}

		if string(entry.Data) != "hello" {
			t.Fatalf(
				"expected applied data %q, got %q",
				"hello",
				string(entry.Data),
			)
		}
	}
}

func TestHeartbeatDueOnlyForLeader(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if node.heartbeatDue() {
		t.Fatal("follower should not send heartbeat")
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	if !node.heartbeatDue() {
		t.Fatal("leader should send heartbeat when heartbeat timer expires")
	}
}

func TestHeartbeatResetsFollowerElectionTimer(t *testing.T) {
	leader := NewRaftNode("leader")
	follower := NewRaftNode("follower")

	leader.SetPeers([]Peer{follower})
	follower.SetPeers([]Peer{leader})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := follower.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap follower membership: %v", err)
	}

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	follower.mu.Lock()
	follower.electionElapsed = 5
	follower.mu.Unlock()

	args, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("failed to build heartbeat")
	}

	if len(args.Entries) != 0 {
		t.Fatal("expected empty heartbeat")
	}

	reply := follower.AppendEntries(args)
	if !reply.Success {
		t.Fatal("heartbeat should succeed")
	}

	state := follower.State()

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}

	follower.mu.RLock()
	electionElapsed := follower.electionElapsed
	follower.mu.RUnlock()

	if electionElapsed != 0 {
		t.Fatalf("expected election timer reset, got %d", electionElapsed)
	}
}

func TestLeaderTickSendsHeartbeat(t *testing.T) {
	leader := NewRaftNode("leader")
	follower := NewRaftNode("follower")

	leader.SetPeers([]Peer{follower})
	follower.SetPeers([]Peer{leader})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := follower.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap follower membership: %v", err)
	}

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	follower.mu.Lock()
	follower.electionElapsed = 5
	follower.mu.Unlock()

	leader.mu.Lock()
	leader.heartbeatTimeout = 1
	leader.mu.Unlock()

	electionDue := leader.Tick()

	if electionDue {
		t.Fatal("leader should not start an election")
	}

	follower.mu.RLock()
	electionElapsed := follower.electionElapsed
	followerRole := follower.state.Role
	followerLeaderID := follower.state.LeaderID
	follower.mu.RUnlock()

	if electionElapsed != 0 {
		t.Fatalf(
			"expected follower election timer to reset, got %d",
			electionElapsed,
		)
	}

	if followerRole != Follower {
		t.Fatalf("expected follower role, got %v", followerRole)
	}

	if followerLeaderID != leader.ID() {
		t.Fatalf(
			"expected leader ID %q, got %q",
			leader.ID(),
			followerLeaderID,
		)
	}
}
func TestFollowerRejectsStaleHeartbeat(t *testing.T) {
	follower := NewRaftNode("follower")

	follower.mu.Lock()
	follower.state.Persistent.CurrentTerm = 5
	follower.electionElapsed = 4
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:     4,
		LeaderID: "old-leader",
	}

	reply := follower.AppendEntries(args)

	if reply.Success {
		t.Fatal("follower should reject stale heartbeat")
	}

	state := follower.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	follower.mu.RLock()
	electionElapsed := follower.electionElapsed
	follower.mu.RUnlock()

	if electionElapsed != 4 {
		t.Fatalf(
			"expected election timer to remain 4, got %d",
			electionElapsed,
		)
	}
}

func TestRaftNodeRestoresPersistentState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()

	node.state.Persistent.CurrentTerm = 7
	node.state.Persistent.VotedFor = "B"

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist state: %v", err)
	}

	node.mu.Unlock()

	restored, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 7 {
		t.Fatalf(
			"expected term 7, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "B" {
		t.Fatalf(
			"expected votedFor B, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestRequestVoteHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = "old-candidate"
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.RequestVote(RequestVoteArgs{
		Term:        5,
		CandidateID: "node-2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	restored, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected restored term 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-2" {
		t.Fatalf(
			"expected restored vote for node-2, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}

	if !membershipIsVoter(
		state.Persistent.Membership,
		"node-2",
	) {
		t.Fatal("expected node-2 to remain a voter after restart")
	}
}
func TestAppendEntriesHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = "old-candidate"

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         5,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	restored, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected restored term 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected restored vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}
}

type failingStateStorage struct {
	*storage.MemoryStorage

	saveStateErr error
	syncErr      error
}

func (s *failingStateStorage) SaveState(
	state model.PersistentState,
) error {
	if s.saveStateErr != nil {
		return s.saveStateErr
	}

	return s.MemoryStorage.SaveState(state)
}

func (s *failingStateStorage) Sync() error {
	if s.syncErr != nil {
		return s.syncErr
	}

	return s.MemoryStorage.Sync()
}

func TestAppendEntriesHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail when higher-term persistence fails")
	}

	persisted, err := baseStore.LoadState()
	if err != nil {
		t.Fatalf("load persisted state: %v", err)
	}

	if persisted.CurrentTerm != 2 {
		t.Fatalf(
			"expected persisted term to remain 2, got %d",
			persisted.CurrentTerm,
		)
	}

	if persisted.VotedFor != "old-candidate" {
		t.Fatalf(
			"expected persisted vote to remain old-candidate, got %q",
			persisted.VotedFor,
		)
	}
}

func TestAppendEntriesHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail when higher-term sync fails")
	}
}

func TestHandleVoteReplyHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = node.id
	node.state.Election.VotesReceived = map[NodeID]struct{}{
		node.id: {},
	}

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	node.handleVoteReply(2, RequestVoteReply{
		Term:        5,
		VoterID:     "node-2",
		VoteGranted: false,
	})

	restored, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected restored term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected restored vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected restored leader ID to be empty, got %q",
			state.LeaderID,
		)
	}
}

func TestHandleAppendEntriesReplyHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("leader", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	node.handleAppendEntriesReply(
		"follower",
		AppendEntriesArgs{
			Term: 2,
		},
		AppendEntriesReply{
			Term:       5,
			FollowerID: "follower",
			Success:    false,
		},
	)

	restored, err := NewRaftNodeWithStorage("leader", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected restored term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected restored vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected restored leader ID to be empty, got %q",
			state.LeaderID,
		)
	}
}

func TestRequestVoteStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.RequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected stale-term vote to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term to remain 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf(
			"expected vote to remain for %q, got %q",
			node.id,
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != node.id {
		t.Fatalf(
			"expected leader ID to remain %q, got %q",
			node.id,
			state.LeaderID,
		)
	}
}

func TestAppendEntriesStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         3,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	})

	if reply.Success {
		t.Fatal("expected stale-term AppendEntries to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term to remain 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf(
			"expected vote to remain for %q, got %q",
			node.id,
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != node.id {
		t.Fatalf(
			"expected leader ID to remain %q, got %q",
			node.id,
			state.LeaderID,
		)
	}
}

func TestHandleVoteReplyStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = ""

	node.state.Election.VotesReceived = map[NodeID]struct{}{
		node.id: {},
	}
	node.mu.Unlock()

	node.handleVoteReply(5, RequestVoteReply{
		Term:        3,
		VoterID:     "node-2",
		VoteGranted: true,
	})

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected term to remain 5, got %d",
			state.Persistent.CurrentTerm)
	}

	if state.Role != Candidate {
		t.Fatalf("expected role to remain Candidate, got %v",
			state.Role)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf("expected vote to remain for %q, got %q",
			node.id, state.Persistent.VotedFor)
	}

	if _, ok := state.Election.VotesReceived["node-2"]; ok {
		t.Fatal("stale vote reply must not be counted")
	}
}

func TestHandleAppendEntriesReplyStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("leader", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 6
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	node.state.Leader.NextIndex["follower"] = 3
	node.state.Leader.MatchIndex["follower"] = 1
	node.mu.Unlock()

	node.handleAppendEntriesReply(
		"follower",
		AppendEntriesArgs{
			Term: 5,
			Entries: []LogEntry{
				{
					Index: 2,
					Term:  5,
					Data:  []byte("value"),
				},
			},
		},
		AppendEntriesReply{
			Term:       5,
			FollowerID: "follower",
			Success:    true,
		},
	)

	state := node.State()

	if state.Persistent.CurrentTerm != 6 {
		t.Fatalf(
			"expected term to remain 6, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.Leader.NextIndex["follower"] != 3 {
		t.Fatalf(
			"expected NextIndex to remain 3, got %d",
			state.Leader.NextIndex["follower"],
		)
	}

	if state.Leader.MatchIndex["follower"] != 1 {
		t.Fatalf(
			"expected MatchIndex to remain 1, got %d",
			state.Leader.MatchIndex["follower"],
		)
	}
}

func TestRaftNodeStartStop(t *testing.T) {
	node := NewRaftNode("node-1")

	err := node.Start()
	require.NoError(t, err)

	node.Stop()
}

func TestRaftNodeStartTwice(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NoError(t, node.Start())
	defer node.Stop()

	err := node.Start()
	require.Error(t, err)
}

func TestRaftNodeStopWithoutStart(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NotPanics(t, func() {
		node.Stop()
	})
}

func TestTickDoesNotTriggerElectionBeforeTimeout(t *testing.T) {
	node := NewRaftNode("node-1")

	for i := 0; i < node.electionTimeout-1; i++ {
		electionDue, heartbeatDue := node.tick()

		require.False(t, electionDue)
		require.False(t, heartbeatDue)
	}

	electionDue, heartbeatDue := node.tick()

	require.True(t, electionDue)
	require.False(t, heartbeatDue)
}

func TestTickTriggersHeartbeatForLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Role = Leader
	node.heartbeatElapsed = node.heartbeatTimeout - 1
	node.mu.Unlock()

	electionDue, heartbeatDue := node.tick()

	require.False(t, electionDue)
	require.True(t, heartbeatDue)
}
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

func TestRaftNodeStartDrivesElection(t *testing.T) {
	node := NewRaftNode("node-1")

	node.SetElectionTimeout(3)

	require.NoError(t, node.BootstrapMembership())

	require.NoError(t, node.Start())
	defer node.Stop()

	waitForCondition(t, time.Second, func() bool {
		state := node.State()
		return state.Role == Candidate || state.Role == Leader
	})
}

func TestRaftNodeStartAdvancesElectionTimer(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetElectionTimeout(100)

	require.NoError(t, node.Start())

	waitForCondition(t, time.Second, func() bool {
		node.mu.RLock()
		defer node.mu.RUnlock()

		return node.electionElapsed > 0
	})

	node.Stop()
}

func TestRaftNodeStopWaitsForRunLoop(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NoError(t, node.Start())

	node.Stop()

	node.runMu.Lock()
	running := node.running
	node.runMu.Unlock()

	require.False(t, running)
}

func TestRaftNodeCanRestart(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NoError(t, node.Start())
	node.Stop()

	require.NoError(t, node.Start())
	node.Stop()
}
func TestStartElectionIfNeededPreventsDuplicateElection(t *testing.T) {
	node := NewRaftNode("node-1")

	node.runMu.Lock()
	node.electionInFlight = true
	node.runMu.Unlock()

	node.startElectionIfNeeded()

	node.runMu.Lock()
	defer node.runMu.Unlock()

	require.True(t, node.electionInFlight)
}

func TestCreateSnapshot(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node failed: %v", err)
	}

	node.becomeLeader()

	node.mu.Lock()

	for i := LogIndex(1); i <= 5; i++ {
		if err := node.log.Append(LogEntry{
			Index: i,
			Term:  1,
			Data:  []byte(fmt.Sprintf("command-%d", i)),
		}); err != nil {
			node.mu.Unlock()
			t.Fatalf("append failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = 5
	node.state.Volatile.LastApplied = 5

	node.mu.Unlock()

	snapshotData := []byte(`{"key":"value"}`)

	if err := node.CreateSnapshot(5, snapshotData); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot failed: %v", err)
	}

	if snapshot.LastIncludedIndex != 5 {
		t.Fatalf(
			"expected snapshot index 5, got %d",
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedTerm != 1 {
		t.Fatalf(
			"expected snapshot term 1, got %d",
			snapshot.LastIncludedTerm,
		)
	}

	if string(snapshot.Data) != string(snapshotData) {
		t.Fatalf(
			"expected snapshot data %q, got %q",
			snapshotData,
			snapshot.Data,
		)
	}

	if node.Log().LastIncludedIndex() != 5 {
		t.Fatalf(
			"expected log snapshot boundary 5, got %d",
			node.Log().LastIncludedIndex(),
		)
	}

	if node.Log().LastIndex() != 5 {
		t.Fatalf(
			"expected last index 5, got %d",
			node.Log().LastIndex(),
		)
	}
}

func TestCreateSnapshotRejectsUnappliedIndex(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()

	for i := LogIndex(1); i <= 3; i++ {
		if err := node.log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			node.mu.Unlock()
			t.Fatalf("append failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = 3
	node.state.Volatile.LastApplied = 2

	node.mu.Unlock()

	err := node.CreateSnapshot(3, []byte(`snapshot`))
	if err == nil {
		t.Fatal("expected unapplied snapshot index to be rejected")
	}
}

func TestCreateSnapshotRejectsUncommittedIndex(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()

	for i := LogIndex(1); i <= 3; i++ {
		if err := node.log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			node.mu.Unlock()
			t.Fatalf("append failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = 2
	node.state.Volatile.LastApplied = 2

	node.mu.Unlock()

	err := node.CreateSnapshot(3, []byte(`snapshot`))
	if err == nil {
		t.Fatal("expected uncommitted snapshot index to be rejected")
	}
}

func TestCreateSnapshotRejectsIndexZero(t *testing.T) {
	node := NewRaftNode("node-1")

	err := node.CreateSnapshot(0, []byte(`snapshot`))
	if err == nil {
		t.Fatal("expected index zero snapshot to be rejected")
	}
}

func TestInstallSnapshotPersistsHigherTerm(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  2,
		Data:              []byte(`{"key":"value"}`),
	})

	require.True(t, reply.Success)
	require.Equal(t, Term(2), reply.Term)

	state := node.State()

	require.Equal(t, Term(2), state.Persistent.CurrentTerm)
	require.Equal(t, NodeID(""), state.Persistent.VotedFor)

	restarted, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	restartedState := restarted.State()

	require.Equal(
		t,
		Term(2),
		restartedState.Persistent.CurrentTerm,
	)

	require.Equal(
		t,
		NodeID(""),
		restartedState.Persistent.VotedFor,
	)
}

func TestInstallSnapshotRestoresStateMachine(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	var restored model.Snapshot

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		restored = snapshot
		return nil
	})

	require.NoError(t, node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("entry-1"),
	}))

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte(`{"name":"raftiq"}`),
	})

	require.True(t, reply.Success)

	require.Equal(t, LogIndex(1), restored.LastIncludedIndex)
	require.Equal(t, Term(1), restored.LastIncludedTerm)
	require.Equal(
		t,
		[]byte(`{"name":"raftiq"}`),
		restored.Data,
	)

	state := node.State()

	require.Equal(t, LogIndex(1), state.Volatile.CommitIndex)
	require.Equal(t, LogIndex(1), state.Volatile.LastApplied)
}

func TestRaftNodeRPCTimeout(t *testing.T) {
	transport := &blockingTransport{
		requestVoteStarted: make(chan struct{}),
	}

	node := NewRaftNode(NodeID("node-1"))

	if err := node.SetTransport(
		transport,
		[]NodeID{"node-2"},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if err := node.SetRPCTimeout(50 * time.Millisecond); err != nil {
		t.Fatalf("set RPC timeout: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Persistent.CurrentTerm = 1
	node.mu.Unlock()

	done := make(chan struct{})

	go func() {
		node.requestVotes()
		close(done)
	}()

	select {
	case <-transport.requestVoteStarted:
	case <-time.After(time.Second):
		t.Fatal("RequestVote RPC did not start")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RequestVote did not return after RPC timeout")
	}
}

func TestRaftNodeRPCTimeoutValidation(t *testing.T) {
	node := NewRaftNode(NodeID("node-1"))

	if err := node.SetRPCTimeout(0); err == nil {
		t.Fatal("expected error for zero RPC timeout")
	}

	if err := node.SetRPCTimeout(-time.Second); err == nil {
		t.Fatal("expected error for negative RPC timeout")
	}

	if err := node.SetRPCTimeout(100 * time.Millisecond); err != nil {
		t.Fatalf("valid RPC timeout rejected: %v", err)
	}
}

func TestRaftNodeStopCancelsRPC(t *testing.T) {
	transport := &blockingTransport{
		appendEntriesStarted: make(chan struct{}),
	}

	node := NewRaftNode(NodeID("node-1"))

	if err := node.SetTransport(
		transport,
		[]NodeID{"node-2"},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := node.SetRPCTimeout(10 * time.Second); err != nil {
		t.Fatalf("set RPC timeout: %v", err)
	}

	if err := node.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}

	// Initialize the node using the real Raft leader transition.
	node.becomeLeader()

	go node.sendHeartbeat("node-2")

	select {
	case <-transport.appendEntriesStarted:
	case <-time.After(2 * time.Second):
		node.Stop()
		t.Fatal("AppendEntries RPC did not start")
	}

	stopped := make(chan struct{})

	go func() {
		node.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("node Stop did not cancel the in-flight RPC")
	}
}

type blockingTransport struct {
	requestVoteStarted   chan struct{}
	appendEntriesStarted chan struct{}
}
type grantingTransport struct{}

func (t *grantingTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	return RequestVoteReply{
		Term:        args.Term,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *grantingTransport) PreVote(
	ctx context.Context,
	target NodeID,
	args PreVoteArgs,
) (PreVoteReply, error) {
	return PreVoteReply{
		Term:        args.Term,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *grantingTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}

func (t *grantingTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, nil
}

type electionTransport struct {
	peers []NodeID
}

func (t *electionTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	return RequestVoteReply{
		Term:        args.Term,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *electionTransport) PreVote(
	ctx context.Context,
	target NodeID,
	args PreVoteArgs,
) (PreVoteReply, error) {
	return PreVoteReply{
		Term:        args.Term - 1,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *electionTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}

func (t *electionTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, nil
}

func (t *blockingTransport) RequestVote(
	ctx context.Context,
	_ NodeID,
	_ RequestVoteArgs,
) (RequestVoteReply, error) {
	if t.requestVoteStarted != nil {
		select {
		case <-t.requestVoteStarted:
		default:
			close(t.requestVoteStarted)
		}
	}

	<-ctx.Done()

	return RequestVoteReply{}, ctx.Err()
}

func (t *blockingTransport) PreVote(
	ctx context.Context,
	_ NodeID,
	_ PreVoteArgs,
) (PreVoteReply, error) {
	<-ctx.Done()

	return PreVoteReply{}, ctx.Err()
}

func (t *blockingTransport) AppendEntries(
	ctx context.Context,
	_ NodeID,
	_ AppendEntriesArgs,
) (AppendEntriesReply, error) {
	if t.appendEntriesStarted != nil {
		select {
		case <-t.appendEntriesStarted:
		default:
			close(t.appendEntriesStarted)
		}
	}

	<-ctx.Done()

	return AppendEntriesReply{}, ctx.Err()
}

func (t *blockingTransport) InstallSnapshot(
	ctx context.Context,
	_ NodeID,
	_ InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	<-ctx.Done()

	return InstallSnapshotReply{}, ctx.Err()
}

func TestPreVoteGrantsVoteForUpToDateCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.mu.Unlock()

	reply := node.PreVote(PreVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected pre-vote to be granted")
	}

	if reply.Term != 0 {
		t.Fatalf("expected current term 0, got %d", reply.Term)
	}

	if reply.VoterID != "node-1" {
		t.Fatalf("expected voter ID node-1, got %q", reply.VoterID)
	}
}

func TestPreVoteRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	reply := node.PreVote(PreVoteArgs{
		Term:        0,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected older-term pre-vote to be rejected")
	}

	if reply.Term != 1 {
		t.Fatalf("expected current term 1, got %d", reply.Term)
	}
}

func TestPreVoteRejectsNonVoter(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.mu.Unlock()

	reply := node.PreVote(PreVoteArgs{
		Term:         1,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected PreVote from non-voter to be rejected")
	}

	if reply.VoterID != "node-1" {
		t.Fatalf(
			"expected voter ID node-1, got %q",
			reply.VoterID,
		)
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 0 {
		t.Fatalf(
			"expected term to remain 0, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected VotedFor to remain empty, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestPreVoteDoesNotChangeTermOrVote(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()

	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}

	node.state.Persistent.CurrentTerm = 0
	node.state.Persistent.VotedFor = ""
	node.state.Role = Follower
	node.state.LeaderID = ""
	node.electionElapsed = node.electionTimeout

	node.mu.Unlock()

	before := node.State()

	reply := node.PreVote(PreVoteArgs{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected pre-vote to be granted")
	}

	after := node.State()

	// PreVote must not change the persistent term.
	if after.Persistent.CurrentTerm != before.Persistent.CurrentTerm {
		t.Fatalf(
			"pre-vote changed term: before=%d after=%d",
			before.Persistent.CurrentTerm,
			after.Persistent.CurrentTerm,
		)
	}

	// PreVote must not change the persistent vote.
	if after.Persistent.VotedFor != before.Persistent.VotedFor {
		t.Fatalf(
			"pre-vote changed VotedFor: before=%q after=%q",
			before.Persistent.VotedFor,
			after.Persistent.VotedFor,
		)
	}

	// PreVote must not change the node's role.
	if after.Role != before.Role {
		t.Fatalf(
			"pre-vote changed role: before=%v after=%v",
			before.Role,
			after.Role,
		)
	}

	// PreVote must not establish or change a leader.
	if after.LeaderID != before.LeaderID {
		t.Fatalf(
			"pre-vote changed LeaderID: before=%q after=%q",
			before.LeaderID,
			after.LeaderID,
		)
	}
}

func TestPreVoteRejectsStaleCandidateLog(t *testing.T) {
	node := NewRaftNode("node-1")

	if err := node.log.Append(LogEntry{
		Index: 1,
		Term:  1,
	}); err != nil {
		t.Fatalf("append log entry: %v", err)
	}

	reply := node.PreVote(PreVoteArgs{
		Term:         2,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected stale candidate log to be rejected")
	}
}
func TestPreVoteRejectsWhenRecentLeaderExists(t *testing.T) {
	node := NewRaftNode("node-2")
	node.SetElectionTimeout(10)

	// Simulate a recent heartbeat from the current leader.
	appendReply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-1",
	})

	if !appendReply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected Follower, got %v", state.Role)
	}

	if state.LeaderID != "node-1" {
		t.Fatalf("expected leader node-1, got %q", state.LeaderID)
	}

	preVoteReply := node.PreVote(PreVoteArgs{
		Term:         2,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if preVoteReply.VoteGranted {
		t.Fatal("expected PreVote to be rejected while recent leader is known")
	}

	after := node.State()

	if after.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term to remain 1, got %d", after.Persistent.CurrentTerm)
	}

	if after.LeaderID != "node-1" {
		t.Fatalf("expected leader node-1 to remain, got %q", after.LeaderID)
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 0 {
		t.Fatalf("expected PreVote not to reset election timer, got %d", elapsed)
	}
}

func TestPreVoteGrantsAfterElectionTimeout(t *testing.T) {
	node := NewRaftNode("node-2")
	node.SetElectionTimeout(10)

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
				"node-3",
			},
		},
	}
	node.mu.Unlock()

	// Simulate a heartbeat from the current leader.
	appendReply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-1",
	})

	if !appendReply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	// Simulate the election timer expiring without another heartbeat.
	node.mu.Lock()
	node.electionElapsed = node.electionTimeout
	node.mu.Unlock()

	preVoteReply := node.PreVote(PreVoteArgs{
		Term:         2,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !preVoteReply.VoteGranted {
		t.Fatal("expected PreVote to be granted after election timeout")
	}

	// PreVote must never change the persistent term.
	state := node.State()

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term to remain 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	// PreVote must not change the known leader.
	if state.LeaderID != "node-1" {
		t.Fatalf(
			"expected leader ID to remain node-1, got %q",
			state.LeaderID,
		)
	}

	// PreVote must not change the node's role.
	if state.Role != Follower {
		t.Fatalf(
			"expected node to remain Follower, got %v",
			state.Role,
		)
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if err := node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("committed"),
	}); err != nil {
		t.Fatalf("append log entry: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 1
	node.mu.Unlock()

	node.becomeLeader()

	node.mu.Lock()
	node.state.Volatile.CommitIndex = 1
	node.mu.Unlock()

	state := node.State()

	entry, ok := node.Log().Get(1)
	if !ok {
		t.Fatal("expected log entry at index 1")
	}

	t.Logf(
		"role=%v term=%d commitIndex=%d logTerm=%d",
		state.Role,
		state.Persistent.CurrentTerm,
		state.Volatile.CommitIndex,
		entry.Term,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	index, err := node.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected ReadIndex=1, got %d", index)
	}
}

func TestReadIndexQuorum(t *testing.T) {
	leader := NewRaftNode("A")
	followerB := NewRaftNode("B")
	followerC := NewRaftNode("C")

	leader.SetPeers([]Peer{
		followerB,
		followerC,
	})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("committed"),
	}); err != nil {
		t.Fatalf("append leader log entry: %v", err)
	}

	leader.mu.Lock()
	leader.state.Persistent.CurrentTerm = 1
	leader.mu.Unlock()

	leader.becomeLeader()

	leader.mu.Lock()
	leader.state.Volatile.CommitIndex = 1
	leader.mu.Unlock()

	// Give the followers the same committed entry so the
	// AppendEntries ReadIndex probes succeed.
	for _, follower := range []*RaftNode{
		followerB,
		followerC,
	} {
		if err := follower.Log().Append(LogEntry{
			Index: 1,
			Term:  1,
			Data:  []byte("committed"),
		}); err != nil {
			t.Fatalf("append follower log entry: %v", err)
		}

		follower.mu.Lock()
		follower.state.Persistent.CurrentTerm = 1
		follower.state.Volatile.CommitIndex = 1
		follower.mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	index, err := leader.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected ReadIndex=1, got %d", index)
	}
}

func TestReadIndexNoQuorum(t *testing.T) {
	leader := NewRaftNode("A")
	followerB := NewRaftNode("B")
	followerC := NewRaftNode("C")

	leader.SetPeers([]Peer{
		followerB,
		followerC,
	})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("committed"),
	}); err != nil {
		t.Fatalf("append leader log entry: %v", err)
	}

	leader.mu.Lock()
	leader.state.Persistent.CurrentTerm = 1
	leader.mu.Unlock()

	leader.becomeLeader()

	leader.mu.Lock()
	leader.state.Volatile.CommitIndex = 1
	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()

	_, err := leader.ReadIndex(ctx)
	if err == nil {
		t.Fatal("expected ReadIndex to fail without quorum")
	}
}

func TestReadIndexHigherTermReply(t *testing.T) {
	leader := NewRaftNode("A")
	followerB := NewRaftNode("B")
	followerC := NewRaftNode("C")

	leader.SetPeers([]Peer{
		followerB,
		followerC,
	})

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("committed"),
	}); err != nil {
		t.Fatalf("append leader log entry: %v", err)
	}

	leader.mu.Lock()
	leader.state.Persistent.CurrentTerm = 1
	leader.mu.Unlock()

	leader.becomeLeader()

	leader.mu.Lock()
	leader.state.Volatile.CommitIndex = 1
	leader.mu.Unlock()

	// Force one follower to have a higher term.
	followerB.mu.Lock()
	followerB.state.Persistent.CurrentTerm = 2
	followerB.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	_, err := leader.ReadIndex(ctx)
	if err == nil {
		t.Fatal("expected ReadIndex to fail after higher-term reply")
	}

	state := leader.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected leader to step down to Follower, got %v",
			state.Role,
		)
	}

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term 2 after higher-term reply, got %d",
			state.Persistent.CurrentTerm,
		)
	}
}

type diskFullStorage struct {
	diskFull bool
}

func (s *diskFullStorage) SaveState(model.PersistentState) error {
	return nil
}

func (s *diskFullStorage) LoadState() (model.PersistentState, error) {
	return model.PersistentState{}, nil
}

func (s *diskFullStorage) AppendEntries([]model.LogEntry) error {
	return nil
}

func (s *diskFullStorage) ReplaceSuffix(
	model.LogIndex,
	[]model.LogEntry,
) error {
	return nil
}

func (s *diskFullStorage) LoadEntries() ([]model.LogEntry, error) {
	return nil, nil
}

func (s *diskFullStorage) SaveSnapshot(model.Snapshot) error {
	return nil
}

func (s *diskFullStorage) LoadSnapshot() (model.Snapshot, error) {
	return model.Snapshot{}, nil
}

func (s *diskFullStorage) Sync() error {
	if s.diskFull {
		return storage.ErrWALDiskFull
	}

	return nil
}

func (s *diskFullStorage) Close() error {
	return nil
}

func TestProposeDiskFullStepsDownLeader(t *testing.T) {
	store := &diskFullStorage{}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("NewRaftNodeWithStorage() error: %v", err)
	}

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if _, err := node.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	if state := node.State(); state.Role != Leader {
		t.Fatalf(
			"expected node to be leader, got %v",
			state.Role,
		)
	}

	// Simulate the WAL becoming full after the node
	// has already become leader.
	store.diskFull = true

	_, err = node.Propose([]byte("hello"))
	if err == nil {
		t.Fatal("expected Propose() to fail when WAL is full")
	}

	if !errors.Is(err, storage.ErrWALDiskFull) {
		t.Fatalf(
			"expected ErrWALDiskFull, got %v",
			err,
		)
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected leader to step down to follower, got %v",
			state.Role,
		)
	}

	// Direct election attempts must remain blocked while
	// storage is unhealthy.
	if _, err := node.startElection(); err == nil {
		t.Fatal("expected election to remain blocked while storage is unhealthy")
	}

	if state := node.State(); state.Role != Follower {
		t.Fatalf(
			"expected node to remain follower, got %v",
			state.Role,
		)
	}

	// Verify the real election-timeout path also cannot
	// transition the storage-blocked node back to Candidate.
	node.SetElectionTimeout(1)

	node.Tick()

	if state := node.State(); state.Role != Follower {
		t.Fatalf(
			"expected node to remain follower after election timeout, got %v",
			state.Role,
		)
	}

	// Once stepped down, the node must reject new proposals.
	_, err = node.Propose([]byte("second"))
	if err == nil {
		t.Fatal("expected proposal to be rejected after step-down")
	}
}

func TestStartElectionRejectsNonVoter(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 7
	node.state.Persistent.VotedFor = ""
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-2",
				"node-3",
			},
		},
	}
	node.mu.Unlock()

	term, err := node.startElection()

	require.Error(t, err)
	require.Contains(t, err.Error(), "not a voter")
	require.Equal(t, Term(7), term)

	state := node.State()

	require.Equal(t, Follower, state.Role)
	require.Equal(t, Term(7), state.Persistent.CurrentTerm)
	require.Empty(t, state.Persistent.VotedFor)
}

func TestProposeConfigurationDoesNotActivateBeforeCommit(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node A membership: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node B membership: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node C membership: %v", err)
	}

	nodeA.mu.Lock()
	nodeA.state.Role = Leader
	nodeA.state.Persistent.CurrentTerm = 1
	nodeA.mu.Unlock()

	before := nodeA.State().Persistent.Membership

	nextConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
			"D",
		},
	}

	index, err := nodeA.ProposeConfiguration(nextConfiguration)
	if err != nil {
		t.Fatalf("propose configuration: %v", err)
	}

	state := nodeA.State()

	if state.Volatile.CommitIndex >= index {
		t.Fatalf(
			"configuration entry should not be committed yet: commit=%d index=%d",
			state.Volatile.CommitIndex,
			index,
		)
	}

	if !reflect.DeepEqual(state.Persistent.Membership, before) {
		t.Fatalf(
			"uncommitted configuration changed membership: before=%+v after=%+v",
			before,
			state.Persistent.Membership,
		)
	}
}

func TestCommittedConfigurationActivates(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership A: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership B: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership C: %v", err)
	}

	nodeA.runElection()

	state := nodeA.State()

	if state.Role != Leader {
		t.Fatalf(
			"expected A to become Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != "A" {
		t.Fatalf(
			"expected leader A, got %q",
			state.LeaderID,
		)
	}

	nextConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
			"D",
		},
	}

	index, err := nodeA.ProposeConfiguration(nextConfiguration)
	if err != nil {
		t.Fatalf("propose configuration: %v", err)
	}

	state = nodeA.State()

	if state.Volatile.CommitIndex < index {
		t.Fatalf(
			"configuration entry should be committed: commit=%d index=%d",
			state.Volatile.CommitIndex,
			index,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current,
		nextConfiguration,
	) {
		entry, ok := nodeA.log.Get(index)

		t.Fatalf(
			"committed configuration was not activated: "+
				"expected=%+v actual=%+v "+
				"commit=%d last_applied=%d index=%d entry_exists=%t is_config=%t entry_data=%x",
			nextConfiguration,
			state.Persistent.Membership.Current,
			state.Volatile.CommitIndex,
			state.Volatile.LastApplied,
			index,
			ok,
			ok && IsConfigurationEntry(entry.Data),
			func() []byte {
				if !ok {
					return nil
				}
				return entry.Data
			}(),
		)
	}

	if state.Persistent.Membership.Joint != nil {
		t.Fatalf(
			"stable configuration unexpectedly has joint membership: %+v",
			state.Persistent.Membership.Joint,
		)
	}

	if state.Volatile.LastApplied < index {
		t.Fatalf(
			"configuration entry was committed but not applied: last_applied=%d index=%d",
			state.Volatile.LastApplied,
			index,
		)
	}
}

func TestJointConfigurationTransition(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node A membership: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node B membership: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node C membership: %v", err)
	}

	nodeA.runElection()

	state := nodeA.State()

	if state.Role != Leader {
		t.Fatalf("expected A to become Leader, got %v", state.Role)
	}

	oldConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
		},
	}

	newConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
			"D",
		},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	enterJointIndex, err := nodeA.Propose(enterJointData)
	if err != nil {
		t.Fatalf("propose enter-joint configuration: %v", err)
	}

	state = nodeA.State()

	if state.Volatile.CommitIndex < enterJointIndex {
		t.Fatalf(
			"enter-joint configuration should be committed: commit=%d index=%d",
			state.Volatile.CommitIndex,
			enterJointIndex,
		)
	}

	joint := state.Persistent.Membership.Joint

	if joint == nil {
		t.Fatal("expected joint membership after EnterJoint")
	}

	if !reflect.DeepEqual(joint.Old, oldConfiguration) {
		t.Fatalf(
			"unexpected joint old configuration: expected=%+v actual=%+v",
			oldConfiguration,
			joint.Old,
		)
	}

	if !reflect.DeepEqual(joint.New, newConfiguration) {
		t.Fatalf(
			"unexpected joint new configuration: expected=%+v actual=%+v",
			newConfiguration,
			joint.New,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current,
		oldConfiguration,
	) {
		t.Fatalf(
			"unexpected current configuration during joint consensus: expected=%+v actual=%+v",
			oldConfiguration,
			state.Persistent.Membership.Current,
		)
	}

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	leaveJointIndex, err := nodeA.Propose(leaveJointData)
	if err != nil {
		t.Fatalf("propose leave-joint configuration: %v", err)
	}

	state = nodeA.State()

	if state.Volatile.CommitIndex < leaveJointIndex {
		t.Fatalf(
			"leave-joint configuration should be committed: commit=%d index=%d",
			state.Volatile.CommitIndex,
			leaveJointIndex,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current,
		newConfiguration,
	) {
		t.Fatalf(
			"expected final configuration %v, got %v",
			newConfiguration,
			state.Persistent.Membership.Current,
		)
	}

	if state.Persistent.Membership.Joint != nil {
		t.Fatalf(
			"expected joint membership to be cleared, got %+v",
			state.Persistent.Membership.Joint,
		)
	}

	if state.Volatile.LastApplied < leaveJointIndex {
		t.Fatalf(
			"leave-joint configuration was committed but not applied: last_applied=%d index=%d",
			state.Volatile.LastApplied,
			leaveJointIndex,
		)
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

	node.log.Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("entry-1"),
	})
	node.log.Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("entry-2"),
	})

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
func TestRegisterPeer(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if len(node.peerIDs) != 1 {
		t.Fatalf(
			"expected 1 peer, got %d",
			len(node.peerIDs),
		)
	}

	if node.peerIDs[0] != "D" {
		t.Fatalf(
			"expected peer D, got %q",
			node.peerIDs[0],
		)
	}
}

func TestRegisterPeerRejectsDuplicate(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	if err := node.RegisterPeer("D"); err == nil {
		t.Fatal("expected duplicate peer registration to fail")
	}
}

func TestRegisterPeerRejectsSelf(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.RegisterPeer("A"); err == nil {
		t.Fatal("expected self registration to fail")
	}
}

func TestNewPeerReachableBeforeMembershipChange(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeD := NewRaftNode("D")

	transport := NewLocalTransport()

	if err := transport.AddNode(nodeD); err != nil {
		t.Fatalf("add node D to transport: %v", err)
	}

	if err := nodeA.SetTransport(
		transport,
		[]NodeID{},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := nodeA.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer D: %v", err)
	}

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     "A",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	}

	ctx := context.Background()

	reply, err := transport.AppendEntries(
		ctx,
		"D",
		args,
	)
	if err != nil {
		t.Fatalf("append entries to new peer D: %v", err)
	}

	if reply.Term != 1 {
		t.Fatalf(
			"expected D reply term 1, got %d",
			reply.Term,
		)
	}
}

func TestCatchUpPeerReplicatesExistingLog(t *testing.T) {
	leader := NewRaftNode("A")
	newPeer := NewRaftNode("D")

	transport := NewLocalTransport()

	if err := transport.AddNode(newPeer); err != nil {
		t.Fatalf("add new peer D to transport: %v", err)
	}

	if err := leader.SetTransport(
		transport,
		[]NodeID{},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := leader.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer D: %v", err)
	}

	entries := []LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("entry-1"),
		},
		{
			Index: 2,
			Term:  1,
			Data:  []byte("entry-2"),
		},
		{
			Index: 3,
			Term:  1,
			Data:  []byte("entry-3"),
		},
	}

	for _, entry := range entries {
		if err := leader.log.Append(entry); err != nil {
			t.Fatalf(
				"append leader entry %d: %v",
				entry.Index,
				err,
			)
		}
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	leader.initializeNewPeerReplicationStateLocked("D")

	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	if err := leader.catchUpPeer(ctx, "D", 3); err != nil {
		t.Fatalf("catch up peer D: %v", err)
	}

	if got := newPeer.log.LastIndex(); got != 3 {
		t.Fatalf(
			"expected peer D last index 3, got %d",
			got,
		)
	}

	for _, expected := range entries {
		entry, ok := newPeer.log.Get(expected.Index)
		if !ok {
			t.Fatalf(
				"peer D missing log entry %d",
				expected.Index,
			)
		}

		if entry.Term != expected.Term {
			t.Fatalf(
				"entry %d: expected term %d, got %d",
				expected.Index,
				expected.Term,
				entry.Term,
			)
		}

		if string(entry.Data) != string(expected.Data) {
			t.Fatalf(
				"entry %d: expected data %q, got %q",
				expected.Index,
				expected.Data,
				entry.Data,
			)
		}
	}

	leader.mu.RLock()
	matchIndex := leader.state.Leader.MatchIndex["D"]
	nextIndex := leader.state.Leader.NextIndex["D"]
	leader.mu.RUnlock()

	if matchIndex != 3 {
		t.Fatalf(
			"expected leader MatchIndex[D]=3, got %d",
			matchIndex,
		)
	}

	if nextIndex != 4 {
		t.Fatalf(
			"expected leader NextIndex[D]=4, got %d",
			nextIndex,
		)
	}
}

func TestWaitForApplied(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Volatile.LastApplied = 3
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	if err := node.waitForApplied(ctx, 3); err != nil {
		t.Fatalf("wait for applied: %v", err)
	}
}

func TestAddMember(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	newPeer := NewRaftNode("D")

	transport := NewLocalTransport()

	if err := transport.AddNode(leader); err != nil {
		t.Fatalf("add leader A to transport: %v", err)
	}

	if err := transport.AddNode(peerB); err != nil {
		t.Fatalf("add B to transport: %v", err)
	}

	if err := transport.AddNode(peerC); err != nil {
		t.Fatalf("add C to transport: %v", err)
	}

	if err := transport.AddNode(newPeer); err != nil {
		t.Fatalf("add D to transport: %v", err)
	}

	// Bootstrap the original cluster as A,B,C.
	// D is reachable through the transport but is not yet
	// registered as a Raft peer or voter.
	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		transport,
		[]NodeID{"A", "C"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		transport,
		[]NodeID{"A", "B"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := newPeer.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	// D becomes a communication peer only after the initial
	// membership has been established.
	if err := leader.RegisterPeer("D"); err != nil {
		t.Fatalf("register D: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.initializeNewPeerReplicationStateLocked("D")

	leader.mu.Unlock()

	entries := []LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("entry-1"),
		},
		{
			Index: 2,
			Term:  1,
			Data:  []byte("entry-2"),
		},
		{
			Index: 3,
			Term:  1,
			Data:  []byte("entry-3"),
		},
	}

	for _, entry := range entries {
		if err := leader.log.Append(entry); err != nil {
			t.Fatalf(
				"append leader entry %d: %v",
				entry.Index,
				err,
			)
		}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := leader.AddMember(ctx, "D"); err != nil {
		t.Fatalf("add member D: %v", err)
	}

	state := leader.State()

	if state.Persistent.Membership.Joint != nil {
		t.Fatal("expected final membership to be stable")
	}

	expectedVoters := []NodeID{
		"A",
		"B",
		"C",
		"D",
	}

	if len(state.Persistent.Membership.Current.Voters) !=
		len(expectedVoters) {
		t.Fatalf(
			"expected %d voters, got %d",
			len(expectedVoters),
			len(state.Persistent.Membership.Current.Voters),
		)
	}

	for _, expectedID := range expectedVoters {
		if !configurationContainsVoter(
			state.Persistent.Membership.Current,
			expectedID,
		) {
			t.Fatalf(
				"expected voter %s in final membership",
				expectedID,
			)
		}
	}

	if got := newPeer.log.LastIndex(); got < 3 {
		t.Fatalf(
			"expected new peer D last index >= 3, got %d",
			got,
		)
	}

	for _, expected := range entries {
		entry, ok := newPeer.log.Get(expected.Index)
		if !ok {
			t.Fatalf(
				"new peer D missing log entry %d",
				expected.Index,
			)
		}

		if entry.Term != expected.Term {
			t.Fatalf(
				"entry %d: expected term %d, got %d",
				expected.Index,
				expected.Term,
				entry.Term,
			)
		}

		if string(entry.Data) != string(expected.Data) {
			t.Fatalf(
				"entry %d: expected data %q, got %q",
				expected.Index,
				expected.Data,
				entry.Data,
			)
		}
	}

	leader.mu.RLock()
	matchIndex := leader.state.Leader.MatchIndex["D"]
	nextIndex := leader.state.Leader.NextIndex["D"]
	leader.mu.RUnlock()

	if matchIndex < 3 {
		t.Fatalf(
			"expected MatchIndex[D] >= 3, got %d",
			matchIndex,
		)
	}

	if nextIndex < 4 {
		t.Fatalf(
			"expected NextIndex[D] >= 4, got %d",
			nextIndex,
		)
	}
}

func TestRemoveMember(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	peerD := NewRaftNode("D")

	transport := NewLocalTransport()

	// Add all nodes to the transport.
	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		peerD,
	} {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	// Existing cluster is A,B,C,D.
	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C", "D"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		transport,
		[]NodeID{"A", "C", "D"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		transport,
		[]NodeID{"A", "B", "D"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := peerD.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C", "D"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := leader.RemoveMember(ctx, "D"); err != nil {
		t.Fatalf("remove member D: %v", err)
	}

	state := leader.State()

	// Final membership must be stable.
	if state.Persistent.Membership.Joint != nil {
		t.Fatal("expected final membership to be stable")
	}

	// Final membership must contain A,B,C.
	expectedVoters := []NodeID{
		"A",
		"B",
		"C",
	}

	if len(state.Persistent.Membership.Current.Voters) !=
		len(expectedVoters) {
		t.Fatalf(
			"expected %d voters, got %d",
			len(expectedVoters),
			len(state.Persistent.Membership.Current.Voters),
		)
	}

	for _, expectedID := range expectedVoters {
		if !configurationContainsVoter(
			state.Persistent.Membership.Current,
			expectedID,
		) {
			t.Fatalf(
				"expected voter %s in final membership",
				expectedID,
			)
		}
	}

	// D must no longer be a voter.
	if configurationContainsVoter(
		state.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal("expected D to be removed from final membership")
	}

	// The final membership must exactly match A,B,C.
	for _, voterID := range state.Persistent.Membership.Current.Voters {
		if voterID == "D" {
			t.Fatal("removed member D is still present")
		}
	}
}

func TestRemoveMemberRejectsNonVoter(t *testing.T) {
	node := NewRaftNode("A")

	peer := NewRaftNode("B")
	transport := NewLocalTransport()

	if err := transport.AddNode(node); err != nil {
		t.Fatalf("add A: %v", err)
	}

	if err := transport.AddNode(peer); err != nil {
		t.Fatalf("add B: %v", err)
	}

	if err := node.SetTransport(
		transport,
		[]NodeID{"B"},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "C")
	if err == nil {
		t.Fatal("expected removing non-voter to fail")
	}

	if !strings.Contains(err.Error(), "is not a voter") {
		t.Fatalf(
			"expected non-voter error, got %v",
			err,
		)
	}
}

func TestRemoveMemberRejectsJointConfiguration(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()

	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C"},
		},
		Joint: &model.JointConfiguration{
			Old: model.Configuration{
				Voters: []NodeID{"A", "B", "C"},
			},
			New: model.Configuration{
				Voters: []NodeID{"A", "B"},
			},
		},
	}

	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "C")
	if err == nil {
		t.Fatal("expected removal during joint configuration to fail")
	}

	if !strings.Contains(err.Error(), "joint configuration") {
		t.Fatalf(
			"expected joint-configuration error, got %v",
			err,
		)
	}
}

func TestRemoveMemberRejectsFollower(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C"},
		},
	}
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "B")
	if err == nil {
		t.Fatal("expected follower removal to fail")
	}

	if !strings.Contains(err.Error(), "not leader") {
		t.Fatalf(
			"expected not-leader error, got %v",
			err,
		)
	}
}

func TestRemoveMemberRejectsLeaderSelfRemoval(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C"},
		},
	}
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "A")
	if err == nil {
		t.Fatal("expected self-removal to be rejected")
	}

	if !strings.Contains(err.Error(), "transfer leadership first") {
		t.Fatalf(
			"expected leadership-transfer error, got %v",
			err,
		)
	}
}
func TestLeaderStepsDownAfterSelfRemoval(t *testing.T) {
	node := NewRaftNode("A")

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"B", "C"},
	}

	node.mu.Lock()

	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: oldConfiguration,
		Joint:   nil,
	}

	node.mu.Unlock()

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	node.mu.Lock()

	enterJointEntry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  enterJointData,
	}

	if err := node.applyConfigurationEntryLocked(
		enterJointEntry,
	); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply enter-joint configuration: %v", err)
	}

	node.mu.Unlock()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf(
			"expected leader to remain leader during joint configuration, got %v",
			state.Role,
		)
	}

	if state.Persistent.Membership.Joint == nil {
		t.Fatal("expected membership to be joint")
	}

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	node.mu.Lock()

	leaveJointEntry := LogEntry{
		Index: 2,
		Term:  1,
		Data:  leaveJointData,
	}

	if err := node.applyConfigurationEntryLocked(
		leaveJointEntry,
	); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply leave-joint configuration: %v", err)
	}

	node.mu.Unlock()

	state = node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected leader to step down after self-removal, got %v",
			state.Role,
		)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected LeaderID to be cleared after step-down, got %q",
			state.LeaderID,
		)
	}

	if state.Persistent.Membership.Joint != nil {
		t.Fatal("expected final membership to be stable")
	}

	if configurationContainsVoter(
		state.Persistent.Membership.Current,
		"A",
	) {
		t.Fatal("expected A to be removed from final membership")
	}

	if !configurationContainsVoter(
		state.Persistent.Membership.Current,
		"B",
	) {
		t.Fatal("expected B to remain a voter")
	}

	if !configurationContainsVoter(
		state.Persistent.Membership.Current,
		"C",
	) {
		t.Fatal("expected C to remain a voter")
	}
}

func TestRemovedLeaderCannotStartElection(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()

	node.state.Role = Follower
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"B", "C"},
		},
	}

	node.mu.Unlock()

	_, err := node.startElection()
	if err == nil {
		t.Fatal("expected removed leader to be rejected from election")
	}

	if !strings.Contains(err.Error(), "not a voter") {
		t.Fatalf(
			"expected not-a-voter error, got %v",
			err,
		)
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected node to remain follower, got %v",
			state.Role,
		)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term to remain 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}
}
func TestRemovedLeaderCannotPreVote(t *testing.T) {
	node := NewRaftNode("A")

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"B", "C"},
	}

	node.mu.Lock()

	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: oldConfiguration,
	}

	node.mu.Unlock()

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	node.mu.Lock()

	if err := node.applyConfigurationEntryLocked(LogEntry{
		Index: 1,
		Term:  1,
		Data:  enterJointData,
	}); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply enter-joint configuration: %v", err)
	}

	node.mu.Unlock()

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	node.mu.Lock()

	if err := node.applyConfigurationEntryLocked(LogEntry{
		Index: 2,
		Term:  1,
		Data:  leaveJointData,
	}); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply leave-joint configuration: %v", err)
	}

	node.mu.Unlock()

	state := node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected removed leader to become follower, got %v",
			state.Role,
		)
	}

	reply := node.PreVote(PreVoteArgs{
		Term:         state.Persistent.CurrentTerm + 1,
		CandidateID:  "A",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected removed leader PreVote to be rejected")
	}

	if reply.VoterID != "A" {
		t.Fatalf(
			"expected voter ID A, got %q",
			reply.VoterID,
		)
	}
}

func TestAddMemberFailsSafelyWhenNewPeerBecomesUnreachable(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	newPeer := NewRaftNode("D")

	transport := NewLocalTransport()

	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		newPeer,
	} {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	// D is reachable through the transport, but it is not yet
	// registered as a Raft peer or included in membership.
	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		transport,
		[]NodeID{"A", "C"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		transport,
		[]NodeID{"A", "B"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := newPeer.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	// D becomes a communication peer without becoming a voter.
	if err := leader.RegisterPeer("D"); err != nil {
		t.Fatalf("register D: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.initializeNewPeerReplicationStateLocked("D")

	leader.mu.Unlock()

	// D is unreachable before AddMember starts.
	//
	// The initial catch-up still succeeds because A and D both
	// have an empty log, so D is already caught up to index 0.
	transport.Block("A", "D")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	err := leader.AddMember(ctx, "D")
	if err == nil {
		t.Fatal(
			"expected AddMember to fail when new peer D is unreachable",
		)
	}

	state := leader.State()

	// EnterJoint must have committed using the surviving quorum:
	//
	// Old configuration: A,B,C
	// New configuration: A,B,C,D
	//
	// A,B,C satisfy both quorums, so EnterJoint can commit even
	// though D is unreachable.
	//
	// However, LeaveJoint must not be committed because D cannot
	// be caught up.
	if state.Persistent.Membership.Joint == nil {
		t.Fatal(
			"expected membership to remain joint after AddMember failure",
		)
	}

	joint := state.Persistent.Membership.Joint

	// Verify the old configuration.
	for _, voterID := range []NodeID{"A", "B", "C"} {
		if !configurationContainsVoter(
			joint.Old,
			voterID,
		) {
			t.Fatalf(
				"expected %s in old joint configuration",
				voterID,
			)
		}
	}

	// D must remain in the pending new configuration.
	if !configurationContainsVoter(
		joint.New,
		"D",
	) {
		t.Fatal(
			"expected D to remain in pending new configuration",
		)
	}

	// The stable configuration must still be the old configuration.
	if configurationContainsVoter(
		state.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal(
			"D must not appear in stable current configuration",
		)
	}
}

type removeMemberFailureTransport struct {
	*LocalTransport
}

func (t *removeMemberFailureTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	// Once RemoveMember proposes LeaveJoint, make C and D
	// unreachable for that specific configuration entry.
	//
	// EnterJoint is allowed through normally, so it can commit.
	if target == "C" || target == "D" {
		for _, entry := range args.Entries {
			if IsConfigurationEntry(entry.Data) &&
				len(entry.Data) >= 6 &&
				entry.Data[5] == configurationEntryTypeLeaveJoint {
				return AppendEntriesReply{}, fmt.Errorf(
					"simulated LeaveJoint failure to peer %s",
					target,
				)
			}
		}
	}

	return t.LocalTransport.AppendEntries(
		ctx,
		target,
		args,
	)
}

func (t *removeMemberFailureTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	return t.LocalTransport.RequestVote(ctx, target, args)
}

func (t *removeMemberFailureTransport) PreVote(
	ctx context.Context,
	target NodeID,
	args PreVoteArgs,
) (PreVoteReply, error) {
	return t.LocalTransport.PreVote(ctx, target, args)
}

func (t *removeMemberFailureTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	return t.LocalTransport.InstallSnapshot(ctx, target, args)
}

func TestRemoveMemberFailsSafelyDuringJointConsensus(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	peerD := NewRaftNode("D")

	localTransport := NewLocalTransport()

	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		peerD,
	} {
		if err := localTransport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	// Wrap the local transport so that only LeaveJoint
	// replication to C and D fails.
	transport := &removeMemberFailureTransport{
		LocalTransport: localTransport,
	}

	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C", "D"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		localTransport,
		[]NodeID{"A", "C", "D"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		localTransport,
		[]NodeID{"A", "B", "D"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := peerD.SetTransport(
		localTransport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C", "D"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	err := leader.RemoveMember(ctx, "D")
	if err == nil {
		t.Fatal(
			"expected RemoveMember to fail when LeaveJoint cannot reach old quorum",
		)
	}

	state := leader.State()

	// EnterJoint must have been committed, but LeaveJoint must
	// not have committed.
	if state.Persistent.Membership.Joint == nil {
		t.Fatal(
			"expected membership to remain joint after RemoveMember failure",
		)
	}

	joint := state.Persistent.Membership.Joint

	// Old configuration must remain A,B,C,D.
	for _, voterID := range []NodeID{"A", "B", "C", "D"} {
		if !configurationContainsVoter(
			joint.Old,
			voterID,
		) {
			t.Fatalf(
				"expected %s in old joint configuration",
				voterID,
			)
		}
	}

	// New configuration must be A,B,C.
	if configurationContainsVoter(
		joint.New,
		"D",
	) {
		t.Fatal(
			"D must not appear in the pending new configuration",
		)
	}

	for _, voterID := range []NodeID{"A", "B", "C"} {
		if !configurationContainsVoter(
			joint.New,
			voterID,
		) {
			t.Fatalf(
				"expected %s in new joint configuration",
				voterID,
			)
		}
	}

	// Stable membership must still contain D because LeaveJoint
	// was never committed.
	if !configurationContainsVoter(
		state.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal(
			"D must remain in stable membership until LeaveJoint commits",
		)
	}
}

func TestLeaderFailureDuringJointConsensus(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	peerD := NewRaftNode("D")

	transport := NewLocalTransport()

	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		peerD,
	} {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	leader.SetTransport(
		transport,
		[]NodeID{"B", "C", "D"},
	)

	peerB.SetTransport(
		transport,
		[]NodeID{"A", "C", "D"},
	)

	peerC.SetTransport(
		transport,
		[]NodeID{"A", "B", "D"},
	)

	peerD.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	)

	// Bootstrap the initial stable configuration.
	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	// Start A as the leader in term 1.
	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C", "D"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.mu.Unlock()

	// -----------------------------------------------------------------
	// 1. Enter joint consensus.
	//
	// Old configuration: A B C D
	// New configuration: A B C
	//
	// This represents removing D.
	// -----------------------------------------------------------------

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	enterJointIndex, err := leader.Propose(enterJointData)
	if err != nil {
		t.Fatalf("propose enter-joint configuration: %v", err)
	}

	if err := leader.waitForApplied(
		context.Background(),
		enterJointIndex,
	); err != nil {
		t.Fatalf(
			"wait for leader to apply enter-joint configuration: %v",
			err,
		)
	}

	leader.mu.RLock()

	if leader.state.Persistent.Membership.Joint == nil {
		leader.mu.RUnlock()

		t.Fatal("leader did not enter joint configuration")
	}

	leader.mu.RUnlock()

	// -----------------------------------------------------------------
	// 2. Make B, C, and D receive the committed joint configuration.
	//
	// This is important for the failure scenario:
	//
	// A dies
	// B becomes candidate
	//
	// Old quorum requires:
	//     3/4 -> B + C + D
	//
	// Therefore C and D must have the same committed joint log entry
	// and must be able to participate in the election.
	// -----------------------------------------------------------------

	for _, peerID := range []NodeID{"B", "C", "D"} {
		heartbeatArgs, ok := leader.buildAppendEntries(peerID)
		if !ok {
			t.Fatalf(
				"failed to build heartbeat for peer %s",
				peerID,
			)
		}

		// We only need the committed entry to reach the follower.
		// The follower will apply it because LeaderCommit is carried
		// by this AppendEntries RPC.
		heartbeatArgs.Entries = nil

		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)

		reply, err := transport.AppendEntries(
			ctx,
			peerID,
			heartbeatArgs,
		)

		cancel()

		if err != nil {
			t.Fatalf(
				"replicate committed joint configuration to %s: %v",
				peerID,
				err,
			)
		}

		if !reply.Success {
			t.Fatalf(
				"peer %s rejected committed joint configuration: %+v",
				peerID,
				reply,
			)
		}
	}

	// -----------------------------------------------------------------
	// 3. Verify all surviving voters have the joint configuration.
	// -----------------------------------------------------------------

	for _, node := range []*RaftNode{
		peerB,
		peerC,
		peerD,
	} {
		node.mu.RLock()

		membership := node.state.Persistent.Membership

		if membership.Joint == nil {
			node.mu.RUnlock()

			t.Fatalf(
				"node %s did not apply joint configuration",
				node.id,
			)
		}

		node.mu.RUnlock()
	}

	// -----------------------------------------------------------------
	// 4. Simulate A completely failing.
	//
	// Block both directions so A cannot communicate with B/C/D.
	// -----------------------------------------------------------------

	transport.Block("A", "B")
	transport.Block("A", "C")
	transport.Block("A", "D")

	transport.Block("B", "A")
	transport.Block("C", "A")
	transport.Block("D", "A")

	// Simulate election timeout on surviving nodes.
	for _, node := range []*RaftNode{
		peerB,
		peerC,
		peerD,
	} {
		node.mu.Lock()

		node.state.LeaderID = ""
		node.electionElapsed = node.electionTimeout

		node.mu.Unlock()
	}

	// -----------------------------------------------------------------
	// 5. B must successfully pass PreVote.
	//
	// Joint quorum:
	//
	// Old: B + C + D = 3/4
	// New: B + C     = 2/3
	// -----------------------------------------------------------------

	if !peerB.runPreVote() {
		t.Fatal("B failed PreVote after A failure")
	}

	// -----------------------------------------------------------------
	// 6. B starts a real election.
	// -----------------------------------------------------------------

	if _, err := peerB.startElection(); err != nil {
		t.Fatalf(
			"B failed to start election: %v",
			err,
		)
	}

	peerB.requestVotes()
	peerB.tryBecomeLeader()

	// -----------------------------------------------------------------
	// 7. Verify B became leader while still in joint consensus.
	// -----------------------------------------------------------------

	bState := peerB.State()

	if bState.Role != Leader {
		t.Fatalf(
			"expected B to become leader after A failure, got %v",
			bState.Role,
		)
	}

	if bState.Persistent.Membership.Joint == nil {
		t.Fatal(
			"B became leader without retaining joint configuration",
		)
	}

	// -----------------------------------------------------------------
	// 8. B commits LeaveJoint.
	//
	// Final configuration:
	//     A B C
	//
	// D is removed.
	// -----------------------------------------------------------------

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	leaveJointIndex, err := peerB.Propose(leaveJointData)
	if err != nil {
		t.Fatalf(
			"B failed to propose leave-joint configuration: %v",
			err,
		)
	}

	if err := peerB.waitForApplied(
		context.Background(),
		leaveJointIndex,
	); err != nil {
		t.Fatalf(
			"wait for B to apply leave-joint configuration: %v",
			err,
		)
	}

	// -----------------------------------------------------------------
	// 9. Verify final stable configuration.
	// -----------------------------------------------------------------

	bState = peerB.State()

	if bState.Persistent.Membership.Joint != nil {
		t.Fatal(
			"expected B to leave joint configuration",
		)
	}

	expectedVoters := []NodeID{
		"A",
		"B",
		"C",
	}

	actualVoters := bState.Persistent.Membership.Current.Voters

	if !reflect.DeepEqual(actualVoters, expectedVoters) {
		t.Fatalf(
			"unexpected final voters: got %v, want %v",
			actualVoters,
			expectedVoters,
		)
	}

	for _, voterID := range expectedVoters {
		if !configurationContainsVoter(
			bState.Persistent.Membership.Current,
			voterID,
		) {
			t.Fatalf(
				"expected %s to remain a voter",
				voterID,
			)
		}
	}

	if configurationContainsVoter(
		bState.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal("D should have been removed from final configuration")
	}
}

func TestRestartDuringJointConsensus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	// ---------------------------------------------------------------
	// 1. Create persistent storage and initial node.
	// ---------------------------------------------------------------

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	node, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		store.Close()
		t.Fatalf("create raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 2. Bootstrap the initial stable membership.
	// ---------------------------------------------------------------

	if err := node.BootstrapMembership(); err != nil {
		store.Close()
		t.Fatalf("bootstrap membership: %v", err)
	}

	initialMembership := model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C", "D"},
		},
	}

	node.mu.Lock()

	node.state.Persistent.Membership = initialMembership

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("persist initial membership: %v", err)
	}

	node.mu.Unlock()

	// ---------------------------------------------------------------
	// 3. Build and apply EnterJoint configuration.
	//
	// Old: A B C D
	// New: A B C
	// ---------------------------------------------------------------

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		store.Close()
		t.Fatalf("encode enter-joint entry: %v", err)
	}

	node.mu.Lock()

	entry := model.LogEntry{
		Index: node.log.LastIndex() + 1,
		Term:  1,
		Data:  enterJointData,
	}

	if err := store.AppendEntries([]model.LogEntry{entry}); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry: %v", err)
	}

	if err := store.Sync(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("sync enter-joint entry: %v", err)
	}

	if err := node.log.Append(entry); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry to memory log: %v", err)
	}

	node.state.Volatile.CommitIndex = entry.Index

	node.mu.Unlock()

	node.applyCommitted()

	// ---------------------------------------------------------------
	// 4. Verify node is currently in Joint configuration.
	// ---------------------------------------------------------------

	state := node.State()

	if state.Persistent.Membership.Joint == nil {
		store.Close()
		t.Fatal("expected node to be in joint configuration before restart")
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Joint.Old.Voters,
		oldConfiguration.Voters,
	) {
		store.Close()
		t.Fatalf(
			"unexpected old configuration before restart: got %v, want %v",
			state.Persistent.Membership.Joint.Old.Voters,
			oldConfiguration.Voters,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Joint.New.Voters,
		newConfiguration.Voters,
	) {
		store.Close()
		t.Fatalf(
			"unexpected new configuration before restart: got %v, want %v",
			state.Persistent.Membership.Joint.New.Voters,
			newConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 5. Simulate crash/restart by closing the WAL and reopening it.
	// ---------------------------------------------------------------

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL before restart: %v", err)
	}

	reopenedStore, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	restored, err := NewRaftNodeWithStorage(
		"A",
		reopenedStore,
	)
	if err != nil {
		reopenedStore.Close()
		t.Fatalf("restore raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 6. Verify the restarted node recovered Joint configuration.
	//
	// It MUST NOT incorrectly recover the new stable configuration.
	// LeaveJoint was never committed.
	// ---------------------------------------------------------------

	restoredState := restored.State()

	if restoredState.Persistent.Membership.Joint == nil {
		reopenedStore.Close()
		t.Fatal(
			"expected restarted node to recover joint configuration",
		)
	}

	restoredJoint := restoredState.Persistent.Membership.Joint

	if !reflect.DeepEqual(
		restoredJoint.Old.Voters,
		oldConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered old configuration: got %v, want %v",
			restoredJoint.Old.Voters,
			oldConfiguration.Voters,
		)
	}

	if !reflect.DeepEqual(
		restoredJoint.New.Voters,
		newConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered new configuration: got %v, want %v",
			restoredJoint.New.Voters,
			newConfiguration.Voters,
		)
	}

	// Current remains the old configuration while joint consensus
	// is active.
	if !reflect.DeepEqual(
		restoredState.Persistent.Membership.Current.Voters,
		oldConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered current configuration: got %v, want %v",
			restoredState.Persistent.Membership.Current.Voters,
			oldConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 7. Verify the restored node is still a voter.
	// ---------------------------------------------------------------

	if !membershipIsVoter(
		restoredState.Persistent.Membership,
		"A",
	) {
		reopenedStore.Close()
		t.Fatal(
			"restored node A should remain a voter during joint consensus",
		)
	}

	if err := reopenedStore.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
}

func TestRestartAfterLeaveJoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	// ---------------------------------------------------------------
	// 1. Create persistent storage and node.
	// ---------------------------------------------------------------

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	node, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		store.Close()
		t.Fatalf("create raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 2. Bootstrap the initial stable configuration.
	// ---------------------------------------------------------------

	initialConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	node.mu.Lock()

	node.state.Persistent.Membership = model.Membership{
		Current: initialConfiguration,
		Joint:   nil,
	}

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("persist initial membership: %v", err)
	}

	node.mu.Unlock()

	// ---------------------------------------------------------------
	// 3. Persist and apply EnterJoint.
	//
	// Old: A B C D
	// New: A B C
	// ---------------------------------------------------------------

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		initialConfiguration,
		newConfiguration,
	)
	if err != nil {
		store.Close()
		t.Fatalf("encode enter-joint entry: %v", err)
	}

	node.mu.Lock()

	enterJointEntry := model.LogEntry{
		Index: node.log.LastIndex() + 1,
		Term:  1,
		Data:  enterJointData,
	}

	if err := store.AppendEntries(
		[]model.LogEntry{enterJointEntry},
	); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry: %v", err)
	}

	if err := store.Sync(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("sync enter-joint entry: %v", err)
	}

	if err := node.log.Append(enterJointEntry); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry to memory log: %v", err)
	}

	node.state.Volatile.CommitIndex = enterJointEntry.Index

	node.mu.Unlock()

	node.applyCommitted()

	// ---------------------------------------------------------------
	// 4. Verify the node entered Joint configuration.
	// ---------------------------------------------------------------

	state := node.State()

	if state.Persistent.Membership.Joint == nil {
		store.Close()
		t.Fatal("expected node to be in joint configuration")
	}

	// ---------------------------------------------------------------
	// 5. Persist and apply LeaveJoint.
	//
	// Final stable configuration: A B C
	// ---------------------------------------------------------------

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		store.Close()
		t.Fatalf("encode leave-joint entry: %v", err)
	}

	node.mu.Lock()

	leaveJointEntry := model.LogEntry{
		Index: node.log.LastIndex() + 1,
		Term:  1,
		Data:  leaveJointData,
	}

	if err := store.AppendEntries(
		[]model.LogEntry{leaveJointEntry},
	); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append leave-joint entry: %v", err)
	}

	if err := store.Sync(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("sync leave-joint entry: %v", err)
	}

	if err := node.log.Append(leaveJointEntry); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append leave-joint entry to memory log: %v", err)
	}

	node.state.Volatile.CommitIndex = leaveJointEntry.Index

	node.mu.Unlock()

	node.applyCommitted()

	// ---------------------------------------------------------------
	// 6. Verify the transition completed before restart.
	// ---------------------------------------------------------------

	state = node.State()

	if state.Persistent.Membership.Joint != nil {
		store.Close()
		t.Fatal("expected joint configuration to be cleared")
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current.Voters,
		newConfiguration.Voters,
	) {
		store.Close()
		t.Fatalf(
			"unexpected stable configuration before restart: got %v, want %v",
			state.Persistent.Membership.Current.Voters,
			newConfiguration.Voters,
		)
	}

	if membershipIsVoter(
		state.Persistent.Membership,
		"D",
	) {
		store.Close()
		t.Fatal("D should no longer be a voter before restart")
	}

	// ---------------------------------------------------------------
	// 7. Simulate restart.
	// ---------------------------------------------------------------

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL before restart: %v", err)
	}

	reopenedStore, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	restored, err := NewRaftNodeWithStorage(
		"A",
		reopenedStore,
	)
	if err != nil {
		reopenedStore.Close()
		t.Fatalf("restore raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 8. Verify stable configuration was recovered.
	// ---------------------------------------------------------------

	restoredState := restored.State()

	if restoredState.Persistent.Membership.Joint != nil {
		reopenedStore.Close()
		t.Fatal(
			"restarted node incorrectly recovered joint configuration",
		)
	}

	if !reflect.DeepEqual(
		restoredState.Persistent.Membership.Current.Voters,
		newConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered stable configuration: got %v, want %v",
			restoredState.Persistent.Membership.Current.Voters,
			newConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 9. Verify D remains removed after restart.
	// ---------------------------------------------------------------

	if membershipIsVoter(
		restoredState.Persistent.Membership,
		"D",
	) {
		reopenedStore.Close()
		t.Fatal(
			"D should not be a voter after restart",
		)
	}

	// ---------------------------------------------------------------
	// 10. Verify A, B, and C remain voters.
	// ---------------------------------------------------------------

	for _, voterID := range []NodeID{"A", "B", "C"} {
		if !membershipIsVoter(
			restoredState.Persistent.Membership,
			voterID,
		) {
			reopenedStore.Close()

			t.Fatalf(
				"%s should remain a voter after restart",
				voterID,
			)
		}
	}

	if err := reopenedStore.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
}

func TestRestartedJointMembershipPreservesQuorumSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	// ---------------------------------------------------------------
	// 1. Create a node with persistent joint membership.
	// ---------------------------------------------------------------

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	jointMembership := model.Membership{
		Current: oldConfiguration,
		Joint: &model.JointConfiguration{
			Old: oldConfiguration,
			New: newConfiguration,
		},
	}

	node, err := NewRaftNodeWithStorage("B", store)
	if err != nil {
		store.Close()
		t.Fatalf("create raft node: %v", err)
	}

	node.mu.Lock()

	node.state.Persistent.Membership = jointMembership

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("persist joint membership: %v", err)
	}

	node.mu.Unlock()

	// ---------------------------------------------------------------
	// 2. Close and reopen the WAL to simulate restart.
	// ---------------------------------------------------------------

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL before restart: %v", err)
	}

	reopenedStore, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	restored, err := NewRaftNodeWithStorage(
		"B",
		reopenedStore,
	)
	if err != nil {
		reopenedStore.Close()
		t.Fatalf("restore raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 3. Verify the restarted node recovered Joint configuration.
	// ---------------------------------------------------------------

	state := restored.State()
	membership := state.Persistent.Membership

	if membership.Joint == nil {
		reopenedStore.Close()

		t.Fatal(
			"expected restarted node to recover joint membership",
		)
	}

	if !reflect.DeepEqual(
		membership.Joint.Old.Voters,
		oldConfiguration.Voters,
	) {
		reopenedStore.Close()

		t.Fatalf(
			"unexpected recovered old configuration: got %v, want %v",
			membership.Joint.Old.Voters,
			oldConfiguration.Voters,
		)
	}

	if !reflect.DeepEqual(
		membership.Joint.New.Voters,
		newConfiguration.Voters,
	) {
		reopenedStore.Close()

		t.Fatalf(
			"unexpected recovered new configuration: got %v, want %v",
			membership.Joint.New.Voters,
			newConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 4. Verify the restarted local node is still a voter.
	// ---------------------------------------------------------------

	if !membershipIsVoter(membership, "B") {
		reopenedStore.Close()

		t.Fatal(
			"restarted node B should remain a voter",
		)
	}

	// D is still a voter during Joint Consensus because D belongs
	// to the old configuration.
	if !membershipIsVoter(membership, "D") {
		reopenedStore.Close()

		t.Fatal(
			"D should remain a voter during joint consensus",
		)
	}

	// ---------------------------------------------------------------
	// 5. Verify old configuration quorum.
	//
	// B + C = 2/4 -> insufficient.
	// B + C + D = 3/4 -> sufficient.
	// ---------------------------------------------------------------

	oldPartialVotes := map[NodeID]struct{}{
		"B": {},
		"C": {},
	}

	if configurationHasQuorum(
		membership.Joint.Old,
		oldPartialVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C must not satisfy the old configuration quorum",
		)
	}

	oldFullVotes := map[NodeID]struct{}{
		"B": {},
		"C": {},
		"D": {},
	}

	if !configurationHasQuorum(
		membership.Joint.Old,
		oldFullVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C + D must satisfy the old configuration quorum",
		)
	}

	// ---------------------------------------------------------------
	// 6. Verify new configuration quorum.
	//
	// B + C = 2/3 -> sufficient.
	// ---------------------------------------------------------------

	newVotes := map[NodeID]struct{}{
		"B": {},
		"C": {},
	}

	if !configurationHasQuorum(
		membership.Joint.New,
		newVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C must satisfy the new configuration quorum",
		)
	}

	// ---------------------------------------------------------------
	// 7. Verify the actual Joint quorum requires BOTH configurations.
	//
	// B + C:
	//
	// Old = 2/4 -> ❌
	// New = 2/3 -> ✅
	//
	// Therefore joint quorum must be ❌.
	// ---------------------------------------------------------------

	if membershipHasQuorum(
		membership,
		oldPartialVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C must not satisfy joint quorum",
		)
	}

	// ---------------------------------------------------------------
	// 8. B + C + D:
	//
	// Old = 3/4 -> ✅
	// New = 2/3 -> ✅
	//
	// Therefore joint quorum must be ✅.
	// ---------------------------------------------------------------

	if !membershipHasQuorum(
		membership,
		oldFullVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C + D must satisfy joint quorum",
		)
	}

	// ---------------------------------------------------------------
	// 9. Verify D is still counted during the old configuration.
	// ---------------------------------------------------------------

	if !configurationContainsVoter(
		membership.Joint.Old,
		"D",
	) {
		reopenedStore.Close()

		t.Fatal(
			"D must remain in the old configuration during joint consensus",
		)
	}

	if configurationContainsVoter(
		membership.Joint.New,
		"D",
	) {
		reopenedStore.Close()

		t.Fatal(
			"D must not be in the new configuration",
		)
	}

	if err := reopenedStore.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
}
