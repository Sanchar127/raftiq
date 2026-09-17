package raft

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"

	"github.com/stretchr/testify/require"
)

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
func TestSingleNodeElection(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if _, err := node.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if !node.tryBecomeLeader() {
		t.Fatal("single-node candidate should become leader with its own vote")
	}

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}
}

func TestFourNodeElectionRequiresThreeVotes(t *testing.T) {
	node := NewRaftNode("A")

	node.SetPeers([]Peer{
		NewRaftNode("B"),
		NewRaftNode("C"),
		NewRaftNode("D"),
	})

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	if _, err := node.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	node.recordVote("B", term, true)

	if node.tryBecomeLeader() {
		t.Fatal("candidate must not become leader with 2/4 votes")
	}

	if node.State().Role != Candidate {
		t.Fatalf("expected Candidate, got %v", node.State().Role)
	}

	node.recordVote("C", term, true)

	if !node.tryBecomeLeader() {
		t.Fatal("candidate should become leader with 3/4 votes")
	}

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}
}

func TestJointElectionRequiresBothMajorities(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"A",
				"B",
				"C",
			},
		},
		Joint: &model.JointConfiguration{
			Old: model.Configuration{
				Voters: []NodeID{
					"A",
					"B",
					"C",
				},
			},
			New: model.Configuration{
				Voters: []NodeID{
					"A",
					"D",
					"E",
				},
			},
		},
	}
	node.mu.Unlock()

	if _, err := node.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	// A + B satisfies the old majority but not the new majority.
	node.recordVote("B", term, true)

	if node.tryBecomeLeader() {
		t.Fatal("candidate must not become leader with only old-config majority")
	}

	// A + D satisfies the new majority but not the old majority.
	// B remains recorded, so A+B+D now satisfies both configurations.
	node.recordVote("D", term, true)

	if !node.tryBecomeLeader() {
		t.Fatal("candidate should become leader after both majorities")
	}

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}
}

