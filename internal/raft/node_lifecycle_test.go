package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
