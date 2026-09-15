package raft

import (
	"testing"
)

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
