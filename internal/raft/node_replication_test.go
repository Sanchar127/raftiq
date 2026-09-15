package raft

import (
	"errors"
	"testing"

	"github.com/sanchar127/raftiq/internal/storage"
)

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
