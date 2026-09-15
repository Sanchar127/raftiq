package raft

import (
	"context"
	"testing"
	"time"
)

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
