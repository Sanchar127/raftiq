package raft

import (
	"context"
	"errors"
	"testing"
)

func TestLocalTransportRequestVote(t *testing.T) {
	transport := NewLocalTransport()

	node := NewRaftNode(NodeID("node-2"))

	if err := transport.AddNode(node); err != nil {
		t.Fatalf("add node: %v", err)
	}

	reply, err := transport.RequestVote(
		context.Background(),
		NodeID("node-2"),
		RequestVoteArgs{
			Term:        1,
			CandidateID: "node-1",
		},
	)
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}

	if reply.VoterID != NodeID("node-2") {
		t.Fatalf("unexpected voter ID: %s", reply.VoterID)
	}
}

func TestLocalTransportPeerNotFound(t *testing.T) {
	transport := NewLocalTransport()

	_, err := transport.RequestVote(
		context.Background(),
		NodeID("missing"),
		RequestVoteArgs{},
	)

	if !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("expected ErrPeerNotFound, got %v", err)
	}
}

func TestLocalTransportClosed(t *testing.T) {
	transport := NewLocalTransport()
	transport.Close()

	_, err := transport.RequestVote(
		context.Background(),
		NodeID("node-1"),
		RequestVoteArgs{},
	)

	if !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("expected ErrTransportClosed, got %v", err)
	}
}

func TestLocalTransportContextCancellation(t *testing.T) {
	transport := NewLocalTransport()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := transport.RequestVote(
		ctx,
		NodeID("node-1"),
		RequestVoteArgs{},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLocalTransportDuplicateNode(t *testing.T) {
	transport := NewLocalTransport()
	node := NewRaftNode(NodeID("node-1"))

	if err := transport.AddNode(node); err != nil {
		t.Fatalf("first add: %v", err)
	}

	if err := transport.AddNode(node); err == nil {
		t.Fatal("expected duplicate node error")
	}
}

func TestLocalTransportAppendEntries(t *testing.T) {
	transport := NewLocalTransport()

	node := NewRaftNode(NodeID("node-2"))

	if err := transport.AddNode(node); err != nil {
		t.Fatalf("add node: %v", err)
	}

	reply, err := transport.AppendEntries(
		context.Background(),
		NodeID("node-2"),
		AppendEntriesArgs{
			Term:     1,
			LeaderID: NodeID("node-1"),
		},
	)
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}

	if !reply.Success {
		t.Fatal("expected append entries to succeed")
	}
}

func TestLocalTransportInstallSnapshot(t *testing.T) {
	transport := NewLocalTransport()

	node := NewRaftNode(NodeID("node-2"))

	if err := transport.AddNode(node); err != nil {
		t.Fatalf("add node: %v", err)
	}

	reply, err := transport.InstallSnapshot(
		context.Background(),
		NodeID("node-2"),
		InstallSnapshotArgs{
			Term:              1,
			LeaderID:          NodeID("node-1"),
			LastIncludedIndex: 1,
			LastIncludedTerm:  1,
			Data:              []byte("snapshot"),
		},
	)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	if !reply.Success {
		t.Fatal("expected snapshot installation to succeed")
	}
}
