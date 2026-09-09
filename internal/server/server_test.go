package server

import (
	"context"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestServerStartsAndStops(t *testing.T) {
	raftNode := raft.NewRaftNode("A")
	store := kv.NewStore()

	server := NewServer(raftNode, store)

	server.Start()
	server.Stop()
}

func TestServerGetReturnsReplicatedValue(t *testing.T) {
	nodeA := raft.NewRaftNode("A")
	nodeB := raft.NewRaftNode("B")
	nodeC := raft.NewRaftNode("C")

	nodeA.SetPeers([]raft.Peer{nodeB, nodeC})

	store := kv.NewStore()
	server := NewServer(nodeA, store)

	nodeA.Start()
	defer nodeA.Stop()

	server.Start()
	defer server.Stop()

	waitForLeader(t, nodeA)

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:  kv.CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	})
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	if _, err := nodeA.Propose(commandData); err != nil {
		t.Fatalf("Propose() returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	state := nodeA.State()

	t.Logf(
		"before GET: role=%v term=%d commit=%d applied=%d lastIndex=%d",
		state.Role,
		state.Persistent.CurrentTerm,
		state.Volatile.CommitIndex,
		state.Volatile.LastApplied,
		nodeA.Log().LastIndex(),
	)

	value, ok, err := server.Get(ctx, "name")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	if !ok {
		t.Fatal("expected key to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected value %q, got %q", "raftiq", string(value))
	}
}

func waitForLeader(t *testing.T, node *raft.RaftNode) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		if node.State().Role == raft.Leader {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("node %s did not become leader", node.ID())
}

func TestServerPutAndGet(t *testing.T) {
	nodeA := raft.NewRaftNode("A")
	nodeB := raft.NewRaftNode("B")
	nodeC := raft.NewRaftNode("C")

	nodeA.SetPeers([]raft.Peer{nodeB, nodeC})

	store := kv.NewStore()
	server := NewServer(nodeA, store)

	nodeA.Start()
	defer nodeA.Stop()

	server.Start()
	defer server.Stop()

	waitForLeader(t, nodeA)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := server.Put(ctx, "name", []byte("raftiq")); err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	value, ok, err := server.Get(ctx, "name")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	if !ok {
		t.Fatal("expected key to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected value %q, got %q", "raftiq", string(value))
	}
}

func TestServerDeleteAndGet(t *testing.T) {
	nodeA := raft.NewRaftNode("A")
	nodeB := raft.NewRaftNode("B")
	nodeC := raft.NewRaftNode("C")

	nodeA.SetPeers([]raft.Peer{nodeB, nodeC})

	store := kv.NewStore()
	server := NewServer(nodeA, store)

	nodeA.Start()
	defer nodeA.Stop()

	server.Start()
	defer server.Stop()

	waitForLeader(t, nodeA)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := server.Put(ctx, "name", []byte("raftiq")); err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	if err := server.Delete(ctx, "name"); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	value, ok, err := server.Get(ctx, "name")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	if ok {
		t.Fatalf("expected key to be deleted, got value %q", value)
	}
}
