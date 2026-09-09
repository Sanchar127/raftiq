package client

import (
	"context"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/server"
)

func TestClientKVOperations(t *testing.T) {
	nodeA := raft.NewRaftNode("A")
	nodeB := raft.NewRaftNode("B")
	nodeC := raft.NewRaftNode("C")

	nodeA.SetPeers([]raft.Peer{nodeB, nodeC})

	store := kv.NewStore()
	srv := server.NewServer(nodeA, store)
	cl := New(srv)

	nodeA.Start()
	defer nodeA.Stop()

	srv.Start()
	defer srv.Stop()

	waitForLeader(t, nodeA)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cl.Put(ctx, "name", []byte("raftiq")); err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	value, ok, err := cl.Get(ctx, "name")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}

	if !ok {
		t.Fatal("expected key to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected %q, got %q", "raftiq", string(value))
	}

	if err := cl.Delete(ctx, "name"); err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	_, ok, err = cl.Get(ctx, "name")
	if err != nil {
		t.Fatalf("Get() after Delete returned error: %v", err)
	}

	if ok {
		t.Fatal("expected key to be deleted")
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