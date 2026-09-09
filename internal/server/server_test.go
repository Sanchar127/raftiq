package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/lock"

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
func TestServerAcquireLock(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	grant, err := server.AcquireLock(
		ctx,
		"job:1",
		"worker-A",
		5000,
	)
	if err != nil {
		t.Fatalf("AcquireLock() returned error: %v", err)
	}

	if grant.Key != "job:1" {
		t.Fatalf("unexpected key: %q", grant.Key)
	}

	if grant.OwnerID != "worker-A" {
		t.Fatalf("unexpected owner: %q", grant.OwnerID)
	}

	if grant.FencingToken != 1 {
		t.Fatalf(
			"unexpected fencing token: got %d, want 1",
			grant.FencingToken,
		)
	}

	if grant.ExpiresAt <= time.Now().UnixNano() {
		t.Fatalf("lock expiration is not in the future")
	}
}

func TestServerAcquireLockBusy(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	first, err := server.AcquireLock(
		ctx,
		"job:1",
		"worker-A",
		5000,
	)
	if err != nil {
		t.Fatalf("first AcquireLock() returned error: %v", err)
	}

	if first.FencingToken != 1 {
		t.Fatalf("unexpected first token: %d", first.FencingToken)
	}

	_, err = server.AcquireLock(
		ctx,
		"job:1",
		"worker-B",
		5000,
	)
	if !errors.Is(err, lock.ErrLockBusy) {
		t.Fatalf(
			"expected ErrLockBusy, got %v",
			err,
		)
	}

	current, ok := store.GetLock("job:1")
	if !ok {
		t.Fatal("lock disappeared after contention")
	}

	if current.OwnerID != "worker-A" {
		t.Fatalf(
			"lock owner changed: got %q, want worker-A",
			current.OwnerID,
		)
	}

	if current.FencingToken != 1 {
		t.Fatalf(
			"fencing token changed: got %d, want 1",
			current.FencingToken,
		)
	}
}

func TestServerAcquireLockFencingTokensAreGlobal(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	first, err := server.AcquireLock(
		ctx,
		"job:1",
		"worker-A",
		5000,
	)
	if err != nil {
		t.Fatalf("first AcquireLock() returned error: %v", err)
	}

	second, err := server.AcquireLock(
		ctx,
		"job:2",
		"worker-B",
		5000,
	)
	if err != nil {
		t.Fatalf("second AcquireLock() returned error: %v", err)
	}

	if first.FencingToken != 1 {
		t.Fatalf(
			"first token: got %d, want 1",
			first.FencingToken,
		)
	}

	if second.FencingToken != 2 {
		t.Fatalf(
			"second token: got %d, want 2",
			second.FencingToken,
		)
	}

	if second.FencingToken <= first.FencingToken {
		t.Fatalf(
			"fencing tokens are not increasing: %d -> %d",
			first.FencingToken,
			second.FencingToken,
		)
	}
}

func TestStoreExpireLock(t *testing.T) {
	store := kv.NewStore()

	acquired, ok, err := store.AcquireLock(
		"job-1",
		"worker-a",
		time.Now().Add(time.Minute).UnixNano(),
		10,
	)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}

	if !ok {
		t.Fatal("expected lock acquisition")
	}

	if acquired.FencingToken != 1 {
		t.Fatalf(
			"expected fencing token 1, got %d",
			acquired.FencingToken,
		)
	}

	expired, ok, err := store.ExpireLock(
		"job-1",
		1,
	)
	if err != nil {
		t.Fatalf("expire lock: %v", err)
	}

	if !ok {
		t.Fatal("expected lock expiration")
	}

	if expired.FencingToken != 1 {
		t.Fatalf(
			"expected expired token 1, got %d",
			expired.FencingToken,
		)
	}

	if _, ok := store.GetLock("job-1"); ok {
		t.Fatal("expected lock to be removed")
	}
}

func TestStoreExpireLockRejectsStaleToken(t *testing.T) {
	store := kv.NewStore()

	first, ok, err := store.AcquireLock(
		"job-1",
		"worker-a",
		time.Now().Add(time.Minute).UnixNano(),
		10,
	)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	if !ok {
		t.Fatal("expected first acquisition")
	}

	_, ok, err = store.ExpireLock(
		"job-1",
		first.FencingToken,
	)
	if err != nil {
		t.Fatalf("first expire: %v", err)
	}

	if !ok {
		t.Fatal("expected first expiration")
	}

	second, ok, err := store.AcquireLock(
		"job-1",
		"worker-b",
		time.Now().Add(time.Minute).UnixNano(),
		20,
	)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}

	if !ok {
		t.Fatal("expected second acquisition")
	}

	if second.FencingToken != 2 {
		t.Fatalf(
			"expected fencing token 2, got %d",
			second.FencingToken,
		)
	}

	// Simulate an old expiration arriving late.
	_, expired, err := store.ExpireLock(
		"job-1",
		first.FencingToken,
	)
	if err != nil {
		t.Fatalf("stale expire: %v", err)
	}

	if expired {
		t.Fatal("stale expiration must not remove the new lock")
	}

	current, ok := store.GetLock("job-1")
	if !ok {
		t.Fatal("expected new lock to remain")
	}

	if current.OwnerID != "worker-b" {
		t.Fatalf(
			"expected worker-b, got %s",
			current.OwnerID,
		)
	}

	if current.FencingToken != second.FencingToken {
		t.Fatalf(
			"expected token %d, got %d",
			second.FencingToken,
			current.FencingToken,
		)
	}
}

func TestServerLockExpiresAutomatically(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	first, err := server.AcquireLock(
		ctx,
		"job:1",
		"worker-A",
		100,
	)
	if err != nil {
		t.Fatalf("AcquireLock() returned error: %v", err)
	}

	if first.FencingToken != 1 {
		t.Fatalf(
			"expected fencing token 1, got %d",
			first.FencingToken,
		)
	}

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if _, ok := store.GetLock("job:1"); !ok {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("lock did not expire automatically")
}

func TestServerExpiredLockCanBeReacquired(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	first, err := server.AcquireLock(
		ctx,
		"job:1",
		"worker-A",
		100,
	)
	if err != nil {
		t.Fatalf("first AcquireLock(): %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if _, ok := store.GetLock("job:1"); !ok {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if _, ok := store.GetLock("job:1"); ok {
		t.Fatal("first lock did not expire")
	}

	second, err := server.AcquireLock(
		ctx,
		"job:1",
		"worker-B",
		1000,
	)
	if err != nil {
		t.Fatalf("second AcquireLock(): %v", err)
	}

	if second.OwnerID != "worker-B" {
		t.Fatalf(
			"expected worker-B, got %q",
			second.OwnerID,
		)
	}

	if second.FencingToken != first.FencingToken+1 {
		t.Fatalf(
			"expected fencing token %d, got %d",
			first.FencingToken+1,
			second.FencingToken,
		)
	}
}
