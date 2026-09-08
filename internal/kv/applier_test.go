package kv

import (
	"context"
	"testing"

	"github.com/sanchar127/raftiq/internal/raft"
)

func TestApplierRun(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	applyCh := make(chan raft.LogEntry, 2)

	putData, err := EncodeCommand(Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	})
	if err != nil {
		t.Fatal(err)
	}

	applyCh <- raft.LogEntry{
		Index: 1,
		Term:  1,
		Data:  putData,
	}

	deleteData, err := EncodeCommand(Command{
		Type: CommandDelete,
		Key:  "name",
	})
	if err != nil {
		t.Fatal(err)
	}

	applyCh <- raft.LogEntry{
		Index: 2,
		Term:  1,
		Data:  deleteData,
	}

	close(applyCh)

	if err := applier.Run(context.Background(), applyCh); err != nil {
		t.Fatal(err)
	}

	if _, ok := store.Get("name"); ok {
		t.Fatal("expected name to be deleted")
	}
}

func TestApplierStopsOnContextCancellation(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	applyCh := make(chan raft.LogEntry)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := applier.Run(ctx, applyCh); err != context.Canceled {
		t.Fatalf("expected context canceled, got %v", err)
	}
}
