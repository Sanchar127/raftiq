package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestApplyCommittedDoesNotApplyUncommittedEntry(t *testing.T) {
	node := NewRaftNode("node-1")

	entry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("uncommitted"),
	}

	if err := node.Log().Append(entry); err != nil {
		t.Fatalf("append entry: %v", err)
	}

	node.applyCommitted()

	select {
	case applied := <-node.ApplyCh():
		t.Fatalf("unexpected applied entry: %+v", applied)
	default:
	}

	state := node.State()

	if state.Volatile.CommitIndex != 0 {
		t.Fatalf(
			"expected commit index 0, got %d",
			state.Volatile.CommitIndex,
		)
	}

	if state.Volatile.LastApplied != 0 {
		t.Fatalf(
			"expected last applied 0, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestApplyCommittedAppliesEntriesInOrder(t *testing.T) {
	node := NewRaftNode("node-1")

	for i := LogIndex(1); i <= 3; i++ {
		entry := LogEntry{
			Index: i,
			Term:  1,
			Data:  []byte(fmt.Sprintf("command-%d", i)),
		}

		if err := node.Log().Append(entry); err != nil {
			t.Fatalf("append entry %d: %v", i, err)
		}
	}

	node.mu.Lock()
	node.state.Volatile.CommitIndex = 3
	node.mu.Unlock()

	node.applyCommitted()

	for expected := LogIndex(1); expected <= 3; expected++ {
		select {
		case applied := <-node.ApplyCh():
			if applied.Index != expected {
				t.Fatalf(
					"expected applied index %d, got %d",
					expected,
					applied.Index,
				)
			}

		case <-time.After(time.Second):
			t.Fatalf(
				"timed out waiting for applied entry %d",
				expected,
			)
		}
	}

	state := node.State()

	if state.Volatile.LastApplied != 3 {
		t.Fatalf(
			"expected last applied 3, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestApplyCommittedMissingEntryDoesNotAdvanceLastApplied(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Volatile.CommitIndex = 2
	node.mu.Unlock()

	node.applyCommitted()

	state := node.State()

	if state.Volatile.LastApplied != 0 {
		t.Fatalf(
			"expected last applied to remain 0, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestWaitAppliedWaitsForRequestedIndex(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Volatile.CommitIndex = 5
	node.state.Volatile.LastApplied = 5
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := node.WaitApplied(ctx, 6)

	if err == nil {
		t.Fatal("expected WaitApplied to time out for unapplied index")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf(
			"expected context deadline exceeded, got %v",
			err,
		)
	}
}
