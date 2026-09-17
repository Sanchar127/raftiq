package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
)

func TestAppendEntriesResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("B")
	node.SetElectionTimeout(10)

	node.Tick()
	node.Tick()

	node.mu.RLock()
	elapsedBefore := node.electionElapsed
	node.mu.RUnlock()

	if elapsedBefore != 2 {
		t.Fatalf("expected elapsed time 2, got %d", elapsedBefore)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "A",
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected Follower, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}

	node.mu.RLock()
	elapsedAfter := node.electionElapsed
	node.mu.RUnlock()

	if elapsedAfter != 0 {
		t.Fatalf("expected election timer to reset to 0, got %d", elapsedAfter)
	}
}

func TestAppendEntriesRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("B")
	node.SetElectionTimeout(10)

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	node.Tick()
	node.Tick()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 5
	node.state.Role = Follower
	node.state.LeaderID = "A"
	node.electionElapsed = 2
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     4,
		LeaderID: "C",
	})

	if reply.Success {
		t.Fatal("expected stale AppendEntries to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.LeaderID != "A" {
		t.Fatalf(
			"expected leader A to remain unchanged, got %q",
			state.LeaderID,
		)
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 2 {
		t.Fatalf(
			"expected election timer to remain 2, got %d",
			elapsed,
		)
	}
}

func TestAppendEntriesAppliesCommittedEntry(t *testing.T) {
	node := NewRaftNode("node-1")

	entry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("command"),
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		Entries:      []LogEntry{entry},
		LeaderCommit: 1,
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	select {
	case applied := <-node.ApplyCh():
		if applied.Index != 1 {
			t.Fatalf(
				"expected applied index 1, got %d",
				applied.Index,
			)
		}

		if string(applied.Data) != "command" {
			t.Fatalf(
				"expected applied data command, got %q",
				applied.Data,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("expected committed entry to be applied")
	}

	state := node.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected commit index 1, got %d",
			state.Volatile.CommitIndex,
		)
	}

	if state.Volatile.LastApplied != 1 {
		t.Fatalf(
			"expected last applied 1, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestAppendEntriesHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = "old-candidate"

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         5,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	restored, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected restored term 5, got %d",
			state.Persistent.CurrentTerm,
		)
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
}

func TestAppendEntriesHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail when higher-term persistence fails")
	}

	persisted, err := baseStore.LoadState()
	if err != nil {
		t.Fatalf("load persisted state: %v", err)
	}

	if persisted.CurrentTerm != 2 {
		t.Fatalf(
			"expected persisted term to remain 2, got %d",
			persisted.CurrentTerm,
		)
	}

	if persisted.VotedFor != "old-candidate" {
		t.Fatalf(
			"expected persisted vote to remain old-candidate, got %q",
			persisted.VotedFor,
		)
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected in-memory term to remain 2 after persistence failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "old-candidate" {
		t.Fatalf(
			"expected in-memory vote to remain old-candidate after persistence failure, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestAppendEntriesHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail when higher-term sync fails")
	}
}

func TestAppendEntriesStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         3,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	})

	if reply.Success {
		t.Fatal("expected stale-term AppendEntries to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term to remain 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf(
			"expected vote to remain for %q, got %q",
			node.id,
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != node.id {
		t.Fatalf(
			"expected leader ID to remain %q, got %q",
			node.id,
			state.LeaderID,
		)
	}
}

