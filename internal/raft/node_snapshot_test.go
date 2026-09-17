package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"

	"github.com/stretchr/testify/require"
)

func TestCreateSnapshot(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node failed: %v", err)
	}

	node.becomeLeader()

	node.mu.Lock()

	for i := LogIndex(1); i <= 5; i++ {
		if err := node.log.Append(LogEntry{
			Index: i,
			Term:  1,
			Data:  []byte(fmt.Sprintf("command-%d", i)),
		}); err != nil {
			node.mu.Unlock()
			t.Fatalf("append failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = 5
	node.state.Volatile.LastApplied = 5

	node.mu.Unlock()

	snapshotData := []byte(`{"key":"value"}`)

	if err := node.CreateSnapshot(5, snapshotData); err != nil {
		t.Fatalf("create snapshot failed: %v", err)
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot failed: %v", err)
	}

	if snapshot.LastIncludedIndex != 5 {
		t.Fatalf(
			"expected snapshot index 5, got %d",
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedTerm != 1 {
		t.Fatalf(
			"expected snapshot term 1, got %d",
			snapshot.LastIncludedTerm,
		)
	}

	if string(snapshot.Data) != string(snapshotData) {
		t.Fatalf(
			"expected snapshot data %q, got %q",
			snapshotData,
			snapshot.Data,
		)
	}

	if node.Log().LastIncludedIndex() != 5 {
		t.Fatalf(
			"expected log snapshot boundary 5, got %d",
			node.Log().LastIncludedIndex(),
		)
	}

	if node.Log().LastIndex() != 5 {
		t.Fatalf(
			"expected last index 5, got %d",
			node.Log().LastIndex(),
		)
	}
}

func TestCreateSnapshotCompactsWALAndRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error: %v", err)
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		store.Close()
		t.Fatalf("create node failed: %v", err)
	}

	const totalEntries = LogIndex(10)
	const snapshotIndex = LogIndex(5)

	node.mu.Lock()

	for i := LogIndex(1); i <= totalEntries; i++ {
		entry := LogEntry{
			Index: i,
			Term:  1,
			Data: []byte(fmt.Sprintf(
				"large-command-payload-%d-%s",
				i,
				"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			)),
		}

		if err := node.log.Append(entry); err != nil {
			node.mu.Unlock()
			store.Close()
			t.Fatalf("append to raft log failed: %v", err)
		}

		if err := store.AppendEntries([]model.LogEntry{
			{
				Index: entry.Index,
				Term:  entry.Term,
				Data:  append([]byte(nil), entry.Data...),
			},
		}); err != nil {
			node.mu.Unlock()
			store.Close()
			t.Fatalf("append to WAL failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = snapshotIndex
	node.state.Volatile.LastApplied = snapshotIndex

	node.mu.Unlock()

	if err := store.Sync(); err != nil {
		store.Close()
		t.Fatalf("sync WAL failed: %v", err)
	}

	beforeInfo, err := os.Stat(path)
	if err != nil {
		store.Close()
		t.Fatalf("stat WAL before snapshot failed: %v", err)
	}

	if err := node.CreateSnapshot(
		snapshotIndex,
		[]byte(`{"key":"value"}`),
	); err != nil {
		store.Close()
		t.Fatalf("CreateSnapshot() failed: %v", err)
	}

	afterInfo, err := os.Stat(path)
	if err != nil {
		store.Close()
		t.Fatalf("stat WAL after snapshot failed: %v", err)
	}

	if afterInfo.Size() >= beforeInfo.Size() {
		store.Close()
		t.Fatalf(
			"expected WAL to shrink after snapshot: before=%d after=%d",
			beforeInfo.Size(),
			afterInfo.Size(),
		)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL failed: %v", err)
	}

	reopened, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL failed: %v", err)
	}
	defer reopened.Close()

	snapshot, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot() after reopen failed: %v", err)
	}

	if snapshot.LastIncludedIndex != snapshotIndex {
		t.Fatalf(
			"snapshot index = %d, want %d",
			snapshot.LastIncludedIndex,
			snapshotIndex,
		)
	}

	if snapshot.LastIncludedTerm != 1 {
		t.Fatalf(
			"snapshot term = %d, want 1",
			snapshot.LastIncludedTerm,
		)
	}

	entries, err := reopened.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() after reopen failed: %v", err)
	}

	if len(entries) != int(totalEntries-snapshotIndex) {
		t.Fatalf(
			"recovered %d entries, want %d",
			len(entries),
			totalEntries-snapshotIndex,
		)
	}

	for i, entry := range entries {
		wantIndex := snapshotIndex + LogIndex(i) + 1

		if entry.Index != wantIndex {
			t.Errorf(
				"recovered entry %d has index %d, want %d",
				i,
				entry.Index,
				wantIndex,
			)
		}

		if entry.Term != 1 {
			t.Errorf(
				"recovered entry %d has term %d, want 1",
				i,
				entry.Term,
			)
		}
	}
}

func TestCreateSnapshotRejectsUnappliedIndex(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()

	for i := LogIndex(1); i <= 3; i++ {
		if err := node.log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			node.mu.Unlock()
			t.Fatalf("append failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = 3
	node.state.Volatile.LastApplied = 2

	node.mu.Unlock()

	err := node.CreateSnapshot(3, []byte(`snapshot`))
	if err == nil {
		t.Fatal("expected unapplied snapshot index to be rejected")
	}
}

func TestCreateSnapshotRejectsUncommittedIndex(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()

	for i := LogIndex(1); i <= 3; i++ {
		if err := node.log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			node.mu.Unlock()
			t.Fatalf("append failed: %v", err)
		}
	}

	node.state.Volatile.CommitIndex = 2
	node.state.Volatile.LastApplied = 2

	node.mu.Unlock()

	err := node.CreateSnapshot(3, []byte(`snapshot`))
	if err == nil {
		t.Fatal("expected uncommitted snapshot index to be rejected")
	}
}

func TestCreateSnapshotRejectsIndexZero(t *testing.T) {
	node := NewRaftNode("node-1")

	err := node.CreateSnapshot(0, []byte(`snapshot`))
	if err == nil {
		t.Fatal("expected index zero snapshot to be rejected")
	}
}

func TestInstallSnapshotPersistsHigherTerm(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		return nil
	})

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  2,
		Data:              []byte(`{"key":"value"}`),
	})

	require.True(t, reply.Success)
	require.Equal(t, Term(2), reply.Term)

	state := node.State()

	require.Equal(t, Term(2), state.Persistent.CurrentTerm)
	require.Equal(t, NodeID(""), state.Persistent.VotedFor)

	restarted, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	restartedState := restarted.State()

	require.Equal(
		t,
		Term(2),
		restartedState.Persistent.CurrentTerm,
	)

	require.Equal(
		t,
		NodeID(""),
		restartedState.Persistent.VotedFor,
	)
}

func TestInstallSnapshotRestoresStateMachine(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	var restored model.Snapshot

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		restored = snapshot
		return nil
	})

	require.NoError(t, node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("entry-1"),
	}))

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte(`{"name":"raftiq"}`),
	})

	require.True(t, reply.Success)

	require.Equal(t, LogIndex(1), restored.LastIncludedIndex)
	require.Equal(t, Term(1), restored.LastIncludedTerm)
	require.Equal(
		t,
		[]byte(`{"name":"raftiq"}`),
		restored.Data,
	)

	state := node.State()

	require.Equal(t, LogIndex(1), state.Volatile.CommitIndex)
	require.Equal(t, LogIndex(1), state.Volatile.LastApplied)
}

func TestInstallSnapshotRestoreFailureDoesNotAdvanceRaftState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	require.NoError(t, node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("entry-1"),
	}))

	restoreErr := fmt.Errorf("state machine restore failed")

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		return restoreErr
	})

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte(`{"name":"raftiq"}`),
	})

	require.False(t, reply.Success)

	require.Equal(t, LogIndex(0), node.Log().LastIncludedIndex())

	state := node.State()

	require.Equal(t, LogIndex(0), state.Volatile.CommitIndex)
	require.Equal(t, LogIndex(0), state.Volatile.LastApplied)
}

func TestInstallSnapshotRejectsMissingStateMachineRestore(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	require.NoError(t, node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("entry-1"),
	}))

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
		Data:              []byte(`{"name":"raftiq"}`),
	})

	require.False(t, reply.Success)

	require.Equal(t, LogIndex(0), node.Log().LastIncludedIndex())

	state := node.State()

	require.Equal(t, LogIndex(0), state.Volatile.CommitIndex)
	require.Equal(t, LogIndex(0), state.Volatile.LastApplied)
}

func TestInstallSnapshotCompactsWALAndRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	store, err := storage.OpenWAL(path)
	require.NoError(t, err)

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		return nil
	})

	const totalEntries = LogIndex(10)
	const snapshotIndex = LogIndex(5)

	node.mu.Lock()

	for i := LogIndex(1); i <= totalEntries; i++ {
		entry := LogEntry{
			Index: i,
			Term:  1,
			Data: []byte(fmt.Sprintf(
				"large-command-payload-%d-%s",
				i,
				"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			)),
		}

		if err := node.log.Append(entry); err != nil {
			node.mu.Unlock()
			store.Close()
			t.Fatalf("append to raft log failed: %v", err)
		}

		if err := store.AppendEntries([]model.LogEntry{
			{
				Index: entry.Index,
				Term:  entry.Term,
				Data:  append([]byte(nil), entry.Data...),
			},
		}); err != nil {
			node.mu.Unlock()
			store.Close()
			t.Fatalf("append to WAL failed: %v", err)
		}
	}

	node.mu.Unlock()

	require.NoError(t, store.Sync())

	beforeInfo, err := os.Stat(path)
	require.NoError(t, err)

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-2",
		LastIncludedIndex: snapshotIndex,
		LastIncludedTerm:  1,
		Data:              []byte(`{"key":"value"}`),
	})

	require.True(t, reply.Success)

	afterInfo, err := os.Stat(path)
	require.NoError(t, err)

	require.Less(
		t,
		afterInfo.Size(),
		beforeInfo.Size(),
		"expected WAL to shrink after InstallSnapshot",
	)

	require.Equal(
		t,
		snapshotIndex,
		node.Log().LastIncludedIndex(),
	)

	require.Equal(
		t,
		snapshotIndex,
		node.State().Volatile.CommitIndex,
	)

	require.Equal(
		t,
		snapshotIndex,
		node.State().Volatile.LastApplied,
	)

	require.NoError(t, store.Close())

	reopened, err := storage.OpenWAL(path)
	require.NoError(t, err)
	defer reopened.Close()

	snapshot, err := reopened.LoadSnapshot()
	require.NoError(t, err)

	require.Equal(
		t,
		snapshotIndex,
		snapshot.LastIncludedIndex,
	)

	require.Equal(
		t,
		Term(1),
		snapshot.LastIncludedTerm,
	)

	require.Equal(
		t,
		[]byte(`{"key":"value"}`),
		snapshot.Data,
	)

	entries, err := reopened.LoadEntries()
	require.NoError(t, err)

	require.Len(
		t,
		entries,
		int(totalEntries-snapshotIndex),
	)

	for i, entry := range entries {
		require.Equal(
			t,
			snapshotIndex+LogIndex(i)+1,
			entry.Index,
		)

		require.Equal(
			t,
			Term(1),
			entry.Term,
		)
	}

	state, err := reopened.LoadState()
	require.NoError(t, err)

	require.Equal(
		t,
		Term(2),
		state.CurrentTerm,
	)

	require.Equal(
		t,
		NodeID(""),
		state.VotedFor,
	)
}

func TestInstallSnapshotHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	require.NoError(t, baseStore.SaveState(initialState))

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		return nil
	})

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              5,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  5,
		Data:              []byte(`{"key":"value"}`),
	})

	require.False(t, reply.Success)

	state := node.State()

	require.Equal(t, Term(2), state.Persistent.CurrentTerm)
	require.Equal(t, NodeID("old-candidate"), state.Persistent.VotedFor)
}

func TestInstallSnapshotHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	require.NoError(t, baseStore.SaveState(initialState))

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	node.SetSnapshotRestore(func(snapshot model.Snapshot) error {
		return nil
	})

	reply := node.InstallSnapshot(InstallSnapshotArgs{
		Term:              5,
		LeaderID:          "node-2",
		LastIncludedIndex: 1,
		LastIncludedTerm:  5,
		Data:              []byte(`{"key":"value"}`),
	})

	require.False(t, reply.Success)

	state := node.State()

	require.Equal(t, Term(2), state.Persistent.CurrentTerm)
	require.Equal(t, NodeID("old-candidate"), state.Persistent.VotedFor)
}

func TestHandleInstallSnapshotReplyHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	require.NoError(t, baseStore.SaveState(initialState))

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	args := InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-1",
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte(`{"key":"value"}`),
	}

	reply := InstallSnapshotReply{
		Term:       5,
		FollowerID: "node-1",
		Success:    true,
	}

	node.handleInstallSnapshotReply("node-2", args, reply)

	state := node.State()

	require.Equal(t, Term(2), state.Persistent.CurrentTerm)
	require.Equal(t, NodeID("old-candidate"), state.Persistent.VotedFor)
	require.Equal(t, Follower, state.Role)
}

func TestHandleInstallSnapshotReplyHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	require.NoError(t, baseStore.SaveState(initialState))

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	require.NoError(t, err)

	args := InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node-1",
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte(`{"key":"value"}`),
	}

	reply := InstallSnapshotReply{
		Term:       5,
		FollowerID: "node-1",
		Success:    true,
	}

	node.handleInstallSnapshotReply("node-2", args, reply)

	state := node.State()

	require.Equal(t, Term(2), state.Persistent.CurrentTerm)
	require.Equal(t, NodeID("old-candidate"), state.Persistent.VotedFor)
	require.Equal(t, Follower, state.Role)
}
