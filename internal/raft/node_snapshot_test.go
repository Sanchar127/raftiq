package raft

import (
	"fmt"
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
