package server

import (
	"context"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/stretchr/testify/require"
)

func TestSnapshotConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    SnapshotConfig
		wantError bool
	}{
		{
			name: "valid",
			config: SnapshotConfig{
				Interval:  time.Second,
				Threshold: 100,
			},
		},
		{
			name: "zero interval",
			config: SnapshotConfig{
				Interval:  0,
				Threshold: 100,
			},
			wantError: true,
		},
		{
			name: "negative interval",
			config: SnapshotConfig{
				Interval:  -time.Second,
				Threshold: 100,
			},
			wantError: true,
		},
		{
			name: "zero threshold",
			config: SnapshotConfig{
				Interval:  time.Second,
				Threshold: 0,
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := raft.NewRaftNode("A")
			require.NoError(t, node.BootstrapMembership())

			server := NewServer(node, kv.NewStore())

			err := server.SetSnapshotConfig(tt.config)

			if tt.wantError {
				require.ErrorIs(
					t,
					err,
					ErrInvalidSnapshotConfig,
				)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestMaybeCreateSnapshotRespectsThreshold(t *testing.T) {
	node := raft.NewRaftNode("A")
	require.NoError(t, node.BootstrapMembership())

	store := kv.NewStore()
	server := NewServer(node, store)

	node.Start()
	defer node.Stop()

	require.NoError(t, server.SetSnapshotConfig(SnapshotConfig{
		Interval:  time.Second,
		Threshold: 100,
	}))

	require.NoError(t, server.Start())
	defer server.Stop()

	waitForLeader(t, node)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	require.NoError(
		t,
		server.Put(ctx, "name", []byte("raftiq")),
	)

	require.NoError(t, server.maybeCreateSnapshot())

	snapshot, err := node.Snapshot()
	require.NoError(t, err)

	require.Equal(
		t,
		raft.LogIndex(0),
		snapshot.LastIncludedIndex,
	)
}

func TestSnapshotWorkerCreatesSnapshotAutomatically(t *testing.T) {
	node := raft.NewRaftNode("A")
	require.NoError(t, node.BootstrapMembership())

	store := kv.NewStore()
	server := NewServer(node, store)

	require.NoError(t, server.SetSnapshotConfig(SnapshotConfig{
		Interval:  10 * time.Millisecond,
		Threshold: 1,
	}))

	node.Start()
	defer node.Stop()

	require.NoError(t, server.Start())
	defer server.Stop()

	waitForLeader(t, node)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	require.NoError(
		t,
		server.Put(ctx, "name", []byte("raftiq")),
	)

	require.Eventually(
		t,
		func() bool {
			snapshot, err := node.Snapshot()
			if err != nil {
				return false
			}

			return snapshot.LastIncludedIndex > 0
		},
		time.Second,
		10*time.Millisecond,
	)

	snapshot, err := node.Snapshot()
	require.NoError(t, err)

	state := node.State()

	require.Greater(t, snapshot.LastIncludedIndex, raft.LogIndex(0))
	require.LessOrEqual(
		t,
		snapshot.LastIncludedIndex,
		state.Volatile.LastApplied,
	)

	require.NotEmpty(t, snapshot.Data)

	value, ok, err := server.Get(ctx, "name")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "raftiq", string(value))
}
