package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLocalTransportDirectionalBlock(t *testing.T) {
	transport := NewLocalTransport()

	from := NodeID("node-1")
	to := NodeID("node-2")

	require.False(t, transport.IsBlocked(from, to))
	require.False(t, transport.IsBlocked(to, from))

	transport.Block(from, to)

	require.True(t, transport.IsBlocked(from, to))
	require.False(t, transport.IsBlocked(to, from))

	transport.Unblock(from, to)

	require.False(t, transport.IsBlocked(from, to))
	require.False(t, transport.IsBlocked(to, from))
}

func TestLocalTransportBidirectionalBlock(t *testing.T) {
	transport := NewLocalTransport()

	nodeA := NodeID("node-a")
	nodeB := NodeID("node-b")

	transport.BlockBidirectional(nodeA, nodeB)

	require.True(t, transport.IsBlocked(nodeA, nodeB))
	require.True(t, transport.IsBlocked(nodeB, nodeA))

	transport.UnblockBidirectional(nodeA, nodeB)

	require.False(t, transport.IsBlocked(nodeA, nodeB))
	require.False(t, transport.IsBlocked(nodeB, nodeA))
}

func TestLocalTransportRequestVoteBlocked(t *testing.T) {
	transport := NewLocalTransport()

	target := NewRaftNode(NodeID("node-2"))
	require.NoError(t, transport.AddNode(target))

	transport.Block(NodeID("node-1"), target.ID())

	_, err := transport.RequestVote(
		context.Background(),
		target.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: NodeID("node-1"),
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked")
}

func TestLocalTransportAppendEntriesBlocked(t *testing.T) {
	transport := NewLocalTransport()

	target := NewRaftNode(NodeID("node-2"))
	require.NoError(t, transport.AddNode(target))

	transport.Block(NodeID("node-1"), target.ID())

	_, err := transport.AppendEntries(
		context.Background(),
		target.ID(),
		AppendEntriesArgs{
			Term:     1,
			LeaderID: NodeID("node-1"),
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked")
}

func TestLocalTransportInstallSnapshotBlocked(t *testing.T) {
	transport := NewLocalTransport()

	target := NewRaftNode(NodeID("node-2"))
	require.NoError(t, transport.AddNode(target))

	transport.Block(NodeID("node-1"), target.ID())

	_, err := transport.InstallSnapshot(
		context.Background(),
		target.ID(),
		InstallSnapshotArgs{
			Term:     1,
			LeaderID: NodeID("node-1"),
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked")
}

func TestLocalTransportBlockDoesNotAffectReverseDirection(t *testing.T) {
	transport := NewLocalTransport()

	nodeA := NewRaftNode(NodeID("node-a"))
	nodeB := NewRaftNode(NodeID("node-b"))

	require.NoError(t, transport.AddNode(nodeA))
	require.NoError(t, transport.AddNode(nodeB))

	transport.Block(nodeA.ID(), nodeB.ID())

	_, err := transport.RequestVote(
		context.Background(),
		nodeB.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: nodeA.ID(),
		},
	)

	require.Error(t, err)

	_, err = transport.RequestVote(
		context.Background(),
		nodeA.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: nodeB.ID(),
		},
	)

	require.NoError(t, err)
}

func TestLocalTransportUnblockRestoresCommunication(t *testing.T) {
	transport := NewLocalTransport()

	target := NewRaftNode(NodeID("node-2"))
	require.NoError(t, transport.AddNode(target))

	from := NodeID("node-1")

	transport.Block(from, target.ID())

	_, err := transport.RequestVote(
		context.Background(),
		target.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: from,
		},
	)
	require.Error(t, err)

	transport.Unblock(from, target.ID())

	_, err = transport.RequestVote(
		context.Background(),
		target.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: from,
		},
	)
	require.NoError(t, err)
}

func TestLocalTransportCloseClearsState(t *testing.T) {
	transport := NewLocalTransport()

	node := NewRaftNode(NodeID("node-1"))
	require.NoError(t, transport.AddNode(node))

	transport.Block(node.ID(), NodeID("node-2"))
	require.True(t, transport.IsBlocked(node.ID(), NodeID("node-2")))

	transport.Close()

	require.False(t, transport.IsBlocked(node.ID(), NodeID("node-2")))

	_, err := transport.RequestVote(
		context.Background(),
		node.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: NodeID("node-2"),
		},
	)

	require.ErrorIs(t, err, ErrTransportClosed)
}

func TestLocalTransportBlockedRequestHonorsContext(t *testing.T) {
	transport := NewLocalTransport()

	target := NewRaftNode(NodeID("node-2"))
	require.NoError(t, transport.AddNode(target))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := transport.RequestVote(
		ctx,
		target.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: NodeID("node-1"),
		},
	)

	require.ErrorIs(t, err, context.Canceled)
}

func TestLocalTransportMissingPeer(t *testing.T) {
	transport := NewLocalTransport()

	_, err := transport.RequestVote(
		context.Background(),
		NodeID("missing"),
		RequestVoteArgs{
			Term:        1,
			CandidateID: NodeID("node-1"),
		},
	)

	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPeerNotFound))
}

func TestLocalTransportRPCContextTimeout(t *testing.T) {
	transport := NewLocalTransport()

	target := NewRaftNode(NodeID("node-2"))
	require.NoError(t, transport.AddNode(target))

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()

	time.Sleep(time.Millisecond)

	_, err := transport.RequestVote(
		ctx,
		target.ID(),
		RequestVoteArgs{
			Term:        1,
			CandidateID: NodeID("node-1"),
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
