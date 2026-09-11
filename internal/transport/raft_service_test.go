package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/stretchr/testify/require"
)

type fakeRaftNode struct {
	requestVoteArgs      raft.RequestVoteArgs
	appendEntriesArgs    raft.AppendEntriesArgs
	installSnapshotArgs  raft.InstallSnapshotArgs
	requestVoteReply     raft.RequestVoteReply
	appendEntriesReply   raft.AppendEntriesReply
	installSnapshotReply raft.InstallSnapshotReply
}

func (f *fakeRaftNode) RequestVote(args raft.RequestVoteArgs) raft.RequestVoteReply {
	f.requestVoteArgs = args
	return f.requestVoteReply
}

func (f *fakeRaftNode) AppendEntries(args raft.AppendEntriesArgs) raft.AppendEntriesReply {
	f.appendEntriesArgs = args
	return f.appendEntriesReply
}

func (f *fakeRaftNode) InstallSnapshot(args raft.InstallSnapshotArgs) raft.InstallSnapshotReply {
	f.installSnapshotArgs = args
	return f.installSnapshotReply
}

func TestNewRaftService(t *testing.T) {
	t.Parallel()

	service, err := NewRaftService(&fakeRaftNode{})

	require.NoError(t, err)
	require.NotNil(t, service)
}

func TestNewRaftServiceRejectsNilNode(t *testing.T) {
	t.Parallel()

	service, err := NewRaftService(nil)

	require.Error(t, err)
	require.Nil(t, service)
	require.ErrorContains(t, err, "raft node is required")
}

func TestRaftServiceRequestVote(t *testing.T) {
	t.Parallel()

	node := &fakeRaftNode{
		requestVoteReply: raft.RequestVoteReply{
			Term:        7,
			VoterID:     "node-2",
			VoteGranted: true,
		},
	}

	service, err := NewRaftService(node)
	require.NoError(t, err)

	response, err := service.RequestVote(context.Background(), &raftiqv1.RequestVoteRequest{
		Term:         5,
		CandidateId:  "node-1",
		LastLogIndex: 42,
		LastLogTerm:  4,
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), response.GetTerm())
	require.Equal(t, "node-2", response.GetVoterId())
	require.True(t, response.GetVoteGranted())

	require.Equal(t, raft.RequestVoteArgs{
		Term:         5,
		CandidateID:  "node-1",
		LastLogIndex: 42,
		LastLogTerm:  4,
	}, node.requestVoteArgs)
}

func TestRaftServiceRequestVoteRejectsNilRequest(t *testing.T) {
	t.Parallel()

	service, err := NewRaftService(&fakeRaftNode{})
	require.NoError(t, err)

	response, err := service.RequestVote(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "request vote request is required")
}

func TestRaftServiceAppendEntries(t *testing.T) {
	t.Parallel()

	node := &fakeRaftNode{
		appendEntriesReply: raft.AppendEntriesReply{
			Term:       9,
			FollowerID: "node-2",
			Success:    true,
		},
	}

	service, err := NewRaftService(node)
	require.NoError(t, err)

	response, err := service.AppendEntries(context.Background(), &raftiqv1.AppendEntriesRequest{
		Term:         8,
		LeaderId:     "node-1",
		PrevLogIndex: 10,
		PrevLogTerm:  7,
		Entries: []*raftiqv1.LogEntry{
			{
				Index: 11,
				Term:  8,
				Data:  []byte("first"),
			},
			{
				Index: 12,
				Term:  8,
				Data:  []byte("second"),
			},
		},
		LeaderCommit: 10,
	})

	require.NoError(t, err)
	require.Equal(t, uint64(9), response.GetTerm())
	require.Equal(t, "node-2", response.GetFollowerId())
	require.True(t, response.GetSuccess())

	require.Equal(t, raft.AppendEntriesArgs{
		Term:         8,
		LeaderID:     "node-1",
		PrevLogIndex: 10,
		PrevLogTerm:  7,
		Entries: []raft.LogEntry{
			{
				Index: 11,
				Term:  8,
				Data:  []byte("first"),
			},
			{
				Index: 12,
				Term:  8,
				Data:  []byte("second"),
			},
		},
		LeaderCommit: 10,
	}, node.appendEntriesArgs)
}

func TestRaftServiceAppendEntriesRejectsNilEntry(t *testing.T) {
	t.Parallel()

	service, err := NewRaftService(&fakeRaftNode{})
	require.NoError(t, err)

	response, err := service.AppendEntries(context.Background(), &raftiqv1.AppendEntriesRequest{
		Entries: []*raftiqv1.LogEntry{
			nil,
		},
	})

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "nil log entry")
}

func TestRaftServiceAppendEntriesRejectsNilRequest(t *testing.T) {
	t.Parallel()

	service, err := NewRaftService(&fakeRaftNode{})
	require.NoError(t, err)

	response, err := service.AppendEntries(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "append entries request is required")
}

func TestRaftServiceInstallSnapshot(t *testing.T) {
	t.Parallel()

	node := &fakeRaftNode{
		installSnapshotReply: raft.InstallSnapshotReply{
			Term:       12,
			FollowerID: "node-3",
			Success:    true,
		},
	}

	service, err := NewRaftService(node)
	require.NoError(t, err)

	response, err := service.InstallSnapshot(context.Background(), &raftiqv1.InstallSnapshotRequest{
		Term:              11,
		LeaderId:          "node-1",
		LastIncludedIndex: 100,
		LastIncludedTerm:  10,
		Data:              []byte("snapshot-data"),
	})

	require.NoError(t, err)
	require.Equal(t, uint64(12), response.GetTerm())
	require.Equal(t, "node-3", response.GetFollowerId())
	require.True(t, response.GetSuccess())

	require.Equal(t, raft.InstallSnapshotArgs{
		Term:              11,
		LeaderID:          "node-1",
		LastIncludedIndex: 100,
		LastIncludedTerm:  10,
		Data:              []byte("snapshot-data"),
	}, node.installSnapshotArgs)
}

func TestRaftServiceInstallSnapshotRejectsNilRequest(t *testing.T) {
	t.Parallel()

	service, err := NewRaftService(&fakeRaftNode{})
	require.NoError(t, err)

	response, err := service.InstallSnapshot(context.Background(), nil)

	require.Error(t, err)
	require.Nil(t, response)
	require.ErrorContains(t, err, "install snapshot request is required")
}

func TestRaftServicePreservesRequestDataIndependently(t *testing.T) {
	t.Parallel()

	node := &fakeRaftNode{}
	service, err := NewRaftService(node)
	require.NoError(t, err)

	data := []byte("original")

	_, err = service.InstallSnapshot(context.Background(), &raftiqv1.InstallSnapshotRequest{
		Data: data,
	})
	require.NoError(t, err)

	data[0] = 'X'

	require.Equal(t, []byte("original"), node.installSnapshotArgs.Data)
}

var _ raftRPC = (*fakeRaftNode)(nil)

var _ = errors.New

type fakeRPCMetrics struct {
	requests  map[string]int
	errors    map[string]int
	durations map[string]int
}

func newFakeRPCMetrics() *fakeRPCMetrics {
	return &fakeRPCMetrics{
		requests:  make(map[string]int),
		errors:    make(map[string]int),
		durations: make(map[string]int),
	}
}

func (m *fakeRPCMetrics) IncRPCRequest(method string) {
	m.requests[method]++
}

func (m *fakeRPCMetrics) IncRPCError(method string) {
	m.errors[method]++
}

func (m *fakeRPCMetrics) ObserveRPCDuration(method string, _ time.Duration) {
	m.durations[method]++
}

var _ RPCMetrics = (*fakeRPCMetrics)(nil)

func TestRaftServiceMetricsRequestVoteSuccess(t *testing.T) {
	fakeNode := &fakeRaftNode{
		requestVoteReply: raft.RequestVoteReply{
			Term:        5,
			VoterID:     "node-2",
			VoteGranted: true,
		},
	}

	service, err := NewRaftService(fakeNode)
	require.NoError(t, err)

	metrics := newFakeRPCMetrics()
	service.SetMetrics(metrics)

	_, err = service.RequestVote(context.Background(), &raftiqv1.RequestVoteRequest{
		Term:         5,
		CandidateId:  "node-1",
		LastLogIndex: 10,
		LastLogTerm:  5,
	})

	require.NoError(t, err)
	require.Equal(t, 1, metrics.requests[rpcMethodRequestVote])
	require.Equal(t, 0, metrics.errors[rpcMethodRequestVote])
	require.Equal(t, 1, metrics.durations[rpcMethodRequestVote])
}

func TestRaftServiceMetricsRequestVoteError(t *testing.T) {
	fakeNode := &fakeRaftNode{}

	service, err := NewRaftService(fakeNode)
	require.NoError(t, err)

	metrics := newFakeRPCMetrics()
	service.SetMetrics(metrics)

	_, err = service.RequestVote(context.Background(), nil)

	require.Error(t, err)
	require.Equal(t, 1, metrics.requests[rpcMethodRequestVote])
	require.Equal(t, 1, metrics.errors[rpcMethodRequestVote])
	require.Equal(t, 1, metrics.durations[rpcMethodRequestVote])
}

func TestRaftServiceMetricsAppendEntriesSuccess(t *testing.T) {
	fakeNode := &fakeRaftNode{
		appendEntriesReply: raft.AppendEntriesReply{
			Term:       5,
			FollowerID: "node-2",
			Success:    true,
		},
	}

	service, err := NewRaftService(fakeNode)
	require.NoError(t, err)

	metrics := newFakeRPCMetrics()
	service.SetMetrics(metrics)

	_, err = service.AppendEntries(
		context.Background(),
		&raftiqv1.AppendEntriesRequest{
			Term:         5,
			LeaderId:     "node-1",
			PrevLogIndex: 9,
			PrevLogTerm:  5,
			LeaderCommit: 9,
		},
	)

	require.NoError(t, err)
	require.Equal(t, 1, metrics.requests[rpcMethodAppendEntries])
	require.Equal(t, 0, metrics.errors[rpcMethodAppendEntries])
	require.Equal(t, 1, metrics.durations[rpcMethodAppendEntries])
}

func TestRaftServiceMetricsAppendEntriesError(t *testing.T) {
	fakeNode := &fakeRaftNode{}

	service, err := NewRaftService(fakeNode)
	require.NoError(t, err)

	metrics := newFakeRPCMetrics()
	service.SetMetrics(metrics)

	_, err = service.AppendEntries(context.Background(), nil)

	require.Error(t, err)
	require.Equal(t, 1, metrics.requests[rpcMethodAppendEntries])
	require.Equal(t, 1, metrics.errors[rpcMethodAppendEntries])
	require.Equal(t, 1, metrics.durations[rpcMethodAppendEntries])
}

func TestRaftServiceMetricsInstallSnapshotSuccess(t *testing.T) {
	fakeNode := &fakeRaftNode{
		installSnapshotReply: raft.InstallSnapshotReply{
			Term:       5,
			FollowerID: "node-2",
			Success:    true,
		},
	}

	service, err := NewRaftService(fakeNode)
	require.NoError(t, err)

	metrics := newFakeRPCMetrics()
	service.SetMetrics(metrics)

	_, err = service.InstallSnapshot(
		context.Background(),
		&raftiqv1.InstallSnapshotRequest{
			Term:              5,
			LeaderId:          "node-1",
			LastIncludedIndex: 100,
			LastIncludedTerm:  5,
			Data:              []byte("snapshot"),
		},
	)

	require.NoError(t, err)
	require.Equal(t, 1, metrics.requests[rpcMethodInstallSnapshot])
	require.Equal(t, 0, metrics.errors[rpcMethodInstallSnapshot])
	require.Equal(t, 1, metrics.durations[rpcMethodInstallSnapshot])
}

func TestRaftServiceMetricsInstallSnapshotError(t *testing.T) {
	fakeNode := &fakeRaftNode{}

	service, err := NewRaftService(fakeNode)
	require.NoError(t, err)

	metrics := newFakeRPCMetrics()
	service.SetMetrics(metrics)

	_, err = service.InstallSnapshot(context.Background(), nil)

	require.Error(t, err)
	require.Equal(t, 1, metrics.requests[rpcMethodInstallSnapshot])
	require.Equal(t, 1, metrics.errors[rpcMethodInstallSnapshot])
	require.Equal(t, 1, metrics.durations[rpcMethodInstallSnapshot])
}
