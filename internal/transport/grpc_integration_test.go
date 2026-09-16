package transport

import (
	"context"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const (
	testBufSize    = 1024 * 1024
	testRPCTimeout = 2 * time.Second
)

func TestRaftServiceGRPCIntegration(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(testBufSize)
	grpcServer := grpc.NewServer()

	node := &fakeRaftNode{
		requestVoteReply: raft.RequestVoteReply{
			Term:        7,
			VoterID:     "node-2",
			VoteGranted: true,
		},
	}

	service, err := NewRaftService(node)
	require.NoError(t, err)

	raftiqv1.RegisterRaftServiceServer(grpcServer, service)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())

		select {
		case err := <-serveErr:
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		default:
		}
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	client := raftiqv1.NewRaftServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		testRPCTimeout,
	)
	defer cancel()

	response, err := client.RequestVote(
		ctx,
		&raftiqv1.RequestVoteRequest{
			Term:         5,
			CandidateId:  "node-1",
			LastLogIndex: 42,
			LastLogTerm:  4,
		},
	)

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

func TestKVServiceGRPCIntegration(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(testBufSize)
	grpcServer := grpc.NewServer()

	store := &fakeJobStore{
		fakeKVStore: fakeKVStore{
			getValue: []byte("world"),
			getFound: true,
		},
		job: &model.Job{
			ID:          model.JobID("job-123"),
			Payload:     []byte("hello"),
			State:       model.JobPending,
			ScheduledAt: 123456789,
		},
		index: 42,
	}

	service, err := NewKVService(store)
	require.NoError(t, err)

	raftiqv1.RegisterKVServiceServer(grpcServer, service)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())

		select {
		case err := <-serveErr:
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		default:
		}
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	client := raftiqv1.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		testRPCTimeout,
	)
	defer cancel()

	getResponse, err := client.Get(
		ctx,
		&raftiqv1.GetRequest{
			Key: "hello",
		},
	)

	require.NoError(t, err)
	require.True(t, getResponse.GetFound())
	require.Equal(t, []byte("world"), getResponse.GetValue())
	require.Equal(t, "hello", store.getKey)

	jobResponse, err := client.CreateJob(
		ctx,
		&raftiqv1.CreateJobRequest{
			JobId:       "job-123",
			Payload:     []byte("hello"),
			ScheduledAt: 123456789,
		},
	)

	require.NoError(t, err)
	require.Equal(t, uint64(42), jobResponse.GetIndex())

	require.Equal(t, "job-123", store.createJobID)
	require.Equal(t, []byte("hello"), store.createPayload)
	require.Equal(t, int64(123456789), store.createScheduledAt)
}
