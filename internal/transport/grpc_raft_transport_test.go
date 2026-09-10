package transport

import (
	"context"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const grpcTransportTestBufSize = 1024 * 1024

type grpcTestNode struct {
	raftiqv1.UnimplementedRaftServiceServer

	requestVoteReply     raft.RequestVoteReply
	appendEntriesReply   raft.AppendEntriesReply
	installSnapshotReply raft.InstallSnapshotReply
	requestVoteArgs      raft.RequestVoteArgs
	appendEntriesArgs    raft.AppendEntriesArgs
	installSnapshotArgs  raft.InstallSnapshotArgs
}

func (n *grpcTestNode) RequestVote(
	_ context.Context,
	req *raftiqv1.RequestVoteRequest,
) (*raftiqv1.RequestVoteResponse, error) {
	n.requestVoteArgs = raft.RequestVoteArgs{
		Term:         raft.Term(req.GetTerm()),
		CandidateID:  raft.NodeID(req.GetCandidateId()),
		LastLogIndex: raft.LogIndex(req.GetLastLogIndex()),
		LastLogTerm:  raft.Term(req.GetLastLogTerm()),
	}

	return &raftiqv1.RequestVoteResponse{
		Term:        uint64(n.requestVoteReply.Term),
		VoterId:     string(n.requestVoteReply.VoterID),
		VoteGranted: n.requestVoteReply.VoteGranted,
	}, nil
}

func (n *grpcTestNode) AppendEntries(
	_ context.Context,
	req *raftiqv1.AppendEntriesRequest,
) (*raftiqv1.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, len(req.GetEntries()))

	for i, entry := range req.GetEntries() {
		entries[i] = raft.LogEntry{
			Index: raft.LogIndex(entry.GetIndex()),
			Term:  raft.Term(entry.GetTerm()),
			Data:  append([]byte(nil), entry.GetData()...),
		}
	}

	n.appendEntriesArgs = raft.AppendEntriesArgs{
		Term:         raft.Term(req.GetTerm()),
		LeaderID:     raft.NodeID(req.GetLeaderId()),
		PrevLogIndex: raft.LogIndex(req.GetPrevLogIndex()),
		PrevLogTerm:  raft.Term(req.GetPrevLogTerm()),
		Entries:      entries,
		LeaderCommit: raft.LogIndex(req.GetLeaderCommit()),
	}

	return &raftiqv1.AppendEntriesResponse{
		Term:       uint64(n.appendEntriesReply.Term),
		FollowerId: string(n.appendEntriesReply.FollowerID),
		Success:    n.appendEntriesReply.Success,
	}, nil
}

func (n *grpcTestNode) InstallSnapshot(
	_ context.Context,
	req *raftiqv1.InstallSnapshotRequest,
) (*raftiqv1.InstallSnapshotResponse, error) {
	n.installSnapshotArgs = raft.InstallSnapshotArgs{
		Term:              raft.Term(req.GetTerm()),
		LeaderID:          raft.NodeID(req.GetLeaderId()),
		LastIncludedIndex: raft.LogIndex(req.GetLastIncludedIndex()),
		LastIncludedTerm:  raft.Term(req.GetLastIncludedTerm()),
		Data:              append([]byte(nil), req.GetData()...),
	}

	return &raftiqv1.InstallSnapshotResponse{
		Term:       uint64(n.installSnapshotReply.Term),
		FollowerId: string(n.installSnapshotReply.FollowerID),
		Success:    n.installSnapshotReply.Success,
	}, nil
}

func startGRPCTestServer(
	t *testing.T,
	node *grpcTestNode,
) func() {
	t.Helper()

	listener := bufconn.Listen(grpcTransportTestBufSize)

	server := grpc.NewServer()
	raftiqv1.RegisterRaftServiceServer(server, node)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("gRPC test server failed: %v", err)
		}
	}()

	return func() {
		server.GracefulStop()
		_ = listener.Close()
	}
}

func dialBufconn(
	ctx context.Context,
	address string,
) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		address,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return bufconn.Listen(grpcTransportTestBufSize).Dial()
		}),
	)
}

func TestGRPCTransportRequestVote(t *testing.T) {
	serverNode := &grpcTestNode{
		requestVoteReply: raft.RequestVoteReply{
			Term:        7,
			VoterID:     "node-2",
			VoteGranted: true,
		},
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer()
	raftiqv1.RegisterRaftServiceServer(server, serverNode)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("gRPC test server failed: %v", err)
		}
	}()

	defer func() {
		server.GracefulStop()
		_ = listener.Close()
	}()

	transport := NewGRPCTransport()

	err = transport.AddPeer(
		"node-2",
		listener.Addr().String(),
	)
	if err != nil {
		t.Fatalf("add peer: %v", err)
	}

	defer func() {
		if err := transport.Close(); err != nil {
			t.Fatalf("close transport: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	reply, err := transport.RequestVote(
		ctx,
		"node-2",
		raft.RequestVoteArgs{
			Term:         5,
			CandidateID:  "node-1",
			LastLogIndex: 12,
			LastLogTerm:  4,
		},
	)
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}

	if reply.Term != 7 {
		t.Fatalf("expected term 7, got %d", reply.Term)
	}

	if reply.VoterID != "node-2" {
		t.Fatalf("expected voter node-2, got %s", reply.VoterID)
	}

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if serverNode.requestVoteArgs.Term != 5 {
		t.Fatalf(
			"expected request term 5, got %d",
			serverNode.requestVoteArgs.Term,
		)
	}

	if serverNode.requestVoteArgs.CandidateID != "node-1" {
		t.Fatalf(
			"expected candidate node-1, got %s",
			serverNode.requestVoteArgs.CandidateID,
		)
	}

	if serverNode.requestVoteArgs.LastLogIndex != 12 {
		t.Fatalf(
			"expected last log index 12, got %d",
			serverNode.requestVoteArgs.LastLogIndex,
		)
	}

	if serverNode.requestVoteArgs.LastLogTerm != 4 {
		t.Fatalf(
			"expected last log term 4, got %d",
			serverNode.requestVoteArgs.LastLogTerm,
		)
	}
}
