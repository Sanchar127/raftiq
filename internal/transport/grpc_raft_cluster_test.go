package transport

import (
	"context"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc"
)

type testRaftServer struct {
	server   *grpc.Server
	listener net.Listener
}

func startTestRaftServer(
	t *testing.T,
	node *raft.RaftNode,
) *testRaftServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service, err := NewRaftService(node)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("create raft service: %v", err)
	}

	server := grpc.NewServer()
	raftiqv1.RegisterRaftServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("gRPC server failed: %v", err)
		}
	}()

	return &testRaftServer{
		server:   server,
		listener: listener,
	}
}

func (s *testRaftServer) close() {
	s.server.GracefulStop()
	_ = s.listener.Close()
}

func TestGRPCTransportRealRaftNodeRequestVote(t *testing.T) {
	node1 := raft.NewRaftNode("node-1")
	node2 := raft.NewRaftNode("node-2")

	server2 := startTestRaftServer(t, node2)
	defer server2.close()

	transport := NewGRPCTransport()
	defer func() {
		if err := transport.Close(); err != nil {
			t.Fatalf("close transport: %v", err)
		}
	}()

	if err := transport.AddPeer(
		"node-2",
		server2.listener.Addr().String(),
	); err != nil {
		t.Fatalf("add peer: %v", err)
	}

	if err := node1.SetTransport(
		transport,
		[]raft.NodeID{"node-2"},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	reply, err := transport.RequestVote(
		ctx,
		"node-2",
		raft.RequestVoteArgs{
			Term:         1,
			CandidateID:  "node-1",
			LastLogIndex: 0,
			LastLogTerm:  0,
		},
	)
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}

	if reply.Term != 1 {
		t.Fatalf("expected term 1, got %d", reply.Term)
	}

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if reply.VoterID != "node-2" {
		t.Fatalf(
			"expected voter node-2, got %s",
			reply.VoterID,
		)
	}
}
