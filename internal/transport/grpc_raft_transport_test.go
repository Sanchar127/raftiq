package transport

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

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

// startGRPCTestServer starts a gRPC server backed by node on a random
// localhost port. When tlsConfig is provided, the server requires mTLS.
func startGRPCTestServer(
	t *testing.T,
	node *grpcTestNode,
	tlsConfigs ...*tls.Config,
) (string, func()) {
	t.Helper()

	if len(tlsConfigs) > 1 {
		t.Fatal("startGRPCTestServer accepts at most one TLS config")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var server *grpc.Server

	if len(tlsConfigs) == 1 && tlsConfigs[0] != nil {
		server = grpc.NewServer(
			grpc.Creds(credentials.NewTLS(tlsConfigs[0])),
		)
	} else {
		server = grpc.NewServer()
	}

	raftiqv1.RegisterRaftServiceServer(server, node)

	serveErr := make(chan error, 1)

	go func() {
		err := server.Serve(listener)
		if err != nil && err != grpc.ErrServerStopped {
			serveErr <- err
			return
		}

		serveErr <- nil
	}()

	cleanup := func() {
		server.GracefulStop()
		_ = listener.Close()

		select {
		case err := <-serveErr:
			if err != nil {
				t.Errorf("gRPC test server failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("timed out waiting for gRPC test server to stop")
		}
	}

	return listener.Addr().String(), cleanup
}

// newGRPCTestTLSConfigs creates a test CA and two node certificates.
// The server certificate belongs to serverID and the client certificate
// belongs to clientID. The server only accepts the expected client SAN.
func newGRPCTestTLSConfigs(
	t *testing.T,
	serverID raft.NodeID,
	clientID raft.NodeID,
) (serverTLS *tls.Config, clientTLS *tls.Config) {
	t.Helper()

	dir := t.TempDir()

	ca := newTestCertificateAuthority(t, dir)

	serverFiles := writeTestNodeCertificate(
		t,
		ca,
		dir,
		serverID,
	)

	clientFiles := writeTestNodeCertificate(
		t,
		ca,
		dir,
		clientID,
	)

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   serverFiles.caFile,
			CertFile: serverFiles.serverCertFile,
			KeyFile:  serverFiles.serverKeyFile,
		},
		map[string]struct{}{
			peerServerName(clientID): {},
		},
	)
	if err != nil {
		t.Fatalf("load server TLS config: %v", err)
	}

	clientTLS, err = LoadTLSClientConfig(
		TLSConfig{
			CAFile:   clientFiles.caFile,
			CertFile: clientFiles.clientCertFile,
			KeyFile:  clientFiles.clientKeyFile,
		},
	)
	if err != nil {
		t.Fatalf("load client TLS config: %v", err)
	}

	return serverTLS, clientTLS
}

func TestGRPCTransportRequestVote(t *testing.T) {
	const (
		serverID = raft.NodeID("node-2")
		clientID = raft.NodeID("node-1")
	)

	serverNode := &grpcTestNode{
		requestVoteReply: raft.RequestVoteReply{
			Term:        7,
			VoterID:     serverID,
			VoteGranted: true,
		},
	}

	serverTLS, clientTLS := newGRPCTestTLSConfigs(
		t,
		serverID,
		clientID,
	)

	address, stopServer := startGRPCTestServer(
		t,
		serverNode,
		serverTLS,
	)
	defer stopServer()

	transport := NewGRPCTransport()

	if err := transport.SetTLSConfig(clientTLS); err != nil {
		t.Fatalf("set TLS config: %v", err)
	}

	if err := transport.AddPeer(serverID, address); err != nil {
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
		serverID,
		raft.RequestVoteArgs{
			Term:         5,
			CandidateID:  clientID,
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

	if reply.VoterID != serverID {
		t.Fatalf(
			"expected voter %s, got %s",
			serverID,
			reply.VoterID,
		)
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

	if serverNode.requestVoteArgs.CandidateID != clientID {
		t.Fatalf(
			"expected candidate %s, got %s",
			clientID,
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

func TestGRPCTransportAppendEntries(t *testing.T) {
	const (
		serverID = raft.NodeID("node-2")
		clientID = raft.NodeID("node-1")
	)

	serverNode := &grpcTestNode{
		appendEntriesReply: raft.AppendEntriesReply{
			Term:       8,
			FollowerID: serverID,
			Success:    true,
		},
	}

	serverTLS, clientTLS := newGRPCTestTLSConfigs(
		t,
		serverID,
		clientID,
	)

	address, stopServer := startGRPCTestServer(
		t,
		serverNode,
		serverTLS,
	)
	defer stopServer()

	transport := NewGRPCTransport()

	if err := transport.SetTLSConfig(clientTLS); err != nil {
		t.Fatalf("set TLS config: %v", err)
	}

	if err := transport.AddPeer(serverID, address); err != nil {
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

	entries := []raft.LogEntry{
		{
			Index: 4,
			Term:  3,
			Data:  []byte("set foo=bar"),
		},
		{
			Index: 5,
			Term:  3,
			Data:  []byte("set count=10"),
		},
	}

	reply, err := transport.AppendEntries(
		ctx,
		serverID,
		raft.AppendEntriesArgs{
			Term:         6,
			LeaderID:     clientID,
			PrevLogIndex: 3,
			PrevLogTerm:  2,
			Entries:      entries,
			LeaderCommit: 4,
		},
	)
	if err != nil {
		t.Fatalf("append entries: %v", err)
	}

	if reply.Term != 8 {
		t.Fatalf("expected term 8, got %d", reply.Term)
	}

	if reply.FollowerID != serverID {
		t.Fatalf(
			"expected follower %s, got %s",
			serverID,
			reply.FollowerID,
		)
	}

	if !reply.Success {
		t.Fatal("expected append entries to succeed")
	}

	got := serverNode.appendEntriesArgs

	if got.Term != 6 {
		t.Fatalf("expected term 6, got %d", got.Term)
	}

	if got.LeaderID != clientID {
		t.Fatalf(
			"expected leader %s, got %s",
			clientID,
			got.LeaderID,
		)
	}

	if got.PrevLogIndex != 3 {
		t.Fatalf(
			"expected previous log index 3, got %d",
			got.PrevLogIndex,
		)
	}

	if got.PrevLogTerm != 2 {
		t.Fatalf(
			"expected previous log term 2, got %d",
			got.PrevLogTerm,
		)
	}

	if got.LeaderCommit != 4 {
		t.Fatalf(
			"expected leader commit 4, got %d",
			got.LeaderCommit,
		)
	}

	if len(got.Entries) != 2 {
		t.Fatalf(
			"expected 2 entries, got %d",
			len(got.Entries),
		)
	}

	if got.Entries[0].Index != 4 ||
		got.Entries[0].Term != 3 ||
		string(got.Entries[0].Data) != "set foo=bar" {
		t.Fatalf("unexpected first entry: %+v", got.Entries[0])
	}

	if got.Entries[1].Index != 5 ||
		got.Entries[1].Term != 3 ||
		string(got.Entries[1].Data) != "set count=10" {
		t.Fatalf("unexpected second entry: %+v", got.Entries[1])
	}
}

func TestGRPCTransportInstallSnapshot(t *testing.T) {
	const (
		serverID = raft.NodeID("node-2")
		clientID = raft.NodeID("node-1")
	)

	serverNode := &grpcTestNode{
		installSnapshotReply: raft.InstallSnapshotReply{
			Term:       11,
			FollowerID: serverID,
			Success:    true,
		},
	}

	serverTLS, clientTLS := newGRPCTestTLSConfigs(
		t,
		serverID,
		clientID,
	)

	address, stopServer := startGRPCTestServer(
		t,
		serverNode,
		serverTLS,
	)
	defer stopServer()

	transport := NewGRPCTransport()

	if err := transport.SetTLSConfig(clientTLS); err != nil {
		t.Fatalf("set TLS config: %v", err)
	}

	if err := transport.AddPeer(serverID, address); err != nil {
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

	snapshot := []byte("snapshot-state-v42")

	reply, err := transport.InstallSnapshot(
		ctx,
		serverID,
		raft.InstallSnapshotArgs{
			Term:              10,
			LeaderID:          clientID,
			LastIncludedIndex: 42,
			LastIncludedTerm:  9,
			Data:              snapshot,
		},
	)
	if err != nil {
		t.Fatalf("install snapshot: %v", err)
	}

	if reply.Term != 11 {
		t.Fatalf("expected term 11, got %d", reply.Term)
	}

	if reply.FollowerID != serverID {
		t.Fatalf(
			"expected follower %s, got %s",
			serverID,
			reply.FollowerID,
		)
	}

	if !reply.Success {
		t.Fatal("expected snapshot installation to succeed")
	}

	got := serverNode.installSnapshotArgs

	if got.Term != 10 {
		t.Fatalf("expected term 10, got %d", got.Term)
	}

	if got.LeaderID != clientID {
		t.Fatalf(
			"expected leader %s, got %s",
			clientID,
			got.LeaderID,
		)
	}

	if got.LastIncludedIndex != 42 {
		t.Fatalf(
			"expected last included index 42, got %d",
			got.LastIncludedIndex,
		)
	}

	if got.LastIncludedTerm != 9 {
		t.Fatalf(
			"expected last included term 9, got %d",
			got.LastIncludedTerm,
		)
	}

	if string(got.Data) != string(snapshot) {
		t.Fatalf(
			"expected snapshot %q, got %q",
			snapshot,
			got.Data,
		)
	}
}
