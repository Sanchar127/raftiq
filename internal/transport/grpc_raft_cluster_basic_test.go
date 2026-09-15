package transport

import (
	"context"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

func TestGRPCTransportRealRaftNodeRequestVote(t *testing.T) {
	node1 := raft.NewRaftNode("node-1")
	node2 := raft.NewRaftNode("node-2")

	certFiles := writeTestCertificateFiles(
		t,
		t.TempDir(),
		peerServerName("node-2"),
		peerServerName("node-1"),
	)

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   certFiles.caFile,
			CertFile: certFiles.serverCertFile,
			KeyFile:  certFiles.serverKeyFile,
		},
		map[string]struct{}{
			peerServerName("node-1"): {},
		},
	)
	if err != nil {
		t.Fatalf("load server TLS config: %v", err)
	}

	server2 := startTestRaftServer(t, node2, serverTLS)
	defer server2.close()

	clientTLS, err := LoadTLSConfig(
		TLSConfig{
			CAFile:         certFiles.caFile,
			CertFile:       certFiles.clientCertFile,
			KeyFile:        certFiles.clientKeyFile,
			PeerServerName: peerServerName("node-2"),
		},
	)
	if err != nil {
		t.Fatalf("load client TLS config: %v", err)
	}

	transport := NewGRPCTransport()

	if err := transport.SetTLSConfig(clientTLS); err != nil {
		t.Fatalf("set TLS config: %v", err)
	}

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
		t.Fatalf("set transport for node1: %v", err)
	}

	// node2 also needs node1 in its configured topology so that
	// BootstrapMembership() derives the same two-node membership.
	if err := node2.SetTransport(
		raft.NewLocalTransport(),
		[]raft.NodeID{"node-1"},
	); err != nil {
		t.Fatalf("set transport for node2: %v", err)
	}

	if err := node1.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership for node1: %v", err)
	}

	if err := node2.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership for node2: %v", err)
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
