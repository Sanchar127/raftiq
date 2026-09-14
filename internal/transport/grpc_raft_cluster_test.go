package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type testRaftServer struct {
	server   *grpc.Server
	listener net.Listener
}

func startTestRaftServer(
	t *testing.T,
	node *raft.RaftNode,
	tlsConfigs ...*tls.Config,
) *testRaftServer {
	t.Helper()
	var tlsConfig *tls.Config

	if len(tlsConfigs) > 1 {
		t.Fatal("startTestRaftServer accepts at most one TLS config")
	}

	if len(tlsConfigs) == 1 {
		tlsConfig = tlsConfigs[0]
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service, err := NewRaftService(node)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("create raft service: %v", err)
	}

	var server *grpc.Server

	if tlsConfig != nil {
		server = grpc.NewServer(
			grpc.Creds(credentials.NewTLS(tlsConfig)),
		)
	} else {
		server = grpc.NewServer()
	}
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

func TestGRPCTransportThreeNodeRaftElection(t *testing.T) {
	nodes := []*raft.RaftNode{
		raft.NewRaftNode("node-1"),
		raft.NewRaftNode("node-2"),
		raft.NewRaftNode("node-3"),
	}

	nodes[0].SetElectionTimeout(10)
	nodes[1].SetElectionTimeout(15)
	nodes[2].SetElectionTimeout(20)

	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	certDir := t.TempDir()

	ca := newTestCertificateAuthority(
		t,
		certDir,
	)

	certFiles := make(
		[]testCertificateFiles,
		len(peerIDs),
	)

	for i, peerID := range peerIDs {
		certFiles[i] = writeTestNodeCertificate(
			t,
			ca,
			certDir,
			peerID,
		)
	}

	servers := make(
		[]*testRaftServer,
		0,
		len(nodes),
	)

	for i, node := range nodes {
		allowedPeerSANs := make(
			map[string]struct{},
			len(peerIDs)-1,
		)

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   ca.file,
				CertFile: certFiles[i].serverCertFile,
				KeyFile:  certFiles[i].serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load server TLS config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		servers = append(
			servers,
			startTestRaftServer(
				t,
				node,
				serverTLS,
			),
		)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			server.close()
		}
	})

	transports := make(
		[]*GRPCTransport,
		0,
		len(nodes),
	)

	t.Cleanup(func() {
		for _, transport := range transports {
			if err := transport.Close(); err != nil {
				t.Errorf(
					"close transport: %v",
					err,
				)
			}
		}
	})

	for i, node := range nodes {
		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   ca.file,
				CertFile: certFiles[i].clientCertFile,
				KeyFile:  certFiles[i].clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load client TLS config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		transport := NewGRPCTransport()

		if err := transport.SetTLSConfig(clientTLS); err != nil {
			t.Fatalf(
				"set TLS config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		transports = append(
			transports,
			transport,
		)

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			if err := transport.AddPeer(
				peerID,
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s to %s: %v",
					peerID,
					peerIDs[i],
					err,
				)
			}
		}

		otherPeers := make(
			[]raft.NodeID,
			0,
			len(peerIDs)-1,
		)

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			otherPeers = append(
				otherPeers,
				peerID,
			)
		}

		if err := node.SetTransport(
			transport,
			otherPeers,
		); err != nil {
			t.Fatalf(
				"set transport for %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	deadline := time.Now().Add(5 * time.Second)

	var leader *raft.RaftNode

	for time.Now().Before(deadline) {
		leaders := 0
		leader = nil

		for _, node := range nodes {
			if node.State().Role == raft.Leader {
				leaders++
				leader = node
			}
		}

		if leaders == 1 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("expected exactly one leader to be elected")
	}

	leaders := 0

	for _, node := range nodes {
		if node.State().Role == raft.Leader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf(
			"expected exactly one leader, got %d",
			leaders,
		)
	}
}

func TestGRPCTransportFollowerFailure(t *testing.T) {
	nodes := []*raft.RaftNode{
		raft.NewRaftNode("node-1"),
		raft.NewRaftNode("node-2"),
		raft.NewRaftNode("node-3"),
	}

	nodes[0].SetElectionTimeout(10)
	nodes[1].SetElectionTimeout(15)
	nodes[2].SetElectionTimeout(20)

	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	certDir := t.TempDir()

	ca := newTestCertificateAuthority(t, certDir)

	certFiles := make([]testCertificateFiles, len(nodes))

	for i, peerID := range peerIDs {
		certFiles[i] = writeTestNodeCertificate(
			t,
			ca,
			certDir,
			peerID,
		)
	}

	servers := make([]*testRaftServer, len(nodes))

	for i, node := range nodes {
		allowedPeerSANs := make(map[string]struct{})

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].serverCertFile,
				KeyFile:  certFiles[i].serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load TLS server config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		servers[i] = startTestRaftServer(
			t,
			node,
			serverTLS,
		)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			server.close()
		}
	})

	transports := make([]*GRPCTransport, len(nodes))

	t.Cleanup(func() {
		for _, transport := range transports {
			if transport == nil {
				continue
			}

			if err := transport.Close(); err != nil {
				t.Errorf("close transport: %v", err)
			}
		}
	})

	for i, node := range nodes {
		transport := NewGRPCTransport()
		transports[i] = transport

		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].clientCertFile,
				KeyFile:  certFiles[i].clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load TLS client config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		if err := transport.SetTLSConfig(clientTLS); err != nil {
			t.Fatalf(
				"set TLS config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			if err := transport.AddPeer(
				peerID,
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s to %s: %v",
					peerID,
					peerIDs[i],
					err,
				)
			}
		}

		node.SetTransport(transport, peerIDs)
	}

	for _, node := range nodes {
		node.Start()

		t.Cleanup(func() {
			node.Stop()
		})
	}

	var leader *raft.RaftNode

	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		leaders := 0
		var candidate *raft.RaftNode

		for _, node := range nodes {
			if node.State().Role == raft.Leader {
				leaders++
				candidate = node
			}
		}

		if leaders == 1 {
			leader = candidate
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("expected exactly one leader to be elected")
	}

	leaderIndex := -1
	followerIndex := -1

	for i, node := range nodes {
		if node == leader {
			leaderIndex = i
			continue
		}

		if followerIndex == -1 {
			followerIndex = i
		}
	}

	if leaderIndex == -1 {
		t.Fatal("failed to identify leader index")
	}

	if followerIndex == -1 {
		t.Fatal("failed to identify follower index")
	}

	followerID := peerIDs[followerIndex]

	nodes[followerIndex].Stop()

	if err := transports[followerIndex].Close(); err != nil {
		t.Fatalf(
			"close failed follower transport %s: %v",
			followerID,
			err,
		)
	}

	deadline = time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		if leader.State().Role == raft.Leader {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if leader.State().Role != raft.Leader {
		t.Fatalf(
			"expected node %s to remain leader after follower %s failure",
			peerIDs[leaderIndex],
			followerID,
		)
	}
}

func TestGRPCTransportFollowerRecovery(t *testing.T) {
	nodes := []*raft.RaftNode{
		raft.NewRaftNode("node-1"),
		raft.NewRaftNode("node-2"),
		raft.NewRaftNode("node-3"),
	}

	nodes[0].SetElectionTimeout(10)
	nodes[1].SetElectionTimeout(15)
	nodes[2].SetElectionTimeout(20)

	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	certDir := t.TempDir()

	ca := newTestCertificateAuthority(t, certDir)

	certFiles := make([]testCertificateFiles, len(nodes))

	for i, peerID := range peerIDs {
		certFiles[i] = writeTestNodeCertificate(
			t,
			ca,
			certDir,
			peerID,
		)
	}

	servers := make([]*testRaftServer, len(nodes))

	for i, node := range nodes {
		allowedPeerSANs := make(map[string]struct{})

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].serverCertFile,
				KeyFile:  certFiles[i].serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load TLS server config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		servers[i] = startTestRaftServer(
			t,
			node,
			serverTLS,
		)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			if server != nil {
				server.close()
			}
		}
	})

	transports := make([]*GRPCTransport, len(nodes))

	t.Cleanup(func() {
		for _, transport := range transports {
			if transport == nil {
				continue
			}

			if err := transport.Close(); err != nil {
				t.Errorf("close transport: %v", err)
			}
		}
	})

	for i, node := range nodes {
		transport := NewGRPCTransport()
		transports[i] = transport

		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].clientCertFile,
				KeyFile:  certFiles[i].clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load TLS client config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		if err := transport.SetTLSConfig(clientTLS); err != nil {
			t.Fatalf(
				"set TLS config for %s: %v",
				peerIDs[i],
				err,
			)
		}

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			if err := transport.AddPeer(
				peerID,
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s to node %s: %v",
					peerID,
					peerIDs[i],
					err,
				)
			}
		}

		otherPeers := make(
			[]raft.NodeID,
			0,
			len(peerIDs)-1,
		)

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			otherPeers = append(otherPeers, peerID)
		}

		if err := node.SetTransport(
			transport,
			otherPeers,
		); err != nil {
			t.Fatalf(
				"set transport for %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	leaderIndex := -1
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		leaders := 0
		leaderIndex = -1

		for i, node := range nodes {
			if node.State().Role == raft.Leader {
				leaders++
				leaderIndex = i
			}
		}

		if leaders == 1 {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if leaderIndex == -1 {
		t.Fatal("expected a leader to be elected")
	}

	leader := nodes[leaderIndex]

	followerIndex := -1

	for i := range nodes {
		if i != leaderIndex {
			followerIndex = i
			break
		}
	}

	if followerIndex == -1 {
		t.Fatal("expected a follower")
	}

	follower := nodes[followerIndex]

	initialTerm := leader.State().Persistent.CurrentTerm

	servers[followerIndex].close()

	deadline = time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		state := leader.State()

		if state.Role != raft.Leader {
			t.Fatalf(
				"leader %s lost leadership while follower %s was down",
				peerIDs[leaderIndex],
				peerIDs[followerIndex],
			)
		}

		if state.Persistent.CurrentTerm < initialTerm {
			t.Fatalf(
				"leader term moved backwards: initial=%d current=%d",
				initialTerm,
				state.Persistent.CurrentTerm,
			)
		}

		time.Sleep(50 * time.Millisecond)
	}

	allowedPeerSANs := make(map[string]struct{})

	for j, peerID := range peerIDs {
		if j == followerIndex {
			continue
		}

		allowedPeerSANs[peerServerName(peerID)] = struct{}{}
	}

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   certFiles[followerIndex].caFile,
			CertFile: certFiles[followerIndex].serverCertFile,
			KeyFile:  certFiles[followerIndex].serverKeyFile,
		},
		allowedPeerSANs,
	)
	if err != nil {
		t.Fatalf(
			"load recovered TLS server config for %s: %v",
			peerIDs[followerIndex],
			err,
		)
	}

	servers[followerIndex] = startTestRaftServer(
		t,
		follower,
		serverTLS,
	)

	t.Cleanup(func() {
		servers[followerIndex].close()
	})

	recoveredAddress := servers[followerIndex].
		listener.
		Addr().
		String()

	for i, transport := range transports {
		if i == followerIndex {
			continue
		}

		transport.RemovePeer(peerIDs[followerIndex])

		if err := transport.AddPeer(
			peerIDs[followerIndex],
			recoveredAddress,
		); err != nil {
			t.Fatalf(
				"add recovered peer %s to node %s: %v",
				peerIDs[followerIndex],
				peerIDs[i],
				err,
			)
		}
	}

	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		state := follower.State()

		if state.Role == raft.Follower &&
			state.LeaderID == peerIDs[leaderIndex] &&
			state.Persistent.CurrentTerm >= initialTerm {
			t.Logf(
				"follower recovered: node=%s role=%v leader=%s term=%d",
				peerIDs[followerIndex],
				state.Role,
				state.LeaderID,
				state.Persistent.CurrentTerm,
			)

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	state := follower.State()

	t.Fatalf(
		"recovered follower %s did not converge: role=%v leader=%s term=%d expectedLeader=%s expectedTerm>=%d",
		peerIDs[followerIndex],
		state.Role,
		state.LeaderID,
		state.Persistent.CurrentTerm,
		peerIDs[leaderIndex],
		initialTerm,
	)
}

func TestGRPCTransportLeaderFailureReElection(t *testing.T) {
	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	testDir := t.TempDir()
	certDir := t.TempDir()

	nodes := make([]*raft.RaftNode, len(peerIDs))
	stores := make([]*storage.WALStorage, len(peerIDs))
	servers := make([]*testRaftServer, len(peerIDs))
	transports := make([]*GRPCTransport, len(peerIDs))

	electionTimeouts := []int{
		10,
		15,
		20,
	}

	ca := newTestCertificateAuthority(t, certDir)

	certFiles := make([]testCertificateFiles, len(peerIDs))

	for i, id := range peerIDs {
		certFiles[i] = writeTestNodeCertificate(
			t,
			ca,
			certDir,
			id,
		)
	}

	for i, id := range peerIDs {
		node, store := newPersistentTestNode(
			t,
			id,
			testDir,
		)

		nodes[i] = node
		stores[i] = store

		t.Cleanup(func() {
			node.Stop()
			_ = store.Close()
		})

		node.SetElectionTimeout(electionTimeouts[i])
	}

	for i, node := range nodes {
		allowedPeerSANs := make(map[string]struct{})

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].serverCertFile,
				KeyFile:  certFiles[i].serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load TLS server config for node %s: %v",
				node.ID(),
				err,
			)
		}

		server := startTestRaftServer(
			t,
			node,
			serverTLS,
		)

		servers[i] = server

		t.Cleanup(func() {
			server.close()
		})
	}

	for i := range nodes {
		transport := NewGRPCTransport()

		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].clientCertFile,
				KeyFile:  certFiles[i].clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load TLS client config for node %s: %v",
				peerIDs[i],
				err,
			)
		}

		if err := transport.SetTLSConfig(clientTLS); err != nil {
			t.Fatalf(
				"set TLS config for node %s: %v",
				peerIDs[i],
				err,
			)
		}

		transports[i] = transport

		t.Cleanup(func() {
			_ = transport.Close()
		})
	}

	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}

			if err := transports[i].AddPeer(
				peerIDs[j],
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s to node %s: %v",
					peerIDs[j],
					peerIDs[i],
					err,
				)
			}
		}

		otherPeers := make([]raft.NodeID, 0, len(peerIDs)-1)

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			otherPeers = append(otherPeers, peerID)
		}

		if err := nodes[i].SetTransport(
			transports[i],
			otherPeers,
		); err != nil {
			t.Fatalf(
				"set transport for node %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start node %s: %v",
				node.ID(),
				err,
			)
		}
	}

	initialLeaderIndex := -1
	var initialTerm raft.Term

	leaderDeadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(leaderDeadline) {
		leaderCount := 0
		candidateIndex := -1

		for i, node := range nodes {
			state := node.State()

			if state.Role == raft.Leader {
				leaderCount++
				candidateIndex = i
			}
		}

		if leaderCount == 1 {
			initialLeaderIndex = candidateIndex
			initialTerm = nodes[candidateIndex].State().Persistent.CurrentTerm
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if initialLeaderIndex == -1 {
		t.Fatal("cluster did not elect exactly one initial leader")
	}

	t.Logf(
		"initial leader: node=%s term=%d",
		peerIDs[initialLeaderIndex],
		initialTerm,
	)

	initialLeaderNode := nodes[initialLeaderIndex]

	initialLeaderNode.Stop()
	servers[initialLeaderIndex].close()

	if err := stores[initialLeaderIndex].Close(); err != nil {
		t.Fatalf(
			"close initial leader WAL: %v",
			err,
		)
	}

	newLeaderIndex := -1
	var newTerm raft.Term

	reElectionDeadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(reElectionDeadline) {
		leaderCount := 0
		candidateIndex := -1

		for i, node := range nodes {
			if i == initialLeaderIndex {
				continue
			}

			state := node.State()

			if state.Role == raft.Leader {
				leaderCount++
				candidateIndex = i
			}
		}

		if leaderCount == 1 {
			newLeaderIndex = candidateIndex
			newTerm = nodes[candidateIndex].State().Persistent.CurrentTerm
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if newLeaderIndex == -1 {
		t.Fatal("surviving nodes did not elect a new leader")
	}

	if newTerm <= initialTerm {
		t.Fatalf(
			"new leader term did not advance: initial=%d new=%d",
			initialTerm,
			newTerm,
		)
	}

	t.Logf(
		"new leader after failure: node=%s term=%d",
		peerIDs[newLeaderIndex],
		newTerm,
	)

	recoveredStore, err := storage.OpenWAL(
		filepath.Join(
			testDir,
			fmt.Sprintf("%s.wal", peerIDs[initialLeaderIndex]),
		),
	)
	if err != nil {
		t.Fatalf(
			"reopen WAL for recovered leader %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	recoveredNode, err := raft.NewRaftNodeWithStorage(
		peerIDs[initialLeaderIndex],
		recoveredStore,
	)
	if err != nil {
		_ = recoveredStore.Close()

		t.Fatalf(
			"recreate recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	t.Cleanup(func() {
		recoveredNode.Stop()
		_ = recoveredStore.Close()
	})

	recoveredNode.SetElectionTimeout(
		electionTimeouts[initialLeaderIndex],
	)

	recoveredTransport := NewGRPCTransport()

	recoveredClientTLS, err := LoadTLSClientConfig(
		TLSConfig{
			CAFile:   certFiles[initialLeaderIndex].caFile,
			CertFile: certFiles[initialLeaderIndex].clientCertFile,
			KeyFile:  certFiles[initialLeaderIndex].clientKeyFile,
		},
	)
	if err != nil {
		t.Fatalf(
			"load TLS client config for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	if err := recoveredTransport.SetTLSConfig(
		recoveredClientTLS,
	); err != nil {
		t.Fatalf(
			"set TLS config for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	t.Cleanup(func() {
		_ = recoveredTransport.Close()
	})

	survivorIDs := make([]raft.NodeID, 0, len(peerIDs)-1)

	for i, peerID := range peerIDs {
		if i == initialLeaderIndex {
			continue
		}

		survivorIDs = append(survivorIDs, peerID)

		if err := recoveredTransport.AddPeer(
			peerID,
			servers[i].listener.Addr().String(),
		); err != nil {
			t.Fatalf(
				"add survivor peer %s to recovered node %s: %v",
				peerID,
				peerIDs[initialLeaderIndex],
				err,
			)
		}
	}

	if err := recoveredNode.SetTransport(
		recoveredTransport,
		survivorIDs,
	); err != nil {
		t.Fatalf(
			"set transport for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	allowedRecoveredPeerSANs := make(map[string]struct{})

	for _, peerID := range survivorIDs {
		allowedRecoveredPeerSANs[peerServerName(peerID)] = struct{}{}
	}

	recoveredServerTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   certFiles[initialLeaderIndex].caFile,
			CertFile: certFiles[initialLeaderIndex].serverCertFile,
			KeyFile:  certFiles[initialLeaderIndex].serverKeyFile,
		},
		allowedRecoveredPeerSANs,
	)
	if err != nil {
		t.Fatalf(
			"load TLS server config for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	recoveredServer := startTestRaftServer(
		t,
		recoveredNode,
		recoveredServerTLS,
	)

	t.Cleanup(func() {
		recoveredServer.close()
	})

	recoveredAddress := recoveredServer.listener.Addr().String()

	for i, transport := range transports {
		if i == initialLeaderIndex {
			continue
		}

		transport.RemovePeer(
			peerIDs[initialLeaderIndex],
		)

		if err := transport.AddPeer(
			peerIDs[initialLeaderIndex],
			recoveredAddress,
		); err != nil {
			t.Fatalf(
				"add recovered peer %s to node %s: %v",
				peerIDs[initialLeaderIndex],
				peerIDs[i],
				err,
			)
		}
	}

	if err := recoveredNode.Start(); err != nil {
		t.Fatalf(
			"start recovered node %s: %v",
			recoveredNode.ID(),
			err,
		)
	}

	recoveryDeadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(recoveryDeadline) {
		state := recoveredNode.State()

		if state.Role == raft.Follower &&
			state.LeaderID == peerIDs[newLeaderIndex] &&
			state.Persistent.CurrentTerm >= newTerm {
			t.Logf(
				"leader recovered: node=%s role=%v leader=%s term=%d",
				recoveredNode.ID(),
				state.Role,
				state.LeaderID,
				state.Persistent.CurrentTerm,
			)

			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	state := recoveredNode.State()

	t.Fatalf(
		"recovered leader did not rejoin correctly: role=%v leader=%s term=%d want follower leader=%s term>=%d",
		state.Role,
		state.LeaderID,
		state.Persistent.CurrentTerm,
		peerIDs[newLeaderIndex],
		newTerm,
	)
}

func newPersistentTestNode(
	t *testing.T,
	id raft.NodeID,
	dir string,
) (*raft.RaftNode, *storage.WALStorage) {
	t.Helper()

	path := filepath.Join(
		dir,
		fmt.Sprintf("%s.wal", id),
	)

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf(
			"open WAL for node %s: %v",
			id,
			err,
		)
	}

	node, err := raft.NewRaftNodeWithStorage(
		id,
		store,
	)
	if err != nil {
		_ = store.Close()

		t.Fatalf(
			"create persistent node %s: %v",
			id,
			err,
		)
	}

	return node, store
}

func TestGRPCTransportLeaderRecoveryLogCatchUp(t *testing.T) {
	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	testDir := t.TempDir()
	certDir := t.TempDir()

	nodes := make([]*raft.RaftNode, len(peerIDs))
	stores := make([]*storage.WALStorage, len(peerIDs))
	servers := make([]*testRaftServer, len(peerIDs))
	transports := make([]*GRPCTransport, len(peerIDs))

	electionTimeouts := []int{
		10,
		15,
		20,
	}

	ca := newTestCertificateAuthority(t, certDir)

	certFiles := make([]testCertificateFiles, len(peerIDs))

	for i, id := range peerIDs {
		certFiles[i] = writeTestNodeCertificate(
			t,
			ca,
			certDir,
			id,
		)
	}

	for i, id := range peerIDs {
		node, store := newPersistentTestNode(
			t,
			id,
			testDir,
		)

		nodes[i] = node
		stores[i] = store

		nodes[i].SetElectionTimeout(electionTimeouts[i])

		t.Cleanup(func() {
			node.Stop()
			_ = store.Close()
		})
	}

	for i, node := range nodes {
		allowedPeerSANs := make(map[string]struct{})

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].serverCertFile,
				KeyFile:  certFiles[i].serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load TLS server config for node %s: %v",
				node.ID(),
				err,
			)
		}

		server := startTestRaftServer(
			t,
			node,
			serverTLS,
		)

		servers[i] = server

		t.Cleanup(func() {
			server.close()
		})
	}

	for i := range nodes {
		transport := NewGRPCTransport()

		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   certFiles[i].caFile,
				CertFile: certFiles[i].clientCertFile,
				KeyFile:  certFiles[i].clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load TLS client config for node %s: %v",
				peerIDs[i],
				err,
			)
		}

		if err := transport.SetTLSConfig(clientTLS); err != nil {
			t.Fatalf(
				"set TLS config for node %s: %v",
				peerIDs[i],
				err,
			)
		}

		transports[i] = transport

		t.Cleanup(func() {
			_ = transport.Close()
		})
	}

	for i := range nodes {
		peerList := make(
			[]raft.NodeID,
			0,
			len(peerIDs)-1,
		)

		for j, peerID := range peerIDs {
			if i == j {
				continue
			}

			peerList = append(peerList, peerID)

			if err := transports[i].AddPeer(
				peerID,
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s to node %s: %v",
					peerID,
					peerIDs[i],
					err,
				)
			}
		}

		if err := nodes[i].SetTransport(
			transports[i],
			peerList,
		); err != nil {
			t.Fatalf(
				"set transport for node %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start node %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	initialLeaderIndex := -1
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		leaderCount := 0
		candidate := -1

		for i, node := range nodes {
			if node.State().Role == raft.Leader {
				leaderCount++
				candidate = i
			}
		}

		if leaderCount == 1 {
			initialLeaderIndex = candidate
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if initialLeaderIndex == -1 {
		t.Fatal("expected exactly one initial leader")
	}

	initialTerm :=
		nodes[initialLeaderIndex].State().Persistent.CurrentTerm

	t.Logf(
		"initial leader: %s term=%d",
		peerIDs[initialLeaderIndex],
		initialTerm,
	)

	initialEntries := [][]byte{
		[]byte("entry-a"),
		[]byte("entry-b"),
	}

	initialIndexes := make(
		[]raft.LogIndex,
		0,
		len(initialEntries),
	)

	for _, data := range initialEntries {
		index, err := nodes[initialLeaderIndex].Propose(data)
		if err != nil {
			t.Fatalf(
				"propose initial entry: %v",
				err,
			)
		}

		initialIndexes = append(
			initialIndexes,
			index,
		)
	}

	expectedInitialLastIndex :=
		initialIndexes[len(initialIndexes)-1]

	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		allReplicated := true

		for i, node := range nodes {
			if i == initialLeaderIndex {
				continue
			}

			if node.Log().LastIndex() < expectedInitialLastIndex {
				allReplicated = false
				break
			}
		}

		if allReplicated {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	for i, node := range nodes {
		if node.Log().LastIndex() < expectedInitialLastIndex {
			t.Fatalf(
				"node %s did not receive initial entries: lastIndex=%d want>=%d",
				peerIDs[i],
				node.Log().LastIndex(),
				expectedInitialLastIndex,
			)
		}
	}

	nodes[initialLeaderIndex].Stop()
	servers[initialLeaderIndex].close()

	if err := stores[initialLeaderIndex].Close(); err != nil {
		t.Fatalf(
			"close WAL for failed leader %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	newLeaderIndex := -1
	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		leaderCount := 0
		candidate := -1

		for i, node := range nodes {
			if i == initialLeaderIndex {
				continue
			}

			if node.State().Role == raft.Leader {
				leaderCount++
				candidate = i
			}
		}

		if leaderCount == 1 {
			newLeaderIndex = candidate
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	if newLeaderIndex == -1 {
		t.Fatal("expected surviving nodes to elect a new leader")
	}

	newLeaderTerm :=
		nodes[newLeaderIndex].State().Persistent.CurrentTerm

	if newLeaderTerm <= initialTerm {
		t.Fatalf(
			"new leader term=%d want > initial term=%d",
			newLeaderTerm,
			initialTerm,
		)
	}

	t.Logf(
		"new leader: %s term=%d",
		peerIDs[newLeaderIndex],
		newLeaderTerm,
	)

	for i, transport := range transports {
		if i == initialLeaderIndex {
			continue
		}

		transport.RemovePeer(
			peerIDs[initialLeaderIndex],
		)
	}

	recoveryEntries := [][]byte{
		[]byte("entry-c"),
		[]byte("entry-d"),
		[]byte("entry-e"),
	}

	recoveryIndexes := make(
		[]raft.LogIndex,
		0,
		len(recoveryEntries),
	)

	for _, data := range recoveryEntries {
		index, err := nodes[newLeaderIndex].Propose(data)
		if err != nil {
			t.Fatalf(
				"propose recovery entry %q: %v",
				data,
				err,
			)
		}

		recoveryIndexes = append(
			recoveryIndexes,
			index,
		)
	}

	expectedLastIndex :=
		recoveryIndexes[len(recoveryIndexes)-1]

	t.Logf(
		"new leader committed entries through index %d",
		expectedLastIndex,
	)

	leaderLogLastIndex :=
		nodes[newLeaderIndex].Log().LastIndex()

	if leaderLogLastIndex != expectedLastIndex {
		t.Fatalf(
			"leader last index=%d want=%d",
			leaderLogLastIndex,
			expectedLastIndex,
		)
	}

	walPath := filepath.Join(
		testDir,
		fmt.Sprintf(
			"%s.wal",
			peerIDs[initialLeaderIndex],
		),
	)

	recoveredStore, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf(
			"reopen WAL for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	recoveredNode, err := raft.NewRaftNodeWithStorage(
		peerIDs[initialLeaderIndex],
		recoveredStore,
	)
	if err != nil {
		_ = recoveredStore.Close()

		t.Fatalf(
			"reconstruct recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	t.Cleanup(func() {
		recoveredNode.Stop()
		_ = recoveredStore.Close()
	})

	recoveredNode.SetElectionTimeout(
		electionTimeouts[initialLeaderIndex],
	)

	recoveredTransport := NewGRPCTransport()

	recoveredClientTLS, err := LoadTLSClientConfig(
		TLSConfig{
			CAFile:   certFiles[initialLeaderIndex].caFile,
			CertFile: certFiles[initialLeaderIndex].clientCertFile,
			KeyFile:  certFiles[initialLeaderIndex].clientKeyFile,
		},
	)
	if err != nil {
		t.Fatalf(
			"load TLS client config for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	if err := recoveredTransport.SetTLSConfig(
		recoveredClientTLS,
	); err != nil {
		t.Fatalf(
			"set TLS config for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	t.Cleanup(func() {
		_ = recoveredTransport.Close()
	})

	survivorIDs := make(
		[]raft.NodeID,
		0,
		len(peerIDs)-1,
	)

	for i, id := range peerIDs {
		if i == initialLeaderIndex {
			continue
		}

		survivorIDs = append(
			survivorIDs,
			id,
		)

		if err := recoveredTransport.AddPeer(
			id,
			servers[i].listener.Addr().String(),
		); err != nil {
			t.Fatalf(
				"add surviving peer %s to recovered node: %v",
				id,
				err,
			)
		}
	}

	if err := recoveredNode.SetTransport(
		recoveredTransport,
		survivorIDs,
	); err != nil {
		t.Fatalf(
			"set transport for recovered node: %v",
			err,
		)
	}

	allowedRecoveredPeerSANs := make(map[string]struct{})

	for _, peerID := range survivorIDs {
		allowedRecoveredPeerSANs[peerServerName(peerID)] = struct{}{}
	}

	recoveredServerTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   certFiles[initialLeaderIndex].caFile,
			CertFile: certFiles[initialLeaderIndex].serverCertFile,
			KeyFile:  certFiles[initialLeaderIndex].serverKeyFile,
		},
		allowedRecoveredPeerSANs,
	)
	if err != nil {
		t.Fatalf(
			"load TLS server config for recovered node %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	recoveredServer := startTestRaftServer(
		t,
		recoveredNode,
		recoveredServerTLS,
	)

	t.Cleanup(func() {
		recoveredServer.close()
	})

	recoveredAddress :=
		recoveredServer.listener.Addr().String()

	for i, transport := range transports {
		if i == initialLeaderIndex {
			continue
		}

		if err := transport.AddPeer(
			peerIDs[initialLeaderIndex],
			recoveredAddress,
		); err != nil {
			t.Fatalf(
				"add recovered peer %s to node %s: %v",
				peerIDs[initialLeaderIndex],
				peerIDs[i],
				err,
			)
		}
	}

	if err := recoveredNode.Start(); err != nil {
		t.Fatalf(
			"start recovered node %s: %v",
			recoveredNode.ID(),
			err,
		)
	}

	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		state := recoveredNode.State()

		if state.Role == raft.Follower &&
			state.LeaderID == peerIDs[newLeaderIndex] &&
			state.Persistent.CurrentTerm >= newLeaderTerm &&
			recoveredNode.Log().LastIndex() >= expectedLastIndex {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}

	state := recoveredNode.State()

	if state.Role != raft.Follower {
		t.Fatalf(
			"recovered node role=%v want follower",
			state.Role,
		)
	}

	if state.LeaderID != peerIDs[newLeaderIndex] {
		t.Fatalf(
			"recovered node leader=%s want=%s",
			state.LeaderID,
			peerIDs[newLeaderIndex],
		)
	}

	if state.Persistent.CurrentTerm < newLeaderTerm {
		t.Fatalf(
			"recovered node term=%d want>=%d",
			state.Persistent.CurrentTerm,
			newLeaderTerm,
		)
	}

	recoveredLastIndex :=
		recoveredNode.Log().LastIndex()

	if recoveredLastIndex != expectedLastIndex {
		t.Fatalf(
			"recovered node last index=%d want=%d",
			recoveredLastIndex,
			expectedLastIndex,
		)
	}
}

func TestGRPCTransportFollowerWALRecovery(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	const (
		node1 raft.NodeID = "node-1"
		node2 raft.NodeID = "node-2"
		node3 raft.NodeID = "node-3"
	)

	ids := []raft.NodeID{node1, node2, node3}

	nodes := make([]*raft.RaftNode, len(ids))
	stores := make([]*storage.WALStorage, len(ids))
	transports := make([]*GRPCTransport, len(ids))
	servers := make([]*testRaftServer, len(ids))

	// Build a shared test CA and per-node certificates.
	clusterTLS := newTestClusterTLS(t, ids)

	for i, id := range ids {
		node, store := newPersistentTestNode(t, id, tempDir)

		nodes[i] = node
		stores[i] = store

		transport := NewGRPCTransport()

		if err := transport.SetTLSConfig(
			clusterTLS.clientConfigs[id],
		); err != nil {
			t.Fatalf("set TLS config for %s: %v", id, err)
		}

		transports[i] = transport

		servers[i] = startTestRaftServer(
			t,
			node,
			clusterTLS.serverConfigs[id],
		)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			if server != nil {
				server.close()
			}
		}

		for _, transport := range transports {
			if transport != nil {
				_ = transport.Close()
			}
		}

		for _, store := range stores {
			if store != nil {
				_ = store.Close()
			}
		}
	})

	for i, transport := range transports {
		for j, peerID := range ids {
			if i == j {
				continue
			}

			if err := transport.AddPeer(
				peerID,
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s -> %s: %v",
					ids[i],
					peerID,
					err,
				)
			}
		}

		if err := nodes[i].SetTransport(
			transport,
			peerIDsExcept(ids, ids[i]),
		); err != nil {
			t.Fatalf(
				"set transport for %s: %v",
				ids[i],
				err,
			)
		}

		setDeterministicElectionTimeout(
			nodes[i],
			i,
		)
	}

	for _, node := range nodes {
		node.Start()
	}

	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	leaderIndex := waitForLeader(t, nodes)

	leader := nodes[leaderIndex]

	index1, err := leader.Propose([]byte("wal-entry-a"))
	if err != nil {
		t.Fatalf("propose entry-a: %v", err)
	}

	index2, err := leader.Propose([]byte("wal-entry-b"))
	if err != nil {
		t.Fatalf("propose entry-b: %v", err)
	}

	if index1 != 1 || index2 != 2 {
		t.Fatalf(
			"unexpected indexes: got %d, %d",
			index1,
			index2,
		)
	}

	followerIndex := (leaderIndex + 1) % len(nodes)
	follower := nodes[followerIndex]

	waitForCondition(t, 5*time.Second, func() bool {
		return follower.Log().LastIndex() >= 2
	})

	follower.Stop()
	servers[followerIndex].close()

	if err := stores[followerIndex].Close(); err != nil {
		t.Fatalf(
			"close follower WAL: %v",
			err,
		)
	}

	recoveredStore, err := storage.OpenWAL(
		filepath.Join(
			tempDir,
			fmt.Sprintf("%s.wal", ids[followerIndex]),
		),
	)
	if err != nil {
		t.Fatalf(
			"reopen follower WAL: %v",
			err,
		)
	}
	defer recoveredStore.Close()

	recoveredNode, err := raft.NewRaftNodeWithStorage(
		ids[followerIndex],
		recoveredStore,
	)
	if err != nil {
		t.Fatalf(
			"create recovered follower: %v",
			err,
		)
	}

	recoveredEntries, err := recoveredStore.LoadEntries()
	if err != nil {
		t.Fatalf(
			"load recovered entries: %v",
			err,
		)
	}

	if len(recoveredEntries) < 2 {
		t.Fatalf(
			"follower WAL lost replicated entries: got %d entries",
			len(recoveredEntries),
		)
	}

	if string(recoveredEntries[0].Data) != "wal-entry-a" {
		t.Fatalf(
			"entry 1 = %q, want wal-entry-a",
			recoveredEntries[0].Data,
		)
	}

	if string(recoveredEntries[1].Data) != "wal-entry-b" {
		t.Fatalf(
			"entry 2 = %q, want wal-entry-b",
			recoveredEntries[1].Data,
		)
	}

	if recoveredNode.Log().LastIndex() != 2 {
		t.Fatalf(
			"recovered follower last index = %d, want 2",
			recoveredNode.Log().LastIndex(),
		)
	}

	t.Logf(
		"follower %s recovered %d entries from WAL",
		ids[followerIndex],
		len(recoveredEntries),
	)
}

func TestGRPCTransportDivergentFollowerLogRepair(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	const (
		node1 raft.NodeID = "node-1"
		node2 raft.NodeID = "node-2"
		node3 raft.NodeID = "node-3"
	)

	ids := []raft.NodeID{node1, node2, node3}

	nodes := make([]*raft.RaftNode, len(ids))
	stores := make([]*storage.WALStorage, len(ids))
	transports := make([]*GRPCTransport, len(ids))
	servers := make([]*testRaftServer, len(ids))

	clusterTLS := newTestClusterTLS(t, ids)

	for i, id := range ids {
		node, store := newPersistentTestNode(t, id, tempDir)

		nodes[i] = node
		stores[i] = store

		transport := NewGRPCTransport()

		if err := transport.SetTLSConfig(
			clusterTLS.clientConfigs[id],
		); err != nil {
			t.Fatalf("set TLS config for %s: %v", id, err)
		}

		transports[i] = transport

		servers[i] = startTestRaftServer(
			t,
			node,
			clusterTLS.serverConfigs[id],
		)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			if server != nil {
				server.close()
			}
		}

		for _, transport := range transports {
			if transport != nil {
				_ = transport.Close()
			}
		}

		for _, store := range stores {
			if store != nil {
				_ = store.Close()
			}
		}
	})

	for i, transport := range transports {
		for j, peerID := range ids {
			if i == j {
				continue
			}

			if err := transport.AddPeer(
				peerID,
				servers[j].listener.Addr().String(),
			); err != nil {
				t.Fatalf("add peer %s -> %s: %v", ids[i], peerID, err)
			}
		}

		if err := nodes[i].SetTransport(
			transport,
			peerIDsExcept(ids, ids[i]),
		); err != nil {
			t.Fatalf("set transport for %s: %v", ids[i], err)
		}

		setDeterministicElectionTimeout(nodes[i], i)
	}

	for _, node := range nodes {
		node.Start()
	}

	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	leaderIndex := waitForLeader(t, nodes)
	leader := nodes[leaderIndex]

	for _, data := range []string{"A", "B", "C"} {
		if _, err := leader.Propose([]byte(data)); err != nil {
			t.Fatalf("propose %q: %v", data, err)
		}
	}

	waitForCondition(t, 10*time.Second, func() bool {
		for _, node := range nodes {
			if node.Log().LastIndex() < 3 {
				return false
			}

			expected := []string{"A", "B", "C"}

			for i, want := range expected {
				entry, ok := node.Log().Get(model.LogIndex(i + 1))
				if !ok || string(entry.Data) != want {
					return false
				}
			}
		}

		return true
	})

	followerIndex := (leaderIndex + 1) % len(nodes)
	followerID := ids[followerIndex]

	nodes[followerIndex].Stop()
	servers[followerIndex].close()
	_ = transports[followerIndex].Close()
	_ = stores[followerIndex].Close()

	for _, data := range []string{"D", "E"} {
		if _, err := leader.Propose([]byte(data)); err != nil {
			t.Fatalf("propose %q after follower failure: %v", data, err)
		}
	}

	recoveredPath := filepath.Join(tempDir, string(followerID)+".wal")

	recoveredStore, err := storage.OpenWAL(recoveredPath)
	if err != nil {
		t.Fatalf("reopen follower WAL: %v", err)
	}

	divergentEntries := []model.LogEntry{
		{
			Index: 4,
			Term:  99,
			Data:  []byte("X"),
		},
		{
			Index: 5,
			Term:  99,
			Data:  []byte("Y"),
		},
	}

	if err := recoveredStore.ReplaceSuffix(4, divergentEntries); err != nil {
		_ = recoveredStore.Close()
		t.Fatalf("create divergent suffix: %v", err)
	}

	if err := recoveredStore.Sync(); err != nil {
		_ = recoveredStore.Close()
		t.Fatalf("sync divergent suffix: %v", err)
	}

	if err := recoveredStore.Close(); err != nil {
		t.Fatalf("close divergent store: %v", err)
	}

	recoveredStore, err = storage.OpenWAL(recoveredPath)
	if err != nil {
		t.Fatalf("reopen divergent follower WAL: %v", err)
	}

	recoveredNode, err := raft.NewRaftNodeWithStorage(
		followerID,
		recoveredStore,
	)
	if err != nil {
		_ = recoveredStore.Close()
		t.Fatalf("create recovered follower: %v", err)
	}

	recoveredServer := startTestRaftServer(
		t,
		recoveredNode,
		clusterTLS.serverConfigs[followerID],
	)

	recoveredAddress := recoveredServer.listener.Addr().String()

	t.Cleanup(func() {
		recoveredServer.close()
		_ = recoveredStore.Close()
	})

	recoveredTransport := NewGRPCTransport()

	if err := recoveredTransport.SetTLSConfig(
		clusterTLS.clientConfigs[followerID],
	); err != nil {
		t.Fatalf("set TLS config for recovered follower: %v", err)
	}

	for i, peerID := range ids {
		if peerID == followerID {
			continue
		}

		if err := recoveredTransport.AddPeer(
			peerID,
			servers[i].listener.Addr().String(),
		); err != nil {
			t.Fatalf(
				"add peer %s to recovered follower: %v",
				peerID,
				err,
			)
		}
	}

	for i, transport := range transports {
		if i == followerIndex {
			continue
		}

		transport.RemovePeer(followerID)

		if err := transport.AddPeer(
			followerID,
			recoveredAddress,
		); err != nil {
			t.Fatalf(
				"add recovered peer %s to node %s: %v",
				followerID,
				ids[i],
				err,
			)
		}
	}

	if err := recoveredNode.SetTransport(
		recoveredTransport,
		peerIDsExcept(ids, followerID),
	); err != nil {
		t.Fatalf(
			"set transport for recovered follower: %v",
			err,
		)
	}

	setDeterministicElectionTimeout(
		recoveredNode,
		followerIndex,
	)

	recoveredNode.Start()

	defer recoveredNode.Stop()

	expected := []string{"A", "B", "C", "D", "E"}

	waitForCondition(t, 10*time.Second, func() bool {
		if recoveredNode.State().Role != raft.Follower {
			return false
		}

		for i, want := range expected {
			entry, ok := recoveredNode.Log().Get(model.LogIndex(i + 1))
			if !ok || string(entry.Data) != want {
				return false
			}
		}

		return true
	})

	for i, want := range expected {
		entry, ok := recoveredNode.Log().Get(model.LogIndex(i + 1))
		if !ok {
			t.Fatalf("missing entry %d", i+1)
		}

		if string(entry.Data) != want {
			t.Fatalf(
				"entry %d = %q, want %q",
				i+1,
				string(entry.Data),
				want,
			)
		}
	}

	if got := recoveredNode.Log().LastIndex(); got != 5 {
		t.Fatalf("recovered follower last index = %d, want 5", got)
	}
}

func TestGRPCTransportFullClusterRestartFromWAL(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()

	const (
		node1 raft.NodeID = "node-1"
		node2 raft.NodeID = "node-2"
		node3 raft.NodeID = "node-3"
	)

	ids := []raft.NodeID{node1, node2, node3}

	nodes := make([]*raft.RaftNode, len(ids))
	stores := make([]*storage.WALStorage, len(ids))
	transports := make([]*GRPCTransport, len(ids))
	servers := make([]*testRaftServer, len(ids))

	clusterTLS := newTestClusterTLS(t, ids)

	startCluster := func() {
		for i, id := range ids {
			node, store := newPersistentTestNode(
				t,
				id,
				tempDir,
			)

			nodes[i] = node
			stores[i] = store

			transport := NewGRPCTransport()

			if err := transport.SetTLSConfig(
				clusterTLS.clientConfigs[id],
			); err != nil {
				t.Fatalf("set TLS config for %s: %v", id, err)
			}

			transports[i] = transport

			servers[i] = startTestRaftServer(
				t,
				node,
				clusterTLS.serverConfigs[id],
			)
		}

		for i, transport := range transports {
			for j, peerID := range ids {
				if i == j {
					continue
				}

				if err := transport.AddPeer(
					peerID,
					servers[j].listener.Addr().String(),
				); err != nil {
					t.Fatalf(
						"add peer %s -> %s: %v",
						ids[i],
						peerID,
						err,
					)
				}
			}

			if err := nodes[i].SetTransport(
				transport,
				peerIDsExcept(ids, ids[i]),
			); err != nil {
				t.Fatalf(
					"set transport %s: %v",
					ids[i],
					err,
				)
			}

			setDeterministicElectionTimeout(
				nodes[i],
				i,
			)
		}

		for _, node := range nodes {
			node.Start()
		}
	}

	stopCluster := func() {
		for _, node := range nodes {
			node.Stop()
		}

		for _, server := range servers {
			if server != nil {
				server.close()
			}
		}

		for _, transport := range transports {
			if transport != nil {
				_ = transport.Close()
			}
		}

		for _, store := range stores {
			if store != nil {
				_ = store.Close()
			}
		}
	}

	startCluster()

	leaderIndex := waitForLeader(t, nodes)
	leader := nodes[leaderIndex]

	entries := []string{
		"restart-a",
		"restart-b",
		"restart-c",
	}

	for _, data := range entries {
		if _, err := leader.Propose([]byte(data)); err != nil {
			t.Fatalf(
				"propose %q: %v",
				data,
				err,
			)
		}
	}

	waitForCondition(t, 10*time.Second, func() bool {
		for _, node := range nodes {
			if node.Log().LastIndex() != 3 {
				return false
			}
		}

		return true
	})

	t.Log("stopping complete cluster")
	stopCluster()

	startCluster()

	defer stopCluster()

	newLeaderIndex := waitForLeader(t, nodes)
	newLeader := nodes[newLeaderIndex]

	if newLeader.Log().LastIndex() != 3 {
		t.Fatalf(
			"restarted leader last index = %d, want 3",
			newLeader.Log().LastIndex(),
		)
	}

	for i, want := range entries {
		entry, ok := newLeader.Log().Get(
			model.LogIndex(i + 1),
		)
		if !ok {
			t.Fatalf(
				"restarted leader missing entry %d",
				i+1,
			)
		}

		if string(entry.Data) != want {
			t.Fatalf(
				"restarted leader entry %d = %q, want %q",
				i+1,
				entry.Data,
				want,
			)
		}
	}

	for i, node := range nodes {
		if node.Log().LastIndex() != 3 {
			t.Fatalf(
				"node %s last index = %d, want 3",
				ids[i],
				node.Log().LastIndex(),
			)
		}
	}

	t.Logf(
		"cluster restarted successfully with leader %s and recovered log",
		newLeader.ID(),
	)
}

func registerRaftService(server *grpc.Server, node *raft.RaftNode) {
	service, err := NewRaftService(node)
	if err != nil {
		panic(fmt.Sprintf("create raft service: %v", err))
	}

	raftiqv1.RegisterRaftServiceServer(server, service)
}

func peerIDsExcept(
	ids []raft.NodeID,
	excluded raft.NodeID,
) []raft.NodeID {
	peers := make([]raft.NodeID, 0, len(ids)-1)

	for _, id := range ids {
		if id == excluded {
			continue
		}

		peers = append(peers, id)
	}

	return peers
}

func setDeterministicElectionTimeout(
	node *raft.RaftNode,
	index int,
) {
	node.SetElectionTimeout(10 + index*5)
}

func waitForLeader(
	t *testing.T,
	nodes []*raft.RaftNode,
) int {
	t.Helper()

	leaderIndex := -1

	waitForCondition(t, 10*time.Second, func() bool {
		leaderIndex = -1

		for i, node := range nodes {
			if node.State().Role != raft.Leader {
				continue
			}

			if leaderIndex != -1 {
				return false
			}

			leaderIndex = i
		}

		return leaderIndex >= 0
	})

	return leaderIndex
}

func waitForCondition(
	t *testing.T,
	timeout time.Duration,
	condition func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("condition not satisfied within %s", timeout)
}

type testClusterTLS struct {
	serverConfigs map[raft.NodeID]*tls.Config
	clientConfigs map[raft.NodeID]*tls.Config
}

func newTestClusterTLS(
	t *testing.T,
	ids []raft.NodeID,
) *testClusterTLS {
	t.Helper()

	if len(ids) == 0 {
		t.Fatal("TLS cluster requires at least one node")
	}

	dir := t.TempDir()

	ca := newTestCertificateAuthority(t, dir)

	certs := make(map[raft.NodeID]testCertificateFiles, len(ids))

	for _, id := range ids {
		certs[id] = writeTestNodeCertificate(
			t,
			ca,
			dir,
			id,
		)
	}

	result := &testClusterTLS{
		serverConfigs: make(map[raft.NodeID]*tls.Config, len(ids)),
		clientConfigs: make(map[raft.NodeID]*tls.Config, len(ids)),
	}

	for _, id := range ids {
		allowedPeerSANs := make(map[string]struct{}, len(ids)-1)

		for _, peerID := range ids {
			if peerID == id {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverFiles := certs[id]

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   serverFiles.caFile,
				CertFile: serverFiles.serverCertFile,
				KeyFile:  serverFiles.serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load server TLS config for %s: %v",
				id,
				err,
			)
		}

		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   serverFiles.caFile,
				CertFile: serverFiles.clientCertFile,
				KeyFile:  serverFiles.clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load client TLS config for %s: %v",
				id,
				err,
			)
		}

		result.serverConfigs[id] = serverTLS
		result.clientConfigs[id] = clientTLS
	}

	return result
}
