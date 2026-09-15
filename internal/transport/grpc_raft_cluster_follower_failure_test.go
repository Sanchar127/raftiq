package transport

import (
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

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

	certFiles := make(
		[]testCertificateFiles,
		len(nodes),
	)

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

		otherPeers := peerIDsExcept(
			peerIDs,
			peerIDs[i],
		)

		for _, peerID := range otherPeers {
			peerIndex := indexOfNodeID(peerIDs, peerID)

			if err := transport.AddPeer(
				peerID,
				servers[peerIndex].listener.Addr().String(),
			); err != nil {
				t.Fatalf(
					"add peer %s to node %s: %v",
					peerID,
					peerIDs[i],
					err,
				)
			}
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

		if err := node.BootstrapMembership(); err != nil {
			t.Fatalf(
				"bootstrap membership for %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})

	var leader *raft.RaftNode

	deadline := time.Now().Add(5 * time.Second)

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
