package transport

import (
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

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
	ca := newTestCertificateAuthority(t, certDir)

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
				t.Errorf("close transport: %v", err)
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

		transports = append(transports, transport)

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
					"add peer %s to %s: %v",
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
