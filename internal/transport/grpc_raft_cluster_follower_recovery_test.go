package transport

import (
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

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

	recoveredAddress := servers[followerIndex].listener.Addr().String()

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
