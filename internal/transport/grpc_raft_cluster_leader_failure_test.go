package transport

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
)

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

		node.SetElectionTimeout(electionTimeouts[i])

		if err := node.BootstrapMembership(); err != nil {
			t.Fatalf(
				"bootstrap membership for %s: %v",
				id,
				err,
			)
		}

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
				"set TLS client config for node %s: %v",
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
		otherPeers := peerIDsExcept(
			peerIDs,
			peerIDs[i],
		)

		for _, peerID := range otherPeers {
			peerIndex := indexOfNodeID(peerIDs, peerID)

			if err := transports[i].AddPeer(
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
			initialTerm = nodes[candidateIndex].
				State().
				Persistent.
				CurrentTerm
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
			newTerm = nodes[candidateIndex].
				State().
				Persistent.
				CurrentTerm
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
			fmt.Sprintf(
				"%s.wal",
				peerIDs[initialLeaderIndex],
			),
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

	survivorIDs := make(
		[]raft.NodeID,
		0,
		len(peerIDs)-1,
	)

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
