package transport

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
)

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
				"load client TLS config for %s: %v",
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
					"add peer %s -> %s: %v",
					peerIDs[i],
					peerID,
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

		survivorIDs = append(survivorIDs, id)

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
