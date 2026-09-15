package transport

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
)

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

	clusterTLS := newTestClusterTLS(t, ids)

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

		if err := nodes[i].BootstrapMembership(); err != nil {
			t.Fatalf(
				"bootstrap membership for %s: %v",
				ids[i],
				err,
			)
		}
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
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

		setDeterministicElectionTimeout(nodes[i], i)

		if err := nodes[i].BootstrapMembership(); err != nil {
			t.Fatalf(
				"bootstrap membership for %s: %v",
				ids[i],
				err,
			)
		}
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("start node: %v", err)
		}
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
				entry, ok := node.Log().Get(
					model.LogIndex(i + 1),
				)
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
			t.Fatalf(
				"propose %q after follower failure: %v",
				data,
				err,
			)
		}
	}

	recoveredPath := filepath.Join(
		tempDir,
		string(followerID)+".wal",
	)

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

	if err := recoveredStore.ReplaceSuffix(
		4,
		divergentEntries,
	); err != nil {
		_ = recoveredStore.Close()

		t.Fatalf(
			"create divergent suffix: %v",
			err,
		)
	}

	if err := recoveredStore.Sync(); err != nil {
		_ = recoveredStore.Close()

		t.Fatalf(
			"sync divergent suffix: %v",
			err,
		)
	}

	if err := recoveredStore.Close(); err != nil {
		t.Fatalf(
			"close divergent store: %v",
			err,
		)
	}

	recoveredStore, err = storage.OpenWAL(recoveredPath)
	if err != nil {
		t.Fatalf(
			"reopen divergent follower WAL: %v",
			err,
		)
	}

	recoveredNode, err := raft.NewRaftNodeWithStorage(
		followerID,
		recoveredStore,
	)
	if err != nil {
		_ = recoveredStore.Close()

		t.Fatalf(
			"create recovered follower: %v",
			err,
		)
	}

	recoveredServer := startTestRaftServer(
		t,
		recoveredNode,
		clusterTLS.serverConfigs[followerID],
	)

	recoveredAddress :=
		recoveredServer.listener.Addr().String()

	t.Cleanup(func() {
		recoveredServer.close()
		_ = recoveredStore.Close()
	})

	recoveredTransport := NewGRPCTransport()

	if err := recoveredTransport.SetTLSConfig(
		clusterTLS.clientConfigs[followerID],
	); err != nil {
		t.Fatalf(
			"set TLS config for recovered follower: %v",
			err,
		)
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

	if err := recoveredNode.Start(); err != nil {
		t.Fatalf(
			"start recovered follower: %v",
			err,
		)
	}

	defer recoveredNode.Stop()

	expected := []string{"A", "B", "C", "D", "E"}

	waitForCondition(t, 10*time.Second, func() bool {
		if recoveredNode.State().Role != raft.Follower {
			return false
		}

		for i, want := range expected {
			entry, ok := recoveredNode.Log().Get(
				model.LogIndex(i + 1),
			)
			if !ok || string(entry.Data) != want {
				return false
			}
		}

		return true
	})

	for i, want := range expected {
		entry, ok := recoveredNode.Log().Get(
			model.LogIndex(i + 1),
		)
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
		t.Fatalf(
			"recovered follower last index = %d, want 5",
			got,
		)
	}
}
