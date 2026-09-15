package transport

import (
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
)

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
				t.Fatalf(
					"set TLS config for %s: %v",
					id,
					err,
				)
			}

			transports[i] = transport

			servers[i] = startTestRaftServer(
				t,
				node,
				clusterTLS.serverConfigs[id],
			)
		}

		for i, transport := range transports {
			otherPeers := peerIDsExcept(
				ids,
				ids[i],
			)

			for _, peerID := range otherPeers {
				peerIndex := indexOfNodeID(ids, peerID)

				if err := transport.AddPeer(
					peerID,
					servers[peerIndex].listener.Addr().String(),
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
				otherPeers,
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
