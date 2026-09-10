package transport

import (
	"context"
	"errors"
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
)

type testRaftServer struct {
	server   *grpc.Server
	listener net.Listener
}

func startTestRaftServer(
	t *testing.T,
	node *raft.RaftNode,
) *testRaftServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service, err := NewRaftService(node)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("create raft service: %v", err)
	}

	server := grpc.NewServer()
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

	server2 := startTestRaftServer(t, node2)
	defer server2.close()

	transport := NewGRPCTransport()
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

	// Stagger election timeouts so the nodes do not all become
	// candidates simultaneously. This makes the integration test
	// deterministic while the production election-timeout
	// randomization is implemented separately.
	nodes[0].SetElectionTimeout(10)
	nodes[1].SetElectionTimeout(15)
	nodes[2].SetElectionTimeout(20)

	servers := make([]*testRaftServer, 0, len(nodes))

	for _, node := range nodes {
		servers = append(
			servers,
			startTestRaftServer(t, node),
		)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			server.close()
		}
	})

	transports := make([]*GRPCTransport, 0, len(nodes))

	t.Cleanup(func() {
		for _, transport := range transports {
			if err := transport.Close(); err != nil {
				t.Errorf("close transport: %v", err)
			}
		}
	})

	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	for i, node := range nodes {
		transport := NewGRPCTransport()
		transports = append(transports, transport)

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

	// Deterministic election ordering for the integration test.
	nodes[0].SetElectionTimeout(10)
	nodes[1].SetElectionTimeout(15)
	nodes[2].SetElectionTimeout(20)

	servers := make([]*testRaftServer, 0, len(nodes))

	for _, node := range nodes {
		servers = append(
			servers,
			startTestRaftServer(t, node),
		)
	}

	transports := make([]*GRPCTransport, 0, len(nodes))

	t.Cleanup(func() {
		for _, transport := range transports {
			if err := transport.Close(); err != nil {
				t.Errorf("close transport: %v", err)
			}
		}
	})

	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	for i, node := range nodes {
		transport := NewGRPCTransport()
		transports = append(transports, transport)

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

	// Wait for the initial leader.
	var leaderIndex = -1

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

	// Select a follower to fail.
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

	leader := nodes[leaderIndex]
	follower := nodes[followerIndex]

	initialTerm := leader.State().Persistent.CurrentTerm

	// Simulate follower/network failure by shutting down its gRPC server.
	servers[followerIndex].close()

	// The leader should remain leader because the other two nodes
	// still form a majority.
	deadline = time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		state := leader.State()

		if state.Role != raft.Leader {
			t.Fatalf(
				"leader %s lost leadership after follower %s failed",
				leader.State().LeaderID,
				follower.State().LeaderID,
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

	if leader.State().Role != raft.Leader {
		t.Fatalf(
			"expected node %s to remain leader after follower %s failure",
			peerIDs[leaderIndex],
			peerIDs[followerIndex],
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

	servers := make([]*testRaftServer, len(nodes))

	for i, node := range nodes {
		servers[i] = startTestRaftServer(t, node)
	}

	t.Cleanup(func() {
		for _, server := range servers {
			server.close()
		}
	})

	transports := make([]*GRPCTransport, len(nodes))

	t.Cleanup(func() {
		for _, transport := range transports {
			if err := transport.Close(); err != nil {
				t.Errorf("close transport: %v", err)
			}
		}
	})

	peerIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	for i, node := range nodes {
		transport := NewGRPCTransport()
		transports[i] = transport

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

	// Wait for exactly one leader.
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

	// Select a follower to fail.
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

	// Simulate follower failure by shutting down its gRPC endpoint.
	servers[followerIndex].close()

	// The leader must remain leader because the other two nodes
	// still form a majority.
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

	// Start a new gRPC endpoint for the same follower Raft process.
	servers[followerIndex] = startTestRaftServer(
		t,
		follower,
	)

	t.Cleanup(func() {
		servers[followerIndex].close()
	})

	recoveredAddress := servers[followerIndex].
		listener.
		Addr().
		String()

	// Replace the failed follower's old address in the
	// transports of the other two nodes.
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

	// Wait for the recovered follower to receive heartbeats
	// from the current leader.
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

	nodes := make([]*raft.RaftNode, len(peerIDs))
	stores := make([]*storage.WALStorage, len(peerIDs))
	servers := make([]*testRaftServer, len(peerIDs))
	transports := make([]*GRPCTransport, len(peerIDs))

	electionTimeouts := []int{
		10,
		15,
		20,
	}

	// Create three Raft nodes backed by persistent WAL storage.
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

	// Start one real gRPC server for every Raft node.
	for i, node := range nodes {
		server := startTestRaftServer(t, node)
		servers[i] = server

		t.Cleanup(func() {
			server.close()
		})
	}

	// Create one real gRPC transport per Raft node.
	for i := range nodes {
		transport := NewGRPCTransport()
		transports[i] = transport

		t.Cleanup(func() {
			_ = transport.Close()
		})
	}

	// Connect every node to every other node.
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

	// Start all Raft processes.
	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start node %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	// Wait for exactly one initial leader.
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

	initialState := nodes[initialLeaderIndex].State()
	initialTerm := initialState.Persistent.CurrentTerm

	t.Logf(
		"initial leader: %s term=%d",
		peerIDs[initialLeaderIndex],
		initialTerm,
	)

	// Simulate a complete leader process failure:
	// stop the Raft process and shut down its network endpoint.
	nodes[initialLeaderIndex].Stop()
	servers[initialLeaderIndex].close()

	// Close the old WAL before reopening it later as a new process.
	if err := stores[initialLeaderIndex].Close(); err != nil {
		t.Fatalf(
			"close WAL for failed leader %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	// Wait for one of the surviving nodes to become leader.
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
		t.Fatalf(
			"expected new leader after %s failed",
			peerIDs[initialLeaderIndex],
		)
	}

	newLeaderState := nodes[newLeaderIndex].State()
	newTerm := newLeaderState.Persistent.CurrentTerm

	if newTerm <= initialTerm {
		t.Fatalf(
			"new leader term=%d, want > initial term=%d",
			newTerm,
			initialTerm,
		)
	}

	t.Logf(
		"new leader: %s term=%d",
		peerIDs[newLeaderIndex],
		newTerm,
	)

	// -----------------------------------------------------------------
	// Recover the failed leader as a completely new Raft process.
	// -----------------------------------------------------------------

	walPath := filepath.Join(
		testDir,
		fmt.Sprintf("%s.wal", peerIDs[initialLeaderIndex]),
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

	// The recovered process gets a fresh transport.
	recoveredTransport := NewGRPCTransport()

	t.Cleanup(func() {
		_ = recoveredTransport.Close()
	})

	// Connect the recovered node to both surviving nodes.
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
			"set transport for recovered node %s: %v",
			recoveredNode.ID(),
			err,
		)
	}

	// Start a new network endpoint for the reconstructed process.
	recoveredServer := startTestRaftServer(
		t,
		recoveredNode,
	)

	t.Cleanup(func() {
		recoveredServer.close()
	})

	recoveredAddress := recoveredServer.
		listener.
		Addr().
		String()

	// The surviving nodes still have the crashed leader's old
	// network address. Replace those peer connections.
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

	// Start the reconstructed Raft process.
	if err := recoveredNode.Start(); err != nil {
		t.Fatalf(
			"start recovered node %s: %v",
			recoveredNode.ID(),
			err,
		)
	}

	// The recovered process must:
	//   1. load its old persistent term from WAL,
	//   2. observe the newer leader term,
	//   3. step down to follower,
	//   4. recognize the current leader.
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

	nodes := make([]*raft.RaftNode, len(peerIDs))
	stores := make([]*storage.WALStorage, len(peerIDs))
	servers := make([]*testRaftServer, len(peerIDs))
	transports := make([]*GRPCTransport, len(peerIDs))

	electionTimeouts := []int{
		10,
		15,
		20,
	}

	// ---------------------------------------------------------------
	// Create persistent Raft nodes.
	// ---------------------------------------------------------------

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

	// ---------------------------------------------------------------
	// Start real gRPC servers.
	// ---------------------------------------------------------------

	for i, node := range nodes {
		server := startTestRaftServer(t, node)
		servers[i] = server

		t.Cleanup(func() {
			server.close()
		})
	}

	// ---------------------------------------------------------------
	// Create real gRPC transports.
	// ---------------------------------------------------------------

	for i := range nodes {
		transport := NewGRPCTransport()
		transports[i] = transport

		t.Cleanup(func() {
			_ = transport.Close()
		})
	}

	// ---------------------------------------------------------------
	// Fully connect the cluster.
	// ---------------------------------------------------------------

	for i := range nodes {
		peerList := make([]raft.NodeID, 0, len(peerIDs)-1)

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

	// ---------------------------------------------------------------
	// Start the cluster.
	// ---------------------------------------------------------------

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start node %s: %v",
				peerIDs[i],
				err,
			)
		}
	}

	// ---------------------------------------------------------------
	// Wait for exactly one leader.
	// ---------------------------------------------------------------

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

	// ---------------------------------------------------------------
	// Propose entries while the original leader is still alive.
	//
	// These entries must exist in the leader's WAL before failure.
	// ---------------------------------------------------------------

	initialEntries := [][]byte{
		[]byte("entry-a"),
		[]byte("entry-b"),
	}

	initialIndexes := make([]raft.LogIndex, 0, len(initialEntries))

	for _, data := range initialEntries {
		index, err := nodes[initialLeaderIndex].Propose(data)
		if err != nil {
			t.Fatalf(
				"propose initial entry: %v",
				err,
			)
		}

		initialIndexes = append(initialIndexes, index)
	}

	expectedInitialLastIndex :=
		initialIndexes[len(initialIndexes)-1]

	// Wait until all currently connected followers have the
	// initial entries.
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

	// ---------------------------------------------------------------
	// Crash the original leader completely.
	// ---------------------------------------------------------------

	nodes[initialLeaderIndex].Stop()
	servers[initialLeaderIndex].close()

	if err := stores[initialLeaderIndex].Close(); err != nil {
		t.Fatalf(
			"close WAL for failed leader %s: %v",
			peerIDs[initialLeaderIndex],
			err,
		)
	}

	// ---------------------------------------------------------------
	// Wait for a surviving leader.
	// ---------------------------------------------------------------

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

	// ---------------------------------------------------------------
	// Make sure the surviving nodes no longer have a usable
	// connection to the crashed leader.
	//
	// This makes the failure explicit and avoids relying on the old
	// gRPC endpoint.
	// ---------------------------------------------------------------

	for i, transport := range transports {
		if i == initialLeaderIndex {
			continue
		}

		transport.RemovePeer(peerIDs[initialLeaderIndex])
	}

	// ---------------------------------------------------------------
	// Propose new entries on the surviving leader.
	//
	// The crashed node is offline, so these entries cannot be
	// replicated to it. The two surviving nodes form a majority.
	// ---------------------------------------------------------------

	recoveryEntries := [][]byte{
		[]byte("entry-c"),
		[]byte("entry-d"),
		[]byte("entry-e"),
	}

	recoveryIndexes := make([]raft.LogIndex, 0, len(recoveryEntries))

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

	// ---------------------------------------------------------------
	// Verify the surviving leader contains the complete log.
	// ---------------------------------------------------------------

	leaderLogLastIndex :=
		nodes[newLeaderIndex].Log().LastIndex()

	if leaderLogLastIndex != expectedLastIndex {
		t.Fatalf(
			"leader last index=%d want=%d",
			leaderLogLastIndex,
			expectedLastIndex,
		)
	}

	// ---------------------------------------------------------------
	// Reopen the crashed leader's WAL as a completely new process.
	// ---------------------------------------------------------------

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

	// ---------------------------------------------------------------
	// Give the recovered node a fresh transport connected to the
	// two surviving nodes.
	// ---------------------------------------------------------------

	recoveredTransport := NewGRPCTransport()

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

	// ---------------------------------------------------------------
	// Start a fresh gRPC endpoint for the recovered process.
	// ---------------------------------------------------------------

	recoveredServer := startTestRaftServer(
		t,
		recoveredNode,
	)

	t.Cleanup(func() {
		recoveredServer.close()
	})

	recoveredAddress :=
		recoveredServer.listener.Addr().String()

	// Update surviving nodes with the recovered leader's new address.
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

	// ---------------------------------------------------------------
	// Start the recovered node.
	// ---------------------------------------------------------------

	if err := recoveredNode.Start(); err != nil {
		t.Fatalf(
			"start recovered node %s: %v",
			recoveredNode.ID(),
			err,
		)
	}

	// ---------------------------------------------------------------
	// Wait for the recovered node to:
	//
	//   1. discover the current leader,
	//   2. remain a follower,
	//   3. receive the missing entries,
	//   4. converge to the leader's last log index.
	// ---------------------------------------------------------------

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

	// ---------------------------------------------------------------
	// Verify every log entry matches the surviving leader.
	// ---------------------------------------------------------------

	leaderNode := nodes[newLeaderIndex]

	for index := raft.LogIndex(1); index <= expectedLastIndex; index++ {
		leaderEntry, leaderOK :=
			leaderNode.Log().Get(index)

		recoveredEntry, recoveredOK :=
			recoveredNode.Log().Get(index)

		if !leaderOK {
			t.Fatalf(
				"leader missing log entry at index %d",
				index,
			)
		}

		if !recoveredOK {
			t.Fatalf(
				"recovered node missing log entry at index %d",
				index,
			)
		}

		if recoveredEntry.Index != leaderEntry.Index {
			t.Fatalf(
				"log index mismatch at %d: recovered=%d leader=%d",
				index,
				recoveredEntry.Index,
				leaderEntry.Index,
			)
		}

		if recoveredEntry.Term != leaderEntry.Term {
			t.Fatalf(
				"log term mismatch at index %d: recovered=%d leader=%d",
				index,
				recoveredEntry.Term,
				leaderEntry.Term,
			)
		}

		if string(recoveredEntry.Data) != string(leaderEntry.Data) {
			t.Fatalf(
				"log data mismatch at index %d: recovered=%q leader=%q",
				index,
				recoveredEntry.Data,
				leaderEntry.Data,
			)
		}
	}

	t.Logf(
		"recovered node caught up: node=%s lastIndex=%d leader=%s",
		recoveredNode.ID(),
		recoveredLastIndex,
		leaderNode.ID(),
	)
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
	servers := make([]*grpc.Server, len(ids))
	addresses := make([]string, len(ids))

	for i, id := range ids {
		node, store := newPersistentTestNode(t, id, tempDir)

		nodes[i] = node
		stores[i] = store

		transport := NewGRPCTransport()
		transports[i] = transport

		server := grpc.NewServer()
		servers[i] = server

		registerRaftService(server, node)

		listener, err := net.Listen(
			"tcp",
			"127.0.0.1:0",
		)
		if err != nil {
			t.Fatalf(
				"listen for %s: %v",
				id,
				err,
			)
		}

		addresses[i] = listener.Addr().String()

		go func() {
			if err := server.Serve(listener); err != nil &&
				!errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("gRPC server failed: %v", err)
			}
		}()
	}

	t.Cleanup(func() {
		for _, server := range servers {
			server.Stop()
		}

		for _, transport := range transports {
			_ = transport.Close()
		}

		for _, store := range stores {
			_ = store.Close()
		}
	})

	// Connect every node to every other node.
	for i, transport := range transports {
		for j, peerID := range ids {
			if i == j {
				continue
			}

			if err := transport.AddPeer(
				peerID,
				addresses[j],
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

	// Choose a follower and wait until it has actually replicated
	// both entries before shutting it down.
	followerIndex := (leaderIndex + 1) % len(nodes)
	follower := nodes[followerIndex]

	waitForCondition(t, 5*time.Second, func() bool {
		return follower.Log().LastIndex() >= 2
	})

	// Stop only the follower.
	follower.Stop()
	servers[followerIndex].Stop()

	if err := stores[followerIndex].Close(); err != nil {
		t.Fatalf(
			"close follower WAL: %v",
			err,
		)
	}

	// Reopen the SAME WAL.
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

	// The important assertion:
	// the entries came back from the follower's own WAL.
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
	servers := make([]*grpc.Server, len(ids))
	addresses := make([]string, len(ids))

	// Start the initial three-node cluster.
	for i, id := range ids {
		node, store := newPersistentTestNode(t, id, tempDir)

		nodes[i] = node
		stores[i] = store

		transport := NewGRPCTransport()
		transports[i] = transport

		server := grpc.NewServer()
		servers[i] = server

		registerRaftService(server, node)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen for %s: %v", id, err)
		}

		addresses[i] = listener.Addr().String()

		go func() {
			if err := server.Serve(listener); err != nil &&
				!errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("serve %s: %v", id, err)
			}
		}()
	}

	t.Cleanup(func() {
		for _, server := range servers {
			if server != nil {
				server.Stop()
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

	// Connect every node to every other node.
	for i, transport := range transports {
		for j, peerID := range ids {
			if i == j {
				continue
			}

			transport.AddPeer(peerID, addresses[j])
		}

		nodes[i].SetTransport(
			transport,
			peerIDsExcept(ids, ids[i]),
		)

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

	// Wait for a leader.
	leaderIndex := waitForLeader(t, nodes)
	leader := nodes[leaderIndex]

	// Establish the canonical log:
	//
	//   A B C
	//
	for _, data := range []string{"A", "B", "C"} {
		if _, err := leader.Propose([]byte(data)); err != nil {
			t.Fatalf("propose %q: %v", data, err)
		}
	}

	// Wait until all three nodes contain the canonical prefix.
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

	// Stop one follower and remove its transport/server.
	followerIndex := (leaderIndex + 1) % len(nodes)
	followerID := ids[followerIndex]

	nodes[followerIndex].Stop()
	servers[followerIndex].Stop()
	_ = transports[followerIndex].Close()
	_ = stores[followerIndex].Close()

	// The leader continues with the canonical log:
	//
	//   A B C D E
	//
	for _, data := range []string{"D", "E"} {
		if _, err := leader.Propose([]byte(data)); err != nil {
			t.Fatalf("propose %q after follower failure: %v", data, err)
		}
	}

	// Reopen the stopped follower's WAL.
	recoveredPath := filepath.Join(tempDir, string(followerID)+".wal")

	recoveredStore, err := storage.OpenWAL(recoveredPath)
	if err != nil {
		t.Fatalf("reopen follower WAL: %v", err)
	}

	// Intentionally corrupt the follower's suffix:
	//
	//   canonical:  A B C D E
	//   divergent:  A B C X Y
	//
	// This simulates a follower that has conflicting entries from another
	// leader/term.
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

	// Reopen the intentionally divergent WAL.
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

	// Start a new gRPC server for the recovered follower.
	recoveredServer := grpc.NewServer()
	registerRaftService(recoveredServer, recoveredNode)

	recoveredListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = recoveredStore.Close()
		t.Fatalf("listen for recovered follower: %v", err)
	}

	recoveredAddress := recoveredListener.Addr().String()

	go func() {
		if err := recoveredServer.Serve(recoveredListener); err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve recovered follower: %v", err)
		}
	}()

	t.Cleanup(func() {
		recoveredServer.Stop()
		_ = recoveredListener.Close()
		_ = recoveredStore.Close()
	})

	// Create a fresh transport for the recovered follower.
	recoveredTransport := NewGRPCTransport()

	for i, peerID := range ids {
		if peerID == followerID {
			continue
		}

		recoveredTransport.AddPeer(peerID, addresses[i])
	}

	// Replace the old follower address in the surviving nodes.
	for i, transport := range transports {
		if i == followerIndex {
			continue
		}

		transport.RemovePeer(followerID)
		transport.AddPeer(followerID, recoveredAddress)
	}

	recoveredNode.SetTransport(
		recoveredTransport,
		peerIDsExcept(ids, followerID),
	)

	setDeterministicElectionTimeout(
		recoveredNode,
		followerIndex,
	)

	recoveredNode.Start()

	defer recoveredNode.Stop()

	// The important part:
	//
	// DO NOT wait only for LastIndex == 5.
	//
	// The divergent log A B C X Y already has LastIndex == 5.
	// We must wait until the actual contents have been repaired:
	//
	//   A B C X Y
	//        ↓
	//   A B C D E
	//
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

	// Final verification.
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

	// Verify that the recovered follower has exactly the canonical log length.
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
	servers := make([]*grpc.Server, len(ids))
	addresses := make([]string, len(ids))

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
			transports[i] = transport

			server := grpc.NewServer()
			servers[i] = server

			registerRaftService(server, node)

			listener, err := net.Listen(
				"tcp",
				"127.0.0.1:0",
			)
			if err != nil {
				t.Fatalf("listen: %v", err)
			}

			addresses[i] = listener.Addr().String()

			go func() {
				if err := server.Serve(listener); err != nil &&
					!errors.Is(err, grpc.ErrServerStopped) {
					t.Errorf("server failed: %v", err)
				}
			}()
		}

		for i, transport := range transports {
			for j, peerID := range ids {
				if i == j {
					continue
				}

				if err := transport.AddPeer(
					peerID,
					addresses[j],
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
			server.Stop()
		}

		for _, transport := range transports {
			_ = transport.Close()
		}

		for _, store := range stores {
			_ = store.Close()
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

	// Make sure all nodes have the complete log before
	// shutting down the entire cluster.
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

	// Recreate every node from its existing WAL.
	startCluster()

	defer stopCluster()

	// A new election should happen using recovered persistent state.
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

	// Every node should recover the same log.
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
