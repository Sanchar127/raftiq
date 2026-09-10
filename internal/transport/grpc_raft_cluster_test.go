package transport

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
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
