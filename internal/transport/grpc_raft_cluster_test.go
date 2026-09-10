package transport

import (
	"context"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
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

	// Configure every node with every other node as a peer.
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

	// Start all Raft nodes.
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

	// Simulate follower failure by stopping its gRPC server.
	servers[followerIndex].close()

	// Give the cluster time to operate with only two reachable nodes.
	time.Sleep(500 * time.Millisecond)

	if leader.State().Role != raft.Leader {
		t.Fatalf(
			"expected leader %s to remain leader while follower %s is down",
			peerIDs[leaderIndex],
			peerIDs[followerIndex],
		)
	}

	// Restart the follower's network endpoint while keeping the same
	// RaftNode instance. This simulates the process/network endpoint
	// recovering with its existing Raft state.
	servers[followerIndex] = startTestRaftServer(
		t,
		follower,
	)

	// Update every other node's transport with the recovered endpoint.
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
				"re-add recovered peer %s to %s: %v",
				peerIDs[followerIndex],
				peerIDs[i],
				err,
			)
		}
	}

	// Wait for the recovered follower to observe the current leader.
	deadline = time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		state := follower.State()

		if state.LeaderID == peerIDs[leaderIndex] &&
			state.Persistent.CurrentTerm >= initialTerm {
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
