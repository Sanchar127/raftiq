package chaos_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

const leaderKillWaitTimeout = 5 * time.Second

const leaderKillElectionTicks = 10

func TestLeaderKill(t *testing.T) {
	cluster := newLeaderKillCluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	leader := cluster.waitForLeader(leaderKillWaitTimeout)
	if leader == nil {
		t.Fatal("cluster failed to elect an initial leader")
	}

	initialTerm := leader.State().Persistent.CurrentTerm
	initialLeaderID := leader.ID()

	if _, err := leader.Propose([]byte("before-leader-kill")); err != nil {
		t.Fatalf("initial leader proposal failed: %v", err)
	}

	cluster.waitForLogEntry(
		t,
		leaderKillWaitTimeout,
		[]byte("before-leader-kill"),
	)

	t.Logf(
		"initial leader elected: node=%s term=%d",
		initialLeaderID,
		initialTerm,
	)

	// Removing the leader from the transport makes it unreachable to the
	// surviving nodes. Stopping the node alone would stop its election loop,
	// but LocalTransport could still invoke its RPC handlers.
	cluster.transport.RemoveNode(initialLeaderID)
	leader.Stop()

	t.Logf(
		"killed leader: node=%s term=%d",
		initialLeaderID,
		initialTerm,
	)

	newLeader := cluster.waitForNewLeader(
		initialLeaderID,
		initialTerm,
		leaderKillWaitTimeout,
	)

	if newLeader == nil {
		t.Fatal("surviving majority failed to elect a new leader")
	}

	newTerm := newLeader.State().Persistent.CurrentTerm

	if newTerm <= initialTerm {
		t.Fatalf(
			"new leader term did not advance: old=%d new=%d",
			initialTerm,
			newTerm,
		)
	}

	if newLeader.ID() == initialLeaderID {
		t.Fatalf(
			"killed leader became leader again: %s",
			initialLeaderID,
		)
	}

	t.Logf(
		"new leader elected: node=%s term=%d",
		newLeader.ID(),
		newTerm,
	)

	if _, err := newLeader.Propose([]byte("after-leader-kill")); err != nil {
		t.Fatalf("new leader proposal failed: %v", err)
	}

	cluster.waitForLogEntry(
		t,
		leaderKillWaitTimeout,
		[]byte("after-leader-kill"),
	)

	cluster.assertSingleLeaderPerTerm(t)
}

type leaderKillCluster struct {
	transport *raft.LocalTransport
	nodes     []*raft.RaftNode
}

func newLeaderKillCluster(t *testing.T) *leaderKillCluster {
	t.Helper()

	transport := raft.NewLocalTransport()

	nodes := []*raft.RaftNode{
		raft.NewRaftNode("node-1"),
		raft.NewRaftNode("node-2"),
		raft.NewRaftNode("node-3"),
	}

	peerIDs := []raft.NodeID{
		nodes[0].ID(),
		nodes[1].ID(),
		nodes[2].ID(),
	}

	electionTimeouts := []int{10, 12, 14}

	for i, node := range nodes {
		node.SetElectionTimeout(electionTimeouts[i])

		if err := transport.AddNode(node); err != nil {
			t.Fatalf(
				"add node %s to transport: %v",
				node.ID(),
				err,
			)
		}

		if err := node.SetTransport(
			transport,
			peerIDsWithoutSelf(peerIDs, node.ID()),
		); err != nil {
			t.Fatalf(
				"set transport for node %s: %v",
				node.ID(),
				err,
			)
		}
	}

	return &leaderKillCluster{
		transport: transport,
		nodes:     nodes,
	}
}

func (c *leaderKillCluster) start() {
	for _, node := range c.nodes {
		if err := node.Start(); err != nil {
			panic(fmt.Sprintf(
				"start node %s: %v",
				node.ID(),
				err,
			))
		}
	}
}

func (c *leaderKillCluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}

	c.transport.Close()
}

func (c *leaderKillCluster) waitForLeader(
	timeout time.Duration,
) *raft.RaftNode {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leader *raft.RaftNode

		for _, node := range c.nodes {
			if node.State().Role != raft.Leader {
				continue
			}

			if leader != nil {
				// During a transient election there may briefly be more than
				// one observed leader. Keep waiting for a stable result.
				leader = nil
				break
			}

			leader = node
		}

		if leader != nil {
			return leader
		}

		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (c *leaderKillCluster) waitForNewLeader(
	oldLeaderID raft.NodeID,
	oldTerm raft.Term,
	timeout time.Duration,
) *raft.RaftNode {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leader *raft.RaftNode

		for _, node := range c.nodes {
			if node.ID() == oldLeaderID {
				continue
			}

			state := node.State()

			if state.Role != raft.Leader ||
				state.Persistent.CurrentTerm <= oldTerm {
				continue
			}

			if leader != nil {
				// More than one leader was observed. Continue waiting for a
				// stable result.
				leader = nil
				break
			}

			leader = node
		}

		if leader != nil {
			return leader
		}

		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (c *leaderKillCluster) waitForLogEntry(
	t *testing.T,
	timeout time.Duration,
	data []byte,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if containsLogEntry(node.Log(), data) {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	for _, node := range c.nodes {
		term := node.State().Persistent.CurrentTerm

		t.Fatalf(
			"timed out waiting for log entry %q on node %s (term=%d last_index=%d)",
			data,
			node.ID(),
			term,
			node.Log().LastIndex(),
		)
	}
}

func (c *leaderKillCluster) assertSingleLeaderPerTerm(t *testing.T) {
	t.Helper()

	leadersByTerm := make(map[raft.Term]raft.NodeID)

	for _, node := range c.nodes {
		state := node.State()

		if state.Role != raft.Leader {
			continue
		}

		if previous, exists := leadersByTerm[state.Persistent.CurrentTerm]; exists {
			t.Fatalf(
				"multiple leaders observed in term %d: %s and %s",
				state.Persistent.CurrentTerm,
				previous,
				node.ID(),
			)
		}

		leadersByTerm[state.Persistent.CurrentTerm] = node.ID()
	}
}

func containsLogEntry(log *raft.Log, data []byte) bool {
	lastIndex := log.LastIndex()

	for index := raft.LogIndex(1); index <= lastIndex; index++ {
		entry, ok := log.Get(index)
		if !ok {
			continue
		}

		if string(entry.Data) == string(data) {
			return true
		}
	}

	return false
}

func peerIDsWithoutSelf(
	peerIDs []raft.NodeID,
	self raft.NodeID,
) []raft.NodeID {
	result := make([]raft.NodeID, 0, len(peerIDs)-1)

	for _, id := range peerIDs {
		if id != self {
			result = append(result, id)
		}
	}

	return result
}
