package chaos_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

const followerKillWaitTimeout = 5 * time.Second

func TestFollowerKill(t *testing.T) {
	cluster := newFollowerKillCluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	leader := cluster.waitForLeader(followerKillWaitTimeout)
	if leader == nil {
		t.Fatal("cluster failed to elect an initial leader")
	}

	initialTerm := leader.State().Persistent.CurrentTerm
	leaderID := leader.ID()

	var killedFollower *raft.RaftNode
	var survivingFollower *raft.RaftNode

	for _, node := range cluster.nodes {
		if node.ID() == leaderID {
			continue
		}

		if killedFollower == nil {
			killedFollower = node
			continue
		}

		survivingFollower = node
	}

	if killedFollower == nil || survivingFollower == nil {
		t.Fatal("failed to identify both followers")
	}

	t.Logf(
		"initial leader elected: node=%s term=%d",
		leaderID,
		initialTerm,
	)

	// Remove the follower from the transport before stopping it. This makes
	// the follower unreachable to the surviving nodes.
	cluster.transport.RemoveNode(killedFollower.ID())
	killedFollower.Stop()

	t.Logf(
		"killed follower: node=%s",
		killedFollower.ID(),
	)

	// A 3-node Raft cluster requires 2 nodes for a majority. The leader and
	// surviving follower still form that majority, so leadership should remain
	// stable.
	state := leader.State()

	if state.Role != raft.Leader {
		t.Fatalf(
			"leader lost leadership after follower failure: node=%s role=%v",
			leaderID,
			state.Role,
		)
	}

	if state.Persistent.CurrentTerm != initialTerm {
		t.Fatalf(
			"leader term changed after follower failure: old=%d new=%d",
			initialTerm,
			state.Persistent.CurrentTerm,
		)
	}

	data := []byte("after-follower-kill")

	index, err := leader.Propose(data)
	if err != nil {
		t.Fatalf("proposal after follower failure failed: %v", err)
	}

	t.Logf(
		"post-failure proposal committed: leader=%s index=%d term=%d",
		leaderID,
		index,
		initialTerm,
	)

	cluster.waitForLogEntry(
		t,
		leader,
		followerKillWaitTimeout,
		index,
		data,
	)

	cluster.waitForLogEntry(
		t,
		survivingFollower,
		followerKillWaitTimeout,
		index,
		data,
	)

	// The removed follower must not receive the post-failure entry.
	if entry, ok := killedFollower.Log().Get(index); ok &&
		string(entry.Data) == string(data) {
		t.Fatalf(
			"killed follower received post-failure entry: node=%s index=%d",
			killedFollower.ID(),
			index,
		)
	}

	finalState := leader.State()

	if finalState.Role != raft.Leader {
		t.Fatalf(
			"leader lost leadership after post-failure proposal: node=%s role=%v",
			leaderID,
			finalState.Role,
		)
	}

	if finalState.Persistent.CurrentTerm != initialTerm {
		t.Fatalf(
			"leader term changed after post-failure proposal: old=%d new=%d",
			initialTerm,
			finalState.Persistent.CurrentTerm,
		)
	}

	t.Logf(
		"follower failure tolerated: leader=%s survivingFollower=%s term=%d",
		leaderID,
		survivingFollower.ID(),
		initialTerm,
	)
}

type followerKillCluster struct {
	transport *raft.LocalTransport
	nodes     []*raft.RaftNode
}

func newFollowerKillCluster(t *testing.T) *followerKillCluster {
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

	// Stagger election timeouts so the chaos test does not rely on perfectly
	// synchronized timers avoiding a split vote.
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

	return &followerKillCluster{
		transport: transport,
		nodes:     nodes,
	}
}

func (c *followerKillCluster) start() {
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

func (c *followerKillCluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}

	c.transport.Close()
}

func (c *followerKillCluster) waitForLeader(
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

func (c *followerKillCluster) waitForLogEntry(
	t *testing.T,
	node *raft.RaftNode,
	timeout time.Duration,
	index raft.LogIndex,
	data []byte,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		entry, ok := node.Log().Get(index)
		if ok && string(entry.Data) == string(data) {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf(
		"node %s did not receive expected log entry: index=%d data=%q",
		node.ID(),
		index,
		data,
	)
}
