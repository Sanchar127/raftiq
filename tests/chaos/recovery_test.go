package chaos_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
)

const recoveryWaitTimeout = 5 * time.Second

func TestFollowerRecovery(t *testing.T) {
	cluster := newRecoveryCluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	leader := cluster.waitForLeader(recoveryWaitTimeout)
	if leader == nil {
		t.Fatal("cluster failed to elect an initial leader")
	}

	initialTerm := leader.State().Persistent.CurrentTerm
	leaderID := leader.ID()

	var recoveredID raft.NodeID
	var recoveredNode *raft.RaftNode

	for _, node := range cluster.nodes {
		if node.ID() != leaderID {
			recoveredID = node.ID()
			recoveredNode = node
			break
		}
	}

	if recoveredNode == nil {
		t.Fatal("failed to identify recovery follower")
	}

	t.Logf(
		"initial leader elected: node=%s term=%d recovery_target=%s",
		leaderID,
		initialTerm,
		recoveredID,
	)

	// Commit a baseline entry while all three nodes are healthy. This gives the
	// follower durable state that must still exist after it is restarted.
	baselineData := []byte("recovery-baseline")

	baselineIndex, err := leader.Propose(baselineData)
	if err != nil {
		t.Fatalf("baseline proposal failed: %v", err)
	}

	cluster.waitForLogEntry(
		t,
		leader,
		recoveryWaitTimeout,
		baselineIndex,
		baselineData,
	)

	// Capture the follower's persisted term and log state before failure. The
	// restarted node must reconstruct this state from the same storage.
	recoveredStorage := cluster.storages[recoveredID]

	beforeFailureState := recoveredNode.State()
	beforeFailureLastIndex := recoveredNode.Log().LastIndex()

	if beforeFailureLastIndex < baselineIndex {
		t.Fatalf(
			"recovery follower did not receive baseline entry: node=%s last_index=%d baseline_index=%d",
			recoveredID,
			beforeFailureLastIndex,
			baselineIndex,
		)
	}

	t.Logf(
		"baseline replicated: node=%s term=%d last_index=%d baseline_index=%d",
		recoveredID,
		beforeFailureState.Persistent.CurrentTerm,
		beforeFailureLastIndex,
		baselineIndex,
	)

	// Remove the follower from the transport before stopping it. The storage
	// remains alive, representing durable state surviving a node/process crash.
	cluster.transport.RemoveNode(recoveredID)
	recoveredNode.Stop()

	t.Logf(
		"recovery target stopped: node=%s",
		recoveredID,
	)

	// The leader and the remaining follower still form a majority of the
	// three-node cluster, so the cluster must continue making progress.
	postFailureData := []byte("recovery-post-failure")

	postFailureIndex, err := leader.Propose(postFailureData)
	if err != nil {
		t.Fatalf("post-failure proposal failed: %v", err)
	}

	cluster.waitForLogEntry(
		t,
		leader,
		recoveryWaitTimeout,
		postFailureIndex,
		postFailureData,
	)

	t.Logf(
		"post-failure entry committed: leader=%s index=%d term=%d",
		leaderID,
		postFailureIndex,
		leader.State().Persistent.CurrentTerm,
	)

	// Recreate the failed Raft node using the SAME persistent storage.
	//
	// This is the core of the recovery test: the node object is new, but its
	// durable Raft state belongs to the failed node and must be recovered.
	restarted, err := raft.NewRaftNodeWithStorage(
		recoveredID,
		recoveredStorage,
	)
	if err != nil {
		t.Fatalf(
			"failed to recreate recovered node %s: %v",
			recoveredID,
			err,
		)
	}

	restarted.SetElectionTimeout(
		cluster.electionTimeoutFor(recoveredID),
	)

	if err := restarted.SetTransport(
		cluster.transport,
		peerIDsWithoutSelf(cluster.peerIDs, recoveredID),
	); err != nil {
		t.Fatalf(
			"set transport for recovered node %s: %v",
			recoveredID,
			err,
		)
	}

	// Verify durable state was reconstructed before the node is allowed to
	// communicate with the cluster again.
	recoveredState := restarted.State()
	recoveredLastIndex := restarted.Log().LastIndex()

	if recoveredState.Persistent.CurrentTerm != beforeFailureState.Persistent.CurrentTerm {
		t.Fatalf(
			"recovered term mismatch: node=%s expected=%d actual=%d",
			recoveredID,
			beforeFailureState.Persistent.CurrentTerm,
			recoveredState.Persistent.CurrentTerm,
		)
	}

	if recoveredLastIndex < beforeFailureLastIndex {
		t.Fatalf(
			"recovered log lost durable entries: node=%s expected_at_least=%d actual=%d",
			recoveredID,
			beforeFailureLastIndex,
			recoveredLastIndex,
		)
	}

	if entry, ok := restarted.Log().Get(baselineIndex); !ok ||
		string(entry.Data) != string(baselineData) {
		t.Fatalf(
			"recovered node lost baseline entry: node=%s index=%d",
			recoveredID,
			baselineIndex,
		)
	}

	t.Logf(
		"durable state recovered: node=%s term=%d last_index=%d",
		recoveredID,
		recoveredState.Persistent.CurrentTerm,
		recoveredLastIndex,
	)

	// Replace the failed node in the transport with the newly reconstructed
	// Raft node, then start it. The leader should replicate the missing
	// post-failure entry to it.
	if err := cluster.transport.AddNode(restarted); err != nil {
		t.Fatalf(
			"re-add recovered node %s to transport: %v",
			recoveredID,
			err,
		)
	}

	if err := restarted.Start(); err != nil {
		t.Fatalf(
			"start recovered node %s: %v",
			recoveredID,
			err,
		)
	}

	t.Logf(
		"recovered node restarted: node=%s",
		recoveredID,
	)

	cluster.waitForLogEntry(
		t,
		restarted,
		recoveryWaitTimeout,
		postFailureIndex,
		postFailureData,
	)

	// The recovered node must have caught up with the committed cluster state.
	recoveredFinalState := restarted.State()

	if recoveredFinalState.Volatile.CommitIndex < postFailureIndex {
		t.Fatalf(
			"recovered node did not advance commit index: node=%s commit_index=%d expected_at_least=%d",
			recoveredID,
			recoveredFinalState.Volatile.CommitIndex,
			postFailureIndex,
		)
	}

	if recoveredFinalState.Volatile.LastApplied < postFailureIndex {
		t.Fatalf(
			"recovered node did not apply committed entry: node=%s last_applied=%d expected_at_least=%d",
			recoveredID,
			recoveredFinalState.Volatile.LastApplied,
			postFailureIndex,
		)
	}

	// Verify the recovered node has both the pre-failure and post-failure
	// entries. This proves it retained its durable prefix and caught up with
	// entries committed while it was offline.
	cluster.waitForLogEntry(
		t,
		restarted,
		recoveryWaitTimeout,
		baselineIndex,
		baselineData,
	)

	cluster.waitForLogEntry(
		t,
		restarted,
		recoveryWaitTimeout,
		postFailureIndex,
		postFailureData,
	)

	// The recovered node must not have become a competing leader merely because
	// it restarted. The existing leader remains authoritative in the current
	// term.
	finalLeader := cluster.waitForLeader(recoveryWaitTimeout)
	if finalLeader == nil {
		t.Fatal("cluster failed to maintain a leader after node recovery")
	}

	finalState := finalLeader.State()

	if finalState.Role != raft.Leader {
		t.Fatalf(
			"expected recovered cluster leader, got node=%s role=%v",
			finalLeader.ID(),
			finalState.Role,
		)
	}

	if finalState.Persistent.CurrentTerm < initialTerm {
		t.Fatalf(
			"cluster term regressed after recovery: initial=%d final=%d",
			initialTerm,
			finalState.Persistent.CurrentTerm,
		)
	}

	// Verify the recovered log contains exactly the committed entries we expect
	// at the important indexes.
	assertLogEntry(
		t,
		restarted,
		baselineIndex,
		baselineData,
	)

	assertLogEntry(
		t,
		restarted,
		postFailureIndex,
		postFailureData,
	)

	t.Logf(
		"node recovery successful: recovered=%s leader=%s term=%d baseline_index=%d post_failure_index=%d",
		recoveredID,
		finalLeader.ID(),
		finalState.Persistent.CurrentTerm,
		baselineIndex,
		postFailureIndex,
	)
}

type recoveryCluster struct {
	transport *raft.LocalTransport
	nodes     []*raft.RaftNode
	storages  map[raft.NodeID]*storage.MemoryStorage

	peerIDs          []raft.NodeID
	electionTimeouts map[raft.NodeID]int
}

func newRecoveryCluster(t *testing.T) *recoveryCluster {
	t.Helper()

	transport := raft.NewLocalTransport()

	nodeIDs := []raft.NodeID{
		"node-1",
		"node-2",
		"node-3",
	}

	electionTimeouts := map[raft.NodeID]int{
		"node-1": 10,
		"node-2": 12,
		"node-3": 14,
	}

	storages := make(map[raft.NodeID]*storage.MemoryStorage, len(nodeIDs))
	nodes := make([]*raft.RaftNode, 0, len(nodeIDs))

	for _, id := range nodeIDs {
		store := storage.NewMemoryStorage()

		node, err := raft.NewRaftNodeWithStorage(id, store)
		if err != nil {
			t.Fatalf(
				"create node %s: %v",
				id,
				err,
			)
		}

		node.SetElectionTimeout(electionTimeouts[id])

		storages[id] = store
		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf(
				"add node %s to transport: %v",
				node.ID(),
				err,
			)
		}
	}

	for _, node := range nodes {
		if err := node.SetTransport(
			transport,
			peerIDsWithoutSelf(nodeIDs, node.ID()),
		); err != nil {
			t.Fatalf(
				"set transport for node %s: %v",
				node.ID(),
				err,
			)
		}
	}

	return &recoveryCluster{
		transport:        transport,
		nodes:            nodes,
		storages:         storages,
		peerIDs:          nodeIDs,
		electionTimeouts: electionTimeouts,
	}
}

func (c *recoveryCluster) start() {
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

func (c *recoveryCluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}

	c.transport.Close()
}

func (c *recoveryCluster) electionTimeoutFor(id raft.NodeID) int {
	return c.electionTimeouts[id]
}

func (c *recoveryCluster) waitForLeader(timeout time.Duration) *raft.RaftNode {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leader *raft.RaftNode

		for _, node := range c.nodes {
			state := node.State()

			if state.Role != raft.Leader {
				continue
			}

			if leader != nil && leader.ID() != node.ID() {
				return nil
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

func (c *recoveryCluster) waitForLogEntry(
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

	entry, ok := node.Log().Get(index)
	if !ok {
		t.Fatalf(
			"node %s did not receive log entry: index=%d",
			node.ID(),
			index,
		)
	}

	t.Fatalf(
		"node %s has unexpected log entry: index=%d expected=%q actual=%q",
		node.ID(),
		index,
		data,
		entry.Data,
	)
}

func assertLogEntry(
	t *testing.T,
	node *raft.RaftNode,
	index raft.LogIndex,
	data []byte,
) {
	t.Helper()

	entry, ok := node.Log().Get(index)
	if !ok {
		t.Fatalf(
			"node %s missing log entry: index=%d",
			node.ID(),
			index,
		)
	}

	if string(entry.Data) != string(data) {
		t.Fatalf(
			"node %s has incorrect log entry: index=%d expected=%q actual=%q",
			node.ID(),
			index,
			data,
			entry.Data,
		)
	}
}
