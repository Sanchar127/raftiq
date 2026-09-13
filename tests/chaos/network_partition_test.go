package chaos_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

const networkPartitionWaitTimeout = 5 * time.Second

func TestNetworkPartition(t *testing.T) {
	cluster := newNetworkPartitionCluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	initialLeader := cluster.waitForLeader(networkPartitionWaitTimeout)
	if initialLeader == nil {
		t.Fatal("cluster failed to elect an initial leader")
	}

	initialTerm := initialLeader.State().Persistent.CurrentTerm
	initialLeaderID := initialLeader.ID()

	t.Logf(
		"initial leader elected: node=%s term=%d",
		initialLeaderID,
		initialTerm,
	)

	// Establish a committed entry before the partition. This gives the
	// cluster a common log prefix that must survive the partition.
	baselineData := []byte("before-network-partition")

	baselineIndex, err := initialLeader.Propose(baselineData)
	if err != nil {
		t.Fatalf("baseline proposal failed: %v", err)
	}

	cluster.waitForLogEntry(
		t,
		networkPartitionWaitTimeout,
		baselineIndex,
		baselineData,
	)

	t.Logf(
		"baseline entry committed: index=%d term=%d",
		baselineIndex,
		initialTerm,
	)

	// Isolate the current leader from both followers.
	//
	// Both directions are blocked because:
	//   leader -> follower
	// is required for replication, while:
	//   follower -> leader
	// is required for replies and elections.
	for _, node := range cluster.nodes {
		if node.ID() == initialLeaderID {
			continue
		}

		cluster.transport.BlockBidirectional(
			initialLeaderID,
			node.ID(),
		)
	}

	t.Logf(
		"network partition created: isolated leader=%s",
		initialLeaderID,
	)

	// The isolated leader can still append locally, but it cannot replicate
	// the entry to a majority. Therefore the entry must remain uncommitted.
	isolatedData := []byte("isolated-uncommitted")

	isolatedIndex, err := initialLeader.Propose(isolatedData)
	if err != nil {
		t.Fatalf(
			"isolated leader proposal unexpectedly failed: %v",
			err,
		)
	}

	t.Logf(
		"isolated leader accepted local proposal: node=%s index=%d term=%d",
		initialLeaderID,
		isolatedIndex,
		initialTerm,
	)

	time.Sleep(250 * time.Millisecond)

	isolatedState := initialLeader.State()

	if isolatedState.Volatile.CommitIndex >= isolatedIndex {
		t.Fatalf(
			"isolated leader committed entry without majority: node=%s commit_index=%d isolated_index=%d",
			initialLeaderID,
			isolatedState.Volatile.CommitIndex,
			isolatedIndex,
		)
	}

	// The two followers still have a majority and can communicate with
	// each other, so they must elect a new leader in a higher term.
	majorityLeader := cluster.waitForMajorityLeader(
		initialLeaderID,
		initialTerm,
		networkPartitionWaitTimeout,
	)

	if majorityLeader == nil {
		t.Fatal("majority partition failed to elect a new leader")
	}

	majorityTerm := majorityLeader.State().Persistent.CurrentTerm

	if majorityTerm <= initialTerm {
		t.Fatalf(
			"majority leader term did not advance: old=%d new=%d",
			initialTerm,
			majorityTerm,
		)
	}

	if majorityLeader.ID() == initialLeaderID {
		t.Fatalf(
			"isolated leader remained majority leader: node=%s",
			initialLeaderID,
		)
	}

	t.Logf(
		"majority leader elected: node=%s term=%d",
		majorityLeader.ID(),
		majorityTerm,
	)

	// The majority leader must still be able to make progress while the
	// old leader is isolated.
	majorityData := []byte("majority-committed")

	majorityIndex, err := majorityLeader.Propose(majorityData)
	if err != nil {
		t.Fatalf(
			"majority leader proposal failed: %v",
			err,
		)
	}

	t.Logf(
		"majority proposal accepted: leader=%s index=%d term=%d",
		majorityLeader.ID(),
		majorityIndex,
		majorityTerm,
	)

	// The entry must exist on both nodes forming the majority.
	cluster.waitForLogEntry(
		t,
		networkPartitionWaitTimeout,
		majorityIndex,
		majorityData,
	)

	majorityFollower := cluster.otherMajorityNode(
		initialLeaderID,
		majorityLeader.ID(),
	)

	if majorityFollower == nil {
		t.Fatal("failed to identify majority follower")
	}

	cluster.waitForNodeLogEntry(
		t,
		majorityFollower,
		networkPartitionWaitTimeout,
		majorityIndex,
		majorityData,
	)

	majorityState := majorityLeader.State()

	if majorityState.Volatile.CommitIndex < majorityIndex {
		t.Fatalf(
			"majority leader failed to commit entry: leader=%s commit_index=%d entry_index=%d",
			majorityLeader.ID(),
			majorityState.Volatile.CommitIndex,
			majorityIndex,
		)
	}

	t.Logf(
		"majority committed entry: leader=%s index=%d",
		majorityLeader.ID(),
		majorityIndex,
	)

	// Heal every blocked link.
	for _, node := range cluster.nodes {
		if node.ID() == initialLeaderID {
			continue
		}

		cluster.transport.UnblockBidirectional(
			initialLeaderID,
			node.ID(),
		)
	}

	t.Logf("network partition healed")

	// Once communication resumes, the old leader must discover the higher
	// term and step down.
	if !cluster.waitForFollower(
		initialLeader,
		majorityTerm,
		networkPartitionWaitTimeout,
	) {
		state := initialLeader.State()

		t.Fatalf(
			"isolated leader did not step down after partition healing: node=%s role=%v term=%d expected_term>=%d",
			initialLeader.ID(),
			state.Role,
			state.Persistent.CurrentTerm,
			majorityTerm,
		)
	}

	t.Logf(
		"old leader stepped down: node=%s term=%d",
		initialLeader.ID(),
		initialLeader.State().Persistent.CurrentTerm,
	)

	// The committed entry from the majority partition must eventually
	// converge onto the former leader as well.
	cluster.waitForNodeLogEntry(
		t,
		initialLeader,
		networkPartitionWaitTimeout,
		majorityIndex,
		majorityData,
	)

	// The isolated proposal must not survive as an authoritative conflicting
	// suffix after the cluster converges.
	if entry, ok := initialLeader.Log().Get(isolatedIndex); ok &&
		string(entry.Data) == string(isolatedData) {
		t.Fatalf(
			"isolated uncommitted entry survived log convergence: node=%s index=%d data=%q",
			initialLeader.ID(),
			isolatedIndex,
			isolatedData,
		)
	}

	// Verify that all three nodes eventually contain the same log.
	cluster.waitForLogConvergence(
		t,
		networkPartitionWaitTimeout,
	)

	cluster.assertSingleLeaderPerTerm(t)

	t.Logf(
		"network partition recovered successfully: initial_leader=%s majority_leader=%s initial_term=%d majority_term=%d",
		initialLeaderID,
		majorityLeader.ID(),
		initialTerm,
		majorityTerm,
	)
}

type networkPartitionCluster struct {
	transport *raft.LocalTransport
	nodes     []*raft.RaftNode
}

func newNetworkPartitionCluster(t *testing.T) *networkPartitionCluster {
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

	// Stagger election timeouts so the test does not depend on perfectly
	// synchronized election timers avoiding split votes.
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

	return &networkPartitionCluster{
		transport: transport,
		nodes:     nodes,
	}
}

func (c *networkPartitionCluster) start() {
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

func (c *networkPartitionCluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}

	c.transport.Close()
}

func (c *networkPartitionCluster) waitForLeader(
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

func (c *networkPartitionCluster) waitForMajorityLeader(
	isolatedLeaderID raft.NodeID,
	oldTerm raft.Term,
	timeout time.Duration,
) *raft.RaftNode {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leader *raft.RaftNode

		for _, node := range c.nodes {
			if node.ID() == isolatedLeaderID {
				continue
			}

			state := node.State()

			if state.Role != raft.Leader ||
				state.Persistent.CurrentTerm <= oldTerm {
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

func (c *networkPartitionCluster) waitForLogEntry(
	t *testing.T,
	timeout time.Duration,
	index raft.LogIndex,
	data []byte,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			entry, ok := node.Log().Get(index)
			if ok && string(entry.Data) == string(data) {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf(
		"timed out waiting for log entry: index=%d data=%q",
		index,
		data,
	)
}

func (c *networkPartitionCluster) waitForNodeLogEntry(
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

func (c *networkPartitionCluster) waitForFollower(
	node *raft.RaftNode,
	minTerm raft.Term,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		state := node.State()

		if state.Role == raft.Follower &&
			state.Persistent.CurrentTerm >= minTerm {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}

func (c *networkPartitionCluster) otherMajorityNode(
	isolatedLeaderID raft.NodeID,
	majorityLeaderID raft.NodeID,
) *raft.RaftNode {
	for _, node := range c.nodes {
		if node.ID() == isolatedLeaderID ||
			node.ID() == majorityLeaderID {
			continue
		}

		return node
	}

	return nil
}

func (c *networkPartitionCluster) waitForLogConvergence(
	t *testing.T,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if c.logsConverged() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("cluster logs did not converge after partition healing")
}

func (c *networkPartitionCluster) logsConverged() bool {
	first := c.nodes[0].Log()
	firstLastIndex := first.LastIndex()

	for _, node := range c.nodes[1:] {
		log := node.Log()

		if log.LastIndex() != firstLastIndex {
			return false
		}

		for index := raft.LogIndex(1); index <= firstLastIndex; index++ {
			firstEntry, firstOK := first.Get(index)
			entry, ok := log.Get(index)

			if firstOK != ok {
				return false
			}

			if !firstOK {
				continue
			}

			if firstEntry.Term != entry.Term ||
				string(firstEntry.Data) != string(entry.Data) {
				return false
			}
		}
	}

	return true
}

func (c *networkPartitionCluster) assertSingleLeaderPerTerm(
	t *testing.T,
) {
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

func TestAsymmetricNetworkPartition(t *testing.T) {
	cluster := newNetworkPartitionCluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	initialLeader := cluster.waitForLeader(networkPartitionWaitTimeout)
	if initialLeader == nil {
		t.Fatal("cluster failed to elect an initial leader")
	}

	leaderID := initialLeader.ID()
	initialTerm := initialLeader.State().Persistent.CurrentTerm

	var asymmetricFollower *raft.RaftNode
	var healthyFollower *raft.RaftNode

	for _, node := range cluster.nodes {
		if node.ID() == leaderID {
			continue
		}

		if asymmetricFollower == nil {
			asymmetricFollower = node
			continue
		}

		healthyFollower = node
	}

	if asymmetricFollower == nil || healthyFollower == nil {
		t.Fatal("failed to identify follower nodes")
	}

	// Create a genuinely asymmetric network condition:
	//
	//   leader -> asymmetricFollower : ALLOWED
	//   asymmetricFollower -> leader : BLOCKED
	//
	// Communication between the leader and the healthy follower remains
	// bidirectional, so the leader still has a majority.
	cluster.transport.Block(
		asymmetricFollower.ID(),
		leaderID,
	)

	t.Cleanup(func() {
		cluster.transport.Unblock(
			asymmetricFollower.ID(),
			leaderID,
		)
	})

	if cluster.transport.IsBlocked(
		leaderID,
		asymmetricFollower.ID(),
	) {
		t.Fatal("leader -> asymmetric follower must remain available")
	}

	if !cluster.transport.IsBlocked(
		asymmetricFollower.ID(),
		leaderID,
	) {
		t.Fatal("asymmetric follower -> leader link was not blocked")
	}

	t.Logf(
		"asymmetric partition created: %s -> %s allowed, %s -> %s blocked",
		leaderID,
		asymmetricFollower.ID(),
		asymmetricFollower.ID(),
		leaderID,
	)

	// The leader must remain able to communicate with the healthy follower
	// and therefore retain its majority.
	testData := []byte("asymmetric-partition-majority-progress")

	index, err := initialLeader.Propose(testData)
	if err != nil {
		t.Fatalf(
			"leader proposal failed during asymmetric partition: %v",
			err,
		)
	}

	cluster.waitForNodeLogEntry(
		t,
		healthyFollower,
		networkPartitionWaitTimeout,
		index,
		testData,
	)

	leaderState := initialLeader.State()

	if leaderState.Role != raft.Leader {
		t.Fatalf(
			"leader lost leadership despite retaining majority: node=%s role=%v term=%d",
			leaderID,
			leaderState.Role,
			leaderState.Persistent.CurrentTerm,
		)
	}

	if leaderState.Persistent.CurrentTerm != initialTerm {
		t.Fatalf(
			"leader term changed unexpectedly: node=%s initial_term=%d current_term=%d",
			leaderID,
			initialTerm,
			leaderState.Persistent.CurrentTerm,
		)
	}

	if leaderState.Volatile.CommitIndex < index {
		t.Fatalf(
			"majority entry was not committed: leader=%s commit_index=%d entry_index=%d",
			leaderID,
			leaderState.Volatile.CommitIndex,
			index,
		)
	}

	// A -> B remains available, so the asymmetric follower continues
	// receiving heartbeats. It must therefore remain a follower instead of
	// repeatedly becoming a candidate.
	observationDeadline := time.Now().Add(750 * time.Millisecond)

	for time.Now().Before(observationDeadline) {
		state := asymmetricFollower.State()

		if state.Role != raft.Follower {
			t.Fatalf(
				"asymmetric follower entered election loop: node=%s role=%v term=%d initial_term=%d",
				asymmetricFollower.ID(),
				state.Role,
				state.Persistent.CurrentTerm,
				initialTerm,
			)
		}

		if state.Persistent.CurrentTerm != initialTerm {
			t.Fatalf(
				"asymmetric follower advanced term despite receiving leader heartbeats: node=%s initial_term=%d current_term=%d",
				asymmetricFollower.ID(),
				initialTerm,
				state.Persistent.CurrentTerm,
			)
		}

		time.Sleep(10 * time.Millisecond)
	}

	// There must never be another leader in the same term.
	cluster.assertSingleLeaderPerTerm(t)

	leaderState = initialLeader.State()

	if leaderState.Role != raft.Leader ||
		leaderState.Persistent.CurrentTerm != initialTerm {
		t.Fatalf(
			"asymmetric partition caused leadership instability: leader=%s role=%v term=%d expected_term=%d",
			leaderID,
			leaderState.Role,
			leaderState.Persistent.CurrentTerm,
			initialTerm,
		)
	}

	t.Logf(
		"asymmetric partition remained stable: leader=%s term=%d follower=%s healthy_follower=%s",
		leaderID,
		initialTerm,
		asymmetricFollower.ID(),
		healthyFollower.ID(),
	)

	// Heal the one-way partition and verify that communication remains healthy.
	cluster.transport.Unblock(
		asymmetricFollower.ID(),
		leaderID,
	)

	if cluster.transport.IsBlocked(
		asymmetricFollower.ID(),
		leaderID,
	) {
		t.Fatal("asymmetric partition did not heal")
	}

	if cluster.transport.IsBlocked(
		leaderID,
		asymmetricFollower.ID(),
	) {
		t.Fatal("leader -> follower link unexpectedly became blocked")
	}

	// The cluster should remain stable after healing.
	if !cluster.waitForFollower(
		asymmetricFollower,
		initialTerm,
		networkPartitionWaitTimeout,
	) {
		state := asymmetricFollower.State()

		t.Fatalf(
			"asymmetric follower did not remain follower after healing: node=%s role=%v term=%d",
			asymmetricFollower.ID(),
			state.Role,
			state.Persistent.CurrentTerm,
		)
	}

	cluster.assertSingleLeaderPerTerm(t)

	t.Logf(
		"asymmetric network partition recovered successfully: leader=%s term=%d",
		leaderID,
		initialTerm,
	)
}
