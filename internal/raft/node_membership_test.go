package raft

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
)

func TestProposeConfigurationDoesNotActivateBeforeCommit(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node A membership: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node B membership: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node C membership: %v", err)
	}

	nodeA.mu.Lock()
	nodeA.state.Role = Leader
	nodeA.state.Persistent.CurrentTerm = 1
	nodeA.mu.Unlock()

	before := nodeA.State().Persistent.Membership

	nextConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
			"D",
		},
	}

	index, err := nodeA.ProposeConfiguration(nextConfiguration)
	if err != nil {
		t.Fatalf("propose configuration: %v", err)
	}

	state := nodeA.State()

	if state.Volatile.CommitIndex >= index {
		t.Fatalf(
			"configuration entry should not be committed yet: commit=%d index=%d",
			state.Volatile.CommitIndex,
			index,
		)
	}

	if !reflect.DeepEqual(state.Persistent.Membership, before) {
		t.Fatalf(
			"uncommitted configuration changed membership: before=%+v after=%+v",
			before,
			state.Persistent.Membership,
		)
	}
}

func TestCommittedConfigurationActivates(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership A: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership B: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership C: %v", err)
	}

	nodeA.runElection()

	state := nodeA.State()

	if state.Role != Leader {
		t.Fatalf(
			"expected A to become Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != "A" {
		t.Fatalf(
			"expected leader A, got %q",
			state.LeaderID,
		)
	}

	nextConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
			"D",
		},
	}

	index, err := nodeA.ProposeConfiguration(nextConfiguration)
	if err != nil {
		t.Fatalf("propose configuration: %v", err)
	}

	state = nodeA.State()

	if state.Volatile.CommitIndex < index {
		t.Fatalf(
			"configuration entry should be committed: commit=%d index=%d",
			state.Volatile.CommitIndex,
			index,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current,
		nextConfiguration,
	) {
		entry, ok := nodeA.log.Get(index)

		t.Fatalf(
			"committed configuration was not activated: "+
				"expected=%+v actual=%+v "+
				"commit=%d last_applied=%d index=%d entry_exists=%t is_config=%t entry_data=%x",
			nextConfiguration,
			state.Persistent.Membership.Current,
			state.Volatile.CommitIndex,
			state.Volatile.LastApplied,
			index,
			ok,
			ok && IsConfigurationEntry(entry.Data),
			func() []byte {
				if !ok {
					return nil
				}
				return entry.Data
			}(),
		)
	}

	if state.Persistent.Membership.Joint != nil {
		t.Fatalf(
			"stable configuration unexpectedly has joint membership: %+v",
			state.Persistent.Membership.Joint,
		)
	}

	if state.Volatile.LastApplied < index {
		t.Fatalf(
			"configuration entry was committed but not applied: last_applied=%d index=%d",
			state.Volatile.LastApplied,
			index,
		)
	}
}

func TestJointConfigurationTransition(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if err := nodeA.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node A membership: %v", err)
	}

	if err := nodeB.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node B membership: %v", err)
	}

	if err := nodeC.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap node C membership: %v", err)
	}

	nodeA.runElection()

	state := nodeA.State()

	if state.Role != Leader {
		t.Fatalf("expected A to become Leader, got %v", state.Role)
	}

	oldConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
		},
	}

	newConfiguration := model.Configuration{
		Voters: []model.NodeID{
			"A",
			"B",
			"C",
			"D",
		},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	enterJointIndex, err := nodeA.Propose(enterJointData)
	if err != nil {
		t.Fatalf("propose enter-joint configuration: %v", err)
	}

	state = nodeA.State()

	if state.Volatile.CommitIndex < enterJointIndex {
		t.Fatalf(
			"enter-joint configuration should be committed: commit=%d index=%d",
			state.Volatile.CommitIndex,
			enterJointIndex,
		)
	}

	joint := state.Persistent.Membership.Joint

	if joint == nil {
		t.Fatal("expected joint membership after EnterJoint")
	}

	if !reflect.DeepEqual(joint.Old, oldConfiguration) {
		t.Fatalf(
			"unexpected joint old configuration: expected=%+v actual=%+v",
			oldConfiguration,
			joint.Old,
		)
	}

	if !reflect.DeepEqual(joint.New, newConfiguration) {
		t.Fatalf(
			"unexpected joint new configuration: expected=%+v actual=%+v",
			newConfiguration,
			joint.New,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current,
		oldConfiguration,
	) {
		t.Fatalf(
			"unexpected current configuration during joint consensus: expected=%+v actual=%+v",
			oldConfiguration,
			state.Persistent.Membership.Current,
		)
	}

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	leaveJointIndex, err := nodeA.Propose(leaveJointData)
	if err != nil {
		t.Fatalf("propose leave-joint configuration: %v", err)
	}

	state = nodeA.State()

	if state.Volatile.CommitIndex < leaveJointIndex {
		t.Fatalf(
			"leave-joint configuration should be committed: commit=%d index=%d",
			state.Volatile.CommitIndex,
			leaveJointIndex,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current,
		newConfiguration,
	) {
		t.Fatalf(
			"expected final configuration %v, got %v",
			newConfiguration,
			state.Persistent.Membership.Current,
		)
	}

	if state.Persistent.Membership.Joint != nil {
		t.Fatalf(
			"expected joint membership to be cleared, got %+v",
			state.Persistent.Membership.Joint,
		)
	}

	if state.Volatile.LastApplied < leaveJointIndex {
		t.Fatalf(
			"leave-joint configuration was committed but not applied: last_applied=%d index=%d",
			state.Volatile.LastApplied,
			leaveJointIndex,
		)
	}
}

func TestRegisterPeer(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	if len(node.peerIDs) != 1 {
		t.Fatalf(
			"expected 1 peer, got %d",
			len(node.peerIDs),
		)
	}

	if node.peerIDs[0] != "D" {
		t.Fatalf(
			"expected peer D, got %q",
			node.peerIDs[0],
		)
	}
}

func TestRegisterPeerRejectsDuplicate(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer: %v", err)
	}

	if err := node.RegisterPeer("D"); err == nil {
		t.Fatal("expected duplicate peer registration to fail")
	}
}

func TestRegisterPeerRejectsSelf(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.RegisterPeer("A"); err == nil {
		t.Fatal("expected self registration to fail")
	}
}

func TestNewPeerReachableBeforeMembershipChange(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeD := NewRaftNode("D")

	transport := NewLocalTransport()

	if err := transport.AddNode(nodeD); err != nil {
		t.Fatalf("add node D to transport: %v", err)
	}

	if err := nodeA.SetTransport(
		transport,
		[]NodeID{},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := nodeA.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer D: %v", err)
	}

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     "A",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	}

	ctx := context.Background()

	reply, err := transport.AppendEntries(
		ctx,
		"D",
		args,
	)
	if err != nil {
		t.Fatalf("append entries to new peer D: %v", err)
	}

	if reply.Term != 1 {
		t.Fatalf(
			"expected D reply term 1, got %d",
			reply.Term,
		)
	}
}

func TestCatchUpPeerReplicatesExistingLog(t *testing.T) {
	leader := NewRaftNode("A")
	newPeer := NewRaftNode("D")

	transport := NewLocalTransport()

	if err := transport.AddNode(newPeer); err != nil {
		t.Fatalf("add new peer D to transport: %v", err)
	}

	if err := leader.SetTransport(
		transport,
		[]NodeID{},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := leader.RegisterPeer("D"); err != nil {
		t.Fatalf("register peer D: %v", err)
	}

	entries := []LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("entry-1"),
		},
		{
			Index: 2,
			Term:  1,
			Data:  []byte("entry-2"),
		},
		{
			Index: 3,
			Term:  1,
			Data:  []byte("entry-3"),
		},
	}

	for _, entry := range entries {
		if err := leader.log.Append(entry); err != nil {
			t.Fatalf(
				"append leader entry %d: %v",
				entry.Index,
				err,
			)
		}
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	leader.initializeNewPeerReplicationStateLocked("D")

	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	if err := leader.catchUpPeer(ctx, "D", 3); err != nil {
		t.Fatalf("catch up peer D: %v", err)
	}

	if got := newPeer.log.LastIndex(); got != 3 {
		t.Fatalf(
			"expected peer D last index 3, got %d",
			got,
		)
	}

	for _, expected := range entries {
		entry, ok := newPeer.log.Get(expected.Index)
		if !ok {
			t.Fatalf(
				"peer D missing log entry %d",
				expected.Index,
			)
		}

		if entry.Term != expected.Term {
			t.Fatalf(
				"entry %d: expected term %d, got %d",
				expected.Index,
				expected.Term,
				entry.Term,
			)
		}

		if string(entry.Data) != string(expected.Data) {
			t.Fatalf(
				"entry %d: expected data %q, got %q",
				expected.Index,
				expected.Data,
				entry.Data,
			)
		}
	}

	leader.mu.RLock()
	matchIndex := leader.state.Leader.MatchIndex["D"]
	nextIndex := leader.state.Leader.NextIndex["D"]
	leader.mu.RUnlock()

	if matchIndex != 3 {
		t.Fatalf(
			"expected leader MatchIndex[D]=3, got %d",
			matchIndex,
		)
	}

	if nextIndex != 4 {
		t.Fatalf(
			"expected leader NextIndex[D]=4, got %d",
			nextIndex,
		)
	}
}

func TestWaitForApplied(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Volatile.LastApplied = 3
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	if err := node.waitForApplied(ctx, 3); err != nil {
		t.Fatalf("wait for applied: %v", err)
	}
}

func TestAddMember(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	newPeer := NewRaftNode("D")

	transport := NewLocalTransport()

	if err := transport.AddNode(leader); err != nil {
		t.Fatalf("add leader A to transport: %v", err)
	}

	if err := transport.AddNode(peerB); err != nil {
		t.Fatalf("add B to transport: %v", err)
	}

	if err := transport.AddNode(peerC); err != nil {
		t.Fatalf("add C to transport: %v", err)
	}

	if err := transport.AddNode(newPeer); err != nil {
		t.Fatalf("add D to transport: %v", err)
	}

	// Bootstrap the original cluster as A,B,C.
	// D is reachable through the transport but is not yet
	// registered as a Raft peer or voter.
	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		transport,
		[]NodeID{"A", "C"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		transport,
		[]NodeID{"A", "B"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := newPeer.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	// D becomes a communication peer only after the initial
	// membership has been established.
	if err := leader.RegisterPeer("D"); err != nil {
		t.Fatalf("register D: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.initializeNewPeerReplicationStateLocked("D")

	leader.mu.Unlock()

	entries := []LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("entry-1"),
		},
		{
			Index: 2,
			Term:  1,
			Data:  []byte("entry-2"),
		},
		{
			Index: 3,
			Term:  1,
			Data:  []byte("entry-3"),
		},
	}

	for _, entry := range entries {
		if err := leader.log.Append(entry); err != nil {
			t.Fatalf(
				"append leader entry %d: %v",
				entry.Index,
				err,
			)
		}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := leader.AddMember(ctx, "D"); err != nil {
		t.Fatalf("add member D: %v", err)
	}

	state := leader.State()

	if state.Persistent.Membership.Joint != nil {
		t.Fatal("expected final membership to be stable")
	}

	expectedVoters := []NodeID{
		"A",
		"B",
		"C",
		"D",
	}

	if len(state.Persistent.Membership.Current.Voters) !=
		len(expectedVoters) {
		t.Fatalf(
			"expected %d voters, got %d",
			len(expectedVoters),
			len(state.Persistent.Membership.Current.Voters),
		)
	}

	for _, expectedID := range expectedVoters {
		if !configurationContainsVoter(
			state.Persistent.Membership.Current,
			expectedID,
		) {
			t.Fatalf(
				"expected voter %s in final membership",
				expectedID,
			)
		}
	}

	if got := newPeer.log.LastIndex(); got < 3 {
		t.Fatalf(
			"expected new peer D last index >= 3, got %d",
			got,
		)
	}

	for _, expected := range entries {
		entry, ok := newPeer.log.Get(expected.Index)
		if !ok {
			t.Fatalf(
				"new peer D missing log entry %d",
				expected.Index,
			)
		}

		if entry.Term != expected.Term {
			t.Fatalf(
				"entry %d: expected term %d, got %d",
				expected.Index,
				expected.Term,
				entry.Term,
			)
		}

		if string(entry.Data) != string(expected.Data) {
			t.Fatalf(
				"entry %d: expected data %q, got %q",
				expected.Index,
				expected.Data,
				entry.Data,
			)
		}
	}

	leader.mu.RLock()
	matchIndex := leader.state.Leader.MatchIndex["D"]
	nextIndex := leader.state.Leader.NextIndex["D"]
	leader.mu.RUnlock()

	if matchIndex < 3 {
		t.Fatalf(
			"expected MatchIndex[D] >= 3, got %d",
			matchIndex,
		)
	}

	if nextIndex < 4 {
		t.Fatalf(
			"expected NextIndex[D] >= 4, got %d",
			nextIndex,
		)
	}
}

func TestRemoveMember(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	peerD := NewRaftNode("D")

	transport := NewLocalTransport()

	// Add all nodes to the transport.
	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		peerD,
	} {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	// Existing cluster is A,B,C,D.
	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C", "D"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		transport,
		[]NodeID{"A", "C", "D"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		transport,
		[]NodeID{"A", "B", "D"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := peerD.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C", "D"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	if err := leader.RemoveMember(ctx, "D"); err != nil {
		t.Fatalf("remove member D: %v", err)
	}

	state := leader.State()

	// Final membership must be stable.
	if state.Persistent.Membership.Joint != nil {
		t.Fatal("expected final membership to be stable")
	}

	// Final membership must contain A,B,C.
	expectedVoters := []NodeID{
		"A",
		"B",
		"C",
	}

	if len(state.Persistent.Membership.Current.Voters) !=
		len(expectedVoters) {
		t.Fatalf(
			"expected %d voters, got %d",
			len(expectedVoters),
			len(state.Persistent.Membership.Current.Voters),
		)
	}

	for _, expectedID := range expectedVoters {
		if !configurationContainsVoter(
			state.Persistent.Membership.Current,
			expectedID,
		) {
			t.Fatalf(
				"expected voter %s in final membership",
				expectedID,
			)
		}
	}

	// D must no longer be a voter.
	if configurationContainsVoter(
		state.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal("expected D to be removed from final membership")
	}

	// The final membership must exactly match A,B,C.
	for _, voterID := range state.Persistent.Membership.Current.Voters {
		if voterID == "D" {
			t.Fatal("removed member D is still present")
		}
	}
}

func TestRemoveMemberRejectsNonVoter(t *testing.T) {
	node := NewRaftNode("A")

	peer := NewRaftNode("B")
	transport := NewLocalTransport()

	if err := transport.AddNode(node); err != nil {
		t.Fatalf("add A: %v", err)
	}

	if err := transport.AddNode(peer); err != nil {
		t.Fatalf("add B: %v", err)
	}

	if err := node.SetTransport(
		transport,
		[]NodeID{"B"},
	); err != nil {
		t.Fatalf("set transport: %v", err)
	}

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "C")
	if err == nil {
		t.Fatal("expected removing non-voter to fail")
	}

	if !strings.Contains(err.Error(), "is not a voter") {
		t.Fatalf(
			"expected non-voter error, got %v",
			err,
		)
	}
}

func TestRemoveMemberRejectsJointConfiguration(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()

	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C"},
		},
		Joint: &model.JointConfiguration{
			Old: model.Configuration{
				Voters: []NodeID{"A", "B", "C"},
			},
			New: model.Configuration{
				Voters: []NodeID{"A", "B"},
			},
		},
	}

	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "C")
	if err == nil {
		t.Fatal("expected removal during joint configuration to fail")
	}

	if !strings.Contains(err.Error(), "joint configuration") {
		t.Fatalf(
			"expected joint-configuration error, got %v",
			err,
		)
	}
}

func TestRemoveMemberRejectsFollower(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C"},
		},
	}
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "B")
	if err == nil {
		t.Fatal("expected follower removal to fail")
	}

	if !strings.Contains(err.Error(), "not leader") {
		t.Fatalf(
			"expected not-leader error, got %v",
			err,
		)
	}
}

func TestRemoveMemberRejectsLeaderSelfRemoval(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C"},
		},
	}
	node.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := node.RemoveMember(ctx, "A")
	if err == nil {
		t.Fatal("expected self-removal to be rejected")
	}

	if !strings.Contains(err.Error(), "transfer leadership first") {
		t.Fatalf(
			"expected leadership-transfer error, got %v",
			err,
		)
	}
}

func TestLeaderStepsDownAfterSelfRemoval(t *testing.T) {
	node := NewRaftNode("A")

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"B", "C"},
	}

	node.mu.Lock()

	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: oldConfiguration,
		Joint:   nil,
	}

	node.mu.Unlock()

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	node.mu.Lock()

	enterJointEntry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  enterJointData,
	}

	if err := node.applyConfigurationEntryLocked(
		enterJointEntry,
	); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply enter-joint configuration: %v", err)
	}

	node.mu.Unlock()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf(
			"expected leader to remain leader during joint configuration, got %v",
			state.Role,
		)
	}

	if state.Persistent.Membership.Joint == nil {
		t.Fatal("expected membership to be joint")
	}

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	node.mu.Lock()

	leaveJointEntry := LogEntry{
		Index: 2,
		Term:  1,
		Data:  leaveJointData,
	}

	if err := node.applyConfigurationEntryLocked(
		leaveJointEntry,
	); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply leave-joint configuration: %v", err)
	}

	node.mu.Unlock()

	state = node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected leader to step down after self-removal, got %v",
			state.Role,
		)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected LeaderID to be cleared after step-down, got %q",
			state.LeaderID,
		)
	}

	if state.Persistent.Membership.Joint != nil {
		t.Fatal("expected final membership to be stable")
	}

	if configurationContainsVoter(
		state.Persistent.Membership.Current,
		"A",
	) {
		t.Fatal("expected A to be removed from final membership")
	}

	if !configurationContainsVoter(
		state.Persistent.Membership.Current,
		"B",
	) {
		t.Fatal("expected B to remain a voter")
	}

	if !configurationContainsVoter(
		state.Persistent.Membership.Current,
		"C",
	) {
		t.Fatal("expected C to remain a voter")
	}
}

func TestRemovedLeaderCannotStartElection(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()

	node.state.Role = Follower
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"B", "C"},
		},
	}

	node.mu.Unlock()

	_, err := node.startElection()
	if err == nil {
		t.Fatal("expected removed leader to be rejected from election")
	}

	if !strings.Contains(err.Error(), "not a voter") {
		t.Fatalf(
			"expected not-a-voter error, got %v",
			err,
		)
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected node to remain follower, got %v",
			state.Role,
		)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term to remain 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}
}

func TestRemovedLeaderCannotPreVote(t *testing.T) {
	node := NewRaftNode("A")

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"B", "C"},
	}

	node.mu.Lock()

	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 1
	node.state.Persistent.Membership = model.Membership{
		Current: oldConfiguration,
	}

	node.mu.Unlock()

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	node.mu.Lock()

	if err := node.applyConfigurationEntryLocked(LogEntry{
		Index: 1,
		Term:  1,
		Data:  enterJointData,
	}); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply enter-joint configuration: %v", err)
	}

	node.mu.Unlock()

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	node.mu.Lock()

	if err := node.applyConfigurationEntryLocked(LogEntry{
		Index: 2,
		Term:  1,
		Data:  leaveJointData,
	}); err != nil {
		node.mu.Unlock()
		t.Fatalf("apply leave-joint configuration: %v", err)
	}

	node.mu.Unlock()

	state := node.State()

	if state.Role != Follower {
		t.Fatalf(
			"expected removed leader to become follower, got %v",
			state.Role,
		)
	}

	reply := node.PreVote(PreVoteArgs{
		Term:         state.Persistent.CurrentTerm + 1,
		CandidateID:  "A",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected removed leader PreVote to be rejected")
	}

	if reply.VoterID != "A" {
		t.Fatalf(
			"expected voter ID A, got %q",
			reply.VoterID,
		)
	}
}

func TestAddMemberFailsSafelyWhenNewPeerBecomesUnreachable(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	newPeer := NewRaftNode("D")

	transport := NewLocalTransport()

	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		newPeer,
	} {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	// D is reachable through the transport, but it is not yet
	// registered as a Raft peer or included in membership.
	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		transport,
		[]NodeID{"A", "C"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		transport,
		[]NodeID{"A", "B"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := newPeer.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	// D becomes a communication peer without becoming a voter.
	if err := leader.RegisterPeer("D"); err != nil {
		t.Fatalf("register D: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.initializeNewPeerReplicationStateLocked("D")

	leader.mu.Unlock()

	// D is unreachable before AddMember starts.
	//
	// The initial catch-up still succeeds because A and D both
	// have an empty log, so D is already caught up to index 0.
	transport.Block("A", "D")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	err := leader.AddMember(ctx, "D")
	if err == nil {
		t.Fatal(
			"expected AddMember to fail when new peer D is unreachable",
		)
	}

	state := leader.State()

	// EnterJoint must have committed using the surviving quorum:
	//
	// Old configuration: A,B,C
	// New configuration: A,B,C,D
	//
	// A,B,C satisfy both quorums, so EnterJoint can commit even
	// though D is unreachable.
	//
	// However, LeaveJoint must not be committed because D cannot
	// be caught up.
	if state.Persistent.Membership.Joint == nil {
		t.Fatal(
			"expected membership to remain joint after AddMember failure",
		)
	}

	joint := state.Persistent.Membership.Joint

	// Verify the old configuration.
	for _, voterID := range []NodeID{"A", "B", "C"} {
		if !configurationContainsVoter(
			joint.Old,
			voterID,
		) {
			t.Fatalf(
				"expected %s in old joint configuration",
				voterID,
			)
		}
	}

	// D must remain in the pending new configuration.
	if !configurationContainsVoter(
		joint.New,
		"D",
	) {
		t.Fatal(
			"expected D to remain in pending new configuration",
		)
	}

	// The stable configuration must still be the old configuration.
	if configurationContainsVoter(
		state.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal(
			"D must not appear in stable current configuration",
		)
	}
}

func TestRemoveMemberFailsSafelyDuringJointConsensus(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	peerD := NewRaftNode("D")

	localTransport := NewLocalTransport()

	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		peerD,
	} {
		if err := localTransport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	// Wrap the local transport so that only LeaveJoint
	// replication to C and D fails.
	transport := &removeMemberFailureTransport{
		LocalTransport: localTransport,
	}

	if err := leader.SetTransport(
		transport,
		[]NodeID{"B", "C", "D"},
	); err != nil {
		t.Fatalf("set leader transport: %v", err)
	}

	if err := peerB.SetTransport(
		localTransport,
		[]NodeID{"A", "C", "D"},
	); err != nil {
		t.Fatalf("set B transport: %v", err)
	}

	if err := peerC.SetTransport(
		localTransport,
		[]NodeID{"A", "B", "D"},
	); err != nil {
		t.Fatalf("set C transport: %v", err)
	}

	if err := peerD.SetTransport(
		localTransport,
		[]NodeID{"A", "B", "C"},
	); err != nil {
		t.Fatalf("set D transport: %v", err)
	}

	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C", "D"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.mu.Unlock()

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Second,
	)
	defer cancel()

	err := leader.RemoveMember(ctx, "D")
	if err == nil {
		t.Fatal(
			"expected RemoveMember to fail when LeaveJoint cannot reach old quorum",
		)
	}

	state := leader.State()

	// EnterJoint must have been committed, but LeaveJoint must
	// not have committed.
	if state.Persistent.Membership.Joint == nil {
		t.Fatal(
			"expected membership to remain joint after RemoveMember failure",
		)
	}

	joint := state.Persistent.Membership.Joint

	// Old configuration must remain A,B,C,D.
	for _, voterID := range []NodeID{"A", "B", "C", "D"} {
		if !configurationContainsVoter(
			joint.Old,
			voterID,
		) {
			t.Fatalf(
				"expected %s in old joint configuration",
				voterID,
			)
		}
	}

	// New configuration must be A,B,C.
	if configurationContainsVoter(
		joint.New,
		"D",
	) {
		t.Fatal(
			"D must not appear in the pending new configuration",
		)
	}

	for _, voterID := range []NodeID{"A", "B", "C"} {
		if !configurationContainsVoter(
			joint.New,
			voterID,
		) {
			t.Fatalf(
				"expected %s in new joint configuration",
				voterID,
			)
		}
	}

	// Stable membership must still contain D because LeaveJoint
	// was never committed.
	if !configurationContainsVoter(
		state.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal(
			"D must remain in stable membership until LeaveJoint commits",
		)
	}
}

func TestLeaderFailureDuringJointConsensus(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")
	peerD := NewRaftNode("D")

	transport := NewLocalTransport()

	for _, node := range []*RaftNode{
		leader,
		peerB,
		peerC,
		peerD,
	} {
		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node to transport: %v", err)
		}
	}

	leader.SetTransport(
		transport,
		[]NodeID{"B", "C", "D"},
	)

	peerB.SetTransport(
		transport,
		[]NodeID{"A", "C", "D"},
	)

	peerC.SetTransport(
		transport,
		[]NodeID{"A", "B", "D"},
	)

	peerD.SetTransport(
		transport,
		[]NodeID{"A", "B", "C"},
	)

	// Bootstrap the initial stable configuration.
	if err := leader.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap leader membership: %v", err)
	}

	// Start A as the leader in term 1.
	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 1

	for _, peerID := range []NodeID{"B", "C", "D"} {
		leader.initializeReplicationStateLocked(peerID)
	}

	leader.mu.Unlock()

	// -----------------------------------------------------------------
	// 1. Enter joint consensus.
	//
	// Old configuration: A B C D
	// New configuration: A B C
	//
	// This represents removing D.
	// -----------------------------------------------------------------

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode enter-joint configuration: %v", err)
	}

	enterJointIndex, err := leader.Propose(enterJointData)
	if err != nil {
		t.Fatalf("propose enter-joint configuration: %v", err)
	}

	if err := leader.waitForApplied(
		context.Background(),
		enterJointIndex,
	); err != nil {
		t.Fatalf(
			"wait for leader to apply enter-joint configuration: %v",
			err,
		)
	}

	leader.mu.RLock()

	if leader.state.Persistent.Membership.Joint == nil {
		leader.mu.RUnlock()

		t.Fatal("leader did not enter joint configuration")
	}

	leader.mu.RUnlock()

	// -----------------------------------------------------------------
	// 2. Make B, C, and D receive the committed joint configuration.
	//
	// This is important for the failure scenario:
	//
	// A dies
	// B becomes candidate
	//
	// Old quorum requires:
	//     3/4 -> B + C + D
	//
	// Therefore C and D must have the same committed joint log entry
	// and must be able to participate in the election.
	// -----------------------------------------------------------------

	for _, peerID := range []NodeID{"B", "C", "D"} {
		heartbeatArgs, ok := leader.buildAppendEntries(peerID)
		if !ok {
			t.Fatalf(
				"failed to build heartbeat for peer %s",
				peerID,
			)
		}

		// We only need the committed entry to reach the follower.
		// The follower will apply it because LeaderCommit is carried
		// by this AppendEntries RPC.
		heartbeatArgs.Entries = nil

		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Second,
		)

		reply, err := transport.AppendEntries(
			ctx,
			peerID,
			heartbeatArgs,
		)

		cancel()

		if err != nil {
			t.Fatalf(
				"replicate committed joint configuration to %s: %v",
				peerID,
				err,
			)
		}

		if !reply.Success {
			t.Fatalf(
				"peer %s rejected committed joint configuration: %+v",
				peerID,
				reply,
			)
		}
	}

	// -----------------------------------------------------------------
	// 3. Verify all surviving voters have the joint configuration.
	// -----------------------------------------------------------------

	for _, node := range []*RaftNode{
		peerB,
		peerC,
		peerD,
	} {
		node.mu.RLock()

		membership := node.state.Persistent.Membership

		if membership.Joint == nil {
			node.mu.RUnlock()

			t.Fatalf(
				"node %s did not apply joint configuration",
				node.id,
			)
		}

		node.mu.RUnlock()
	}

	// -----------------------------------------------------------------
	// 4. Simulate A completely failing.
	//
	// Block both directions so A cannot communicate with B/C/D.
	// -----------------------------------------------------------------

	transport.Block("A", "B")
	transport.Block("A", "C")
	transport.Block("A", "D")

	transport.Block("B", "A")
	transport.Block("C", "A")
	transport.Block("D", "A")

	// Simulate election timeout on surviving nodes.
	for _, node := range []*RaftNode{
		peerB,
		peerC,
		peerD,
	} {
		node.mu.Lock()

		node.state.LeaderID = ""
		node.electionElapsed = node.electionTimeout

		node.mu.Unlock()
	}

	// -----------------------------------------------------------------
	// 5. B must successfully pass PreVote.
	//
	// Joint quorum:
	//
	// Old: B + C + D = 3/4
	// New: B + C     = 2/3
	// -----------------------------------------------------------------

	if !peerB.runPreVote() {
		t.Fatal("B failed PreVote after A failure")
	}

	// -----------------------------------------------------------------
	// 6. B starts a real election.
	// -----------------------------------------------------------------

	if _, err := peerB.startElection(); err != nil {
		t.Fatalf(
			"B failed to start election: %v",
			err,
		)
	}

	peerB.requestVotes()
	peerB.tryBecomeLeader()

	// -----------------------------------------------------------------
	// 7. Verify B became leader while still in joint consensus.
	// -----------------------------------------------------------------

	bState := peerB.State()

	if bState.Role != Leader {
		t.Fatalf(
			"expected B to become leader after A failure, got %v",
			bState.Role,
		)
	}

	if bState.Persistent.Membership.Joint == nil {
		t.Fatal(
			"B became leader without retaining joint configuration",
		)
	}

	// -----------------------------------------------------------------
	// 8. B commits LeaveJoint.
	//
	// Final configuration:
	//     A B C
	//
	// D is removed.
	// -----------------------------------------------------------------

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		t.Fatalf("encode leave-joint configuration: %v", err)
	}

	leaveJointIndex, err := peerB.Propose(leaveJointData)
	if err != nil {
		t.Fatalf(
			"B failed to propose leave-joint configuration: %v",
			err,
		)
	}

	if err := peerB.waitForApplied(
		context.Background(),
		leaveJointIndex,
	); err != nil {
		t.Fatalf(
			"wait for B to apply leave-joint configuration: %v",
			err,
		)
	}

	// -----------------------------------------------------------------
	// 9. Verify final stable configuration.
	// -----------------------------------------------------------------

	bState = peerB.State()

	if bState.Persistent.Membership.Joint != nil {
		t.Fatal(
			"expected B to leave joint configuration",
		)
	}

	expectedVoters := []NodeID{
		"A",
		"B",
		"C",
	}

	actualVoters := bState.Persistent.Membership.Current.Voters

	if !reflect.DeepEqual(actualVoters, expectedVoters) {
		t.Fatalf(
			"unexpected final voters: got %v, want %v",
			actualVoters,
			expectedVoters,
		)
	}

	for _, voterID := range expectedVoters {
		if !configurationContainsVoter(
			bState.Persistent.Membership.Current,
			voterID,
		) {
			t.Fatalf(
				"expected %s to remain a voter",
				voterID,
			)
		}
	}

	if configurationContainsVoter(
		bState.Persistent.Membership.Current,
		"D",
	) {
		t.Fatal("D should have been removed from final configuration")
	}
}

func TestRestartDuringJointConsensus(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	// ---------------------------------------------------------------
	// 1. Create persistent storage and initial node.
	// ---------------------------------------------------------------

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	node, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		store.Close()
		t.Fatalf("create raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 2. Bootstrap the initial stable membership.
	// ---------------------------------------------------------------

	if err := node.BootstrapMembership(); err != nil {
		store.Close()
		t.Fatalf("bootstrap membership: %v", err)
	}

	initialMembership := model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B", "C", "D"},
		},
	}

	node.mu.Lock()

	node.state.Persistent.Membership = initialMembership

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("persist initial membership: %v", err)
	}

	node.mu.Unlock()

	// ---------------------------------------------------------------
	// 3. Build and apply EnterJoint configuration.
	//
	// Old: A B C D
	// New: A B C
	// ---------------------------------------------------------------

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		store.Close()
		t.Fatalf("encode enter-joint entry: %v", err)
	}

	node.mu.Lock()

	entry := model.LogEntry{
		Index: node.log.LastIndex() + 1,
		Term:  1,
		Data:  enterJointData,
	}

	if err := store.AppendEntries([]model.LogEntry{entry}); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry: %v", err)
	}

	if err := store.Sync(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("sync enter-joint entry: %v", err)
	}

	if err := node.log.Append(entry); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry to memory log: %v", err)
	}

	node.state.Volatile.CommitIndex = entry.Index

	node.mu.Unlock()

	node.applyCommitted()

	// ---------------------------------------------------------------
	// 4. Verify node is currently in Joint configuration.
	// ---------------------------------------------------------------

	state := node.State()

	if state.Persistent.Membership.Joint == nil {
		store.Close()
		t.Fatal("expected node to be in joint configuration before restart")
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Joint.Old.Voters,
		oldConfiguration.Voters,
	) {
		store.Close()
		t.Fatalf(
			"unexpected old configuration before restart: got %v, want %v",
			state.Persistent.Membership.Joint.Old.Voters,
			oldConfiguration.Voters,
		)
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Joint.New.Voters,
		newConfiguration.Voters,
	) {
		store.Close()
		t.Fatalf(
			"unexpected new configuration before restart: got %v, want %v",
			state.Persistent.Membership.Joint.New.Voters,
			newConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 5. Simulate crash/restart by closing the WAL and reopening it.
	// ---------------------------------------------------------------

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL before restart: %v", err)
	}

	reopenedStore, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	restored, err := NewRaftNodeWithStorage(
		"A",
		reopenedStore,
	)
	if err != nil {
		reopenedStore.Close()
		t.Fatalf("restore raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 6. Verify the restarted node recovered Joint configuration.
	//
	// It MUST NOT incorrectly recover the new stable configuration.
	// LeaveJoint was never committed.
	// ---------------------------------------------------------------

	restoredState := restored.State()

	if restoredState.Persistent.Membership.Joint == nil {
		reopenedStore.Close()
		t.Fatal(
			"expected restarted node to recover joint configuration",
		)
	}

	restoredJoint := restoredState.Persistent.Membership.Joint

	if !reflect.DeepEqual(
		restoredJoint.Old.Voters,
		oldConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered old configuration: got %v, want %v",
			restoredJoint.Old.Voters,
			oldConfiguration.Voters,
		)
	}

	if !reflect.DeepEqual(
		restoredJoint.New.Voters,
		newConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered new configuration: got %v, want %v",
			restoredJoint.New.Voters,
			newConfiguration.Voters,
		)
	}

	// Current remains the old configuration while joint consensus
	// is active.
	if !reflect.DeepEqual(
		restoredState.Persistent.Membership.Current.Voters,
		oldConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered current configuration: got %v, want %v",
			restoredState.Persistent.Membership.Current.Voters,
			oldConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 7. Verify the restored node is still a voter.
	// ---------------------------------------------------------------

	if !membershipIsVoter(
		restoredState.Persistent.Membership,
		"A",
	) {
		reopenedStore.Close()
		t.Fatal(
			"restored node A should remain a voter during joint consensus",
		)
	}

	if err := reopenedStore.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
}

func TestRestartAfterLeaveJoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	// ---------------------------------------------------------------
	// 1. Create persistent storage and node.
	// ---------------------------------------------------------------

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	node, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		store.Close()
		t.Fatalf("create raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 2. Bootstrap the initial stable configuration.
	// ---------------------------------------------------------------

	initialConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	node.mu.Lock()

	node.state.Persistent.Membership = model.Membership{
		Current: initialConfiguration,
		Joint:   nil,
	}

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("persist initial membership: %v", err)
	}

	node.mu.Unlock()

	// ---------------------------------------------------------------
	// 3. Persist and apply EnterJoint.
	//
	// Old: A B C D
	// New: A B C
	// ---------------------------------------------------------------

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		initialConfiguration,
		newConfiguration,
	)
	if err != nil {
		store.Close()
		t.Fatalf("encode enter-joint entry: %v", err)
	}

	node.mu.Lock()

	enterJointEntry := model.LogEntry{
		Index: node.log.LastIndex() + 1,
		Term:  1,
		Data:  enterJointData,
	}

	if err := store.AppendEntries(
		[]model.LogEntry{enterJointEntry},
	); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry: %v", err)
	}

	if err := store.Sync(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("sync enter-joint entry: %v", err)
	}

	if err := node.log.Append(enterJointEntry); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append enter-joint entry to memory log: %v", err)
	}

	node.state.Volatile.CommitIndex = enterJointEntry.Index

	node.mu.Unlock()

	node.applyCommitted()

	// ---------------------------------------------------------------
	// 4. Verify the node entered Joint configuration.
	// ---------------------------------------------------------------

	state := node.State()

	if state.Persistent.Membership.Joint == nil {
		store.Close()
		t.Fatal("expected node to be in joint configuration")
	}

	// ---------------------------------------------------------------
	// 5. Persist and apply LeaveJoint.
	//
	// Final stable configuration: A B C
	// ---------------------------------------------------------------

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		store.Close()
		t.Fatalf("encode leave-joint entry: %v", err)
	}

	node.mu.Lock()

	leaveJointEntry := model.LogEntry{
		Index: node.log.LastIndex() + 1,
		Term:  1,
		Data:  leaveJointData,
	}

	if err := store.AppendEntries(
		[]model.LogEntry{leaveJointEntry},
	); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append leave-joint entry: %v", err)
	}

	if err := store.Sync(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("sync leave-joint entry: %v", err)
	}

	if err := node.log.Append(leaveJointEntry); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("append leave-joint entry to memory log: %v", err)
	}

	node.state.Volatile.CommitIndex = leaveJointEntry.Index

	node.mu.Unlock()

	node.applyCommitted()

	// ---------------------------------------------------------------
	// 6. Verify the transition completed before restart.
	// ---------------------------------------------------------------

	state = node.State()

	if state.Persistent.Membership.Joint != nil {
		store.Close()
		t.Fatal("expected joint configuration to be cleared")
	}

	if !reflect.DeepEqual(
		state.Persistent.Membership.Current.Voters,
		newConfiguration.Voters,
	) {
		store.Close()
		t.Fatalf(
			"unexpected stable configuration before restart: got %v, want %v",
			state.Persistent.Membership.Current.Voters,
			newConfiguration.Voters,
		)
	}

	if membershipIsVoter(
		state.Persistent.Membership,
		"D",
	) {
		store.Close()
		t.Fatal("D should no longer be a voter before restart")
	}

	// ---------------------------------------------------------------
	// 7. Simulate restart.
	// ---------------------------------------------------------------

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL before restart: %v", err)
	}

	reopenedStore, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	restored, err := NewRaftNodeWithStorage(
		"A",
		reopenedStore,
	)
	if err != nil {
		reopenedStore.Close()
		t.Fatalf("restore raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 8. Verify stable configuration was recovered.
	// ---------------------------------------------------------------

	restoredState := restored.State()

	if restoredState.Persistent.Membership.Joint != nil {
		reopenedStore.Close()
		t.Fatal(
			"restarted node incorrectly recovered joint configuration",
		)
	}

	if !reflect.DeepEqual(
		restoredState.Persistent.Membership.Current.Voters,
		newConfiguration.Voters,
	) {
		reopenedStore.Close()
		t.Fatalf(
			"unexpected recovered stable configuration: got %v, want %v",
			restoredState.Persistent.Membership.Current.Voters,
			newConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 9. Verify D remains removed after restart.
	// ---------------------------------------------------------------

	if membershipIsVoter(
		restoredState.Persistent.Membership,
		"D",
	) {
		reopenedStore.Close()
		t.Fatal(
			"D should not be a voter after restart",
		)
	}

	// ---------------------------------------------------------------
	// 10. Verify A, B, and C remain voters.
	// ---------------------------------------------------------------

	for _, voterID := range []NodeID{"A", "B", "C"} {
		if !membershipIsVoter(
			restoredState.Persistent.Membership,
			voterID,
		) {
			reopenedStore.Close()

			t.Fatalf(
				"%s should remain a voter after restart",
				voterID,
			)
		}
	}

	if err := reopenedStore.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
}

func TestRestartedJointMembershipPreservesQuorumSafety(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	// ---------------------------------------------------------------
	// 1. Create a node with persistent joint membership.
	// ---------------------------------------------------------------

	oldConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C", "D"},
	}

	newConfiguration := model.Configuration{
		Voters: []NodeID{"A", "B", "C"},
	}

	jointMembership := model.Membership{
		Current: oldConfiguration,
		Joint: &model.JointConfiguration{
			Old: oldConfiguration,
			New: newConfiguration,
		},
	}

	node, err := NewRaftNodeWithStorage("B", store)
	if err != nil {
		store.Close()
		t.Fatalf("create raft node: %v", err)
	}

	node.mu.Lock()

	node.state.Persistent.Membership = jointMembership

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		store.Close()
		t.Fatalf("persist joint membership: %v", err)
	}

	node.mu.Unlock()

	// ---------------------------------------------------------------
	// 2. Close and reopen the WAL to simulate restart.
	// ---------------------------------------------------------------

	if err := store.Close(); err != nil {
		t.Fatalf("close WAL before restart: %v", err)
	}

	reopenedStore, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	restored, err := NewRaftNodeWithStorage(
		"B",
		reopenedStore,
	)
	if err != nil {
		reopenedStore.Close()
		t.Fatalf("restore raft node: %v", err)
	}

	// ---------------------------------------------------------------
	// 3. Verify the restarted node recovered Joint configuration.
	// ---------------------------------------------------------------

	state := restored.State()
	membership := state.Persistent.Membership

	if membership.Joint == nil {
		reopenedStore.Close()

		t.Fatal(
			"expected restarted node to recover joint membership",
		)
	}

	if !reflect.DeepEqual(
		membership.Joint.Old.Voters,
		oldConfiguration.Voters,
	) {
		reopenedStore.Close()

		t.Fatalf(
			"unexpected recovered old configuration: got %v, want %v",
			membership.Joint.Old.Voters,
			oldConfiguration.Voters,
		)
	}

	if !reflect.DeepEqual(
		membership.Joint.New.Voters,
		newConfiguration.Voters,
	) {
		reopenedStore.Close()

		t.Fatalf(
			"unexpected recovered new configuration: got %v, want %v",
			membership.Joint.New.Voters,
			newConfiguration.Voters,
		)
	}

	// ---------------------------------------------------------------
	// 4. Verify the restarted local node is still a voter.
	// ---------------------------------------------------------------

	if !membershipIsVoter(membership, "B") {
		reopenedStore.Close()

		t.Fatal(
			"restarted node B should remain a voter",
		)
	}

	// D is still a voter during Joint Consensus because D belongs
	// to the old configuration.
	if !membershipIsVoter(membership, "D") {
		reopenedStore.Close()

		t.Fatal(
			"D should remain a voter during joint consensus",
		)
	}

	// ---------------------------------------------------------------
	// 5. Verify old configuration quorum.
	//
	// B + C = 2/4 -> insufficient.
	// B + C + D = 3/4 -> sufficient.
	// ---------------------------------------------------------------

	oldPartialVotes := map[NodeID]struct{}{
		"B": {},
		"C": {},
	}

	if configurationHasQuorum(
		membership.Joint.Old,
		oldPartialVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C must not satisfy the old configuration quorum",
		)
	}

	oldFullVotes := map[NodeID]struct{}{
		"B": {},
		"C": {},
		"D": {},
	}

	if !configurationHasQuorum(
		membership.Joint.Old,
		oldFullVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C + D must satisfy the old configuration quorum",
		)
	}

	// ---------------------------------------------------------------
	// 6. Verify new configuration quorum.
	//
	// B + C = 2/3 -> sufficient.
	// ---------------------------------------------------------------

	newVotes := map[NodeID]struct{}{
		"B": {},
		"C": {},
	}

	if !configurationHasQuorum(
		membership.Joint.New,
		newVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C must satisfy the new configuration quorum",
		)
	}

	// ---------------------------------------------------------------
	// 7. Verify the actual Joint quorum requires BOTH configurations.
	//
	// B + C:
	//
	// Old = 2/4 -> ❌
	// New = 2/3 -> ✅
	//
	// Therefore joint quorum must be ❌.
	// ---------------------------------------------------------------

	if membershipHasQuorum(
		membership,
		oldPartialVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C must not satisfy joint quorum",
		)
	}

	// ---------------------------------------------------------------
	// 8. B + C + D:
	//
	// Old = 3/4 -> ✅
	// New = 2/3 -> ✅
	//
	// Therefore joint quorum must be ✅.
	// ---------------------------------------------------------------

	if !membershipHasQuorum(
		membership,
		oldFullVotes,
	) {
		reopenedStore.Close()

		t.Fatal(
			"B + C + D must satisfy joint quorum",
		)
	}

	// ---------------------------------------------------------------
	// 9. Verify D is still counted during the old configuration.
	// ---------------------------------------------------------------

	if !configurationContainsVoter(
		membership.Joint.Old,
		"D",
	) {
		reopenedStore.Close()

		t.Fatal(
			"D must remain in the old configuration during joint consensus",
		)
	}

	if configurationContainsVoter(
		membership.Joint.New,
		"D",
	) {
		reopenedStore.Close()

		t.Fatal(
			"D must not be in the new configuration",
		)
	}

	if err := reopenedStore.Close(); err != nil {
		t.Fatalf("close reopened WAL: %v", err)
	}
}
