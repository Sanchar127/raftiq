package raft

import (
	"errors"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewRaftNode(t *testing.T) {
	node := NewRaftNode("node-1")

	if node == nil {
		t.Fatal("expected node, got nil")
	}

	if node.ID() != "node-1" {
		t.Fatalf("expected node ID node-1, got %q", node.ID())
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 0 {
		t.Fatalf(
			"expected current term 0, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf("expected no vote, got %q", state.Persistent.VotedFor)
	}

	if state.Volatile.CommitIndex != 0 {
		t.Fatalf(
			"expected commit index 0, got %d",
			state.Volatile.CommitIndex,
		)
	}

	if state.Volatile.LastApplied != 0 {
		t.Fatalf(
			"expected last applied 0, got %d",
			state.Volatile.LastApplied,
		)
	}

	if node.Log() == nil {
		t.Fatal("expected log, got nil")
	}

	if node.Log().LastIndex() != 0 {
		t.Fatalf(
			"expected empty log with last index 0, got %d",
			node.Log().LastIndex(),
		)
	}
}

func TestBecomeLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected leader role, got %v", state.Role)
	}

	if state.LeaderID != "node-1" {
		t.Fatalf(
			"expected leader ID node-1, got %q",
			state.LeaderID,
		)
	}
}

func TestBecomeCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected candidate role, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-1" {
		t.Fatalf(
			"expected vote for node-1, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestBecomeFollower(t *testing.T) {
	node := NewRaftNode("node-1")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()
	if err := node.becomeFollower(2); err != nil {
		t.Fatalf("become follower: %v", err)
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term 2, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected leader ID to be cleared, got %q",
			state.LeaderID,
		)
	}
}

func TestRequestVoteGrantsVote(t *testing.T) {
	node := NewRaftNode("node-1")

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if reply.Term != 1 {
		t.Fatalf("expected term 1, got %d", reply.Term)
	}
}

func TestRequestVoteRejectsSecondCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	first := node.RequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "node-2",
	})

	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := node.RequestVote(RequestVoteArgs{
		Term:        1,
		CandidateID: "node-3",
	})

	if second.VoteGranted {
		t.Fatal("expected second vote to be rejected")
	}
}

func TestRequestVoteRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("node-1")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:        0,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected older-term vote to be rejected")
	}

	if reply.Term != 1 {
		t.Fatalf("expected current term 1, got %d", reply.Term)
	}
}

func TestRequestVoteUpdatesHigherTerm(t *testing.T) {
	node := NewRaftNode("node-1")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:        2,
		CandidateID: "node-2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term 2, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}
}

func TestCandidateVotesForItself(t *testing.T) {
	node := NewRaftNode("A")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if state.Persistent.VotedFor != "A" {
		t.Fatalf("expected A to vote for itself, got %q", state.Persistent.VotedFor)
	}

	if _, ok := state.Election.VotesReceived["A"]; !ok {
		t.Fatal("expected candidate's own vote to be recorded")
	}
}

func TestRecordVote(t *testing.T) {
	node := NewRaftNode("A")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	recorded := node.recordVote("B", term, true)

	if !recorded {
		t.Fatal("expected vote to be recorded")
	}

	state := node.State()

	if _, ok := state.Election.VotesReceived["B"]; !ok {
		t.Fatal("expected B's vote to be recorded")
	}
}

func TestDuplicateVoteIsIgnored(t *testing.T) {
	node := NewRaftNode("A")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	if !node.recordVote("B", term, true) {
		t.Fatal("expected first vote to be recorded")
	}

	if node.recordVote("B", term, true) {
		t.Fatal("expected duplicate vote to be ignored")
	}
}

func TestVoteFromOldElectionIsIgnored(t *testing.T) {
	node := NewRaftNode("A")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	currentTerm := node.State().Persistent.CurrentTerm

	if node.recordVote("B", currentTerm-1, true) {
		t.Fatal("expected vote from old term to be ignored")
	}
}

func TestCandidateBecomesLeaderAfterMajority(t *testing.T) {
	node := NewRaftNode("A")

	node.SetPeers([]Peer{
		NewRaftNode("B"),
		NewRaftNode("C"),
	})

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	term := node.State().Persistent.CurrentTerm

	if node.tryBecomeLeader() {
		t.Fatal("candidate should not become leader with only one vote")
	}

	node.recordVote("B", term, true)

	if !node.tryBecomeLeader() {
		t.Fatal("candidate should become leader after majority")
	}

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}
}

func TestThreeNodeElection(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})

	nodeA.runElection()

	stateA := nodeA.State()

	if stateA.Role != Leader {
		t.Fatalf("expected A to become Leader, got %v", stateA.Role)
	}

	if stateA.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", stateA.LeaderID)
	}

	if stateA.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term 1, got %d",
			stateA.Persistent.CurrentTerm,
		)
	}
}

func TestHigherTermVoteReplyMakesCandidateFollower(t *testing.T) {
	node := NewRaftNode("A")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	currentTerm := node.State().Persistent.CurrentTerm

	node.recordVote("B", currentTerm, true)

	reply := RequestVoteReply{
		Term:        currentTerm + 1,
		VoterID:     "C",
		VoteGranted: false,
	}

	node.handleVoteReply(currentTerm, reply)

	state := node.State()

	if state.Persistent.CurrentTerm != currentTerm+1 {
		t.Fatalf(
			"expected term %d, got %d",
			currentTerm+1,
			state.Persistent.CurrentTerm,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected Follower, got %v", state.Role)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected VotedFor to be empty, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected LeaderID to be empty, got %q",
			state.LeaderID,
		)
	}

	if len(state.Election.VotesReceived) != 0 {
		t.Fatalf(
			"expected election votes to be cleared, got %d",
			len(state.Election.VotesReceived),
		)
	}
}

func TestStaleElectionVoteReplyIsIgnored(t *testing.T) {
	node := NewRaftNode("A")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	oldTerm := node.State().Persistent.CurrentTerm

	_, err = node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	currentTerm := node.State().Persistent.CurrentTerm

	if currentTerm != oldTerm+1 {
		t.Fatalf(
			"expected current term %d, got %d",
			oldTerm+1,
			currentTerm,
		)
	}

	reply := RequestVoteReply{
		Term:        oldTerm,
		VoterID:     "B",
		VoteGranted: true,
	}

	node.handleVoteReply(oldTerm, reply)

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if len(state.Election.VotesReceived) != 1 {
		t.Fatalf(
			"expected only self vote, got %d votes",
			len(state.Election.VotesReceived),
		)
	}

	if _, ok := state.Election.VotesReceived["B"]; ok {
		t.Fatal("stale vote from B should not have been counted")
	}
}

func TestSplitVoteProducesNoLeader(t *testing.T) {
	nodeA := NewRaftNode("A")
	nodeB := NewRaftNode("B")
	nodeC := NewRaftNode("C")

	nodeA.SetPeers([]Peer{nodeB, nodeC})
	nodeB.SetPeers([]Peer{nodeA, nodeC})
	nodeC.SetPeers([]Peer{nodeA, nodeB})

	if _, err := nodeA.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if _, err := nodeB.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if _, err := nodeC.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	if nodeA.tryBecomeLeader() {
		t.Fatal("A should not become leader with only its own vote")
	}

	if nodeB.tryBecomeLeader() {
		t.Fatal("B should not become leader with only its own vote")
	}

	if nodeC.tryBecomeLeader() {
		t.Fatal("C should not become leader with only its own vote")
	}

	if nodeA.State().Role != Candidate {
		t.Fatal("A should remain Candidate")
	}

	if nodeB.State().Role != Candidate {
		t.Fatal("B should remain Candidate")
	}

	if nodeC.State().Role != Candidate {
		t.Fatal("C should remain Candidate")
	}
}

func TestSetElectionTimeout(t *testing.T) {
	node := NewRaftNode("A")

	node.SetElectionTimeout(10)

	node.mu.RLock()
	timeout := node.electionTimeout
	node.mu.RUnlock()

	if timeout != 10 {
		t.Fatalf("expected election timeout 10, got %d", timeout)
	}
}

func TestTickTriggersElectionTimeout(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(3)

	if node.Tick() {
		t.Fatal("expected no election after first tick")
	}

	if node.Tick() {
		t.Fatal("expected no election after second tick")
	}

	if !node.Tick() {
		t.Fatal("expected election after third tick")
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 0 {
		t.Fatalf("expected election elapsed to reset to 0, got %d", elapsed)
	}
}

func TestTickStartsElection(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(3)

	if node.Tick() {
		t.Fatal("expected no election after first tick")
	}

	if node.Tick() {
		t.Fatal("expected no election after second tick")
	}

	if !node.Tick() {
		t.Fatal("expected election timeout after third tick")
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term 1, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "A" {
		t.Fatalf("expected A to vote for itself, got %q", state.Persistent.VotedFor)
	}
}

func TestLeaderDoesNotStartElectionOnTimeout(t *testing.T) {
	node := NewRaftNode("A")
	node.SetPeers([]Peer{})

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.tryBecomeLeader()

	if node.State().Role != Leader {
		t.Fatal("expected node to become Leader")
	}

	node.onElectionTimeout()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected Leader to remain Leader, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term to remain 1, got %d", state.Persistent.CurrentTerm)
	}
}

func TestElectionTimeoutStartsElection(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(3)

	if node.Tick() {
		t.Fatal("expected no timeout on first tick")
	}

	if node.Tick() {
		t.Fatal("expected no timeout on second tick")
	}

	if !node.Tick() {
		t.Fatal("expected timeout on third tick")
	}

	node.onElectionTimeout()

	state := node.State()

	if state.Role != Candidate {
		t.Fatalf("expected Candidate, got %v", state.Role)
	}

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term 1, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "A" {
		t.Fatalf("expected self-vote for A, got %q", state.Persistent.VotedFor)
	}
}

func TestRequestVoteResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(10)

	node.Tick()
	node.Tick()

	node.mu.RLock()
	elapsedBefore := node.electionElapsed
	node.mu.RUnlock()

	if elapsedBefore != 2 {
		t.Fatalf("expected elapsed time 2, got %d", elapsedBefore)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:         1,
		CandidateID:  "B",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	node.mu.RLock()
	elapsedAfter := node.electionElapsed
	node.mu.RUnlock()

	if elapsedAfter != 0 {
		t.Fatalf("expected election timer to reset to 0, got %d", elapsedAfter)
	}
}

func TestAppendEntriesResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("B")
	node.SetElectionTimeout(10)

	node.Tick()
	node.Tick()

	node.mu.RLock()
	elapsedBefore := node.electionElapsed
	node.mu.RUnlock()

	if elapsedBefore != 2 {
		t.Fatalf("expected elapsed time 2, got %d", elapsedBefore)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "A",
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected Follower, got %v", state.Role)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A, got %q", state.LeaderID)
	}

	node.mu.RLock()
	elapsedAfter := node.electionElapsed
	node.mu.RUnlock()

	if elapsedAfter != 0 {
		t.Fatalf("expected election timer to reset to 0, got %d", elapsedAfter)
	}
}

func TestAppendEntriesRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("B")
	node.SetElectionTimeout(10)

	node.Tick()
	node.Tick()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 5
	node.state.Role = Follower
	node.state.LeaderID = "A"
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     4,
		LeaderID: "C",
	})

	if reply.Success {
		t.Fatal("expected stale AppendEntries to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.LeaderID != "A" {
		t.Fatalf("expected leader A to remain unchanged, got %q", state.LeaderID)
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 2 {
		t.Fatalf("expected election timer to remain 2, got %d", elapsed)
	}
}

func TestBecomeLeaderInitializesReplicationState(t *testing.T) {
	node := NewRaftNode("A")

	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")

	node.SetPeers([]Peer{peerB, peerC})

	if err := node.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := node.Log().Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("two"),
	}); err != nil {
		t.Fatal(err)
	}

	node.becomeLeader()

	state := node.State()

	if state.Role != Leader {
		t.Fatalf("expected leader, got %v", state.Role)
	}

	if state.Leader.NextIndex["B"] != 3 {
		t.Fatalf("expected B NextIndex=3, got %d", state.Leader.NextIndex["B"])
	}

	if state.Leader.NextIndex["C"] != 3 {
		t.Fatalf("expected C NextIndex=3, got %d", state.Leader.NextIndex["C"])
	}

	if state.Leader.MatchIndex["B"] != 0 {
		t.Fatalf("expected B MatchIndex=0, got %d", state.Leader.MatchIndex["B"])
	}

	if state.Leader.MatchIndex["C"] != 0 {
		t.Fatalf("expected C MatchIndex=0, got %d", state.Leader.MatchIndex["C"])
	}
}

func TestAdvanceCommitIndex(t *testing.T) {
	leader := NewRaftNode("A")
	peerB := NewRaftNode("B")
	peerC := NewRaftNode("C")

	leader.SetPeers([]Peer{peerB, peerC})

	leader.mu.Lock()

	leader.state.Role = Leader
	leader.state.Persistent.CurrentTerm = 2

	if err := leader.log.Append(LogEntry{
		Index: 1,
		Term:  2,
		Data:  []byte("one"),
	}); err != nil {
		leader.mu.Unlock()
		t.Fatal(err)
	}

	leader.state.Leader.MatchIndex["B"] = 1
	leader.state.Leader.MatchIndex["C"] = 0

	leader.mu.Unlock()

	leader.advanceCommitIndex()

	state := leader.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected commit index 1, got %d",
			state.Volatile.CommitIndex,
		)
	}
}

func TestAppendEntriesAppliesCommittedEntry(t *testing.T) {
	node := NewRaftNode("node-1")

	entry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("command"),
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		Entries:      []LogEntry{entry},
		LeaderCommit: 1,
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	select {
	case applied := <-node.ApplyCh():
		if applied.Index != 1 {
			t.Fatalf(
				"expected applied index 1, got %d",
				applied.Index,
			)
		}

		if string(applied.Data) != "command" {
			t.Fatalf(
				"expected applied data command, got %q",
				applied.Data,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("expected committed entry to be applied")
	}

	state := node.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected commit index 1, got %d",
			state.Volatile.CommitIndex,
		)
	}

	if state.Volatile.LastApplied != 1 {
		t.Fatalf(
			"expected last applied 1, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestProposeAsLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	index, err := node.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose() returned error: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	entry, ok := node.Log().Get(index)
	if !ok {
		t.Fatalf("expected proposed entry at index %d", index)
	}

	if string(entry.Data) != "hello" {
		t.Fatalf("expected data %q, got %q", "hello", string(entry.Data))
	}

	if entry.Term != 1 {
		t.Fatalf("expected term 1, got %d", entry.Term)
	}
}

func TestLeaderBacktracksNextIndexOnReplicationFailure(t *testing.T) {
	leader := NewRaftNode("leader")
	follower := NewRaftNode("follower")

	leader.SetPeers([]Peer{follower})
	follower.SetPeers([]Peer{leader})

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	if err := leader.Log().Append(LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("one"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("two"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := leader.Log().Append(LogEntry{
		Index: 3,
		Term:  2,
		Data:  []byte("three"),
	}); err != nil {
		t.Fatal(err)
	}

	leader.mu.Lock()
	leader.state.Leader.NextIndex[follower.ID()] = 4
	leader.mu.Unlock()

	args, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("expected AppendEntries arguments to be built")
	}

	reply := follower.AppendEntries(args)

	if reply.Success {
		t.Fatal("expected replication to fail")
	}

	leader.handleAppendEntriesReply(follower.ID(), args, reply)

	leader.mu.RLock()
	nextIndex := leader.state.Leader.NextIndex[follower.ID()]
	leader.mu.RUnlock()

	if nextIndex != 3 {
		t.Fatalf("expected NextIndex 3 after failure, got %d", nextIndex)
	}

	retryArgs, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("expected retry AppendEntries arguments to be built")
	}

	if retryArgs.PrevLogIndex != 2 {
		t.Fatalf(
			"expected retry PrevLogIndex 2, got %d",
			retryArgs.PrevLogIndex,
		)
	}

	if retryArgs.PrevLogTerm != 1 {
		t.Fatalf(
			"expected retry PrevLogTerm 1, got %d",
			retryArgs.PrevLogTerm,
		)
	}

	if len(retryArgs.Entries) != 1 {
		t.Fatalf(
			"expected retry to contain 1 entry, got %d",
			len(retryArgs.Entries),
		)
	}

	if retryArgs.Entries[0].Index != 3 {
		t.Fatalf(
			"expected retry entry index 3, got %d",
			retryArgs.Entries[0].Index,
		)
	}
}

func TestProposeReplicatesAndCommits(t *testing.T) {
	leader := NewRaftNode("leader")
	follower1 := NewRaftNode("follower-1")
	follower2 := NewRaftNode("follower-2")

	leader.SetPeers([]Peer{follower1, follower2})
	follower1.SetPeers([]Peer{leader, follower2})
	follower2.SetPeers([]Peer{leader, follower1})

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	index, err := leader.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose() returned error: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected proposed index 1, got %d", index)
	}

	for _, follower := range []*RaftNode{follower1, follower2} {
		entry, ok := follower.Log().Get(1)
		if !ok {
			t.Fatalf("follower %s does not have entry 1", follower.ID())
		}

		if string(entry.Data) != "hello" {
			t.Fatalf(
				"follower %s has data %q, expected %q",
				follower.ID(),
				string(entry.Data),
				"hello",
			)
		}
	}

	state := leader.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected leader CommitIndex 1, got %d",
			state.Volatile.CommitIndex,
		)
	}

	select {
	case entry := <-leader.ApplyCh():
		if entry.Index != 1 {
			t.Fatalf(
				"expected applied index 1, got %d",
				entry.Index,
			)
		}

		if string(entry.Data) != "hello" {
			t.Fatalf(
				"expected applied data %q, got %q",
				"hello",
				string(entry.Data),
			)
		}

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed entry")
	}

	state = leader.State()

	if state.Volatile.LastApplied != 1 {
		t.Fatalf(
			"expected LastApplied 1, got %d",
			state.Volatile.LastApplied,
		)
	}
}

func TestHeartbeatDueOnlyForLeader(t *testing.T) {
	node := NewRaftNode("A")

	if node.heartbeatDue() {
		t.Fatal("follower should not send heartbeat")
	}

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	node.becomeLeader()

	if !node.heartbeatDue() {
		t.Fatal("leader should send heartbeat when heartbeat timer expires")
	}
}

func TestHeartbeatResetsFollowerElectionTimer(t *testing.T) {
	leader := NewRaftNode("leader")
	follower := NewRaftNode("follower")

	leader.SetPeers([]Peer{follower})
	follower.SetPeers([]Peer{leader})

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	follower.mu.Lock()
	follower.electionElapsed = 5
	follower.mu.Unlock()

	args, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("failed to build heartbeat")
	}

	if len(args.Entries) != 0 {
		t.Fatal("expected empty heartbeat")
	}

	reply := follower.AppendEntries(args)
	if !reply.Success {
		t.Fatal("heartbeat should succeed")
	}

	state := follower.State()

	if state.Role != Follower {
		t.Fatalf("expected follower role, got %v", state.Role)
	}

	follower.mu.RLock()
	electionElapsed := follower.electionElapsed
	follower.mu.RUnlock()

	if electionElapsed != 0 {
		t.Fatalf("expected election timer reset, got %d", electionElapsed)
	}
}

func TestLeaderTickSendsHeartbeat(t *testing.T) {
	leader := NewRaftNode("leader")
	follower := NewRaftNode("follower")

	leader.SetPeers([]Peer{follower})
	follower.SetPeers([]Peer{leader})

	if _, err := leader.startElection(); err != nil {
		t.Fatalf("start election: %v", err)
	}

	leader.becomeLeader()

	follower.mu.Lock()
	follower.electionElapsed = 5
	follower.mu.Unlock()

	leader.mu.Lock()
	leader.heartbeatTimeout = 1
	leader.mu.Unlock()

	electionDue := leader.Tick()

	if electionDue {
		t.Fatal("leader should not start an election")
	}

	follower.mu.RLock()
	electionElapsed := follower.electionElapsed
	followerRole := follower.state.Role
	followerLeaderID := follower.state.LeaderID
	follower.mu.RUnlock()

	if electionElapsed != 0 {
		t.Fatalf(
			"expected follower election timer to reset, got %d",
			electionElapsed,
		)
	}

	if followerRole != Follower {
		t.Fatalf("expected follower role, got %v", followerRole)
	}

	if followerLeaderID != leader.ID() {
		t.Fatalf(
			"expected leader ID %q, got %q",
			leader.ID(),
			followerLeaderID,
		)
	}
}

func TestFollowerRejectsStaleHeartbeat(t *testing.T) {
	follower := NewRaftNode("follower")

	follower.mu.Lock()
	follower.state.Persistent.CurrentTerm = 5
	follower.electionElapsed = 4
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:     4,
		LeaderID: "old-leader",
	}

	reply := follower.AppendEntries(args)

	if reply.Success {
		t.Fatal("follower should reject stale heartbeat")
	}

	state := follower.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	follower.mu.RLock()
	electionElapsed := follower.electionElapsed
	follower.mu.RUnlock()

	if electionElapsed != 4 {
		t.Fatalf(
			"expected election timer to remain 4, got %d",
			electionElapsed,
		)
	}
}

func TestRaftNodeRestoresPersistentState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()

	node.state.Persistent.CurrentTerm = 7
	node.state.Persistent.VotedFor = "B"

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist state: %v", err)
	}

	node.mu.Unlock()

	restored, err := NewRaftNodeWithStorage("A", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 7 {
		t.Fatalf(
			"expected term 7, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "B" {
		t.Fatalf(
			"expected votedFor B, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestRequestVoteHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = "old-candidate"

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.RequestVote(RequestVoteArgs{
		Term:        5,
		CandidateID: "node-2",
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	restored, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected restored term 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-2" {
		t.Fatalf(
			"expected restored vote for node-2, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}
}

func TestAppendEntriesHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = "old-candidate"

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         5,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	restored, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected restored term 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected restored vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}
}

type failingStateStorage struct {
	*storage.MemoryStorage

	saveStateErr error
	syncErr      error
}

func (s *failingStateStorage) SaveState(
	state model.PersistentState,
) error {
	if s.saveStateErr != nil {
		return s.saveStateErr
	}

	return s.MemoryStorage.SaveState(state)
}

func (s *failingStateStorage) Sync() error {
	if s.syncErr != nil {
		return s.syncErr
	}

	return s.MemoryStorage.Sync()
}

func TestAppendEntriesHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail when higher-term persistence fails")
	}

	persisted, err := baseStore.LoadState()
	if err != nil {
		t.Fatalf("load persisted state: %v", err)
	}

	if persisted.CurrentTerm != 2 {
		t.Fatalf(
			"expected persisted term to remain 2, got %d",
			persisted.CurrentTerm,
		)
	}

	if persisted.VotedFor != "old-candidate" {
		t.Fatalf(
			"expected persisted vote to remain old-candidate, got %q",
			persisted.VotedFor,
		)
	}
}

func TestAppendEntriesHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("follower", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:     5,
		LeaderID: "leader",
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail when higher-term sync fails")
	}
}

func TestHandleVoteReplyHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = node.id
	node.state.Election.VotesReceived = map[NodeID]struct{}{
		node.id: {},
	}

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	node.handleVoteReply(2, RequestVoteReply{
		Term:        5,
		VoterID:     "node-2",
		VoteGranted: false,
	})

	restored, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected restored term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected restored vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected restored leader ID to be empty, got %q",
			state.LeaderID,
		)
	}
}

func TestHandleAppendEntriesReplyHigherTermPersistsAcrossRestart(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("leader", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 2
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	node.handleAppendEntriesReply(
		"follower",
		AppendEntriesArgs{
			Term: 2,
		},
		AppendEntriesReply{
			Term:       5,
			FollowerID: "follower",
			Success:    false,
		},
	)

	restored, err := NewRaftNodeWithStorage("leader", store)
	if err != nil {
		t.Fatalf("restore node: %v", err)
	}

	state := restored.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected restored term 5, got %d", state.Persistent.CurrentTerm)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected restored vote to be cleared, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf("expected restored node to be follower, got %v", state.Role)
	}

	if state.LeaderID != "" {
		t.Fatalf(
			"expected restored leader ID to be empty, got %q",
			state.LeaderID,
		)
	}
}

func TestRequestVoteStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.RequestVote(RequestVoteArgs{
		Term:         3,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected stale-term vote to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term to remain 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf(
			"expected vote to remain for %q, got %q",
			node.id,
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != node.id {
		t.Fatalf(
			"expected leader ID to remain %q, got %q",
			node.id,
			state.LeaderID,
		)
	}
}

func TestAppendEntriesStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	if err := node.persistStateLocked(); err != nil {
		node.mu.Unlock()
		t.Fatalf("persist initial state: %v", err)
	}
	node.mu.Unlock()

	reply := node.AppendEntries(AppendEntriesArgs{
		Term:         3,
		LeaderID:     "node-2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	})

	if reply.Success {
		t.Fatal("expected stale-term AppendEntries to be rejected")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf(
			"expected term to remain 5, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf(
			"expected vote to remain for %q, got %q",
			node.id,
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.LeaderID != node.id {
		t.Fatalf(
			"expected leader ID to remain %q, got %q",
			node.id,
			state.LeaderID,
		)
	}
}

func TestHandleVoteReplyStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Persistent.CurrentTerm = 5
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = ""

	node.state.Election.VotesReceived = map[NodeID]struct{}{
		node.id: {},
	}
	node.mu.Unlock()

	node.handleVoteReply(5, RequestVoteReply{
		Term:        3,
		VoterID:     "node-2",
		VoteGranted: true,
	})

	state := node.State()

	if state.Persistent.CurrentTerm != 5 {
		t.Fatalf("expected term to remain 5, got %d",
			state.Persistent.CurrentTerm)
	}

	if state.Role != Candidate {
		t.Fatalf("expected role to remain Candidate, got %v",
			state.Role)
	}

	if state.Persistent.VotedFor != node.id {
		t.Fatalf("expected vote to remain for %q, got %q",
			node.id, state.Persistent.VotedFor)
	}

	if _, ok := state.Election.VotesReceived["node-2"]; ok {
		t.Fatal("stale vote reply must not be counted")
	}
}

func TestHandleAppendEntriesReplyStaleTermDoesNotChangeState(t *testing.T) {
	store := storage.NewMemoryStorage()

	node, err := NewRaftNodeWithStorage("leader", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Leader
	node.state.Persistent.CurrentTerm = 6
	node.state.Persistent.VotedFor = node.id
	node.state.LeaderID = node.id

	node.state.Leader.NextIndex["follower"] = 3
	node.state.Leader.MatchIndex["follower"] = 1
	node.mu.Unlock()

	node.handleAppendEntriesReply(
		"follower",
		AppendEntriesArgs{
			Term: 5,
			Entries: []LogEntry{
				{
					Index: 2,
					Term:  5,
					Data:  []byte("value"),
				},
			},
		},
		AppendEntriesReply{
			Term:       5,
			FollowerID: "follower",
			Success:    true,
		},
	)

	state := node.State()

	if state.Persistent.CurrentTerm != 6 {
		t.Fatalf(
			"expected term to remain 6, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Role != Leader {
		t.Fatalf(
			"expected role to remain Leader, got %v",
			state.Role,
		)
	}

	if state.Leader.NextIndex["follower"] != 3 {
		t.Fatalf(
			"expected NextIndex to remain 3, got %d",
			state.Leader.NextIndex["follower"],
		)
	}

	if state.Leader.MatchIndex["follower"] != 1 {
		t.Fatalf(
			"expected MatchIndex to remain 1, got %d",
			state.Leader.MatchIndex["follower"],
		)
	}
}

func TestRaftNodeStartStop(t *testing.T) {
	node := NewRaftNode("node-1")

	err := node.Start()
	require.NoError(t, err)

	node.Stop()
}

func TestRaftNodeStartTwice(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NoError(t, node.Start())
	defer node.Stop()

	err := node.Start()
	require.Error(t, err)
}

func TestRaftNodeStopWithoutStart(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NotPanics(t, func() {
		node.Stop()
	})
}

func TestTickDoesNotTriggerElectionBeforeTimeout(t *testing.T) {
	node := NewRaftNode("node-1")

	for i := 0; i < node.electionTimeout-1; i++ {
		electionDue, heartbeatDue := node.tick()

		require.False(t, electionDue)
		require.False(t, heartbeatDue)
	}

	electionDue, heartbeatDue := node.tick()

	require.True(t, electionDue)
	require.False(t, heartbeatDue)
}

func TestTickTriggersHeartbeatForLeader(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Role = Leader
	node.heartbeatElapsed = node.heartbeatTimeout - 1
	node.mu.Unlock()

	electionDue, heartbeatDue := node.tick()

	require.False(t, electionDue)
	require.True(t, heartbeatDue)
}
func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

func TestRaftNodeStartDrivesElection(t *testing.T) {
	node := NewRaftNode("node-1")

	node.SetElectionTimeout(3)

	require.NoError(t, node.Start())
	defer node.Stop()

	waitForCondition(t, time.Second, func() bool {
		state := node.State()
		return state.Role == Candidate || state.Role == Leader
	})
}

func TestRaftNodeStartAdvancesElectionTimer(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetElectionTimeout(100)

	require.NoError(t, node.Start())

	waitForCondition(t, time.Second, func() bool {
		node.mu.RLock()
		defer node.mu.RUnlock()

		return node.electionElapsed > 0
	})

	node.Stop()
}

func TestRaftNodeStopWaitsForRunLoop(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NoError(t, node.Start())

	node.Stop()

	node.runMu.Lock()
	running := node.running
	node.runMu.Unlock()

	require.False(t, running)
}

func TestRaftNodeCanRestart(t *testing.T) {
	node := NewRaftNode("node-1")

	require.NoError(t, node.Start())
	node.Stop()

	require.NoError(t, node.Start())
	node.Stop()
}
func TestStartElectionIfNeededPreventsDuplicateElection(t *testing.T) {
	node := NewRaftNode("node-1")

	node.runMu.Lock()
	node.electionInFlight = true
	node.runMu.Unlock()

	node.startElectionIfNeeded()

	node.runMu.Lock()
	defer node.runMu.Unlock()

	require.True(t, node.electionInFlight)
}
