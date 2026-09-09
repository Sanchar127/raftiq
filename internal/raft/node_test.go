package raft

import (
	"testing"
	"time"
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

	node.startElection()
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

	node.startElection()

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

	node.startElection()
	node.becomeLeader()

	node.becomeFollower(2)

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

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

	node.startElection()

	oldTerm := node.State().Persistent.CurrentTerm

	// Start a new election.
	node.startElection()

	currentTerm := node.State().Persistent.CurrentTerm

	if currentTerm != oldTerm+1 {
		t.Fatalf(
			"expected current term %d, got %d",
			oldTerm+1,
			currentTerm,
		)
	}

	// Response from the old election.
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

	nodeA.startElection()
	nodeB.startElection()
	nodeC.startElection()

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

	node.startElection()

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

	node.startElection()
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

	// Move the follower's timer forward.
	node.Tick()
	node.Tick()

	// Establish current term 5.
	node.startElection()

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

	node.startElection()
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

	// Put the leader into leader state.
	leader.startElection()
	leader.becomeLeader()

	// Leader has three entries.
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

	// Pretend the leader believes the follower is caught up.
	leader.mu.Lock()
	leader.state.Leader.NextIndex[follower.ID()] = 4
	leader.mu.Unlock()

	// The follower does not have entry 3, so the first attempt
	// must fail because PrevLogIndex=3 cannot be found.
	args, ok := leader.buildAppendEntries(follower.ID())
	if !ok {
		t.Fatal("expected AppendEntries arguments to be built")
	}

	reply := follower.AppendEntries(args)

	if reply.Success {
		t.Fatal("expected replication to fail")
	}

	// Process the failed replication response.
	leader.handleAppendEntriesReply(follower.ID(), args, reply)

	// The leader should back up from 4 to 3.
	leader.mu.RLock()
	nextIndex := leader.state.Leader.NextIndex[follower.ID()]
	leader.mu.RUnlock()

	if nextIndex != 3 {
		t.Fatalf("expected NextIndex 3 after failure, got %d", nextIndex)
	}

	// Build the retry.
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

	// Make the leader authoritative for this test.
	leader.startElection()
	leader.becomeLeader()

	index, err := leader.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose() returned error: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected proposed index 1, got %d", index)
	}

	// Verify both followers received the entry.
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

	// The leader should have committed the entry after reaching
	// a majority.
	state := leader.State()

	if state.Volatile.CommitIndex != 1 {
		t.Fatalf(
			"expected leader CommitIndex 1, got %d",
			state.Volatile.CommitIndex,
		)
	}

	// The committed entry should eventually appear on ApplyCh.
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

	// The entry should now be marked as applied.
	state = leader.State()

	if state.Volatile.LastApplied != 1 {
		t.Fatalf(
			"expected LastApplied 1, got %d",
			state.Volatile.LastApplied,
		)
	}
}
