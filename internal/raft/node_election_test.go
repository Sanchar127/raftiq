package raft

import (
	"errors"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
	"testing"
)

func TestRequestVoteGrantsVote(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"node-1",
		"node-2",
	}
	node.mu.Unlock()

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

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"node-1",
		"node-2",
		"node-3",
	}
	node.mu.Unlock()

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

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

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

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"node-1",
		"node-2",
	}
	node.mu.Unlock()

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
		t.Fatalf(
			"expected follower role, got %v",
			state.Role,
		)
	}
}

func TestCandidateVotesForItself(t *testing.T) {
	node := NewRaftNode("A")

	if err := node.BootstrapMembership(); err != nil {
		t.Fatalf("bootstrap membership: %v", err)
	}

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

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"A",
				"B",
			},
		},
	}
	node.mu.Unlock()

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

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B"},
		},
	}
	node.mu.Unlock()

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

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{"A", "B"},
		},
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	currentTerm := node.State().Persistent.CurrentTerm

	if node.recordVote("B", currentTerm-1, true) {
		t.Fatal("expected vote from old term to be ignored")
	}
}

func TestStaleElectionVoteReplyIsIgnored(t *testing.T) {
	node := NewRaftNode("A")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"A",
				"B",
			},
		},
	}
	node.mu.Unlock()

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

func TestRequestVoteResetsElectionTimer(t *testing.T) {
	node := NewRaftNode("A")
	node.SetElectionTimeout(10)

	node.mu.Lock()
	node.state.Persistent.Membership.Current.Voters = []NodeID{
		"A",
		"B",
	}
	node.mu.Unlock()

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
		t.Fatalf(
			"expected election timer to reset to 0, got %d",
			elapsedAfter,
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
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}

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

	if !membershipIsVoter(
		state.Persistent.Membership,
		"node-2",
	) {
		t.Fatal("expected node-2 to remain a voter after restart")
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

func TestPreVoteGrantsVoteForUpToDateCandidate(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.mu.Unlock()

	reply := node.PreVote(PreVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected pre-vote to be granted")
	}

	if reply.Term != 0 {
		t.Fatalf("expected current term 0, got %d", reply.Term)
	}

	if reply.VoterID != "node-1" {
		t.Fatalf("expected voter ID node-1, got %q", reply.VoterID)
	}
}

func TestPreVoteRejectsOlderTerm(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.mu.Unlock()

	_, err := node.startElection()
	if err != nil {
		t.Fatalf("start election: %v", err)
	}

	reply := node.PreVote(PreVoteArgs{
		Term:        0,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected older-term pre-vote to be rejected")
	}

	if reply.Term != 1 {
		t.Fatalf("expected current term 1, got %d", reply.Term)
	}
}

func TestPreVoteRejectsNonVoter(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.mu.Unlock()

	reply := node.PreVote(PreVoteArgs{
		Term:         1,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected PreVote from non-voter to be rejected")
	}

	if reply.VoterID != "node-1" {
		t.Fatalf(
			"expected voter ID node-1, got %q",
			reply.VoterID,
		)
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 0 {
		t.Fatalf(
			"expected term to remain 0, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected VotedFor to remain empty, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestPreVoteDoesNotChangeTermOrVote(t *testing.T) {
	node := NewRaftNode("node-1")

	node.mu.Lock()

	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}

	node.state.Persistent.CurrentTerm = 0
	node.state.Persistent.VotedFor = ""
	node.state.Role = Follower
	node.state.LeaderID = ""
	node.electionElapsed = node.electionTimeout

	node.mu.Unlock()

	before := node.State()

	reply := node.PreVote(PreVoteArgs{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected pre-vote to be granted")
	}

	after := node.State()

	// PreVote must not change the persistent term.
	if after.Persistent.CurrentTerm != before.Persistent.CurrentTerm {
		t.Fatalf(
			"pre-vote changed term: before=%d after=%d",
			before.Persistent.CurrentTerm,
			after.Persistent.CurrentTerm,
		)
	}

	// PreVote must not change the persistent vote.
	if after.Persistent.VotedFor != before.Persistent.VotedFor {
		t.Fatalf(
			"pre-vote changed VotedFor: before=%q after=%q",
			before.Persistent.VotedFor,
			after.Persistent.VotedFor,
		)
	}

	// PreVote must not change the node's role.
	if after.Role != before.Role {
		t.Fatalf(
			"pre-vote changed role: before=%v after=%v",
			before.Role,
			after.Role,
		)
	}

	// PreVote must not establish or change a leader.
	if after.LeaderID != before.LeaderID {
		t.Fatalf(
			"pre-vote changed LeaderID: before=%q after=%q",
			before.LeaderID,
			after.LeaderID,
		)
	}
}

func TestPreVoteRejectsStaleCandidateLog(t *testing.T) {
	node := NewRaftNode("node-1")

	if err := node.log.Append(LogEntry{
		Index: 1,
		Term:  1,
	}); err != nil {
		t.Fatalf("append log entry: %v", err)
	}

	reply := node.PreVote(PreVoteArgs{
		Term:         2,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if reply.VoteGranted {
		t.Fatal("expected stale candidate log to be rejected")
	}
}

func TestPreVoteRejectsWhenRecentLeaderExists(t *testing.T) {
	node := NewRaftNode("node-2")
	node.SetElectionTimeout(10)

	// Simulate a recent heartbeat from the current leader.
	appendReply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-1",
	})

	if !appendReply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	state := node.State()

	if state.Role != Follower {
		t.Fatalf("expected Follower, got %v", state.Role)
	}

	if state.LeaderID != "node-1" {
		t.Fatalf("expected leader node-1, got %q", state.LeaderID)
	}

	preVoteReply := node.PreVote(PreVoteArgs{
		Term:         2,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if preVoteReply.VoteGranted {
		t.Fatal("expected PreVote to be rejected while recent leader is known")
	}

	after := node.State()

	if after.Persistent.CurrentTerm != 1 {
		t.Fatalf("expected term to remain 1, got %d", after.Persistent.CurrentTerm)
	}

	if after.LeaderID != "node-1" {
		t.Fatalf("expected leader node-1 to remain, got %q", after.LeaderID)
	}

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 0 {
		t.Fatalf("expected PreVote not to reset election timer, got %d", elapsed)
	}
}

func TestPreVoteGrantsAfterElectionTimeout(t *testing.T) {
	node := NewRaftNode("node-2")
	node.SetElectionTimeout(10)

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
				"node-3",
			},
		},
	}
	node.mu.Unlock()

	// Simulate a heartbeat from the current leader.
	appendReply := node.AppendEntries(AppendEntriesArgs{
		Term:     1,
		LeaderID: "node-1",
	})

	if !appendReply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	// Simulate the election timer expiring without another heartbeat.
	node.mu.Lock()
	node.electionElapsed = node.electionTimeout
	node.mu.Unlock()

	preVoteReply := node.PreVote(PreVoteArgs{
		Term:         2,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !preVoteReply.VoteGranted {
		t.Fatal("expected PreVote to be granted after election timeout")
	}

	// PreVote must never change the persistent term.
	state := node.State()

	if state.Persistent.CurrentTerm != 1 {
		t.Fatalf(
			"expected term to remain 1, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	// PreVote must not change the known leader.
	if state.LeaderID != "node-1" {
		t.Fatalf(
			"expected leader ID to remain node-1, got %q",
			state.LeaderID,
		)
	}

	// PreVote must not change the node's role.
	if state.Role != Follower {
		t.Fatalf(
			"expected node to remain Follower, got %v",
			state.Role,
		)
	}
}

func TestRunElectionResetsTimerAfterPreVoteFailure(t *testing.T) {
	node := NewRaftNode("node-1")
	node.SetElectionTimeout(10)

	node.mu.Lock()
	node.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
			},
		},
	}
	node.electionElapsed = node.electionTimeout
	node.mu.Unlock()

	// No transport means the PreVote cannot obtain a quorum.
	node.runElection()

	node.mu.RLock()
	elapsed := node.electionElapsed
	node.mu.RUnlock()

	if elapsed != 0 {
		t.Fatalf(
			"expected election timer to reset after failed PreVote, got %d",
			elapsed,
		)
	}
}

func TestRequestVoteHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:        5,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be denied when higher-term persistence fails")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected in-memory term to remain 2 after persistence failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "old-candidate" {
		t.Fatalf(
			"expected in-memory vote to remain old-candidate after persistence failure, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestRequestVoteHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "old-candidate",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:        5,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be denied when higher-term sync fails")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected in-memory term to remain 2 after sync failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "old-candidate" {
		t.Fatalf(
			"expected in-memory vote to remain old-candidate after sync failure, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestRequestVoteVotePersistenceFailureDoesNotChangeMemory(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	reply := node.RequestVote(RequestVoteArgs{
		Term:        2,
		CandidateID: "node-2",
	})

	if reply.VoteGranted {
		t.Fatal("expected vote to be denied when vote persistence fails")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term to remain 2, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected in-memory vote to remain empty, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestHandleVoteReplyHigherTermSaveStateFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "node-1",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Election.VotesReceived = map[NodeID]struct{}{
		node.id: {},
	}
	node.mu.Unlock()

	node.handleVoteReply(2, RequestVoteReply{
		Term:        5,
		VoterID:     "node-2",
		VoteGranted: false,
	})

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected in-memory term to remain 2 after persistence failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-1" {
		t.Fatalf(
			"expected in-memory vote to remain node-1, got %q",
			state.Persistent.VotedFor,
		)
	}
}

func TestHandleVoteReplyHigherTermSyncFailure(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "node-1",
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.mu.Lock()
	node.state.Role = Candidate
	node.state.Election.VotesReceived = map[NodeID]struct{}{
		node.id: {},
	}
	node.mu.Unlock()

	node.handleVoteReply(2, RequestVoteReply{
		Term:        5,
		VoterID:     "node-2",
		VoteGranted: false,
	})

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected in-memory term to remain 2 after sync failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "node-1" {
		t.Fatalf(
			"expected in-memory vote to remain node-1, got %q",
			state.Persistent.VotedFor,
		)
	}
}
func TestStartElectionSaveStateFailureDoesNotChangeMemory(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		saveStateErr:  errors.New("injected SaveState failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	_, err = node.startElection()
	if err == nil {
		t.Fatal("expected startElection to fail")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term to remain 2 after SaveState failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected VotedFor to remain empty, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf(
			"expected role to remain Follower, got %v",
			state.Role,
		)
	}
}
func TestStartElectionSyncFailureDoesNotChangeMemory(t *testing.T) {
	baseStore := storage.NewMemoryStorage()

	initialState := model.PersistentState{
		CurrentTerm: 2,
		VotedFor:    "",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
				},
			},
		},
	}

	if err := baseStore.SaveState(initialState); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	store := &failingStateStorage{
		MemoryStorage: baseStore,
		syncErr:       errors.New("injected Sync failure"),
	}

	node, err := NewRaftNodeWithStorage("node-1", store)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	_, err = node.startElection()
	if err == nil {
		t.Fatal("expected startElection to fail")
	}

	state := node.State()

	if state.Persistent.CurrentTerm != 2 {
		t.Fatalf(
			"expected term to remain 2 after Sync failure, got %d",
			state.Persistent.CurrentTerm,
		)
	}

	if state.Persistent.VotedFor != "" {
		t.Fatalf(
			"expected VotedFor to remain empty, got %q",
			state.Persistent.VotedFor,
		)
	}

	if state.Role != Follower {
		t.Fatalf(
			"expected role to remain Follower, got %v",
			state.Role,
		)
	}
}
