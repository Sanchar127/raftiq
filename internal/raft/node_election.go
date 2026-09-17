package raft

import (
	"fmt"
	"time"
)

func (n *RaftNode) isCandidateLogUpToDate(
	lastLogIndex LogIndex,
	lastLogTerm Term,
) bool {
	localLastTerm := n.log.LastTerm()
	localLastIndex := n.log.LastIndex()

	if lastLogTerm != localLastTerm {
		return lastLogTerm > localLastTerm
	}

	return lastLogIndex >= localLastIndex
}

func (n *RaftNode) recordVote(
	peerID NodeID,
	term Term,
	granted bool,
) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.Role != Candidate {
		return false
	}

	if term != n.state.Persistent.CurrentTerm {
		return false
	}

	if !granted {
		return false
	}

	// Only voters in the active membership can contribute
	// to an election quorum.
	if !membershipIsVoter(
		n.state.Persistent.Membership,
		peerID,
	) {
		return false
	}

	if _, alreadyReceived := n.state.Election.VotesReceived[peerID]; alreadyReceived {
		return false
	}

	n.state.Election.VotesReceived[peerID] = struct{}{}

	return true
}

func (n *RaftNode) tryBecomeLeader() bool {
	logger := n.getLogger()

	n.mu.Lock()

	if n.state.Role != Candidate {
		n.mu.Unlock()
		return false
	}

	if !membershipHasQuorum(
		n.state.Persistent.Membership,
		n.state.Election.VotesReceived,
	) {
		n.mu.Unlock()
		return false
	}

	n.becomeLeaderLocked()

	term := n.state.Persistent.CurrentTerm
	votes := len(n.state.Election.VotesReceived)

	n.mu.Unlock()

	logger.Info(
		"raft election won",
		"term", term,
		"votes", votes,
	)

	return true
}

func (n *RaftNode) startElection() (Term, error) {
	logger := n.getLogger()

	n.mu.Lock()

	if !membershipIsVoter(n.state.Persistent.Membership, n.id) {
		term := n.state.Persistent.CurrentTerm
		n.mu.Unlock()

		return term, fmt.Errorf("node %s is not a voter", n.id)
	}

	if n.storageWriteBlocked {
		term := n.state.Persistent.CurrentTerm
		n.mu.Unlock()

		return term, fmt.Errorf("storage writes are blocked")
	}

	// Build the new persistent election state without publishing it to memory.
	persistentState := n.state.Persistent
	persistentState.CurrentTerm++
	persistentState.VotedFor = n.id

	// The new term and self-vote must be durable before becoming a candidate.
	if err := n.storage.SaveState(persistentState); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to persist election state",
			"error", err,
		)

		return 0, err
	}

	if err := n.storage.Sync(); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to sync election state",
			"error", err,
		)

		return 0, err
	}

	// Persistence succeeded, so publish the new state.
	n.state.Persistent = persistentState
	n.state.Role = Candidate
	n.state.LeaderID = ""

	n.electionElapsed = 0

	n.state.Election.VotesReceived = map[NodeID]struct{}{
		n.id: {},
	}

	n.electionStartedAt = time.Now()
	n.metrics.IncElections()
	n.updateStateMetricsLocked()

	term := n.state.Persistent.CurrentTerm
	peerCount := len(n.peerIDs)

	n.mu.Unlock()

	logger.Info(
		"raft election started",
		"term", term,
		"peer_count", peerCount,
	)

	return term, nil
}

func (n *RaftNode) requestVotes() {
	n.mu.RLock()

	// Only an eligible voter can participate as an election candidate.
	if n.state.Role != Candidate ||
		!membershipIsVoter(
			n.state.Persistent.Membership,
			n.id,
		) {
		n.mu.RUnlock()

		n.getLogger().Debug(
			"raft vote requests skipped",
			"node_id", n.id,
			"reason", "not_eligible_candidate",
		)

		return
	}

	term := n.state.Persistent.CurrentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	transport := n.transport
	peerIDs := append([]NodeID(nil), n.peerIDs...)
	candidateID := n.id

	n.mu.RUnlock()

	if transport == nil {
		n.getLogger().Debug(
			"raft vote requests skipped",
			"term", term,
			"reason", "transport_unavailable",
		)

		return
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peerID := range peerIDs {
		ctx, cancel := n.rpcContext()

		reply, err := transport.RequestVote(
			ctx,
			peerID,
			args,
		)

		cancel()

		if err != nil {
			n.getLogger().Debug(
				"vote request failed",
				"peer_id", peerID,
				"term", term,
				"error", err,
			)

			continue
		}

		n.handleVoteReply(term, reply)
	}
}

func (n *RaftNode) RequestVote(
	args RequestVoteArgs,
) (reply RequestVoteReply) {
	logger := n.getLogger()

	n.mu.Lock()
	defer n.mu.Unlock()

	defer func() {
		n.metrics.IncVoteRequests()

		if reply.VoteGranted {
			n.metrics.IncVotesGranted()
		}
	}()

	reply = RequestVoteReply{
		Term:    n.state.Persistent.CurrentTerm,
		VoterID: n.id,
	}

	// Reject requests from an older term.
	if args.Term < n.state.Persistent.CurrentTerm {
		logger.Debug(
			"vote denied",
			"candidate_id", args.CandidateID,
			"request_term", args.Term,
			"current_term", n.state.Persistent.CurrentTerm,
			"reason", "stale_term",
		)

		return reply
	}

	// A higher-term request must be durable before the new
	// persistent state becomes visible in memory.
	if args.Term > n.state.Persistent.CurrentTerm {
		persistentState := n.state.Persistent
		persistentState.CurrentTerm = args.Term
		persistentState.VotedFor = ""

		if err := n.storage.SaveState(persistentState); err != nil {
			logger.Error(
				"failed to persist higher-term vote state",
				"candidate_id", args.CandidateID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}

		if err := n.storage.Sync(); err != nil {
			logger.Error(
				"failed to sync higher-term vote state",
				"candidate_id", args.CandidateID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}

		n.state.Persistent = persistentState
		n.state.Role = Follower
		n.state.LeaderID = ""

		n.updateStateMetricsLocked()
	}

	reply.Term = n.state.Persistent.CurrentTerm

	// Only nodes in the active voting configuration can
	// participate as election candidates.
	if !membershipIsVoter(
		n.state.Persistent.Membership,
		args.CandidateID,
	) {
		logger.Debug(
			"vote denied",
			"candidate_id", args.CandidateID,
			"request_term", args.Term,
			"current_term", n.state.Persistent.CurrentTerm,
			"reason", "candidate_not_voter",
		)

		return reply
	}

	// We can vote only once per term, unless we already voted
	// for this same candidate.
	if n.state.Persistent.VotedFor != "" &&
		n.state.Persistent.VotedFor != args.CandidateID {
		logger.Debug(
			"vote denied",
			"candidate_id", args.CandidateID,
			"request_term", args.Term,
			"current_term", n.state.Persistent.CurrentTerm,
			"reason", "already_voted",
			"voted_for", n.state.Persistent.VotedFor,
		)

		return reply
	}

	// Candidate's log must be at least as up-to-date as ours.
	if !n.isCandidateLogUpToDate(
		args.LastLogIndex,
		args.LastLogTerm,
	) {
		logger.Debug(
			"vote denied",
			"candidate_id", args.CandidateID,
			"request_term", args.Term,
			"current_term", n.state.Persistent.CurrentTerm,
			"reason", "candidate_log_outdated",
			"last_log_index", args.LastLogIndex,
			"last_log_term", args.LastLogTerm,
		)

		return reply
	}

	// Persist the vote before publishing it to in-memory state.
	persistentState := n.state.Persistent
	persistentState.VotedFor = args.CandidateID

	if err := n.storage.SaveState(persistentState); err != nil {
		reply.Term = n.state.Persistent.CurrentTerm

		logger.Error(
			"failed to persist vote",
			"candidate_id", args.CandidateID,
			"term", n.state.Persistent.CurrentTerm,
			"error", err,
		)

		return reply
	}

	if err := n.storage.Sync(); err != nil {
		reply.Term = n.state.Persistent.CurrentTerm

		logger.Error(
			"failed to sync vote",
			"candidate_id", args.CandidateID,
			"term", n.state.Persistent.CurrentTerm,
			"error", err,
		)

		return reply
	}

	n.state.Persistent = persistentState

	reply.Term = n.state.Persistent.CurrentTerm
	reply.VoteGranted = true
	n.electionElapsed = 0

	logger.Debug(
		"vote granted",
		"candidate_id", args.CandidateID,
		"term", n.state.Persistent.CurrentTerm,
	)

	return reply
}

func (n *RaftNode) handleVoteReply(
	electionTerm Term,
	reply RequestVoteReply,
) {
	logger := n.getLogger()

	n.mu.Lock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		persistentState := n.state.Persistent
		persistentState.CurrentTerm = reply.Term
		persistentState.VotedFor = ""

		if err := n.storage.SaveState(persistentState); err != nil {
			n.mu.Unlock()

			logger.Error(
				"failed to persist higher-term follower transition",
				"higher_term", reply.Term,
				"error", err,
			)

			return
		}

		if err := n.storage.Sync(); err != nil {
			n.mu.Unlock()

			logger.Error(
				"failed to sync higher-term follower transition",
				"higher_term", reply.Term,
				"error", err,
			)

			return
		}

		n.state.Persistent = persistentState
		n.finishElectionLocked("lost")
		n.state.Role = Follower
		n.state.LeaderID = ""
		n.state.Election.VotesReceived = make(map[NodeID]struct{})

		n.updateStateMetricsLocked()
		n.mu.Unlock()

		logger.Info(
			"raft election stepped down due to higher term",
			"election_term", electionTerm,
			"higher_term", reply.Term,
		)

		return
	}

	if n.state.Role != Candidate {
		n.mu.Unlock()
		return
	}

	if electionTerm != n.state.Persistent.CurrentTerm {
		n.mu.Unlock()
		return
	}

	if reply.Term != electionTerm {
		n.mu.Unlock()
		return
	}

	if !reply.VoteGranted {
		n.mu.Unlock()

		logger.Debug(
			"vote request rejected",
			"voter_id", reply.VoterID,
			"term", electionTerm,
		)

		return
	}

	// Only voters in the active membership can contribute
	// to the election quorum.
	if !membershipIsVoter(
		n.state.Persistent.Membership,
		reply.VoterID,
	) {
		n.mu.Unlock()

		logger.Debug(
			"vote ignored",
			"voter_id", reply.VoterID,
			"term", electionTerm,
			"reason", "voter_not_in_membership",
		)

		return
	}

	if _, alreadyReceived := n.state.Election.VotesReceived[reply.VoterID]; alreadyReceived {
		n.mu.Unlock()
		return
	}

	n.state.Election.VotesReceived[reply.VoterID] = struct{}{}

	votes := len(n.state.Election.VotesReceived)

	n.mu.Unlock()

	logger.Debug(
		"vote received",
		"voter_id", reply.VoterID,
		"term", electionTerm,
		"votes", votes,
	)
}

func (n *RaftNode) runElection() {
	n.mu.RLock()

	if n.storageWriteBlocked {
		n.mu.RUnlock()

		n.getLogger().Debug(
			"raft election skipped",
			"reason", "storage_write_blocked",
		)

		return
	}

	n.mu.RUnlock()

	if !n.runPreVote() {
		// The PreVote failed, so wait for another complete election
		// timeout before retrying.
		n.resetElectionTimer()
		return
	}

	// A valid leader may have contacted us while the PreVote RPCs
	// were in flight. Re-check the election state before incrementing
	// the real Raft term.
	n.mu.RLock()

	role := n.state.Role
	electionElapsed := n.electionElapsed
	electionTimeout := n.electionTimeout
	leaderID := n.state.LeaderID

	n.mu.RUnlock()

	if role == Leader {
		return
	}

	if leaderID != "" && electionElapsed < electionTimeout {
		// We heard from a valid leader while PreVote was running.
		return
	}

	if _, err := n.startElection(); err != nil {
		// If the election could not be persisted, don't immediately
		// spin another election attempt.
		n.resetElectionTimer()
		return
	}

	n.requestVotes()
	n.tryBecomeLeader()
}

func (n *RaftNode) runPreVote() bool {
	n.mu.RLock()

	term := n.state.Persistent.CurrentTerm + 1
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	candidateID := n.id
	transport := n.transport
	peerIDs := append([]NodeID(nil), n.peerIDs...)
	membership := n.state.Persistent.Membership

	n.mu.RUnlock()

	// Only a node in the active voting configuration
	// can participate as an election candidate.
	if !membershipIsVoter(membership, candidateID) {
		n.getLogger().Debug(
			"raft prevote skipped",
			"node_id", candidateID,
			"term", term,
			"reason", "candidate_not_voter",
		)

		return false
	}

	votes := map[NodeID]struct{}{
		candidateID: {},
	}

	// A single-node/current-membership quorum may already be satisfied.
	if membershipHasQuorum(membership, votes) {
		return true
	}

	if transport == nil {
		n.getLogger().Debug(
			"raft prevote skipped",
			"term", term,
			"reason", "transport_unavailable",
		)

		return false
	}

	args := PreVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peerID := range peerIDs {
		ctx, cancel := n.rpcContext()

		reply, err := transport.PreVote(
			ctx,
			peerID,
			args,
		)

		cancel()

		if err != nil {
			n.getLogger().Debug(
				"prevote request failed",
				"peer_id", peerID,
				"term", term,
				"error", err,
			)

			continue
		}

		// PreVote deliberately does not change our persistent term.
		if !reply.VoteGranted {
			continue
		}

		votes[peerID] = struct{}{}

		if membershipHasQuorum(membership, votes) {
			n.getLogger().Debug(
				"raft prevote won",
				"term", term,
				"votes", len(votes),
			)

			return true
		}
	}

	n.getLogger().Debug(
		"raft prevote lost",
		"term", term,
		"votes", len(votes),
	)

	return false
}

func (n *RaftNode) PreVote(args PreVoteArgs) PreVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := PreVoteReply{
		Term:        n.state.Persistent.CurrentTerm,
		VoterID:     n.id,
		VoteGranted: false,
	}

	// A leader must never grant a PreVote.
	if n.state.Role == Leader {
		return reply
	}

	// Only candidates in the active voting configuration
	// can participate in pre-voting.
	if !membershipIsVoter(
		n.state.Persistent.Membership,
		args.CandidateID,
	) {
		return reply
	}

	// A PreVote never changes persistent term/vote state.
	if args.Term < n.state.Persistent.CurrentTerm {
		return reply
	}

	// If we recently heard from a valid leader, do not allow an
	// isolated/stale candidate to start another election.
	if n.state.LeaderID != "" &&
		n.electionElapsed < n.electionTimeout {
		return reply
	}

	// Candidate's log must be at least as up-to-date as ours.
	if !n.isCandidateLogUpToDate(
		args.LastLogIndex,
		args.LastLogTerm,
	) {
		return reply
	}

	reply.VoteGranted = true
	return reply
}
