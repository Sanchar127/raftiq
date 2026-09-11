package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
)

const DefaultRPCTimeout = 2 * time.Second

type RaftNode struct {
	mu      sync.RWMutex
	applyMu sync.Mutex

	runMu   sync.Mutex
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	id     NodeID
	state  State
	log    *Log
	logger *slog.Logger

	transport Transport
	peerIDs   []NodeID

	metrics Metrics

	electionStartedAt time.Time

	applyCh chan LogEntry

	storage storage.Storage

	snapshotRestore func(model.Snapshot) error

	electionElapsed  int
	electionTimeout  int
	electionInFlight bool

	heartbeatElapsed int
	heartbeatTimeout int

	tickInterval time.Duration
	rpcTimeout   time.Duration
}

func NewRaftNode(id NodeID) *RaftNode {
	node, err := NewRaftNodeWithStorage(id, storage.NewMemoryStorage())
	if err != nil {
		panic(err)
	}

	return node
}

// SetPeers is retained as a compatibility helper for existing tests.
//
// New production code should use SetTransport so the Raft core depends
// only on the Transport abstraction rather than an in-process Peer.
func (n *RaftNode) SetPeers(peers []Peer) {
	transport := NewLocalTransport()
	peerIDs := make([]NodeID, 0, len(peers))

	for _, peer := range peers {
		if peer == nil {
			continue
		}

		if err := transport.AddPeer(peer); err != nil {
			continue
		}

		peerIDs = append(peerIDs, peer.ID())
	}

	n.mu.Lock()
	n.transport = transport
	n.peerIDs = peerIDs
	n.mu.Unlock()

	n.getLogger().Debug(
		"raft peers configured",
		"peer_count", len(peerIDs),
	)
}

func (n *RaftNode) SetTransport(
	transport Transport,
	peerIDs []NodeID,
) error {
	if transport == nil {
		return errors.New("raft transport is required")
	}

	ids := append([]NodeID(nil), peerIDs...)

	n.mu.Lock()
	n.transport = transport
	n.peerIDs = ids
	n.mu.Unlock()

	n.getLogger().Debug(
		"raft transport configured",
		"peer_count", len(ids),
	)

	return nil
}

func (n *RaftNode) transportSnapshot() (Transport, []NodeID) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.transport, append([]NodeID(nil), n.peerIDs...)
}

func (n *RaftNode) ID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id
}

func (n *RaftNode) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.state
}

func (n *RaftNode) Log() *Log {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.log
}

func (n *RaftNode) becomeFollower(term Term) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Follower
	n.state.Persistent.CurrentTerm = term
	n.state.Persistent.VotedFor = ""
	n.state.LeaderID = ""

	if err := n.persistStateLocked(); err != nil {
		return fmt.Errorf("persist follower transition: %w", err)
	}

	return nil
}

func (n *RaftNode) becomeLeaderLocked() {
	n.state.Role = Leader
	n.state.LeaderID = n.id
	n.heartbeatElapsed = 0

	n.finishElectionLocked("won")
	n.metrics.IncLeaderChanges()

	nextIndex := n.log.LastIndex() + 1

	for _, peerID := range n.peerIDs {
		n.state.Leader.NextIndex[peerID] = nextIndex
		n.state.Leader.MatchIndex[peerID] = 0
	}
}

func (n *RaftNode) becomeLeader() {
	logger := n.getLogger()

	n.mu.Lock()

	n.becomeLeaderLocked()

	term := n.state.Persistent.CurrentTerm
	peerCount := len(n.peerIDs)

	n.mu.Unlock()

	logger.Info(
		"raft node became leader",
		"term", term,
		"peer_count", peerCount,
	)
}

func (n *RaftNode) Propose(data []byte) (LogIndex, error) {
	n.mu.Lock()

	if n.state.Role != Leader {
		role := n.state.Role
		term := n.state.Persistent.CurrentTerm
		nodeID := n.id

		n.mu.Unlock()

		err := fmt.Errorf(
			"node %s is not the leader",
			nodeID,
		)

		n.getLogger().Debug(
			"raft proposal rejected",
			"role", role,
			"term", term,
			"error", err,
		)

		return 0, err
	}

	index := n.log.LastIndex() + 1

	entry := LogEntry{
		Index: index,
		Term:  n.state.Persistent.CurrentTerm,
		Data:  append([]byte(nil), data...),
	}

	if err := n.storage.AppendEntries([]LogEntry{entry}); err != nil {
		n.mu.Unlock()

		n.getLogger().Error(
			"failed to persist proposed raft entry",
			"index", index,
			"term", entry.Term,
			"error", err,
		)

		return 0, fmt.Errorf(
			"persist proposed entry: %w",
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		n.mu.Unlock()

		n.getLogger().Error(
			"failed to sync proposed raft entry",
			"index", index,
			"term", entry.Term,
			"error", err,
		)

		return 0, fmt.Errorf(
			"sync proposed entry: %w",
			err,
		)
	}

	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()

		n.getLogger().Error(
			"failed to append proposed entry to raft log",
			"index", index,
			"term", entry.Term,
			"error", err,
		)

		return 0, fmt.Errorf(
			"append proposed entry to raft log: %w",
			err,
		)
	}

	advanced := n.advanceCommitIndexLocked()

	transport := n.transport
	peerIDs := append([]NodeID(nil), n.peerIDs...)
	term := n.state.Persistent.CurrentTerm

	n.mu.Unlock()

	n.getLogger().Debug(
		"raft proposal accepted",
		"index", index,
		"term", term,
		"data_size", len(data),
		"peer_count", len(peerIDs),
		"commit_advanced", advanced,
	)

	if advanced {
		n.applyCommitted()
	}

	if transport == nil {
		return index, nil
	}

	for _, peerID := range peerIDs {
		n.replicateTo(peerID)
	}

	return index, nil
}

func (n *RaftNode) RequestVote(args RequestVoteArgs) (reply RequestVoteReply) {
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

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			reply.Term = n.state.Persistent.CurrentTerm

			logger.Error(
				"failed to persist higher-term vote state",
				"candidate_id", args.CandidateID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}
	}

	reply.Term = n.state.Persistent.CurrentTerm

	if n.state.Persistent.VotedFor != "" &&
		n.state.Persistent.VotedFor != args.CandidateID {
		logger.Debug(
			"vote denied",
			"candidate_id", args.CandidateID,
			"term", reply.Term,
			"reason", "already_voted",
		)

		return reply
	}

	if !n.isCandidateLogUpToDate(
		args.LastLogIndex,
		args.LastLogTerm,
	) {
		logger.Debug(
			"vote denied",
			"candidate_id", args.CandidateID,
			"term", reply.Term,
			"reason", "candidate_log_outdated",
			"candidate_last_log_index", args.LastLogIndex,
			"candidate_last_log_term", args.LastLogTerm,
		)

		return reply
	}

	n.state.Persistent.VotedFor = args.CandidateID

	if err := n.persistStateLocked(); err != nil {
		reply.Term = n.state.Persistent.CurrentTerm

		logger.Error(
			"failed to persist granted vote",
			"candidate_id", args.CandidateID,
			"term", reply.Term,
			"error", err,
		)

		return reply
	}

	reply.Term = n.state.Persistent.CurrentTerm
	reply.VoteGranted = true
	n.electionElapsed = 0

	logger.Debug(
		"vote granted",
		"candidate_id", args.CandidateID,
		"term", reply.Term,
	)

	return reply
}

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

	if _, alreadyReceived := n.state.Election.VotesReceived[peerID]; alreadyReceived {
		return false
	}

	n.state.Election.VotesReceived[peerID] = struct{}{}

	return true
}

func majority(clusterSize int) int {
	return clusterSize/2 + 1
}

func (n *RaftNode) hasElectionMajority(clusterSize int) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return len(n.state.Election.VotesReceived) >= majority(clusterSize)
}

func (n *RaftNode) tryBecomeLeader() bool {
	logger := n.getLogger()

	n.mu.Lock()

	if n.state.Role != Candidate {
		n.mu.Unlock()
		return false
	}

	clusterSize := len(n.peerIDs) + 1
	requiredVotes := clusterSize/2 + 1

	if len(n.state.Election.VotesReceived) < requiredVotes {
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
		"required_votes", requiredVotes,
	)

	return true
}

type Peer interface {
	ID() NodeID

	RequestVote(args RequestVoteArgs) RequestVoteReply

	AppendEntries(args AppendEntriesArgs) AppendEntriesReply

	InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply
}

func (n *RaftNode) startElection() (Term, error) {
	logger := n.getLogger()

	n.mu.Lock()

	n.state.Role = Candidate
	n.state.Persistent.CurrentTerm++
	n.state.Persistent.VotedFor = n.id
	n.state.LeaderID = ""

	n.state.Election.VotesReceived = map[NodeID]struct{}{
		n.id: {},
	}

	n.electionStartedAt = time.Now()
	n.metrics.IncElections()

	if err := n.persistStateLocked(); err != nil {
		n.finishElectionLocked("failed")

		term := n.state.Persistent.CurrentTerm

		n.mu.Unlock()

		logger.Error(
			"failed to persist election state",
			"term", term,
			"error", err,
		)

		return 0, err
	}

	term := n.state.Persistent.CurrentTerm
	peerCount := len(n.peerIDs)

	n.mu.Unlock()

	logger.Info(
		"raft election started",
		"term", term,
		"peer_count", peerCount,
		"required_votes", majority(peerCount+1),
	)

	return term, nil
}

func (n *RaftNode) requestVotes() {
	n.mu.RLock()

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

func (n *RaftNode) handleVoteReply(
	electionTerm Term,
	reply RequestVoteReply,
) {
	logger := n.getLogger()

	n.mu.Lock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		n.finishElectionLocked("lost")

		n.state.Persistent.CurrentTerm = reply.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""
		n.state.Election.VotesReceived = make(map[NodeID]struct{})

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			logger.Error(
				"failed to persist higher-term follower transition",
				"higher_term", reply.Term,
				"error", err,
			)

			return
		}

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
	if _, err := n.startElection(); err != nil {
		return
	}

	n.requestVotes()
	n.tryBecomeLeader()
}

func (n *RaftNode) Tick() bool {
	electionDue, heartbeatDue := n.tick()

	if heartbeatDue {
		n.heartbeat()
	}

	return electionDue
}

func (n *RaftNode) SetElectionTimeout(timeout int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.electionTimeout = timeout
}

func (n *RaftNode) onElectionTimeout() {
	state := n.State()

	if state.Role == Leader {
		return
	}

	n.getLogger().Debug(
		"raft election timeout reached",
		"term", state.Persistent.CurrentTerm,
		"role", state.Role,
	)

	if _, err := n.startElection(); err != nil {
		return
	}

	n.requestVotes()
}

func (n *RaftNode) resetElectionTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.electionElapsed = 0
}

func (n *RaftNode) AppendEntries(
	args AppendEntriesArgs,
) (reply AppendEntriesReply) {
	startedAt := time.Now()

	n.mu.Lock()

	defer func() {
		result := "failure"
		if reply.Success {
			result = "success"
		}

		n.metrics.IncAppendEntries(args.LeaderID, result)

		if !reply.Success {
			n.metrics.IncAppendEntriesFailures(args.LeaderID)
		}

		n.metrics.ObserveAppendEntriesDuration(
			args.LeaderID,
			time.Since(startedAt),
		)
	}()

	reply = AppendEntriesReply{
		Term:       n.state.Persistent.CurrentTerm,
		FollowerID: n.id,
	}

	if args.Term < n.state.Persistent.CurrentTerm {
		n.mu.Unlock()

		n.getLogger().Debug(
			"append entries rejected",
			"leader_id", args.LeaderID,
			"request_term", args.Term,
			"current_term", reply.Term,
			"entry_count", len(args.Entries),
			"reason", "stale_term",
		)

		return reply
	}

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			n.getLogger().Error(
				"failed to persist higher-term append entries state",
				"leader_id", args.LeaderID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}
	}

	if args.PrevLogIndex > 0 {
		prevEntry, ok := n.log.Get(args.PrevLogIndex)
		if !ok || prevEntry.Term != args.PrevLogTerm {
			n.mu.Unlock()

			n.getLogger().Debug(
				"append entries rejected",
				"leader_id", args.LeaderID,
				"request_term", args.Term,
				"entry_count", len(args.Entries),
				"reason", "log_mismatch",
				"prev_log_index", args.PrevLogIndex,
				"prev_log_term", args.PrevLogTerm,
			)

			return reply
		}
	}

	n.state.Role = Follower
	n.state.LeaderID = args.LeaderID
	n.electionElapsed = 0

	firstNew := -1
	replaceFrom := model.LogIndex(0)

	for i, entry := range args.Entries {
		existing, ok := n.log.Get(entry.Index)
		if !ok {
			firstNew = i
			break
		}

		if existing.Term != entry.Term {
			firstNew = i
			replaceFrom = entry.Index
			break
		}
	}

	if firstNew >= 0 {
		newEntries := cloneEntries(args.Entries[firstNew:])

		if replaceFrom > 0 {
			if err := n.storage.ReplaceSuffix(
				replaceFrom,
				newEntries,
			); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to replace raft log suffix",
					"leader_id", args.LeaderID,
					"replace_from", replaceFrom,
					"entry_count", len(newEntries),
					"error", err,
				)

				return reply
			}

			if err := n.storage.Sync(); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to sync replaced raft log suffix",
					"leader_id", args.LeaderID,
					"replace_from", replaceFrom,
					"error", err,
				)

				return reply
			}

			n.log.TruncateFrom(replaceFrom)

			for _, entry := range newEntries {
				if err := n.log.Append(entry); err != nil {
					n.mu.Unlock()

					n.getLogger().Error(
						"failed to append replicated raft entry",
						"leader_id", args.LeaderID,
						"index", entry.Index,
						"error", err,
					)

					return reply
				}
			}
		} else {
			if err := n.storage.AppendEntries(newEntries); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to persist replicated raft entries",
					"leader_id", args.LeaderID,
					"entry_count", len(newEntries),
					"error", err,
				)

				return reply
			}

			if err := n.storage.Sync(); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to sync replicated raft entries",
					"leader_id", args.LeaderID,
					"entry_count", len(newEntries),
					"error", err,
				)

				return reply
			}

			for _, entry := range newEntries {
				if err := n.log.Append(entry); err != nil {
					n.mu.Unlock()

					n.getLogger().Error(
						"failed to append replicated raft entry",
						"leader_id", args.LeaderID,
						"index", entry.Index,
						"error", err,
					)

					return reply
				}
			}
		}
	}

	commitAdvanced := false

	if args.LeaderCommit > n.state.Volatile.CommitIndex {
		lastIndex := n.log.LastIndex()
		oldCommitIndex := n.state.Volatile.CommitIndex

		if args.LeaderCommit < lastIndex {
			n.state.Volatile.CommitIndex = args.LeaderCommit
		} else {
			n.state.Volatile.CommitIndex = lastIndex
		}

		commitAdvanced =
			n.state.Volatile.CommitIndex > oldCommitIndex
	}

	reply.Term = n.state.Persistent.CurrentTerm
	reply.Success = true

	entryCount := len(args.Entries)
	commitIndex := n.state.Volatile.CommitIndex

	n.mu.Unlock()

	if entryCount > 0 {
		n.getLogger().Debug(
			"append entries accepted",
			"leader_id", args.LeaderID,
			"term", reply.Term,
			"entry_count", entryCount,
			"commit_index", commitIndex,
			"commit_advanced", commitAdvanced,
		)
	}

	if commitAdvanced {
		n.applyCommitted()
	}

	return reply
}

func (n *RaftNode) ApplyCh() <-chan LogEntry {
	return n.applyCh
}

func (n *RaftNode) applyCommitted() {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	for {
		n.mu.Lock()

		if n.state.Volatile.LastApplied >=
			n.state.Volatile.CommitIndex {
			n.mu.Unlock()
			return
		}

		nextIndex := n.state.Volatile.LastApplied + 1

		entry, ok := n.log.Get(nextIndex)
		if !ok {
			n.mu.Unlock()
			return
		}

		n.mu.Unlock()

		n.applyCh <- entry

		n.mu.Lock()

		if n.state.Volatile.LastApplied < nextIndex {
			n.state.Volatile.LastApplied = nextIndex
		}

		n.mu.Unlock()
	}
}

func (n *RaftNode) buildAppendEntries(
	peerID NodeID,
) (AppendEntriesArgs, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.state.Role != Leader {
		return AppendEntriesArgs{}, false
	}

	nextIndex, ok := n.state.Leader.NextIndex[peerID]
	if !ok {
		return AppendEntriesArgs{}, false
	}

	args := AppendEntriesArgs{
		Term:         n.state.Persistent.CurrentTerm,
		LeaderID:     n.id,
		LeaderCommit: n.state.Volatile.CommitIndex,
	}

	if nextIndex > 1 {
		prevIndex := nextIndex - 1

		prevEntry, ok := n.log.Get(prevIndex)
		if !ok {
			return AppendEntriesArgs{}, false
		}

		args.PrevLogIndex = prevIndex
		args.PrevLogTerm = prevEntry.Term
	}

	for index := nextIndex; index <= n.log.LastIndex(); index++ {
		entry, ok := n.log.Get(index)
		if !ok {
			return AppendEntriesArgs{}, false
		}

		entry.Data = append([]byte(nil), entry.Data...)
		args.Entries = append(args.Entries, entry)
	}

	return args, true
}

func (n *RaftNode) replicateTo(peerID NodeID) {
	for {
		n.mu.RLock()
		transport := n.transport
		n.mu.RUnlock()

		if transport == nil {
			return
		}

		if n.sendInstallSnapshot(peerID) {
			continue
		}

		args, ok := n.buildAppendEntries(peerID)
		if !ok {
			return
		}

		ctx, cancel := n.rpcContext()

		reply, err := transport.AppendEntries(
			ctx,
			peerID,
			args,
		)

		cancel()

		if err != nil {
			n.getLogger().Debug(
				"append entries transport failure",
				"peer_id", peerID,
				"term", args.Term,
				"entry_count", len(args.Entries),
				"error", err,
			)

			return
		}

		n.handleAppendEntriesReply(
			peerID,
			args,
			reply,
		)

		if reply.Success {
			if len(args.Entries) > 0 {
				n.getLogger().Debug(
					"raft entries replicated",
					"peer_id", peerID,
					"term", args.Term,
					"entry_count", len(args.Entries),
					"last_entry_index", args.Entries[len(args.Entries)-1].Index,
				)
			}

			return
		}

		n.mu.RLock()
		role := n.state.Role
		n.mu.RUnlock()

		if role != Leader {
			return
		}
	}
}

func (n *RaftNode) handleAppendEntriesReply(
	peerID NodeID,
	args AppendEntriesArgs,
	reply AppendEntriesReply,
) {
	n.mu.Lock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = reply.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			n.getLogger().Error(
				"failed to persist higher-term follower transition",
				"peer_id", peerID,
				"higher_term", reply.Term,
				"error", err,
			)

			return
		}

		n.mu.Unlock()

		n.getLogger().Info(
			"raft leader stepped down after higher term",
			"peer_id", peerID,
			"higher_term", reply.Term,
		)

		return
	}

	if n.state.Role != Leader {
		n.mu.Unlock()
		return
	}

	if args.Term != n.state.Persistent.CurrentTerm {
		n.mu.Unlock()
		return
	}

	if !reply.Success {
		if nextIndex := n.state.Leader.NextIndex[peerID]; nextIndex > 1 {
			n.state.Leader.NextIndex[peerID]--
		}

		nextIndex := n.state.Leader.NextIndex[peerID]

		n.mu.Unlock()

		n.getLogger().Debug(
			"raft log replication rejected",
			"peer_id", peerID,
			"term", args.Term,
			"next_index", nextIndex,
		)

		return
	}

	if len(args.Entries) == 0 {
		n.mu.Unlock()
		return
	}

	lastReplicated :=
		args.Entries[len(args.Entries)-1].Index

	if lastReplicated > n.state.Leader.MatchIndex[peerID] {
		n.state.Leader.MatchIndex[peerID] = lastReplicated
		n.state.Leader.NextIndex[peerID] = lastReplicated + 1
	}

	advanced := n.advanceCommitIndexLocked()
	commitIndex := n.state.Volatile.CommitIndex

	n.mu.Unlock()

	if advanced {
		n.getLogger().Debug(
			"raft commit index advanced",
			"commit_index", commitIndex,
			"peer_id", peerID,
		)

		n.applyCommitted()
	}
}

func (n *RaftNode) advanceCommitIndexLocked() bool {
	if n.state.Role != Leader {
		return false
	}

	oldCommitIndex := n.state.Volatile.CommitIndex

	clusterSize := len(n.peerIDs) + 1
	majority := clusterSize/2 + 1

	for index := n.state.Volatile.CommitIndex + 1; index <= n.log.LastIndex(); index++ {
		if n.logTerm(index) != n.state.Persistent.CurrentTerm {
			continue
		}

		replicated := 1

		for _, peerID := range n.peerIDs {
			if n.state.Leader.MatchIndex[peerID] >= index {
				replicated++
			}
		}

		if replicated >= majority {
			n.state.Volatile.CommitIndex = index
		}
	}

	return n.state.Volatile.CommitIndex > oldCommitIndex
}

func (n *RaftNode) logTerm(index LogIndex) Term {
	entry, ok := n.log.Get(index)
	if !ok {
		return 0
	}

	return entry.Term
}

func (n *RaftNode) advanceCommitIndex() {
	n.mu.Lock()

	advanced := n.advanceCommitIndexLocked()

	n.mu.Unlock()

	if advanced {
		n.getLogger().Debug(
			"raft commit index advanced",
			"commit_index", n.State().Volatile.CommitIndex,
		)

		n.applyCommitted()
	}
}

func (n *RaftNode) heartbeatDue() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.Role != Leader {
		return false
	}

	n.heartbeatElapsed++

	if n.heartbeatElapsed < n.heartbeatTimeout {
		return false
	}

	n.heartbeatElapsed = 0
	return true
}

func (n *RaftNode) sendHeartbeats() {
	n.mu.RLock()

	if n.state.Role != Leader {
		n.mu.RUnlock()
		return
	}

	peerIDs := append([]NodeID(nil), n.peerIDs...)

	n.mu.RUnlock()

	for _, peerID := range peerIDs {
		go n.replicateTo(peerID)
	}
}

func (n *RaftNode) sendHeartbeat(peerID NodeID) {
	n.mu.RLock()
	transport := n.transport
	n.mu.RUnlock()

	if transport == nil {
		return
	}

	args, ok := n.buildAppendEntries(peerID)
	if !ok {
		return
	}

	if len(args.Entries) > 0 {
		return
	}

	ctx, cancel := n.rpcContext()

	reply, err := transport.AppendEntries(
		ctx,
		peerID,
		args,
	)

	cancel()

	if err != nil {
		n.getLogger().Debug(
			"heartbeat transport failure",
			"peer_id", peerID,
			"term", args.Term,
			"error", err,
		)

		return
	}

	n.handleAppendEntriesReply(
		peerID,
		args,
		reply,
	)
}

func (n *RaftNode) heartbeat() {
	n.mu.RLock()

	if n.state.Role != Leader {
		n.mu.RUnlock()
		return
	}

	peerIDs := append([]NodeID(nil), n.peerIDs...)

	n.mu.RUnlock()

	for _, peerID := range peerIDs {
		n.replicateTo(peerID)
	}
}

func NewRaftNodeWithStorage(
	id NodeID,
	store storage.Storage,
) (*RaftNode, error) {
	if store == nil {
		return nil, fmt.Errorf("storage must not be nil")
	}

	persistentState, err := store.LoadState()
	if err != nil {
		return nil, fmt.Errorf(
			"load persistent state: %w",
			err,
		)
	}

	entries, err := store.LoadEntries()
	if err != nil {
		return nil, fmt.Errorf(
			"load log entries: %w",
			err,
		)
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf(
			"load snapshot: %w",
			err,
		)
	}

	log := NewLog()

	for _, entry := range entries {
		if err := log.Append(entry); err != nil {
			return nil, fmt.Errorf(
				"restore log entry %d: %w",
				entry.Index,
				err,
			)
		}
	}

	commitIndex := LogIndex(0)
	lastApplied := LogIndex(0)

	if snapshot.LastIncludedIndex > 0 {
		if err := log.RestoreSnapshot(snapshot); err != nil {
			return nil, fmt.Errorf(
				"restore snapshot boundary: %w",
				err,
			)
		}

		commitIndex = snapshot.LastIncludedIndex
		lastApplied = snapshot.LastIncludedIndex
	}

	return &RaftNode{
		id:        id,
		storage:   store,
		transport: NewLocalTransport(),
		peerIDs:   make([]NodeID, 0),
		applyCh:   make(chan LogEntry, 100),
		metrics:   NoopMetrics{},
		logger:    discardRaftLogger(),

		state: State{
			Persistent: persistentState,

			Volatile: VolatileState{
				CommitIndex: commitIndex,
				LastApplied: lastApplied,
			},

			Leader: LeaderState{
				NextIndex:  make(map[NodeID]LogIndex),
				MatchIndex: make(map[NodeID]LogIndex),
			},

			Election: ElectionState{
				VotesReceived: make(map[NodeID]struct{}),
			},

			Role:     Follower,
			LeaderID: "",
		},

		log:              log,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		tickInterval:     100 * time.Millisecond,
		rpcTimeout:       DefaultRPCTimeout,
	}, nil
}

func (n *RaftNode) Storage() storage.Storage {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.storage
}

func (n *RaftNode) SetRPCTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("raft RPC timeout must be positive")
	}

	n.mu.Lock()
	n.rpcTimeout = timeout
	n.mu.Unlock()

	return nil
}

func (n *RaftNode) rpcContext() (context.Context, context.CancelFunc) {
	n.mu.RLock()
	timeout := n.rpcTimeout
	n.mu.RUnlock()

	if timeout <= 0 {
		timeout = DefaultRPCTimeout
	}

	return context.WithTimeout(
		context.Background(),
		timeout,
	)
}

func (n *RaftNode) persistStateLocked() error {
	if err := n.storage.SaveState(n.state.Persistent); err != nil {
		return fmt.Errorf(
			"save persistent state: %w",
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		return fmt.Errorf(
			"sync persistent state: %w",
			err,
		)
	}

	return nil
}

func (n *RaftNode) Start() error {
	n.runMu.Lock()
	defer n.runMu.Unlock()

	if n.running {
		err := fmt.Errorf(
			"raft node %s is already running",
			n.id,
		)

		n.getLogger().Warn(
			"raft node start rejected",
			"error", err,
		)

		return err
	}

	n.stopCh = make(chan struct{})
	n.doneCh = make(chan struct{})
	n.running = true

	go n.run()

	n.getLogger().Info(
		"raft node started",
		"tick_interval", n.tickInterval.String(),
		"election_timeout", n.electionTimeout,
		"heartbeat_timeout", n.heartbeatTimeout,
	)

	return nil
}

func (n *RaftNode) run() {
	defer close(n.doneCh)

	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.runTick()

		case <-n.stopCh:
			return
		}
	}
}

func (n *RaftNode) runTick() {
	electionDue, heartbeatDue := n.tick()

	if heartbeatDue {
		go n.heartbeat()
	}

	if electionDue {
		n.startElectionIfNeeded()
	}
}

func (n *RaftNode) startElectionIfNeeded() {
	n.runMu.Lock()

	if n.electionInFlight {
		n.runMu.Unlock()
		return
	}

	n.electionInFlight = true

	n.runMu.Unlock()

	go func() {
		defer func() {
			n.runMu.Lock()
			n.electionInFlight = false
			n.runMu.Unlock()
		}()

		n.runElection()
	}()
}

func (n *RaftNode) Stop() {
	n.runMu.Lock()

	if !n.running {
		n.runMu.Unlock()
		return
	}

	close(n.stopCh)
	doneCh := n.doneCh
	n.running = false

	n.runMu.Unlock()

	<-doneCh

	n.getLogger().Info(
		"raft node stopped",
	)
}

func (n *RaftNode) tick() (
	electionDue,
	heartbeatDue bool,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.electionElapsed++

	if n.state.Role == Leader {
		n.heartbeatElapsed++

		if n.heartbeatElapsed >= n.heartbeatTimeout {
			n.heartbeatElapsed = 0
			heartbeatDue = true
		}
	}

	electionDue = n.electionElapsed >= n.electionTimeout

	if electionDue {
		n.electionElapsed = 0
	}

	return electionDue, heartbeatDue
}

func (n *RaftNode) WaitApplied(
	ctx context.Context,
	index LogIndex,
) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.RLock()
		applied := n.state.Volatile.LastApplied >= index
		n.mu.RUnlock()

		if applied {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n *RaftNode) CreateSnapshot(
	index LogIndex,
	data []byte,
) error {
	logger := n.getLogger()

	n.mu.Lock()

	if index > n.state.Volatile.LastApplied {
		err := fmt.Errorf(
			"cannot snapshot unapplied index %d: last applied %d",
			index,
			n.state.Volatile.LastApplied,
		)

		lastApplied := n.state.Volatile.LastApplied

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"last_applied", lastApplied,
			"error", err,
		)

		return err
	}

	if index > n.state.Volatile.CommitIndex {
		err := fmt.Errorf(
			"cannot snapshot uncommitted index %d: commit index %d",
			index,
			n.state.Volatile.CommitIndex,
		)

		commitIndex := n.state.Volatile.CommitIndex

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"commit_index", commitIndex,
			"error", err,
		)

		return err
	}

	if index == 0 {
		err := fmt.Errorf("cannot snapshot index 0")

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"error", err,
		)

		return err
	}

	entry, ok := n.log.Get(index)
	if !ok {
		err := fmt.Errorf(
			"cannot snapshot missing log index %d",
			index,
		)

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"error", err,
		)

		return err
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  entry.Term,
		Data:              append([]byte(nil), data...),
	}

	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		wrappedErr := fmt.Errorf(
			"save snapshot: %w",
			err,
		)

		n.mu.Unlock()

		logger.Error(
			"failed to persist snapshot",
			"last_included_index", index,
			"last_included_term", entry.Term,
			"error", wrappedErr,
		)

		return wrappedErr
	}

	if err := n.storage.Sync(); err != nil {
		wrappedErr := fmt.Errorf(
			"sync snapshot: %w",
			err,
		)

		n.mu.Unlock()

		logger.Error(
			"failed to sync snapshot",
			"last_included_index", index,
			"last_included_term", entry.Term,
			"error", wrappedErr,
		)

		return wrappedErr
	}

	if err := n.log.Compact(snapshot); err != nil {
		wrappedErr := fmt.Errorf(
			"compact log after snapshot: %w",
			err,
		)

		n.mu.Unlock()

		logger.Error(
			"failed to compact raft log after snapshot",
			"last_included_index", index,
			"last_included_term", entry.Term,
			"error", wrappedErr,
		)

		return wrappedErr
	}

	n.mu.Unlock()

	logger.Info(
		"raft snapshot created",
		"last_included_index", index,
		"last_included_term", entry.Term,
		"snapshot_size", len(data),
	)

	return nil
}

func (n *RaftNode) Snapshot() (model.Snapshot, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	snapshot, err := n.storage.LoadSnapshot()
	if err != nil {
		return model.Snapshot{}, fmt.Errorf(
			"load snapshot: %w",
			err,
		)
	}

	snapshot.Data = append([]byte(nil), snapshot.Data...)

	return snapshot, nil
}

func (n *RaftNode) SetSnapshotRestore(
	restore func(model.Snapshot) error,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.snapshotRestore = restore
}

func (n *RaftNode) InstallSnapshot(
	args InstallSnapshotArgs,
) InstallSnapshotReply {
	logger := n.getLogger()

	n.mu.Lock()

	reply := InstallSnapshotReply{
		Term:       n.state.Persistent.CurrentTerm,
		FollowerID: n.id,
	}

	if args.Term < n.state.Persistent.CurrentTerm {
		n.mu.Unlock()

		logger.Debug(
			"snapshot rejected",
			"leader_id", args.LeaderID,
			"request_term", args.Term,
			"current_term", reply.Term,
			"reason", "stale_term",
		)

		return reply
	}

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Persistent.VotedFor = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			logger.Error(
				"failed to persist term from snapshot",
				"leader_id", args.LeaderID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}
	}

	n.state.Role = Follower
	n.state.LeaderID = args.LeaderID
	n.electionElapsed = 0

	reply.Term = n.state.Persistent.CurrentTerm

	if args.LastIncludedIndex <= n.log.LastIncludedIndex() {
		reply.Success = true
		n.mu.Unlock()

		logger.Debug(
			"snapshot already installed",
			"leader_id", args.LeaderID,
			"last_included_index", args.LastIncludedIndex,
		)

		return reply
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              append([]byte(nil), args.Data...),
	}

	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to persist installed snapshot",
			"leader_id", args.LeaderID,
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return reply
	}

	if err := n.storage.Sync(); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to sync installed snapshot",
			"leader_id", args.LeaderID,
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return reply
	}

	restore := n.snapshotRestore

	n.mu.Unlock()

	if restore != nil {
		if err := restore(snapshot); err != nil {
			logger.Error(
				"failed to restore state machine from snapshot",
				"leader_id", args.LeaderID,
				"last_included_index", args.LastIncludedIndex,
				"error", err,
			)

			return reply
		}
	}

	n.mu.Lock()

	if err := n.log.RestoreSnapshot(snapshot); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to restore raft log snapshot boundary",
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return reply
	}

	if n.state.Volatile.CommitIndex <
		snapshot.LastIncludedIndex {
		n.state.Volatile.CommitIndex =
			snapshot.LastIncludedIndex
	}

	if n.state.Volatile.LastApplied <
		snapshot.LastIncludedIndex {
		n.state.Volatile.LastApplied =
			snapshot.LastIncludedIndex
	}

	reply.Term = n.state.Persistent.CurrentTerm
	reply.Success = true

	n.mu.Unlock()

	logger.Info(
		"raft snapshot installed",
		"leader_id", args.LeaderID,
		"term", args.Term,
		"last_included_index", args.LastIncludedIndex,
		"last_included_term", args.LastIncludedTerm,
		"snapshot_size", len(args.Data),
	)

	return reply
}

func (n *RaftNode) buildInstallSnapshot(
	peerID NodeID,
) (InstallSnapshotArgs, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.state.Role != Leader {
		return InstallSnapshotArgs{}, false
	}

	nextIndex, ok := n.state.Leader.NextIndex[peerID]
	if !ok {
		return InstallSnapshotArgs{}, false
	}

	snapshot, err := n.storage.LoadSnapshot()
	if err != nil {
		return InstallSnapshotArgs{}, false
	}

	if snapshot.LastIncludedIndex == 0 {
		return InstallSnapshotArgs{}, false
	}

	if nextIndex > snapshot.LastIncludedIndex {
		return InstallSnapshotArgs{}, false
	}

	return InstallSnapshotArgs{
		Term:              n.state.Persistent.CurrentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              append([]byte(nil), snapshot.Data...),
	}, true
}

func (n *RaftNode) handleInstallSnapshotReply(
	peerID NodeID,
	args InstallSnapshotArgs,
	reply InstallSnapshotReply,
) {
	n.mu.Lock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = reply.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			n.getLogger().Error(
				"failed to persist higher-term follower transition",
				"peer_id", peerID,
				"higher_term", reply.Term,
				"error", err,
			)

			return
		}

		n.mu.Unlock()

		n.getLogger().Info(
			"raft leader stepped down after higher-term snapshot reply",
			"peer_id", peerID,
			"higher_term", reply.Term,
		)

		return
	}

	if n.state.Role != Leader {
		n.mu.Unlock()
		return
	}

	if args.Term != n.state.Persistent.CurrentTerm {
		n.mu.Unlock()
		return
	}

	if !reply.Success {
		n.mu.Unlock()

		n.getLogger().Debug(
			"snapshot replication rejected",
			"peer_id", peerID,
			"term", args.Term,
			"last_included_index", args.LastIncludedIndex,
		)

		return
	}

	if args.LastIncludedIndex >
		n.state.Leader.MatchIndex[peerID] {
		n.state.Leader.MatchIndex[peerID] =
			args.LastIncludedIndex
	}

	nextIndex := args.LastIncludedIndex + 1

	if nextIndex > n.state.Leader.NextIndex[peerID] {
		n.state.Leader.NextIndex[peerID] = nextIndex
	}

	advanced := n.advanceCommitIndexLocked()

	n.mu.Unlock()

	n.getLogger().Info(
		"raft snapshot replicated",
		"peer_id", peerID,
		"term", args.Term,
		"last_included_index", args.LastIncludedIndex,
	)

	if advanced {
		n.applyCommitted()
	}
}

func (n *RaftNode) sendInstallSnapshot(
	peerID NodeID,
) bool {
	n.mu.RLock()
	transport := n.transport
	n.mu.RUnlock()

	if transport == nil {
		return false
	}

	args, ok := n.buildInstallSnapshot(peerID)
	if !ok {
		return false
	}

	ctx, cancel := n.rpcContext()

	reply, err := transport.InstallSnapshot(
		ctx,
		peerID,
		args,
	)

	cancel()

	if err != nil {
		n.getLogger().Debug(
			"snapshot replication transport failure",
			"peer_id", peerID,
			"term", args.Term,
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return false
	}

	n.handleInstallSnapshotReply(
		peerID,
		args,
		reply,
	)

	return reply.Success
}

func cloneEntries(entries []LogEntry) []LogEntry {
	cloned := make([]LogEntry, len(entries))

	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}

	return cloned
}

func (n *RaftNode) SetMetrics(metrics Metrics) {
	if metrics == nil {
		metrics = NoopMetrics{}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.metrics = metrics
	n.updateStateMetricsLocked()
}

func (n *RaftNode) updateStateMetricsLocked() {
	n.metrics.SetCurrentTerm(
		n.state.Persistent.CurrentTerm,
	)

	n.metrics.SetRole(
		n.state.Role,
	)

	n.metrics.SetCommitIndex(
		n.state.Volatile.CommitIndex,
	)

	n.metrics.SetLastApplied(
		n.state.Volatile.LastApplied,
	)

	n.metrics.SetLastLogIndex(
		n.log.LastIndex(),
	)

	n.metrics.SetLogSize(
		n.log.Size(),
	)
}

func (n *RaftNode) finishElectionLocked(result string) {
	if n.electionStartedAt.IsZero() {
		return
	}

	n.metrics.ObserveElectionDuration(
		time.Since(n.electionStartedAt),
		result,
	)

	n.electionStartedAt = time.Time{}
}

func discardRaftLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

func (n *RaftNode) getLogger() *slog.Logger {
	n.mu.RLock()
	logger := n.logger
	n.mu.RUnlock()

	if logger == nil {
		return discardRaftLogger()
	}

	return logger
}

func (n *RaftNode) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardRaftLogger()
	}

	logger = logger.With(
		slog.String("component", "raft"),
		slog.String("node_id", string(n.id)),
	)

	n.mu.Lock()
	n.logger = logger
	n.mu.Unlock()
}
