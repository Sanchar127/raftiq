package raft

import (
	"context"
	"errors"
	"fmt"
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

	id    NodeID
	state State
	log   *Log

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
	n.mu.Lock()
	defer n.mu.Unlock()

	n.becomeLeaderLocked()
}

func (n *RaftNode) Propose(data []byte) (LogIndex, error) {
	n.mu.Lock()

	if n.state.Role != Leader {
		n.mu.Unlock()
		return 0, fmt.Errorf(
			"node %s is not the leader",
			n.id,
		)
	}

	index := n.log.LastIndex() + 1

	entry := LogEntry{
		Index: index,
		Term:  n.state.Persistent.CurrentTerm,
		Data:  append([]byte(nil), data...),
	}

	// Persist the entry before making it visible in the in-memory
	// Raft log. This keeps the persistent log as the durability
	// boundary for newly proposed entries.
	if err := n.storage.AppendEntries([]LogEntry{entry}); err != nil {
		n.mu.Unlock()
		return 0, fmt.Errorf(
			"persist proposed entry: %w",
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		n.mu.Unlock()
		return 0, fmt.Errorf(
			"sync proposed entry: %w",
			err,
		)
	}

	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()
		return 0, fmt.Errorf(
			"append proposed entry to raft log: %w",
			err,
		)
	}

	// The leader itself counts toward the replication majority.
	//
	// This is especially important for a single-node cluster, where
	// the leader is already the complete majority and therefore the
	// newly appended entry can be committed immediately.
	advanced := n.advanceCommitIndexLocked()

	transport := n.transport
	peerIDs := append([]NodeID(nil), n.peerIDs...)

	n.mu.Unlock()

	// Apply outside the Raft lock because applying may block on the
	// apply channel and must never hold the Raft mutex.
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
		return reply
	}

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			reply.Term = n.state.Persistent.CurrentTerm
			return reply
		}
	}

	reply.Term = n.state.Persistent.CurrentTerm

	if n.state.Persistent.VotedFor != "" &&
		n.state.Persistent.VotedFor != args.CandidateID {
		return reply
	}

	if !n.isCandidateLogUpToDate(
		args.LastLogIndex,
		args.LastLogTerm,
	) {
		return reply
	}

	n.state.Persistent.VotedFor = args.CandidateID

	if err := n.persistStateLocked(); err != nil {
		reply.Term = n.state.Persistent.CurrentTerm
		return reply
	}

	reply.Term = n.state.Persistent.CurrentTerm
	reply.VoteGranted = true
	n.electionElapsed = 0

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
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.Role != Candidate {
		return false
	}

	clusterSize := len(n.peerIDs) + 1
	requiredVotes := clusterSize/2 + 1

	if len(n.state.Election.VotesReceived) < requiredVotes {
		return false
	}

	n.becomeLeaderLocked()

	return true
}

type Peer interface {
	ID() NodeID

	RequestVote(args RequestVoteArgs) RequestVoteReply

	AppendEntries(args AppendEntriesArgs) AppendEntriesReply

	InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply
}

func (n *RaftNode) startElection() (Term, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

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
		return 0, err
	}

	return n.state.Persistent.CurrentTerm, nil
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
			continue
		}

		n.handleVoteReply(term, reply)
	}
}

func (n *RaftNode) handleVoteReply(
	electionTerm Term,
	reply RequestVoteReply,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		n.finishElectionLocked("lost")

		n.state.Persistent.CurrentTerm = reply.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""
		n.state.Election.VotesReceived = make(map[NodeID]struct{})

		if err := n.persistStateLocked(); err != nil {
			return
		}

		return
	}

	if n.state.Role != Candidate {
		return
	}

	if electionTerm != n.state.Persistent.CurrentTerm {
		return
	}

	if reply.Term != electionTerm {
		return
	}

	if !reply.VoteGranted {
		return
	}

	n.state.Election.VotesReceived[reply.VoterID] = struct{}{}
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
		return reply
	}

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()
			return reply
		}
	}

	if args.PrevLogIndex > 0 {
		prevEntry, ok := n.log.Get(args.PrevLogIndex)
		if !ok || prevEntry.Term != args.PrevLogTerm {
			n.mu.Unlock()
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
				return reply
			}

			if err := n.storage.Sync(); err != nil {
				n.mu.Unlock()
				return reply
			}

			n.log.TruncateFrom(replaceFrom)

			for _, entry := range newEntries {
				if err := n.log.Append(entry); err != nil {
					n.mu.Unlock()
					return reply
				}
			}
		} else {
			if err := n.storage.AppendEntries(newEntries); err != nil {
				n.mu.Unlock()
				return reply
			}

			if err := n.storage.Sync(); err != nil {
				n.mu.Unlock()
				return reply
			}

			for _, entry := range newEntries {
				if err := n.log.Append(entry); err != nil {
					n.mu.Unlock()
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

	n.mu.Unlock()

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
			return
		}

		n.handleAppendEntriesReply(
			peerID,
			args,
			reply,
		)

		if reply.Success {
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
			return
		}

		n.mu.Unlock()
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

		n.mu.Unlock()
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

	n.mu.Unlock()

	if advanced {
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
		return fmt.Errorf(
			"raft node %s is already running",
			n.id,
		)
	}

	n.stopCh = make(chan struct{})
	n.doneCh = make(chan struct{})
	n.running = true

	go n.run()

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
	n.mu.Lock()
	defer n.mu.Unlock()

	if index > n.state.Volatile.LastApplied {
		return fmt.Errorf(
			"cannot snapshot unapplied index %d: last applied %d",
			index,
			n.state.Volatile.LastApplied,
		)
	}

	if index > n.state.Volatile.CommitIndex {
		return fmt.Errorf(
			"cannot snapshot uncommitted index %d: commit index %d",
			index,
			n.state.Volatile.CommitIndex,
		)
	}

	if index == 0 {
		return fmt.Errorf("cannot snapshot index 0")
	}

	entry, ok := n.log.Get(index)
	if !ok {
		return fmt.Errorf(
			"cannot snapshot missing log index %d",
			index,
		)
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  entry.Term,
		Data:              append([]byte(nil), data...),
	}

	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		return fmt.Errorf(
			"save snapshot: %w",
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		return fmt.Errorf(
			"sync snapshot: %w",
			err,
		)
	}

	if err := n.log.Compact(snapshot); err != nil {
		return fmt.Errorf(
			"compact log after snapshot: %w",
			err,
		)
	}

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
	n.mu.Lock()

	reply := InstallSnapshotReply{
		Term:       n.state.Persistent.CurrentTerm,
		FollowerID: n.id,
	}

	// 1. Reject snapshots from an older term.
	if args.Term < n.state.Persistent.CurrentTerm {
		n.mu.Unlock()
		return reply
	}

	// 2. Adopt the newer term.
	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Persistent.VotedFor = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()
			return reply
		}
	}

	// 3. This node is now following the snapshot sender.
	n.state.Role = Follower
	n.state.LeaderID = args.LeaderID
	n.electionElapsed = 0

	reply.Term = n.state.Persistent.CurrentTerm

	// 4. Ignore a snapshot that is already covered by our
	// current snapshot boundary.
	if args.LastIncludedIndex <= n.log.LastIncludedIndex() {
		reply.Success = true
		n.mu.Unlock()
		return reply
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              append([]byte(nil), args.Data...),
	}

	// 5. Persist the snapshot before modifying the logical log.
	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		n.mu.Unlock()
		return reply
	}

	if err := n.storage.Sync(); err != nil {
		n.mu.Unlock()
		return reply
	}

	// 6. Restore the state machine before declaring the snapshot
	// installed in Raft state.
	restore := n.snapshotRestore

	n.mu.Unlock()

	if restore != nil {
		if err := restore(snapshot); err != nil {
			return reply
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// 7. Establish the new snapshot boundary.
	if err := n.log.RestoreSnapshot(snapshot); err != nil {
		return reply
	}

	// 8. The snapshot represents committed/applied state.
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
			return
		}

		n.mu.Unlock()
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
