package raft

import "sync"

type RaftNode struct {
	mu sync.RWMutex

	id    NodeID
	state State
	log   *Log
	peers []Peer
}

func NewRaftNode(id NodeID) *RaftNode {
	return &RaftNode{
		id:    id,
		peers: make([]Peer, 0),
		state: State{
			Persistent: PersistentState{
				CurrentTerm: 0,
				VotedFor:    "",
			},
			Volatile: VolatileState{
				CommitIndex: 0,
				LastApplied: 0,
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
		log: NewLog(),
	}
}

func (n *RaftNode) SetPeers(peers []Peer) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.peers = append([]Peer(nil), peers...)
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

func (n *RaftNode) becomeFollower(term Term) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Follower
	n.state.Persistent.CurrentTerm = term
	n.state.Persistent.VotedFor = ""
	n.state.LeaderID = ""
}

func (n *RaftNode) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Candidate
	n.state.Persistent.CurrentTerm++
	n.state.Persistent.VotedFor = n.id
	n.state.LeaderID = ""

	n.state.Election.VotesReceived = map[NodeID]struct{}{
		n.id: {},
	}
}

func (n *RaftNode) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Leader
	n.state.LeaderID = n.id
}

func (n *RaftNode) RequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{
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
	}

	reply.Term = n.state.Persistent.CurrentTerm

	if n.state.Persistent.VotedFor != "" &&
		n.state.Persistent.VotedFor != args.CandidateID {
		return reply
	}

	if !n.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return reply
	}

	n.state.Persistent.VotedFor = args.CandidateID
	reply.VoteGranted = true

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

func (n *RaftNode) recordVote(peerID NodeID, term Term, granted bool) bool {
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

	clusterSize := len(n.peers) + 1
	requiredVotes := clusterSize/2 + 1

	if len(n.state.Election.VotesReceived) < requiredVotes {
		return false
	}

	n.state.Role = Leader
	n.state.LeaderID = n.id

	return true
}

type Peer interface {
	ID() NodeID
	RequestVote(args RequestVoteArgs) RequestVoteReply
}

func (n *RaftNode) startElection() Term {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state.Role = Candidate
	n.state.Persistent.CurrentTerm++
	n.state.Persistent.VotedFor = n.id
	n.state.LeaderID = ""

	n.state.Election.VotesReceived = map[NodeID]struct{}{
		n.id: {},
	}

	return n.state.Persistent.CurrentTerm
}

func (n *RaftNode) requestVotes() {
	n.mu.RLock()

	term := n.state.Persistent.CurrentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	peers := append([]Peer(nil), n.peers...)
	n.mu.RUnlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range peers {
		reply := peer.RequestVote(args)
		n.handleVoteReply(term, reply)
	}
}

func (n *RaftNode) handleVoteReply(electionTerm Term, reply RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = reply.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

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
	n.startElection()
	n.requestVotes()
	n.tryBecomeLeader()
}
