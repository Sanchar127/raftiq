package raft

type NodeID string

type Term uint64

type LogIndex uint64

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index LogIndex
	Term  Term
	Data  []byte
}

type RequestVoteArgs struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex LogIndex
	LastLogTerm  Term
}

type RequestVoteReply struct {
	Term        Term
	VoterID     NodeID
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

type AppendEntriesReply struct {
	Term       Term
	FollowerID NodeID
	Success    bool
}
