package raft

import "github.com/sanchar127/raftiq/internal/model"

type NodeID = model.NodeID
type Term = model.Term
type LogIndex = model.LogIndex
type LogEntry = model.LogEntry

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

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

type InstallSnapshotArgs struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Data              []byte
}

type InstallSnapshotReply struct {
	Term       Term
	FollowerID NodeID
	Success    bool
}