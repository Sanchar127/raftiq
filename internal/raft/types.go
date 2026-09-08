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
