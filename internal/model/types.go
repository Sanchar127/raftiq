package model

type NodeID string
type Term uint64
type LogIndex uint64

type LogEntry struct {
	Index LogIndex
	Term  Term
	Data  []byte
}

type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
}
