package model

type Snapshot struct {
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Data              []byte
}
