package storage

import "github.com/sanchar127/raftiq/internal/raft"

type Storage interface {
	SaveState(state raft.PersistentState) error
	LoadState() (raft.PersistentState, error)

	AppendEntries(entries []raft.LogEntry) error
	LoadEntries() ([]raft.LogEntry, error)

	Sync() error
	Close() error
}
