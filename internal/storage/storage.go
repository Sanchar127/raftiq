package storage

import "github.com/sanchar127/raftiq/internal/model"

type Storage interface {
	SaveState(state model.PersistentState) error
	LoadState() (model.PersistentState, error)

	AppendEntries(entries []model.LogEntry) error
	LoadEntries() ([]model.LogEntry, error)

	Sync() error
	Close() error
}
