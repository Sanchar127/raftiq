package storage

import "github.com/sanchar127/raftiq/internal/model"

type Storage interface {
	SaveState(state model.PersistentState) error
	LoadState() (model.PersistentState, error)

	AppendEntries(entries []model.LogEntry) error
	ReplaceSuffix(
		from model.LogIndex,
		entries []model.LogEntry,
	) error
	LoadEntries() ([]model.LogEntry, error)

	SaveSnapshot(snapshot model.Snapshot) error
	LoadSnapshot() (model.Snapshot, error)

	Sync() error
	Close() error
}
