package storage

import (
	"sync"

	"github.com/sanchar127/raftiq/internal/model"
)

type MemoryStorage struct {
	mu      sync.RWMutex
	state   model.PersistentState
	entries []model.LogEntry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: make([]model.LogEntry, 0),
	}
}

func (s *MemoryStorage) SaveState(state model.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state

	return nil
}

func (s *MemoryStorage) LoadState() (model.PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.state, nil
}

func (s *MemoryStorage) AppendEntries(entries []model.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		entry.Data = append([]byte(nil), entry.Data...)
		s.entries = append(s.entries, entry)
	}

	return nil
}

func (s *MemoryStorage) LoadEntries() ([]model.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]model.LogEntry, len(s.entries))

	for i, entry := range s.entries {
		entries[i] = entry
		entries[i].Data = append([]byte(nil), entry.Data...)
	}

	return entries, nil
}

func (s *MemoryStorage) Sync() error {
	return nil
}

func (s *MemoryStorage) Close() error {
	return nil
}

var _ Storage = (*MemoryStorage)(nil)
