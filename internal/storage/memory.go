package storage

import (
	"fmt"
	"github.com/sanchar127/raftiq/internal/model"
	"sync"
)

type MemoryStorage struct {
	mu       sync.RWMutex
	state    model.PersistentState
	entries  []model.LogEntry
	snapshot *model.Snapshot
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: make([]model.LogEntry, 0),
	}
}

func (s *MemoryStorage) SaveSnapshot(snapshot model.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot.Data = append([]byte(nil), snapshot.Data...)
	s.snapshot = &snapshot

	return nil
}

func (s *MemoryStorage) LoadSnapshot() (model.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot == nil {
		return model.Snapshot{}, nil
	}

	snapshot := *s.snapshot
	snapshot.Data = append([]byte(nil), s.snapshot.Data...)

	return snapshot, nil
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

func (s *MemoryStorage) ReplaceSuffix(
	fromIndex model.LogIndex,
	entries []model.LogEntry,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if fromIndex == 0 {
		return fmt.Errorf("invalid replacement index: %d", fromIndex)
	}

	if len(entries) > 0 {
		for i, entry := range entries {
			expected := fromIndex + model.LogIndex(i)
			if entry.Index != expected {
				return fmt.Errorf(
					"non-contiguous replacement entries: expected index %d, got %d",
					expected,
					entry.Index,
				)
			}
		}
	}

	keep := 0
	for keep < len(s.entries) && s.entries[keep].Index < fromIndex {
		keep++
	}

	if keep < len(s.entries) {
		s.entries = s.entries[:keep]
	}

	for _, entry := range entries {
		entry.Data = append([]byte(nil), entry.Data...)
		s.entries = append(s.entries, entry)
	}

	return nil
}
