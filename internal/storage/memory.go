
package storage

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

type MemoryStorage struct {
	mu       sync.RWMutex
	state    model.PersistentState
	entries  []model.LogEntry
	snapshot *model.Snapshot
	logger   *slog.Logger
}

func discardMemoryStorageLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

func (s *MemoryStorage) getLogger() *slog.Logger {
	if s.logger == nil {
		return discardMemoryStorageLogger()
	}

	return s.logger
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: make([]model.LogEntry, 0),
		logger:  discardMemoryStorageLogger(),
	}
}

func (s *MemoryStorage) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardMemoryStorageLogger()
	}

	s.logger = logger.With(
		slog.String("component", "storage"),
		slog.String("backend", "memory"),
	)
}

func (s *MemoryStorage) SaveSnapshot(snapshot model.Snapshot) error {
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot.Data = append([]byte(nil), snapshot.Data...)
	s.snapshot = &snapshot

	logger.Debug(
		"memory snapshot saved",
		"snapshot_index", snapshot.LastIncludedIndex,
		"snapshot_term", snapshot.LastIncludedTerm,
		"snapshot_size", len(snapshot.Data),
		"duration", time.Since(startedAt),
	)

	return nil
}

func (s *MemoryStorage) LoadSnapshot() (model.Snapshot, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot == nil {
		logger.Debug(
			"memory snapshot load completed",
			"found", false,
			"duration", time.Since(startedAt),
		)

		return model.Snapshot{}, nil
	}

	snapshot := *s.snapshot
	snapshot.Data = append([]byte(nil), s.snapshot.Data...)

	logger.Debug(
		"memory snapshot loaded",
		"found", true,
		"snapshot_index", snapshot.LastIncludedIndex,
		"snapshot_term", snapshot.LastIncludedTerm,
		"snapshot_size", len(snapshot.Data),
		"duration", time.Since(startedAt),
	)

	return snapshot, nil
}

func (s *MemoryStorage) SaveState(state model.PersistentState) error {
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state

	logger.Debug(
		"memory persistent state saved",
		"term", state.CurrentTerm,
		"duration", time.Since(startedAt),
	)

	return nil
}

func (s *MemoryStorage) LoadState() (model.PersistentState, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	state := s.state

	logger.Debug(
		"memory persistent state loaded",
		"term", state.CurrentTerm,
		"duration", time.Since(startedAt),
	)

	return state, nil
}

func (s *MemoryStorage) AppendEntries(entries []model.LogEntry) error {
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		entry.Data = append([]byte(nil), entry.Data...)
		s.entries = append(s.entries, entry)
	}

	logger.Debug(
		"memory log entries appended",
		"entry_count", len(entries),
		"duration", time.Since(startedAt),
	)

	return nil
}

func (s *MemoryStorage) LoadEntries() ([]model.LogEntry, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]model.LogEntry, len(s.entries))

	for i, entry := range s.entries {
		entries[i] = entry
		entries[i].Data = append([]byte(nil), entry.Data...)
	}

	logger.Debug(
		"memory log entries loaded",
		"entry_count", len(entries),
		"duration", time.Since(startedAt),
	)

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
	startedAt := time.Now()
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	if fromIndex == 0 {
		logger.Error(
			"memory log suffix replacement rejected",
			"from_index", fromIndex,
			"reason", "invalid replacement index",
			"duration", time.Since(startedAt),
		)

		return fmt.Errorf("invalid replacement index: %d", fromIndex)
	}

	if len(entries) > 0 {
		for i, entry := range entries {
			expected := fromIndex + model.LogIndex(i)
			if entry.Index != expected {
				logger.Error(
					"memory log suffix replacement rejected",
					"from_index", fromIndex,
					"entry_count", len(entries),
					"expected_index", expected,
					"actual_index", entry.Index,
					"reason", "non-contiguous replacement entries",
					"duration", time.Since(startedAt),
				)

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

	logger.Debug(
		"memory log suffix replaced",
		"from_index", fromIndex,
		"entry_count", len(entries),
		"duration", time.Since(startedAt),
	)

	return nil
}

