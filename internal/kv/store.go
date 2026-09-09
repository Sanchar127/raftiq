package kv

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
)

type Store struct {
	mu    sync.RWMutex
	data  map[string][]byte
	locks *lock.State
}

type snapshotEnvelope struct {
	Version   uint64               `json:"version"`
	Data      map[string][]byte    `json:"data"`
	Locks     map[string]lock.Lock `json:"locks"`
	NextToken uint64               `json:"next_token"`
}

func NewStore() *Store {
	return &Store{
		data:  make(map[string][]byte),
		locks: lock.NewState(),
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return nil, false
	}

	return append([]byte(nil), value...), true
}

func (s *Store) Put(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = append([]byte(nil), value...)
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false
	}

	delete(s.data, key)
	return true
}

func (s *Store) AcquireLock(
	key string,
	ownerID string,
	expiresAt int64,
	grantIndex model.LogIndex,
) (lock.Lock, bool, error) {
	if key == "" {
		return lock.Lock{}, false, lock.ErrInvalidKey
	}

	if ownerID == "" {
		return lock.Lock{}, false, lock.ErrInvalidOwner
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result, acquired := s.locks.Acquire(
		key,
		ownerID,
		expiresAt,
		grantIndex,
	)

	return result, acquired, nil
}

func (s *Store) GetLock(key string) (lock.Lock, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result, ok := s.locks.Get(key)
	return result, ok
}

type snapshotState struct {
	Version   uint64               `json:"version"`
	Data      map[string][]byte    `json:"data"`
	Locks     map[string]lock.Lock `json:"locks"`
	NextToken uint64               `json:"next_token"`
}

const snapshotVersion uint64 = 1

func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string][]byte, len(s.data))

	for key, value := range s.data {
		data[key] = append([]byte(nil), value...)
	}

	locks := make(map[string]lock.Lock, len(s.locks.Locks))

	for key, value := range s.locks.Locks {
		locks[key] = value
	}

	snapshot := snapshotState{
		Version:   snapshotVersion,
		Data:      data,
		Locks:     locks,
		NextToken: s.locks.NextToken,
	}

	result, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("marshal KV snapshot: %w", err)
	}

	return result, nil
}
func (s *Store) Restore(data []byte) error {
	var raw map[string]json.RawMessage

	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshal KV snapshot: %w", err)
	}

	if raw == nil {
		raw = make(map[string]json.RawMessage)
	}

	// Versioned snapshot.
	if versionRaw, ok := raw["version"]; ok {
		var version uint64

		if err := json.Unmarshal(versionRaw, &version); err != nil {
			return fmt.Errorf("decode KV snapshot version: %w", err)
		}

		if version != snapshotVersion {
			return fmt.Errorf(
				"unsupported KV snapshot version %d",
				version,
			)
		}

		var snapshot snapshotState

		if err := json.Unmarshal(data, &snapshot); err != nil {
			return fmt.Errorf("decode KV snapshot: %w", err)
		}

		if snapshot.Data == nil {
			snapshot.Data = make(map[string][]byte)
		}

		if snapshot.Locks == nil {
			snapshot.Locks = make(map[string]lock.Lock)
		}

		restoredData := cloneData(snapshot.Data)
		restoredLocks := cloneLocks(snapshot.Locks)

		s.mu.Lock()
		s.data = restoredData
		s.locks = &lock.State{
			Locks:     restoredLocks,
			NextToken: snapshot.NextToken,
		}
		s.mu.Unlock()

		return nil
	}

	// Legacy version-0 KV-only snapshot.
	var legacyData map[string][]byte

	if err := json.Unmarshal(data, &legacyData); err != nil {
		return fmt.Errorf("decode legacy KV snapshot: %w", err)
	}

	if legacyData == nil {
		legacyData = make(map[string][]byte)
	}

	restoredData := cloneData(legacyData)

	s.mu.Lock()
	s.data = restoredData

	// Legacy snapshots contain no lock state.
	s.locks = lock.NewState()

	s.mu.Unlock()

	return nil
}

func cloneData(
	data map[string][]byte,
) map[string][]byte {
	result := make(map[string][]byte, len(data))

	for key, value := range data {
		result[key] = append([]byte(nil), value...)
	}

	return result
}

func cloneLocks(
	locks map[string]lock.Lock,
) map[string]lock.Lock {
	result := make(map[string]lock.Lock, len(locks))

	for key, value := range locks {
		result[key] = value
	}

	return result
}

func (s *Store) ExpireLock(
	key string,
	expectedToken uint64,
) (lock.Lock, bool, error) {
	if key == "" {
		return lock.Lock{}, false, lock.ErrInvalidKey
	}

	if expectedToken == 0 {
		return lock.Lock{}, false, fmt.Errorf(
			"invalid fencing token",
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result, expired := s.locks.Expire(key, expectedToken)

	return result, expired, nil
}
