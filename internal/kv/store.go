package kv

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
)

type Store struct {
	mu     sync.RWMutex
	data   map[string][]byte
	locks  *lock.State
	fenced map[string]FencedValue
	jobs   map[model.JobID]model.Job
	logger *slog.Logger
}

type FencedValue struct {
	Value        []byte
	FencingToken uint64
}

type snapshotEnvelope struct {
	Version   uint64            `json:"version"`
	Data      map[string][]byte `json:"data"`
	Locks     map[string]lock.Lock `json:"locks"`
	NextToken uint64            `json:"next_token"`
}

type snapshotState struct {
	Version   uint64                    `json:"version"`
	Data      map[string][]byte         `json:"data"`
	Locks     map[string]lock.Lock      `json:"locks"`
	NextToken uint64                    `json:"next_token"`
	Fenced    map[string]FencedValue    `json:"fenced"`
	Jobs      map[model.JobID]model.Job `json:"jobs"`
}

const snapshotVersion uint64 = 1

func discardStoreLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *Store) getLogger() *slog.Logger {
	if s.logger == nil {
		return discardStoreLogger()
	}

	return s.logger
}

func (s *Store) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardStoreLogger()
	}

	s.logger = logger.With(
		"component", "kv_store",
	)
}

func NewStore() *Store {
	return &Store{
		data:   make(map[string][]byte),
		locks:  lock.NewState(),
		fenced: make(map[string]FencedValue),
		jobs:   make(map[model.JobID]model.Job),
		logger: discardStoreLogger(),
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		logger.Debug(
			"key lookup missed",
			"operation", "get",
		)

		return nil, false
	}

	logger.Debug(
		"key lookup succeeded",
		"operation", "get",
		"value_bytes", len(value),
	)

	return append([]byte(nil), value...), true
}

func (s *Store) Put(key string, value []byte) {
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = append([]byte(nil), value...)

	logger.Debug(
		"key stored",
		"operation", "put",
		"value_bytes", len(value),
	)
}

func (s *Store) Delete(key string) bool {
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		logger.Debug(
			"key deletion skipped because key was not found",
			"operation", "delete",
		)

		return false
	}

	delete(s.data, key)

	logger.Debug(
		"key deleted",
		"operation", "delete",
	)

	return true
}

func (s *Store) AcquireLock(
	key string,
	ownerID string,
	expiresAt int64,
	grantIndex model.LogIndex,
) (lock.Lock, bool, error) {
	logger := s.getLogger()

	if key == "" {
		logger.Warn(
			"lock acquisition rejected",
			"operation", "acquire_lock",
			"reason", "invalid_key",
		)

		return lock.Lock{}, false, lock.ErrInvalidKey
	}

	if ownerID == "" {
		logger.Warn(
			"lock acquisition rejected",
			"operation", "acquire_lock",
			"reason", "invalid_owner",
		)

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

	if acquired {
		logger.Info(
			"lock acquired",
			"operation", "acquire_lock",
			"grant_index", grantIndex,
			"fencing_token", result.FencingToken,
		)
	} else {
		logger.Debug(
			"lock acquisition rejected",
			"operation", "acquire_lock",
		)
	}

	return result, acquired, nil
}

func (s *Store) GetLock(key string) (lock.Lock, bool) {
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	result, ok := s.locks.Get(key)

	if ok {
		logger.Debug(
			"lock lookup succeeded",
			"operation", "get_lock",
			"fencing_token", result.FencingToken,
		)
	} else {
		logger.Debug(
			"lock lookup missed",
			"operation", "get_lock",
		)
	}

	return result, ok
}

func (s *Store) Snapshot() ([]byte, error) {
	logger := s.getLogger()

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

	fenced := make(map[string]FencedValue, len(s.fenced))

	for key, value := range s.fenced {
		fenced[key] = FencedValue{
			Value:        append([]byte(nil), value.Value...),
			FencingToken: value.FencingToken,
		}
	}

	jobs := make(map[model.JobID]model.Job, len(s.jobs))

	for id, job := range s.jobs {
		jobs[id] = cloneJob(job)
	}

	snapshot := snapshotState{
		Version:   snapshotVersion,
		Data:      data,
		Locks:     locks,
		NextToken: s.locks.NextToken,
		Fenced:    fenced,
		Jobs:      jobs,
	}

	result, err := json.Marshal(snapshot)
	if err != nil {
		logger.Error(
			"KV snapshot serialization failed",
			"operation", "snapshot",
			"error", err,
		)

		return nil, fmt.Errorf("marshal KV snapshot: %w", err)
	}

	logger.Info(
		"KV snapshot created",
		"operation", "snapshot",
		"version", snapshotVersion,
		"data_entries", len(data),
		"locks", len(locks),
		"fenced_values", len(fenced),
		"jobs", len(jobs),
		"snapshot_bytes", len(result),
	)

	return result, nil
}

func (s *Store) Restore(data []byte) error {
	logger := s.getLogger()

	logger.Info(
		"KV snapshot restore started",
		"operation", "restore",
		"snapshot_bytes", len(data),
	)

	var raw map[string]json.RawMessage

	if err := json.Unmarshal(data, &raw); err != nil {
		logger.Error(
			"KV snapshot restore failed",
			"operation", "restore",
			"stage", "decode_envelope",
			"error", err,
		)

		return fmt.Errorf("unmarshal KV snapshot: %w", err)
	}

	if raw == nil {
		raw = make(map[string]json.RawMessage)
	}

	// Versioned snapshot.
	if versionRaw, ok := raw["version"]; ok {
		var version uint64

		if err := json.Unmarshal(versionRaw, &version); err != nil {
			logger.Error(
				"KV snapshot restore failed",
				"operation", "restore",
				"stage", "decode_version",
				"error", err,
			)

			return fmt.Errorf("decode KV snapshot version: %w", err)
		}

		if version != snapshotVersion {
			logger.Warn(
				"KV snapshot restore rejected",
				"operation", "restore",
				"version", version,
				"supported_version", snapshotVersion,
			)

			return fmt.Errorf(
				"unsupported KV snapshot version %d",
				version,
			)
		}

		var snapshot snapshotState

		if err := json.Unmarshal(data, &snapshot); err != nil {
			logger.Error(
				"KV snapshot restore failed",
				"operation", "restore",
				"stage", "decode_snapshot",
				"error", err,
			)

			return fmt.Errorf("decode KV snapshot: %w", err)
		}

		if snapshot.Data == nil {
			snapshot.Data = make(map[string][]byte)
		}

		if snapshot.Locks == nil {
			snapshot.Locks = make(map[string]lock.Lock)
		}

		if snapshot.Fenced == nil {
			snapshot.Fenced = make(map[string]FencedValue)
		}

		if snapshot.Jobs == nil {
			snapshot.Jobs = make(map[model.JobID]model.Job)
		}

		restoredData := cloneData(snapshot.Data)
		restoredLocks := cloneLocks(snapshot.Locks)
		restoredFenced := cloneFenced(snapshot.Fenced)
		restoredJobs := make(map[model.JobID]model.Job, len(snapshot.Jobs))

		for id, job := range snapshot.Jobs {
			restoredJobs[id] = cloneJob(job)
		}

		s.mu.Lock()
		s.data = restoredData
		s.locks = &lock.State{
			Locks:     restoredLocks,
			NextToken: snapshot.NextToken,
		}
		s.fenced = restoredFenced
		s.jobs = restoredJobs
		s.mu.Unlock()

		logger.Info(
			"KV snapshot restored",
			"operation", "restore",
			"version", version,
			"data_entries", len(restoredData),
			"locks", len(restoredLocks),
			"fenced_values", len(restoredFenced),
			"jobs", len(restoredJobs),
			"next_token", snapshot.NextToken,
		)

		return nil
	}

	// Legacy version-0 KV-only snapshot.
	var legacyData map[string][]byte

	if err := json.Unmarshal(data, &legacyData); err != nil {
		logger.Error(
			"legacy KV snapshot restore failed",
			"operation", "restore",
			"stage", "decode_legacy_snapshot",
			"error", err,
		)

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
	s.fenced = make(map[string]FencedValue)
	s.jobs = make(map[model.JobID]model.Job)

	s.mu.Unlock()

	logger.Info(
		"legacy KV snapshot restored",
		"operation", "restore",
		"version", 0,
		"data_entries", len(restoredData),
	)

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
	logger := s.getLogger()

	if key == "" {
		logger.Warn(
			"lock expiration rejected",
			"operation", "expire_lock",
			"reason", "invalid_key",
		)

		return lock.Lock{}, false, lock.ErrInvalidKey
	}

	if expectedToken == 0 {
		logger.Warn(
			"lock expiration rejected",
			"operation", "expire_lock",
			"reason", "invalid_fencing_token",
		)

		return lock.Lock{}, false, fmt.Errorf(
			"invalid fencing token",
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	result, expired := s.locks.Expire(key, expectedToken)

	if expired {
		logger.Info(
			"lock expired",
			"operation", "expire_lock",
			"fencing_token", expectedToken,
		)
	} else {
		logger.Debug(
			"lock expiration skipped",
			"operation", "expire_lock",
			"fencing_token", expectedToken,
		)
	}

	return result, expired, nil
}

func (s *Store) ListLocks() []lock.Lock {
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]lock.Lock, 0, len(s.locks.Locks))

	for _, current := range s.locks.Locks {
		result = append(result, current)
	}

	logger.Debug(
		"locks listed",
		"operation", "list_locks",
		"count", len(result),
	)

	return result
}

func (s *Store) FencedPut(
	key string,
	value []byte,
	fencingToken uint64,
) error {
	logger := s.getLogger()

	if key == "" {
		logger.Warn(
			"fenced write rejected",
			"operation", "fenced_put",
			"reason", "invalid_key",
		)

		return lock.ErrInvalidKey
	}

	if fencingToken == 0 {
		logger.Warn(
			"fenced write rejected",
			"operation", "fenced_put",
			"reason", "invalid_fencing_token",
		)

		return lock.ErrStaleFencingToken
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	currentLock, ok := s.locks.Get(key)
	if !ok {
		logger.Warn(
			"fenced write rejected",
			"operation", "fenced_put",
			"reason", "lock_not_found",
			"fencing_token", fencingToken,
		)

		return lock.ErrLockNotFound
	}

	if currentLock.FencingToken != fencingToken {
		logger.Warn(
			"fenced write rejected",
			"operation", "fenced_put",
			"reason", "stale_fencing_token",
			"fencing_token", fencingToken,
			"current_fencing_token", currentLock.FencingToken,
		)

		return lock.ErrStaleFencingToken
	}

	s.fenced[key] = FencedValue{
		Value:        append([]byte(nil), value...),
		FencingToken: fencingToken,
	}

	logger.Debug(
		"fenced value stored",
		"operation", "fenced_put",
		"value_bytes", len(value),
		"fencing_token", fencingToken,
	)

	return nil
}

func (s *Store) GetFenced(key string) (FencedValue, bool) {
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	current, ok := s.fenced[key]
	if !ok {
		logger.Debug(
			"fenced value lookup missed",
			"operation", "get_fenced",
		)

		return FencedValue{}, false
	}

	current.Value = append([]byte(nil), current.Value...)

	logger.Debug(
		"fenced value lookup succeeded",
		"operation", "get_fenced",
		"value_bytes", len(current.Value),
		"fencing_token", current.FencingToken,
	)

	return current, true
}

func cloneFenced(
	fenced map[string]FencedValue,
) map[string]FencedValue {
	result := make(map[string]FencedValue, len(fenced))

	for key, value := range fenced {
		result[key] = FencedValue{
			Value:        append([]byte(nil), value.Value...),
			FencingToken: value.FencingToken,
		}
	}

	return result
}

func cloneJob(job model.Job) model.Job {
	job.Payload = append([]byte(nil), job.Payload...)
	return job
}