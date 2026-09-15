package kv

import (
	"fmt"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
)

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
