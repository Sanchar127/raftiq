package kv

import "github.com/sanchar127/raftiq/internal/lock"

type FencedValue struct {
	Value        []byte
	FencingToken uint64
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
