package kv

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
