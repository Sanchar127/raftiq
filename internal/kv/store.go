package kv

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStore() *Store {
	return &Store{
		data: make(map[string][]byte),
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

func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string][]byte, len(s.data))

	for key, value := range s.data {
		data[key] = append([]byte(nil), value...)
	}

	result, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("marshal KV snapshot: %w", err)
	}

	return result, nil
}

func (s *Store) Restore(data []byte) error {
	var snapshot map[string][]byte

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("unmarshal KV snapshot: %w", err)
	}

	if snapshot == nil {
		snapshot = make(map[string][]byte)
	}

	restored := make(map[string][]byte, len(snapshot))

	for key, value := range snapshot {
		restored[key] = append([]byte(nil), value...)
	}

	s.mu.Lock()
	s.data = restored
	s.mu.Unlock()

	return nil
}
