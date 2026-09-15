package kv

import (
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
