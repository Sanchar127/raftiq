package kv

import (
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

var (
	ErrJobNotFound = errors.New("job not found")
	ErrInvalidJob  = errors.New("invalid job")
)

func (s *Store) CreateJob(job model.Job) error {
	if job.ID == "" {
		return fmt.Errorf("%w: missing job ID", ErrInvalidJob)
	}

	if job.State == "" {
		return fmt.Errorf("%w: missing job state", ErrInvalidJob)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; exists {
		return fmt.Errorf("job %q already exists", job.ID)
	}

	job.Payload = append([]byte(nil), job.Payload...)

	s.jobs[job.ID] = job

	return nil
}

func (s *Store) GetJob(
	id model.JobID,
) (model.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return model.Job{}, false
	}

	job.Payload = append([]byte(nil), job.Payload...)

	return job, true
}

func (s *Store) UpdateJob(job model.Job) error {
	if job.ID == "" {
		return fmt.Errorf("%w: missing job ID", ErrInvalidJob)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; !exists {
		return ErrJobNotFound
	}

	job.Payload = append([]byte(nil), job.Payload...)

	s.jobs[job.ID] = job

	return nil
}

func (s *Store) DeleteJob(id model.JobID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; !exists {
		return false
	}

	delete(s.jobs, id)

	return true
}
