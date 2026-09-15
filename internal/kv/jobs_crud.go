package kv

import (
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

// CreateJob inserts a new job into the state machine.
func (s *Store) CreateJob(job model.Job) error {
	logger := s.getLogger()

	if job.ID == "" {
		logger.Warn(
			"job creation rejected",
			"operation", "create_job",
			"reason", "missing_job_id",
		)

		return fmt.Errorf("%w: missing job ID", ErrInvalidJob)
	}

	if job.State == "" {
		logger.Warn(
			"job creation rejected",
			"operation", "create_job",
			"reason", "missing_job_state",
		)

		return fmt.Errorf("%w: missing job state", ErrInvalidJob)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; exists {
		logger.Warn(
			"job creation rejected",
			"operation", "create_job",
			"reason", "job_already_exists",
		)

		return fmt.Errorf("job %q already exists", job.ID)
	}

	job.Payload = append([]byte(nil), job.Payload...)
	s.jobs[job.ID] = job

	logger.Info(
		"job created",
		"operation", "create_job",
		"state", job.State,
	)

	return nil
}

// GetJob returns a defensive copy of a job.
func (s *Store) GetJob(id model.JobID) (model.Job, bool) {
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		logger.Debug(
			"job lookup missed",
			"operation", "get_job",
		)

		return model.Job{}, false
	}

	logger.Debug(
		"job lookup succeeded",
		"operation", "get_job",
		"state", job.State,
		"attempt", job.Attempt,
		"fencing_token", job.FencingToken,
	)

	return cloneJob(job), true
}

// UpdateJob replaces an existing job.
func (s *Store) UpdateJob(job model.Job) error {
	logger := s.getLogger()

	if job.ID == "" {
		logger.Warn(
			"job update rejected",
			"operation", "update_job",
			"reason", "missing_job_id",
		)

		return fmt.Errorf("%w: missing job ID", ErrInvalidJob)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; !exists {
		logger.Warn(
			"job update rejected",
			"operation", "update_job",
			"reason", "job_not_found",
		)

		return ErrJobNotFound
	}

	job.Payload = append([]byte(nil), job.Payload...)
	s.jobs[job.ID] = job

	logger.Info(
		"job updated",
		"operation", "update_job",
		"state", job.State,
		"attempt", job.Attempt,
		"fencing_token", job.FencingToken,
	)

	return nil
}

// DeleteJob removes a job.
func (s *Store) DeleteJob(id model.JobID) bool {
	logger := s.getLogger()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[id]; !exists {
		logger.Debug(
			"job deletion skipped because job was not found",
			"operation", "delete_job",
		)

		return false
	}

	delete(s.jobs, id)

	logger.Info(
		"job deleted",
		"operation", "delete_job",
	)

	return true
}
