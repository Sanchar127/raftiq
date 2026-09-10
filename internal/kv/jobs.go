package kv

import (
	"errors"
	"fmt"
	"sort"

	"github.com/sanchar127/raftiq/internal/model"
)

var (
	ErrJobNotFound       = errors.New("job not found")
	ErrInvalidJob        = errors.New("invalid job")
	ErrJobNotClaimable   = errors.New("job is not claimable")
	ErrJobAlreadyClaimed = errors.New("job is already claimed")
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

// ListPendingJobs returns a deterministic snapshot of all pending jobs.
//
// Jobs are ordered by ScheduledAt and then JobID. The returned jobs are
// independent copies and can therefore be safely inspected by callers
// without holding the store lock.
func (s *Store) ListPendingJobs() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]model.Job, 0, len(s.jobs))

	for _, job := range s.jobs {
		if job.State != model.JobPending {
			continue
		}

		jobs = append(jobs, cloneJob(job))
	}

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].ScheduledAt != jobs[j].ScheduledAt {
			return jobs[i].ScheduledAt < jobs[j].ScheduledAt
		}

		return jobs[i].ID < jobs[j].ID
	})

	return jobs
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

// ClaimJob atomically assigns a pending job to a worker and allocates
// a new fencing token through the replicated lock state.
func (s *Store) ClaimJob(
	id model.JobID,
	workerID string,
	expiresAt int64,
	grantIndex model.LogIndex,
) (model.Job, error) {
	if id == "" {
		return model.Job{}, fmt.Errorf(
			"%w: missing job ID",
			ErrInvalidJob,
		)
	}

	if workerID == "" {
		return model.Job{}, fmt.Errorf(
			"%w: missing worker ID",
			ErrInvalidJob,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[id]
	if !exists {
		return model.Job{}, ErrJobNotFound
	}

	switch job.State {
	case model.JobPending:
		// Valid transition. Continue below.

	case model.JobScheduled:
		if job.AssignedWorkerID == workerID {
			// Idempotent replay of the same claim.
			return cloneJob(job), nil
		}

		return model.Job{}, fmt.Errorf(
			"%w: job %q is assigned to worker %q",
			ErrJobAlreadyClaimed,
			id,
			job.AssignedWorkerID,
		)

	default:
		return model.Job{}, fmt.Errorf(
			"%w: job %q is in state %q",
			ErrJobNotClaimable,
			id,
			job.State,
		)
	}

	currentLock, acquired := s.locks.Acquire(
		string(id),
		workerID,
		expiresAt,
		grantIndex,
	)

	if !acquired {
		if currentLock.OwnerID == workerID {
			// The lock already belongs to this worker. Treat this as
			// an idempotent replay rather than allocating another token.
			job.State = model.JobScheduled
			job.AssignedWorkerID = workerID
			job.FencingToken = currentLock.FencingToken

			s.jobs[id] = job

			return cloneJob(job), nil
		}

		return model.Job{}, fmt.Errorf(
			"%w: job %q is owned by worker %q",
			ErrJobAlreadyClaimed,
			id,
			currentLock.OwnerID,
		)
	}

	job.State = model.JobScheduled
	job.AssignedWorkerID = workerID
	job.FencingToken = currentLock.FencingToken
	job.Attempt++

	s.jobs[id] = job

	return cloneJob(job), nil
}

// ListAssignedJobs returns jobs currently assigned to the specified worker.
//
// Only SCHEDULED jobs are returned because RUNNING/SUCCEEDED/FAILED state
// transitions will be introduced by the worker lifecycle implementation.
func (s *Store) ListAssignedJobs(workerID string) []model.Job {
	if workerID == "" {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]model.Job, 0)

	for _, job := range s.jobs {
		if job.State != model.JobScheduled {
			continue
		}

		if job.AssignedWorkerID != workerID {
			continue
		}

		copied := job
		copied.Payload = append([]byte(nil), job.Payload...)

		jobs = append(jobs, copied)
	}

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].ScheduledAt != jobs[j].ScheduledAt {
			return jobs[i].ScheduledAt < jobs[j].ScheduledAt
		}

		return jobs[i].ID < jobs[j].ID
	})

	return jobs
}

// ValidateJobOwnership verifies that the supplied worker still owns the job
// under the supplied fencing token and that the corresponding lock has not
// expired.
//
// This method is read-only. It does not renew, acquire, or mutate the lock.
func (s *Store) ValidateJobOwnership(
	jobID model.JobID,
	workerID string,
	fencingToken uint64,
	now int64,
) bool {
	if jobID == "" || workerID == "" || fencingToken == 0 {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return false
	}

	if job.AssignedWorkerID != workerID {
		return false
	}

	if job.FencingToken != fencingToken {
		return false
	}

	if job.State != model.JobScheduled &&
		job.State != model.JobRunning {
		return false
	}

	currentLock, ok := s.locks.Get(string(jobID))
	if !ok {
		return false
	}

	if currentLock.OwnerID != workerID {
		return false
	}

	if currentLock.FencingToken != fencingToken {
		return false
	}

	if currentLock.ExpiresAt <= now {
		return false
	}

	return true
}
