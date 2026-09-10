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
	ErrInvalidJobState   = errors.New("invalid job state transition")
	ErrJobOwnershipLost  = errors.New("job ownership lost")
)

// CreateJob inserts a new job into the state machine.
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

// GetJob returns a defensive copy of a job.
func (s *Store) GetJob(id model.JobID) (model.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return model.Job{}, false
	}

	return cloneJob(job), true
}

// ListPendingJobs returns a deterministic snapshot of all pending jobs.
//
// Jobs are ordered by ScheduledAt and then JobID.
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

// ListExpiredJobs returns claimed jobs whose replicated lock has expired.
//
// Both SCHEDULED and RUNNING jobs are returned. This is important for worker
// crash recovery: a worker may crash either before JOB_START is committed or
// after the job has entered RUNNING.
func (s *Store) ListExpiredJobs(now int64) []model.Job {
	if now <= 0 {
		return nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]model.Job, 0)

	for _, job := range s.jobs {
		if job.State != model.JobScheduled &&
			job.State != model.JobRunning {
			continue
		}

		currentLock, ok := s.locks.Get(string(job.ID))
		if !ok {
			continue
		}

		if currentLock.FencingToken != job.FencingToken {
			continue
		}

		if currentLock.ExpiresAt > now {
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

// UpdateJob replaces an existing job.
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

// DeleteJob removes a job.
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

	if expiresAt <= 0 {
		return model.Job{}, fmt.Errorf(
			"%w: invalid expiration time",
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
		// Valid transition.

	case model.JobScheduled:
		if job.AssignedWorkerID == workerID {
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
	job.ExecutionID = executionID(job.ID, job.Attempt)

	s.jobs[id] = job

	return cloneJob(job), nil
}

// ReclaimExpiredJob releases an expired job lease and returns the job to
// PENDING so another worker can claim it.
//
// The fencing token is mandatory. This prevents an old scheduler from
// reclaiming a newer ownership generation.
//
// The operation is intended to be invoked through the Raft state machine.
func (s *Store) ReclaimExpiredJob(
	id model.JobID,
	expectedToken uint64,
	at int64,
) (model.Job, error) {
	if id == "" || expectedToken == 0 || at <= 0 {
		return model.Job{}, ErrInvalidJob
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return model.Job{}, ErrJobNotFound
	}

	if job.State != model.JobScheduled &&
		job.State != model.JobRunning {
		return model.Job{}, ErrInvalidJobState
	}

	if job.FencingToken != expectedToken {
		return model.Job{}, ErrJobOwnershipLost
	}

	currentLock, ok := s.locks.Get(string(id))
	if !ok {
		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.FencingToken != expectedToken {
		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.ExpiresAt > at {
		return model.Job{}, ErrJobOwnershipLost
	}

	delete(s.locks.Locks, string(id))

	job.State = model.JobPending
	job.AssignedWorkerID = ""
	job.FencingToken = 0

	job.Payload = append([]byte(nil), job.Payload...)
	s.jobs[id] = job

	return cloneJob(job), nil
}

// ListAssignedJobs returns jobs currently assigned to the specified worker.
//
// Only SCHEDULED jobs are returned because the worker must explicitly
// transition SCHEDULED -> RUNNING before executing application work.
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

// ValidateJobOwnership verifies that the supplied worker still owns the job
// under the supplied fencing token and that the corresponding lock has not
// expired.
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

// TransitionJobState changes a claimed job's lifecycle state.
//
// The transition is validated atomically against the current job ownership,
// fencing token, lock owner, lock fencing token, and lock expiry.
func (s *Store) TransitionJobState(
	jobID model.JobID,
	workerID string,
	executionID string,
	fencingToken uint64,
	expectedState model.JobState,
	nextState model.JobState,
	at int64,
) (model.Job, error) {
	if jobID == "" || workerID == "" || executionID == "" || fencingToken == 0 {
		return model.Job{}, ErrInvalidJob
	}

	if at <= 0 {
		return model.Job{}, ErrInvalidJob
	}

	if expectedState != model.JobScheduled &&
		expectedState != model.JobRunning {
		return model.Job{}, ErrInvalidJobState
	}

	if nextState != model.JobRunning &&
		nextState != model.JobSucceeded &&
		nextState != model.JobFailed {
		return model.Job{}, ErrInvalidJobState
	}

	if expectedState == model.JobScheduled &&
		nextState != model.JobRunning {
		return model.Job{}, ErrInvalidJobState
	}

	if expectedState == model.JobRunning &&
		nextState != model.JobSucceeded &&
		nextState != model.JobFailed {
		return model.Job{}, ErrInvalidJobState
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return model.Job{}, ErrJobNotFound
	}

	if job.State != expectedState {
		return model.Job{}, ErrInvalidJobState
	}

	if job.AssignedWorkerID != workerID {
		return model.Job{}, ErrJobOwnershipLost
	}

	if job.ExecutionID != executionID {
		return model.Job{}, ErrJobOwnershipLost
	}

	if job.FencingToken != fencingToken {
		return model.Job{}, ErrJobOwnershipLost
	}

	currentLock, ok := s.locks.Get(string(jobID))
	if !ok {
		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.OwnerID != workerID {
		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.FencingToken != fencingToken {
		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.ExpiresAt <= at {
		return model.Job{}, ErrJobOwnershipLost
	}

	job.State = nextState

	copied := cloneJob(job)
	s.jobs[jobID] = copied

	return copied, nil
}
func executionID(jobID model.JobID, attempt uint32) string {
	return fmt.Sprintf("%s/%d", jobID, attempt)
}
