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

// ListPendingJobs returns a deterministic snapshot of all pending jobs.
//
// Jobs are ordered by ScheduledAt and then JobID.
func (s *Store) ListPendingJobs() []model.Job {
	logger := s.getLogger()

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

	logger.Debug(
		"pending jobs listed",
		"operation", "list_pending_jobs",
		"count", len(jobs),
	)

	return jobs
}

// ListExpiredJobs returns claimed jobs whose replicated lock has expired.
//
// Both SCHEDULED and RUNNING jobs are returned. This is important for worker
// crash recovery: a worker may crash either before JOB_START is committed or
// after the job has entered RUNNING.
func (s *Store) ListExpiredJobs(now int64) []model.Job {
	logger := s.getLogger()

	if now <= 0 {
		logger.Warn(
			"expired job listing rejected",
			"operation", "list_expired_jobs",
			"reason", "invalid_timestamp",
		)

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

	logger.Debug(
		"expired jobs listed",
		"operation", "list_expired_jobs",
		"count", len(jobs),
		"timestamp", now,
	)

	return jobs
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

// ClaimJob atomically assigns a pending job to a worker and allocates
// a new fencing token through the replicated lock state.
func (s *Store) ClaimJob(
	id model.JobID,
	workerID string,
	expiresAt int64,
	grantIndex model.LogIndex,
) (model.Job, error) {
	logger := s.getLogger()

	if id == "" {
		logger.Warn(
			"job claim rejected",
			"operation", "claim_job",
			"reason", "missing_job_id",
		)

		return model.Job{}, fmt.Errorf(
			"%w: missing job ID",
			ErrInvalidJob,
		)
	}

	if workerID == "" {
		logger.Warn(
			"job claim rejected",
			"operation", "claim_job",
			"reason", "missing_worker_id",
		)

		return model.Job{}, fmt.Errorf(
			"%w: missing worker ID",
			ErrInvalidJob,
		)
	}

	if expiresAt <= 0 {
		logger.Warn(
			"job claim rejected",
			"operation", "claim_job",
			"reason", "invalid_expiration_time",
		)

		return model.Job{}, fmt.Errorf(
			"%w: invalid expiration time",
			ErrInvalidJob,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[id]
	if !exists {
		logger.Debug(
			"job claim failed",
			"operation", "claim_job",
			"reason", "job_not_found",
		)

		return model.Job{}, ErrJobNotFound
	}

	switch job.State {
	case model.JobPending:
		// Valid transition.

	case model.JobScheduled:
		if job.AssignedWorkerID == workerID {
			logger.Debug(
				"job claim was idempotent",
				"operation", "claim_job",
				"state", job.State,
				"fencing_token", job.FencingToken,
			)

			return cloneJob(job), nil
		}

		logger.Debug(
			"job claim rejected",
			"operation", "claim_job",
			"reason", "job_already_claimed",
			"state", job.State,
		)

		return model.Job{}, fmt.Errorf(
			"%w: job %q is assigned to worker %q",
			ErrJobAlreadyClaimed,
			id,
			job.AssignedWorkerID,
		)

	default:
		logger.Debug(
			"job claim rejected",
			"operation", "claim_job",
			"reason", "job_not_claimable",
			"state", job.State,
		)

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

			logger.Debug(
				"job claim reused existing ownership",
				"operation", "claim_job",
				"state", job.State,
				"fencing_token", job.FencingToken,
			)

			return cloneJob(job), nil
		}

		logger.Debug(
			"job claim rejected because lock is owned by another worker",
			"operation", "claim_job",
			"reason", "lock_owned",
		)

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

	logger.Info(
		"job claimed",
		"operation", "claim_job",
		"state", job.State,
		"attempt", job.Attempt,
		"fencing_token", job.FencingToken,
		"grant_index", grantIndex,
	)

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
	logger := s.getLogger()

	if id == "" || expectedToken == 0 || at <= 0 {
		logger.Warn(
			"expired job reclaim rejected",
			"operation", "reclaim_expired_job",
			"reason", "invalid_arguments",
		)

		return model.Job{}, ErrInvalidJob
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		logger.Debug(
			"expired job reclaim failed",
			"operation", "reclaim_expired_job",
			"reason", "job_not_found",
		)

		return model.Job{}, ErrJobNotFound
	}

	if job.State != model.JobScheduled &&
		job.State != model.JobRunning {
		logger.Debug(
			"expired job reclaim rejected",
			"operation", "reclaim_expired_job",
			"reason", "invalid_job_state",
			"state", job.State,
		)

		return model.Job{}, ErrInvalidJobState
	}

	if job.FencingToken != expectedToken {
		logger.Warn(
			"expired job reclaim rejected",
			"operation", "reclaim_expired_job",
			"reason", "ownership_lost",
			"expected_token", expectedToken,
			"job_token", job.FencingToken,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	currentLock, ok := s.locks.Get(string(id))
	if !ok {
		logger.Warn(
			"expired job reclaim rejected",
			"operation", "reclaim_expired_job",
			"reason", "lock_not_found",
			"expected_token", expectedToken,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.FencingToken != expectedToken {
		logger.Warn(
			"expired job reclaim rejected",
			"operation", "reclaim_expired_job",
			"reason", "lock_fencing_token_mismatch",
			"expected_token", expectedToken,
			"lock_token", currentLock.FencingToken,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.ExpiresAt > at {
		logger.Debug(
			"expired job reclaim rejected",
			"operation", "reclaim_expired_job",
			"reason", "lease_not_expired",
			"expires_at", currentLock.ExpiresAt,
			"timestamp", at,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	delete(s.locks.Locks, string(id))

	job.State = model.JobPending
	job.AssignedWorkerID = ""
	job.FencingToken = 0

	job.Payload = append([]byte(nil), job.Payload...)
	s.jobs[id] = job

	logger.Info(
		"expired job reclaimed",
		"operation", "reclaim_expired_job",
		"previous_state", job.State,
		"new_state", model.JobPending,
		"attempt", job.Attempt,
		"fencing_token", expectedToken,
	)

	return cloneJob(job), nil
}

// ListAssignedJobs returns jobs currently assigned to the specified worker.
//
// Only SCHEDULED jobs are returned because the worker must explicitly
// transition SCHEDULED -> RUNNING before executing application work.
func (s *Store) ListAssignedJobs(workerID string) []model.Job {
	logger := s.getLogger()

	if workerID == "" {
		logger.Warn(
			"assigned job listing rejected",
			"operation", "list_assigned_jobs",
			"reason", "missing_worker_id",
		)

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

	logger.Debug(
		"assigned jobs listed",
		"operation", "list_assigned_jobs",
		"count", len(jobs),
	)

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
	logger := s.getLogger()

	if jobID == "" || workerID == "" || fencingToken == 0 {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "invalid_arguments",
		)

		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[jobID]
	if !ok {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "job_not_found",
		)

		return false
	}

	if job.AssignedWorkerID != workerID {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "worker_mismatch",
		)

		return false
	}

	if job.FencingToken != fencingToken {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "fencing_token_mismatch",
		)

		return false
	}

	if job.State != model.JobScheduled &&
		job.State != model.JobRunning {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "invalid_job_state",
			"state", job.State,
		)

		return false
	}

	currentLock, ok := s.locks.Get(string(jobID))
	if !ok {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "lock_not_found",
		)

		return false
	}

	if currentLock.OwnerID != workerID {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "lock_owner_mismatch",
		)

		return false
	}

	if currentLock.FencingToken != fencingToken {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "lock_fencing_token_mismatch",
		)

		return false
	}

	if currentLock.ExpiresAt <= now {
		logger.Debug(
			"job ownership validation failed",
			"operation", "validate_job_ownership",
			"reason", "lock_expired",
			"expires_at", currentLock.ExpiresAt,
			"timestamp", now,
		)

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
	logger := s.getLogger()

	if jobID == "" || workerID == "" || executionID == "" || fencingToken == 0 {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "invalid_arguments",
		)

		return model.Job{}, ErrInvalidJob
	}

	if at <= 0 {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "invalid_timestamp",
		)

		return model.Job{}, ErrInvalidJob
	}

	if expectedState != model.JobScheduled &&
		expectedState != model.JobRunning {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "invalid_expected_state",
			"expected_state", expectedState,
		)

		return model.Job{}, ErrInvalidJobState
	}

	if nextState != model.JobRunning &&
		nextState != model.JobSucceeded &&
		nextState != model.JobFailed {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "invalid_next_state",
			"next_state", nextState,
		)

		return model.Job{}, ErrInvalidJobState
	}

	if expectedState == model.JobScheduled &&
		nextState != model.JobRunning {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "invalid_transition",
			"expected_state", expectedState,
			"next_state", nextState,
		)

		return model.Job{}, ErrInvalidJobState
	}

	if expectedState == model.JobRunning &&
		nextState != model.JobSucceeded &&
		nextState != model.JobFailed {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "invalid_transition",
			"expected_state", expectedState,
			"next_state", nextState,
		)

		return model.Job{}, ErrInvalidJobState
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		logger.Debug(
			"job state transition failed",
			"operation", "transition_job_state",
			"reason", "job_not_found",
		)

		return model.Job{}, ErrJobNotFound
	}

	if job.State != expectedState {
		logger.Debug(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "state_mismatch",
			"actual_state", job.State,
			"expected_state", expectedState,
		)

		return model.Job{}, ErrInvalidJobState
	}

	if job.AssignedWorkerID != workerID {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "worker_ownership_lost",
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if job.ExecutionID != executionID {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "execution_ownership_lost",
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if job.FencingToken != fencingToken {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "job_fencing_token_mismatch",
			"expected_token", fencingToken,
			"job_token", job.FencingToken,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	currentLock, ok := s.locks.Get(string(jobID))
	if !ok {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "lock_not_found",
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.OwnerID != workerID {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "lock_owner_mismatch",
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.FencingToken != fencingToken {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "lock_fencing_token_mismatch",
			"expected_token", fencingToken,
			"lock_token", currentLock.FencingToken,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	if currentLock.ExpiresAt <= at {
		logger.Warn(
			"job state transition rejected",
			"operation", "transition_job_state",
			"reason", "lock_expired",
			"expires_at", currentLock.ExpiresAt,
			"timestamp", at,
		)

		return model.Job{}, ErrJobOwnershipLost
	}

	job.State = nextState

	copied := cloneJob(job)
	s.jobs[jobID] = copied

	logger.Info(
		"job state transitioned",
		"operation", "transition_job_state",
		"previous_state", expectedState,
		"next_state", nextState,
		"attempt", job.Attempt,
		"fencing_token", fencingToken,
	)

	return copied, nil
}

func executionID(jobID model.JobID, attempt uint32) string {
	return fmt.Sprintf("%s/%d", jobID, attempt)
}
