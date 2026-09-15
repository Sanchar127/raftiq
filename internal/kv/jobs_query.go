package kv

import (
	"sort"

	"github.com/sanchar127/raftiq/internal/model"
)

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