package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
)

// Run starts the worker execution loop.
//
// The loop only executes jobs that are already assigned to this worker.
// Durable state transitions, fencing validation, retries, and reclaim
// semantics are handled by the scheduling/state-machine layers.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf(
			"%w: nil execution context",
			ErrInvalidWorker,
		)
	}

	if w.source == nil {
		return fmt.Errorf(
			"%w: job source is not configured",
			ErrInvalidWorker,
		)
	}

	if w.interval <= 0 {
		return fmt.Errorf(
			"%w: invalid worker interval",
			ErrInvalidWorker,
		)
	}

	logger := w.getLogger().With(
		"component", "worker",
		"worker_id", w.id,
		"interval", w.interval.String(),
	)

	logger.Info("worker execution loop started")

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.executeAssignedJobs(ctx); err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				logger.Debug(
					"worker execution loop stopped",
					"error", err,
				)
				return err
			}

			logger.Error(
				"worker execution iteration failed",
				"error", err,
			)
		}

		select {
		case <-ctx.Done():
			logger.Debug(
				"worker execution loop stopped",
				"error", ctx.Err(),
			)
			return ctx.Err()

		case <-ticker.C:
		}
	}
}

// executeAssignedJobs executes all currently assigned jobs for this worker.
//
// Ownership is validated before starting each job. The JOB_START transition
// is committed through Raft before the handler executes. The committed
// RUNNING job returned by that transition is then passed to the handler and
// reused for subsequent terminal transitions.
func (w *Worker) executeAssignedJobs(
	ctx context.Context,
) error {
	jobs := w.source.ListAssignedJobs(w.id)

	logger := w.getLogger().With(
		"component", "worker",
		"worker_id", w.id,
	)

	for _, job := range jobs {
		if !w.source.ValidateJobOwnership(
			job.ID,
			w.id,
			job.FencingToken,
			time.Now().UnixNano(),
		) {
			w.metrics.IncLeaseLosses()

			logger.Debug(
				"job ownership lost before execution",
				"job_id", job.ID,
				"fencing_token", job.FencingToken,
			)

			continue
		}

		runningJob, err := w.transitionJob(
			ctx,
			job,
			kv.CommandJobStart,
			model.JobScheduled,
			model.JobRunning,
		)
		if err != nil {
			if errors.Is(err, kv.ErrJobOwnershipLost) ||
				errors.Is(err, ErrOwnershipLost) {
				w.metrics.IncLeaseLosses()

				logger.Debug(
					"job ownership lost during start transition",
					"job_id", job.ID,
					"fencing_token", job.FencingToken,
				)

				continue
			}

			if errors.Is(err, kv.ErrInvalidJobState) {
				logger.Debug(
					"job skipped because state changed before start",
					"job_id", job.ID,
				)

				continue
			}

			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			w.metrics.IncJobExecutionFailures()

			logger.Error(
				"job start transition failed",
				"job_id", job.ID,
				"fencing_token", job.FencingToken,
				"error", err,
			)

			continue
		}

		logger.Debug(
			"job execution started",
			"job_id", runningJob.ID,
			"execution_id", runningJob.ExecutionID,
			"fencing_token", runningJob.FencingToken,
		)

		executionStarted := time.Now()
		executionErr := w.Execute(ctx, runningJob)
		executionDuration := time.Since(executionStarted)

		if executionErr == nil {
			_, transitionErr := w.transitionJob(
				ctx,
				runningJob,
				kv.CommandJobSucceeded,
				model.JobRunning,
				model.JobSucceeded,
			)

			if errors.Is(
				transitionErr,
				kv.ErrJobOwnershipLost,
			) || errors.Is(
				transitionErr,
				ErrOwnershipLost,
			) {
				w.metrics.IncLeaseLosses()

				logger.Warn(
					"job ownership lost before success transition",
					"job_id", runningJob.ID,
					"execution_id", runningJob.ExecutionID,
					"fencing_token", runningJob.FencingToken,
					"execution_duration", executionDuration.String(),
				)

				continue
			}

			if transitionErr != nil {
				if errors.Is(
					transitionErr,
					context.Canceled,
				) || errors.Is(
					transitionErr,
					context.DeadlineExceeded,
				) {
					return transitionErr
				}

				w.metrics.IncJobExecutionFailures()

				logger.Error(
					"job success transition failed",
					"job_id", runningJob.ID,
					"execution_id", runningJob.ExecutionID,
					"fencing_token", runningJob.FencingToken,
					"execution_duration", executionDuration.String(),
					"error", transitionErr,
				)

				continue
			}

			w.metrics.IncExecutedJobs()

			logger.Info(
				"job execution completed",
				"job_id", runningJob.ID,
				"execution_id", runningJob.ExecutionID,
				"fencing_token", runningJob.FencingToken,
				"execution_duration", executionDuration.String(),
			)

			continue
		}

		logger.Error(
			"job execution failed",
			"job_id", runningJob.ID,
			"execution_id", runningJob.ExecutionID,
			"fencing_token", runningJob.FencingToken,
			"execution_duration", executionDuration.String(),
			"error", executionErr,
		)

		_, transitionErr := w.transitionJob(
			ctx,
			runningJob,
			kv.CommandJobFailed,
			model.JobRunning,
			model.JobFailed,
		)

		if errors.Is(
			transitionErr,
			kv.ErrJobOwnershipLost,
		) || errors.Is(
			transitionErr,
			ErrOwnershipLost,
		) {
			w.metrics.IncLeaseLosses()

			logger.Warn(
				"job ownership lost before failure transition",
				"job_id", runningJob.ID,
				"execution_id", runningJob.ExecutionID,
				"fencing_token", runningJob.FencingToken,
			)

			continue
		}

		if transitionErr != nil &&
			(errors.Is(
				transitionErr,
				context.Canceled,
			) ||
				errors.Is(
					transitionErr,
					context.DeadlineExceeded,
				)) {
			return transitionErr
		}

		if transitionErr != nil {
			logger.Error(
				"job failure transition failed",
				"job_id", runningJob.ID,
				"execution_id", runningJob.ExecutionID,
				"fencing_token", runningJob.FencingToken,
				"error", transitionErr,
			)
		}

		w.metrics.IncJobExecutionFailures()
	}

	return nil
}
