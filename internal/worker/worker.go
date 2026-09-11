package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

var (
	ErrInvalidWorker  = errors.New("invalid worker")
	ErrInvalidJob     = errors.New("invalid job")
	ErrOwnershipLost  = errors.New("job ownership lost")
	ErrInvalidFencing = errors.New("invalid fencing token")
)

// JobHandler executes the application-specific work represented by a job.
//
// Handlers should honor context cancellation and return an error when the
// execution fails. Durable job-state transitions are intentionally outside
// the handler; those transitions must go through the Raft state machine.
type JobHandler interface {
	Execute(ctx context.Context, job model.Job) error
}

// HandlerFunc adapts a function into a JobHandler.
type HandlerFunc func(context.Context, model.Job) error

// Execute implements JobHandler.
func (f HandlerFunc) Execute(
	ctx context.Context,
	job model.Job,
) error {
	return f(ctx, job)
}

// JobSource provides jobs available for local worker execution.
//
// The source is intentionally read-only from the worker's perspective.
// Durable job-state mutations belong to the Raft state machine.
type JobSource interface {
	ListAssignedJobs(workerID string) []model.Job
	ValidateJobOwnership(
		jobID model.JobID,
		workerID string,
		fencingToken uint64,
		now int64,
	) bool
}

// Worker represents a worker capable of executing claimed jobs.
type Worker struct {
	id       string
	handler  JobHandler
	source   JobSource
	interval time.Duration
	raft     *raft.RaftNode
	applier  *kv.Applier
	metrics  WorkerMetrics

	loggerMu sync.RWMutex
	logger   *slog.Logger
}

// Config controls worker execution-loop behavior.
type Config struct {
	Interval time.Duration
	Raft     *raft.RaftNode
	Applier  *kv.Applier
	Metrics  WorkerMetrics
}

// New creates a worker with a stable worker ID and execution handler.
func New(id string, handler JobHandler) (*Worker, error) {
	if id == "" {
		return nil, fmt.Errorf(
			"%w: missing worker ID",
			ErrInvalidWorker,
		)
	}

	if handler == nil {
		return nil, fmt.Errorf(
			"%w: missing job handler",
			ErrInvalidWorker,
		)
	}

	return &Worker{
		id:      id,
		handler: handler,
		metrics: NoopWorkerMetrics{},
		logger:  discardWorkerLogger(),
	}, nil
}

// ID returns the worker's stable identity.
func (w *Worker) ID() string {
	return w.id
}

// SetLogger configures the worker's structured logger.
//
// A nil logger disables worker logging safely.
func (w *Worker) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardWorkerLogger()
	}

	w.loggerMu.Lock()
	w.logger = logger
	w.loggerMu.Unlock()
}

func (w *Worker) getLogger() *slog.Logger {
	w.loggerMu.RLock()
	logger := w.logger
	w.loggerMu.RUnlock()

	if logger == nil {
		return discardWorkerLogger()
	}

	return logger
}

func discardWorkerLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Execute runs the configured handler for a job.
func (w *Worker) Execute(
	ctx context.Context,
	job model.Job,
) error {
	if job.ID == "" {
		return fmt.Errorf(
			"%w: missing job ID",
			ErrInvalidJob,
		)
	}

	if ctx == nil {
		return fmt.Errorf(
			"%w: nil execution context",
			ErrInvalidJob,
		)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := w.handler.Execute(ctx, job); err != nil {
		return fmt.Errorf(
			"execute job %q: %w",
			job.ID,
			err,
		)
	}

	return nil
}

// ConfigureLoop attaches the job source and execution-loop configuration.
func (w *Worker) ConfigureLoop(
	source JobSource,
	config Config,
) error {
	if source == nil {
		return fmt.Errorf(
			"%w: missing job source",
			ErrInvalidWorker,
		)
	}

	if config.Interval <= 0 {
		return fmt.Errorf(
			"%w: worker interval must be positive",
			ErrInvalidWorker,
		)
	}

	if config.Raft == nil {
		return fmt.Errorf(
			"%w: missing Raft node",
			ErrInvalidWorker,
		)
	}

	if config.Applier == nil {
		return fmt.Errorf(
			"%w: missing applier",
			ErrInvalidWorker,
		)
	}

	metrics := config.Metrics
	if metrics == nil {
		metrics = NoopWorkerMetrics{}
	}

	w.source = source
	w.interval = config.Interval
	w.raft = config.Raft
	w.applier = config.Applier
	w.metrics = metrics

	return nil
}

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

// transitionJob commits a durable job-state transition through Raft and waits
// until the corresponding state-machine result is available.
func (w *Worker) transitionJob(
	ctx context.Context,
	job model.Job,
	commandType kv.CommandType,
	expectedState model.JobState,
	nextState model.JobState,
) (model.Job, error) {
	logger := w.getLogger().With(
		"component", "worker",
		"worker_id", w.id,
		"job_id", job.ID,
		"execution_id", job.ExecutionID,
		"fencing_token", job.FencingToken,
		"command_type", commandType,
	)

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:          commandType,
		JobID:         string(job.ID),
		OwnerID:       w.id,
		ExecutionID:   job.ExecutionID,
		FencingToken:  job.FencingToken,
		ExpectedState: expectedState,
		At:            time.Now().UnixNano(),
	})
	if err != nil {
		logger.Error(
			"failed to encode job transition",
			"error", err,
		)

		return model.Job{}, fmt.Errorf(
			"encode job transition: %w",
			err,
		)
	}

	index, err := w.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose job transition",
			"error", err,
		)

		return model.Job{}, fmt.Errorf(
			"propose job transition: %w",
			err,
		)
	}

	result, err := w.applier.WaitResult(ctx, index)
	if err != nil {
		logger.Error(
			"failed waiting for job transition result",
			"raft_index", index,
			"error", err,
		)

		return model.Job{}, fmt.Errorf(
			"wait for job transition at index %d: %w",
			index,
			err,
		)
	}

	if result.Err != nil {
		return model.Job{}, result.Err
	}

	if result.Job == nil {
		err := errors.New("job transition returned no job")

		logger.Error(
			"job transition returned no job",
			"raft_index", index,
			"error", err,
		)

		return model.Job{}, err
	}

	if result.Job.State != nextState {
		err := fmt.Errorf(
			"job transition produced state %s, want %s",
			result.Job.State,
			nextState,
		)

		logger.Error(
			"job transition produced unexpected state",
			"raft_index", index,
			"actual_state", result.Job.State,
			"expected_state", nextState,
			"error", err,
		)

		return model.Job{}, err
	}

	logger.Debug(
		"job state transition committed",
		"raft_index", index,
		"from_state", expectedState,
		"to_state", nextState,
	)

	return *result.Job, nil
}
