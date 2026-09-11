package worker

import (
	"context"
	"errors"
	"fmt"
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
	}, nil
}

// ID returns the worker's stable identity.
func (w *Worker) ID() string {
	return w.id
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

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		if err := w.executeAssignedJobs(ctx); err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}

		select {
		case <-ctx.Done():
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

	for _, job := range jobs {
		if !w.source.ValidateJobOwnership(
			job.ID,
			w.id,
			job.FencingToken,
			time.Now().UnixNano(),
		) {
			w.metrics.IncLeaseLosses()
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
				continue
			}

			if errors.Is(err, kv.ErrInvalidJobState) {
				continue
			}

			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			w.metrics.IncJobExecutionFailures()
			continue
		}

		executionErr := w.Execute(ctx, runningJob)

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
				continue
			}

			w.metrics.IncExecutedJobs()
			continue
		}

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
		return model.Job{}, fmt.Errorf(
			"encode job transition: %w",
			err,
		)
	}

	index, err := w.raft.Propose(commandData)
	if err != nil {
		return model.Job{}, fmt.Errorf(
			"propose job transition: %w",
			err,
		)
	}

	result, err := w.applier.WaitResult(ctx, index)
	if err != nil {
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
		return model.Job{}, errors.New(
			"job transition returned no job",
		)
	}

	if result.Job.State != nextState {
		return model.Job{}, fmt.Errorf(
			"job transition produced state %s, want %s",
			result.Job.State,
			nextState,
		)
	}

	return *result.Job, nil
}
