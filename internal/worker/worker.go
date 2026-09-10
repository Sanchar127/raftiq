package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
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
}

// Config controls worker execution-loop behavior.
type Config struct {
	Interval time.Duration
}

// New creates a worker with a stable worker ID and execution handler.
//
// The worker source and execution interval are configured separately so the
// basic Execute method remains useful without a scheduler/store dependency.
func New(id string, handler JobHandler) (*Worker, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: missing worker ID", ErrInvalidWorker)
	}

	if handler == nil {
		return nil, fmt.Errorf("%w: missing job handler", ErrInvalidWorker)
	}

	return &Worker{
		id:      id,
		handler: handler,
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
		return fmt.Errorf("%w: missing job ID", ErrInvalidJob)
	}

	if ctx == nil {
		return fmt.Errorf("%w: nil execution context", ErrInvalidJob)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := w.handler.Execute(ctx, job); err != nil {
		return fmt.Errorf("execute job %q: %w", job.ID, err)
	}

	return nil
}

// ConfigureLoop attaches the job source and execution-loop configuration.
//
// This keeps construction backward-compatible with the simple Worker API
// while allowing the execution loop to be introduced independently.
func (w *Worker) ConfigureLoop(
	source JobSource,
	config Config,
) error {
	if source == nil {
		return fmt.Errorf("%w: missing job source", ErrInvalidWorker)
	}

	if config.Interval <= 0 {
		return fmt.Errorf(
			"%w: worker interval must be positive",
			ErrInvalidWorker,
		)
	}

	w.source = source
	w.interval = config.Interval

	return nil
}

// Run starts the worker execution loop.
//
// The loop only executes jobs that are already assigned to this worker.
// Durable state transitions, fencing validation, retries, and reclaim
// semantics are intentionally handled by later scheduling layers.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil execution context", ErrInvalidWorker)
	}

	if w.source == nil {
		return fmt.Errorf("%w: job source is not configured", ErrInvalidWorker)
	}

	if w.interval <= 0 {
		return fmt.Errorf("%w: invalid worker interval", ErrInvalidWorker)
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

func (w *Worker) executeAssignedJobs(ctx context.Context) error {
	jobs := w.source.ListAssignedJobs(w.id)
	now := time.Now().UnixNano()

	for _, job := range jobs {
		if !w.source.ValidateJobOwnership(
			job.ID,
			w.id,
			0,
			now,
		) {
			continue
		}

		if err := w.Execute(ctx, job); err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}
	}

	return nil
}
