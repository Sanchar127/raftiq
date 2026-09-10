package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

var (
	ErrInvalidWorker = errors.New("invalid worker")
	ErrInvalidJob    = errors.New("invalid job")
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

// Worker represents a worker capable of executing claimed jobs.
//
// Worker currently contains only execution concerns. Ownership validation,
// fencing validation, retries, and durable state transitions are handled by
// the surrounding distributed-scheduling layer.
type Worker struct {
	id      string
	handler JobHandler
}

// New creates a worker with a stable worker ID and execution handler.
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
//
// At this stage, Execute only performs local execution. Distributed ownership
// and fencing checks will be added before execution is exposed through the
// scheduling loop.
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
