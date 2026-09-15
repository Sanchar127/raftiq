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
