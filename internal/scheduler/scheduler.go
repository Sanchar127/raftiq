package scheduler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

const (
	DefaultInterval = 100 * time.Millisecond
	DefaultLease    = 30 * time.Second
)

var (
	ErrNoWorkers       = errors.New("no workers configured")
	ErrInvalidInterval = errors.New("scheduler interval must be positive")
	ErrInvalidLease    = errors.New("scheduler lease must be positive")
)

type WorkerSelector interface {
	SelectWorker(job model.Job) (string, error)
}

// HashWorkerSelector deterministically maps a job to a worker.
//
// The mapping depends only on the job ID and the configured worker set.
// It does not depend on scheduler-local state, so a different Raft leader
// produces the same assignment for the same job.
type HashWorkerSelector struct {
	workers []string
}

func NewHashWorkerSelector(workers []string) (*HashWorkerSelector, error) {
	if len(workers) == 0 {
		return nil, ErrNoWorkers
	}

	copied := append([]string(nil), workers...)

	for _, worker := range copied {
		if worker == "" {
			return nil, fmt.Errorf("worker ID must not be empty")
		}
	}

	sort.Strings(copied)

	for i := 1; i < len(copied); i++ {
		if copied[i] == copied[i-1] {
			return nil, fmt.Errorf("duplicate worker ID %q", copied[i])
		}
	}

	return &HashWorkerSelector{
		workers: copied,
	}, nil
}

func (s *HashWorkerSelector) SelectWorker(job model.Job) (string, error) {
	if len(s.workers) == 0 {
		return "", ErrNoWorkers
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(job.ID))

	index := uint64(hash.Sum32()) % uint64(len(s.workers))

	return s.workers[index], nil
}

type Scheduler struct {
	raft     *raft.RaftNode
	store    *kv.Store
	applier  *kv.Applier
	selector WorkerSelector
	interval time.Duration
	lease    time.Duration

	metricsMu sync.RWMutex
	metrics   SchedulerMetrics

	loggerMu sync.RWMutex
	logger   *slog.Logger
}

type Config struct {
	Interval time.Duration
	Lease    time.Duration
}

func New(
	raftNode *raft.RaftNode,
	store *kv.Store,
	applier *kv.Applier,
	selector WorkerSelector,
	config Config,
) (*Scheduler, error) {
	if raftNode == nil {
		return nil, errors.New("raft node is required")
	}

	if store == nil {
		return nil, errors.New("store is required")
	}

	if applier == nil {
		return nil, errors.New("applier is required")
	}

	if selector == nil {
		return nil, errors.New("worker selector is required")
	}

	if config.Interval <= 0 {
		return nil, ErrInvalidInterval
	}

	if config.Lease <= 0 {
		return nil, ErrInvalidLease
	}

	return &Scheduler{
		raft:     raftNode,
		store:    store,
		applier:  applier,
		selector: selector,
		interval: config.Interval,
		lease:    config.Lease,
		metrics:  NoopSchedulerMetrics{},
		logger:   discardSchedulerLogger(),
	}, nil
}

func (s *Scheduler) SetMetrics(metrics SchedulerMetrics) {
	s.metricsMu.Lock()
	defer s.metricsMu.Unlock()

	if metrics == nil {
		s.metrics = NoopSchedulerMetrics{}
		return
	}

	s.metrics = metrics
}

func (s *Scheduler) getMetrics() SchedulerMetrics {
	s.metricsMu.RLock()
	defer s.metricsMu.RUnlock()

	return s.metrics
}

func (s *Scheduler) SetLogger(logger *slog.Logger) {
	s.loggerMu.Lock()
	defer s.loggerMu.Unlock()

	if logger == nil {
		s.logger = discardSchedulerLogger()
		return
	}

	s.logger = logger
}

func (s *Scheduler) getLogger() *slog.Logger {
	s.loggerMu.RLock()
	defer s.loggerMu.RUnlock()

	if s.logger == nil {
		return discardSchedulerLogger()
	}

	return s.logger
}

func (s *Scheduler) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("scheduler context is required")
	}

	logger := s.getLogger()

	logger.Info(
		"scheduler started",
		slog.String("component", "scheduler"),
		slog.Duration("interval", s.interval),
		slog.Duration("lease", s.lease),
	)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug(
				"scheduler stopped",
				slog.String("component", "scheduler"),
				slog.String("reason", "context canceled"),
			)
			return ctx.Err()

		case <-ticker.C:
			if err := s.scheduleDueJobs(ctx); err != nil {
				if errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					return err
				}

				logger.Error(
					"scheduler iteration failed",
					slog.String("component", "scheduler"),
					slog.Any("error", err),
				)
			}
		}
	}
}

func (s *Scheduler) scheduleDueJobs(ctx context.Context) error {
	if s.raft.State().Role != raft.Leader {
		return nil
	}

	now := time.Now().UnixNano()

	if err := s.reclaimExpiredJobs(ctx, now); err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		s.getLogger().Error(
			"failed to reclaim expired jobs",
			slog.String("component", "scheduler"),
			slog.Any("error", err),
		)
	}

	for _, job := range s.store.ListPendingJobs() {
		if job.ScheduledAt > now {
			break
		}

		if err := s.scheduleJob(ctx, job); err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			s.getLogger().Error(
				"failed to schedule job",
				slog.String("component", "scheduler"),
				slog.String("job_id", string(job.ID)),
				slog.Any("error", err),
			)
		}
	}

	return nil
}

func (s *Scheduler) reclaimExpiredJobs(
	ctx context.Context,
	now int64,
) error {
	for _, job := range s.store.ListExpiredJobs(now) {
		if err := s.reclaimExpiredJob(ctx, job, now); err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}

			if isExpectedReclaimError(err) {
				s.getLogger().Debug(
					"expired job was not reclaimed due to expected state",
					slog.String("component", "scheduler"),
					slog.String("job_id", string(job.ID)),
					slog.Any("error", err),
				)
				continue
			}

			return err
		}
	}

	return nil
}

func (s *Scheduler) reclaimExpiredJob(
	ctx context.Context,
	job model.Job,
	now int64,
) error {
	logger := s.getLogger()

	if job.FencingToken == 0 {
		logger.Debug(
			"skipping expired job without fencing token",
			slog.String("component", "scheduler"),
			slog.String("job_id", string(job.ID)),
		)
		return nil
	}

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:         kv.CommandJobReclaim,
		JobID:        string(job.ID),
		FencingToken: job.FencingToken,
		At:           now,
	})
	if err != nil {
		logger.Error(
			"failed to encode job reclaim command",
			slog.String("component", "scheduler"),
			slog.String("job_id", string(job.ID)),
			slog.Any("error", err),
		)

		return fmt.Errorf("encode job reclaim command: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		return err
	}

	result, err := s.applier.WaitResult(ctx, index)
	if err != nil {
		return fmt.Errorf("wait for job reclaim result: %w", err)
	}

	if result.Err != nil {
		if isExpectedReclaimError(result.Err) {
			return result.Err
		}

		return result.Err
	}

	s.getMetrics().IncLeaseLosses()

	logger.Info(
		"expired job lease reclaimed",
		slog.String("component", "scheduler"),
		slog.String("job_id", string(job.ID)),
		slog.Uint64("fencing_token", job.FencingToken),
		slog.Uint64("raft_index", uint64(index)),
	)

	return nil
}

func (s *Scheduler) scheduleJob(
	ctx context.Context,
	job model.Job,
) error {
	logger := s.getLogger()

	workerID, err := s.selector.SelectWorker(job)
	if err != nil {
		logger.Error(
			"failed to select worker for job",
			slog.String("component", "scheduler"),
			slog.String("job_id", string(job.ID)),
			slog.Any("error", err),
		)

		return err
	}

	logger.Debug(
		"selected worker for job",
		slog.String("component", "scheduler"),
		slog.String("job_id", string(job.ID)),
		slog.String("worker_id", workerID),
	)

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:      kv.CommandClaimJob,
		JobID:     string(job.ID),
		OwnerID:   workerID,
		ExpiresAt: time.Now().Add(s.lease).UnixNano(),
	})
	if err != nil {
		logger.Error(
			"failed to encode job claim command",
			slog.String("component", "scheduler"),
			slog.String("job_id", string(job.ID)),
			slog.String("worker_id", workerID),
			slog.Any("error", err),
		)

		return fmt.Errorf("encode claim job command: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		return err
	}

	result, err := s.applier.WaitResult(ctx, index)
	if err != nil {
		return fmt.Errorf("wait for job claim result: %w", err)
	}

	if result.Err != nil {
		if errors.Is(result.Err, kv.ErrJobAlreadyClaimed) ||
			errors.Is(result.Err, kv.ErrJobNotClaimable) ||
			errors.Is(result.Err, kv.ErrJobNotFound) {
			logger.Debug(
				"job claim rejected due to expected state",
				slog.String("component", "scheduler"),
				slog.String("job_id", string(job.ID)),
				slog.String("worker_id", workerID),
				slog.Any("error", result.Err),
			)
			return nil
		}

		logger.Error(
			"job claim failed",
			slog.String("component", "scheduler"),
			slog.String("job_id", string(job.ID)),
			slog.String("worker_id", workerID),
			slog.Any("error", result.Err),
		)

		return result.Err
	}

	metrics := s.getMetrics()
	metrics.IncScheduledJobs()
	metrics.IncLeaseAcquisitions()

	logger.Info(
		"job scheduled",
		slog.String("component", "scheduler"),
		slog.String("job_id", string(job.ID)),
		slog.String("worker_id", workerID),
		slog.Uint64("raft_index", uint64(index)),
		slog.Duration("lease", s.lease),
	)

	return nil
}

func isExpectedReclaimError(err error) bool {
	return errors.Is(err, kv.ErrJobNotFound) ||
		errors.Is(err, kv.ErrInvalidJobState) ||
		errors.Is(err, kv.ErrJobOwnershipLost)
}

func discardSchedulerLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}
