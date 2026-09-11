package scheduler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
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

func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := s.scheduleDueJobs(ctx); err != nil {
				if errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					return err
				}
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
	if job.FencingToken == 0 {
		return nil
	}

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:         kv.CommandJobReclaim,
		JobID:        string(job.ID),
		FencingToken: job.FencingToken,
		At:           now,
	})
	if err != nil {
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
			return nil
		}

		return result.Err
	}

	s.getMetrics().IncLeaseLosses()

	return nil
}

func (s *Scheduler) scheduleJob(
	ctx context.Context,
	job model.Job,
) error {
	workerID, err := s.selector.SelectWorker(job)
	if err != nil {
		return err
	}

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:      kv.CommandClaimJob,
		JobID:     string(job.ID),
		OwnerID:   workerID,
		ExpiresAt: time.Now().Add(s.lease).UnixNano(),
	})
	if err != nil {
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
			return nil
		}

		return result.Err
	}

	metrics := s.getMetrics()
	metrics.IncScheduledJobs()
	metrics.IncLeaseAcquisitions()

	return nil
}

func isExpectedReclaimError(err error) bool {
	return errors.Is(err, kv.ErrJobNotFound) ||
		errors.Is(err, kv.ErrInvalidJobState) ||
		errors.Is(err, kv.ErrJobOwnershipLost)
}
