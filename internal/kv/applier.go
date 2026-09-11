package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

type ApplyResult struct {
	Job *model.Job
	Err error
}

type Applier struct {
	store   *Store
	metrics KVMetrics
	logger  *slog.Logger

	mu          sync.Mutex
	lastApplied model.LogIndex
	applyErr    error

	results map[model.LogIndex]ApplyResult
	cond    *sync.Cond
}

func NewApplier(store *Store) *Applier {
	return &Applier{
		store:   store,
		metrics: NoopKVMetrics{},
		logger:  discardLogger(),
		results: make(map[model.LogIndex]ApplyResult),
	}
}

func (a *Applier) SetMetrics(metrics KVMetrics) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if metrics == nil {
		a.metrics = NoopKVMetrics{}
		return
	}

	a.metrics = metrics
}

func (a *Applier) SetLogger(logger *slog.Logger) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if logger == nil {
		a.logger = discardLogger()
		return
	}

	a.logger = logger
}

func (a *Applier) Run(
	ctx context.Context,
	applyCh <-chan raft.LogEntry,
) error {
	if ctx == nil {
		return errors.New("applier context is required")
	}

	if applyCh == nil {
		return errors.New("applier apply channel is required")
	}

	a.logger.Debug(
		"applier started",
		slog.String("component", "kv-applier"),
	)

	for {
		select {
		case <-ctx.Done():
			a.logger.Debug(
				"applier stopped",
				slog.String("component", "kv-applier"),
				slog.String("reason", "context canceled"),
			)
			return ctx.Err()

		case entry, ok := <-applyCh:
			if !ok {
				a.logger.Debug(
					"applier stopped",
					slog.String("component", "kv-applier"),
					slog.String("reason", "apply channel closed"),
				)
				return nil
			}

			a.mu.Lock()
			metrics := a.metrics
			if metrics == nil {
				metrics = NoopKVMetrics{}
			}
			a.mu.Unlock()

			result := ApplyWithMetrics(a.store, entry, metrics)

			a.mu.Lock()

			if entry.Index > a.lastApplied {
				a.lastApplied = entry.Index
			}

			if commandNeedsResult(entry) {
				a.results[entry.Index] = result
			}

			if result.Err != nil && !isExpectedApplyError(result.Err) {
				a.applyErr = fmt.Errorf(
					"apply entry %d: %w",
					entry.Index,
					result.Err,
				)

				err := a.applyErr

				a.mu.Unlock()

				a.logger.Error(
					"failed to apply Raft log entry",
					slog.String("component", "kv-applier"),
					slog.Uint64("log_index", uint64(entry.Index)),
					slog.Any("error", result.Err),
				)

				return err
			}

			a.mu.Unlock()

			if result.Err != nil {
				a.logger.Debug(
					"Raft log entry produced expected state-machine error",
					slog.String("component", "kv-applier"),
					slog.Uint64("log_index", uint64(entry.Index)),
					slog.Any("error", result.Err),
				)
				continue
			}

			a.logger.Debug(
				"Raft log entry applied",
				slog.String("component", "kv-applier"),
				slog.Uint64("log_index", uint64(entry.Index)),
			)
		}
	}
}

func (a *Applier) WaitApplied(
	ctx context.Context,
	index model.LogIndex,
) error {
	if ctx == nil {
		return errors.New("wait applied context is required")
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		a.mu.Lock()

		applied := a.lastApplied >= index
		err := a.applyErr

		a.mu.Unlock()

		if err != nil {
			return err
		}

		if applied {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
		}
	}
}

func (a *Applier) WaitResult(
	ctx context.Context,
	index model.LogIndex,
) (ApplyResult, error) {
	if ctx == nil {
		return ApplyResult{}, errors.New("wait result context is required")
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		a.mu.Lock()

		result, ok := a.results[index]
		if ok {
			delete(a.results, index)
			a.mu.Unlock()

			return result, nil
		}

		if a.applyErr != nil {
			err := a.applyErr
			a.mu.Unlock()

			return ApplyResult{}, err
		}

		a.mu.Unlock()

		select {
		case <-ctx.Done():
			return ApplyResult{}, ctx.Err()

		case <-ticker.C:
		}
	}
}

func (a *Applier) RestoreSnapshot(
	snapshot model.Snapshot,
) error {
	if err := a.store.Restore(snapshot.Data); err != nil {
		a.logger.Error(
			"failed to restore KV snapshot",
			slog.String("component", "kv-applier"),
			slog.Uint64(
				"last_included_index",
				uint64(snapshot.LastIncludedIndex),
			),
			slog.Uint64(
				"last_included_term",
				uint64(snapshot.LastIncludedTerm),
			),
			slog.Any("error", err),
		)

		return fmt.Errorf("restore KV snapshot: %w", err)
	}

	a.mu.Lock()
	a.lastApplied = snapshot.LastIncludedIndex
	a.applyErr = nil
	a.results = make(map[model.LogIndex]ApplyResult)
	a.mu.Unlock()

	a.logger.Info(
		"KV snapshot restored",
		slog.String("component", "kv-applier"),
		slog.Uint64(
			"last_included_index",
			uint64(snapshot.LastIncludedIndex),
		),
		slog.Uint64(
			"last_included_term",
			uint64(snapshot.LastIncludedTerm),
		),
	)

	return nil
}

func commandNeedsResult(entry raft.LogEntry) bool {
	var command Command

	if err := json.Unmarshal(entry.Data, &command); err != nil {
		return false
	}

	switch command.Type {
	case CommandFencedPut,
		CommandClaimJob,
		CommandJobReclaim,
		CommandJobStart,
		CommandJobSucceeded,
		CommandJobFailed:
		return true

	default:
		return false
	}
}

func isExpectedApplyError(err error) bool {
	return errors.Is(err, lock.ErrStaleFencingToken) ||
		errors.Is(err, lock.ErrLockNotFound) ||
		errors.Is(err, ErrJobNotFound) ||
		errors.Is(err, ErrInvalidJob) ||
		errors.Is(err, ErrJobNotClaimable) ||
		errors.Is(err, ErrJobAlreadyClaimed) ||
		errors.Is(err, ErrInvalidJobState) ||
		errors.Is(err, ErrJobOwnershipLost)
}

func discardLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}
