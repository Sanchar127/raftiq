package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	mu          sync.Mutex
	lastApplied model.LogIndex
	applyErr    error

	results map[model.LogIndex]ApplyResult
	cond    *sync.Cond
}

func NewApplier(store *Store) *Applier {
	applier := &Applier{
		store:   store,
		metrics: NoopKVMetrics{},
		results: make(map[model.LogIndex]ApplyResult),
	}

	return applier
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

func (a *Applier) Run(
	ctx context.Context,
	applyCh <-chan raft.LogEntry,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case entry, ok := <-applyCh:
			if !ok {
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

				return err
			}

			a.mu.Unlock()
		}
	}
}

func (a *Applier) WaitApplied(
	ctx context.Context,
	index model.LogIndex,
) error {
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
		return fmt.Errorf("restore KV snapshot: %w", err)
	}

	a.mu.Lock()
	a.lastApplied = snapshot.LastIncludedIndex
	a.applyErr = nil
	a.results = make(map[model.LogIndex]ApplyResult)
	a.mu.Unlock()

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
