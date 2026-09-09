package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

type ApplyResult struct {
	Err error
}

type Applier struct {
	store *Store

	mu          sync.Mutex
	lastApplied model.LogIndex
	applyErr    error

	results map[model.LogIndex]ApplyResult
	cond    *sync.Cond
}

func NewApplier(store *Store) *Applier {
	applier := &Applier{
		store:   store,
		results: make(map[model.LogIndex]ApplyResult),
	}

	applier.cond = sync.NewCond(&applier.mu)

	return applier
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

			result := Apply(a.store, entry)

			a.mu.Lock()

			if entry.Index > a.lastApplied {
				a.lastApplied = entry.Index
			}

			a.results[entry.Index] = result

			if result.Err != nil &&
				!errors.Is(result.Err, lock.ErrStaleFencingToken) &&
				!errors.Is(result.Err, lock.ErrLockNotFound) {
				a.applyErr = fmt.Errorf(
					"apply entry %d: %w",
					entry.Index,
					result.Err,
				)

				err := a.applyErr

				a.cond.Broadcast()
				a.mu.Unlock()

				return err
			}

			a.cond.Broadcast()
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
