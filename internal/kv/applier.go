package kv

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
)

type Applier struct {
	store *Store

	mu          sync.RWMutex
	lastApplied raft.LogIndex
	applyErr    error
}

func NewApplier(store *Store) *Applier {
	return &Applier{
		store: store,
	}
}

func (a *Applier) Run(ctx context.Context, applyCh <-chan raft.LogEntry) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case entry, ok := <-applyCh:
			if !ok {
				return nil
			}

			if err := Apply(a.store, entry); err != nil {
				a.mu.Lock()
				a.applyErr = fmt.Errorf(
					"apply entry %d: %w",
					entry.Index,
					err,
				)
				a.mu.Unlock()

				return a.applyErr
			}

			a.mu.Lock()

			if entry.Index > a.lastApplied {
				a.lastApplied = entry.Index
			}

			a.mu.Unlock()
		}
	}
}

func (a *Applier) WaitApplied(
	ctx context.Context,
	index raft.LogIndex,
) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		a.mu.RLock()

		applied := a.lastApplied >= index
		err := a.applyErr

		a.mu.RUnlock()

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
