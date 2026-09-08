package kv

import (
	"context"
	"fmt"

	"github.com/sanchar127/raftiq/internal/raft"
)

type Applier struct {
	store *Store
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
				return fmt.Errorf("apply entry %d: %w", entry.Index, err)
			}
		}
	}
}
