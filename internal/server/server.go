package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/raft"
)

type Server struct {
	raft    *raft.RaftNode
	store   *kv.Store
	applier *kv.Applier

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type LockGrant struct {
	Key          string
	OwnerID      string
	FencingToken uint64
	ExpiresAt    int64
}

func NewServer(raftNode *raft.RaftNode, store *kv.Store) *Server {
	applier := kv.NewApplier(store)

	raftNode.SetSnapshotRestore(applier.RestoreSnapshot)

	return &Server{
		raft:    raftNode,
		store:   store,
		applier: applier,
	}
}

func (s *Server) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	snapshot, err := s.raft.Snapshot()
	if err != nil {
		s.cancel()
		return fmt.Errorf("load raft snapshot: %w", err)
	}

	if snapshot.LastIncludedIndex > 0 {
		if err := s.applier.RestoreSnapshot(snapshot); err != nil {
			s.cancel()
			return fmt.Errorf("restore snapshot: %w", err)
		}
	}

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		_ = s.applier.Run(s.ctx, s.raft.ApplyCh())
	}()

	return nil
}

func (s *Server) Stop() {
	if s.cancel == nil {
		return
	}

	s.cancel()
	s.wg.Wait()
}

func (s *Server) Get(ctx context.Context, key string) ([]byte, bool, error) {
	commandData, err := kv.EncodeCommand(kv.Command{
		Type: kv.CommandReadBarrier,
	})
	if err != nil {
		return nil, false, fmt.Errorf("encode read barrier: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		return nil, false, err
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		return nil, false, fmt.Errorf("wait for read barrier: %w", err)
	}

	value, ok := s.store.Get(key)

	return value, ok, nil
}

func (s *Server) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	commandData, err := kv.EncodeCommand(kv.Command{
		Type:  kv.CommandPut,
		Key:   key,
		Value: append([]byte(nil), value...),
	})
	if err != nil {
		return fmt.Errorf("encode put command: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		return fmt.Errorf("propose put command: %w", err)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		return fmt.Errorf("wait for put application: %w", err)
	}

	return nil
}

func (s *Server) Delete(
	ctx context.Context,
	key string,
) error {
	commandData, err := kv.EncodeCommand(kv.Command{
		Type: kv.CommandDelete,
		Key:  key,
	})
	if err != nil {
		return fmt.Errorf("encode delete command: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		return fmt.Errorf("propose delete command: %w", err)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		return fmt.Errorf("wait for delete application: %w", err)
	}

	return nil
}

func (s *Server) AcquireLock(
	ctx context.Context,
	key string,
	ownerID string,
	leaseMillis int64,
) (LockGrant, error) {
	if key == "" {
		return LockGrant{}, lock.ErrInvalidKey
	}

	if ownerID == "" {
		return LockGrant{}, lock.ErrInvalidOwner
	}

	if leaseMillis <= 0 {
		return LockGrant{}, fmt.Errorf("lease duration must be positive")
	}

	expiresAt := time.Now().
		Add(time.Duration(leaseMillis) * time.Millisecond).
		UnixNano()

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:      kv.CommandLockAcquire,
		Key:       key,
		OwnerID:   ownerID,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		return LockGrant{}, fmt.Errorf(
			"encode lock acquire command: %w",
			err,
		)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		return LockGrant{}, fmt.Errorf(
			"propose lock acquire command: %w",
			err,
		)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		return LockGrant{}, fmt.Errorf(
			"wait for lock acquisition: %w",
			err,
		)
	}

	current, ok := s.store.GetLock(key)
	if !ok || current.GrantIndex != index {
		return LockGrant{}, lock.ErrLockBusy
	}

	if current.OwnerID != ownerID {
		return LockGrant{}, lock.ErrLockBusy
	}

	return LockGrant{
		Key:          current.Key,
		OwnerID:      current.OwnerID,
		FencingToken: current.FencingToken,
		ExpiresAt:    current.ExpiresAt,
	}, nil
}
