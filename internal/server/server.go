package server

import (
	"context"
	"fmt"
	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
	"sync"
)

type Server struct {
	raft    *raft.RaftNode
	store   *kv.Store
	applier *kv.Applier

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewServer(raftNode *raft.RaftNode, store *kv.Store) *Server {
	return &Server{
		raft:    raftNode,
		store:   store,
		applier: kv.NewApplier(store),
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
