package server

import (
	"context"
	"sync"
	"fmt"
	"github.com/sanchar127/raftiq/internal/kv"
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

func NewServer(raftNode *raft.RaftNode, store *kv.Store) *Server {
	return &Server{
		raft:    raftNode,
		store:   store,
		applier: kv.NewApplier(store),
	}
}

func (s *Server) Start() {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		_ = s.applier.Run(s.ctx, s.raft.ApplyCh())
	}()
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

	if err := s.raft.WaitApplied(ctx, index); err != nil {
		return nil, false, fmt.Errorf("wait for read barrier: %w", err)
	}

	value, ok := s.store.Get(key)

	return value, ok, nil
}