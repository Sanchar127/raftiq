package server

import (
	"context"
	"sync"

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