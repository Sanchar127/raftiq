package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
)

type Server struct {
	raft    *raft.RaftNode
	store   *kv.Store
	applier *kv.Applier
	logger  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	expirationMu      sync.Mutex
	pendingExpiration map[string]uint64
}

func discardServerLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *Server) getLogger() *slog.Logger {
	if s.logger == nil {
		return discardServerLogger()
	}

	return s.logger
}

func (s *Server) SetLogger(logger *slog.Logger) {
	if logger == nil {
		s.logger = discardServerLogger()
		return
	}

	s.logger = logger
}

func NewServer(raftNode *raft.RaftNode, store *kv.Store) *Server {
	applier := kv.NewApplier(store)

	raftNode.SetSnapshotRestore(applier.RestoreSnapshot)

	return &Server{
		raft:              raftNode,
		store:             store,
		applier:           applier,
		logger:            discardServerLogger(),
		pendingExpiration: make(map[string]uint64),
	}
}

func (s *Server) Start() error {
	logger := s.getLogger()

	logger.Info(
		"starting server",
	)

	s.ctx, s.cancel = context.WithCancel(context.Background())

	snapshot, err := s.raft.Snapshot()
	if err != nil {
		logger.Error(
			"failed to load raft snapshot",
			"error", err,
		)

		s.cancel()
		return fmt.Errorf("load raft snapshot: %w", err)
	}

	if snapshot.LastIncludedIndex > 0 {
		logger.Info(
			"restoring raft snapshot",
			"last_included_index", snapshot.LastIncludedIndex,
			"last_included_term", snapshot.LastIncludedTerm,
			"snapshot_size", len(snapshot.Data),
		)

		if err := s.applier.RestoreSnapshot(snapshot); err != nil {
			logger.Error(
				"failed to restore raft snapshot",
				"last_included_index", snapshot.LastIncludedIndex,
				"error", err,
			)

			s.cancel()
			return fmt.Errorf("restore snapshot: %w", err)
		}
	}

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		if err := s.applier.Run(s.ctx, s.raft.ApplyCh()); err != nil {
			logger.Error(
				"kv applier stopped with error",
				"error", err,
			)
		}
	}()

	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		s.runLockExpirationWorker()
	}()

	logger.Info(
		"server started",
	)

	return nil
}

func (s *Server) Stop() {
	logger := s.getLogger()

	if s.cancel == nil {
		logger.Debug(
			"server stop requested but server is not running",
		)
		return
	}

	logger.Info(
		"stopping server",
	)

	s.cancel()
	s.wg.Wait()

	logger.Info(
		"server stopped",
	)
}
func (s *Server) SetKVMetrics(metrics kv.KVMetrics) {
	s.applier.SetMetrics(metrics)
}
