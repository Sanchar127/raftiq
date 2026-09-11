package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
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
	logger  *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	expirationMu      sync.Mutex
	pendingExpiration map[string]uint64
}

type LockGrant struct {
	Key          string
	OwnerID      string
	FencingToken uint64
	ExpiresAt    int64
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

func (s *Server) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	logger := s.getLogger()
	start := time.Now()

	commandData, err := kv.EncodeCommand(kv.Command{
		Type: kv.CommandReadBarrier,
	})
	if err != nil {
		logger.Error(
			"failed to encode read barrier",
			"error", err,
		)

		return nil, false, fmt.Errorf("encode read barrier: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose read barrier",
			"error", err,
		)

		return nil, false, err
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		logger.Error(
			"failed waiting for read barrier",
			"index", index,
			"error", err,
		)

		return nil, false, fmt.Errorf("wait for read barrier: %w", err)
	}

	value, ok := s.store.Get(key)

	logger.Debug(
		"read completed",
		"index", index,
		"found", ok,
		"duration", time.Since(start),
	)

	return value, ok, nil
}

func (s *Server) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	logger := s.getLogger()
	start := time.Now()

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:  kv.CommandPut,
		Key:   key,
		Value: append([]byte(nil), value...),
	})
	if err != nil {
		logger.Error(
			"failed to encode put command",
			"error", err,
		)

		return fmt.Errorf("encode put command: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose put command",
			"error", err,
		)

		return fmt.Errorf("propose put command: %w", err)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		logger.Error(
			"failed waiting for put application",
			"index", index,
			"error", err,
		)

		return fmt.Errorf("wait for put application: %w", err)
	}

	logger.Debug(
		"put completed",
		"index", index,
		"value_size", len(value),
		"duration", time.Since(start),
	)

	return nil
}

func (s *Server) Delete(
	ctx context.Context,
	key string,
) error {
	logger := s.getLogger()
	start := time.Now()

	commandData, err := kv.EncodeCommand(kv.Command{
		Type: kv.CommandDelete,
		Key:  key,
	})
	if err != nil {
		logger.Error(
			"failed to encode delete command",
			"error", err,
		)

		return fmt.Errorf("encode delete command: %w", err)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose delete command",
			"error", err,
		)

		return fmt.Errorf("propose delete command: %w", err)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		logger.Error(
			"failed waiting for delete application",
			"index", index,
			"error", err,
		)

		return fmt.Errorf("wait for delete application: %w", err)
	}

	logger.Debug(
		"delete completed",
		"index", index,
		"duration", time.Since(start),
	)

	return nil
}

func (s *Server) AcquireLock(
	ctx context.Context,
	key string,
	ownerID string,
	leaseMillis int64,
) (LockGrant, error) {
	logger := s.getLogger()
	start := time.Now()

	if key == "" {
		logger.Warn(
			"lock acquisition rejected",
			"reason", "invalid key",
		)

		return LockGrant{}, lock.ErrInvalidKey
	}

	if ownerID == "" {
		logger.Warn(
			"lock acquisition rejected",
			"reason", "invalid owner",
		)

		return LockGrant{}, lock.ErrInvalidOwner
	}

	if leaseMillis <= 0 {
		logger.Warn(
			"lock acquisition rejected",
			"reason", "invalid lease duration",
			"lease_millis", leaseMillis,
		)

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
		logger.Error(
			"failed to encode lock acquire command",
			"error", err,
		)

		return LockGrant{}, fmt.Errorf(
			"encode lock acquire command: %w",
			err,
		)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose lock acquire command",
			"error", err,
		)

		return LockGrant{}, fmt.Errorf(
			"propose lock acquire command: %w",
			err,
		)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		logger.Error(
			"failed waiting for lock acquisition",
			"index", index,
			"error", err,
		)

		return LockGrant{}, fmt.Errorf(
			"wait for lock acquisition: %w",
			err,
		)
	}

	current, ok := s.store.GetLock(key)
	if !ok || current.GrantIndex != index {
		logger.Debug(
			"lock acquisition did not produce expected grant",
			"index", index,
			"grant_found", ok,
			"duration", time.Since(start),
		)

		return LockGrant{}, lock.ErrLockBusy
	}

	if current.OwnerID != ownerID {
		logger.Debug(
			"lock acquisition completed for another owner",
			"index", index,
			"duration", time.Since(start),
		)

		return LockGrant{}, lock.ErrLockBusy
	}

	logger.Info(
		"lock acquired",
		"index", index,
		"fencing_token", current.FencingToken,
		"expires_at", current.ExpiresAt,
		"duration", time.Since(start),
	)

	return LockGrant{
		Key:          current.Key,
		OwnerID:      current.OwnerID,
		FencingToken: current.FencingToken,
		ExpiresAt:    current.ExpiresAt,
	}, nil
}

const lockExpirationPollInterval = 50 * time.Millisecond

func (s *Server) runLockExpirationWorker() {
	logger := s.getLogger()

	logger.Info(
		"lock expiration worker started",
		"poll_interval", lockExpirationPollInterval,
	)

	ticker := time.NewTicker(lockExpirationPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			logger.Info(
				"lock expiration worker stopped",
			)
			return

		case <-ticker.C:
			s.expireLocks()
		}
	}
}

func (s *Server) expireLocks() {
	logger := s.getLogger()

	if s.raft.State().Role != raft.Leader {
		return
	}

	now := time.Now().UnixNano()

	for _, current := range s.store.ListLocks() {
		if current.ExpiresAt <= 0 || current.ExpiresAt > now {
			continue
		}

		if !s.markExpirationPending(current.Key, current.FencingToken) {
			continue
		}

		logger.Debug(
			"proposing expired lock removal",
			"fencing_token", current.FencingToken,
			"expires_at", current.ExpiresAt,
		)

		go s.proposeLockExpiration(
			current.Key,
			current.FencingToken,
		)
	}
}

func (s *Server) markExpirationPending(
	key string,
	token uint64,
) bool {
	s.expirationMu.Lock()
	defer s.expirationMu.Unlock()

	current, ok := s.pendingExpiration[key]
	if ok && current == token {
		return false
	}

	s.pendingExpiration[key] = token
	return true
}

func (s *Server) clearExpirationPending(
	key string,
	token uint64,
) {
	s.expirationMu.Lock()
	defer s.expirationMu.Unlock()

	current, ok := s.pendingExpiration[key]
	if !ok || current != token {
		return
	}

	delete(s.pendingExpiration, key)
}

func (s *Server) proposeLockExpiration(
	key string,
	token uint64,
) {
	logger := s.getLogger()
	start := time.Now()

	defer s.clearExpirationPending(key, token)

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:         kv.CommandLockExpire,
		Key:           key,
		FencingToken: token,
	})
	if err != nil {
		logger.Error(
			"failed to encode lock expiration command",
			"fencing_token", token,
			"error", err,
		)

		return
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose lock expiration",
			"fencing_token", token,
			"error", err,
		)

		return
	}

	if err := s.applier.WaitApplied(s.ctx, index); err != nil {
		logger.Error(
			"failed waiting for lock expiration",
			"index", index,
			"fencing_token", token,
			"error", err,
		)

		return
	}

	logger.Info(
		"lock expiration applied",
		"index", index,
		"fencing_token", token,
		"duration", time.Since(start),
	)
}

func (s *Server) FencedPut(
	ctx context.Context,
	key string,
	value []byte,
	fencingToken uint64,
) error {
	logger := s.getLogger()
	start := time.Now()

	if key == "" {
		logger.Warn(
			"fenced put rejected",
			"reason", "invalid key",
		)

		return lock.ErrInvalidKey
	}

	if fencingToken == 0 {
		logger.Warn(
			"fenced put rejected",
			"reason", "invalid fencing token",
		)

		return lock.ErrStaleFencingToken
	}

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:         kv.CommandFencedPut,
		Key:          key,
		Value:        value,
		FencingToken: fencingToken,
	})
	if err != nil {
		logger.Error(
			"failed to encode fenced put command",
			"fencing_token", fencingToken,
			"error", err,
		)

		return fmt.Errorf(
			"encode fenced put command: %w",
			err,
		)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose fenced put command",
			"fencing_token", fencingToken,
			"error", err,
		)

		return fmt.Errorf(
			"propose fenced put command: %w",
			err,
		)
	}

	result, err := s.applier.WaitResult(ctx, index)
	if err != nil {
		logger.Error(
			"failed waiting for fenced put",
			"index", index,
			"fencing_token", fencingToken,
			"error", err,
		)

		return fmt.Errorf(
			"wait for fenced put: %w",
			err,
		)
	}

	if result.Err != nil {
		logger.Warn(
			"fenced put rejected by state machine",
			"index", index,
			"fencing_token", fencingToken,
			"error", result.Err,
			"duration", time.Since(start),
		)

		return result.Err
	}

	logger.Debug(
		"fenced put completed",
		"index", index,
		"fencing_token", fencingToken,
		"value_size", len(value),
		"duration", time.Since(start),
	)

	return nil
}

