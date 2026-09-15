package server

import (
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
)

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
		Key:          key,
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
