package server

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/lock"
)

type LockGrant struct {
	Key          string
	OwnerID      string
	FencingToken uint64
	ExpiresAt    int64
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
