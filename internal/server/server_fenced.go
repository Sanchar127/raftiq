package server

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/lock"
)

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
