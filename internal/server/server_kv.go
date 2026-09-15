package server

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
)

func (s *Server) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	logger := s.getLogger()
	start := time.Now()

	index, err := s.raft.ReadIndex(ctx)
	if err != nil {
		logger.Error(
			"failed to establish read index",
			"error", err,
		)

		return nil, false, fmt.Errorf("read index: %w", err)
	}

	if err := s.applier.WaitApplied(ctx, index); err != nil {
		logger.Error(
			"failed waiting for read index",
			"index", index,
			"error", err,
		)

		return nil, false, fmt.Errorf("wait for read index: %w", err)
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
