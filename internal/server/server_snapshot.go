package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

const (
	DefaultSnapshotInterval                 = time.Second
	DefaultSnapshotThreshold model.LogIndex = 1000
)

var ErrInvalidSnapshotConfig = errors.New("invalid snapshot configuration")

type SnapshotConfig struct {
	Interval  time.Duration
	Threshold model.LogIndex
}

func (c SnapshotConfig) validate() error {
	if c.Interval <= 0 {
		return fmt.Errorf(
			"%w: interval must be positive",
			ErrInvalidSnapshotConfig,
		)
	}

	if c.Threshold <= 0 {
		return fmt.Errorf(
			"%w: threshold must be positive",
			ErrInvalidSnapshotConfig,
		)
	}

	return nil
}

func (s *Server) runSnapshotWorker() {
	logger := s.getLogger()
	config := s.snapshotConfig

	ticker := time.NewTicker(config.Interval)
	defer ticker.Stop()

	logger.Info(
		"snapshot worker started",
		"interval", config.Interval,
		"threshold", config.Threshold,
	)

	for {
		select {
		case <-s.ctx.Done():
			logger.Debug(
				"snapshot worker stopped",
				"reason", "context canceled",
			)
			return

		case <-ticker.C:
			if err := s.maybeCreateSnapshot(); err != nil {
				logger.Error(
					"snapshot attempt failed",
					"error", err,
				)
			}
		}
	}
}

func (s *Server) maybeCreateSnapshot() error {
	currentSnapshot, err := s.raft.Snapshot()
	if err != nil {
		return fmt.Errorf("load current snapshot: %w", err)
	}

	data, appliedIndex, err := s.applier.Snapshot()
	if err != nil {
		return fmt.Errorf("capture applied state: %w", err)
	}

	if appliedIndex == 0 {
		return nil
	}

	if appliedIndex <= currentSnapshot.LastIncludedIndex {
		return nil
	}

	if appliedIndex-currentSnapshot.LastIncludedIndex <
		s.snapshotConfig.Threshold {
		return nil
	}

	logger := s.getLogger()

	logger.Info(
		"snapshot threshold reached",
		"last_included_index", currentSnapshot.LastIncludedIndex,
		"applied_index", appliedIndex,
		"threshold", s.snapshotConfig.Threshold,
	)

	if err := s.raft.CreateSnapshot(appliedIndex, data); err != nil {
		return fmt.Errorf(
			"create snapshot at index %d: %w",
			appliedIndex,
			err,
		)
	}

	return nil
}
