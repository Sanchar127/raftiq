package server

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

func (s *Server) CreateJob(
	ctx context.Context,
	jobID string,
	payload []byte,
	scheduledAt int64,
) (*model.Job, raft.LogIndex, error) {
	logger := s.getLogger()
	start := time.Now()

	if jobID == "" {
		logger.Warn(
			"job creation rejected",
			"reason", "empty job id",
		)

		return nil, 0, fmt.Errorf(
			"%w: job id is required",
			kv.ErrInvalidJob,
		)
	}

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:        kv.CommandCreateJob,
		JobID:       jobID,
		Payload:     append([]byte(nil), payload...),
		ScheduledAt: scheduledAt,
	})
	if err != nil {
		logger.Error(
			"failed to encode create job command",
			"job_id", jobID,
			"error", err,
		)

		return nil, 0, fmt.Errorf(
			"encode create job command: %w",
			err,
		)
	}

	index, err := s.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose create job command",
			"job_id", jobID,
			"error", err,
		)

		return nil, 0, fmt.Errorf(
			"propose create job command: %w",
			err,
		)
	}

	result, err := s.applier.WaitResult(ctx, index)
	if err != nil {
		logger.Error(
			"failed waiting for job creation",
			"job_id", jobID,
			"index", index,
			"error", err,
		)

		return nil, 0, fmt.Errorf(
			"wait for job creation: %w",
			err,
		)
	}

	if result.Err != nil {
		logger.Warn(
			"job creation rejected by state machine",
			"job_id", jobID,
			"index", index,
			"error", result.Err,
			"duration", time.Since(start),
		)

		return nil, index, result.Err
	}

	if result.Job == nil {
		logger.Error(
			"job creation completed without job result",
			"job_id", jobID,
			"index", index,
		)

		return nil, index, fmt.Errorf(
			"create job: missing job result at index %d",
			index,
		)
	}

	logger.Debug(
		"job created",
		"job_id", jobID,
		"index", index,
		"scheduled_at", scheduledAt,
		"payload_size", len(payload),
		"duration", time.Since(start),
	)

	return result.Job, index, nil
}
