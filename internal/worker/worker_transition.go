package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
)

// transitionJob commits a durable job-state transition through Raft and waits
// until the corresponding state-machine result is available.
func (w *Worker) transitionJob(
	ctx context.Context,
	job model.Job,
	commandType kv.CommandType,
	expectedState model.JobState,
	nextState model.JobState,
) (model.Job, error) {
	logger := w.getLogger().With(
		"component", "worker",
		"worker_id", w.id,
		"job_id", job.ID,
		"execution_id", job.ExecutionID,
		"fencing_token", job.FencingToken,
		"command_type", commandType,
	)

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:          commandType,
		JobID:         string(job.ID),
		OwnerID:       w.id,
		ExecutionID:   job.ExecutionID,
		FencingToken:  job.FencingToken,
		ExpectedState: expectedState,
		At:            time.Now().UnixNano(),
	})
	if err != nil {
		logger.Error(
			"failed to encode job transition",
			"error", err,
		)

		return model.Job{}, fmt.Errorf(
			"encode job transition: %w",
			err,
		)
	}

	index, err := w.raft.Propose(commandData)
	if err != nil {
		logger.Error(
			"failed to propose job transition",
			"error", err,
		)

		return model.Job{}, fmt.Errorf(
			"propose job transition: %w",
			err,
		)
	}

	result, err := w.applier.WaitResult(ctx, index)
	if err != nil {
		logger.Error(
			"failed waiting for job transition result",
			"raft_index", index,
			"error", err,
		)

		return model.Job{}, fmt.Errorf(
			"wait for job transition at index %d: %w",
			index,
			err,
		)
	}

	if result.Err != nil {
		return model.Job{}, result.Err
	}

	if result.Job == nil {
		err := errors.New("job transition returned no job")

		logger.Error(
			"job transition returned no job",
			"raft_index", index,
			"error", err,
		)

		return model.Job{}, err
	}

	if result.Job.State != nextState {
		err := fmt.Errorf(
			"job transition produced state %s, want %s",
			result.Job.State,
			nextState,
		)

		logger.Error(
			"job transition produced unexpected state",
			"raft_index", index,
			"actual_state", result.Job.State,
			"expected_state", nextState,
			"error", err,
		)

		return model.Job{}, err
	}

	logger.Debug(
		"job state transition committed",
		"raft_index", index,
		"from_state", expectedState,
		"to_state", nextState,
	)

	return *result.Job, nil
}
