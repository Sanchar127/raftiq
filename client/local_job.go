package client

import (
	"context"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

type localJobBackend interface {
	CreateJob(
		ctx context.Context,
		jobID string,
		payload []byte,
		scheduledAt int64,
	) (*model.Job, raft.LogIndex, error)
}

type localJob struct {
	backend localJobBackend
}

func newLocalJob(backend localJobBackend) (*localJob, error) {
	if backend == nil {
		return nil, ErrClientClosed
	}

	return &localJob{
		backend: backend,
	}, nil
}

func (j *localJob) CreateJob(
	ctx context.Context,
	jobID string,
	payload []byte,
	scheduledAt int64,
) (uint64, error) {
	if j == nil || j.backend == nil {
		return 0, ErrClientClosed
	}

	_, index, err := j.backend.CreateJob(
		ctx,
		jobID,
		payload,
		scheduledAt,
	)
	if err != nil {
		return 0, err
	}

	return uint64(index), nil
}
