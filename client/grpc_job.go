package client

import (
	"context"
	"errors"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
)

type grpcJob struct {
	client raftiqv1.KVServiceClient
}

func newGRPCJob(client raftiqv1.KVServiceClient) (*grpcJob, error) {
	if client == nil {
		return nil, errors.New("job grpc client is required")
	}

	return &grpcJob{
		client: client,
	}, nil
}

func (c *grpcJob) CreateJob(
	ctx context.Context,
	jobID string,
	payload []byte,
	scheduledAt int64,
) (uint64, error) {
	if c == nil || c.client == nil {
		return 0, ErrGRPCClientClosed
	}

	response, err := c.client.CreateJob(
		ctx,
		&raftiqv1.CreateJobRequest{
			JobId:       jobID,
			Payload:     append([]byte(nil), payload...),
			ScheduledAt: scheduledAt,
		},
	)
	if err != nil {
		return 0, err
	}

	return response.GetIndex(), nil
}
