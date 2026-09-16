package client

import "context"

type KV interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type Job interface {
	CreateJob(
		ctx context.Context,
		jobID string,
		payload []byte,
		scheduledAt int64,
	) (uint64, error)
}
