package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVService struct {
	raftiqv1.UnimplementedKVServiceServer
	store  kvRPC
	jobs   jobRPC
	logger *slog.Logger
}

type kvRPC interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

type jobRPC interface {
	CreateJob(
		ctx context.Context,
		jobID string,
		payload []byte,
		scheduledAt int64,
	) (*model.Job, raft.LogIndex, error)
}

func discardKVServiceLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

func (s *KVService) getLogger() *slog.Logger {
	if s.logger == nil {
		return discardKVServiceLogger()
	}

	return s.logger
}

func NewKVService(store kvRPC) (*KVService, error) {
	if store == nil {
		return nil, errors.New("kv store is required")
	}

	service := &KVService{
		store:  store,
		logger: discardKVServiceLogger(),
	}

	if jobs, ok := store.(jobRPC); ok {
		service.jobs = jobs
	}

	return service, nil
}

func (s *KVService) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardKVServiceLogger()
	}

	s.logger = logger.With(
		slog.String("component", "rpc"),
	)
}

func (s *KVService) Get(
	ctx context.Context,
	req *raftiqv1.GetRequest,
) (*raftiqv1.GetResponse, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	if req == nil {
		logger.Error(
			"kv get request rejected",
			"rpc_method", "Get",
			"reason", "nil request",
			"duration", time.Since(startedAt),
		)

		return nil, errors.New("get request is required")
	}

	key := req.GetKey()

	value, found, err := s.store.Get(ctx, key)
	if err != nil {
		logger.Error(
			"kv get failed",
			"rpc_method", "Get",
			"key_length", len(key),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return nil, err
	}

	logger.Debug(
		"kv get completed",
		"rpc_method", "Get",
		"key_length", len(key),
		"found", found,
		"value_size", len(value),
		"duration", time.Since(startedAt),
	)

	return &raftiqv1.GetResponse{
		Value: append([]byte(nil), value...),
		Found: found,
	}, nil
}

func (s *KVService) Put(
	ctx context.Context,
	req *raftiqv1.PutRequest,
) (*raftiqv1.PutResponse, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	if req == nil {
		logger.Error(
			"kv put request rejected",
			"rpc_method", "Put",
			"reason", "nil request",
			"duration", time.Since(startedAt),
		)

		return nil, errors.New("put request is required")
	}

	key := req.GetKey()
	value := req.GetValue()

	if err := s.store.Put(
		ctx,
		key,
		append([]byte(nil), value...),
	); err != nil {
		logger.Error(
			"kv put failed",
			"rpc_method", "Put",
			"key_length", len(key),
			"value_size", len(value),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return nil, err
	}

	logger.Debug(
		"kv put completed",
		"rpc_method", "Put",
		"key_length", len(key),
		"value_size", len(value),
		"duration", time.Since(startedAt),
	)

	return &raftiqv1.PutResponse{}, nil
}

func (s *KVService) Delete(
	ctx context.Context,
	req *raftiqv1.DeleteRequest,
) (*raftiqv1.DeleteResponse, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	if req == nil {
		logger.Error(
			"kv delete request rejected",
			"rpc_method", "Delete",
			"reason", "nil request",
			"duration", time.Since(startedAt),
		)

		return nil, errors.New("delete request is required")
	}

	key := req.GetKey()

	if err := s.store.Delete(ctx, key); err != nil {
		logger.Error(
			"kv delete failed",
			"rpc_method", "Delete",
			"key_length", len(key),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return nil, err
	}

	logger.Debug(
		"kv delete completed",
		"rpc_method", "Delete",
		"key_length", len(key),
		"duration", time.Since(startedAt),
	)

	return &raftiqv1.DeleteResponse{}, nil
}

func (s *KVService) CreateJob(
	ctx context.Context,
	req *raftiqv1.CreateJobRequest,
) (*raftiqv1.CreateJobResponse, error) {
	startedAt := time.Now()
	logger := s.getLogger()

	if req == nil {
		logger.Error(
			"job creation request rejected",
			"rpc_method", "CreateJob",
			"reason", "nil request",
			"duration", time.Since(startedAt),
		)

		return nil, status.Error(
			codes.InvalidArgument,
			"create job request is required",
		)
	}

	if s.jobs == nil {
		logger.Error(
			"job creation rejected",
			"rpc_method", "CreateJob",
			"reason", "backend does not support jobs",
			"duration", time.Since(startedAt),
		)

		return nil, status.Error(
			codes.Unimplemented,
			"job operations are not supported",
		)
	}

	job, index, err := s.jobs.CreateJob(
		ctx,
		req.GetJobId(),
		append([]byte(nil), req.GetPayload()...),
		req.GetScheduledAt(),
	)
	if err != nil {
		logger.Error(
			"job creation failed",
			"rpc_method", "CreateJob",
			"job_id", req.GetJobId(),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return nil, err
	}

	if job == nil {
		logger.Error(
			"job creation returned nil job",
			"rpc_method", "CreateJob",
			"job_id", req.GetJobId(),
			"index", index,
			"duration", time.Since(startedAt),
		)

		return nil, status.Error(
			codes.Internal,
			"job creation returned no job",
		)
	}

	logger.Debug(
		"job creation completed",
		"rpc_method", "CreateJob",
		"job_id", job.ID,
		"index", index,
		"scheduled_at", job.ScheduledAt,
		"payload_size", len(job.Payload),
		"duration", time.Since(startedAt),
	)

	return &raftiqv1.CreateJobResponse{
		Index: uint64(index),
	}, nil
}
