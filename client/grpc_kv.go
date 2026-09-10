package client

import (
	"context"
	"errors"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
)

var ErrGRPCClientClosed = errors.New("grpc client is closed")

type grpcKV struct {
	client raftiqv1.KVServiceClient
}

func newGRPCKV(client raftiqv1.KVServiceClient) (*grpcKV, error) {
	if client == nil {
		return nil, errors.New("kv grpc client is required")
	}

	return &grpcKV{
		client: client,
	}, nil
}

func (c *grpcKV) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, ErrGRPCClientClosed
	}

	response, err := c.client.Get(ctx, &raftiqv1.GetRequest{
		Key: key,
	})
	if err != nil {
		return nil, false, err
	}

	return append([]byte(nil), response.GetValue()...), response.GetFound(), nil
}

func (c *grpcKV) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	if c == nil || c.client == nil {
		return ErrGRPCClientClosed
	}

	_, err := c.client.Put(ctx, &raftiqv1.PutRequest{
		Key:   key,
		Value: append([]byte(nil), value...),
	})

	return err
}

func (c *grpcKV) Delete(
	ctx context.Context,
	key string,
) error {
	if c == nil || c.client == nil {
		return ErrGRPCClientClosed
	}

	_, err := c.client.Delete(ctx, &raftiqv1.DeleteRequest{
		Key: key,
	})

	return err
}