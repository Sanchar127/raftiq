package transport

import (
	"context"
	"errors"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
)

type KVService struct {
	raftiqv1.UnimplementedKVServiceServer
	store kvRPC
}

type kvRPC interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

func NewKVService(store kvRPC) (*KVService, error) {
	if store == nil {
		return nil, errors.New("kv store is required")
	}

	return &KVService{
		store: store,
	}, nil
}

func (s *KVService) Get(
	ctx context.Context,
	req *raftiqv1.GetRequest,
) (*raftiqv1.GetResponse, error) {
	if req == nil {
		return nil, errors.New("get request is required")
	}

	value, found, err := s.store.Get(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}

	return &raftiqv1.GetResponse{
		Value: append([]byte(nil), value...),
		Found: found,
	}, nil
}

func (s *KVService) Put(
	ctx context.Context,
	req *raftiqv1.PutRequest,
) (*raftiqv1.PutResponse, error) {
	if req == nil {
		return nil, errors.New("put request is required")
	}

	if err := s.store.Put(
		ctx,
		req.GetKey(),
		append([]byte(nil), req.GetValue()...),
	); err != nil {
		return nil, err
	}

	return &raftiqv1.PutResponse{}, nil
}

func (s *KVService) Delete(
	ctx context.Context,
	req *raftiqv1.DeleteRequest,
) (*raftiqv1.DeleteResponse, error) {
	if req == nil {
		return nil, errors.New("delete request is required")
	}

	if err := s.store.Delete(ctx, req.GetKey()); err != nil {
		return nil, err
	}

	return &raftiqv1.DeleteResponse{}, nil
}