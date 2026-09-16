package client

import (
	"context"
	"errors"
	"sync"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"google.golang.org/grpc"
)

var (
	ErrClientClosed = errors.New("client is closed")
)

type Client struct {
	mu     sync.RWMutex
	kv     KV
	job    Job
	conn   *grpc.ClientConn
	closed bool
}

func New(kv KV) *Client {
	client := &Client{
		kv: kv,
	}

	if backend, ok := kv.(localJobBackend); ok {
		job, err := newLocalJob(backend)
		if err == nil {
			client.job = job
		}
	}

	return client
}
func newWithConnection(conn *grpc.ClientConn) (*Client, error) {
	if conn == nil {
		return nil, errors.New("grpc client connection is required")
	}

	grpcClient := raftiqv1.NewKVServiceClient(conn)

	kv, err := newGRPCKV(grpcClient)
	if err != nil {
		return nil, err
	}

	job, err := newGRPCJob(grpcClient)
	if err != nil {
		return nil, err
	}

	return &Client{
		kv:   kv,
		job:  job,
		conn: conn,
	}, nil
}

func (c *Client) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	kv, err := c.backend()
	if err != nil {
		return nil, false, err
	}

	return kv.Get(ctx, key)
}

func (c *Client) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	kv, err := c.backend()
	if err != nil {
		return err
	}

	return kv.Put(ctx, key, value)
}

func (c *Client) Delete(
	ctx context.Context,
	key string,
) error {
	kv, err := c.backend()
	if err != nil {
		return err
	}

	return kv.Delete(ctx, key)
}

func (c *Client) CreateJob(
	ctx context.Context,
	jobID string,
	payload []byte,
	scheduledAt int64,
) (uint64, error) {
	if c == nil {
		return 0, ErrClientClosed
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.job == nil {
		return 0, ErrClientClosed
	}

	return c.job.CreateJob(
		ctx,
		jobID,
		payload,
		scheduledAt,
	)
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	if c.conn == nil {
		return nil
	}

	if err := c.conn.Close(); err != nil {
		return err
	}

	return nil
}

func (c *Client) backend() (KV, error) {
	if c == nil {
		return nil, ErrClientClosed
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || c.kv == nil {
		return nil, ErrClientClosed
	}

	return c.kv, nil
}
