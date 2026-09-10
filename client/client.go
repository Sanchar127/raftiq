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
	conn   *grpc.ClientConn
	closed bool
}

func New(kv KV) *Client {
	return &Client{
		kv: kv,
	}
}

func newWithConnection(conn *grpc.ClientConn) (*Client, error) {
	if conn == nil {
		return nil, errors.New("grpc client connection is required")
	}

	kv, err := newGRPCKV(
		// The generated constructor accepts grpc.ClientConnInterface.
		// This keeps the transport behind our KV abstraction.
		raftiqv1.NewKVServiceClient(conn),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		kv:   kv,
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
