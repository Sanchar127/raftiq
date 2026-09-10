package client

import (
	"context"
	"errors"
)

var ErrClientClosed = errors.New("client is closed")

type Client struct {
	kv KV
}

func New(kv KV) *Client {
	return &Client{
		kv: kv,
	}
}

func (c *Client) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	if c == nil || c.kv == nil {
		return nil, false, ErrClientClosed
	}

	return c.kv.Get(ctx, key)
}

func (c *Client) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	if c == nil || c.kv == nil {
		return ErrClientClosed
	}

	return c.kv.Put(ctx, key, value)
}

func (c *Client) Delete(
	ctx context.Context,
	key string,
) error {
	if c == nil || c.kv == nil {
		return ErrClientClosed
	}

	return c.kv.Delete(ctx, key)
}
