package client

import (
	"context"

	"github.com/sanchar127/raftiq/internal/server"
)

type Client struct {
	server *server.Server
}

func New(server *server.Server) *Client {
	return &Client{
		server: server,
	}
}

func (c *Client) Get(
	ctx context.Context,
	key string,
) ([]byte, bool, error) {
	return c.server.Get(ctx, key)
}

func (c *Client) Put(
	ctx context.Context,
	key string,
	value []byte,
) error {
	return c.server.Put(ctx, key, value)
}

func (c *Client) Delete(
	ctx context.Context,
	key string,
) error {
	return c.server.Delete(ctx, key)
}
