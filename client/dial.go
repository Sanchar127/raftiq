package client

import (
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial creates a new RaftIQ client connected to the given gRPC address.
func Dial(address string, opts ...grpc.DialOption) (*Client, error) {
	if address == "" {
		return nil, errors.New("client address is required")
	}

	dialOpts := append(
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		opts...,
	)

	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial %q: %w", address, err)
	}

	client, err := newWithConnection(conn)
	if err != nil {
		if closeErr := conn.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"create client: %w; close connection: %v",
				err,
				closeErr,
			)
		}

		return nil, fmt.Errorf("create client: %w", err)
	}

	return client, nil
}
