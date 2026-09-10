package client

import (
	"context"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestDialAndKVOperations(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(grpcKVTestBufSize)
	grpcServer := grpc.NewServer()

	store := &fakeKVGRPCServer{
		getValue: []byte("raftiq"),
		getFound: true,
	}

	raftiqv1.RegisterKVServiceServer(grpcServer, store)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	defer func() {
		grpcServer.Stop()
		require.NoError(t, listener.Close())

		select {
		case err := <-serveErr:
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		default:
		}
	}()

	cl, err := Dial(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, cl.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	value, found, err := cl.Get(ctx, "name")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("raftiq"), value)
	require.Equal(t, "name", store.getKey)
}

func TestDialRejectsEmptyAddress(t *testing.T) {
	t.Parallel()

	cl, err := Dial("")

	require.Error(t, err)
	require.Nil(t, cl)
	require.Contains(t, err.Error(), "client address is required")
}

func TestDialCloseMakesClientUnavailable(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(grpcKVTestBufSize)
	grpcServer := grpc.NewServer()

	store := &fakeKVGRPCServer{
		getValue: []byte("raftiq"),
		getFound: true,
	}

	raftiqv1.RegisterKVServiceServer(grpcServer, store)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	defer func() {
		grpcServer.Stop()
		_ = listener.Close()
	}()

	cl, err := Dial(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)

	require.NoError(t, cl.Close())
	require.NoError(t, cl.Close())

	_, _, err = cl.Get(context.Background(), "name")
	require.ErrorIs(t, err, ErrClientClosed)

	err = cl.Put(context.Background(), "name", []byte("value"))
	require.ErrorIs(t, err, ErrClientClosed)

	err = cl.Delete(context.Background(), "name")
	require.ErrorIs(t, err, ErrClientClosed)
}
