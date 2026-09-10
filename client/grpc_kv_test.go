package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const grpcKVTestBufSize = 1024 * 1024

type fakeKVGRPCServer struct {
	raftiqv1.UnimplementedKVServiceServer

	getValue  []byte
	getFound  bool
	getErr    error
	putErr    error
	deleteErr error
	getKey    string
	putKey    string
	putValue  []byte
	deleteKey string
}

func (s *fakeKVGRPCServer) Get(
	_ context.Context,
	req *raftiqv1.GetRequest,
) (*raftiqv1.GetResponse, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}

	s.getKey = req.GetKey()

	return &raftiqv1.GetResponse{
		Value: append([]byte(nil), s.getValue...),
		Found: s.getFound,
	}, nil
}

func (s *fakeKVGRPCServer) Put(
	_ context.Context,
	req *raftiqv1.PutRequest,
) (*raftiqv1.PutResponse, error) {
	if s.putErr != nil {
		return nil, s.putErr
	}

	s.putKey = req.GetKey()
	s.putValue = append([]byte(nil), req.GetValue()...)

	return &raftiqv1.PutResponse{}, nil
}

func (s *fakeKVGRPCServer) Delete(
	_ context.Context,
	req *raftiqv1.DeleteRequest,
) (*raftiqv1.DeleteResponse, error) {
	if s.deleteErr != nil {
		return nil, s.deleteErr
	}

	s.deleteKey = req.GetKey()

	return &raftiqv1.DeleteResponse{}, nil
}

func TestNewGRPCKV(t *testing.T) {
	t.Parallel()

	client, err := newGRPCKV(nil)

	require.Error(t, err)
	require.Nil(t, client)
}

func TestGRPCKVGet(t *testing.T) {
	t.Parallel()

	server := &fakeKVGRPCServer{
		getValue: []byte("raftiq"),
		getFound: true,
	}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	value, found, err := kv.Get(ctx, "name")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("raftiq"), value)
	require.Equal(t, "name", server.getKey)
}

func TestGRPCKVGetNotFound(t *testing.T) {
	t.Parallel()

	server := &fakeKVGRPCServer{}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	value, found, err := kv.Get(ctx, "missing")

	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, value)
}

func TestGRPCKVGetPropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("get failed")

	server := &fakeKVGRPCServer{
		getErr: expectedErr,
	}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, err := kv.Get(ctx, "name")

	require.Error(t, err)
	require.Contains(t, err.Error(), "get failed")
}

func TestGRPCKVPut(t *testing.T) {
	t.Parallel()

	server := &fakeKVGRPCServer{}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	value := []byte("raftiq")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := kv.Put(ctx, "name", value)

	require.NoError(t, err)
	require.Equal(t, "name", server.putKey)
	require.Equal(t, value, server.putValue)

	value[0] = 'X'

	require.Equal(t, []byte("raftiq"), server.putValue)
}

func TestGRPCKVPutPropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("put failed")

	server := &fakeKVGRPCServer{
		putErr: expectedErr,
	}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := kv.Put(ctx, "name", []byte("raftiq"))

	require.Error(t, err)
	require.Contains(t, err.Error(), "put failed")
}

func TestGRPCKVDelete(t *testing.T) {
	t.Parallel()

	server := &fakeKVGRPCServer{}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := kv.Delete(ctx, "name")

	require.NoError(t, err)
	require.Equal(t, "name", server.deleteKey)
}

func TestGRPCKVDeletePropagatesError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("delete failed")

	server := &fakeKVGRPCServer{
		deleteErr: expectedErr,
	}

	kv, cleanup := newTestGRPCKV(t, server)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := kv.Delete(ctx, "name")

	require.Error(t, err)
	require.Contains(t, err.Error(), "delete failed")
}

func TestGRPCKVNilClient(t *testing.T) {
	t.Parallel()

	var kv *grpcKV

	ctx := context.Background()

	_, _, err := kv.Get(ctx, "name")
	require.ErrorIs(t, err, ErrGRPCClientClosed)

	require.ErrorIs(t, kv.Put(ctx, "name", []byte("value")), ErrGRPCClientClosed)
	require.ErrorIs(t, kv.Delete(ctx, "name"), ErrGRPCClientClosed)
}

func newTestGRPCKV(
	t *testing.T,
	server raftiqv1.KVServiceServer,
) (*grpcKV, func()) {
	t.Helper()

	listener := bufconn.Listen(grpcKVTestBufSize)
	grpcServer := grpc.NewServer()

	raftiqv1.RegisterKVServiceServer(grpcServer, server)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	kv, err := newGRPCKV(raftiqv1.NewKVServiceClient(conn))
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, conn.Close())
		grpcServer.Stop()
		require.NoError(t, listener.Close())

		select {
		case err := <-serveErr:
			require.ErrorIs(t, err, grpc.ErrServerStopped)
		default:
		}
	}

	return kv, cleanup
}
