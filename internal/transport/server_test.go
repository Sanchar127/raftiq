package transport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	t.Parallel()

	server, err := NewServer("127.0.0.1:0")
	require.NoError(t, err)
	require.NotNil(t, server)

	t.Cleanup(func() {
		require.NoError(t, server.Shutdown(context.Background()))
	})

	address := server.Address()
	require.NotEmpty(t, address)
	require.NotContains(t, address, ":0")
}

func TestNewServerRejectsEmptyAddress(t *testing.T) {
	t.Parallel()

	server, err := NewServer("")
	require.Error(t, err)
	require.Nil(t, server)
	require.ErrorContains(t, err, "address is required")
}

func TestServerServeAndShutdown(t *testing.T) {
	t.Parallel()

	server, err := NewServer("127.0.0.1:0")
	require.NoError(t, err)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- server.Serve()
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, server.Shutdown(shutdownCtx))

	select {
	case err := <-serveErr:
		if err != nil {
			require.ErrorContains(t, err, "server has been stopped")
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after shutdown")
	}
}

func TestServerShutdownWithCancelledContext(t *testing.T) {
	t.Parallel()

	server, err := NewServer("127.0.0.1:0")
	require.NoError(t, err)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- server.Serve()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = server.Shutdown(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	select {
	case <-serveErr:
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after forced shutdown")
	}
}

func TestNilServerMethods(t *testing.T) {
	t.Parallel()

	var server *Server

	require.Equal(t, "", server.Address())
	require.ErrorIs(t, server.Serve(), ErrServerClosed)
	require.NoError(t, server.Shutdown(context.Background()))
}

func TestServerRegisterRaftService(t *testing.T) {
	t.Parallel()

	server, err := NewServer("127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, server.Shutdown(context.Background()))
	})

	service, err := NewRaftService(&fakeRaftNode{})
	require.NoError(t, err)

	require.NoError(t, server.RegisterRaftService(service))
}

func TestServerRegisterKVService(t *testing.T) {
	t.Parallel()

	server, err := NewServer("127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, server.Shutdown(context.Background()))
	})

	service, err := NewKVService(&fakeKVStore{})
	require.NoError(t, err)

	require.NoError(t, server.RegisterKVService(service))
}

func TestServerRegistrationRejectsNilServices(t *testing.T) {
	t.Parallel()

	server, err := NewServer("127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, server.Shutdown(context.Background()))
	})

	require.ErrorContains(
		t,
		server.RegisterRaftService(nil),
		"raft service is required",
	)

	require.ErrorContains(
		t,
		server.RegisterKVService(nil),
		"kv service is required",
	)
}

func TestNilServerRegistration(t *testing.T) {
	t.Parallel()

	var server *Server

	require.ErrorIs(t, server.RegisterRaftService(nil), ErrServerClosed)
	require.ErrorIs(t, server.RegisterKVService(nil), ErrServerClosed)
}
