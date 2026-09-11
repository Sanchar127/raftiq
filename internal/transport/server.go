package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"google.golang.org/grpc"
)

var ErrServerClosed = errors.New("transport server is closed")

type Server struct {
	address  string
	listener net.Listener
	grpc     *grpc.Server
	logger   *slog.Logger
}

func discardTransportServerLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

func (s *Server) getLogger() *slog.Logger {
	if s == nil || s.logger == nil {
		return discardTransportServerLogger()
	}

	return s.logger
}

func NewServer(address string, opts ...grpc.ServerOption) (*Server, error) {
	if address == "" {
		return nil, errors.New("transport server address is required")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on %q: %w", address, err)
	}

	return &Server{
		address:  address,
		listener: listener,
		grpc:     grpc.NewServer(opts...),
		logger:   discardTransportServerLogger(),
	}, nil
}

func (s *Server) SetLogger(logger *slog.Logger) {
	if s == nil {
		return
	}

	if logger == nil {
		logger = discardTransportServerLogger()
	}

	s.logger = logger.With(
		slog.String("component", "rpc"),
		slog.String("server", "grpc"),
	)
}

func (s *Server) Serve() error {
	logger := s.getLogger()

	if s == nil || s.grpc == nil || s.listener == nil {
		logger.Error(
			"gRPC transport server serve rejected",
			"reason", "server is closed or uninitialized",
		)

		return ErrServerClosed
	}

	logger.Info(
		"gRPC transport server started",
		"address", s.Address(),
	)

	if err := s.grpc.Serve(s.listener); err != nil {
		logger.Error(
			"gRPC transport server stopped with error",
			"address", s.Address(),
			"error", err,
		)

		return fmt.Errorf("serve gRPC: %w", err)
	}

	logger.Info(
		"gRPC transport server stopped",
		"address", s.Address(),
	)

	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	logger := s.getLogger()

	if s == nil || s.grpc == nil {
		logger.Debug(
			"gRPC transport server shutdown skipped",
			"reason", "server is already closed or uninitialized",
		)

		return nil
	}

	if ctx == nil {
		logger.Error(
			"gRPC transport server shutdown rejected",
			"reason", "nil context",
		)

		return errors.New("shutdown context is nil")
	}

	logger.Info(
		"gRPC transport server graceful shutdown started",
		"address", s.Address(),
	)

	stopped := make(chan struct{})

	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		logger.Info(
			"gRPC transport server graceful shutdown completed",
			"address", s.Address(),
		)

		return nil

	case <-ctx.Done():
		s.grpc.Stop()

		logger.Warn(
			"gRPC transport server forced shutdown",
			"address", s.Address(),
			"error", ctx.Err(),
		)

		return fmt.Errorf("graceful shutdown: %w", ctx.Err())
	}
}

func (s *Server) Address() string {
	if s == nil || s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) RegisterRaftService(service *RaftService) error {
	logger := s.getLogger()

	if s == nil || s.grpc == nil {
		logger.Error(
			"raft service registration rejected",
			"reason", "server is closed or uninitialized",
		)

		return ErrServerClosed
	}

	if service == nil {
		logger.Error(
			"raft service registration rejected",
			"reason", "nil service",
		)

		return errors.New("raft service is required")
	}

	raftiqv1.RegisterRaftServiceServer(s.grpc, service)

	logger.Info(
		"raft gRPC service registered",
	)

	return nil
}

func (s *Server) RegisterKVService(service *KVService) error {
	logger := s.getLogger()

	if s == nil || s.grpc == nil {
		logger.Error(
			"KV service registration rejected",
			"reason", "server is closed or uninitialized",
		)

		return ErrServerClosed
	}

	if service == nil {
		logger.Error(
			"KV service registration rejected",
			"reason", "nil service",
		)

		return errors.New("kv service is required")
	}

	raftiqv1.RegisterKVServiceServer(s.grpc, service)

	logger.Info(
		"KV gRPC service registered",
	)

	return nil
}
