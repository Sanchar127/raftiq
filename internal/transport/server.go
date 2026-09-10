package transport

import (
	"context"
	"errors"
	"fmt"
	"net"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"google.golang.org/grpc"
)

var ErrServerClosed = errors.New("transport server is closed")

type Server struct {
	address  string
	listener net.Listener
	grpc     *grpc.Server
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
	}, nil
}

func (s *Server) Serve() error {
	if s == nil || s.grpc == nil || s.listener == nil {
		return ErrServerClosed
	}

	if err := s.grpc.Serve(s.listener); err != nil {
		return fmt.Errorf("serve gRPC: %w", err)
	}

	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.grpc == nil {
		return nil
	}

	stopped := make(chan struct{})

	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.grpc.Stop()
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
	if s == nil || s.grpc == nil {
		return ErrServerClosed
	}

	if service == nil {
		return errors.New("raft service is required")
	}

	raftiqv1.RegisterRaftServiceServer(s.grpc, service)
	return nil
}

func (s *Server) RegisterKVService(service *KVService) error {
	if s == nil || s.grpc == nil {
		return ErrServerClosed
	}

	if service == nil {
		return errors.New("kv service is required")
	}

	raftiqv1.RegisterKVServiceServer(s.grpc, service)
	return nil
}
