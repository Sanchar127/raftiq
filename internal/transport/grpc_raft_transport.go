package transport

import (
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"sync"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc"
)

var (
	ErrGRPCTransportClosed = errors.New("gRPC raft transport is closed")
	ErrGRPCPeerNotFound    = errors.New("gRPC raft peer not found")
)

type GRPCTransport struct {
	mu     sync.RWMutex
	peers  map[raft.NodeID]raftiqv1.RaftServiceClient
	conns  map[raft.NodeID]*grpc.ClientConn
	closed bool
	logger *slog.Logger
	tls    *tls.Config
}

func discardGRPCTransportLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

func (t *GRPCTransport) getLogger() *slog.Logger {
	if t.logger == nil {
		return discardGRPCTransportLogger()
	}

	return t.logger
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		peers:  make(map[raft.NodeID]raftiqv1.RaftServiceClient),
		conns:  make(map[raft.NodeID]*grpc.ClientConn),
		logger: discardGRPCTransportLogger(),
	}
}
