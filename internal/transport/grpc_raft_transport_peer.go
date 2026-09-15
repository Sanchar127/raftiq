package transport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func (t *GRPCTransport) SetTLSConfig(config *tls.Config) error {
	if config == nil {
		return errors.New("TLS config is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrGRPCTransportClosed
	}

	t.tls = config.Clone()

	return nil
}

func (t *GRPCTransport) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardGRPCTransportLogger()
	}

	t.mu.Lock()
	t.logger = logger.With(
		slog.String("component", "rpc"),
		slog.String("transport", "grpc_raft"),
	)
	t.mu.Unlock()
}

func (t *GRPCTransport) AddPeer(
	id raft.NodeID,
	address string,
	opts ...grpc.DialOption,
) error {
	startedAt := time.Now()
	logger := t.getLogger()

	if id == "" {
		logger.Error(
			"raft peer registration rejected",
			"operation", "add_peer",
			"reason", "empty peer ID",
		)

		return errors.New("raft peer ID is required")
	}

	if address == "" {
		logger.Error(
			"raft peer registration rejected",
			"operation", "add_peer",
			"peer_id", id,
			"reason", "empty peer address",
		)

		return errors.New("raft peer address is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		logger.Warn(
			"raft peer registration rejected",
			"operation", "add_peer",
			"peer_id", id,
			"reason", "transport closed",
			"duration", time.Since(startedAt),
		)

		return ErrGRPCTransportClosed
	}

	if _, exists := t.peers[id]; exists {
		logger.Warn(
			"raft peer registration rejected",
			"operation", "add_peer",
			"peer_id", id,
			"reason", "peer already registered",
			"duration", time.Since(startedAt),
		)

		return fmt.Errorf("raft peer %s already registered", id)
	}

	if t.tls == nil {
		return errors.New("TLS config is not configured")
	}

	tlsConfig := t.tls.Clone()
	tlsConfig.ServerName = peerServerName(id)

	dialOpts := append(
		[]grpc.DialOption{
			grpc.WithTransportCredentials(
				credentials.NewTLS(tlsConfig),
			),
		},
		opts...,
	)

	conn, err := grpc.NewClient(
		address,
		dialOpts...,
	)
	if err != nil {
		logger.Error(
			"raft peer connection creation failed",
			"operation", "add_peer",
			"peer_id", id,
			"address", address,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return fmt.Errorf(
			"create gRPC connection to peer %s: %w",
			id,
			err,
		)
	}

	t.conns[id] = conn
	t.peers[id] = raftiqv1.NewRaftServiceClient(conn)

	logger.Info(
		"raft peer registered",
		"operation", "add_peer",
		"peer_id", id,
		"address", address,
		"duration", time.Since(startedAt),
	)

	return nil
}

func (t *GRPCTransport) RemovePeer(id raft.NodeID) {
	startedAt := time.Now()
	logger := t.getLogger()

	t.mu.Lock()
	defer t.mu.Unlock()

	conn, exists := t.conns[id]
	if exists {
		if err := conn.Close(); err != nil {
			logger.Warn(
				"raft peer connection close failed",
				"operation", "remove_peer",
				"peer_id", id,
				"error", err,
			)
		}
	}

	delete(t.conns, id)
	delete(t.peers, id)

	logger.Info(
		"raft peer removed",
		"operation", "remove_peer",
		"peer_id", id,
		"was_registered", exists,
		"duration", time.Since(startedAt),
	)
}

func (t *GRPCTransport) Close() error {
	startedAt := time.Now()
	logger := t.getLogger()

	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()

		logger.Debug(
			"gRPC raft transport already closed",
			"operation", "close",
		)

		return nil
	}

	t.closed = true

	conns := make([]*grpc.ClientConn, 0, len(t.conns))

	for _, conn := range t.conns {
		conns = append(conns, conn)
	}

	peerCount := len(t.peers)

	t.peers = make(map[raft.NodeID]raftiqv1.RaftServiceClient)
	t.conns = make(map[raft.NodeID]*grpc.ClientConn)

	t.mu.Unlock()

	var closeErr error

	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	if closeErr != nil {
		logger.Error(
			"gRPC raft transport close completed with errors",
			"operation", "close",
			"peer_count", peerCount,
			"error", closeErr,
			"duration", time.Since(startedAt),
		)

		return closeErr
	}

	logger.Info(
		"gRPC raft transport closed",
		"operation", "close",
		"peer_count", peerCount,
		"duration", time.Since(startedAt),
	)

	return nil
}

func (t *GRPCTransport) peer(
	target raft.NodeID,
) (raftiqv1.RaftServiceClient, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, ErrGRPCTransportClosed
	}

	peer, ok := t.peers[target]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrGRPCPeerNotFound,
			target,
		)
	}

	return peer, nil
}
