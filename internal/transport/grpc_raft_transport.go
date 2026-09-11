package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	conn, err := grpc.NewClient(
		address,
		append(
			[]grpc.DialOption{
				grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				),
			},
			opts...,
		)...,
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

func (t *GRPCTransport) RequestVote(
	ctx context.Context,
	target raft.NodeID,
	args raft.RequestVoteArgs,
) (raft.RequestVoteReply, error) {
	startedAt := time.Now()
	logger := t.getLogger()

	if err := contextError(ctx); err != nil {
		logger.Debug(
			"raft RequestVote cancelled before dispatch",
			"rpc_method", "RequestVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.RequestVoteReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		logger.Debug(
			"raft RequestVote peer lookup failed",
			"rpc_method", "RequestVote",
			"target", target,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.RequestVoteReply{}, err
	}

	reply, err := peer.RequestVote(
		ctx,
		&raftiqv1.RequestVoteRequest{
			Term:         uint64(args.Term),
			CandidateId:  string(args.CandidateID),
			LastLogIndex: uint64(args.LastLogIndex),
			LastLogTerm:  uint64(args.LastLogTerm),
		},
	)
	if err != nil {
		logger.Debug(
			"raft RequestVote RPC failed",
			"rpc_method", "RequestVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.RequestVoteReply{}, fmt.Errorf(
			"request vote from peer %s: %w",
			target,
			err,
		)
	}

	result := "denied"
	if reply.GetVoteGranted() {
		result = "granted"
	}

	logger.Debug(
		"raft RequestVote RPC completed",
		"rpc_method", "RequestVote",
		"target", target,
		"term", args.Term,
		"reply_term", reply.GetTerm(),
		"vote_result", result,
		"duration", time.Since(startedAt),
	)

	return raft.RequestVoteReply{
		Term:        raft.Term(reply.GetTerm()),
		VoterID:     raft.NodeID(reply.GetVoterId()),
		VoteGranted: reply.GetVoteGranted(),
	}, nil
}

func (t *GRPCTransport) AppendEntries(
	ctx context.Context,
	target raft.NodeID,
	args raft.AppendEntriesArgs,
) (raft.AppendEntriesReply, error) {
	startedAt := time.Now()
	logger := t.getLogger()

	if err := contextError(ctx); err != nil {
		logger.Debug(
			"raft AppendEntries cancelled before dispatch",
			"rpc_method", "AppendEntries",
			"target", target,
			"term", args.Term,
			"entry_count", len(args.Entries),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.AppendEntriesReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		logger.Debug(
			"raft AppendEntries peer lookup failed",
			"rpc_method", "AppendEntries",
			"target", target,
			"term", args.Term,
			"entry_count", len(args.Entries),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.AppendEntriesReply{}, err
	}

	entries := make(
		[]*raftiqv1.LogEntry,
		0,
		len(args.Entries),
	)

	for _, entry := range args.Entries {
		entries = append(entries, &raftiqv1.LogEntry{
			Index: uint64(entry.Index),
			Term:  uint64(entry.Term),
			Data:  append([]byte(nil), entry.Data...),
		})
	}

	reply, err := peer.AppendEntries(
		ctx,
		&raftiqv1.AppendEntriesRequest{
			Term:         uint64(args.Term),
			LeaderId:     string(args.LeaderID),
			PrevLogIndex: uint64(args.PrevLogIndex),
			PrevLogTerm:  uint64(args.PrevLogTerm),
			Entries:      entries,
			LeaderCommit: uint64(args.LeaderCommit),
		},
	)
	if err != nil {
		logger.Debug(
			"raft AppendEntries RPC failed",
			"rpc_method", "AppendEntries",
			"target", target,
			"term", args.Term,
			"entry_count", len(args.Entries),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.AppendEntriesReply{}, fmt.Errorf(
			"append entries to peer %s: %w",
			target,
			err,
		)
	}

	if len(args.Entries) > 0 {
		result := "failure"
		if reply.GetSuccess() {
			result = "success"
		}

		logger.Debug(
			"raft AppendEntries RPC completed",
			"rpc_method", "AppendEntries",
			"target", target,
			"term", args.Term,
			"reply_term", reply.GetTerm(),
			"entry_count", len(args.Entries),
			"result", result,
			"duration", time.Since(startedAt),
		)
	}

	return raft.AppendEntriesReply{
		Term:       raft.Term(reply.GetTerm()),
		FollowerID: raft.NodeID(reply.GetFollowerId()),
		Success:    reply.GetSuccess(),
	}, nil
}

func (t *GRPCTransport) InstallSnapshot(
	ctx context.Context,
	target raft.NodeID,
	args raft.InstallSnapshotArgs,
) (raft.InstallSnapshotReply, error) {
	startedAt := time.Now()
	logger := t.getLogger()

	if err := contextError(ctx); err != nil {
		logger.Debug(
			"raft InstallSnapshot cancelled before dispatch",
			"rpc_method", "InstallSnapshot",
			"target", target,
			"term", args.Term,
			"snapshot_index", args.LastIncludedIndex,
			"snapshot_size", len(args.Data),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.InstallSnapshotReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		logger.Debug(
			"raft InstallSnapshot peer lookup failed",
			"rpc_method", "InstallSnapshot",
			"target", target,
			"term", args.Term,
			"snapshot_index", args.LastIncludedIndex,
			"snapshot_size", len(args.Data),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.InstallSnapshotReply{}, err
	}

	reply, err := peer.InstallSnapshot(
		ctx,
		&raftiqv1.InstallSnapshotRequest{
			Term:              uint64(args.Term),
			LeaderId:          string(args.LeaderID),
			LastIncludedIndex: uint64(args.LastIncludedIndex),
			LastIncludedTerm:  uint64(args.LastIncludedTerm),
			Data:              append([]byte(nil), args.Data...),
		},
	)
	if err != nil {
		logger.Debug(
			"raft InstallSnapshot RPC failed",
			"rpc_method", "InstallSnapshot",
			"target", target,
			"term", args.Term,
			"snapshot_index", args.LastIncludedIndex,
			"snapshot_size", len(args.Data),
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.InstallSnapshotReply{}, fmt.Errorf(
			"install snapshot on peer %s: %w",
			target,
			err,
		)
	}

	result := "failure"
	if reply.GetSuccess() {
		result = "success"
	}

	logger.Debug(
		"raft InstallSnapshot RPC completed",
		"rpc_method", "InstallSnapshot",
		"target", target,
		"term", args.Term,
		"reply_term", reply.GetTerm(),
		"snapshot_index", args.LastIncludedIndex,
		"snapshot_size", len(args.Data),
		"result", result,
		"duration", time.Since(startedAt),
	)

	return raft.InstallSnapshotReply{
		Term:       raft.Term(reply.GetTerm()),
		FollowerID: raft.NodeID(reply.GetFollowerId()),
		Success:    reply.GetSuccess(),
	}, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("transport context is nil")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
