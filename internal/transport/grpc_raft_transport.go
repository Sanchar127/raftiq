package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{
		peers: make(map[raft.NodeID]raftiqv1.RaftServiceClient),
		conns: make(map[raft.NodeID]*grpc.ClientConn),
	}
}

func (t *GRPCTransport) AddPeer(
	id raft.NodeID,
	address string,
	opts ...grpc.DialOption,
) error {
	if id == "" {
		return errors.New("raft peer ID is required")
	}

	if address == "" {
		return errors.New("raft peer address is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrGRPCTransportClosed
	}

	if _, exists := t.peers[id]; exists {
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
		return fmt.Errorf(
			"create gRPC connection to peer %s: %w",
			id,
			err,
		)
	}

	t.conns[id] = conn
	t.peers[id] = raftiqv1.NewRaftServiceClient(conn)

	return nil
}

func (t *GRPCTransport) RemovePeer(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn, exists := t.conns[id]
	if exists {
		_ = conn.Close()
	}

	delete(t.conns, id)
	delete(t.peers, id)
}

func (t *GRPCTransport) Close() error {
	t.mu.Lock()

	if t.closed {
		t.mu.Unlock()
		return nil
	}

	t.closed = true

	conns := make([]*grpc.ClientConn, 0, len(t.conns))

	for _, conn := range t.conns {
		conns = append(conns, conn)
	}

	t.peers = make(map[raft.NodeID]raftiqv1.RaftServiceClient)
	t.conns = make(map[raft.NodeID]*grpc.ClientConn)

	t.mu.Unlock()

	var closeErr error

	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}

	return closeErr
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
	if err := contextError(ctx); err != nil {
		return raft.RequestVoteReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
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
		return raft.RequestVoteReply{}, fmt.Errorf(
			"request vote from peer %s: %w",
			target,
			err,
		)
	}

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
	if err := contextError(ctx); err != nil {
		return raft.AppendEntriesReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
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
		return raft.AppendEntriesReply{}, fmt.Errorf(
			"append entries to peer %s: %w",
			target,
			err,
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
	if err := contextError(ctx); err != nil {
		return raft.InstallSnapshotReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
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
		return raft.InstallSnapshotReply{}, fmt.Errorf(
			"install snapshot on peer %s: %w",
			target,
			err,
		)
	}

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
