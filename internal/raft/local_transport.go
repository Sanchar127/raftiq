package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrTransportClosed = errors.New("raft transport is closed")
	ErrPeerNotFound    = errors.New("raft peer not found")
)

type LocalTransport struct {
	mu     sync.RWMutex
	peers  map[NodeID]Peer
	closed bool
}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		peers: make(map[NodeID]Peer),
	}
}

func (t *LocalTransport) AddPeer(peer Peer) error {
	if peer == nil {
		return errors.New("raft peer is required")
	}

	peerID := peer.ID()

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTransportClosed
	}

	if _, exists := t.peers[peerID]; exists {
		return fmt.Errorf("raft peer %s already registered", peerID)
	}

	t.peers[peerID] = peer

	return nil
}

func (t *LocalTransport) AddNode(node *RaftNode) error {
	if node == nil {
		return errors.New("raft node is required")
	}

	return t.AddPeer(node)
}

func (t *LocalTransport) RemoveNode(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peers, id)
}

func (t *LocalTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	t.peers = make(map[NodeID]Peer)
}

func (t *LocalTransport) peer(target NodeID) (Peer, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, ErrTransportClosed
	}

	peer, ok := t.peers[target]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrPeerNotFound,
			target,
		)
	}

	return peer, nil
}

func (t *LocalTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	if err := contextError(ctx); err != nil {
		return RequestVoteReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		return RequestVoteReply{}, err
	}

	replyCh := make(chan RequestVoteReply, 1)

	go func() {
		replyCh <- peer.RequestVote(args)
	}()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	}
}

func (t *LocalTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	if err := contextError(ctx); err != nil {
		return AppendEntriesReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		return AppendEntriesReply{}, err
	}

	replyCh := make(chan AppendEntriesReply, 1)

	go func() {
		replyCh <- peer.AppendEntries(args)
	}()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	}
}

func (t *LocalTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	if err := contextError(ctx); err != nil {
		return InstallSnapshotReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		return InstallSnapshotReply{}, err
	}

	replyCh := make(chan InstallSnapshotReply, 1)

	go func() {
		replyCh <- peer.InstallSnapshot(args)
	}()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		return InstallSnapshotReply{}, ctx.Err()
	}
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
