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
	nodes  map[NodeID]*RaftNode
	closed bool
}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		nodes: make(map[NodeID]*RaftNode),
	}
}

func (t *LocalTransport) AddNode(node *RaftNode) error {
	if node == nil {
		return errors.New("raft node is required")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTransportClosed
	}

	nodeID := node.ID()

	if _, exists := t.nodes[nodeID]; exists {
		return fmt.Errorf("raft node %s already registered", nodeID)
	}

	t.nodes[nodeID] = node

	return nil
}

func (t *LocalTransport) RemoveNode(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.nodes, id)
}

func (t *LocalTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	t.nodes = make(map[NodeID]*RaftNode)
}

func (t *LocalTransport) node(
	target NodeID,
) (*RaftNode, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, ErrTransportClosed
	}

	node, ok := t.nodes[target]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrPeerNotFound,
			target,
		)
	}

	return node, nil
}

func (t *LocalTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	if err := contextError(ctx); err != nil {
		return RequestVoteReply{}, err
	}

	node, err := t.node(target)
	if err != nil {
		return RequestVoteReply{}, err
	}

	replyCh := make(chan RequestVoteReply, 1)

	go func() {
		replyCh <- node.RequestVote(args)
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

	node, err := t.node(target)
	if err != nil {
		return AppendEntriesReply{}, err
	}

	replyCh := make(chan AppendEntriesReply, 1)

	go func() {
		replyCh <- node.AppendEntries(args)
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

	node, err := t.node(target)
	if err != nil {
		return InstallSnapshotReply{}, err
	}

	replyCh := make(chan InstallSnapshotReply, 1)

	go func() {
		replyCh <- node.InstallSnapshot(args)
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
