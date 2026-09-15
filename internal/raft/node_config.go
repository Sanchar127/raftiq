package raft

import (
    "context"
    "errors"
    "fmt"
    "log/slog"
    "time"

    "github.com/sanchar127/raftiq/internal/model"
)
// SetPeers is retained as a compatibility helper for existing tests.
//
// New production code should use SetTransport so the Raft core depends
// only on the Transport abstraction rather than an in-process Peer.
func (n *RaftNode) SetPeers(peers []Peer) {
	transport := NewLocalTransport()
	peerIDs := make([]NodeID, 0, len(peers))

	for _, peer := range peers {
		if peer == nil {
			continue
		}

		if err := transport.AddPeer(peer); err != nil {
			continue
		}

		peerIDs = append(peerIDs, peer.ID())
	}

	n.mu.Lock()
	n.transport = transport
	n.peerIDs = peerIDs
	n.mu.Unlock()

	n.getLogger().Debug(
		"raft peers configured",
		"peer_count", len(peerIDs),
	)
}

func (n *RaftNode) SetTransport(
	transport Transport,
	peerIDs []NodeID,
) error {
	if transport == nil {
		return errors.New("raft transport is required")
	}

	ids := append([]NodeID(nil), peerIDs...)

	n.mu.Lock()
	n.transport = transport
	n.peerIDs = ids
	n.mu.Unlock()

	n.getLogger().Debug(
		"raft transport configured",
		"peer_count", len(ids),
	)

	return nil
}

func (n *RaftNode) RegisterPeer(peerID NodeID) error {
	if peerID == "" {
		return errors.New("raft peer ID is required")
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if peerID == n.id {
		return fmt.Errorf("cannot register self as peer")
	}

	for _, existingID := range n.peerIDs {
		if existingID == peerID {
			return fmt.Errorf("raft peer %s already registered", peerID)
		}
	}

	n.peerIDs = append(n.peerIDs, peerID)

	return nil
}

func (n *RaftNode) SetRPCTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("raft RPC timeout must be positive")
	}

	n.mu.Lock()
	n.rpcTimeout = timeout
	n.mu.Unlock()

	return nil
}

func (n *RaftNode) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = discardRaftLogger()
	}

	logger = logger.With(
		slog.String("component", "raft"),
		slog.String("node_id", string(n.id)),
	)

	n.mu.Lock()
	n.logger = logger
	n.mu.Unlock()
}

func (n *RaftNode) SetMetrics(metrics Metrics) {
	if metrics == nil {
		metrics = NoopMetrics{}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.metrics = metrics
	n.updateStateMetricsLocked()
}

func (n *RaftNode) SetSnapshotRestore(
	restore func(model.Snapshot) error,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.snapshotRestore = restore
}

func (n *RaftNode) rpcContext() (context.Context, context.CancelFunc) {
	n.mu.RLock()
	timeout := n.rpcTimeout
	n.mu.RUnlock()

	if timeout <= 0 {
		timeout = DefaultRPCTimeout
	}

	return context.WithTimeout(
		context.Background(),
		timeout,
	)
}