package raft

func (n *RaftNode) heartbeatDue() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.Role != Leader {
		return false
	}

	n.heartbeatElapsed++

	if n.heartbeatElapsed < n.heartbeatTimeout {
		return false
	}

	n.heartbeatElapsed = 0
	return true
}

func (n *RaftNode) sendHeartbeats() {
	n.mu.RLock()

	if n.state.Role != Leader {
		n.mu.RUnlock()
		return
	}

	peerIDs := append([]NodeID(nil), n.peerIDs...)

	n.mu.RUnlock()

	for _, peerID := range peerIDs {
		go n.replicateTo(peerID)
	}
}

func (n *RaftNode) sendHeartbeat(peerID NodeID) {
	n.mu.RLock()
	transport := n.transport
	n.mu.RUnlock()

	if transport == nil {
		return
	}

	args, ok := n.buildAppendEntries(peerID)
	if !ok {
		return
	}

	if len(args.Entries) > 0 {
		return
	}

	ctx, cancel := n.rpcContext()

	reply, err := transport.AppendEntries(
		ctx,
		peerID,
		args,
	)

	cancel()

	if err != nil {
		n.getLogger().Debug(
			"heartbeat transport failure",
			"peer_id", peerID,
			"term", args.Term,
			"error", err,
		)

		return
	}

	n.handleAppendEntriesReply(
		peerID,
		args,
		reply,
	)
}

func (n *RaftNode) heartbeat() {
	n.mu.RLock()

	if n.state.Role != Leader {
		n.mu.RUnlock()
		return
	}

	peerIDs := append([]NodeID(nil), n.peerIDs...)

	n.mu.RUnlock()

	for _, peerID := range peerIDs {
		n.replicateTo(peerID)
	}
}