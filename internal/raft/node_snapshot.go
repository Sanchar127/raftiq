package raft

import (
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

func (n *RaftNode) CreateSnapshot(
	index LogIndex,
	data []byte,
) error {
	logger := n.getLogger()

	n.mu.Lock()

	if index > n.state.Volatile.LastApplied {
		err := fmt.Errorf(
			"cannot snapshot unapplied index %d: last applied %d",
			index,
			n.state.Volatile.LastApplied,
		)

		lastApplied := n.state.Volatile.LastApplied

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"last_applied", lastApplied,
			"error", err,
		)

		return err
	}

	if index > n.state.Volatile.CommitIndex {
		err := fmt.Errorf(
			"cannot snapshot uncommitted index %d: commit index %d",
			index,
			n.state.Volatile.CommitIndex,
		)

		commitIndex := n.state.Volatile.CommitIndex

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"commit_index", commitIndex,
			"error", err,
		)

		return err
	}

	if index == 0 {
		err := fmt.Errorf("cannot snapshot index 0")

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"error", err,
		)

		return err
	}

	entry, ok := n.log.Get(index)
	if !ok {
		err := fmt.Errorf(
			"cannot snapshot missing log index %d",
			index,
		)

		n.mu.Unlock()

		logger.Warn(
			"snapshot creation rejected",
			"index", index,
			"error", err,
		)

		return err
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: index,
		LastIncludedTerm:  entry.Term,
		Data:              append([]byte(nil), data...),
	}

	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		wrappedErr := fmt.Errorf(
			"save snapshot: %w",
			err,
		)

		n.mu.Unlock()

		logger.Error(
			"failed to persist snapshot",
			"last_included_index", index,
			"last_included_term", entry.Term,
			"error", wrappedErr,
		)

		return wrappedErr
	}

	if err := n.storage.Sync(); err != nil {
		wrappedErr := fmt.Errorf(
			"sync snapshot: %w",
			err,
		)

		n.mu.Unlock()

		logger.Error(
			"failed to sync snapshot",
			"last_included_index", index,
			"last_included_term", entry.Term,
			"error", wrappedErr,
		)

		return wrappedErr
	}

	if err := n.log.Compact(snapshot); err != nil {
		wrappedErr := fmt.Errorf(
			"compact log after snapshot: %w",
			err,
		)

		n.mu.Unlock()

		logger.Error(
			"failed to compact raft log after snapshot",
			"last_included_index", index,
			"last_included_term", entry.Term,
			"error", wrappedErr,
		)

		return wrappedErr
	}

	n.mu.Unlock()

	logger.Info(
		"raft snapshot created",
		"last_included_index", index,
		"last_included_term", entry.Term,
		"snapshot_size", len(data),
	)

	return nil
}

func (n *RaftNode) InstallSnapshot(
	args InstallSnapshotArgs,
) InstallSnapshotReply {
	logger := n.getLogger()

	n.mu.Lock()

	reply := InstallSnapshotReply{
		Term:       n.state.Persistent.CurrentTerm,
		FollowerID: n.id,
	}

	if args.Term < n.state.Persistent.CurrentTerm {
		n.mu.Unlock()

		logger.Debug(
			"snapshot rejected",
			"leader_id", args.LeaderID,
			"request_term", args.Term,
			"current_term", reply.Term,
			"reason", "stale_term",
		)

		return reply
	}

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Persistent.VotedFor = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			logger.Error(
				"failed to persist term from snapshot",
				"leader_id", args.LeaderID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}
	}

	n.state.Role = Follower
	n.state.LeaderID = args.LeaderID
	n.electionElapsed = 0
	n.updateStateMetricsLocked()

	reply.Term = n.state.Persistent.CurrentTerm

	if args.LastIncludedIndex <= n.log.LastIncludedIndex() {
		reply.Success = true
		n.mu.Unlock()

		logger.Debug(
			"snapshot already installed",
			"leader_id", args.LeaderID,
			"last_included_index", args.LastIncludedIndex,
		)

		return reply
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              append([]byte(nil), args.Data...),
	}

	if err := n.storage.SaveSnapshot(snapshot); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to persist installed snapshot",
			"leader_id", args.LeaderID,
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return reply
	}

	if err := n.storage.Sync(); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to sync installed snapshot",
			"leader_id", args.LeaderID,
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return reply
	}

	restore := n.snapshotRestore

	n.mu.Unlock()

	if restore != nil {
		if err := restore(snapshot); err != nil {
			logger.Error(
				"failed to restore state machine from snapshot",
				"leader_id", args.LeaderID,
				"last_included_index", args.LastIncludedIndex,
				"error", err,
			)

			return reply
		}
	}

	n.mu.Lock()

	if err := n.log.RestoreSnapshot(snapshot); err != nil {
		n.mu.Unlock()

		logger.Error(
			"failed to restore raft log snapshot boundary",
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return reply
	}

	if n.state.Volatile.CommitIndex <
		snapshot.LastIncludedIndex {
		n.state.Volatile.CommitIndex =
			snapshot.LastIncludedIndex
	}

	if n.state.Volatile.LastApplied <
		snapshot.LastIncludedIndex {
		n.state.Volatile.LastApplied =
			snapshot.LastIncludedIndex
	}

	reply.Term = n.state.Persistent.CurrentTerm
	reply.Success = true

	n.mu.Unlock()

	logger.Info(
		"raft snapshot installed",
		"leader_id", args.LeaderID,
		"term", args.Term,
		"last_included_index", args.LastIncludedIndex,
		"last_included_term", args.LastIncludedTerm,
		"snapshot_size", len(args.Data),
	)

	return reply
}

func (n *RaftNode) buildInstallSnapshot(
	peerID NodeID,
) (InstallSnapshotArgs, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.state.Role != Leader {
		return InstallSnapshotArgs{}, false
	}

	nextIndex, ok := n.state.Leader.NextIndex[peerID]
	if !ok {
		return InstallSnapshotArgs{}, false
	}

	snapshot, err := n.storage.LoadSnapshot()
	if err != nil {
		return InstallSnapshotArgs{}, false
	}

	if snapshot.LastIncludedIndex == 0 {
		return InstallSnapshotArgs{}, false
	}

	if nextIndex > snapshot.LastIncludedIndex {
		return InstallSnapshotArgs{}, false
	}

	return InstallSnapshotArgs{
		Term:              n.state.Persistent.CurrentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              append([]byte(nil), snapshot.Data...),
	}, true
}

func (n *RaftNode) handleInstallSnapshotReply(
	peerID NodeID,
	args InstallSnapshotArgs,
	reply InstallSnapshotReply,
) {
	n.mu.Lock()

	if reply.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = reply.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			n.getLogger().Error(
				"failed to persist higher-term follower transition",
				"peer_id", peerID,
				"higher_term", reply.Term,
				"error", err,
			)

			return
		}
		n.updateStateMetricsLocked()
		n.mu.Unlock()

		n.getLogger().Info(
			"raft leader stepped down after higher-term snapshot reply",
			"peer_id", peerID,
			"higher_term", reply.Term,
		)

		return
	}

	if n.state.Role != Leader {
		n.mu.Unlock()
		return
	}

	if args.Term != n.state.Persistent.CurrentTerm {
		n.mu.Unlock()
		return
	}

	if !reply.Success {
		n.mu.Unlock()

		n.getLogger().Debug(
			"snapshot replication rejected",
			"peer_id", peerID,
			"term", args.Term,
			"last_included_index", args.LastIncludedIndex,
		)

		return
	}

	if args.LastIncludedIndex >
		n.state.Leader.MatchIndex[peerID] {
		n.state.Leader.MatchIndex[peerID] =
			args.LastIncludedIndex
	}

	nextIndex := args.LastIncludedIndex + 1

	if nextIndex > n.state.Leader.NextIndex[peerID] {
		n.state.Leader.NextIndex[peerID] = nextIndex
	}

	advanced := n.advanceCommitIndexLocked()

	n.mu.Unlock()

	n.getLogger().Info(
		"raft snapshot replicated",
		"peer_id", peerID,
		"term", args.Term,
		"last_included_index", args.LastIncludedIndex,
	)

	if advanced {
		n.applyCommitted()
	}
}

func (n *RaftNode) sendInstallSnapshot(
	peerID NodeID,
) bool {
	n.mu.RLock()
	transport := n.transport
	n.mu.RUnlock()

	if transport == nil {
		return false
	}

	args, ok := n.buildInstallSnapshot(peerID)
	if !ok {
		return false
	}

	ctx, cancel := n.rpcContext()

	reply, err := transport.InstallSnapshot(
		ctx,
		peerID,
		args,
	)

	cancel()

	if err != nil {
		n.getLogger().Debug(
			"snapshot replication transport failure",
			"peer_id", peerID,
			"term", args.Term,
			"last_included_index", args.LastIncludedIndex,
			"error", err,
		)

		return false
	}

	n.handleInstallSnapshotReply(
		peerID,
		args,
		reply,
	)

	return reply.Success
}