package raft

import (
    "errors"
    "fmt"
    "time"

    "github.com/sanchar127/raftiq/internal/model"
    "github.com/sanchar127/raftiq/internal/storage"
)

func (n *RaftNode) Propose(data []byte) (LogIndex, error) {
	n.mu.Lock()

	if n.state.Role != Leader {
		role := n.state.Role
		term := n.state.Persistent.CurrentTerm
		nodeID := n.id

		n.mu.Unlock()

		err := fmt.Errorf(
			"node %s is not the leader",
			nodeID,
		)

		n.getLogger().Debug(
			"raft proposal rejected",
			"role", role,
			"term", term,
			"error", err,
		)

		return 0, err
	}

	index := n.log.LastIndex() + 1

	entry := LogEntry{
		Index: index,
		Term:  n.state.Persistent.CurrentTerm,
		Data:  append([]byte(nil), data...),
	}

	if err := n.storage.AppendEntries([]LogEntry{entry}); err != nil {
		n.mu.Unlock()

		n.getLogger().Error(
			"failed to persist proposed raft entry",
			"index", index,
			"term", entry.Term,
			"error", err,
		)

		return 0, fmt.Errorf(
			"persist proposed entry: %w",
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		if errors.Is(err, storage.ErrWALDiskFull) {
			n.stepDownForStorageFailureLocked()

			n.mu.Unlock()

			n.getLogger().Error(
				"raft leader stepped down due to WAL disk full",
				"index", index,
				"term", entry.Term,
				"error", err,
			)

			return 0, fmt.Errorf(
				"storage disk full: %w",
				err,
			)
		}

		n.mu.Unlock()

		n.getLogger().Error(
			"failed to sync proposed raft entry",
			"index", index,
			"term", entry.Term,
			"error", err,
		)

		return 0, fmt.Errorf(
			"sync proposed entry: %w",
			err,
		)
	}

	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()

		n.getLogger().Error(
			"failed to append proposed entry to raft log",
			"index", index,
			"term", entry.Term,
			"error", err,
		)

		return 0, fmt.Errorf(
			"append proposed entry to raft log: %w",
			err,
		)
	}

	advanced := n.advanceCommitIndexLocked()

	transport := n.transport
	peerIDs := append([]NodeID(nil), n.peerIDs...)
	term := n.state.Persistent.CurrentTerm

	n.mu.Unlock()

	n.getLogger().Debug(
		"raft proposal accepted",
		"index", index,
		"term", term,
		"data_size", len(data),
		"peer_count", len(peerIDs),
		"commit_advanced", advanced,
	)

	if advanced {
		n.applyCommitted()
	}

	if transport == nil {
		return index, nil
	}

	for _, peerID := range peerIDs {
		n.replicateTo(peerID)
	}

	return index, nil
}

func (n *RaftNode) ProposeConfiguration(
	configuration model.Configuration,
) (LogIndex, error) {
	data, err := EncodeConfigurationEntry(configuration)
	if err != nil {
		return 0, fmt.Errorf(
			"encode configuration proposal: %w",
			err,
		)
	}

	return n.Propose(data)
}

func (n *RaftNode) buildAppendEntries(
	peerID NodeID,
) (AppendEntriesArgs, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.state.Role != Leader {
		return AppendEntriesArgs{}, false
	}

	nextIndex, ok := n.state.Leader.NextIndex[peerID]
	if !ok {
		return AppendEntriesArgs{}, false
	}

	args := AppendEntriesArgs{
		Term:         n.state.Persistent.CurrentTerm,
		LeaderID:     n.id,
		LeaderCommit: n.state.Volatile.CommitIndex,
	}

	if nextIndex > 1 {
		prevIndex := nextIndex - 1

		prevEntry, ok := n.log.Get(prevIndex)
		if !ok {
			return AppendEntriesArgs{}, false
		}

		args.PrevLogIndex = prevIndex
		args.PrevLogTerm = prevEntry.Term
	}

	for index := nextIndex; index <= n.log.LastIndex(); index++ {
		entry, ok := n.log.Get(index)
		if !ok {
			return AppendEntriesArgs{}, false
		}

		entry.Data = append([]byte(nil), entry.Data...)
		args.Entries = append(args.Entries, entry)
	}

	return args, true
}

func (n *RaftNode) replicateTo(peerID NodeID) {
	for {
		n.mu.RLock()
		transport := n.transport
		n.mu.RUnlock()

		if transport == nil {
			return
		}

		if n.sendInstallSnapshot(peerID) {
			continue
		}

		args, ok := n.buildAppendEntries(peerID)
		if !ok {
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
				"append entries transport failure",
				"peer_id", peerID,
				"term", args.Term,
				"entry_count", len(args.Entries),
				"error", err,
			)

			return
		}

		n.handleAppendEntriesReply(
			peerID,
			args,
			reply,
		)

		if reply.Success {
			if len(args.Entries) > 0 {
				n.getLogger().Debug(
					"raft entries replicated",
					"peer_id", peerID,
					"term", args.Term,
					"entry_count", len(args.Entries),
					"last_entry_index", args.Entries[len(args.Entries)-1].Index,
				)
			}

			return
		}

		n.mu.RLock()
		role := n.state.Role
		n.mu.RUnlock()

		if role != Leader {
			return
		}
	}
}

func (n *RaftNode) AppendEntries(
	args AppendEntriesArgs,
) (reply AppendEntriesReply) {
	startedAt := time.Now()

	n.mu.Lock()

	defer func() {
		result := "failure"
		if reply.Success {
			result = "success"
		}

		n.metrics.IncAppendEntries(args.LeaderID, result)

		if !reply.Success {
			n.metrics.IncAppendEntriesFailures(args.LeaderID)
		}

		n.metrics.ObserveAppendEntriesDuration(
			args.LeaderID,
			time.Since(startedAt),
		)
	}()

	reply = AppendEntriesReply{
		Term:       n.state.Persistent.CurrentTerm,
		FollowerID: n.id,
	}

	if args.Term < n.state.Persistent.CurrentTerm {
		n.mu.Unlock()

		n.getLogger().Debug(
			"append entries rejected",
			"leader_id", args.LeaderID,
			"request_term", args.Term,
			"current_term", reply.Term,
			"entry_count", len(args.Entries),
			"reason", "stale_term",
		)

		return reply
	}

	if args.Term > n.state.Persistent.CurrentTerm {
		n.state.Persistent.CurrentTerm = args.Term
		n.state.Role = Follower
		n.state.Persistent.VotedFor = ""
		n.state.LeaderID = ""

		if err := n.persistStateLocked(); err != nil {
			n.mu.Unlock()

			n.getLogger().Error(
				"failed to persist higher-term append entries state",
				"leader_id", args.LeaderID,
				"term", args.Term,
				"error", err,
			)

			return reply
		}
	}

	if args.PrevLogIndex > 0 {
		prevEntry, ok := n.log.Get(args.PrevLogIndex)
		if !ok || prevEntry.Term != args.PrevLogTerm {
			n.mu.Unlock()

			n.getLogger().Debug(
				"append entries rejected",
				"leader_id", args.LeaderID,
				"request_term", args.Term,
				"entry_count", len(args.Entries),
				"reason", "log_mismatch",
				"prev_log_index", args.PrevLogIndex,
				"prev_log_term", args.PrevLogTerm,
			)

			return reply
		}
	}

	n.state.Role = Follower
	n.state.LeaderID = args.LeaderID
	n.electionElapsed = 0

	// Synchronize runtime Raft state with observability.
	n.updateStateMetricsLocked()

	firstNew := -1
	replaceFrom := model.LogIndex(0)

	for i, entry := range args.Entries {
		existing, ok := n.log.Get(entry.Index)
		if !ok {
			firstNew = i
			break
		}

		if existing.Term != entry.Term {
			firstNew = i
			replaceFrom = entry.Index
			break
		}
	}

	if firstNew >= 0 {
		newEntries := cloneEntries(args.Entries[firstNew:])

		if replaceFrom > 0 {
			if err := n.storage.ReplaceSuffix(
				replaceFrom,
				newEntries,
			); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to replace raft log suffix",
					"leader_id", args.LeaderID,
					"replace_from", replaceFrom,
					"entry_count", len(newEntries),
					"error", err,
				)

				return reply
			}

			if err := n.storage.Sync(); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to sync replaced raft log suffix",
					"leader_id", args.LeaderID,
					"replace_from", replaceFrom,
					"error", err,
				)

				return reply
			}

			n.log.TruncateFrom(replaceFrom)

			for _, entry := range newEntries {
				if err := n.log.Append(entry); err != nil {
					n.mu.Unlock()

					n.getLogger().Error(
						"failed to append replicated raft entry",
						"leader_id", args.LeaderID,
						"index", entry.Index,
						"error", err,
					)

					return reply
				}
			}
		} else {
			if err := n.storage.AppendEntries(newEntries); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to persist replicated raft entries",
					"leader_id", args.LeaderID,
					"entry_count", len(newEntries),
					"error", err,
				)

				return reply
			}

			if err := n.storage.Sync(); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to sync replicated raft entries",
					"leader_id", args.LeaderID,
					"entry_count", len(newEntries),
					"error", err,
				)

				return reply
			}

			for _, entry := range newEntries {
				if err := n.log.Append(entry); err != nil {
					n.mu.Unlock()

					n.getLogger().Error(
						"failed to append replicated raft entry",
						"leader_id", args.LeaderID,
						"index", entry.Index,
						"error", err,
					)

					return reply
				}
			}
		}
	}

	commitAdvanced := false

	if args.LeaderCommit > n.state.Volatile.CommitIndex {
		lastIndex := n.log.LastIndex()
		oldCommitIndex := n.state.Volatile.CommitIndex

		if args.LeaderCommit < lastIndex {
			n.state.Volatile.CommitIndex = args.LeaderCommit
		} else {
			n.state.Volatile.CommitIndex = lastIndex
		}

		commitAdvanced =
			n.state.Volatile.CommitIndex > oldCommitIndex
	}

	reply.Term = n.state.Persistent.CurrentTerm
	reply.Success = true

	entryCount := len(args.Entries)
	commitIndex := n.state.Volatile.CommitIndex

	n.mu.Unlock()

	if entryCount > 0 {
		n.getLogger().Debug(
			"append entries accepted",
			"leader_id", args.LeaderID,
			"term", reply.Term,
			"entry_count", entryCount,
			"commit_index", commitIndex,
			"commit_advanced", commitAdvanced,
		)
	}

	if commitAdvanced {
		n.applyCommitted()
	}

	return reply
}

func (n *RaftNode) handleAppendEntriesReply(
	peerID NodeID,
	args AppendEntriesArgs,
	reply AppendEntriesReply,
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
			"raft leader stepped down after higher term",
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
		if nextIndex := n.state.Leader.NextIndex[peerID]; nextIndex > 1 {
			n.state.Leader.NextIndex[peerID]--
		}

		nextIndex := n.state.Leader.NextIndex[peerID]

		n.mu.Unlock()

		n.getLogger().Debug(
			"raft log replication rejected",
			"peer_id", peerID,
			"term", args.Term,
			"next_index", nextIndex,
		)

		return
	}

	if len(args.Entries) == 0 {
		n.mu.Unlock()
		return
	}

	lastReplicated :=
		args.Entries[len(args.Entries)-1].Index

	if lastReplicated > n.state.Leader.MatchIndex[peerID] {
		n.state.Leader.MatchIndex[peerID] = lastReplicated
		n.state.Leader.NextIndex[peerID] = lastReplicated + 1
	}

	advanced := n.advanceCommitIndexLocked()
	commitIndex := n.state.Volatile.CommitIndex

	n.mu.Unlock()

	if advanced {
		n.getLogger().Debug(
			"raft commit index advanced",
			"commit_index", commitIndex,
			"peer_id", peerID,
		)

		n.applyCommitted()
	}
}

func (n *RaftNode) advanceCommitIndexLocked() bool {
	if n.state.Role != Leader {
		return false
	}

	oldCommitIndex := n.state.Volatile.CommitIndex
	membership := n.state.Persistent.Membership

	for index := n.state.Volatile.CommitIndex + 1; index <= n.log.LastIndex(); index++ {
		if n.logTerm(index) != n.state.Persistent.CurrentTerm {
			continue
		}

		replicated := map[NodeID]struct{}{
			n.id: {},
		}

		for _, peerID := range n.peerIDs {
			if n.state.Leader.MatchIndex[peerID] >= index {
				replicated[peerID] = struct{}{}
			}
		}

		if membershipHasQuorum(membership, replicated) {
			n.state.Volatile.CommitIndex = index
		}
	}

	return n.state.Volatile.CommitIndex > oldCommitIndex
}

func (n *RaftNode) logTerm(index LogIndex) Term {
	entry, ok := n.log.Get(index)
	if !ok {
		return 0
	}

	return entry.Term
}

func (n *RaftNode) advanceCommitIndex() {
	n.mu.Lock()

	advanced := n.advanceCommitIndexLocked()

	n.mu.Unlock()

	if advanced {
		n.getLogger().Debug(
			"raft commit index advanced",
			"commit_index", n.State().Volatile.CommitIndex,
		)

		n.applyCommitted()
	}
}