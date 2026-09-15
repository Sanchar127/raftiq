package transport

import (
	"context"
	"fmt"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
)

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
