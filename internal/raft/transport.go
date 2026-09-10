package raft

import "context"

type Transport interface {
	RequestVote(
		ctx context.Context,
		target NodeID,
		args RequestVoteArgs,
	) (RequestVoteReply, error)

	AppendEntries(
		ctx context.Context,
		target NodeID,
		args AppendEntriesArgs,
	) (AppendEntriesReply, error)

	InstallSnapshot(
		ctx context.Context,
		target NodeID,
		args InstallSnapshotArgs,
	) (InstallSnapshotReply, error)
}
