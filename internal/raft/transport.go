package raft

import "context"

type Transport interface {
	RequestVote(
		ctx context.Context,
		target NodeID,
		args RequestVoteArgs,
	) (RequestVoteReply, error)

	PreVote(
		ctx context.Context,
		target NodeID,
		args PreVoteArgs,
	) (PreVoteReply, error)

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
