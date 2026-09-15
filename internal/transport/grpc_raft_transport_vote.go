package transport

import (
	"context"
	"fmt"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
)

func (t *GRPCTransport) RequestVote(
	ctx context.Context,
	target raft.NodeID,
	args raft.RequestVoteArgs,
) (raft.RequestVoteReply, error) {
	startedAt := time.Now()
	logger := t.getLogger()

	if err := contextError(ctx); err != nil {
		logger.Debug(
			"raft RequestVote cancelled before dispatch",
			"rpc_method", "RequestVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.RequestVoteReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		logger.Debug(
			"raft RequestVote peer lookup failed",
			"rpc_method", "RequestVote",
			"target", target,
			"error", err,
			"duration", time.Since(startedAt),
		)

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
		logger.Debug(
			"raft RequestVote RPC failed",
			"rpc_method", "RequestVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.RequestVoteReply{}, fmt.Errorf(
			"request vote from peer %s: %w",
			target,
			err,
		)
	}

	result := "denied"
	if reply.GetVoteGranted() {
		result = "granted"
	}

	logger.Debug(
		"raft RequestVote RPC completed",
		"rpc_method", "RequestVote",
		"target", target,
		"term", args.Term,
		"reply_term", reply.GetTerm(),
		"vote_result", result,
		"duration", time.Since(startedAt),
	)

	return raft.RequestVoteReply{
		Term:        raft.Term(reply.GetTerm()),
		VoterID:     raft.NodeID(reply.GetVoterId()),
		VoteGranted: reply.GetVoteGranted(),
	}, nil
}

func (t *GRPCTransport) PreVote(
	ctx context.Context,
	target raft.NodeID,
	args raft.PreVoteArgs,
) (raft.PreVoteReply, error) {
	startedAt := time.Now()
	logger := t.getLogger()

	if err := contextError(ctx); err != nil {
		logger.Debug(
			"raft PreVote cancelled before dispatch",
			"rpc_method", "PreVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.PreVoteReply{}, err
	}

	peer, err := t.peer(target)
	if err != nil {
		logger.Debug(
			"raft PreVote peer lookup failed",
			"rpc_method", "PreVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.PreVoteReply{}, err
	}

	reply, err := peer.PreVote(
		ctx,
		&raftiqv1.PreVoteRequest{
			Term:         uint64(args.Term),
			CandidateId:  string(args.CandidateID),
			LastLogIndex: uint64(args.LastLogIndex),
			LastLogTerm:  uint64(args.LastLogTerm),
		},
	)
	if err != nil {
		logger.Debug(
			"raft PreVote RPC failed",
			"rpc_method", "PreVote",
			"target", target,
			"term", args.Term,
			"error", err,
			"duration", time.Since(startedAt),
		)

		return raft.PreVoteReply{}, fmt.Errorf(
			"pre-vote from peer %s: %w",
			target,
			err,
		)
	}

	result := "denied"
	if reply.GetVoteGranted() {
		result = "granted"
	}

	logger.Debug(
		"raft PreVote RPC completed",
		"rpc_method", "PreVote",
		"target", target,
		"term", args.Term,
		"reply_term", reply.GetTerm(),
		"vote_result", result,
		"duration", time.Since(startedAt),
	)

	return raft.PreVoteReply{
		Term:        raft.Term(reply.GetTerm()),
		VoterID:     raft.NodeID(reply.GetVoterId()),
		VoteGranted: reply.GetVoteGranted(),
	}, nil
}
