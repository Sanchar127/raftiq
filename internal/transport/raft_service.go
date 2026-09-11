package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
)

const (
	rpcMethodRequestVote     = "RequestVote"
	rpcMethodAppendEntries   = "AppendEntries"
	rpcMethodInstallSnapshot = "InstallSnapshot"
)

type RaftService struct {
	raftiqv1.UnimplementedRaftServiceServer

	node    raftRPC
	metrics RPCMetrics
	logger  *slog.Logger
}

type raftRPC interface {
	RequestVote(args raft.RequestVoteArgs) raft.RequestVoteReply
	AppendEntries(args raft.AppendEntriesArgs) raft.AppendEntriesReply
	InstallSnapshot(args raft.InstallSnapshotArgs) raft.InstallSnapshotReply
}

func NewRaftService(node raftRPC) (*RaftService, error) {
	if node == nil {
		return nil, errors.New("raft node is required")
	}

	return &RaftService{
		node:    node,
		metrics: NoopRPCMetrics{},
		logger:  discardRPCLogger(),
	}, nil
}

func (s *RaftService) SetMetrics(metrics RPCMetrics) {
	if metrics == nil {
		metrics = NoopRPCMetrics{}
	}

	s.metrics = metrics
}

func (s *RaftService) SetLogger(logger *slog.Logger) {
	if logger == nil {
		s.logger = discardRPCLogger()
		return
	}

	s.logger = logger
}

func (s *RaftService) observeRPC(
	method string,
	startedAt time.Time,
	err error,
) {
	s.metrics.IncRPCRequest(method)

	if err != nil {
		s.metrics.IncRPCError(method)
	}

	s.metrics.ObserveRPCDuration(
		method,
		time.Since(startedAt),
	)
}

func (s *RaftService) RequestVote(
	_ context.Context,
	req *raftiqv1.RequestVoteRequest,
) (_ *raftiqv1.RequestVoteResponse, err error) {
	startedAt := time.Now()

	defer func() {
		s.observeRPC(
			rpcMethodRequestVote,
			startedAt,
			err,
		)
	}()

	if req == nil {
		err = errors.New("request vote request is required")

		s.logger.Warn(
			"rejected RequestVote RPC",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodRequestVote),
			slog.Any("error", err),
		)

		return nil, err
	}

	reply := s.node.RequestVote(raft.RequestVoteArgs{
		Term:         raft.Term(req.GetTerm()),
		CandidateID:  raft.NodeID(req.GetCandidateId()),
		LastLogIndex: raft.LogIndex(req.GetLastLogIndex()),
		LastLogTerm:  raft.Term(req.GetLastLogTerm()),
	})

	if !reply.VoteGranted {
		s.logger.Debug(
			"RequestVote denied",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodRequestVote),
			slog.String("candidate_id", req.GetCandidateId()),
			slog.Uint64("request_term", req.GetTerm()),
			slog.Uint64("response_term", uint64(reply.Term)),
		)
	} else {
		s.logger.Debug(
			"RequestVote granted",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodRequestVote),
			slog.String("candidate_id", req.GetCandidateId()),
			slog.Uint64("request_term", req.GetTerm()),
			slog.Uint64("response_term", uint64(reply.Term)),
		)
	}

	return &raftiqv1.RequestVoteResponse{
		Term:        uint64(reply.Term),
		VoterId:     string(reply.VoterID),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *RaftService) AppendEntries(
	_ context.Context,
	req *raftiqv1.AppendEntriesRequest,
) (_ *raftiqv1.AppendEntriesResponse, err error) {
	startedAt := time.Now()

	defer func() {
		s.observeRPC(
			rpcMethodAppendEntries,
			startedAt,
			err,
		)
	}()

	if req == nil {
		err = errors.New("append entries request is required")

		s.logger.Warn(
			"rejected AppendEntries RPC",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodAppendEntries),
			slog.Any("error", err),
		)

		return nil, err
	}

	entries := make([]raft.LogEntry, len(req.GetEntries()))

	for i, entry := range req.GetEntries() {
		if entry == nil {
			err = errors.New("append entries contains nil log entry")

			s.logger.Warn(
				"rejected AppendEntries RPC with nil log entry",
				slog.String("component", "rpc"),
				slog.String("rpc_method", rpcMethodAppendEntries),
				slog.String("leader_id", req.GetLeaderId()),
				slog.Uint64("term", req.GetTerm()),
				slog.Int("entry_position", i),
				slog.Any("error", err),
			)

			return nil, err
		}

		entries[i] = raft.LogEntry{
			Index: raft.LogIndex(entry.GetIndex()),
			Term:  raft.Term(entry.GetTerm()),
			Data:  append([]byte(nil), entry.GetData()...),
		}
	}

	reply := s.node.AppendEntries(raft.AppendEntriesArgs{
		Term:         raft.Term(req.GetTerm()),
		LeaderID:     raft.NodeID(req.GetLeaderId()),
		PrevLogIndex: raft.LogIndex(req.GetPrevLogIndex()),
		PrevLogTerm:  raft.Term(req.GetPrevLogTerm()),
		Entries:      entries,
		LeaderCommit: raft.LogIndex(req.GetLeaderCommit()),
	})

	if !reply.Success {
		s.logger.Debug(
			"AppendEntries rejected",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodAppendEntries),
			slog.String("leader_id", req.GetLeaderId()),
			slog.Uint64("request_term", req.GetTerm()),
			slog.Uint64("response_term", uint64(reply.Term)),
			slog.Int("entries", len(entries)),
			slog.Uint64("prev_log_index", req.GetPrevLogIndex()),
			slog.Uint64("leader_commit", req.GetLeaderCommit()),
		)
	} else if len(entries) > 0 {
		s.logger.Debug(
			"AppendEntries accepted",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodAppendEntries),
			slog.String("leader_id", req.GetLeaderId()),
			slog.Uint64("request_term", req.GetTerm()),
			slog.Int("entries", len(entries)),
			slog.Uint64("leader_commit", req.GetLeaderCommit()),
		)
	}

	return &raftiqv1.AppendEntriesResponse{
		Term:       uint64(reply.Term),
		FollowerId: string(reply.FollowerID),
		Success:    reply.Success,
	}, nil
}

func (s *RaftService) InstallSnapshot(
	_ context.Context,
	req *raftiqv1.InstallSnapshotRequest,
) (_ *raftiqv1.InstallSnapshotResponse, err error) {
	startedAt := time.Now()

	defer func() {
		s.observeRPC(
			rpcMethodInstallSnapshot,
			startedAt,
			err,
		)
	}()

	if req == nil {
		err = errors.New("install snapshot request is required")

		s.logger.Warn(
			"rejected InstallSnapshot RPC",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodInstallSnapshot),
			slog.Any("error", err),
		)

		return nil, err
	}

	reply := s.node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              raft.Term(req.GetTerm()),
		LeaderID:          raft.NodeID(req.GetLeaderId()),
		LastIncludedIndex: raft.LogIndex(req.GetLastIncludedIndex()),
		LastIncludedTerm:  raft.Term(req.GetLastIncludedTerm()),
		Data:              append([]byte(nil), req.GetData()...),
	})

	if !reply.Success {
		s.logger.Warn(
			"InstallSnapshot rejected",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodInstallSnapshot),
			slog.String("leader_id", req.GetLeaderId()),
			slog.Uint64("request_term", req.GetTerm()),
			slog.Uint64("response_term", uint64(reply.Term)),
			slog.Uint64(
				"last_included_index",
				req.GetLastIncludedIndex(),
			),
		)
	} else {
		s.logger.Info(
			"snapshot installed through RPC",
			slog.String("component", "rpc"),
			slog.String("rpc_method", rpcMethodInstallSnapshot),
			slog.String("leader_id", req.GetLeaderId()),
			slog.Uint64("term", req.GetTerm()),
			slog.Uint64(
				"last_included_index",
				req.GetLastIncludedIndex(),
			),
		)
	}

	return &raftiqv1.InstallSnapshotResponse{
		Term:       uint64(reply.Term),
		FollowerId: string(reply.FollowerID),
		Success:    reply.Success,
	}, nil
}

func discardRPCLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}
