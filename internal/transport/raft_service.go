package transport

import (
	"context"
	"errors"
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
	}, nil
}

func (s *RaftService) SetMetrics(metrics RPCMetrics) {
	if metrics == nil {
		metrics = NoopRPCMetrics{}
	}

	s.metrics = metrics
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
		return nil, errors.New("request vote request is required")
	}

	reply := s.node.RequestVote(raft.RequestVoteArgs{
		Term:         raft.Term(req.GetTerm()),
		CandidateID:  raft.NodeID(req.GetCandidateId()),
		LastLogIndex: raft.LogIndex(req.GetLastLogIndex()),
		LastLogTerm:  raft.Term(req.GetLastLogTerm()),
	})

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
		return nil, errors.New("append entries request is required")
	}

	entries := make([]raft.LogEntry, len(req.GetEntries()))

	for i, entry := range req.GetEntries() {
		if entry == nil {
			return nil, errors.New("append entries contains nil log entry")
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
		return nil, errors.New("install snapshot request is required")
	}

	reply := s.node.InstallSnapshot(raft.InstallSnapshotArgs{
		Term:              raft.Term(req.GetTerm()),
		LeaderID:          raft.NodeID(req.GetLeaderId()),
		LastIncludedIndex: raft.LogIndex(req.GetLastIncludedIndex()),
		LastIncludedTerm:  raft.Term(req.GetLastIncludedTerm()),
		Data:              append([]byte(nil), req.GetData()...),
	})

	return &raftiqv1.InstallSnapshotResponse{
		Term:       uint64(reply.Term),
		FollowerId: string(reply.FollowerID),
		Success:    reply.Success,
	}, nil
}
