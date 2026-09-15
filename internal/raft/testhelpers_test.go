package raft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
)

type failingStateStorage struct {
	*storage.MemoryStorage

	saveStateErr error
	syncErr      error
}

func (s *failingStateStorage) SaveState(
	state model.PersistentState,
) error {
	if s.saveStateErr != nil {
		return s.saveStateErr
	}

	return s.MemoryStorage.SaveState(state)
}

func (s *failingStateStorage) Sync() error {
	if s.syncErr != nil {
		return s.syncErr
	}

	return s.MemoryStorage.Sync()
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

type blockingTransport struct {
	requestVoteStarted   chan struct{}
	appendEntriesStarted chan struct{}
}

type grantingTransport struct{}

func (t *grantingTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	return RequestVoteReply{
		Term:        args.Term,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *grantingTransport) PreVote(
	ctx context.Context,
	target NodeID,
	args PreVoteArgs,
) (PreVoteReply, error) {
	return PreVoteReply{
		Term:        args.Term,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *grantingTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}

func (t *grantingTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, nil
}

type electionTransport struct {
	peers []NodeID
}

func (t *electionTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	return RequestVoteReply{
		Term:        args.Term,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *electionTransport) PreVote(
	ctx context.Context,
	target NodeID,
	args PreVoteArgs,
) (PreVoteReply, error) {
	return PreVoteReply{
		Term:        args.Term - 1,
		VoterID:     target,
		VoteGranted: true,
	}, nil
}

func (t *electionTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	return AppendEntriesReply{}, nil
}

func (t *electionTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	return InstallSnapshotReply{}, nil
}

func (t *blockingTransport) RequestVote(
	ctx context.Context,
	_ NodeID,
	_ RequestVoteArgs,
) (RequestVoteReply, error) {
	if t.requestVoteStarted != nil {
		select {
		case <-t.requestVoteStarted:
		default:
			close(t.requestVoteStarted)
		}
	}

	<-ctx.Done()

	return RequestVoteReply{}, ctx.Err()
}

func (t *blockingTransport) PreVote(
	ctx context.Context,
	_ NodeID,
	_ PreVoteArgs,
) (PreVoteReply, error) {
	<-ctx.Done()

	return PreVoteReply{}, ctx.Err()
}

func (t *blockingTransport) AppendEntries(
	ctx context.Context,
	_ NodeID,
	_ AppendEntriesArgs,
) (AppendEntriesReply, error) {
	if t.appendEntriesStarted != nil {
		select {
		case <-t.appendEntriesStarted:
		default:
			close(t.appendEntriesStarted)
		}
	}

	<-ctx.Done()

	return AppendEntriesReply{}, ctx.Err()
}

func (t *blockingTransport) InstallSnapshot(
	ctx context.Context,
	_ NodeID,
	_ InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	<-ctx.Done()

	return InstallSnapshotReply{}, ctx.Err()
}

type diskFullStorage struct {
	diskFull bool
}

func (s *diskFullStorage) SaveState(model.PersistentState) error {
	return nil
}

func (s *diskFullStorage) LoadState() (model.PersistentState, error) {
	return model.PersistentState{}, nil
}

func (s *diskFullStorage) AppendEntries([]model.LogEntry) error {
	return nil
}

func (s *diskFullStorage) ReplaceSuffix(
	model.LogIndex,
	[]model.LogEntry,
) error {
	return nil
}

func (s *diskFullStorage) LoadEntries() ([]model.LogEntry, error) {
	return nil, nil
}

func (s *diskFullStorage) SaveSnapshot(model.Snapshot) error {
	return nil
}

func (s *diskFullStorage) LoadSnapshot() (model.Snapshot, error) {
	return model.Snapshot{}, nil
}

func (s *diskFullStorage) Sync() error {
	if s.diskFull {
		return storage.ErrWALDiskFull
	}

	return nil
}

func (s *diskFullStorage) Close() error {
	return nil
}

type removeMemberFailureTransport struct {
	*LocalTransport
}

func (t *removeMemberFailureTransport) AppendEntries(
	ctx context.Context,
	target NodeID,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	// Once RemoveMember proposes LeaveJoint, make C and D
	// unreachable for that specific configuration entry.
	//
	// EnterJoint is allowed through normally, so it can commit.
	if target == "C" || target == "D" {
		for _, entry := range args.Entries {
			if IsConfigurationEntry(entry.Data) &&
				len(entry.Data) >= 6 &&
				entry.Data[5] == configurationEntryTypeLeaveJoint {
				return AppendEntriesReply{}, fmt.Errorf(
					"simulated LeaveJoint failure to peer %s",
					target,
				)
			}
		}
	}

	return t.LocalTransport.AppendEntries(
		ctx,
		target,
		args,
	)
}

func (t *removeMemberFailureTransport) RequestVote(
	ctx context.Context,
	target NodeID,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	return t.LocalTransport.RequestVote(ctx, target, args)
}

func (t *removeMemberFailureTransport) PreVote(
	ctx context.Context,
	target NodeID,
	args PreVoteArgs,
) (PreVoteReply, error) {
	return t.LocalTransport.PreVote(ctx, target, args)
}

func (t *removeMemberFailureTransport) InstallSnapshot(
	ctx context.Context,
	target NodeID,
	args InstallSnapshotArgs,
) (InstallSnapshotReply, error) {
	return t.LocalTransport.InstallSnapshot(ctx, target, args)
}
