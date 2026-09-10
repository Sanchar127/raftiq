package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestApplierClaimJobReturnsJob(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	err := store.CreateJob(model.Job{
		ID:      "job-1",
		Payload: []byte("payload"),
		State:   model.JobPending,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	command, err := EncodeCommand(Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-1",
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applyCh := make(chan raft.LogEntry, 1)

	done := make(chan error, 1)

	go func() {
		done <- applier.Run(ctx, applyCh)
	}()

	applyCh <- raft.LogEntry{
		Index: 42,
		Term:  3,
		Data:  command,
	}

	resultCtx, resultCancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer resultCancel()

	result, err := applier.WaitResult(resultCtx, 42)
	if err != nil {
		t.Fatalf("WaitResult() error = %v", err)
	}

	if result.Err != nil {
		t.Fatalf("claim result error = %v", result.Err)
	}

	if result.Job == nil {
		t.Fatal("expected claimed job in ApplyResult")
	}

	if result.Job.ID != "job-1" {
		t.Fatalf("job ID = %q, want %q", result.Job.ID, "job-1")
	}

	if result.Job.State != model.JobScheduled {
		t.Fatalf(
			"job state = %q, want %q",
			result.Job.State,
			model.JobScheduled,
		)
	}

	if result.Job.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"worker ID = %q, want %q",
			result.Job.AssignedWorkerID,
			"worker-1",
		)
	}

	if result.Job.FencingToken != 1 {
		t.Fatalf(
			"fencing token = %d, want 1",
			result.Job.FencingToken,
		)
	}

	if result.Job.Attempt != 1 {
		t.Fatalf(
			"attempt = %d, want 1",
			result.Job.Attempt,
		)
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}

	case <-time.After(time.Second):
		t.Fatal("Applier.Run() did not stop")
	}
}

func TestApplierClaimJobConflictDoesNotStopApplier(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	err := store.CreateJob(model.Job{
		ID:      "job-1",
		Payload: []byte("payload"),
		State:   model.JobPending,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	firstCommand, err := EncodeCommand(Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-1",
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("EncodeCommand(first) error = %v", err)
	}

	secondCommand, err := EncodeCommand(Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-2",
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("EncodeCommand(second) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applyCh := make(chan raft.LogEntry, 2)

	done := make(chan error, 1)

	go func() {
		done <- applier.Run(ctx, applyCh)
	}()

	applyCh <- raft.LogEntry{
		Index: 1,
		Term:  1,
		Data:  firstCommand,
	}

	applyCh <- raft.LogEntry{
		Index: 2,
		Term:  1,
		Data:  secondCommand,
	}

	waitCtx, waitCancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer waitCancel()

	firstResult, err := applier.WaitResult(waitCtx, 1)
	if err != nil {
		t.Fatalf("WaitResult(first) error = %v", err)
	}

	if firstResult.Err != nil {
		t.Fatalf("first claim failed: %v", firstResult.Err)
	}

	secondResult, err := applier.WaitResult(waitCtx, 2)
	if err != nil {
		t.Fatalf("WaitResult(second) error = %v", err)
	}

	if secondResult.Err == nil {
		t.Fatal("expected second claim to fail")
	}

	if !errors.Is(secondResult.Err, ErrJobAlreadyClaimed) {
		t.Fatalf(
			"second claim error = %v, want ErrJobAlreadyClaimed",
			secondResult.Err,
		)
	}

	select {
	case err := <-done:
		t.Fatalf(
			"Applier stopped after expected claim conflict: %v",
			err,
		)

	default:
	}

	job, ok := store.GetJob("job-1")
	if !ok {
		t.Fatal("expected job to exist")
	}

	if job.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"assigned worker = %q, want %q",
			job.AssignedWorkerID,
			"worker-1",
		)
	}

	if job.FencingToken != 1 {
		t.Fatalf(
			"fencing token = %d, want 1",
			job.FencingToken,
		)
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context canceled", err)
		}

	case <-time.After(time.Second):
		t.Fatal("Applier.Run() did not stop")
	}
}
