package kv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
func TestApplierCreateJobReturnsJob(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	command, err := EncodeCommand(Command{
		Type:        CommandCreateJob,
		JobID:       "job-1",
		Payload:     []byte("send-email"),
		ScheduledAt: 123456789,
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
		t.Fatalf("create job result error = %v", result.Err)
	}

	if result.Job == nil {
		t.Fatal("expected created job in ApplyResult")
	}

	if result.Job.ID != "job-1" {
		t.Fatalf("job ID = %q, want %q", result.Job.ID, "job-1")
	}

	if string(result.Job.Payload) != "send-email" {
		t.Fatalf(
			"job payload = %q, want %q",
			result.Job.Payload,
			"send-email",
		)
	}

	if result.Job.State != model.JobPending {
		t.Fatalf(
			"job state = %q, want %q",
			result.Job.State,
			model.JobPending,
		)
	}

	if result.Job.ScheduledAt != 123456789 {
		t.Fatalf(
			"scheduled at = %d, want %d",
			result.Job.ScheduledAt,
			123456789,
		)
	}

	if result.Job.CreatedIndex != 42 {
		t.Fatalf(
			"created index = %d, want %d",
			result.Job.CreatedIndex,
			42,
		)
	}

	job, ok := store.GetJob("job-1")
	if !ok {
		t.Fatal("expected job to exist in store")
	}

	if job.State != model.JobPending {
		t.Fatalf(
			"stored job state = %q, want %q",
			job.State,
			model.JobPending,
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

func TestApplierCreateJobConflictDoesNotStopApplier(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	err := store.CreateJob(model.Job{
		ID:      "job-1",
		Payload: []byte("existing"),
		State:   model.JobPending,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	firstCommand, err := EncodeCommand(Command{
		Type:        CommandCreateJob,
		JobID:       "job-1",
		Payload:     []byte("first"),
		ScheduledAt: 100,
	})
	if err != nil {
		t.Fatalf("EncodeCommand(first) error = %v", err)
	}

	secondCommand, err := EncodeCommand(Command{
		Type:        CommandCreateJob,
		JobID:       "job-1",
		Payload:     []byte("second"),
		ScheduledAt: 200,
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

	if firstResult.Err == nil {
		t.Fatal("expected first create to fail because job already exists")
	}

	if !errors.Is(firstResult.Err, ErrJobAlreadyExists) {
		t.Fatalf(
			"first create error = %v, want ErrJobAlreadyExists",
			firstResult.Err,
		)
	}

	secondResult, err := applier.WaitResult(waitCtx, 2)
	if err != nil {
		t.Fatalf("WaitResult(second) error = %v", err)
	}

	if secondResult.Err == nil {
		t.Fatal("expected second create to fail because job already exists")
	}

	if !errors.Is(secondResult.Err, ErrJobAlreadyExists) {
		t.Fatalf(
			"second create error = %v, want ErrJobAlreadyExists",
			secondResult.Err,
		)
	}

	select {
	case err := <-done:
		t.Fatalf(
			"Applier stopped after expected create conflict: %v",
			err,
		)

	default:
	}

	job, ok := store.GetJob("job-1")
	if !ok {
		t.Fatal("expected existing job to remain")
	}

	if string(job.Payload) != "existing" {
		t.Fatalf(
			"stored payload = %q, want %q",
			job.Payload,
			"existing",
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

func TestApplierSnapshotReturnsAppliedStateAndIndex(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applyCh := make(chan raft.LogEntry, 2)
	done := make(chan error, 1)

	go func() {
		done <- applier.Run(ctx, applyCh)
	}()

	putCommand, err := EncodeCommand(Command{
		Type:  CommandPut,
		Key:   "snapshot-key",
		Value: []byte("value-at-42"),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	applyCh <- raft.LogEntry{
		Index: 42,
		Term:  3,
		Data:  putCommand,
	}

	waitCtx, waitCancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer waitCancel()

	if err := applier.WaitApplied(waitCtx, 42); err != nil {
		t.Fatalf("WaitApplied() error = %v", err)
	}

	data, index, err := applier.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if index != 42 {
		t.Fatalf(
			"snapshot index = %d, want 42",
			index,
		)
	}

	snapshotStore := NewStore()

	if err := snapshotStore.Restore(data); err != nil {
		t.Fatalf(
			"Restore(snapshot data) error = %v",
			err,
		)
	}

	value, ok := snapshotStore.Get("snapshot-key")
	if !ok {
		t.Fatal("snapshot did not contain applied key")
	}

	if string(value) != "value-at-42" {
		t.Fatalf(
			"snapshot value = %q, want %q",
			value,
			"value-at-42",
		)
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"Run() error = %v, want context canceled",
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("Applier.Run() did not stop")
	}
}

func TestApplierSnapshotConcurrentWithApply(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applyCh := make(chan raft.LogEntry, 32)
	done := make(chan error, 1)

	go func() {
		done <- applier.Run(ctx, applyCh)
	}()

	const entries = 100

	for i := 1; i <= entries; i++ {
		command, err := EncodeCommand(Command{
			Type:  CommandPut,
			Key:   "snapshot-key",
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("EncodeCommand() error = %v", err)
		}

		applyCh <- raft.LogEntry{
			Index: model.LogIndex(i),
			Term:  1,
			Data:  command,
		}
	}

	waitCtx, waitCancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer waitCancel()

	if err := applier.WaitApplied(waitCtx, entries); err != nil {
		t.Fatalf("WaitApplied() error = %v", err)
	}

	var previousIndex model.LogIndex

	for i := 0; i < 100; i++ {
		data, index, err := applier.Snapshot()
		if err != nil {
			t.Fatalf("Snapshot() error = %v", err)
		}

		if index < previousIndex {
			t.Fatalf(
				"snapshot index moved backward: previous=%d current=%d",
				previousIndex,
				index,
			)
		}

		if index > entries {
			t.Fatalf(
				"snapshot index = %d, want <= %d",
				index,
				entries,
			)
		}

		snapshotStore := NewStore()

		if err := snapshotStore.Restore(data); err != nil {
			t.Fatalf(
				"Restore(snapshot data) error = %v at index %d",
				err,
				index,
			)
		}

		value, ok := snapshotStore.Get("snapshot-key")
		if !ok {
			t.Fatalf(
				"snapshot at index %d missing snapshot-key",
				index,
			)
		}

		expected := fmt.Sprintf("value-%d", index)

		if string(value) != expected {
			t.Fatalf(
				"snapshot at index %d contains value %q, want %q",
				index,
				value,
				expected,
			)
		}

		previousIndex = index
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"Run() error = %v, want context canceled",
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("Applier.Run() did not stop")
	}
}

func TestApplierRestoreSnapshotResetsApplicationState(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	// Seed state that should be replaced by the snapshot.
	store.Put("old-key", []byte("old-value"))

	applier.mu.Lock()
	applier.lastApplied = 100
	applier.applyErr = errors.New("previous apply error")
	applier.results[99] = ApplyResult{
		Err: errors.New("stale result"),
	}
	applier.mu.Unlock()

	snapshotStore := NewStore()
	snapshotStore.Put("snapshot-key", []byte("snapshot-value"))

	snapshotData, err := snapshotStore.Snapshot()
	if err != nil {
		t.Fatalf("snapshot Store.Snapshot() error = %v", err)
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 42,
		LastIncludedTerm:  7,
		Data:              snapshotData,
	}

	if err := applier.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}

	if _, ok := store.Get("old-key"); ok {
		t.Fatal("old state survived snapshot restore")
	}

	value, ok := store.Get("snapshot-key")
	if !ok {
		t.Fatal("snapshot state was not restored")
	}

	if string(value) != "snapshot-value" {
		t.Fatalf(
			"restored value = %q, want %q",
			value,
			"snapshot-value",
		)
	}

	applier.mu.Lock()
	defer applier.mu.Unlock()

	if applier.lastApplied != 42 {
		t.Fatalf(
			"lastApplied = %d, want 42",
			applier.lastApplied,
		)
	}

	if applier.applyErr != nil {
		t.Fatalf(
			"applyErr = %v, want nil after snapshot restore",
			applier.applyErr,
		)
	}

	if len(applier.results) != 0 {
		t.Fatalf(
			"results contains %d entries, want 0 after snapshot restore",
			len(applier.results),
		)
	}
}
func TestApplierConcurrentSetLoggerAndRun(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applyCh := make(chan raft.LogEntry, 1)
	done := make(chan error, 1)

	go func() {
		done <- applier.Run(ctx, applyCh)
	}()

	for i := 0; i < 1000; i++ {
		applier.SetLogger(slog.Default())
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

func TestApplierConcurrentSetLoggerAndRestoreSnapshot(t *testing.T) {
	store := NewStore()
	applier := NewApplier(store)

	snapshotStore := NewStore()
	snapshotStore.Put("snapshot-key", []byte("snapshot-value"))

	snapshotData, err := snapshotStore.Snapshot()
	if err != nil {
		t.Fatalf("snapshot Store.Snapshot() error = %v", err)
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 42,
		LastIncludedTerm:  7,
		Data:               snapshotData,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)

		for i := 0; i < 1000; i++ {
			applier.SetLogger(slog.Default())
		}
	}()

	for i := 0; i < 1000; i++ {
		if err := applier.RestoreSnapshot(snapshot); err != nil {
			t.Fatalf("RestoreSnapshot() error = %v", err)
		}
	}

	<-done
}