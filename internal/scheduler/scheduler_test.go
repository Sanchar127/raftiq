package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	node := raft.NewRaftNode("A")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := NewHashWorkerSelector([]string{"worker-1"})
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	tests := []struct {
		name     string
		node     *raft.RaftNode
		store    *kv.Store
		applier  *kv.Applier
		selector WorkerSelector
		config   Config
		wantErr  string
	}{
		{
			name:     "nil raft node",
			store:    store,
			applier:  applier,
			selector: selector,
			config: Config{
				Interval: time.Second,
				Lease:    time.Second,
			},
			wantErr: "raft node is required",
		},
		{
			name:     "nil store",
			node:     node,
			applier:  applier,
			selector: selector,
			config: Config{
				Interval: time.Second,
				Lease:    time.Second,
			},
			wantErr: "store is required",
		},
		{
			name:     "nil applier",
			node:     node,
			store:    store,
			selector: selector,
			config: Config{
				Interval: time.Second,
				Lease:    time.Second,
			},
			wantErr: "applier is required",
		},
		{
			name:    "nil selector",
			node:    node,
			store:   store,
			applier: applier,
			config: Config{
				Interval: time.Second,
				Lease:    time.Second,
			},
			wantErr: "worker selector is required",
		},
		{
			name:     "invalid interval",
			node:     node,
			store:    store,
			applier:  applier,
			selector: selector,
			config: Config{
				Interval: 0,
				Lease:    time.Second,
			},
			wantErr: ErrInvalidInterval.Error(),
		},
		{
			name:     "invalid lease",
			node:     node,
			store:    store,
			applier:  applier,
			selector: selector,
			config: Config{
				Interval: time.Second,
				Lease:    0,
			},
			wantErr: ErrInvalidLease.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(
				tt.node,
				tt.store,
				tt.applier,
				tt.selector,
				tt.config,
			)

			if err == nil {
				t.Fatal("expected error")
			}

			if err.Error() != tt.wantErr {
				t.Fatalf(
					"expected error %q, got %q",
					tt.wantErr,
					err.Error(),
				)
			}
		})
	}
}

func TestNewHashWorkerSelectorRejectsInvalidWorkers(t *testing.T) {
	t.Parallel()

	if _, err := NewHashWorkerSelector(nil); !errors.Is(err, ErrNoWorkers) {
		t.Fatalf("expected ErrNoWorkers, got %v", err)
	}

	if _, err := NewHashWorkerSelector([]string{""}); err == nil {
		t.Fatal("expected empty worker ID to fail")
	}

	if _, err := NewHashWorkerSelector(
		[]string{"worker-1", "worker-1"},
	); err == nil {
		t.Fatal("expected duplicate worker ID to fail")
	}
}

func TestHashWorkerSelectorIsStable(t *testing.T) {
	t.Parallel()

	selectorA, err := NewHashWorkerSelector(
		[]string{"worker-1", "worker-2", "worker-3"},
	)
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	selectorB, err := NewHashWorkerSelector(
		[]string{"worker-3", "worker-1", "worker-2"},
	)
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	jobs := []model.Job{
		{ID: "job-1"},
		{ID: "job-2"},
		{ID: "job-3"},
		{ID: "job-4"},
		{ID: "job-5"},
	}

	for _, job := range jobs {
		workerA, err := selectorA.SelectWorker(job)
		if err != nil {
			t.Fatalf("selectorA.SelectWorker() error = %v", err)
		}

		workerB, err := selectorB.SelectWorker(job)
		if err != nil {
			t.Fatalf("selectorB.SelectWorker() error = %v", err)
		}

		if workerA != workerB {
			t.Fatalf(
				"job %q mapped to different workers: %q != %q",
				job.ID,
				workerA,
				workerB,
			)
		}
	}
}

func TestHashWorkerSelectorReturnsConfiguredWorker(t *testing.T) {
	t.Parallel()

	selector, err := NewHashWorkerSelector(
		[]string{"worker-1", "worker-2"},
	)
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	worker, err := selector.SelectWorker(model.Job{
		ID: "job-1",
	})
	if err != nil {
		t.Fatalf("SelectWorker() error = %v", err)
	}

	if worker != "worker-1" && worker != "worker-2" {
		t.Fatalf("unexpected worker %q", worker)
	}
}

func TestSchedulerSchedulesDueJob(t *testing.T) {
	node := raft.NewRaftNode("A")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := NewHashWorkerSelector([]string{"worker-1"})
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	scheduler, err := New(
		node,
		store,
		applier,
		selector,
		Config{
			Interval: 10 * time.Millisecond,
			Lease:    time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := model.Job{
		ID:          "job-due",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: time.Now().Add(-time.Second).UnixNano(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.scheduleDueJobs(ctx); err != nil {
		t.Fatalf("scheduleDueJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobScheduled {
		t.Fatalf(
			"expected job state %q, got %q",
			model.JobScheduled,
			got.State,
		)
	}

	if got.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"expected worker %q, got %q",
			"worker-1",
			got.AssignedWorkerID,
		)
	}

	if got.FencingToken == 0 {
		t.Fatal("expected non-zero fencing token")
	}

	if got.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", got.Attempt)
	}
}

func TestSchedulerIgnoresFutureJob(t *testing.T) {
	node := raft.NewRaftNode("A")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := NewHashWorkerSelector([]string{"worker-1"})
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	scheduler, err := New(
		node,
		store,
		applier,
		selector,
		Config{
			Interval: 10 * time.Millisecond,
			Lease:    time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := model.Job{
		ID:          "job-future",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: time.Now().Add(time.Hour).UnixNano(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.scheduleDueJobs(ctx); err != nil {
		t.Fatalf("scheduleDueJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobPending {
		t.Fatalf(
			"expected future job to remain %q, got %q",
			model.JobPending,
			got.State,
		)
	}

	if got.FencingToken != 0 {
		t.Fatalf("expected no fencing token, got %d", got.FencingToken)
	}

	if got.Attempt != 0 {
		t.Fatalf("expected attempt 0, got %d", got.Attempt)
	}
}

func TestSchedulerDoesNothingOnFollower(t *testing.T) {
	node := raft.NewRaftNode("A")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := NewHashWorkerSelector([]string{"worker-1"})
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	scheduler, err := New(
		node,
		store,
		applier,
		selector,
		Config{
			Interval: 10 * time.Millisecond,
			Lease:    time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := model.Job{
		ID:          "job-follower",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: time.Now().Add(-time.Second).UnixNano(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if err := scheduler.scheduleDueJobs(ctx); err != nil {
		t.Fatalf("scheduleDueJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobPending {
		t.Fatalf(
			"expected follower to leave job %q pending, got %q",
			model.JobPending,
			got.State,
		)
	}
}

func TestSchedulerRunStopsOnCancellation(t *testing.T) {
	t.Parallel()

	node := raft.NewRaftNode("A")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := NewHashWorkerSelector([]string{"worker-1"})
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	scheduler, err := New(
		node,
		store,
		applier,
		selector,
		Config{
			Interval: time.Hour,
			Lease:    time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = scheduler.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSchedulerHandlesClaimConflict(t *testing.T) {
	node := raft.NewRaftNode("A")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := NewHashWorkerSelector([]string{"worker-2"})
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	scheduler, err := New(
		node,
		store,
		applier,
		selector,
		Config{
			Interval: 10 * time.Millisecond,
			Lease:    time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := model.Job{
		ID:          "job-conflict",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: time.Now().Add(-time.Second).UnixNano(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	claimed, err := store.ClaimJob(
		job.ID,
		"worker-1",
		time.Now().Add(time.Minute).UnixNano(),
		1,
	)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}

	if claimed.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"expected worker-1, got %q",
			claimed.AssignedWorkerID,
		)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.scheduleDueJobs(ctx); err != nil {
		t.Fatalf("scheduleDueJobs() returned unexpected error: %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"expected existing worker-1 assignment to remain, got %q",
			got.AssignedWorkerID,
		)
	}

	if got.FencingToken != claimed.FencingToken {
		t.Fatalf(
			"expected fencing token %d, got %d",
			claimed.FencingToken,
			got.FencingToken,
		)
	}
}

func waitForSchedulerLeader(t *testing.T, node *raft.RaftNode) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		if node.State().Role == raft.Leader {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("node %s did not become leader", node.ID())
}
