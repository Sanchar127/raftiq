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

func TestSchedulerReclaimsExpiredScheduledJob(t *testing.T) {
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

	now := time.Now().UnixNano()

	job := model.Job{
		ID:          "job-expired-scheduled",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: now - time.Second.Nanoseconds(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	claimed, err := store.ClaimJob(
		job.ID,
		"worker-1",
		now-time.Millisecond.Nanoseconds(),
		1,
	)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.reclaimExpiredJobs(ctx, now); err != nil {
		t.Fatalf("reclaimExpiredJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobPending {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobPending,
			got.State,
		)
	}

	if got.AssignedWorkerID != "" {
		t.Fatalf(
			"expected no assigned worker, got %q",
			got.AssignedWorkerID,
		)
	}

	if got.FencingToken != 0 {
		t.Fatalf(
			"expected fencing token 0, got %d",
			got.FencingToken,
		)
	}

	if got.Attempt != claimed.Attempt {
		t.Fatalf(
			"expected attempt %d to be preserved, got %d",
			claimed.Attempt,
			got.Attempt,
		)
	}
}

func TestSchedulerReclaimsExpiredRunningJob(t *testing.T) {
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

	now := time.Now().UnixNano()

	job := model.Job{
		ID:          "job-expired-running",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: now - time.Second.Nanoseconds(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	// Claim with a lease that is still valid at `now` so the
	// transition to Running can succeed.
	expiresAt := now + time.Minute.Nanoseconds()

	claimed, err := store.ClaimJob(
		job.ID,
		"worker-1",
		expiresAt,
		1,
	)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}

	if _, err := store.TransitionJobState(
		job.ID,
		"worker-1",
		claimed.FencingToken,
		model.JobScheduled,
		model.JobRunning,
		now,
	); err != nil {
		t.Fatalf("TransitionJobState() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	// Reclaim at a moment after the lease has expired.
	reclaimAt := expiresAt + 1

	if err := scheduler.reclaimExpiredJobs(ctx, reclaimAt); err != nil {
		t.Fatalf("reclaimExpiredJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobPending {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobPending,
			got.State,
		)
	}

	if got.AssignedWorkerID != "" {
		t.Fatalf(
			"expected no assigned worker, got %q",
			got.AssignedWorkerID,
		)
	}

	if got.FencingToken != 0 {
		t.Fatalf(
			"expected fencing token 0, got %d",
			got.FencingToken,
		)
	}
}

func TestSchedulerDoesNotReclaimUnexpiredJob(t *testing.T) {
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

	now := time.Now().UnixNano()

	job := model.Job{
		ID:          "job-unexpired",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: now - time.Second.Nanoseconds(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	claimed, err := store.ClaimJob(
		job.ID,
		"worker-1",
		now+time.Minute.Nanoseconds(),
		1,
	)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.reclaimExpiredJobs(ctx, now); err != nil {
		t.Fatalf("reclaimExpiredJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobScheduled {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobScheduled,
			got.State,
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

func TestSchedulerReclaimedJobGetsNewFencingToken(t *testing.T) {
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

	now := time.Now().UnixNano()

	job := model.Job{
		ID:          "job-new-token",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: now - time.Second.Nanoseconds(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	first, err := store.ClaimJob(
		job.ID,
		"worker-1",
		now-time.Millisecond.Nanoseconds(),
		1,
	)
	if err != nil {
		t.Fatalf("first ClaimJob() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.reclaimExpiredJobs(ctx, now); err != nil {
		t.Fatalf("reclaimExpiredJobs() error = %v", err)
	}

	reclaimed, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected reclaimed job to exist")
	}

	if reclaimed.State != model.JobPending {
		t.Fatalf(
			"expected reclaimed state %q, got %q",
			model.JobPending,
			reclaimed.State,
		)
	}

	if err := scheduler.scheduleDueJobs(ctx); err != nil {
		t.Fatalf("scheduleDueJobs() error = %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobScheduled {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobScheduled,
			got.State,
		)
	}

	if got.FencingToken <= first.FencingToken {
		t.Fatalf(
			"expected new fencing token greater than %d, got %d",
			first.FencingToken,
			got.FencingToken,
		)
	}

	if got.Attempt != first.Attempt+1 {
		t.Fatalf(
			"expected attempt %d, got %d",
			first.Attempt+1,
			got.Attempt,
		)
	}
}

func TestSchedulerStaleReclaimCannotRemoveNewClaim(t *testing.T) {
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

	now := time.Now().UnixNano()

	job := model.Job{
		ID:          "job-stale-reclaim",
		Payload:     []byte("task"),
		State:       model.JobPending,
		ScheduledAt: now - time.Second.Nanoseconds(),
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	first, err := store.ClaimJob(
		job.ID,
		"worker-1",
		now-time.Millisecond.Nanoseconds(),
		1,
	)
	if err != nil {
		t.Fatalf("first ClaimJob() error = %v", err)
	}

	node.Start()
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = applier.Run(ctx, node.ApplyCh())
	}()

	waitForSchedulerLeader(t, node)

	if err := scheduler.reclaimExpiredJobs(ctx, now); err != nil {
		t.Fatalf("first reclaimExpiredJobs() error = %v", err)
	}

	if err := scheduler.scheduleDueJobs(ctx); err != nil {
		t.Fatalf("scheduleDueJobs() error = %v", err)
	}

	current, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if current.FencingToken <= first.FencingToken {
		t.Fatalf(
			"expected new token greater than %d, got %d",
			first.FencingToken,
			current.FencingToken,
		)
	}

	newToken := current.FencingToken

	commandData, err := kv.EncodeCommand(kv.Command{
		Type:         kv.CommandJobReclaim,
		JobID:        string(job.ID),
		FencingToken: first.FencingToken,
		At:           now + time.Second.Nanoseconds(),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	index, err := node.Propose(commandData)
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	result, err := applier.WaitResult(ctx, index)
	if err != nil {
		t.Fatalf("WaitResult() error = %v", err)
	}

	if !errors.Is(result.Err, kv.ErrJobOwnershipLost) {
		t.Fatalf(
			"expected ErrJobOwnershipLost, got %v",
			result.Err,
		)
	}

	current, ok = store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if current.FencingToken != newToken {
		t.Fatalf(
			"expected fencing token %d to remain current, got %d",
			newToken,
			current.FencingToken,
		)
	}

	if current.State != model.JobScheduled {
		t.Fatalf(
			"expected state %q to remain unchanged, got %q",
			model.JobScheduled,
			current.State,
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
