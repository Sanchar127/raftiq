package chaos

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/worker"
)

func TestZombieWorkerCannotCompleteReclaimedJob(t *testing.T) {
	const (
		jobID   = model.JobID("job-zombie-worker")
		workerA = "worker-a"
		workerB = "worker-b"
	)

	now := time.Now().UnixNano()
	leaseA := now + int64(30*time.Second)
	reclaimAt := leaseA + 1
	leaseB := reclaimAt + int64(30*time.Second)

	store := kv.NewStore()
	node := raft.NewRaftNode("node-1")
	applier := kv.NewApplier(store)

	if err := node.Start(); err != nil {
		t.Fatalf("start raft node: %v", err)
	}
	defer node.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applierDone := make(chan error, 1)

	go func() {
		applierDone <- applier.Run(ctx, node.ApplyCh())
	}()

	waitForSingleNodeLeader(t, node)

	job := model.Job{
		ID:      jobID,
		Payload: []byte("execute-task"),
		State:   model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimJob := func(workerID string, expiresAt int64) model.Job {
		t.Helper()

		result := proposeJobCommand(t, node, applier, kv.Command{
			Type:      kv.CommandClaimJob,
			JobID:     string(jobID),
			OwnerID:   workerID,
			ExpiresAt: expiresAt,
			At:        time.Now().UnixNano(),
		})

		if result.Err != nil {
			t.Fatalf(
				"claim job for %s: %v",
				workerID,
				result.Err,
			)
		}

		if result.Job == nil {
			t.Fatalf(
				"claim job for %s returned nil job",
				workerID,
			)
		}

		return *result.Job
	}

	reclaimJob := func(expectedToken uint64, at int64) model.Job {
		t.Helper()

		result := proposeJobCommand(t, node, applier, kv.Command{
			Type:         kv.CommandJobReclaim,
			JobID:        string(jobID),
			FencingToken: expectedToken,
			At:           at,
		})

		if result.Err != nil {
			t.Fatalf(
				"reclaim job with token %d: %v",
				expectedToken,
				result.Err,
			)
		}

		if result.Job == nil {
			t.Fatalf("reclaim returned nil job")
		}

		return *result.Job
	}

	// ---------------------------------------------------------------------
	// 1. Worker A claims the job with fencing token 1.
	// ---------------------------------------------------------------------

	workerAJob := claimJob(workerA, leaseA)

	if workerAJob.FencingToken != 1 {
		t.Fatalf(
			"expected worker A fencing token 1, got %d",
			workerAJob.FencingToken,
		)
	}

	if workerAJob.AssignedWorkerID != workerA {
		t.Fatalf(
			"expected worker A to own job, got %q",
			workerAJob.AssignedWorkerID,
		)
	}

	if workerAJob.State != model.JobScheduled {
		t.Fatalf(
			"expected scheduled state after worker A claim, got %s",
			workerAJob.State,
		)
	}

	// ---------------------------------------------------------------------
	// 2. Start Worker A and block it inside the handler.
	// ---------------------------------------------------------------------

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})

	var handlerOnce sync.Once

	handler := worker.HandlerFunc(
		func(ctx context.Context, executingJob model.Job) error {
			handlerOnce.Do(func() {
				close(handlerStarted)
			})

			select {
			case <-releaseHandler:
				return nil

			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)

	zombieWorker, err := worker.New(workerA, handler)
	if err != nil {
		t.Fatalf("create worker A: %v", err)
	}

	zombieWorker.ConfigureLoop(
		store,
		worker.Config{
			Interval: 10 * time.Millisecond,
			Raft:     node,
			Applier:  applier,
		},
	)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)

	go func() {
		workerDone <- zombieWorker.Run(workerCtx)
	}()

	// Worker A must actually enter the handler before its lease is reclaimed.
	select {
	case <-handlerStarted:

	case <-time.After(3 * time.Second):
		workerCancel()

		select {
		case <-workerDone:
		case <-time.After(time.Second):
		}

		t.Fatal("worker A never started executing the job")
	}

	// ---------------------------------------------------------------------
	// 3. Verify the job transitioned to RUNNING under Worker A.
	// ---------------------------------------------------------------------

	waitForJobState(
		t,
		store,
		jobID,
		model.JobRunning,
		3*time.Second,
	)

	current, ok := store.GetJob(jobID)
	if !ok {
		t.Fatalf("job %s not found while verifying running state", jobID)
	}

	if current.State != model.JobRunning {
		t.Fatalf(
			"expected job to be running under worker A, got %s",
			current.State,
		)
	}

	if current.AssignedWorkerID != workerA {
		t.Fatalf(
			"expected worker A to own running job, got %q",
			current.AssignedWorkerID,
		)
	}

	if current.FencingToken != workerAJob.FencingToken {
		t.Fatalf(
			"expected running token %d, got %d",
			workerAJob.FencingToken,
			current.FencingToken,
		)
	}

	// ---------------------------------------------------------------------
	// 4. Reclaim Worker A's expired lease while its handler is blocked.
	// ---------------------------------------------------------------------

	reclaimedJob := reclaimJob(
		workerAJob.FencingToken,
		reclaimAt,
	)

	if reclaimedJob.State != model.JobPending {
		t.Fatalf(
			"expected reclaimed job to return to pending, got %s",
			reclaimedJob.State,
		)
	}

	if reclaimedJob.AssignedWorkerID != "" {
		t.Fatalf(
			"expected reclaimed job to have no owner, got %q",
			reclaimedJob.AssignedWorkerID,
		)
	}

	if reclaimedJob.FencingToken != 0 {
		t.Fatalf(
			"expected reclaimed job token 0, got %d",
			reclaimedJob.FencingToken,
		)
	}

	// ---------------------------------------------------------------------
	// 5. Worker B claims the reclaimed job and receives token 2.
	// ---------------------------------------------------------------------

	workerBJob := claimJob(workerB, leaseB)

	if workerBJob.FencingToken != 2 {
		t.Fatalf(
			"expected worker B fencing token 2, got %d",
			workerBJob.FencingToken,
		)
	}

	if workerBJob.AssignedWorkerID != workerB {
		t.Fatalf(
			"expected worker B to own job, got %q",
			workerBJob.AssignedWorkerID,
		)
	}

	if workerBJob.State != model.JobScheduled {
		t.Fatalf(
			"expected worker B job to be scheduled, got %s",
			workerBJob.State,
		)
	}

	if workerBJob.ExecutionID == workerAJob.ExecutionID {
		t.Fatalf(
			"expected worker B to receive a new execution ID, got %q",
			workerBJob.ExecutionID,
		)
	}

	if workerBJob.FencingToken <= workerAJob.FencingToken {
		t.Fatalf(
			"expected worker B token %d to be newer than worker A token %d",
			workerBJob.FencingToken,
			workerAJob.FencingToken,
		)
	}

	// ---------------------------------------------------------------------
	// 6. Release Worker A's handler.
	//
	// Worker A will now attempt:
	//
	//     JobSucceeded(token=1)
	//
	// Worker B is already authoritative with:
	//
	//     token=2
	//
	// Therefore Worker A's completion must be rejected.
	// ---------------------------------------------------------------------

	close(releaseHandler)

	// Worker.Run is a long-lived loop. The handler returning does not cause
	// Run() to return. Wait for the state machine to settle instead.
	waitForAuthoritativeWorker(
		t,
		store,
		jobID,
		workerB,
		workerBJob.FencingToken,
		workerBJob.ExecutionID,
		3*time.Second,
	)

	// ---------------------------------------------------------------------
	// 7. Verify the zombie completion did not overwrite Worker B.
	// ---------------------------------------------------------------------

	finalJob, ok := store.GetJob(jobID)
	if !ok {
		t.Fatalf("job %s not found after zombie completion", jobID)
	}

	if finalJob.State != model.JobScheduled {
		t.Fatalf(
			"expected job to remain scheduled for worker B, got %s",
			finalJob.State,
		)
	}

	if finalJob.AssignedWorkerID != workerB {
		t.Fatalf(
			"expected worker B to remain owner, got %q",
			finalJob.AssignedWorkerID,
		)
	}

	if finalJob.FencingToken != workerBJob.FencingToken {
		t.Fatalf(
			"expected fencing token %d, got %d",
			workerBJob.FencingToken,
			finalJob.FencingToken,
		)
	}

	if finalJob.ExecutionID != workerBJob.ExecutionID {
		t.Fatalf(
			"expected worker B execution ID %q, got %q",
			workerBJob.ExecutionID,
			finalJob.ExecutionID,
		)
	}

	// ---------------------------------------------------------------------
	// 8. Worker.Run is long-lived, so explicitly stop Worker A.
	// ---------------------------------------------------------------------

	workerCancel()

	select {
	case err := <-workerDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"worker A returned unexpected error: %v",
				err,
			)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("worker A did not stop after cancellation")
	}

	// The applier should remain healthy until the test context is cancelled.
	select {
	case err := <-applierDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"applier returned unexpected error: %v",
				err,
			)
		}

	default:
	}
}

func proposeJobCommand(
	t *testing.T,
	node *raft.RaftNode,
	applier *kv.Applier,
	command kv.Command,
) kv.ApplyResult {
	t.Helper()

	data, err := kv.EncodeCommand(command)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	index, err := node.Propose(data)
	if err != nil {
		t.Fatalf("propose command: %v", err)
	}

	result, err := applier.WaitResult(
		context.Background(),
		index,
	)
	if err != nil {
		t.Fatalf(
			"wait for command result at index %d: %v",
			index,
			err,
		)
	}

	return result
}

func waitForSingleNodeLeader(
	t *testing.T,
	node *raft.RaftNode,
) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		if node.State().Role == raft.Leader {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf(
		"single-node raft did not become leader; state=%v",
		node.State().Role,
	)
}

func waitForJobState(
	t *testing.T,
	store *kv.Store,
	jobID model.JobID,
	expectedState model.JobState,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		job, ok := store.GetJob(jobID)
		if !ok {
			t.Fatalf(
				"job %s not found while waiting for state %s",
				jobID,
				expectedState,
			)
		}

		if job.State == expectedState {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	job, ok := store.GetJob(jobID)
	if !ok {
		t.Fatalf(
			"job %s not found after waiting for state %s",
			jobID,
			expectedState,
		)
	}

	t.Fatalf(
		"job %s did not reach state %s within %s; current state=%s owner=%q token=%d execution_id=%q",
		jobID,
		expectedState,
		timeout,
		job.State,
		job.AssignedWorkerID,
		job.FencingToken,
		job.ExecutionID,
	)
}

func waitForAuthoritativeWorker(
	t *testing.T,
	store *kv.Store,
	jobID model.JobID,
	expectedWorker string,
	expectedToken uint64,
	expectedExecutionID string,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		job, ok := store.GetJob(jobID)
		if !ok {
			t.Fatalf(
				"job %s not found while waiting for worker authority",
				jobID,
			)
		}

		if job.State == model.JobScheduled &&
			job.AssignedWorkerID == expectedWorker &&
			job.FencingToken == expectedToken &&
			job.ExecutionID == expectedExecutionID {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	job, ok := store.GetJob(jobID)
	if !ok {
		t.Fatalf(
			"job %s not found after waiting for worker authority",
			jobID,
		)
	}

	t.Fatalf(
		"worker %q did not remain authoritative within %s; state=%s owner=%q token=%d execution_id=%q",
		expectedWorker,
		timeout,
		job.State,
		job.AssignedWorkerID,
		job.FencingToken,
		job.ExecutionID,
	)
}
