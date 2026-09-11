package chaos

import (
	"errors"
	"testing"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestStaleFencingTokenRejectsZombieWorker(t *testing.T) {
	store := kv.NewStore()

	const (
		jobID     = model.JobID("job-zombie")
		workerA   = "worker-a"
		workerB   = "worker-b"
		fencedKey = string(jobID)
		tokenA    = uint64(1)
		tokenB    = uint64(2)
		leaseA    = int64(100)
		reclaimAt = int64(101)
		leaseB    = int64(300)
	)

	job := model.Job{
		ID:      jobID,
		Payload: []byte("execute-task"),
		State:   model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	applyCommand := func(index raft.LogIndex, command kv.Command) kv.ApplyResult {
		t.Helper()

		data, err := kv.EncodeCommand(command)
		if err != nil {
			t.Fatalf("encode command: %v", err)
		}

		return kv.Apply(
			store,
			raft.LogEntry{
				Index: index,
				Term:  1,
				Data:  data,
			},
		)
	}

	t.Run("worker A claims job with first fencing token", func(t *testing.T) {
		result := applyCommand(
			1,
			kv.Command{
				Type:      kv.CommandClaimJob,
				JobID:     string(jobID),
				OwnerID:   workerA,
				ExpiresAt: leaseA,
			},
		)

		if result.Err != nil {
			t.Fatalf("worker A claim failed: %v", result.Err)
		}

		got, ok := store.GetJob(jobID)
		if !ok {
			t.Fatal("expected job to exist after worker A claim")
		}

		if got.AssignedWorkerID != workerA {
			t.Fatalf(
				"assigned worker = %q, want %q",
				got.AssignedWorkerID,
				workerA,
			)
		}

		if got.FencingToken != tokenA {
			t.Fatalf(
				"worker A fencing token = %d, want %d",
				got.FencingToken,
				tokenA,
			)
		}

		if got.Attempt != 1 {
			t.Fatalf("attempt = %d, want 1", got.Attempt)
		}
	})

	t.Run("expired lease is reclaimed", func(t *testing.T) {
		result := applyCommand(
			2,
			kv.Command{
				Type:         kv.CommandJobReclaim,
				JobID:        string(jobID),
				FencingToken: tokenA,
				At:           reclaimAt,
			},
		)

		if result.Err != nil {
			t.Fatalf("reclaim failed: %v", result.Err)
		}

		got, ok := store.GetJob(jobID)
		if !ok {
			t.Fatal("expected job to exist after reclaim")
		}

		if got.State != model.JobPending {
			t.Fatalf(
				"job state after reclaim = %q, want %q",
				got.State,
				model.JobPending,
			)
		}

		if got.AssignedWorkerID != "" {
			t.Fatalf(
				"assigned worker after reclaim = %q, want empty",
				got.AssignedWorkerID,
			)
		}

		if got.FencingToken != 0 {
			t.Fatalf(
				"fencing token after reclaim = %d, want 0",
				got.FencingToken,
			)
		}
	})

	t.Run("worker B receives a new fencing token", func(t *testing.T) {
		result := applyCommand(
			3,
			kv.Command{
				Type:      kv.CommandClaimJob,
				JobID:     string(jobID),
				OwnerID:   workerB,
				ExpiresAt: leaseB,
			},
		)

		if result.Err != nil {
			t.Fatalf("worker B claim failed: %v", result.Err)
		}

		got, ok := store.GetJob(jobID)
		if !ok {
			t.Fatal("expected job to exist after worker B claim")
		}

		if got.AssignedWorkerID != workerB {
			t.Fatalf(
				"assigned worker = %q, want %q",
				got.AssignedWorkerID,
				workerB,
			)
		}

		if got.FencingToken != tokenB {
			t.Fatalf(
				"worker B fencing token = %d, want %d",
				got.FencingToken,
				tokenB,
			)
		}

		if got.Attempt != 2 {
			t.Fatalf("attempt = %d, want 2", got.Attempt)
		}

		if tokenB <= tokenA {
			t.Fatalf(
				"new fencing token %d must be greater than old token %d",
				tokenB,
				tokenA,
			)
		}
	})

	t.Run("worker B can mutate fenced state", func(t *testing.T) {
		result := applyCommand(
			4,
			kv.Command{
				Type:         kv.CommandFencedPut,
				Key:          fencedKey,
				Value:        []byte("worker-b-result"),
				FencingToken: tokenB,
			},
		)

		if result.Err != nil {
			t.Fatalf("worker B fenced write failed: %v", result.Err)
		}

		value, ok := store.GetFenced(fencedKey)
		if !ok {
			t.Fatal("expected fenced value to exist")
		}

		if string(value.Value) != "worker-b-result" {
			t.Fatalf(
				"fenced value = %q, want %q",
				string(value.Value),
				"worker-b-result",
			)
		}

		if value.FencingToken != tokenB {
			t.Fatalf(
				"stored fencing token = %d, want %d",
				value.FencingToken,
				tokenB,
			)
		}
	})

	t.Run("zombie worker cannot reclaim newer ownership", func(t *testing.T) {
		result := applyCommand(
			5,
			kv.Command{
				Type:         kv.CommandJobReclaim,
				JobID:        string(jobID),
				FencingToken: tokenA,
				At:           reclaimAt + 1,
			},
		)

		if !errors.Is(result.Err, kv.ErrJobOwnershipLost) {
			t.Fatalf(
				"stale reclaim error = %v, want %v",
				result.Err,
				kv.ErrJobOwnershipLost,
			)
		}

		got, ok := store.GetJob(jobID)
		if !ok {
			t.Fatal("expected job to remain present")
		}

		if got.AssignedWorkerID != workerB {
			t.Fatalf(
				"worker after stale reclaim = %q, want %q",
				got.AssignedWorkerID,
				workerB,
			)
		}

		if got.FencingToken != tokenB {
			t.Fatalf(
				"token after stale reclaim = %d, want %d",
				got.FencingToken,
				tokenB,
			)
		}
	})

	t.Run("zombie worker cannot overwrite fenced state", func(t *testing.T) {
		result := applyCommand(
			6,
			kv.Command{
				Type:         kv.CommandFencedPut,
				Key:          fencedKey,
				Value:        []byte("worker-a-zombie-result"),
				FencingToken: tokenA,
			},
		)

		if !errors.Is(result.Err, lock.ErrStaleFencingToken) {
			t.Fatalf(
				"stale fenced write error = %v, want %v",
				result.Err,
				lock.ErrStaleFencingToken,
			)
		}

		value, ok := store.GetFenced(fencedKey)
		if !ok {
			t.Fatal("expected worker B's fenced value to remain")
		}

		if string(value.Value) != "worker-b-result" {
			t.Fatalf(
				"fenced value after zombie write = %q, want %q",
				string(value.Value),
				"worker-b-result",
			)
		}

		if value.FencingToken != tokenB {
			t.Fatalf(
				"fenced token after zombie write = %d, want %d",
				value.FencingToken,
				tokenB,
			)
		}
	})
}
