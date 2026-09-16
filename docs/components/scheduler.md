# Component: Scheduler

`internal/scheduler` is a job-scheduling library that operates against the
same replicated `kv.Store`/`raft.RaftNode` as everything else. **It is
fully implemented and tested, but `cmd/raftiq/main.go` never constructs
one** — no flag starts a scheduler loop. See [`../AUDIT.md`](../AUDIT.md).

## Job model (`internal/model/job.go`)

```go
type JobState string

const (
	JobPending   JobState = "PENDING"
	JobScheduled JobState = "SCHEDULED"
	JobRunning   JobState = "RUNNING"
	JobSucceeded JobState = "SUCCEEDED"
	JobFailed    JobState = "FAILED"
)

type Job struct {
	ID               JobID
	Payload          []byte
	State            JobState
	ScheduledAt      int64  // unix nanos; job becomes eligible at this time
	AssignedWorkerID string
	FencingToken     uint64
	Attempt          uint32
	ExecutionID      string
	CreatedIndex     LogIndex
}
```

Job state lives inside `kv.Store.jobs` (`map[model.JobID]model.Job`) and
is mutated only via replicated `Command`s (`CommandClaimJob`,
`CommandJobStart`, `CommandJobSucceeded`, `CommandJobFailed`,
`CommandJobReclaim`) — the same apply-in-order guarantee that makes
`Store`'s KV data converge across replicas applies to job state too. See
[`kv.md`](kv.md).

## `WorkerSelector` and `HashWorkerSelector`

```go
type WorkerSelector interface {
	SelectWorker(job model.Job) (string, error)
}
```

`HashWorkerSelector` (`scheduler.go`) deterministically maps a job to a
worker ID using an FNV hash of the job ID against a fixed, sorted worker
list — by design, the mapping **does not depend on scheduler-local
state**, so if leadership moves to a different node mid-operation, the new
leader's scheduler computes the exact same assignment for the same job.
This is what makes scheduling itself safe to run from whichever node
happens to be Raft leader, without needing separate scheduler-leader
election logic.

## `Scheduler` (`scheduler.go`)

```go
func New(raftNode *raft.RaftNode, store *kv.Store, applier *kv.Applier,
    selector WorkerSelector, config Config) (*Scheduler, error)
```

`Config{Interval, Lease}` — `Interval` is how often the scheduler loop
runs (`DefaultInterval = 100ms`), `Lease` is how long a claimed job stays
assigned before it's considered abandoned (`DefaultLease = 30s`).

`Scheduler.Run(ctx)` loops on the configured interval and, each tick:

- **`scheduleDueJobs(ctx)`** — finds jobs in `JobPending` whose
  `ScheduledAt` has passed, picks a worker via the selector, and proposes
  `CommandClaimJob` through Raft (same propose-and-wait-applied pattern as
  everything else in this project).
- **`reclaimExpiredJobs(ctx)`** — finds jobs in `JobRunning`/`JobScheduled`
  whose lease has expired without a corresponding
  `JobSucceeded`/`JobFailed` transition, and proposes `CommandJobReclaim`
  to make them eligible for reassignment. This is the mechanism that
  recovers from a worker that claimed a job and then died or partitioned
  away silently.

Only a Raft leader should be actively driving scheduling in practice
(proposing against a non-leader `RaftNode` fails), though nothing in
`Scheduler` itself checks leadership before attempting a propose — it
relies on `RaftNode.Propose`'s own leader check.

## Fencing tokens for jobs

`AssignedWorkerID`/`FencingToken` on a `Job` play the same role as
`internal/lock`'s fencing tokens: if a job is reclaimed and reassigned
(new token issued), a worker still operating on the *old* assignment can
be detected and rejected via `worker.JobSource.ValidateJobOwnership` — see
[`worker.md`](worker.md) and `tests/chaos/`'s
`TestStaleFencingTokenRejectsZombieWorker`/
`TestZombieWorkerCannotCompleteReclaimedJob`.

## Reachability

`internal/scheduler` has ~14 unit tests and is exercised by chaos tests,
but `cmd/raftiq/main.go` doesn't import it and there's no CLI flag to
start one. To use it, embed `scheduler.New(...)` in your own process
alongside a `raft.RaftNode`/`kv.Store`/`kv.Applier` you construct yourself
— it's a library, not a running subsystem of the shipped binary today.
