# Component: Worker

`internal/worker` executes jobs claimed by [`scheduler.md`](scheduler.md).
Like the scheduler, it's a fully implemented, tested library that
**`cmd/raftiq/main.go` does not start**. See [`../AUDIT.md`](../AUDIT.md).

## `JobHandler` — the application-specific part

```go
type JobHandler interface {
	Execute(ctx context.Context, job model.Job) error
}
type HandlerFunc func(context.Context, model.Job) error // adapter
```

RaftIQ doesn't know or care what a job *does* — `JobHandler.Execute` is
supplied by whatever application embeds `internal/worker`. The doc
comment on `JobHandler` is explicit about the separation of concerns:
*"Durable job-state transitions are intentionally outside the handler;
those transitions must go through the Raft state machine."* A handler
should do the actual work and return success/failure; it must not itself
try to mutate `kv.Store`'s job state directly.

## `JobSource` — read-only view into replicated job state

```go
type JobSource interface {
	ListAssignedJobs(workerID string) []model.Job
	ValidateJobOwnership(jobID model.JobID, workerID string,
		fencingToken uint64, now int64) bool
}
```

Implemented by `kv.Store` (or a compatible type) so the worker never reads
job state through any path other than the replicated store — no local
worker-side cache of "what I think I own."

## `Worker` — the execution loop

```go
func New(id string, handler JobHandler) (*Worker, error)
func (w *Worker) ConfigureLoop(interval time.Duration, raftNode *raft.RaftNode, applier *kv.Applier, metrics WorkerMetrics) error
func (w *Worker) Run(ctx context.Context) error
```

`Run(ctx)` loops on the configured interval calling
`executeAssignedJobs(ctx)`, which for each job currently assigned to this
worker ID (`source.ListAssignedJobs(w.id)`):

1. **Re-validates ownership first**, before doing anything —
   `source.ValidateJobOwnership(job.ID, w.id, job.FencingToken, now)`. If
   this fails (the job's fencing token has since moved on — e.g. the
   scheduler reclaimed it because the lease expired), the worker skips the
   job entirely and increments a "lease loss" metric. **This check exists
   specifically to make a "zombie worker" (one that's been partitioned
   away, or was just slow, and doesn't realize its claim expired) safe** —
   see `TestStaleFencingTokenRejectsZombieWorker`.
2. **Transitions the job to `Running`** via `transitionJob(ctx, job,
   CommandJobStart, expectedFrom: JobScheduled, expectedTo: JobRunning)`
   — this proposes the transition through Raft (not a local mutation) and
   can itself fail with `ErrJobOwnershipLost`/`ErrInvalidJobState` if the
   job moved out from under the worker between steps 1 and 2 (another
   race the fencing check + expected-state check together close).
3. **Calls `handler.Execute(ctx, runningJob)`** to actually do the work.
4. **Transitions to `Succeeded` or `Failed`** based on the handler's
   result, again via a Raft-proposed `transitionJob` call
   (`CommandJobSucceeded`/`CommandJobFailed`), with the same
   ownership/state races checked and handled the same way.

Every state transition is expected-state-gated (e.g. "only move
`Scheduled` → `Running` if it's still `Scheduled`") so a worker that lost
its job to a reclaim mid-execution can't accidentally complete a job that
was already handed to someone else — see
`TestZombieWorkerCannotCompleteReclaimedJob`.

## Errors

| Error | Meaning |
|---|---|
| `ErrInvalidWorker` | Missing/invalid worker ID or nil handler passed to `New` |
| `ErrInvalidJob` | Malformed job encountered during a transition |
| `ErrOwnershipLost` | Fencing-token check failed — another party now owns the job |
| `ErrInvalidFencing` | Fencing token mismatch on a transition attempt |

## Reachability

Like the scheduler, `internal/worker` is a library (~10 unit tests, plus
the chaos tests referenced above) meant to be embedded by a consumer
process alongside a `raft.RaftNode`, `kv.Store`, and `kv.Applier` —
`cmd/raftiq` does not instantiate one.
