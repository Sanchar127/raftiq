# Component: KV Store

`internal/kv` is the deterministic state machine that sits on top of
`raft.RaftNode`. It's larger in scope than "just a key-value store" — the
same `Store` and `Command` machinery also backs locking and job-lifecycle
state, since all of it needs to go through the same replicated,
deterministically-applied log.

## `Store` (`store.go`)

```go
type Store struct {
	data   map[string][]byte
	locks  *lock.State
	fenced map[string]FencedValue
	jobs   map[model.JobID]model.Job
	// ...
}
```

One struct, four logical sub-stores, all guarded by one `sync.RWMutex`
(`s.mu`) and all mutated only from the applier goroutine (see below) so
every replica converges on identical state:

- **Plain KV** — `Get`/`Put`/`Delete`, the network-reachable Get/Put/Delete
  path.
- **Locks** — embeds an `internal/lock.State` (see
  [`lock.md`](lock.md)); `AcquireLock`/`GetLock`/`ExpireLock`/`ListLocks`.
- **Fenced values** — `FencedPut`/`GetFenced`: a value write that also
  requires a valid, current fencing token, rejecting writes from a holder
  whose lock has since expired or been re-granted to someone else.
- **Jobs** — `jobs map[model.JobID]model.Job`, mutated by the
  scheduler/worker command types (`CLAIM_JOB`, `JOB_START`,
  `JOB_SUCCEEDED`, `JOB_FAILED`, `JOB_RECLAIM`) — see
  [`scheduler.md`](scheduler.md) and [`worker.md`](worker.md). This state
  exists and is fully wired into the state machine even though nothing in
  `cmd/raftiq` currently drives it.

`Snapshot()`/`Restore(data)` serialize/deserialize the entire `Store`
(JSON-encoded envelope, `snapshotEnvelope`/`snapshotState`) — this is what
`RaftNode.CreateSnapshot`/`InstallSnapshot`'s registered hooks actually
call. See [`../raft/snapshots.md`](../raft/snapshots.md).

## Commands (`command.go`)

```go
type CommandType string

const (
	CommandPut         CommandType = "PUT"
	CommandDelete      CommandType = "DELETE"
	CommandReadBarrier CommandType = "READ_BARRIER"
	CommandLockAcquire CommandType = "LOCK_ACQUIRE"
	CommandLockExpire  CommandType = "LOCK_EXPIRE"
	CommandFencedPut   CommandType = "FENCED_PUT"
	CommandClaimJob     CommandType = "CLAIM_JOB"
	CommandJobReclaim   CommandType = "JOB_RECLAIM"
	CommandJobStart     CommandType = "JOB_START"
	CommandJobSucceeded CommandType = "JOB_SUCCEEDED"
	CommandJobFailed    CommandType = "JOB_FAILED"
)
```

A `Command` is JSON-encoded (`EncodeCommand`/`DecodeCommand`) and used as
the payload of a `Propose`d `raft.LogEntry`. This is the *only* format the
applier understands — anything proposed to `RaftNode` outside this
encoding (e.g. a raw membership entry) is filtered out before reaching the
KV applier via `IsConfigurationEntry` (see
[`../raft/commitment.md`](../raft/commitment.md)).

## `Applier` (`applier.go`)

`NewApplier(store)` + `Applier.Run(ctx, node.ApplyCh())` is the single
goroutine allowed to mutate `Store` — it reads committed `raft.LogEntry`s
off the channel in order and calls `ApplyWithMetrics(store, entry,
metrics)`, which decodes the `Command` and dispatches to the matching
`Store` method. This ordering guarantee (apply strictly in committed
index order, one entry at a time, from one goroutine) is what makes every
replica's `Store` converge to identical state.

- `WaitApplied(ctx, index)` — blocks until `lastApplied >= index`; used by
  `server.Server` after `Propose`/`ReadIndex` to make writes/reads wait for
  local application (see [`../raft/linearizable-reads.md`](../raft/linearizable-reads.md)).
- `WaitResult(ctx, index)` — like `WaitApplied`, but also returns the
  `ApplyResult` (value/error) produced by that specific command, used for
  commands like `CommandFencedPut` where the caller needs to know if the
  fencing check itself succeeded, not just that *something* was applied.
- `RestoreSnapshot(snapshot)` — called by `server.Server.Start()` at
  startup if a snapshot was recovered, before normal operation begins.

## Reachability

`Get`/`Put`/`Delete` are reachable over gRPC via `KVService`. Locking,
fenced writes, and job commands are **not** — there is no RPC for
`AcquireLock`/`FencedPut`/job commands in `api/proto/raftiq.proto`. They're
reachable only by calling `server.Server`'s Go methods directly, or by
`Propose`-ing a `kv.Command` yourself against a `raft.RaftNode` you're
embedding. See [`../AUDIT.md`](../AUDIT.md).
