# Component: Distributed Lock (Fencing Tokens)

`internal/lock` is a small, in-memory lock table with monotonically
increasing fencing tokens. It's embedded inside `kv.Store`
(`Store.locks *lock.State`) and mutated only via replicated `Command`s —
so, unlike a naive "local mutex per node" implementation, a lock granted
by RaftIQ is agreed on by the whole cluster, not just the node a client
happened to talk to.

## `lock.State` (`types.go`)

```go
type Lock struct {
	Key          string
	OwnerID      string
	FencingToken uint64
	ExpiresAt    int64
	GrantIndex   model.LogIndex
}

type State struct {
	Locks     map[string]Lock
	NextToken uint64
}
```

- `Acquire(key, ownerID, expiresAt, grantIndex)` — grants the lock only if
  `key` isn't currently held; assigns the next fencing token
  (`NextToken++`, so tokens are strictly increasing and never reused).
  Returns `(Lock{}, false)` — not an error — if the key is already held;
  the caller is expected to treat "not acquired" as a normal outcome, not
  a failure.
- `Expire(key, expectedToken)` — removes the lock **only if** the current
  fencing token still matches `expectedToken`. This guards against
  expiring a *newer* grant of the same key by an in-flight expiration
  command that was proposed against an older grant — see
  `internal/server.Server.expireLocks`/`proposeLockExpiration` below.
- `Get(key)` — read the current holder, if any.

## How a lock is actually acquired end-to-end

1. `server.Server.AcquireLock(ctx, key, ownerID, leaseMillis)` computes an
   absolute `expiresAt`, encodes `kv.Command{Type: CommandLockAcquire, ...}`,
   and **proposes it through Raft** (`s.raft.Propose`) — same path as an
   ordinary KV write.
2. Waits for that index to be applied (`applier.WaitApplied`).
3. Reads back `s.store.GetLock(key)` and checks the grant actually matches
   this caller (`current.GrantIndex == index && current.OwnerID ==
   ownerID`) — if someone else's `AcquireLock` command for the same key
   got applied first (a race between two callers proposing concurrently),
   this one returns `lock.ErrLockBusy` rather than falsely claiming
   success.

Because acquisition goes through the replicated log, every node's
`kv.Store.locks` converges on the same grant — there's no
split-brain risk from two nodes independently granting the same lock.

## Fenced writes (`FencedPut`)

`server.Server.FencedPut`/`Store.FencedPut` let a lock holder write a
value that's rejected if their fencing token is no longer current (e.g.
their lease expired and someone else re-acquired the lock in the
meantime). This is the standard fencing-token pattern for safely
coordinating an external resource (e.g. writes to a file, a device, an
external system) even when a "stale" holder is still alive and believes
it holds the lock — the resource-side check (comparing the presented
token against the last-seen token) is what actually provides the safety
property; RaftIQ's job is just to hand out strictly increasing tokens
correctly, which `NextToken++` under Raft-ordered application guarantees.

## Lock expiration

`server.Server.runLockExpirationWorker()` is a background goroutine
(started by `Server.Start()`) that periodically calls `expireLocks()`,
which scans `s.store.ListLocks()` for leases past `ExpiresAt` and, for
each, proposes a `CommandLockExpire` command
(`proposeLockExpiration`) — again through Raft, not a local mutation —
guarded by `markExpirationPending`/`clearExpirationPending` to avoid
proposing duplicate expiration commands for the same lock while one is
already in flight.

## Reachability

Everything above is real, replicated, tested logic
(`internal/lock`'s own tests plus `internal/server/server_test.go`). It's
**not exposed as a gRPC RPC** — `api/proto/raftiq.proto`'s `KVService`
only has `Get`/`Put`/`Delete`. To use locking today you call
`server.Server.AcquireLock`/`FencedPut` directly from Go code that embeds
`internal/server.Server`, not from the `client` gRPC package. See
[`../AUDIT.md`](../AUDIT.md).
