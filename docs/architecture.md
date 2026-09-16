# Architecture

See [`AUDIT.md`](AUDIT.md) for the evidence behind every status claim in
this document.

## Executive summary

RaftIQ separates four concerns into independent, interface-bound
subsystems: consensus (`internal/raft`), durable storage
(`internal/storage`), the deterministic state machine
(`internal/kv`, `internal/lock`), and network transport
(`internal/transport`). `internal/server` composes these into the process
that `cmd/raftiq` actually runs.

> Consensus decides *what* state transition is committed. The state
> machine decides *how* that committed transition changes application
> state. `internal/raft` has no knowledge of what a log entry's bytes mean.

## Component diagram

```mermaid
graph TD
    subgraph Network
        Client[gRPC Client]
        Peer[Raft Peer Node]
    end

    subgraph Process["raftiq process (cmd/raftiq)"]
        KVSvc[transport.KVService]
        RaftSvc[transport.RaftService]
        AppServer[server.Server]
        RaftNode[raft.RaftNode]
        GRPCT[transport.GRPCTransport]
        KVStore[kv.Store]
        Applier[kv.Applier]
        LockState[lock.State]
        WAL[storage.WALStorage]
        Metrics[observability.Metrics]
    end

    Client -->|Get/Put/Delete| KVSvc
    Peer -->|RequestVote/PreVote/AppendEntries/InstallSnapshot| RaftSvc
    KVSvc --> AppServer
    RaftSvc --> RaftNode
    AppServer -->|Propose/ReadIndex| RaftNode
    AppServer --> KVStore
    AppServer --> LockState
    RaftNode -->|ApplyCh| Applier --> KVStore
    RaftNode --> WAL
    RaftNode --> GRPCT --> Peer
    RaftNode -.metrics.-> Metrics
```

## Process and package boundaries

A single `raftiq` process (`cmd/raftiq/main.go`) runs:

1. One `raft.RaftNode`, backed by one `storage.WALStorage` on `-data-dir`.
2. One `server.Server`, which owns the `kv.Store`, the `kv.Applier`
   (consuming `RaftNode.ApplyCh()`), and the `lock.State` used by
   `AcquireLock`/`FencedPut`.
3. Two gRPC listeners: `-raft-addr` (peer-to-peer `RaftService`) and
   `-kv-addr` (client-facing `KVService`).
4. One HTTP listener on `-metrics-addr` serving `/metrics`.

`internal/raft` is intentionally the only package with zero I/O
dependencies — it depends on `internal/model` and the `Storage`/`Transport`
interfaces it defines itself, nothing else. Every other package either
implements one of those interfaces (`storage.WALStorage`,
`transport.GRPCTransport`) or sits above `raft.RaftNode` and reacts to
`ApplyCh` / calls `Propose`/`ReadIndex` (`kv.Applier`, `server.Server`).

`internal/scheduler` and `internal/worker` are peers of `internal/kv` in
this model — libraries meant to sit above a replicated log — but
`cmd/raftiq` does not currently instantiate them. See
[`components/scheduler.md`](components/scheduler.md).

## Data flow: a write (Put)

```mermaid
sequenceDiagram
    participant C as Client
    participant Svc as transport.KVService
    participant Srv as server.Server
    participant Raft as raft.RaftNode
    participant WAL as storage.WALStorage
    participant App as kv.Applier

    C->>Svc: Put(key, value)
    Svc->>Srv: Put(ctx, key, value)
    Srv->>Srv: kv.EncodeCommand(CommandPut)
    Srv->>Raft: Propose(commandData)
    Raft->>WAL: AppendEntries([entry])
    Raft-->>Raft: replicate to followers, advance commitIndex
    Raft-->>App: entry delivered on ApplyCh once committed
    App->>App: apply to kv.Store in order
    Srv->>Srv: applier.WaitApplied(ctx, index)
    Srv-->>Svc: nil error
    Svc-->>C: PutResponse{}
```

If the node is not the leader, `RaftNode.Propose` returns an error and the
RPC fails — there is no leader-redirect payload in the response (see
[`AUDIT.md`](AUDIT.md)).

## Data flow: a linearizable read (Get)

```mermaid
sequenceDiagram
    participant C as Client
    participant Svc as transport.KVService
    participant Srv as server.Server
    participant Raft as raft.RaftNode
    participant KV as kv.Store

    C->>Svc: Get(key)
    Svc->>Srv: Get(ctx, key)
    Srv->>Raft: ReadIndex(ctx)
    Raft-->>Raft: confirm leadership via heartbeat quorum
    Raft-->>Srv: index
    Srv->>Srv: applier.WaitApplied(ctx, index)
    Srv->>KV: Get(key)
    KV-->>Srv: value, found
    Srv-->>Svc: value, found
    Svc-->>C: GetResponse
```

See [`raft/linearizable-reads.md`](raft/linearizable-reads.md) for why
`ReadIndex` + `WaitApplied` together are required for linearizability.

## Failure handling, at a glance

- **Leader failure:** followers' election timers fire, PreVote + election
  run, a new leader is chosen from the majority with the most up-to-date
  log. See [`raft/leader-election.md`](raft/leader-election.md) and
  [`raft/failure-recovery.md`](raft/failure-recovery.md).
- **Follower failure:** the leader keeps committing as long as a majority
  of voters is reachable; a recovering follower catches up via
  `AppendEntries` backfill or `InstallSnapshot` if it's too far behind.
- **Network partition:** minority-side nodes cannot reach election majority
  or commit majority, so they cannot make progress; see
  `tests/chaos/network_partition_test.go`.
- **Crash / restart:** `storage.WALStorage.OpenWAL` replays valid records
  and truncates an incomplete trailing record. See
  [`persistence.md`](persistence.md) and [`storage/wal.md`](storage/wal.md).

## Observability

`internal/observability` provides Prometheus metrics
(`raftiq_raft_*`, `raftiq_rpc_*`, `raftiq_storage_*`, `raftiq_kv_*`,
scheduler/lease metrics), served at `/metrics` by `main.go`. Health and
readiness HTTP handlers exist in `internal/observability/health.go` and
`raft_readiness.go` but are not registered by `main.go` — see
[`components/observability.md`](components/observability.md).
