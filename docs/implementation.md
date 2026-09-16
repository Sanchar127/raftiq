# Implementation Walkthrough

A guided tour through the code, for someone about to make a change. For
"what exists" see [`AUDIT.md`](AUDIT.md); for "how the pieces fit" see
[`architecture.md`](architecture.md). This document is "where do I look."

## Starting point: `cmd/raftiq/main.go`

Read this first — it wires every long-lived component together in one
place: opens the WAL (`storage.OpenWAL`), builds the `Metrics` registry,
constructs `raft.NewRaftNodeWithStorage`, constructs `kv.NewStore()` and
`server.NewServer(node, kvStore)`, builds the peer `GRPCTransport`, calls
`node.BootstrapMembership()`, stands up two gRPC servers
(`transport.NewServer`) for Raft and KV, and a `promhttp` handler for
metrics. Everything else in the codebase exists to be called from, or
tested independently of, this wiring.

## `internal/raft/node.go` — the core

This is the largest file in the repository (~3,500 lines) and the one to
understand most carefully before touching consensus behavior.

- **State**: `RaftNode.state` holds `Persistent` (`model.PersistentState`:
  current term, voted-for, membership) and volatile fields (role, commit
  index, last applied, leader-only replication state). Persistent fields
  are written to `Storage` before being acted on further (see
  `persistStateLocked`).
- **Roles**: `Follower`, `Candidate`, `Leader`/`Pre-candidate`-equivalent
  handled via `runPreVote` before `startElection`. Transitions go through
  `becomeFollowerLocked` / `becomeLeaderLocked`, which reset the relevant
  volatile state.
- **The driver loop**: `Start()` launches `run()` → `runTick()`, which
  calls `tick()` every tick interval to decide whether an election or a
  heartbeat round is due (`electionDue`, `heartbeatDue`).
- **RPC handlers** (`RequestVote`, `PreVote`, `AppendEntries`,
  `InstallSnapshot`) are exported methods called directly by
  `transport.RaftService` — there is no separate RPC-to-domain mapping
  layer; the gRPC service methods just translate proto messages to/from
  the `raft` package's Go types and call these methods.
- **Client-facing entry points**: `Propose` (writes), `ReadIndex` (reads),
  `AddMember`/`RemoveMember` (membership), `CreateSnapshot`. These are the
  methods `internal/server.Server` calls.

## `internal/raft/log.go` — the log

A thin, mutex-guarded wrapper around `[]LogEntry` plus
`LastIncludedIndex`/`LastIncludedTerm` (the snapshot boundary). `Compact`
discards entries at or below a snapshot index; `RestoreSnapshot` resets the
log to start after an installed snapshot. `TruncateFrom` implements the
"leader's AppendEntries conflicts with my log" case — it must never be
called at or below the commit index (that would violate committed-entry
immutability).

## `internal/raft/config_entry.go` — membership as log entries

Membership changes are themselves special log entries, not out-of-band
control messages — this is what makes them replicated and crash-safe for
free. `EncodeConfigurationEntry`/`DecodeConfigurationEntry` handle plain
configuration entries; `EncodeEnterJointConfigurationEntry`/
`EncodeLeaveJointConfigurationEntry` handle the two-phase joint-consensus
entries. `IsConfigurationEntry` lets `applyCommitted` distinguish these
from ordinary KV command entries so `applyConfigurationEntryLocked` can
handle them specially instead of forwarding them to `kv.Applier`.

## `internal/storage/wal.go` — durability

See [`storage/wal.md`](storage/wal.md) for the byte-level format. In brief:
`OpenWAL` scans the file, decoding one framed record at a time
(`decodeRecord`), replaying `recordState`/`recordEntries`/`recordSnapshot`/
`recordReplaceSuffix` records into in-memory `state`/`entries`/`snapshot`
fields. A partially-written trailing record (crash mid-write) is detected
and the tail is truncated rather than surfaced as a fatal error.

## `internal/kv` — the state machine

`Command`/`EncodeCommand`/`DecodeCommand` (`command.go`) define the wire
format for `Propose`d log entry payloads (`CommandPut`, `CommandDelete`).
`Applier.Run` reads `LogEntry`s off `RaftNode.ApplyCh()` in order and calls
`Store.Put`/`Store.Delete` — this is the only path that's allowed to
mutate `Store`, which is what keeps replicas deterministic. `Store` also
implements snapshotting (`store_snapshot_test.go` covers
serialize/restore).

## `internal/server/server.go` — gluing it together

`Server` is what actually implements the "linearizable Get" pattern
(`ReadIndex` + `WaitApplied`) and the "propose + wait applied" pattern for
writes. It also owns `internal/lock.State` and implements
`AcquireLock`/`FencedPut`/the lock-expiration background worker — none of
which is exposed by `KVService`'s proto today (see AUDIT.md).

## `internal/transport` — the network boundary

`grpc_raft_transport.go` (`GRPCTransport`) is the *client* side used by a
leader/candidate to call peers; `raft_service.go` (`RaftService`) is the
*server* side that receives those calls and forwards them to a local
`RaftNode`. `kv_service.go` (`KVService`) does the same for the KV API.
`server.go` (`transport.Server`, distinct from `internal/server.Server`) is
a small wrapper around a `*grpc.Server` used for both listeners. `tls.go`
provides TLS/mTLS config loading, implemented and tested but not called
from `cmd/raftiq/main.go`.

## Where to look for a given change

| I want to change... | Start here |
|---|---|
| Election/voting rules | `internal/raft/node.go`: `startElection`, `runElection`, `RequestVote`, `PreVote` |
| Replication/log matching | `internal/raft/node.go`: `replicateTo`, `AppendEntries`, `validateAppend` |
| Commit rules | `internal/raft/node.go`: `advanceCommitIndexLocked` |
| Snapshot behavior | `internal/raft/node.go`: `CreateSnapshot`, `InstallSnapshot`; `internal/raft/log.go`: `Compact` |
| Membership changes | `internal/raft/node.go`: `AddMember`, `RemoveMember`; `internal/raft/config_entry.go` |
| WAL format / recovery | `internal/storage/wal.go` |
| KV commands | `internal/kv/command.go`, `internal/kv/applier.go` |
| Locking / fencing | `internal/lock/types.go`, `internal/server/server.go` |
| Scheduling | `internal/scheduler/scheduler.go` |
| Worker execution | `internal/worker/worker.go` |
| gRPC wire format | `api/proto/raftiq.proto` (regenerate with `protoc`) |
| TLS | `internal/transport/tls.go` |
| Metrics | `internal/observability/metrics.go` + the per-package `Metrics` interfaces (e.g. `internal/raft/metrics.go`) |
