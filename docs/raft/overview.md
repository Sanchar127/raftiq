# Raft: Overview

`internal/raft` implements the consensus core: leader election, log
replication, commit tracking, snapshotting, and membership changes. It has
no network or disk dependencies of its own — see
[`../architecture.md`](../architecture.md).

## Core types

| Type | File | Role |
|---|---|---|
| `RaftNode` | `node.go` | The state machine: role, term, log, replication state, driver loop |
| `Log` | `log.go` | In-memory log buffer with compaction/snapshot boundary |
| `PersistentState` | `internal/model/types.go` | Term, voted-for, membership — must survive a crash |
| `Transport` | `transport.go` | Interface `RaftNode` uses to call peers |
| `Storage` | `internal/storage/storage.go` | Interface `RaftNode` uses to persist state |
| `Metrics` | `metrics.go` | Interface for consensus-level observability |

## Node states

Three roles, matching the Raft paper: `Follower`, `Candidate`, `Leader`
(see the `Role` type and `State` in `internal/raft/state.go`/`types.go`).
A `PreVote` phase (`runPreVote`) runs before a node actually becomes a
candidate and bumps its term — see [`leader-election.md`](leader-election.md).

## The driver loop

`RaftNode.Start()` launches a single goroutine (`run()` → `runTick()`)
that calls `tick()` on an interval, which returns whether an election or a
heartbeat round is due. This is the only place elections/heartbeats are
triggered from real time; every other method is either an RPC handler
(called by `internal/transport`) or a client-facing call
(`Propose`/`ReadIndex`/`AddMember`/etc., called by `internal/server`).

## Sub-topics

- [`leader-election.md`](leader-election.md) — PreVote, RequestVote, term rules
- [`log-replication.md`](log-replication.md) — AppendEntries, log matching
- [`commitment.md`](commitment.md) — commit-index advancement, apply pipeline
- [`membership.md`](membership.md) — AddMember/RemoveMember basics
- [`joint-consensus.md`](joint-consensus.md) — the two-phase membership protocol
- [`snapshots.md`](snapshots.md) — CreateSnapshot/InstallSnapshot
- [`linearizable-reads.md`](linearizable-reads.md) — ReadIndex
- [`failure-recovery.md`](failure-recovery.md) — what happens when nodes crash/partition/restart

## Test coverage

`internal/raft/node_test.go` alone has roughly 150 top-level test
functions, using `LocalTransport` and `MemoryStorage` to simulate
multi-node scenarios without real network or disk. See
[`../testing.md`](../testing.md) for the breakdown.
