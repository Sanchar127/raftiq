# Testing

RaftIQ's correctness claims rest on its test suite more than on the
implementation matching the Raft paper by inspection. This document
explains what each layer of tests actually covers, with real numbers as of
this audit (counts of `func Test...` per package — re-run the `grep` below
yourself if this drifts).

```bash
grep -rl "^func Test" internal/*/*_test.go client/*_test.go tests/chaos/*.go
```

## Unit tests

| Package | Approx. test count | What it covers |
|---|---:|---|
| `internal/raft` | ~150 | Election, PreVote, replication, commit, snapshots, membership/joint consensus, restarts, ReadIndex |
| `internal/transport` | ~58 | gRPC Raft/KV services, TLS config loading, server lifecycle |
| `internal/observability` | ~49 | Metrics registration/values, logging, health checks |
| `internal/kv` | ~38 | Command encode/decode, applier ordering, store snapshotting |
| `internal/storage` | ~29 | WAL record encode/decode, recovery, corruption/truncation, metrics |
| `client` | ~15 | gRPC client dial/Get/Put/Delete behavior |
| `internal/server` | ~13 | Get/Put/Delete/AcquireLock/FencedPut wiring |
| `internal/scheduler` | ~14 | Job scheduling, worker selection, reclaim-on-expiry |
| `internal/worker` | ~10 | Job execution loop, handler invocation, transitions |
| `internal/lock` | ~3 | Fencing-token acquire/expire state transitions |

(Counts are `func Test` occurrences, not sub-test/table-test cases, which
are far more numerous in practice — e.g. `internal/raft/node_test.go` has
extensive table-driven sub-tests within many of its ~150 top-level funcs.)

## What the Raft tests actually exercise

Notable specific tests worth knowing about when you touch consensus code:

- **Election correctness**: `TestThreeNodeElection`,
  `TestVoteFromOldElectionIsIgnored`, `TestStaleElectionVoteReplyIsIgnored`.
- **PreVote**: `TestPreVoteRejectsStaleCandidateLog`,
  `TestPreVoteRejectsWhenRecentLeaderExists`,
  `TestPreVoteGrantsAfterElectionTimeout`, `TestPreVoteRejectsNonVoter`.
- **Linearizable reads**: `TestReadIndexSingleNode`, `TestReadIndexQuorum`,
  `TestReadIndexNoQuorum`, `TestReadIndexHigherTermReply`.
- **Snapshots**: `TestCreateSnapshotRejectsUnappliedIndex`,
  `TestCreateSnapshotRejectsUncommittedIndex`,
  `TestInstallSnapshotPersistsHigherTerm`,
  `TestInstallSnapshotRestoresStateMachine`.
- **Membership/joint consensus**: `TestJointConfigurationTransition`,
  `TestAddMember`, `TestRemoveMember`,
  `TestRemoveMemberRejectsLeaderSelfRemoval`,
  `TestLeaderFailureDuringJointConsensus`,
  `TestRestartDuringJointConsensus`, `TestRestartAfterLeaveJoint`,
  `TestRestartedJointMembershipPreservesQuorumSafety`,
  `TestRemovedLeaderCannotStartElection`,
  `TestAddMemberFailsSafelyWhenNewPeerBecomesUnreachable`.

## Integration tests

`internal/transport/grpc_integration_test.go` and
`grpc_raft_cluster_test.go` spin up real gRPC servers on loopback and
exercise multi-node Raft over the actual wire protocol, not
`LocalTransport` — this is what validates the proto encoding and gRPC
service layer, as distinct from the pure-Go consensus logic.

## Race tests

```bash
go test -race ./...
```

Required for anything touching `RaftNode` state, `Log`, or `WALStorage`
concurrency — these types are accessed from multiple goroutines (the
driver loop, per-peer RPC goroutines, the applier goroutine) and a data
race there is a correctness bug, not just a lint warning.
`tests/chaos/race_stress_test.go` (`TestRaceStress`) specifically
stress-tests concurrent access patterns under `-race`.

## Chaos tests (`tests/chaos/`)

| Test | Scenario |
|---|---|
| `TestLeaderKill` | Leader process/node is killed; cluster must elect a new leader and continue |
| `TestFollowerKill` / `TestFollowerRecovery` | A follower is killed and later rejoins/catches up |
| `TestNetworkPartition` / `TestAsymmetricNetworkPartition` | Minority side must not make committed progress |
| `TestLinearizability` | Concurrent client ops under failures/restarts must remain linearizable |
| `TestStaleFencingTokenRejectsZombieWorker` | An old fencing token must be rejected after a newer one is issued |
| `TestZombieWorkerCannotCompleteReclaimedJob` | A worker that lost its job to reclaim can't complete it after the fact |
| `TestRaceStress` | Concurrent operations under `-race`, no data races |

These use `LocalTransport`'s partition-simulation hooks
(`Block`/`Unblock`/`BlockBidirectional`), not real network manipulation —
so "network partition" here means "the in-process transport refuses to
deliver messages between these two node IDs," which is a faithful model of
message loss but not of, say, TCP-level slow-start effects.

## Failure injection mechanisms

- `LocalTransport.Block(from, to)` / `BlockBidirectional(a, b)` — drop RPCs
  between specific nodes.
- Stopping a `RaftNode`'s driver goroutine (`Stop()`) and later restarting
  a fresh node backed by the same `WALStorage` path — simulates a crash
  and restart from durable state.
- Corrupting/truncating a WAL file on disk directly in
  `wal_test.go`/`wal_snapshot_test.go` to test recovery paths.

There is no dedicated `ENOSPC`/disk-full injection test at the time of this
audit — see the storage entry in [`AUDIT.md`](AUDIT.md).

## Recommended validation before a PR

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go test -v -race ./tests/chaos/...
```

CI (`.github/workflows/ci.yml`) runs the `gofmt -l` check, `go mod verify`,
`go mod tidy -diff`, `go vet`, `go test ./... -count=1`,
`go test -race ./... -count=1`, and a chaos-test step, on every push/PR to
`main`.
