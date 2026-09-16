# RaftIQ Implementation Audit

This document records what was actually found in the codebase during the
documentation pass (September 2026), as opposed to what prior README/docs
text claimed. It exists so future contributors can see the evidence behind
the rest of `docs/`, and so documentation claims can be re-verified as the
code changes. Treat "implementation + tests" as ground truth over comments
or prior prose.

## Method

Every claim below is backed by a file/type/function/test reference. Nothing
here is inferred from what a "typical Raft system" would have — if a
feature isn't reachable from `cmd/raftiq/main.go`, a test, or an exported
API, it is marked accordingly rather than assumed.

## Implementation inventory

### Raft consensus core — IMPLEMENTED
- `internal/raft/node.go` — `RaftNode`: `RequestVote`, `PreVote`, `AppendEntries`,
  `InstallSnapshot`, `Propose`, `startElection`, `runElection`, `runPreVote`,
  `advanceCommitIndexLocked`, `applyCommitted`, `Tick`/`run`/`runTick`.
- Election safety, PreVote (to avoid disruptive elections from partitioned
  nodes), leader-completeness-preserving vote rules, and log-matching checks
  are all present and exercised by ~150 tests in `internal/raft/node_test.go`.
- Heartbeats and replication: `sendHeartbeats`, `replicateTo`,
  `handleAppendEntriesReply`, `buildAppendEntries`.
- Tests: `TestThreeNodeElection`, `TestPreVoteRejectsStaleCandidateLog`,
  `TestPreVoteRejectsWhenRecentLeaderExists`, `TestVoteFromOldElectionIsIgnored`,
  `TestStaleElectionVoteReplyIsIgnored`, and the full election-timeout suite.

### Linearizable reads (ReadIndex) — IMPLEMENTED
- `internal/raft/node.go` — `RaftNode.ReadIndex(ctx)`.
- Wired into the client-facing read path in `internal/server/server.go`
  (`Server.Get` calls `ReadIndex` then `applier.WaitApplied`).
- Tests: `TestReadIndexSingleNode`, `TestReadIndexQuorum`,
  `TestReadIndexNoQuorum`, `TestReadIndexHigherTermReply`.

### Membership changes / joint consensus — IMPLEMENTED, but not network-reachable
- `internal/raft/node.go` — `AddMember(ctx, peerID)`, `RemoveMember(ctx, peerID)`.
- `internal/raft/config_entry.go` — `EncodeEnterJointConfigurationEntry`,
  `EncodeLeaveJointConfigurationEntry`, `DecodeEnterJointConfigurationEntry`,
  `DecodeLeaveJointConfigurationEntry`, `validateConfiguration`.
- `internal/raft/membership.go` — joint-quorum math (`membershipHasQuorum`,
  `configurationHasQuorum`).
- Failure/restart safety is heavily tested: `TestJointConfigurationTransition`,
  `TestAddMember`, `TestRemoveMember`, `TestRemoveMemberRejectsLeaderSelfRemoval`,
  `TestLeaderFailureDuringJointConsensus`, `TestRestartDuringJointConsensus`,
  `TestRestartAfterLeaveJoint`, `TestRestartedJointMembershipPreservesQuorumSafety`,
  `TestRemovedLeaderCannotStartElection`.
- **Gap:** `AddMember`/`RemoveMember` are Go-level `RaftNode` methods only.
  Neither `api/proto/raftiq.proto` nor `cmd/raftiq/main.go` exposes them —
  there is no RPC or CLI flag to trigger a membership change against a
  running cluster. A caller must be embedding `raft.RaftNode` directly in
  Go. This is a real gap between "implemented" and "operable."

### Persistence / WAL — IMPLEMENTED
- `internal/storage/wal.go` — `WALStorage`, `OpenWAL`, framed records
  (`recordState`, `recordEntries`, `recordSnapshot`, `recordReplaceSuffix`),
  CRC32 footer, `recover()`, `ReplaceSuffix`, `Sync`.
- Crash-tail handling: `OpenWAL`'s doc comment and `recover()` truncate an
  incomplete trailing record rather than failing to start.
- Tests: `TestWALStorageRecoversValidRecordsBeforeTruncatedTail`,
  `TestWALStorageRejectsCorruptedRecordOnRecovery`,
  `TestWALStorageReplaceSuffixTruncatesOnly`, plus `wal_snapshot_test.go`
  and `wal_metrics_test.go`.
- `internal/storage/memory.go` — `MemoryStorage`, an in-memory implementation
  of the same `Storage` interface, used in unit tests and available for
  non-durable/local runs.
- `ErrWALDiskFull` is a defined sentinel in `internal/storage/wal.go`, but
  the only exercised disk-pressure path in tests is the corrupted/truncated
  tail case, not an actual `ENOSPC` simulation — treat "disk full" handling
  as PARTIALLY IMPLEMENTED (the error exists and is returned on write
  failure, but there's no dedicated chaos test forcing ENOSPC).

### Snapshots — IMPLEMENTED
- `RaftNode.CreateSnapshot`, `RaftNode.InstallSnapshot`,
  `RaftNode.SetSnapshotRestore`, `Log.Compact`, `Log.RestoreSnapshot`.
- Tests: `TestCreateSnapshot`, `TestCreateSnapshotRejectsUnappliedIndex`,
  `TestCreateSnapshotRejectsUncommittedIndex`, `TestCreateSnapshotRejectsIndexZero`,
  `TestInstallSnapshotPersistsHigherTerm`, `TestInstallSnapshotRestoresStateMachine`.

### KV state machine — IMPLEMENTED
- `internal/kv/store.go`, `internal/kv/applier.go`, `internal/kv/command.go`
  (`Command`, `EncodeCommand`, `CommandPut`/`CommandDelete`).
- Applied strictly in committed order via `Applier.Run(ctx, node.ApplyCh())`.
- Reachable over gRPC: `internal/transport/kv_service.go` (`Get`/`Put`/`Delete`),
  wired in `cmd/raftiq/main.go`.
- **Gap:** `KVService`'s proto responses (`GetResponse`, `PutResponse`,
  `DeleteResponse` in `api/proto/raftiq.proto`) carry no leader-hint field.
  A non-leader node returns a plain gRPC error, not a redirect payload.
  The former README claim that "followers can return leader information so
  clients can redirect" is **not accurate** for the network-facing service —
  it describes desired behavior, not implemented behavior.

### Distributed locking / fencing — IMPLEMENTED in-process, not exposed over gRPC
- `internal/lock/types.go` — `State`, `Lock`, `Acquire`, `Expire`.
- `internal/server/server.go` — `Server.AcquireLock`, `Server.FencedPut`,
  `runLockExpirationWorker`, `expireLocks`, `proposeLockExpiration`.
- **Gap:** `api/proto/raftiq.proto` has no lock RPCs (`KVService` only
  defines `Get`/`Put`/`Delete`). Locking/fencing is reachable from Go code
  embedding `internal/server.Server`, but not from the gRPC client
  (`client/` package) or the `raftiq` binary's network surface.

### Scheduler and worker — IMPLEMENTED as standalone packages, NOT wired into the binary
- `internal/scheduler/scheduler.go` — `Scheduler`, `HashWorkerSelector`,
  `scheduleDueJobs`, `reclaimExpiredJobs`.
- `internal/worker/worker.go` — `Worker`, `JobHandler`, `JobSource`,
  `ConfigureLoop`, `executeAssignedJobs`, `transitionJob`.
- Both have real unit tests (14 and 10 `Test*` functions respectively) and
  chaos coverage: `TestStaleFencingTokenRejectsZombieWorker`,
  `TestZombieWorkerCannotCompleteReclaimedJob`.
- **Gap:** `cmd/raftiq/main.go` never imports `internal/scheduler` or
  `internal/worker`, and there is no CLI flag to start a scheduler loop or
  register a worker. These packages are usable libraries against the
  `kv`/`raft` state machine, but the shipped `raftiq` binary does not run a
  scheduler or worker process. Prior docs describing "Distributed Job
  Scheduler" as a running subsystem overstated this — it is implemented and
  tested code that the binary does not currently start.

### Transport — gRPC IMPLEMENTED, TLS/mTLS IMPLEMENTED but NOT wired into the CLI
- `internal/transport/grpc_raft_transport.go` — `GRPCTransport` (client side
  of Raft RPCs), `internal/transport/raft_service.go` — `RaftService`
  (server side), `internal/transport/server.go` — `Server` (generic gRPC
  listener used for both the Raft and KV ports).
- `internal/raft/local_transport.go` — `LocalTransport`, an in-process
  transport with `Block`/`Unblock`/`BlockBidirectional` used to simulate
  network partitions in tests (`tests/chaos/network_partition_test.go`).
- `internal/transport/tls.go` — `LoadTLSConfig`, `LoadTLSServerConfig`,
  `LoadTLSClientConfig`, SAN verification (`verifyPeerSAN`/`verifyPeerSANs`).
  Fully implemented and unit-tested (`tls_test.go`).
- **Gap:** `cmd/raftiq/main.go` has no `-tls-*` flags and never calls
  `SetTLSConfig`/`LoadTLSServerConfig`. TLS/mTLS exists as a library
  capability but every node started via the shipped binary runs in
  plaintext gRPC. This is the most operationally significant gap found.

### Cluster bootstrap / membership at startup — IMPLEMENTED, static only
- `cmd/raftiq/main.go` builds a fixed peer set from `-peers id=host:port,...`
  and calls `node.BootstrapMembership()` once at startup.
- There is no dynamic "join an existing cluster" flag; adding a node to a
  running cluster requires driving `RaftNode.AddMember` from Go code (see
  membership gap above).

### Observability — IMPLEMENTED
- `internal/observability/metrics.go` defines Prometheus metrics under the
  `raftiq_raft_*`, `raftiq_rpc_*`, `raftiq_storage_*`, `raftiq_kv_*`, and
  scheduler/lease metric families (namespace `raftiq`).
- `cmd/raftiq/main.go` serves them on `-metrics-addr` (default `:9090`) at
  `/metrics` via `promhttp`.
- `deploy/grafana/dashboards/raftiq-overview.json` and
  `deploy/grafana/provisioning/*` provide a matching dashboard and
  datasource config. `deploy/prometheus/` and `deploy/docker/` exist as
  empty directories (`.gitkeep` only) — no scrape config or Dockerfile is
  actually present despite the directories existing.
- Health/readiness: `internal/observability/health.go`, `raft_readiness.go`,
  `http.go` — present and tested, but **not wired into `main.go`** (no
  `/healthz` or `/readyz` HTTP handler is registered there; only `/metrics`
  is).

### Configuration — CLI flags only, no config file, no `internal/config` package
- `cmd/raftiq/main.go`: `-id`, `-raft-addr`, `-kv-addr`, `-metrics-addr`,
  `-data-dir`, `-peers`, `-heartbeat`, `-election`, `-log-level`.
- No YAML/JSON/env-based configuration loader exists in the repository
  despite `docs/configuration.md` being a natural place to expect one; a
  prior high-level plan may have envisioned `internal/config`, but no such
  package exists in this tree.

### Docker — directory scaffolding only, NOT IMPLEMENTED
- `deploy/docker/` contains only `.gitkeep`. No `Dockerfile` or
  `docker-compose.yml` exists anywhere in the repository (`.dockerignore`
  exists at the root, implying Docker support was planned).

### CI — IMPLEMENTED
- `.github/workflows/ci.yml`: `gofmt -l`, `go mod verify`, `go mod tidy -diff`,
  `go vet`, `go test ./... -count=1`, `go test -race ./... -count=1`, and a
  chaos-test step. `.golangci.yml` configures `golangci-lint` (invoked via
  `make lint`, not currently in CI as inspected).

### Legal/community files — INCOMPLETE
- `LICENSE` exists as a file but is **empty (0 bytes)**. The README and its
  badge claim "MIT License," but no license text is actually committed.
  This is a real, user-facing inconsistency — a clone of this repository
  today has no enforceable license despite the README's claim.
- `SECURITY.md` and `CODE_OF_CONDUCT.md` both exist as empty files (0 bytes).
- `CONTRIBUTING.md` referred to the project as `raftkv` (old name), pointed
  at `github.com/sanchar127/raftkv.git` (wrong repo), and referenced
  `internal/raft/raft_test.go`, which does not exist — the actual test file
  is `internal/raft/node_test.go`. Corrected in this pass.

## Summary table

| Subsystem | Status | Network-reachable? |
|---|---|---|
| Leader election / PreVote / log replication | Implemented | Yes (Raft gRPC) |
| Linearizable reads (ReadIndex) | Implemented | Yes (KV `Get`) |
| Snapshots / InstallSnapshot | Implemented | Yes (Raft gRPC) |
| Membership changes / joint consensus | Implemented | **No** (Go API only) |
| WAL persistence & recovery | Implemented | n/a (local disk) |
| KV store (Put/Get/Delete) | Implemented | Yes |
| Leader-redirect on KV writes | Not implemented | — |
| Distributed locking & fencing | Implemented | **No** (Go API only) |
| Scheduler | Implemented, tested | **No** (not started by `main.go`) |
| Worker | Implemented, tested | **No** (not started by `main.go`) |
| gRPC transport | Implemented | Yes |
| TLS / mTLS | Implemented | **No** (not wired into CLI) |
| Prometheus metrics | Implemented | Yes (`/metrics`) |
| Health/readiness HTTP endpoints | Implemented | **No** (not registered in `main.go`) |
| Config file support | Not implemented | — |
| Docker packaging | Not implemented | — |
| License text | Missing | — |
| SECURITY.md / CODE_OF_CONDUCT.md content | Missing (now added) | — |

The rest of `docs/` is written to be honest about this table: subsystems
that exist only as Go APIs are documented as such, not presented as
cluster-operable features.
