# RaftIQ Observability & Chaos Engineering

## Observability

RaftIQ exposes operational telemetry for consensus, storage, KV operations, scheduling, and workers.

---

## Important Metrics

| Metric                             | Type      | Description               |
| ---------------------------------- | --------- | ------------------------- |
| `raftiq_raft_state`                | Gauge     | Current Raft role         |
| `raftiq_raft_term`                 | Gauge     | Current Raft term         |
| `raftiq_raft_elections_total`      | Counter   | Elections started         |
| `raftiq_raft_rpc_latency_seconds`  | Histogram | Raft RPC latency          |
| `raftiq_wal_write_latency_seconds` | Histogram | WAL write latency         |
| `raftiq_wal_sync_latency_seconds`  | Histogram | WAL sync latency          |
| `raftiq_kv_operations_total`       | Counter   | KV mutations              |
| `raftiq_scheduler_tasks_active`    | Gauge     | Active tasks              |
| `raftiq_worker_heartbeat_total`    | Counter   | Worker heartbeat activity |

Metric names should match the actual implementation when dashboards are generated.

---

## Health Checks

Operational endpoints should distinguish between:

```text
Health
  |
  +-- Process is alive

Readiness
  |
  +-- Node is capable of serving its intended role
```

A live process is not necessarily a ready consensus node.

---

# Chaos Engineering

The chaos suite validates behavior under failure rather than only successful execution.

---

## Leader Failure

```text
Leader
  |
  X
Crash
  |
  v
Election
  |
  v
New Leader
  |
  v
Replication Continues
```

The test verifies that the cluster can recover leadership while preserving committed state.

---

## Follower Failure

A follower is terminated while the remaining quorum continues operating.

The failed follower is later restarted and must catch up from the replicated log or snapshot path as appropriate.

---

## Network Partition

The cluster is divided into groups.

```text
       Network Partition

+---------+       X       +---------+
| Majority|               | Minority|
|         |               |         |
| Leader  |               | Old Node|
+---------+               +---------+
```

The minority must not be able to commit operations without the required quorum.

---

## Stale Fencing

The test intentionally delays or isolates an old worker.

```text
Worker A
 Token 10
    |
    X
 Network Partition
    |
Worker B
 Token 11
    |
    v
New Ownership

Worker A returns
    |
    | Token 10
    v
Rejected
```

---

## Linearizability

Concurrent operations are issued while nodes experience failures and restarts.

The test validates that completed operations maintain a valid real-time ordering consistent with the replicated state machine.

---

## Recovery Testing

Nodes are repeatedly:

```text
Start
  |
  v
Operate
  |
  v
Crash
  |
  v
Restart
  |
  v
Recover WAL
  |
  v
Rejoin Cluster
```

The recovered node must preserve the required Raft and state-machine invariants.

---

## Race Detection

The Go race detector is used for concurrency validation:

```bash
go test -race ./...
```

The goal is to detect unsafe concurrent access across:

* Raft state
* Storage
* KV state
* Scheduler
* Workers
* Transport
* Metrics
