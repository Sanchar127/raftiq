# raftkv

**A Raft-consensus distributed key-value store, used to solve exactly-once job scheduling across service replicas.**

[![Go Reference](https://img.shields.io/badge/go-1.22-blue)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Status: In Development](https://img.shields.io/badge/status-in--development-orange)]()

---

## Why this exists

When you run multiple replicas of a backend service (the normal setup on Kubernetes/EKS), every replica is identical and independently capable of doing anything the service does. That's fine for handling API traffic, but it's a real problem for **scheduled or periodic jobs** — a nightly reconciliation job, a batch sweep, a cron-style task. Without coordination, every replica tries to run the job at the same time, causing duplicate work and, in transaction-sensitive systems, data corruption.

The common fix is a Redis-based lock (`SETNX ... EX ttl`). It mostly works, but it has a well-documented failure mode: if the lock holder stalls (GC pause, network blip) past its TTL, another replica takes over — and then the original replica wakes up, doesn't know it lost the lock, and finishes its own copy of the job anyway. Two executions, and nothing detected the conflict. This is the "zombie writer" problem, and it's a known critique of Redis-based locking (Redlock).

**raftkv solves this properly**, not with a longer timeout, but with:

1. A **real consensus protocol (Raft)** as the source of truth for who holds the lock — not a best-effort timer.
2. **Fencing tokens** — a monotonically increasing number issued with every lock grant. Any write from a stale holder is rejected because a newer token has already been issued, closing the zombie-writer hole entirely.

The result: given N identical replicas, a scheduled job runs **exactly once** — never zero times, never twice — even if the replica doing the work crashes or freezes mid-job.

Full design rationale, requirements, and architecture: see [`docs/design.md`](docs/design.md) (or the original project documentation).

---

## Project status

This repository is being built incrementally and in the open. Current state:

| Component | Status |
|---|---|
| Raft core (leader election, log replication, term/vote handling) | ✅ Implemented, unit tested |
| Write-ahead log (WAL) persistence + crash recovery | ✅ Implemented |
| Snapshotting / log compaction | ✅ Implemented |
| KV state machine | ✅ Implemented |
| Distributed lock service with fencing tokens | ✅ Implemented |
| gRPC transport (inter-node + client API) | 🚧 In progress |
| Client SDK | 🚧 Planned |
| Demo job-scheduler application | 🚧 Planned |
| Docker Compose 3-node cluster | 🚧 Planned |
| Chaos-test harness (leader kill, network partition) | 🚧 Planned |
| Prometheus + Grafana observability | 🚧 Planned |

The consensus core, storage layer, KV store, and lock service depend only on the Go standard library and are fully buildable and testable offline. The gRPC transport layer requires network access to fetch `google.golang.org/grpc` and `google.golang.org/protobuf` (see [Building](#building)).

---

## How it works

### Architecture

```
                     ┌─────────────────────────┐
                     │        Client            │
                     └────────────┬─────────────┘
                                  │ gRPC (KV / Lock API)
                                  ▼
        ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
        │   Node A     │   │   Node B     │   │   Node C     │
        │  (Follower)  │◄─►│  (Leader)    │◄─►│  (Follower)  │
        └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
               │                  │                  │
         ┌─────▼─────┐     ┌──────▼──────┐     ┌─────▼─────┐
         │  WAL + KV  │     │  WAL + KV   │     │  WAL + KV │
         │  (disk)    │     │  (disk)     │     │  (disk)   │
         └────────────┘     └─────────────┘     └───────────┘
```

- Clients always write through the current **leader**; a follower contacted directly responds with a redirect to the leader.
- Every write is appended to the leader's log and replicated to followers via `AppendEntries`. Once a **majority** acknowledges, the entry is committed and applied to each node's local KV state machine.
- The **lock service** is built entirely on top of the KV store: a lock is just a specially-prefixed key holding `{holder_id, fencing_token, expires_at}`, replicated with the exact same durability guarantees as any other write.
- If the leader crashes, the remaining majority detects missed heartbeats and elects a new leader within a randomized election timeout — no single point of failure as long as 2 of 3 nodes are healthy.

### Package layout

```
raftkv/
├── cmd/raftkvd/          # server binary entrypoint (planned)
├── internal/raft/        # core consensus: election, replication, RPC types
├── internal/storage/     # WAL persistence + snapshot manager
├── internal/kv/          # replicated key-value state machine
├── internal/lock/        # lease-based lock service with fencing tokens
├── internal/transport/   # gRPC transport binding (planned)
├── api/proto/            # gRPC service definitions (planned)
├── pkg/client/           # public Go client SDK (planned)
├── deployments/          # Dockerfile + docker-compose (planned)
├── test/chaos/           # chaos-test harness (planned)
└── docs/                 # design documentation
```

The consensus core in `internal/raft` deliberately has **zero network or disk dependencies** — it talks to the outside world only through the `Transport` and `Persister` interfaces. This is what makes it possible to unit test leader election and log replication deterministically, using an in-memory fake transport, without spinning up a real cluster (see `internal/raft/raft_test.go`).

---

## Building

Clone and build the core packages (no external dependencies, works fully offline):

```bash
git clone https://github.com/sanchar127/raftkv.git
cd raftkv
go build ./...
go test ./...
```

Once the gRPC transport layer lands, `go mod tidy` will additionally need network access to `proxy.golang.org` to fetch `google.golang.org/grpc` and `google.golang.org/protobuf`.

---

## Design principles

- **Consensus core stays pure.** `internal/raft` has no I/O; it depends only on the `Transport` and `Persister` interfaces, so correctness can be verified with fast, deterministic unit tests instead of flaky integration tests.
- **Durability before acknowledgment.** Every persisted state change is written to the WAL and `fsync`ed before a node replies to any RPC that depends on it — an acknowledged write must never be lost to an unclean shutdown.
- **Locks are consensus, not timers.** The lock service never trusts wall-clock timeouts alone to decide who owns a resource; ownership is a Raft-committed fact, and every write against a lock is validated against its fencing token before being trusted.
- **Small, honest scope.** This project intentionally does not implement dynamic cluster membership, sharding, a query language, or TLS/auth in v1 — see [`docs/design.md`](docs/design.md) for the full in-scope/out-of-scope breakdown and the reasoning behind it.

---

## Roadmap

- [ ] gRPC transport for inter-node RPCs and client API
- [ ] Go client SDK with automatic leader discovery/retry
- [ ] Demo multi-replica job scheduler proving exactly-once execution
- [ ] Docker Compose 3-node cluster (`docker-compose up` and go)
- [ ] Chaos-test harness: leader kill, network partition, zombie-writer rejection tests
- [ ] Prometheus metrics + Grafana dashboard
- [ ] Benchmark results: failover time, write throughput, p95/p99 latency

---

## Contributing

Issues and pull requests are welcome — see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

[MIT](LICENSE)
