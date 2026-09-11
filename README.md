# RaftIQ: Distributed Fault-Tolerant Key-Value Store & Distributed Job Scheduler

[![CI Build](https://github.com/sanchar127/raftiq/actions/workflows/ci.yml/badge.svg)](https://github.com/sanchar127/raftiq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/sanchar127/raftiq.svg)](https://pkg.go.dev/github.com/sanchar127/raftiq)
[![Go Report Card](https://goreportcard.com/badge/github.com/sanchar127/raftiq)](https://goreportcard.com/report/github.com/sanchar127/raftiq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**RaftIQ** is a distributed, strongly consistent key-value store and fault-tolerant distributed job scheduler built from scratch in Go.

The system combines a custom Raft consensus implementation, durable WAL storage, deterministic state-machine replication, lease-based distributed locking, fencing tokens, distributed job scheduling, gRPC transport, production observability, and chaos testing.

The primary goal of RaftIQ is to demonstrate how distributed-systems guarantees can be implemented and validated from first principles rather than relying on an existing consensus library.

---

## Key Technical Highlights

* **Custom Raft Consensus:** Leader election, term management, log replication, commit tracking, safety rules, and snapshot installation.
* **Durable WAL:** Append-only binary WAL with CRC32 validation, crash recovery, tail truncation, suffix replacement, and snapshot persistence.
* **Strongly Consistent KV Store:** State-machine mutations are applied strictly in committed log order.
* **Distributed Locking:** Lease-based locking with monotonically increasing fencing tokens.
* **Fault-Tolerant Scheduler:** Distributed job coordination with worker leases, deterministic worker selection, retries, and stale-worker fencing.
* **gRPC Transport:** Client APIs and inter-node Raft RPCs implemented using protobuf/gRPC.
* **Production Observability:** Prometheus metrics, structured logging, tracing hooks, health/readiness endpoints, and Grafana dashboards.
* **Chaos Testing:** Leader failure, follower failure, network partition, stale worker fencing, recovery, race detection, and linearizability testing.

---

## High-Level Architecture

```text
                         +-------------------+
                         |    gRPC Client    |
                         +---------+---------+
                                   |
                  Dynamic Leader Redirection
                                   |
             +---------------------+---------------------+
             |                                           |
             v                                           v
   +--------------------+                     +--------------------+
   |    RaftIQ Node 1   |                     |    RaftIQ Node 2   |
   |      LEADER        |                     |     FOLLOWER       |
   +--------------------+                     +--------------------+
   |                    |                     |                    |
   |  gRPC Server       |<---- Raft RPCs ---->|  gRPC Server       |
   |       |            |                     |       |            |
   |  Raft Engine       |                     |  Raft Engine       |
   |       |            |                     |       |            |
   |  Applier Pipeline  |                     |  Applier Pipeline  |
   |       |            |                     |       |            |
   |  KV / Lock Engine  |                     |  KV / Lock Engine  |
   |       |            |                     |       |            |
   |  Job Scheduler     |                     |  Job Scheduler     |
   |       |            |                     |       |            |
   |  WAL / Snapshot    |                     |  WAL / Snapshot    |
   +--------------------+                     +--------------------+
             |                                           |
             +-------------------+-----------------------+
                                 |
                                 v
                    +-------------------------+
                    |   Distributed Workers   |
                    |  Fenced Job Execution   |
                    +-------------------------+
```

---

## Core Architecture Components

### 1. Raft Consensus Subsystem

Located in:

```text
internal/raft
```

Responsibilities:

* Leader election
* Candidate/follower/leader state transitions
* Term management
* RequestVote RPC handling
* AppendEntries RPC handling
* Log replication
* Commit index advancement
* Applied index tracking
* Snapshot installation
* Leader replication state
* Election timeout handling
* Persistent Raft state

The implementation follows the core Raft safety properties including election safety, log matching, leader completeness, and state-machine safety.

---

### 2. Deterministic State Machine

Located primarily in:

```text
internal/kv
```

Committed Raft entries are passed through a dedicated applier pipeline.

```text
Raft Commit
     |
     v
Applier
     |
     v
State Machine
     |
     +---- KV State
     |
     +---- Lock State
     |
     +---- Job State
```

Only committed entries are applied to the state machine.

This ensures that replicas execute the same committed command sequence and therefore converge on the same deterministic state.

---

### 3. Distributed Lock & Fencing Engine

The locking subsystem provides:

* Lease-based locks
* Monotonically increasing fencing tokens
* Lease expiration
* Stale worker detection
* Fencing of delayed/zombie workers
* State-machine-based lock transitions

A worker holding fencing token `N` must not be able to mutate state after token `N+1` has been issued.

```text
Worker A
   |
   | Token = 41
   v
Lock Granted
   |
   | Worker becomes partitioned
   |
   X

Lease expires

Worker B
   |
   | Token = 42
   v
Lock Granted

Worker A later sends mutation
   |
   | Token = 41
   v
Rejected
   |
   +---- ErrInvalidFencing
```

This protects the system against stale workers and delayed network messages.

---

### 4. Distributed Job Scheduler

Located in:

```text
internal/scheduler
internal/worker
```

The scheduler coordinates job execution across workers.

Typical lifecycle:

```text
PENDING
   |
   v
CLAIMED
   |
   v
RUNNING
   |
   +----------+
   |          |
   v          v
COMPLETED   FAILED
```

The scheduler uses:

* Worker registration
* Worker heartbeats
* Lease expiration
* Deterministic worker selection
* Job state replication
* Fencing tokens
* Retry handling
* Worker failure recovery

---

### 5. Durable Storage & WAL

Located in:

```text
internal/storage
```

The storage layer provides:

* Append-only WAL
* Binary record encoding
* CRC32 validation
* Full-write handling
* Crash recovery
* Corrupted-tail detection
* Tail truncation
* Log suffix replacement
* Snapshot persistence
* Snapshot recovery
* Explicit durability/sync semantics

Example record structure:

```text
+--------------+-------------------+----------------------+--------------+
| Record Type  | Payload Length    | Payload Data         | CRC32        |
|    1 byte    |      4 bytes      |      N bytes         |   4 bytes    |
+--------------+-------------------+----------------------+--------------+
```

The WAL is scanned during startup and validated before recovered state is exposed.

---

### 6. gRPC Transport Layer

Located in:

```text
internal/transport
api/proto
```

Supports:

#### Client operations

* `Get`
* `Put`
* `Delete`
* Job submission and related APIs

#### Raft operations

* `AppendEntries`
* `RequestVote`
* `InstallSnapshot`

Followers can return leader information so clients can redirect requests to the current leader.

---

### 7. Observability

Located primarily in:

```text
internal/observability
```

RaftIQ exposes operational telemetry including:

* Raft state
* Current term
* Election counts
* RPC latency
* WAL write latency
* WAL sync latency
* KV operation counters
* Scheduler queue depth
* Worker state
* Job execution metrics
* Health/readiness status

Prometheus and Grafana can be used for cluster monitoring.

---

# Low-Level System Design

## 1. Consensus & Log Replication

The basic write path is:

```text
Client
  |
  v
gRPC Server
  |
  v
RaftNode.Propose()
  |
  v
Append Entry to Local Log
  |
  v
Persist to WAL
  |
  v
AppendEntries RPCs
  |
  +----------+----------+
  |                     |
  v                     v
Follower 1           Follower 2
  |                     |
  +----------+----------+
             |
             v
        Quorum ACK
             |
             v
       Advance CommitIndex
             |
             v
       Applier Pipeline
             |
             v
       State Machine
             |
             v
        Client Response
```

A write is considered committed only after the leader has established the required quorum according to the Raft rules.

---

# 2. Fencing Tokens

Distributed workers can become stale because of:

* Network partitions
* Long GC pauses
* Process stalls
* Delayed packets
* Worker crashes
* Lease expiration

RaftIQ uses monotonically increasing fencing tokens to protect against these conditions.

Example:

```text
Worker A obtains token 10

Worker A
   |
   | token=10
   v
Partition

Lease expires

Worker B obtains token 11

Worker B
   |
   | token=11
   v
Executes job

Worker A reconnects
   |
   | token=10
   v
Rejected by state machine
```

The state machine therefore prevents an older worker from modifying state after a newer ownership decision has been committed.

---

# 3. WAL Recovery

On startup:

```text
Open WAL
   |
   v
Read Record
   |
   v
Validate Header
   |
   v
Validate Length
   |
   v
Read Payload
   |
   v
Validate CRC32
   |
   +---- Invalid tail?
   |          |
   |          v
   |     Truncate Tail
   |
   v
Replay Record
   |
   v
Recover State
```

A partial final record caused by an interrupted write can therefore be detected and repaired without replaying corrupted state.

---

# 4. Snapshotting

Snapshots prevent the Raft log from growing indefinitely.

Conceptually:

```text
Original Log

[1][2][3][4][5][6][7][8][9][10]
                ^
             Snapshot
```

After snapshotting:

```text
Snapshot
  |
  +---- State through index 6

Remaining Log

[7][8][9][10]
```

A follower that is too far behind the compacted log can receive an `InstallSnapshot` RPC instead of requiring every historical log entry.

---

# Strong Consistency

RaftIQ is designed around replicated state-machine consistency.

For writes:

```text
Client
  |
  v
Leader
  |
  v
Replicated Log
  |
  v
Quorum
  |
  v
Commit
  |
  v
Apply
```

For linearizable reads, the implementation must ensure the serving node has sufficiently established current leadership/commit state rather than simply returning potentially stale follower state.

---

# Testing & Chaos Engineering

RaftIQ includes testing across multiple failure classes.

Examples include:

```text
tests/chaos/
```

Important scenarios include:

### Leader Failure

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
Continue Operations
```

### Follower Failure

The remaining quorum continues operating while the failed follower is eventually brought back and catches up.

### Network Partition

The minority side must not be able to make committed decisions without quorum.

### Zombie Worker

A stale worker with an old fencing token must be rejected.

### Recovery

Nodes restart from durable state and reconstruct the correct Raft/storage state.

### Linearizability

Concurrent client operations are exercised while failures and restarts occur to validate consistency guarantees.

---

# Documentation

Detailed technical documentation is available under `docs/`.

| Document                          | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| `docs/architecture.md`            | Overall architecture, module boundaries, and execution flows |
| `docs/raft-engine.md`             | Raft state machine, elections, replication, and safety       |
| `docs/storage-engine.md`          | WAL, durability, recovery, and snapshots                     |
| `docs/scheduler-and-worker.md`    | Distributed scheduling, leases, workers, and fencing         |
| `docs/observability-and-chaos.md` | Metrics, monitoring, and chaos testing                       |

---

# Getting Started

## Prerequisites

* Go 1.22+
* Protocol Buffers compiler (`protoc`)
* Docker and Docker Compose (optional)

## Clone

```bash
git clone https://github.com/sanchar127/raftiq.git
cd raftiq
```

## Build

```bash
make build
```

## Run Tests

```bash
make test
```

## Run Race Detector

```bash
go test -race ./...
```

## Run Chaos Tests

```bash
go test -v -race ./tests/chaos/...
```

---

# Go Client Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sanchar127/raftiq/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := client.Dial(ctx, "localhost:50051")
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer cli.Close()

	if err := cli.Put(ctx, "cluster:config:max_connections", []byte("10000")); err != nil {
		log.Fatalf("put failed: %v", err)
	}

	value, err := cli.Get(ctx, "cluster:config:max_connections")
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}

	fmt.Printf("Retrieved Key: %s\n", string(value))
}
```

---

# Engineering Standards

RaftIQ follows production-oriented engineering practices:

* Go idioms and standard library first
* Strict error handling
* `gofmt`
* `go vet`
* `golangci-lint`
* Race detector testing
* Unit tests
* Integration tests
* Failure injection
* Chaos testing
* Durable storage validation
* Concurrent execution testing
* Observability instrumentation

---

# License

RaftIQ is released under the MIT License.
