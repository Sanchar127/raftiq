# RaftIQ Architecture Overview

## Executive Summary

RaftIQ separates distributed consensus, durable storage, state-machine application, scheduling, worker execution, and client transport into independent subsystems.

The core architectural principle is:

> Consensus decides **what state transition is committed**; the deterministic state machine decides **how that committed transition changes application state**.

---

## High-Level Architecture

```text
                     +-------------------+
                     |    gRPC Client    |
                     +---------+---------+
                               |
                               v
                     +-------------------+
                     |    gRPC Server    |
                     +---------+---------+
                               |
                +--------------+--------------+
                |                             |
                v                             v
        +---------------+             +---------------+
        | Client APIs   |             | Raft RPCs     |
        +-------+-------+             +-------+-------+
                |                             |
                v                             v
        +---------------------------------------------+
        |                Raft Node                    |
        |                                             |
        |  +-------------+       +---------------+   |
        |  | Raft Core   |<----->| WAL / Storage |   |
        |  +------+------+       +---------------+   |
        |         |                                   |
        |         v                                   |
        |  +-------------+                            |
        |  |   Applier   |                            |
        |  +------+------+                            |
        |         |                                   |
        |         v                                   |
        |  +-------------+                            |
        |  | KV / Locks  |                            |
        |  +------+------+                            |
        |         |                                   |
        |         v                                   |
        |  +-------------+                            |
        |  | Scheduler   |                            |
        |  +------+------+                            |
        +---------|-----------------------------------+
                  |
                  v
          +---------------+
          |    Workers    |
          +---------------+
```

---

## Client Request Lifecycle

For a mutation:

```text
Client
  |
  v
gRPC
  |
  v
Leader Check
  |
  v
Raft Proposal
  |
  v
Local Log
  |
  v
WAL Persistence
  |
  v
AppendEntries
  |
  v
Quorum
  |
  v
CommitIndex
  |
  v
Applier
  |
  v
State Machine
  |
  v
Response
```

---

## Node Responsibilities

Every RaftIQ node contains:

* gRPC server
* Raft engine
* Persistent storage
* State-machine applier
* KV state
* Lock state
* Scheduler state
* Worker coordination
* Observability

A node can transition between:

```text
Follower
   |
   v
Candidate
   |
   v
Leader
```

The current role changes according to Raft's election rules.

---

## Replication Boundary

Only the leader accepts normal replicated mutations.

Followers receive replicated log entries through Raft RPCs.

```text
              Leader
                 |
       +---------+---------+
       |                   |
       v                   v
   Follower 1          Follower 2
```

The state machine must never independently invent state transitions outside the replicated command stream.

---

## Concurrency Model

The implementation separates:

1. Consensus state
2. Persistent storage
3. State-machine application
4. Network transport
5. Worker execution

This prevents application-level execution from directly modifying consensus state.

Committed entries are delivered to the applier pipeline in commit order.

---

## Failure Boundaries

Important failure boundaries include:

```text
Client
  |
Network Failure
  |
gRPC
  |
Node Failure
  |
Raft
  |
Disk Failure
  |
WAL
  |
State Machine
  |
Worker Failure
```

Each subsystem therefore has its own recovery and validation mechanisms.
