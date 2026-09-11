# RaftIQ Distributed Scheduler & Worker Engine

## Overview

RaftIQ provides distributed job scheduling on top of the replicated state machine.

The scheduler does not treat local memory as the source of truth for job ownership. Important job transitions are coordinated through replicated state.

---

## Job Lifecycle

```text
PENDING
   |
   v
CLAIMED
   |
   v
RUNNING
   |
   +----------------+
   |                |
   v                v
COMPLETED         FAILED
```

---

## Scheduling Flow

```text
Job Submission
      |
      v
Raft Proposal
      |
      v
Quorum Commit
      |
      v
State Machine
      |
      v
Scheduler
      |
      v
Worker Selection
      |
      v
Lease + Fencing Token
      |
      v
Worker Execution
```

---

## Worker Selection

RaftIQ uses deterministic worker selection logic.

The same replicated scheduling state should produce the same scheduling decision across replicas.

This avoids nodes independently making contradictory ownership decisions.

---

## Worker Leases

Workers periodically send heartbeats.

Conceptually:

```text
Worker
  |
  | Heartbeat
  v
Lease State
  |
  | Renew
  v
Active
```

If heartbeats stop:

```text
Active
  |
  | Timeout
  v
Expired
  |
  v
Job Reassignment
```

---

## Fencing Tokens

Every ownership transition receives a monotonically increasing fencing token.

Example:

```text
Worker A -> Token 41

Worker A loses connectivity

Lease expires

Worker B -> Token 42

Worker A returns

Worker A sends mutation with Token 41

             |
             v

        REJECTED
```

The stale worker cannot overwrite state owned by the newer worker.

---

## Exactly-Once vs At-Least-Once

Distributed execution requires careful distinction between:

* scheduling ownership
* state transitions
* actual external side effects

RaftIQ's consensus and fencing mechanisms prevent multiple valid owners from simultaneously controlling the same replicated job state.

Actual external side effects may still require idempotency or transactional integration with the external system to provide true end-to-end exactly-once effects.

---

## Worker Failure Recovery

When a worker fails:

```text
Worker Failure
     |
     v
Heartbeat Timeout
     |
     v
Lease Expiration
     |
     v
New Fencing Token
     |
     v
Job Reassignment
     |
     v
New Worker
```

---

## Zombie Worker Protection

A delayed worker may continue executing after losing ownership.

The fencing token provides the protection boundary:

```text
Old Token < Current Token
        |
        v
Reject Mutation
```

This is especially important under network partitions and long process pauses.
