# RaftIQ Storage Engine & WAL

## Overview

The storage subsystem provides durable persistence for Raft state, log entries, snapshots, and recovery metadata.

---

## WAL Record Format

RaftIQ uses framed binary records:

```text
+--------------+-------------------+----------------------+--------------+
| Record Type  | Payload Length    | Payload Data         | CRC32        |
|    1 byte    |      4 bytes      |      N bytes         |   4 bytes    |
+--------------+-------------------+----------------------+--------------+
```

The CRC protects the record against corrupted writes.

---

## Record Types

The exact set is implementation-defined, but the WAL supports records representing concepts such as:

```text
State
Log Entry
Snapshot
Log Suffix Replacement
```

---

## Write Path

```text
Raft Operation
     |
     v
Encode Record
     |
     v
Append WAL
     |
     v
Full Write
     |
     v
Sync / Durability Boundary
     |
     v
Operation Continues
```

---

## Recovery

At startup:

```text
Open WAL
   |
   v
Read Header
   |
   v
Validate Length
   |
   v
Read Payload
   |
   v
Validate CRC
   |
   +---- Invalid final record?
   |             |
   |             v
   |        Truncate Tail
   |
   v
Replay Record
   |
   v
Reconstruct State
```

A truncated final record caused by a crash can therefore be removed without accepting corrupted state.

---

## Log Suffix Replacement

Raft may need to remove conflicting uncommitted entries.

Example:

```text
Before:

[1][2][3][4][5][6]
             ^
           Conflict

After:

[1][2][3][4][7][8]
```

The storage layer must persist the resulting log state safely.

---

## Snapshots

Snapshots allow old committed log entries to be compacted.

A snapshot contains enough deterministic state to reconstruct the state machine up to a particular committed index.

Typical metadata includes:

```text
snapshotIndex
snapshotTerm
stateMachineState
```

---

## Crash Safety

The storage layer is designed around:

* Complete writes
* Explicit sync semantics
* CRC validation
* Tail repair
* Recovery invariant checks
* Concurrency safety
* Closed-storage detection
* Snapshot recovery

The storage implementation must never silently accept corrupted persistent state.
