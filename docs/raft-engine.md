# RaftIQ Raft Consensus Engine

## Overview

The Raft engine implements the distributed consensus layer responsible for:

* Leader election
* Term management
* Log replication
* Commit advancement
* State-machine application
* Snapshot installation
* Failure recovery

---

## Server States

RaftIQ nodes operate in three primary states:

```text
+----------+
| Follower |
+----+-----+
     |
 Election Timeout
     |
     v
+-----------+
| Candidate |
+-----+-----+
      |
      | Majority Votes
      v
+----------+
|  Leader  |
+----------+
```

---

## Follower

A follower:

* Receives AppendEntries
* Receives RequestVote
* Persists term/vote changes
* Resets election timers after valid leader communication
* Does not normally accept replicated writes

---

## Candidate

When a follower's election timeout expires:

1. Increment term.
2. Become candidate.
3. Vote for itself.
4. Request votes from peers.
5. Become leader if a quorum grants votes.
6. Step down if a higher term is observed.

---

## Leader

A leader:

* Accepts proposals
* Appends entries locally
* Replicates entries to followers
* Tracks `nextIndex`
* Tracks `matchIndex`
* Advances `commitIndex`
* Sends heartbeats
* Steps down when a higher term is observed

---

## AppendEntries

Conceptually:

```text
Leader
  |
  | prevLogIndex
  | prevLogTerm
  | entries
  | leaderCommit
  v
Follower
```

The follower verifies the previous log position before accepting new entries.

If the logs conflict, conflicting uncommitted entries are removed and the leader's entries are replicated.

---

## RequestVote

Election requests contain:

```text
Candidate Term
Candidate ID
Last Log Index
Last Log Term
```

A follower evaluates:

* Term freshness
* Whether it has already voted
* Candidate log freshness

---

## Commit Advancement

The leader tracks follower replication progress:

```text
nextIndex[peer]
matchIndex[peer]
```

Once the required quorum has replicated an entry according to Raft's commitment rules, the leader advances `commitIndex`.

---

## State Machine Safety

The fundamental invariant is:

> If a server applies a particular log entry at an index, another server must never apply a different entry at that same index.

Therefore:

```text
Committed Log
      |
      v
Ordered Applier
      |
      v
Deterministic State Machine
```

---

## Higher-Term Handling

Every received RPC and RPC response must be checked for a higher term.

If:

```text
remoteTerm > currentTerm
```

the node must:

1. Persist the newer term.
2. Clear its vote as required by the implementation.
3. Transition to follower.
4. Stop acting as leader/candidate.

This prevents stale leaders from continuing to operate.

---

## Snapshots

Snapshots compact committed state.

Instead of retaining the complete historical log:

```text
[1][2][3][4][5][6][7][8][9][10]
                  ^
              Snapshot
```

the system can retain:

```text
Snapshot through index 6

[7][8][9][10]
```

Followers that fall behind the compacted log can receive the snapshot using `InstallSnapshot`.

---

## Safety Properties

The implementation targets these Raft properties:

### Election Safety

At most one leader can be elected in a given term.

### Leader Append-Only

A leader does not overwrite or truncate its own existing log entries.

### Log Matching

If two logs contain the same index and term, the logs are identical through that point.

### Leader Completeness

Committed entries remain present in leaders of later terms.

### State Machine Safety

No two servers apply different commands at the same log index.

---

## Persistence

Persistent Raft state includes the information required for safe restart, such as:

```text
currentTerm
votedFor
log
```

Durability boundaries are coordinated with the storage subsystem before the corresponding state is considered safely persisted.
