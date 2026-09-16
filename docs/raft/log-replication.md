# Raft: Log Replication

## Leader side

`replicateTo(peerID)` builds an `AppendEntries` request via
`buildAppendEntries(peerID)` — using the peer's `nextIndex` to decide which
entries (if any) to send and what `prevLogIndex`/`prevLogTerm` to assert —
and sends it through the configured `Transport`. This runs per-peer,
concurrently, so one slow/unreachable follower doesn't block replication
to the others.

`handleAppendEntriesReply` processes the response:
- On success, advances that peer's `matchIndex`/`nextIndex` and attempts
  `advanceCommitIndexLocked`.
- On failure (log mismatch), decrements `nextIndex` for that peer so the
  next round backs up further into the log, per the standard Raft
  backtracking approach (RaftIQ does not implement the "conflict term/index
  hint" fast-backtrack optimization from the paper's extended version —
  check `handleAppendEntriesReply` directly if you're relying on that).

## Follower side

`RaftNode.AppendEntries` (the RPC handler) is where the **log-matching
property** is enforced: `validateAppend`/`validateRecoveredAppend` check
that the follower's log actually has an entry at `prevLogIndex` with term
`prevLogTerm` before accepting the leader's entries. A mismatch returns
`success=false`, telling the leader to back up `nextIndex` and retry.

On acceptance:
- Any conflicting existing entries (same index, different term) are
  discarded and replaced — persisted via `Storage.ReplaceSuffix`, which
  must never touch an index at or below the follower's own commit index.
- New entries are appended and persisted (`Storage.AppendEntries`).
- The follower's `commitIndex` is advanced to
  `min(leaderCommit, last new entry index)`.
- The election timer is reset — a valid `AppendEntries` from the current
  leader counts as a "still alive" signal (`TestAppendEntriesResetsElectionTimer`).

## Heartbeats

Heartbeats are `AppendEntries` calls with an empty entry list, sent on the
`-heartbeat` interval by the leader (`sendHeartbeats`/`sendHeartbeat`).
They serve two purposes: resetting followers' election timers, and (via
`ReadIndex`) confirming the leader is still the leader for linearizable
reads — see [`linearizable-reads.md`](linearizable-reads.md).

## Term handling

Both `AppendEntries` and `RequestVote`/`PreVote` handlers check the
incoming term against the node's own. Any RPC (request or reply) carrying
a higher term causes the receiving node to step down to follower at that
term (`becomeFollowerLocked`) — this is the mechanism that guarantees
terms never decrease and that a stale leader (e.g. one that was
partitioned and is still sending heartbeats) gets corrected the moment it
talks to anyone with a higher term.

## Local (in-process) transport for tests

`internal/raft/local_transport.go`'s `LocalTransport` implements the same
`Transport` interface as the real gRPC transport but delivers RPCs via
direct Go calls between in-process `RaftNode`s, with `Block`/`Unblock`/
`BlockBidirectional` hooks to simulate dropped links. This is what lets
`internal/raft/node_test.go` exercise multi-node replication scenarios
without any real networking. See [`../transport/local.md`](../transport/local.md).
