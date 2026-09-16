# Raft: Linearizable Reads (ReadIndex)

## Why this exists

A follower's local state can lag the leader's. Simply reading local state
on whichever node a client happens to talk to would be fast but not
linearizable — a client could see stale data, or even see writes go
"backward" across successive reads. `RaftNode.ReadIndex` (plus
`WaitApplied` on top) is what makes `server.Server.Get` linearizable
without paying the cost of a full `Propose` round-trip through the log for
every read.

## Algorithm (`RaftNode.ReadIndex`, `internal/raft/node.go`)

1. **Must be leader.** A non-leader returns an error immediately — reads
   only go through the leader.
2. **Current-term commit requirement.** Per Raft §8, the leader must have
   committed at least one entry *from its own current term* before it can
   serve a linearizable read. This guards against the case where a
   newly-elected leader hasn't yet confirmed which of the previous
   leader's entries are actually committed. Checked via
   `hasCommittedInCurrentTerm` — if false, `ReadIndex` fails outright
   rather than returning a possibly-wrong answer.
3. **Capture the current commit index** as the "read index" — the point
   the read needs to observe up to.
4. **Quorum confirmation.** The leader counts itself, then asks every peer
   to confirm it's still the leader (implemented as a heartbeat-style
   `AppendEntries` round) and waits for a quorum of acknowledgements
   *at the same term the read started in*. If a peer's reply carries a
   higher term, the leader steps down and the read fails
   (`TestReadIndexHigherTermReply`) — this is what prevents a partitioned,
   stale "leader" from serving reads as if it still held authority.
   A single-node (or otherwise self-quorate) configuration skips the RPC
   round entirely (`membershipHasQuorum(membership, acks)` with just
   itself) — `TestReadIndexSingleNode`.
5. **Context cancellation.** Each per-peer RPC goroutine races against
   `ctx.Done()` so a caller's timeout/cancellation is respected rather than
   blocking forever waiting for an unreachable peer.
6. Returns the captured commit index once quorum is confirmed.

## Completing the read (`server.Server.Get`)

`ReadIndex` alone only proves "as of this committed index, I was still the
leader." The caller still needs the **local state machine to have caught
up to that index** before reading it — otherwise you could confirm
leadership at index 100 but read stale data if the applier hasn't applied
index 100 yet. `server.Server.Get` does exactly this:

```go
index, err := s.raft.ReadIndex(ctx)
// ...
if err := s.applier.WaitApplied(ctx, index); err != nil { ... }
value, ok := s.store.Get(key)
```

Skipping `WaitApplied` and reading `kv.Store` immediately after
`ReadIndex` would reintroduce a race and break linearizability — see
[`../troubleshooting.md`](../troubleshooting.md#reads-seem-stale--linearizability-concerns).

## Interaction with joint consensus

Quorum for `ReadIndex` is computed the same way as commit-index quorum —
via `membershipHasQuorum`, which accounts for a joint configuration's
double-majority requirement automatically. No special-casing is needed in
`ReadIndex` itself.

## Tests

`TestReadIndexSingleNode`, `TestReadIndexQuorum`, `TestReadIndexNoQuorum`
(fails to reach quorum → error, not a stale answer),
`TestReadIndexHigherTermReply` (a peer reveals a higher term → leader
steps down, read fails rather than succeeding on stale authority).
