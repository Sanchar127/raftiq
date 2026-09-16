# Raft: Leader Election

## PreVote

Before bumping its term and requesting real votes, a node runs
`runPreVote()` (`internal/raft/node.go`), which asks peers "would you vote
for me at term T+1?" **without** persisting the term increase or a vote.
This is the standard PreVote optimization: it stops a partitioned node
that keeps timing out from disrupting a healthy leader by forcing a real
term bump every time it wakes up.

Key rules, each backed by a test:
- Only a node currently in the voting configuration can even attempt
  PreVote (`membershipIsVoter` check) — `TestPreVoteRejectsNonVoter`.
- A peer rejects PreVote from a candidate whose log isn't at least as
  up-to-date as its own — `TestPreVoteRejectsStaleCandidateLog`.
- A peer rejects PreVote if it has heard from a leader recently (i.e. its
  election timer hasn't expired) — `TestPreVoteRejectsWhenRecentLeaderExists`.
  This is what actually prevents a reconnected, stale node from disrupting
  a stable leader.
- PreVote never mutates `CurrentTerm` or `VotedFor` — see the comment
  `// PreVote deliberately does not change our persistent term.` in
  `runPreVote` — confirmed by `TestPreVoteDoesNotChangeTermOrVote`.
- A single-node (or otherwise already-quorate) configuration can win
  PreVote against itself without sending any RPCs — see the early-return
  `if membershipHasQuorum(membership, votes) { return true }`.

Only after `runPreVote()` returns `true` does `startElection()` actually
bump the term and send real `RequestVote` RPCs.

## RequestVote

`RaftNode.RequestVote` (the real election, not PreVote) grants a vote if:
- The candidate's term is at least the voter's current term (and the voter
  steps down to follower / updates its term if the candidate's is higher).
- The voter hasn't already voted for someone else this term
  (`VotedFor` check via `recordVote`).
- The candidate's log is at least as up-to-date as the voter's
  (`isCandidateLogUpToDate` — last-log-term first, then last-log-index —
  this is the leader-completeness-preserving check from the Raft paper).

`recordVote` persists the vote (`SaveState`) before the RPC reply is sent,
so a crash right after voting can't result in double-voting on restart.

## Winning an election

`tryBecomeLeader`/`hasElectionMajority` compute majority against the
*current effective configuration* — during a joint configuration this
means majority of both the old and new voter sets independently (see
[`joint-consensus.md`](joint-consensus.md)). `becomeLeaderLocked`
initializes per-peer replication state (`nextIndex`/`matchIndex`) and
starts sending heartbeats immediately to establish authority before any
other node's election timer fires.

## Stale votes and stale replies

`handleVoteReply` checks the reply's term against the node's current term
before acting on it — a vote reply from an election the node has since
abandoned (e.g. it already lost and moved on) is ignored. Tests:
`TestStaleElectionVoteReplyIsIgnored`, `TestVoteFromOldElectionIsIgnored`.

## Election/heartbeat timing

Configured via `-election`/`-heartbeat` CLI flags
(`RaftNode.SetElectionTimeout`, `sendHeartbeats`/`heartbeatDue`). The CLI
enforces `-election > -heartbeat` at startup (`validateConfig`) — see
[`../configuration.md`](../configuration.md).
