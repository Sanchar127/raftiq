# Raft: Commitment

## Advancing the commit index

`advanceCommitIndexLocked` (leader-only logic) finds the highest log index
that has been replicated (`matchIndex`) to a quorum of the *current
effective configuration* — see [`joint-consensus.md`](joint-consensus.md)
for what "effective configuration" means when a membership change is in
flight. Critically, per the Raft paper's safety argument, a leader only
directly commits an entry by counting replicas **if that entry was created
in the leader's own current term** (`logTerm(index) == currentTerm`) —
older-term entries become committed only indirectly, as a side effect of a
later same-term entry being committed. This is what prevents a subtle
safety violation where a leader could commit an entry from a previous term
that a future leader might not have.

`majority(clusterSize)` and `membershipHasQuorum`/`configurationHasQuorum`
(`internal/raft/membership.go`) compute the actual quorum thresholds,
including the joint-configuration double-majority requirement.

## Applying committed entries

`applyCommitted()` pushes every newly committed entry (in order, one at a
time) onto `RaftNode.ApplyCh()`. `internal/kv.Applier.Run` is the consumer
— it reads from this channel and applies each entry to `kv.Store` in the
same order, which is what makes replicas deterministic and convergent:
every node applies the exact same sequence of committed commands.

Configuration entries (produced by `AddMember`/`RemoveMember`/joint
transitions) are intercepted before reaching the KV applier —
`applyConfigurationEntryLocked` recognizes them via
`IsConfigurationEntry` and updates `RaftNode`'s own membership state
instead of forwarding them as KV commands.

## Waiting for application

Client-facing writes (`server.Server.Put`/`Delete`) call `Propose` to get
a log index, then `Applier.WaitApplied(ctx, index)` to block until that
index has actually been applied to the state machine before returning
success to the caller. `RaftNode.WaitApplied` provides the lower-level
primitive this is built on, using the applied-index tracking updated by
`applyCommitted`.

## Why this ordering matters

A write is only acknowledged to the client after: (1) committed — a
majority durably has it — and (2) applied — the local state machine has
actually processed it. Skipping step 2 would let a client's own
just-written value appear missing from an immediately-following read on
the same node.
