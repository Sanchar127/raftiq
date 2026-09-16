# Raft: Joint Consensus

RaftIQ implements the standard two-phase (joint-consensus) approach to
membership changes rather than the single-entry "add/remove one node
directly" simplification, so that quorum is always well-defined even
mid-transition.

## State transitions

```text
Stable configuration (Current = {A,B,C})
       |
       v  EncodeEnterJointConfigurationEntry(old, new)
Joint configuration (Current = {A,B,C}, Joint = {A,B,C,D})
       |
       v  EncodeLeaveJointConfigurationEntry(new)
New stable configuration (Current = {A,B,C,D})
```

Represented in `model.Membership{ Current model.Configuration,
Joint *model.JointConfiguration }` — `Joint` is `nil` in a stable
configuration and non-nil only during the transition window.

## Why two phases

If a membership change went directly from `{A,B,C}` to `{A,B,C,D}` in one
step, different nodes could apply the change at different times (it's
still just a replicated log entry, applied asynchronously per node).
During that window, two disjoint majorities computed against two
different configurations could both believe they have quorum and elect
two different leaders — a safety violation. Joint consensus avoids this by
requiring, throughout the transition, that any decision (election or
commit) has a majority under **both** the old and the new configuration
simultaneously.

## Quorum during joint configuration

`internal/raft/membership.go`:

- `configurationHasQuorum(configuration, votes)` — plain single-configuration
  majority check.
- `membershipHasQuorum(membership, votes)` — if `membership.Joint` is set,
  requires `configurationHasQuorum` to hold against **both**
  `membership.Current` (the old set) and `membership.Joint`'s new set;
  otherwise falls back to checking just `membership.Current`.

This function is used uniformly for election quorum
(`hasElectionMajority`), commit quorum (`advanceCommitIndexLocked`), and
read quorum (`ReadIndex`) — there's no separate joint-aware code path in
each of those; they all delegate to `membershipHasQuorum`.

## The full `AddMember` sequence, in terms of joint consensus

1. **Catch up the new peer first**, *before* entering joint configuration
   (`catchUpPeer` up to the leader's current log tail). This avoids
   putting a completely-empty new voter into the quorum calculation, which
   would make even simple commits require waiting on a peer that has
   nothing yet.
2. **Propose `EnterJoint`** (old config + new config) and wait for it to
   be applied (`waitForApplied`). From this point, quorum requires
   majority of both the old and new voter sets.
3. **Re-replicate to the new peer** — `Propose` may commit `EnterJoint`
   after the new peer already received its last `AppendEntries` reply, so
   the leader explicitly calls `replicateTo(peerID)` again to make sure
   the new peer's `LeaderCommit` is up to date and it can apply the joint
   entry.
4. **Catch the new peer up through the joint configuration's log tail**
   (`catchUpPeer` again, to the now-later `jointTargetIndex`).
5. **Propose `LeaveJoint`** (just the new config) and wait for it to be
   applied. From this point, quorum only requires majority of the new set.
6. Verify the final membership is no longer joint and does contain the new
   peer before returning success.

`RemoveMember` follows the same EnterJoint → LeaveJoint shape with a
shrunk voter set, minus the catch-up steps (removing a peer doesn't
require it to be caught up).

## Persistence and recovery

Both `EnterJoint` and `LeaveJoint` are encoded as regular log entries
(`config_entry.go`), so they're persisted to the WAL exactly like any
other entry and replayed on recovery — a node that crashes and restarts
mid-joint-configuration reconstructs the correct `Joint` state from its
WAL, not from any separate metadata file. See
`TestRestartDuringJointConsensus`, `TestRestartAfterLeaveJoint`, and
`TestRestartedJointMembershipPreservesQuorumSafety` for the specific
scenarios this is checked against.

## Failure during a transition

If the leader driving `AddMember`/`RemoveMember` fails mid-transition, a
newly elected leader inherits whatever membership state was actually
committed (either still joint, or already leave-joint'd, depending on
exactly what made it to a majority before the failure) — there's no
separate "resume the in-flight membership change" logic; the new leader
simply continues normal operation against whatever configuration is
current. `TestLeaderFailureDuringJointConsensus` verifies this doesn't
leave the cluster in an unsafe or stuck state.
