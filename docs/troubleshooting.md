# Troubleshooting

## "put/delete/get fails against this node"

The node you're talking to probably isn't the leader. `Propose`/`ReadIndex`
fail on a non-leader node, and `KVService`'s gRPC responses carry no
leader-redirect information (a deliberate documentation of a real gap —
see [`AUDIT.md`](AUDIT.md)). Check `raftiq_raft_role` on each node's
`/metrics` to find the current leader, or retry against a different peer
in your `-peers` list.

## "the cluster won't elect a leader" / repeated elections

- Check `-election` vs `-heartbeat` on every node — `-election` must be
  strictly greater than `-heartbeat` (enforced at startup by
  `validateConfig`, but a misconfigured subset of nodes with different
  values can still produce odd timing behavior).
- Check `raftiq_raft_elections_total` and `raftiq_raft_term` across nodes —
  a rapidly climbing term with no stable leader usually means either a
  network partition preventing majority quorum, or clock/timeout
  configuration is too aggressive for the actual network latency between
  nodes.
- Confirm every node's `-peers` list agrees on the full voter set. A
  mismatched `-peers` across nodes means they disagree about what quorum
  size is required.
- PreVote (`runPreVote`) should prevent a partitioned node from disrupting
  a healthy leader by starting spurious elections — if you're seeing
  disruptive elections from a node that was recently partitioned, check
  `TestPreVoteRejectsWhenRecentLeaderExists` in `node_test.go` for the
  intended behavior and compare against what you're observing.

## "a follower never catches up" / stuck behind

- Check whether the follower's required log entries have already been
  compacted into a snapshot on the leader (`Log.Compact`). If so, it
  should receive `InstallSnapshot` automatically — check
  `raftiq_raft_snapshots_installed_total` on the follower.
- Check connectivity on `-raft-addr` between leader and follower
  specifically; `AppendEntries` and `InstallSnapshot` both use it.

## "node won't start" / crashes on boot

- `OpenWAL` failing usually means the WAL file exists but is corrupted
  beyond the automatic tail-truncation recovery — see
  [`storage/wal.md`](storage/wal.md#recovery) for exactly what's tolerated
  (a valid prefix of records plus one incomplete trailing record) versus
  what isn't (corruption in the middle of the file, e.g. a bad CRC on a
  record that isn't the last one — this returns `ErrWALCorrupt` rather
  than being silently repaired).
- Check `-data-dir` is writable and has free disk space; `ErrWALDiskFull`
  is returned on write failure due to insufficient space, though this path
  has less dedicated test coverage than the corruption/truncation paths
  (see [`AUDIT.md`](AUDIT.md)).
- `validateConfig` failures exit with status `2` and print a clear message
  to stderr before anything else runs — check that first.

## "membership change / AddMember call fails"

Common rejections, all deliberate safety checks in `RaftNode.AddMember`/
`RemoveMember` (`internal/raft/node.go`):
- Caller is not the current leader (`role != Leader`).
- A membership change is already in progress (`membership.Joint != nil`).
- The target is already a voter (`AddMember`) or isn't a voter
  (`RemoveMember`).
- `RemoveMember` targeting the leader itself is rejected outright — see
  `TestRemoveMemberRejectsLeaderSelfRemoval`. There's no automated
  leadership-transfer-then-remove-self flow; you'd need to trigger a
  leadership change out of band first.

Remember: none of this is reachable over gRPC or the CLI today — you can
only hit these errors by calling `RaftNode.AddMember`/`RemoveMember`
directly from Go code. See [`raft/membership.md`](raft/membership.md).

## "reads seem stale" / linearizability concerns

Make sure reads go through `server.Server.Get` (which calls `ReadIndex`
then `WaitApplied`), not a direct `kv.Store.Get` call bypassing that path.
If you've embedded these packages yourself and skipped `ReadIndex`, you've
opted into follower-local (potentially stale) reads.

## "TLS doesn't seem to be active"

It isn't, by default — the stock `raftiq` binary runs both gRPC listeners
in plaintext. See [`transport/security.md`](transport/security.md).

## "the scheduler/worker/lock isn't doing anything"

`cmd/raftiq/main.go` doesn't start a scheduler or worker process, and
`KVService` has no lock RPCs. These are library packages you need to
embed yourself — see [`components/scheduler.md`](components/scheduler.md),
[`components/worker.md`](components/worker.md), and
[`components/lock.md`](components/lock.md).

## Where metrics can help

| Symptom | Metric to check |
|---|---|
| Who's the leader? | `raftiq_raft_role`, `raftiq_raft_current_term` per node |
| Election churn | `raftiq_raft_elections_total`, `raftiq_raft_leader_changes_total` |
| Replication lag | `raftiq_raft_last_log_index` vs `raftiq_raft_commit_index`/`raftiq_raft_last_applied` |
| WAL health | `raftiq_storage_sync_errors_total`, `raftiq_storage_operation_errors_total` |
| RPC errors | `raftiq_rpc_errors_total` |
