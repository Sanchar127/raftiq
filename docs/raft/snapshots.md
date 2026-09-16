# Raft: Snapshots

Snapshots bound Raft log growth by periodically replacing a prefix of the
log with a compact, point-in-time state-machine image.

```text
Original log:  [1][2][3][4][5][6][7][8][9][10]
                              ^
                     snapshot taken through 6

After compaction:
  Snapshot (state through index 6, term T)
  Remaining log: [7][8][9][10]
```

## `CreateSnapshot(index, data)`

Callable on any node (typically triggered by application-level policy —
RaftIQ itself has no automatic "snapshot every N entries" scheduler; a
caller decides when to invoke this). Rejects the request outright if:

- `index > LastApplied` — **can't snapshot an index the state machine
  hasn't actually applied yet** (`TestCreateSnapshotRejectsUnappliedIndex`).
- `index > CommitIndex` — **can't snapshot an uncommitted index**
  (`TestCreateSnapshotRejectsUncommittedIndex`) — a snapshot must only
  ever represent durably-agreed state.
- `index == 0` — nothing to snapshot yet (`TestCreateSnapshotRejectsIndexZero`).

On success: persists the snapshot (`Storage.SaveSnapshot`), compacts the
in-memory log (`Log.Compact`), and updates
`LastIncludedIndex`/`LastIncludedTerm`. See
[`../storage/snapshots.md`](../storage/snapshots.md) for the on-disk
representation — note that compaction does not shrink the WAL file itself,
only the in-memory log.

## `InstallSnapshot` (leader → lagging follower)

When a leader's `nextIndex` for a follower falls at or below its own
`LastIncludedIndex` (the follower needs log entries the leader has already
compacted away), the leader sends `InstallSnapshot` instead of
`AppendEntries` (`buildInstallSnapshot`/`sendInstallSnapshot`). The
follower's handler (`RaftNode.InstallSnapshot`):

- Persists the higher term if the leader's term exceeds its own, exactly
  like other RPCs (`TestInstallSnapshotPersistsHigherTerm`).
- Calls the registered snapshot-restore hook
  (`SetSnapshotRestore`) to rebuild the state machine directly from the
  snapshot data, **replacing** whatever local state it had —
  `TestInstallSnapshotRestoresStateMachine`.
- Resets its log to start after the snapshot boundary
  (`Log.RestoreSnapshot`).

`handleInstallSnapshotReply` on the leader side advances that follower's
`matchIndex`/`nextIndex` past the snapshot boundary on success, so
subsequent replication resumes with normal `AppendEntries`.

## Startup restore

At process start, `NewRaftNodeWithStorage` loads any persisted snapshot
(`Storage.LoadSnapshot`) and restores the log's compaction boundary before
the node begins participating in the cluster; `server.Server.Start()`
separately invokes the KV-level restore hook
(`applier.RestoreSnapshot(snapshot)`) so `kv.Store` reflects the snapshot
before normal operation (including serving reads) begins. See
[`../storage/snapshots.md`](../storage/snapshots.md#restore-path).

## Relationship to membership

Snapshots capture whatever the state-machine's serialize hook returns —
this does not include Raft's own membership configuration, which is
tracked separately as part of `PersistentState` and persisted via ordinary
`SaveState` calls, not inside the snapshot payload itself.
