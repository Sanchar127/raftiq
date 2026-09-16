# Storage: Snapshots

See [`../raft/snapshots.md`](../raft/snapshots.md) for the Raft-level
protocol (`CreateSnapshot`/`InstallSnapshot`). This page covers how a
snapshot is actually persisted.

## What's in a `model.Snapshot`

Defined in `internal/model/snapshot.go`: the snapshot's `LastIncludedIndex`
and `LastIncludedTerm` (the log position it replaces), plus opaque `Data`
bytes — the serialized state-machine content, produced by whatever hook
was registered via `RaftNode.SetSnapshotRestore`/the snapshot-source
callback. `internal/raft` itself never interprets `Data`; only the state
machine (`internal/kv`) knows how to serialize/deserialize it.

## On-disk representation

A snapshot is written as a single framed WAL record with type
`recordSnapshot` (see [`wal.md`](wal.md#record-format)), encoded by
`encodeSnapshotPayload`/decoded by `decodeSnapshotPayload`. It lives in the
same WAL file as log entries and state records — there is no separate
snapshot file on disk. `WALStorage.SaveSnapshot`/`LoadSnapshot` implement
this; `MemoryStorage.SaveSnapshot`/`LoadSnapshot` do the equivalent
in-memory for tests.

## Compaction

Once a snapshot covering index `N` is durably saved, `Log.Compact`
discards in-memory log entries at or below `N`, keeping
`LastIncludedIndex`/`LastIncludedTerm` as the new "beginning" of the log
for purposes of `AppendEntries` consistency checks. The WAL itself is not
rewritten to remove now-superseded `recordEntries` records — recovery
replays the full history in order (entries, then the later snapshot record
whose restore supersedes them via `RestoreSnapshot`), so old entry records
remaining in the file are functionally overridden, not physically removed.
This means WAL file size is not automatically reclaimed by
snapshotting/compaction — check `TestCreateSnapshot`/`TestInstallSnapshot*`
in `internal/raft/node_test.go` if you're relying on file size shrinking.

## Restore path

On `OpenWAL`, if a `recordSnapshot` record was recovered, it becomes the
in-memory `snapshot` field; `Log.RestoreSnapshot` is invoked by
`RaftNode` at startup (via `NewRaftNodeWithStorage`) to set the log's
`LastIncludedIndex`/`LastIncludedTerm` boundary, and the registered
snapshot-restore hook is called to rebuild the actual state machine
(`kv.Store`) from `Data`. See `server.Server.Start()` in
`internal/server/server.go`, which explicitly checks
`snapshot.LastIncludedIndex > 0` and calls `applier.RestoreSnapshot` before
starting normal operation.
