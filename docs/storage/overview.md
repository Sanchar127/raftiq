# Storage: Overview

`internal/storage` defines the `Storage` interface `RaftNode` depends on,
and provides two implementations. See [`../persistence.md`](../persistence.md)
for the "why," this page for the package layout.

## The interface

```go
// internal/storage/storage.go
type Storage interface {
	SaveState(state model.PersistentState) error
	LoadState() (model.PersistentState, error)

	AppendEntries(entries []model.LogEntry) error
	ReplaceSuffix(from model.LogIndex, entries []model.LogEntry) error
	LoadEntries() ([]model.LogEntry, error)

	SaveSnapshot(snapshot model.Snapshot) error
	LoadSnapshot() (model.Snapshot, error)

	Sync() error
	Close() error
}
```

`RaftNode` only ever calls these methods — never touches a file, a socket,
or an in-memory map directly for persistence. This is what makes
`internal/raft`'s ~150 tests runnable without touching a disk (via
`MemoryStorage`) while still letting `WALStorage` provide real durability
in production.

## Implementations

| Type | File | Use case |
|---|---|---|
| `WALStorage` | `wal.go` | Durable, disk-backed, used by `cmd/raftiq` |
| `MemoryStorage` | `memory.go` | In-memory, non-durable, used in tests |

Both are constructed differently (`OpenWAL(path)` vs `NewMemoryStorage()`)
but satisfy the same interface, so `raft.NewRaftNodeWithStorage` doesn't
care which one it's given.

## Metrics

`internal/storage/metrics.go` defines a `StorageMetrics` interface
(`IncOperation`, `IncOperationError`, `ObserveOperationDuration`,
`IncSync`, `IncSyncError`, `ObserveSyncDuration`) with a `NoopStorageMetrics`
default. `WALStorage.SetMetrics` wires a real implementation (backed by
`internal/observability`) in `cmd/raftiq/main.go`.

## See also

- [`wal.md`](wal.md) — the on-disk record format and recovery algorithm.
- [`snapshots.md`](snapshots.md) — how snapshots are stored and restored.
